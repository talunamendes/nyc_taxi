# ADR-006: Estratégia de ingestão HTTP para Landing Zone

- Status: Accepted
- Date: 2026-05-14

## Context

A landing zone do pipeline (`src/.../landing/main.py`) faz download dos arquivos Parquet do NYC TLC CDN e os persiste em UC Volume com particionamento Hive-style. Para o escopo inicial do case (Yellow Taxi, Jan–Mai/2023), são **5 arquivos grandes** (~500 MB a 1 GB cada), um por mês.

Antes de escolher como fazer essas chamadas HTTP, avaliamos quatro famílias de abordagens em um benchmark interno (vide notebook `benchmark_ingestao_api.ipynb`):

1. `requests` síncrono — uma chamada por vez, bloqueante.
2. `requests` + `ThreadPoolExecutor` — N threads paralelas no driver.
3. `aiohttp` / `httpx` assíncrono — concorrência via event loop.
4. Spark distribuído (`spark.read.json(rdd)` ou UDFs/`mapInPandas`) — paralelismo no cluster.

O benchmark mostrou que, para **muitas chamadas pequenas** (centenas de IDs, payload em KB), abordagens paralelas (threads, async, `mapInPandas`) chegam a ser 5–10× mais rápidas que o síncrono. Mas o cenário do case é o oposto: **poucas chamadas grandes**, dominadas por throughput de download, não por latência de handshake.

Precisamos definir a estratégia de ingestão HTTP da landing alinhada com:

- o volume real do case (5 arquivos, escalável até dezenas/mês em produção);
- a restrição de rodar em **Databricks Free Edition** (cluster single-node, sem autoscaling);
- a política de falha parcial já estabelecida no [ADR-003](./ADR-003-partial-failure-policy.md);
- a possibilidade futura de expandir para outras agências (Green Taxi, FHV), o que aumenta o nº de arquivos mas mantém a característica de “poucos arquivos grandes”.

## Decision

A ingestão da landing usa **`requests` síncrono com streaming (`stream=True` + `iter_content`) e retry exponencial**, processando um mês por iteração de um loop sequencial no driver.

Não usaremos paralelização (threads, async, Spark distribuído) na landing zone. A paralelização entra apenas a partir da camada Bronze, onde o Spark lê os Parquets já materializados em UC Volume.

**Por que essa escolha?**
Porque o gargalo do case é **throughput de bytes**, não **número de chamadas**. Para 5 arquivos de ~700 MB, o tempo de download é dominado pela banda do CDN e do driver, não pela latência por requisição. Paralelizar 5 downloads gigantes na mesma máquina **divide a mesma banda** entre eles — ganho prático próximo de zero, com custo extra de complexidade, gestão de retries por thread e maior chance de OOM ao manter múltiplos streams ativos. O streaming com `iter_content(1 MB)` mantém a memória do driver constante (~poucos MB) independente do tamanho do arquivo, o que é crítico no single-node do Free Edition.

## Consequences

### Positivas

- Código simples, linear, fácil de auditar e debugar — uma stack trace por mês, não N stacks concorrentes.
- Streaming garante memória constante: arquivos de 1 GB+ não derrubam o driver.
- Retry exponencial localizado por mês compõe naturalmente com a política do ADR-003 (falha de um mês não bloqueia os outros).
- Idempotência via checksum MD5 + verificação de existência é trivial de implementar e testar com loop sequencial.
- Sem necessidade de gerenciar pool de conexões, semáforos ou event loop — reduz superfície de bug.

### Negativas (trade-offs)

- Tempo total cresce linearmente com o número de meses. Para 12 meses (~10 GB), o tempo pode passar de 1h dependendo da banda; para 60 meses (5 anos), vira inviável.
- Não aproveita CPU/banda ociosa do driver enquanto espera resposta do servidor.
- Se a fonte mudar para uma API REST com milhares de chamadas pequenas (ex: paginação por dia), a abordagem precisa ser revista — o benchmark mostra que síncrono perde feio nesse cenário.

## Alternatives

### Rejeitada: `requests` + `ThreadPoolExecutor` no driver

**Por que não a alternativa óbvia?**
Threads são a paralelização mais barata em Python para I/O. Mas em arquivos grandes com download via streaming, a banda da NIC é o recurso disputado, não o GIL. Testes preliminares apontaram ganho marginal (<15%) para 5 arquivos, ao custo de:

- gestão de retries por thread (cada falha tem que ser reportada e contabilizada sem bagunçar o sumário JSON do ADR-003);
- N streams simultâneos na memória do driver, agravando risco de OOM no single-node;
- ordem de finalização não-determinística, complicando logs estruturados (`log_with_context` precisaria de correlation ID).

O ganho não compensa a complexidade para o volume do case. Reservamos threads para um cenário futuro de “muitos arquivos pequenos” (ex: ingestão diária particionada).

### Rejeitada: `aiohttp` / `httpx.AsyncClient`

Mesma análise do item anterior, agravada por dois fatores: (a) `requests` já é dependência do projeto, `aiohttp` adiciona uma nova; (b) `asyncio` em wheel task Databricks exige `nest_asyncio` ou cuidado com event loop, sem benefício prático neste perfil de carga. Faz sentido reconsiderar quando/se a fonte virar uma API paginada.

### Rejeitada: ingestão distribuída via Spark (`spark.read.parquet(url)` ou UDF)

Spark **lê** Parquets de URL HTTP/S3 nativamente, então em tese poderíamos pular a landing e ler direto do CDN para Bronze. Rejeitamos por três motivos:

1. **Auditoria**: o case exige landing zone com arquivos originais preservados; pular essa camada perde a possibilidade de reprocessar Bronze sem novo hit no CDN.
2. **Acoplamento com fonte externa**: cada reprocessamento Bronze dependeria do CDN estar no ar e respondendo dentro do timeout do executor. Desacoplar via landing é mais resiliente.
3. **Free Edition**: cluster single-node não ganha nada com paralelismo distribuído para 5 arquivos.

Já uma UDF que distribui downloads pelos executors faz sentido apenas em cluster multi-node com **muitos** arquivos (centenas+) — fora do perfil atual.

### Outras consideradas

- **`wget`/`curl` via shell**: descartado por perder integração com `log_with_context`, retry estruturado e exit codes da wheel task.
- **Auto Loader direto do CDN**: Auto Loader assume cloud storage (S3/ADLS/GCS), não HTTP. Não se aplica à landing — entra no fluxo Bronze, lendo do próprio UC Volume.

## Validation

Critérios de validação contínua:

- Tempo total de ingestão dos 5 meses deve permanecer abaixo de 30 min na Free Edition. Se ultrapassar, revisitar.
- Uso de memória do driver durante o download não deve exceder 1 GB (validável via `dbutils.fs.ls` no metadata + métricas do cluster).
- A política do ADR-003 deve continuar válida: nenhuma decisão desta ADR conflita com falha parcial por mês.
- Checksum MD5 persistido em `_ingestion_metadata.json` deve permitir verificação de integridade pós-ingestão.

**Quando essa decisão deve ser revisitada?**

- Quando o número de arquivos por execução passar de ~20 (ex: ingestão diária ou expansão para Green/FHV) — nesse ponto, threads no driver passam a compensar o overhead.
- Quando a fonte mudar de CDN de arquivos para API REST paginada — o perfil vira “muitas chamadas pequenas” e async/`mapInPandas` passam a dominar.
- Quando o pipeline sair da Free Edition para cluster multi-node — a paralelização distribuída via UDF/`mapInPandas` se torna viável e o trade-off muda.
- Quando o tamanho médio dos arquivos crescer a ponto de o tempo sequencial estourar o SLA da janela de execução.