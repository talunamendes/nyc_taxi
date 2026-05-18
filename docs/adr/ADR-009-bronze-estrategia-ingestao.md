# ADR-009: Auto Loader como estratégia de ingestão Bronze

- Status: Accepted
- Date: 2026-05-14

## Context

A camada Bronze precisa carregar os Parquets do NYC TLC que já estão na landing zone (UC Volume, particionada `year=/month=`) para uma tabela Delta append-only. O requisito é que **novos arquivos sejam detectados e processados** sem reprocessar o que já foi ingerido.

O escopo inicial é pequeno (5 arquivos, Jan–Mai/2023), mas a estratégia escolhida vai dimensionar uma característica não-funcional importante: **como o pipeline se comporta quando arquivos novos chegam na landing** — seja pela próxima janela mensal do TLC, seja por reprocessamento manual de uma partition corrompida.

Avaliamos quatro abordagens no Databricks:

1. **Auto Loader** (`spark.readStream.format("cloudFiles")` com `trigger(availableNow=True)`).
2. **`COPY INTO`** (comando SQL declarativo, append-only por padrão).
3. **`spark.read.parquet` em batch** com controle manual de "já processados".
4. **DLT (Delta Live Tables)** com bronze definida como tabela streaming.

## Decision

A camada Bronze usa **Auto Loader in batch mode (`trigger(availableNow=True)`)**, com `schemaLocation` e `checkpointLocation` persistidos em UC Volumes dedicados (`_schemas` e `_checkpoints`).

**Por que essa escolha?**
Porque Auto Loader resolve **três problemas que estariam no nosso colo** com qualquer outra abordagem: (a) **descoberta de arquivos novos** via tracking de estado em checkpoint, sem precisar manter um registro próprio de "o que já foi ingerido"; (b) **idempotência ponto-a-ponto** — re-execuções do job só processam o que é genuinamente novo, sem listar diretório inteiro a cada vez; (c) **escalabilidade futura** — quando o volume cresce de 5 para milhares de arquivos, o mesmo código continua funcionando sem refatoração porque o estado é mantido pelo próprio Auto Loader. Para um pipeline MVP que precisa "permitir evolução", isso é mais valor por menos código que qualquer alternativa.

## Consequences

### Positivas

- Idempotência grátis: o checkpoint do Auto Loader registra quais arquivos foram processados; re-executar o job é seguro.
- `trigger(availableNow=True)` deixa o stream se comportar como batch — processa o pendente, termina, libera o cluster. Casa naturalmente com Workflows agendados, sem custo de stream contínuo ativo 24/7.
- Detecção de arquivos novos é incremental (`directory listing` otimizado em volumes pequenos, `file notification` em S3/ADLS quando escalar) — não relista tudo a cada execução.
- Schema evolution e linhagem (`_metadata.file_path`, `file_modification_time`) são fornecidos pelo próprio Auto Loader; não precisamos reimplementar.
- Mesma API se a fonte mudar de Parquet para JSON/CSV/Avro no futuro — só troca `cloudFiles.format`.

### Negativas (trade-offs)

- Dependência forte do Databricks: Auto Loader não roda fora do ecossistema (não há equivalente OSS). Reduz portabilidade do código se o projeto migrar de plataforma.
- Curva de aprendizado: opções de `cloudFiles.*` são abundantes e a interação entre `schemaLocation`, `schemaEvolutionMode` e `mergeSchema` no write é sutil — vide ADR-010.
- Volumes UC para `_checkpoints` e `_schemas` precisam ser criados antecipadamente; perder esses diretórios significa reprocessar tudo (mas é recuperável e seguro com append-only Delta).
- Custo de cluster mínimo por execução (mesmo que processe zero arquivo): `trigger(availableNow=True)` ainda precisa iniciar o Spark driver. Para volumes muito baixos com janela de execução rara, pode ser overhead relativo.

## Alternatives

### Rejeitada: `COPY INTO`

`COPY INTO` é o comando SQL nativo do Databricks para ingestão de arquivos em Delta. Tem rastreamento de arquivos processados (via metadados da tabela), é idempotente, declarativo, e mais simples de escrever (`COPY INTO bronze FROM '/volume/landing/...' FILEFORMAT = PARQUET`).

**Por que não a alternativa óbvia?**
Três razões:

1. **Schema evolution menos flexível**: `COPY INTO` aceita `MERGESCHEMA` mas não tem o equivalente ao `schemaEvolutionMode=addNewColumns` do Auto Loader, que separa **detecção de schema** (no `schemaLocation`) de **aplicação no destino** (via `mergeSchema`). Para o requisito de "permitir evolução", Auto Loader dá controle mais granular.
2. **Tracking de arquivos é uma caixa-preta**: `COPY INTO` rastreia internamente o que foi processado, mas o estado fica acoplado à tabela. Não há um `_checkpoints` que se possa inspecionar/manipular facilmente para reprocessar uma janela específica — você precisa de `COPY_OPTIONS('force' = 'true')`, que reprocessa tudo.
3. **Sem coluna virtual `_metadata`** do mesmo jeito: dá pra obter o arquivo de origem via `input_file_name()`, mas não há `file_modification_time` ou path-based metadata expostos uniformemente como no Auto Loader.

Para uma ingestão *one-shot* sem necessidade de schema evolution, `COPY INTO` seria perfeitamente adequado e até preferível pela simplicidade. Mas o requisito de "permitir evolução" e "detectar novos arquivos continuamente" pende para Auto Loader.

### Rejeitada: `spark.read.parquet` em batch + filtro manual

Ler tudo da landing, comparar com o que já existe na bronze (via subquery `WHERE _source_file NOT IN (SELECT ...)`), e inserir o diff.

**Por que não essa alternativa?** É a abordagem mais "manual" possível. Os problemas: (a) o filtro `NOT IN` cresce O(N) com a tabela bronze, ficando caro rapidamente; (b) precisa de cuidado adicional para garantir atomicidade (se o job morre no meio, o que aconteceu?); (c) reinventa o que Auto Loader resolve; (d) `spark.read.parquet` em diretório com schemas divergentes (caso comum em schema evolution) falha por default. Faz sentido só para POC bem rápida ou se Auto Loader não estiver disponível.

### Rejeitada: DLT (Delta Live Tables)

DLT define o pipeline declarativamente em SQL/Python, com expectations, lineage automática, e reruns gerenciados pelo Databricks.

**Por que não essa alternativa?** Três razões:

1. **Free Edition não suporta DLT** — bloqueador absoluto no escopo atual.
2. **Overhead conceitual**: DLT introduz outro modelo mental (`@dlt.table`, pipeline JSON, modes `triggered`/`continuous`) que não agrega valor proporcional para 5 arquivos.
3. **Acoplamento maior**: pipeline DLT é gerenciado pelo serviço Databricks, não roda em cluster genérico — reduz controle sobre lifecycle e debugging.

Vale reconsiderar se o projeto migrar para edição paga com múltiplas tabelas e expectations formais.

### Outras consideradas

- **MERGE em batch**: usado quando há upsert por chave. Não se aplica à bronze append-only — não temos chave estável para fazer merge (o mesmo Parquet pode ter trips com IDs duplicados em republicações do TLC, por isso bronze é "fato bruto" e dedupe vai pra silver).
- **Structured Streaming sem `cloudFiles`**: `spark.readStream.format("parquet")` existe, mas não tem tracking de arquivos novos da mesma forma — Auto Loader é a versão "produtiva" dessa API.

## Validation

Critérios de validação contínua:

- Re-execução do job sem mudança na landing deve resultar em `rows_ingested = 0` (idempotência).
- Inserir um Parquet novo (ex: simulando mês de Junho/2023) e re-executar deve resultar em **apenas o novo arquivo processado**, não todos.
- Checkpoint não deve crescer descontroladamente: monitorar tamanho do diretório `_checkpoints/bronze_yellow_trips` ao longo do tempo.
- Tempo de detecção de arquivos novos deve permanecer abaixo de ~10s para o volume atual; degradação significativa indica que o listing mode precisa ser revisitado (file notification em vez de directory listing).

**Quando essa decisão deve ser revisitada?**

- Quando o projeto sair do ecossistema Databricks — Auto Loader é proprietário e a portabilidade vai pesar.
- Quando o volume passar de algumas dezenas de milhares de arquivos: avaliar mudança de `cloudFiles` directory listing para file notification mode (SNS/Event Grid), que muda assunções de IAM/permissões.
- Quando o requisito mudar para latência sub-minuto contínua: `trigger(availableNow=True)` deixa de fazer sentido; migra para `trigger(processingTime='30 seconds')` com cluster sempre quente.
- Quando passarmos para Free Edition paga com DLT disponível: reavaliar se a declaratividade de DLT compensa o lock-in adicional.