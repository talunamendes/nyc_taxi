# ADR-014: Gold Data Model — uma view única de consumo

- Status: Accepted
- Date: 2026-05-18

## Context

A camada Gold consome as silvers conformadas (`yellow_taxi_trips`,
`green_taxi_trips` — ver ADR-013) e é o ponto de entrega para o
consumidor analítico. Ela precisa, no mínimo, satisfazer o contrato
de consumo:

> "É necessário garantir que as colunas `VendorID`, `passenger_count`,
> `total_amount`, `tpep_pickup_datetime` e `tpep_dropoff_datetime`
> estejam presentes na camada de consumo. As outras colunas podem
> ser ignoradas."

E precisa permitir que sejam respondidas as duas perguntas analíticas
suportadas (a média mensal de `total_amount` em yellow, e a média de
`passenger_count` por hora em maio/2023 considerando todos os taxis).

Características relevantes do escopo:

- **Volume**: ~15M linhas yellow + ~3.5M green ≈ 18M linhas para 5
  meses (jan–mai/2023). Aggregation ao vivo sobre esse volume custa
  poucos segundos em Serverless.
- **Tráfego de consumo**: escopo atual — as queries das duas
  perguntas rodam algumas vezes durante revisão, não centenas de
  vezes ao dia.
- **Governança**: leitor único. Não há múltiplas equipes
  reimplementando a mesma métrica nem requisito de auditoria
  financeira sobre o valor histórico de cada métrica.
- **Heterogeneidade de schema**: yellow publica `tpep_pickup_datetime` /
  `tpep_dropoff_datetime`; green publica `lpep_*` (ADR-013 preserva
  os nomes nativos no silver). O nome `tpep_*` exigido pelo contrato
  de consumo precisa ser exposto na gold para corridas green também.
- **Diretriz do projeto**: "evitar over-engineering" está nas
  instruções explícitas do repositório.

A pergunta arquitetural é: a gold deve materializar **fatos
pré-agregados** que respondam diretamente cada pergunta analítica, ou
deve ser uma **camada fina de consumo** sobre o silver, com as perguntas
respondidas por SQL ad-hoc?

## Decision

A Gold publica **um único objeto**: a view `gold.vw_taxi_trips`.

```sql
CREATE OR REPLACE VIEW <catalog>.gold.vw_taxi_trips (
    VendorID              COMMENT '...',
    passenger_count       COMMENT '...',
    total_amount          COMMENT '...',
    tpep_pickup_datetime  COMMENT '...',
    tpep_dropoff_datetime COMMENT '...',
    taxi_type             COMMENT 'yellow | green'
)
COMMENT 'Gold: view de consumo unificada (yellow+green) com as colunas obrigatorias do contrato de consumo.'
AS
SELECT VendorID, passenger_count, total_amount,
       tpep_pickup_datetime, tpep_dropoff_datetime,
       'yellow' AS taxi_type
FROM <catalog>.silver.yellow_taxi_trips
UNION ALL
SELECT VendorID, passenger_count, total_amount,
       lpep_pickup_datetime  AS tpep_pickup_datetime,
       lpep_dropoff_datetime AS tpep_dropoff_datetime,
       'green' AS taxi_type
FROM <catalog>.silver.green_taxi_trips;
```

Características:

- **VIEW e não TABLE**: a única transformação é `UNION ALL` + alias de
  coluna — ambas metadata-only no Spark/Delta. A view não duplica
  armazenamento e fica sempre fresca em relação ao silver. Cada
  `SELECT` paga o mesmo full scan que o consumidor pagaria fazendo a
  UNION manualmente.
- **`CREATE OR REPLACE VIEW`** com schema declarado (`(col COMMENT ...)`):
  contrato explícito visível no DDL; redeploy reescreve a definição
  sem `DROP` prévio.
- **Alias `lpep_*` -> `tpep_*` na metade green do UNION**: normaliza o
  nome ao contrato de consumo sem perder a linhagem (`taxi_type`
  literal preserva a origem por linha).
- **Sem `SET TAGS`**: `ALTER VIEW SET TAGS` não é uniforme entre
  runtimes; metadados de governança ficam no `COMMENT`.

**As perguntas analíticas são respondidas por SQL ad-hoc**
contra essa view, vivendo em `analysis/perguntas_analiticas.sql`. Não há
tabela `fct_*` pré-agregada no Lakehouse.

**Por que essa escolha?**

Porque pré-agregação **só paga dividendo quando pelo menos uma destas
três condições é verdade**:

1. **Volume torna `GROUP BY` ao vivo caro** ao ponto do pré-agregado
   economizar tempo de espera real.
2. **Tráfego de consumo é repetitivo** ao ponto do custo do refresh
   único da pre-agregação ser amortizado por milhares de leituras
   baratas.
3. **Governança ou auditoria exige snapshot imutável** da métrica
   (ex.: relatório regulatório que precisa reproduzir o número de uma
   data específica).

**Nenhuma das três condições existe no escopo atual**: 18M linhas
agregam em segundos; o tráfego de consumo é a query rodando algumas
vezes; não há requisito regulatório. Materializar fatos aqui
adicionaria tabelas Delta, código de `ensure_*`, `build_*` e
`overwrite`, refresh adicional a cada execução do job — sem
contrapartida em performance, em correção ou em valor de negócio.

Também há um argumento de fidelidade ao requisito: a resposta esperada
é "código SQL ou PySpark estruturado **da forma que preferir** com as
respostas para as perguntas analíticas". Resposta = query; o caminho
mais direto é colocar a query em `analysis/` rodando contra a view,
não pré-materializar a resposta dentro de uma tabela.

A análise de "fato por pergunta" continua válida como princípio em
contextos de produção com volume + tráfego + governança que justifiquem.
Quando esse contexto aparecer, novos fatos viram ADR específico —
ver "Quando essa decisão deve ser revisitada".

## Consequences

### Positivas

- **Camada gold mínima e legível**: 1 objeto, 1 DDL, ~120 linhas de
  Python (a maioria docstring). Onboarding e revisão de PR são
  instantâneos.
- **Frescor garantido**: a view não tem janela de staleness — leituras
  refletem o estado mais recente do silver no instante da query.
- **Zero storage adicional**: a transformação é metadata-only. O custo
  marginal da gold é zero em armazenamento.
- **Contrato de consumo atendido no nível certo da pilha**: as 5
  colunas obrigatórias estão na *camada de consumo* (gold), não
  enterradas na silver (que mantém os nomes nativos do TLC, `lpep_*`
  inclusive).
- **Reduz a superfície de erros operacionais**: sem refresh de fato
  significa que não há "fato desatualizado em relação ao silver"
  possível.
- **Coerente com a diretriz do projeto** ("evitar over-engineering")
  e com a expectativa do contrato (SQL com a resposta).

### Negativas (trade-offs)

- **Cada consulta às perguntas analíticas paga o custo do `GROUP BY`
  sobre o silver**. Para 18M linhas em Serverless é segundos; para
  bilhões seria minutos. O ponto onde isso vira problema é objetivo
  e mensurável — quando aparecer, viramos para fatos pré-agregados
  (ver "Quando revisitar").
- **A definição da métrica vive na query SQL, não na schema da
  tabela**. Para um caso com 2 perguntas e 1 consumidor isso não
  importa; para 10 dashboards de 4 squads diferentes, importa muito.
  Mitigação: as queries vivem em `analysis/perguntas_analiticas.sql`,
  versionadas com o pipeline.
- **Sem snapshot imutável da métrica**: o consumidor que rodar a
  query hoje pode obter número diferente do que obteve ontem se a
  silver foi reprocessada. Aceitável dado que o silver é idempotente
  (MERGE pela chave de negócio, ADR-013) e o reprocessamento corrige —
  nunca "esquece" — dado.
- **Sem cluster keys na gold**: View não tem clustering físico. Mas
  filtros temporais aplicados na query são empurrados ao silver, que
  tem Liquid Clustering por `pickup_date` (ADR-012). Skipping ainda
  acontece — só não acontece "duas vezes".

## Alternatives

### Rejeitada: um fato pré-agregado por pergunta (`fct_yellow_trips_monthly` + `fct_taxi_trips_hourly_may2023`)

Cada pergunta vira uma tabela Delta com a métrica como coluna
materializada; consumo é `SELECT ... ORDER BY ...` sem `GROUP BY`,
refresh por `INSERT OVERWRITE` atômico a cada execução do job.

**Por que não essa alternativa?**

Tem razão de ser quando há volume/tráfego/governança que justifique o
custo de manter as tabelas. **Nenhum dos três fatores aplica ao
escopo atual**:

1. **Volume**: agregação ao vivo sobre 18M linhas custa segundos em
   Serverless. Pre-agregação economizaria... segundos. Não muda a
   experiência do consumidor.
2. **Tráfego**: a query roda algumas vezes durante revisão. O custo
   do refresh único da pré-agregação **excede** o custo somado de
   todas as execuções ad-hoc projetadas no horizonte atual.
3. **Governança**: leitor único. Não há squads reimplementando a
   métrica de jeitos diferentes; não há auditor pedindo "qual era o
   valor desta métrica em 12/maio". A definição vive no SQL versionado
   em `analysis/`, e isso é suficiente.

Manter os fatos seria sinalização de "sei modelagem dimensional"
mais do que necessidade técnica — e a diretriz do projeto rejeita
explicitamente over-engineering. **Quando alguma das três condições
aparecer**, promover métricas específicas a fatos pré-agregados é
direto: a query já existe em `analysis/`, virar fato é
`CREATE TABLE AS SELECT` + agendamento.

### Rejeitada: tabela única "tabelão" materializada (`gold.fct_taxi_trips`)

Materializar a UNION yellow + green como tabela Delta (não view), com
clustering físico próprio na gold.

**Por que não essa alternativa?**

Duplica armazenamento sem economizar computação significativa: a
única transformação sobre o silver é alias de coluna (metadata-only)
e `UNION ALL`. View entrega o mesmo resultado lendo direto do silver,
que já tem Liquid Clustering apropriado (ADR-012). Materializar seria
pagar storage 2× para o mesmo `EXPLAIN`. Faria sentido apenas se a
gold tivesse cluster keys *diferentes* das do silver para um padrão
de query distinto — não é o caso.

### Rejeitada: star schema completo (`dim_date`, `dim_taxi_zone`, `dim_vendor`, `fct_trips_hourly`)

Modelagem dimensional formal desde já.

**Por que não essa alternativa?**

Especulação prematura. O escopo atual tem 2 perguntas; modelo
dimensional paga dividendo a partir de ~10 queries recorrentes. Fazer
agora seria exatamente o tipo de over-engineering que o projeto pede
para evitar.

### Rejeitada: não publicar nada na gold; deixar consumo direto no silver

Consumir `silver.yellow_taxi_trips` e `silver.green_taxi_trips`
diretamente, sem camada gold.

**Por que não essa alternativa?**

Falharia o contrato de consumo: as colunas obrigatórias
(`tpep_pickup_datetime`, `tpep_dropoff_datetime`) não existem em
green-silver — lá são `lpep_*` (ADR-013 preserva os nomes nativos do
TLC no silver, por boas razões). A gold é onde esse contrato se
realiza. Sem a view, o consumidor precisa conhecer a heterogeneidade
do TLC; com a view, o contrato é uniforme.

## Validation

Critérios de validação contínua:

- **Schema fidelity**: `DESCRIBE EXTENDED <catalog>.gold.vw_taxi_trips`
  deve listar exatamente as 6 colunas declaradas
  (`_CONSUMPTION_REQUIRED_COLUMNS` + `taxi_type`). Drift = PR explícito.
- **Sanity numérico**: as duas queries de Pergunta A e B em
  `analysis/perguntas_analiticas.sql` devem rodar sem erro e produzir
  resultado coerente com os dicionários do TLC (ex.: `avg_passenger_count`
  entre 1 e 9).
- **Idempotência**: rodar a gold duas vezes seguidas deve produzir
  exatamente o mesmo objeto. Garantido pelo `CREATE OR REPLACE VIEW`.
- **Custo de query monitorado**: se `EXPLAIN` da query de Pergunta B
  passar a ler frações altas do silver inteiro (filtros não empurrados
  para o cluster key), é gatilho para revisitar.

**Quando essa decisão deve ser revisitada?**

A pré-agregação volta à mesa quando **pelo menos uma destas três
condições virar verdade**:

1. **Volume cresce significativamente**: silver passa de ~50M linhas
   e/ou a query de Pergunta B começa a custar dezenas de segundos
   consistentemente. Métrica objetiva: tempo médio da query
   ad-hoc > 10s.
2. **Tráfego de consumo recorrente**: a mesma query passa a ser
   executada por dashboards/jobs >50 vezes/dia. Métrica objetiva:
   query history mostrando padrão de chamada repetida.
3. **Governança/auditoria requer snapshot imutável**: chega exigência
   de "qual era o valor desta métrica em dia X" reprodutível sem
   reprocessar silver. Métrica objetiva: requisito formal de auditoria.

Quando qualquer uma virar verdade, o caminho é direto: a query da
Pergunta correspondente em `analysis/perguntas_analiticas.sql` vira
um fato pré-agregado (`CREATE TABLE AS SELECT` + agendamento mensal),
e este ADR ganha um sucessor explicando o que mudou e por quê.

Outros gatilhos de revisão:

- **Surge uma terceira pergunta cross-cutting** que se beneficie de
  modelagem dimensional formal (`dim_*` + `fct_*`).
- **Feature store ou ML serving entram no escopo**: pode justificar
  promover algumas agregações para tabelas materializadas que sirvam
  features online.
- **Múltiplas equipes consomem a gold**: o risco de drift de definição
  de métrica passa a ser real, e fatos pré-agregados como fonte única
  de verdade voltam a ser a resposta certa.
