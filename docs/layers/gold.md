# Gold Layer

## Papel da camada

A camada `gold` consome as silvers (`yellow_taxi_trips`,
`green_taxi_trips`) e publica **uma unica view de consumo** que
materializa o contrato das colunas obrigatorias na camada analitica:

> "É necessário garantir que as colunas `VendorID`, `passenger_count`,
> `total_amount`, `tpep_pickup_datetime` e `tpep_dropoff_datetime`
> estejam presentes na camada de consumo."

A view une yellow + green com alias `lpep_*` -> `tpep_*` na metade
green e adiciona `taxi_type` literal para lineage por linha. As duas
perguntas analiticas suportadas sao respondidas por SQL ad-hoc rodando
contra essa view — ver `analysis/perguntas_analiticas.sql`.

A justificativa de modelagem (uma view de consumo, sem fatos
pre-agregados) esta no
[ADR-014 — Gold Data Model: uma view unica de consumo](../adr/ADR-014-gold-data-model-per-question.md).
Em resumo: pre-agregacao paga dividendo apenas com volume / trafico
de consumo / governanca que nao existem no escopo atual
(~18M linhas, leitor unico, sem auditoria regulatoria). Materializar
fatos aqui seria over-engineering, contra a diretriz do projeto.

Arquivo de referencia: `src/nyc_taxi/lakehouse/gold/main.py`.

## Implementacao atual

### Entradas de CLI

O entrypoint `gold_layer` (alias de `publish_gold`) recebe:

- `--catalog`
- `--environment` (`dev`, `stg`, `prd`)

Sem `--taxi-type`: a view une ambos os taxis em todas as execucoes.

### Bootstrap Databricks

`_get_spark()` cria `SparkSession`. Sem `DBUtils` (gold nao manipula
filesystem). Fora de cluster Databricks, levanta `RuntimeError`.

### Objeto resultante

```
nyc_taxi_dev.gold.vw_taxi_trips
```

Schema da view:

| Coluna                  | Origem                                                            |
| ----------------------- | ----------------------------------------------------------------- |
| `VendorID`              | nativo em yellow e green.                                         |
| `passenger_count`       | nativo em yellow e green.                                         |
| `total_amount`          | nativo em yellow e green.                                         |
| `tpep_pickup_datetime`  | yellow: nativo; green: alias de `lpep_pickup_datetime`.           |
| `tpep_dropoff_datetime` | yellow: nativo; green: alias de `lpep_dropoff_datetime`.          |
| `taxi_type`             | literal (`'yellow'` ou `'green'`) — extra para lineage por linha. |

Materializada como VIEW (`CREATE OR REPLACE VIEW`), nao tabela:

- nenhuma transformacao alem de `UNION ALL` + alias de colunas;
- `UNION ALL` e alias de coluna sao metadata-only no Spark/Delta —
  view nao paga CPU adicional sobre o full scan que o silver ja faria;
- nao duplica armazenamento;
- esta sempre fresca em relacao ao silver, sem refresh adicional.

### DDL emitido

```sql
CREATE OR REPLACE VIEW <catalog>.gold.vw_taxi_trips (
    VendorID              COMMENT '...',
    passenger_count       COMMENT '...',
    total_amount          COMMENT '...',
    tpep_pickup_datetime  COMMENT '...',
    tpep_dropoff_datetime COMMENT '...',
    taxi_type             COMMENT 'yellow | green'
)
COMMENT 'Gold: view de consumo unificada (yellow+green) ...'
AS
SELECT
    VendorID, passenger_count, total_amount,
    tpep_pickup_datetime, tpep_dropoff_datetime,
    'yellow' AS taxi_type
FROM <catalog>.silver.yellow_taxi_trips
UNION ALL
SELECT
    VendorID, passenger_count, total_amount,
    lpep_pickup_datetime  AS tpep_pickup_datetime,
    lpep_dropoff_datetime AS tpep_dropoff_datetime,
    'green' AS taxi_type
FROM <catalog>.silver.green_taxi_trips;
```

`CREATE OR REPLACE VIEW` e idempotente: re-deploy reescreve a
definicao sem `DROP` previo.

`_CONSUMPTION_REQUIRED_COLUMNS` no modulo e a fonte unica de verdade
das colunas obrigatorias do contrato de consumo. Mudancas no contrato
so podem entrar via PR alterando essa tupla, casando com a DDL e com
`analysis/perguntas_analiticas.sql`.

### Politica de falha

**Fail-fast**: erro no `CREATE VIEW` interrompe o job (mesma politica
do silver — gold e camada de contrato). Diferente da bronze, onde
falha parcial e proposital.

### Saida e codigos de retorno

`main()` imprime JSON com o status do objeto publicado:

```json
{
  "objects": {
    "nyc_taxi_dev.gold.vw_taxi_trips": {"status": "ok"}
  }
}
```

Retorno:

- `0` quando a view foi publicada com sucesso;
- exit nao-zero (propagado pela excecao) em qualquer falha.

## Exemplos de execucao

```bash
publish_gold --catalog=nyc_taxi_dev --environment=dev
```

Via bundle (mesmo schedule mensal das demais camadas):

```bash
make dab-run ENV=dev WORKFLOW=nyc_taxi_job CATALOG=nyc_taxi_dev
```

## Consultas analiticas (respostas as perguntas analiticas)

As duas perguntas analiticas suportadas rodam direto contra
`vw_taxi_trips`. SQL completo em `analysis/perguntas_analiticas.sql`.

### Pergunta A — media de `total_amount` por mes (yellow)

```sql
SELECT
    date_trunc('month', tpep_pickup_datetime) AS month_ref,
    AVG(total_amount)                          AS avg_total_amount,
    COUNT(*)                                   AS trips_count
FROM <catalog>.gold.vw_taxi_trips
WHERE taxi_type = 'yellow'
GROUP BY 1
ORDER BY 1;
```

### Pergunta B — media de `passenger_count` por hora em mai/2023 (todos os taxis)

```sql
SELECT
    hour(tpep_pickup_datetime) AS hour_of_day,
    AVG(passenger_count)       AS avg_passenger_count,
    COUNT(*)                   AS trips_count
FROM <catalog>.gold.vw_taxi_trips
WHERE tpep_pickup_datetime >= TIMESTAMP('2023-05-01')
  AND tpep_pickup_datetime <  TIMESTAMP('2023-06-01')
  AND passenger_count IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

## Riscos tecnicos atuais

- **Cada query paga o custo do `GROUP BY` ao vivo sobre o silver**. Para
  o escopo atual (~18M linhas, Serverless) e' segundos; para silver
  com bilhoes de linhas isso vira problema. Ver gatilhos em ADR-014
  ("Quando essa decisao deve ser revisitada").
- **Definicao da metrica fora do schema**: a media mensal/horaria nao
  vive numa coluna de tabela — vive na query SQL em `analysis/`.
  Aceitavel com um consumidor; vira problema se varias equipes
  reimplementarem a metrica de jeitos diferentes.
- **Sem snapshot imutavel**: rodar a mesma query hoje e amanha pode
  dar valores diferentes se a silver foi reprocessada nesse intervalo.
  Para auditoria financeira, isso e' insuficiente. Mitigacao: silver
  e' idempotente (MERGE pela chave de negocio, ADR-013), entao
  reprocessamento corrige — nunca esquece — dado.
- **View sem tags UC**: `ALTER VIEW SET TAGS` nao e' uniforme entre
  runtimes. Tags ficam embutidas no `COMMENT` por enquanto.

## Proximos passos sugeridos

- **Materializar fatos pre-agregados** quando volume / trafico /
  governanca justificar (criterios objetivos em ADR-014). A migracao e'
  direta: a query em `analysis/` vira `CREATE TABLE AS SELECT` +
  agendamento mensal.
- **Migrar para `SET TAGS`** quando o suporte em `ALTER VIEW` for
  uniforme entre runtimes — alinhar com o tagging das outras camadas.
- **Adicionar testes de regressao** com fixture de silver minimo para
  validar numericamente as duas metricas (sanity check semantico).
- **Promover algumas agregacoes para feature store** quando ML
  serving entrar no escopo (cenario fora do escopo atual).
