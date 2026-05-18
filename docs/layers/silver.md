# Silver Layer

## Papel da camada

A camada `silver` consome a bronze e materializa **uma tabela Delta
conformada por tipo de taxi**:

- `nyc_taxi_dev.silver.yellow_taxi_trips`
- `nyc_taxi_dev.silver.green_taxi_trips`

Cada tabela preserva o schema nativo da fonte (yellow mantem `tpep_*`,
green mantem `lpep_*`) e **todas** as colunas vindas da bronze. A
silver adiciona apenas colunas especificas do pipeline (`pickup_date`,
`_bronze_ingestion_ts`, `_silver_processed_ts`).

A justificativa formal dessa modelagem esta no
[ADR-013 — Silver Data Model per Taxi](../adr/ADR-013-silver-data-model-per-taxi.md).
Em resumo: preservamos sinais analiticos exclusivos (`airport_fee`,
`trip_type`), evitamos schema poluido com NULLs estruturais e mantemos
lineage limpa por dataset.

Arquivo de referencia: `src/nyc_taxi/lakehouse/silver/main.py`.

## Implementacao atual

### Entradas de CLI

O entrypoint `transform_silver` recebe:

- `--taxi-type` (`yellow`, `green` ou `both`; default `both`)
- `--catalog`
- `--environment` (`dev`, `stg`, `prd`)

Nao existe filtro por ano/mes na silver. O MERGE com a chave de negocio
garante idempotencia ao reprocessar a bronze inteira.

### Bootstrap Databricks

`_get_spark()` cria `SparkSession`. Esta camada nao usa `DBUtils`. Fora
de cluster Databricks, levanta `RuntimeError`.

### Resolucao da lista de taxis

`_resolve_taxi_types(args.taxi_type, cfg)` expande `both` em
`cfg.supported_taxi_types`; valor unico vira tupla de 1 elemento. O loop
principal itera pela tupla, em ordem.

### Mapa de colunas nativas

```python
_PICKUP_COLS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green":  ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
}
```

Esse map e a unica fonte de verdade para nome de colunas de timestamp.
Nada e renomeado entre taxis (ver ADR-013).

### Garantia de tabela (por taxi)

`ensure_silver_table(cfg, taxi_type, spark)` executa, por taxi:

1. `CREATE TABLE IF NOT EXISTS cfg.silver_table_fqn_for(taxi_type)`
   **sem lista de colunas** — mesmo padrao da bronze. O schema vem do
   primeiro write em `merge_into_silver`.
2. `TBLPROPERTIES`:
   - `delta.autoOptimize.optimizeWrite = true`
   - `delta.autoOptimize.autoCompact = true`
   - `delta.enableChangeDataFeed = true`
   - `delta.columnMapping.mode = name`
   - `delta.minReaderVersion = 2`, `delta.minWriterVersion = 5`
   - `delta.feature.timestampNtz = supported`
   - `COMMENT` referenciando o tipo de taxi
3. `ALTER TABLE ... SET TAGS` com `layer = silver`, `domain = mobility`,
   `taxi_type = <yellow|green>`, `criticality = tier-2`, `pii = none`.

Note que **`CLUSTER BY` nao entra na DDL**. O Liquid Clustering por
`pickup_date` so e aplicado depois do primeiro write — quando a coluna
ja existe na tabela. Ver "MERGE / upsert" abaixo.

Por que sem schema declarado? Para casar com o padrao da bronze e
deixar a silver evoluir junto com a fonte. Schema evolution
(`mergeSchema=true` no append, `.withSchemaEvolution()` no builder
do MERGE) garante que colunas novas no TLC fluam para a silver sem
mudanca de codigo. Evitamos
`spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`
porque Serverless (ADR-003) bloqueia essa configuracao com
`SERVERLESS_DELTA_SCHEMA_AUTO_MERGE_ENABLED`.

Por que `CLUSTER BY (pickup_date)`: as analises da gold (media mensal,
media por hora em maio) filtram/agrupam por tempo. `taxi_type` esta
implicito no nome da tabela, entao nao entra como cluster key
(ADR-012).

### Regras de DQ (limpeza + validacao)

A validacao e ancorada nos **data dictionaries oficiais do TLC**
(`docs/nyc/data_dictionary_trip_records_{yellow,green}.pdf`). As
constantes ficam no topo do modulo — atualizar uma e a unica
alteracao de codigo necessaria quando o TLC publicar valor novo.

Enums aceitos:

| Constante               | Valores                  | Origem                                            |
| ----------------------- | ------------------------ | ------------------------------------------------- |
| `_VALID_VENDOR_IDS`     | `(1, 2, 6, 7)`           | Uniao yellow+green (yellow tem 7, green nao)      |
| `_VALID_RATECODE_IDS`   | `(1, 2, 3, 4, 5, 6, 99)` | Data dictionary                                   |
| `_VALID_PAYMENT_TYPES`  | `(0, 1, 2, 3, 4, 5, 6)`  | Data dictionary                                   |
| `_VALID_STORE_FWD_FLAGS`| `("Y", "N")`             | Data dictionary                                   |
| `_VALID_TRIP_TYPES`     | `(1, 2)`                 | Data dictionary (green-only)                      |

Limites sane (conservadores — aim e descartar garbage obvio, nao virar
fraud detector):

| Constante                               | Valor       | Justificativa                                                          |
| --------------------------------------- | ----------- | ---------------------------------------------------------------------- |
| `_MAX_TRIP_DURATION_SECONDS`            | `24 * 3600` | 24h cobre ate trips muito longos                                       |
| `_MAX_TRIP_DISTANCE_MILES`              | `200`       | NYC + arredores; > 200mi e garbage                                     |
| `_MAX_PASSENGER_COUNT`                  | `9`         | Vans do TLC vao ate ~6; 9 e folga                                      |
| `_MIN_LOCATION_ID` / `_MAX_LOCATION_ID` | `1..265`    | TLC Taxi Zones: 1–263 reais + 264 (Unknown) + 265 (Outside NYC)        |

`_validation_expression(taxi_type, fn)` monta a expressao `Column`
booleana com:

- **Chaves de negocio**: `VendorID` no enum + `pickup`/`dropoff` not
  null. `isin` em NULL retorna NULL (falsy em WHERE), entao drop de
  NULL e implicito.
- **Coerencia temporal**: `dropoff > pickup` e `duration <= 24h`.
- **Valores monetarios**: `total_amount >= 0` obrigatorio (descarta
  voided/disputed); `fare_amount`, `tip_amount`, `tolls_amount`
  nulaveis mas `>= 0` quando informados.
- **Distancia e passageiros**: nulaveis; ranges sane quando presentes.
- **Enums do TLC** (`RatecodeID`, `payment_type`, `store_and_fwd_flag`,
  `trip_type`): NULL aceito (= "missing data"), valores fora do
  dicionario rejeitados.
- **Taxi Zones**: nulaveis; `1..265` quando informadas.

A funcao recebe `fn` (`pyspark.sql.functions`) como argumento
explicito para permitir testar a montagem da expressao sem
SparkSession.

### Construcao do DataFrame (por taxi)

`build_silver_dataframe(cfg, taxi_type, spark)`:

1. valida `taxi_type` contra `_PICKUP_COLS`;
2. le `cfg.bronze_table_fqn_for(taxi_type)` — todas as colunas, sem
   `select` explicito;
3. aplica `where(_validation_expression(taxi_type, F))` — DQ inline
   com drop dos registros invalidos;
4. renomeia `_ingestion_ts` para `_bronze_ingestion_ts` (lineage
   explicito de qual camada produziu o timestamp);
5. adiciona `pickup_date = to_date({pickup_col})` (chave do Liquid
   Clustering);
6. adiciona `_silver_processed_ts = current_timestamp()`;
7. `dropDuplicates(["VendorID", pickup_col, dropoff_col])`.

`taxi_type` nao entra na chave de dedup porque cada tabela ja e
fisicamente segregada por taxi (ADR-013).

Nao ha `select` final — o schema completo da bronze flui para a
silver. Se o TLC publicar coluna nova, ela chega ao destino via
schema evolution sem mudanca de codigo.

### MERGE / upsert (por taxi)

`merge_into_silver(cfg, taxi_type, spark, silver_df)` tem dois
caminhos, decididos por `len(spark.table(table_fqn).schema) > 0`:

**Primeiro run** (tabela criada por `ensure_silver_table` mas ainda
sem schema):

1. `silver_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_fqn)`
   — o append materializa o schema a partir do DF;
2. `ALTER TABLE {table_fqn} CLUSTER BY (pickup_date)` — agora a
   coluna `pickup_date` existe, entao o Liquid Clustering pode ser
   declarado. Operacao metadata-only e idempotente.

**Runs seguintes** (tabela ja tem schema):

1. `DeltaTable.forName(spark, table_fqn).alias("t").merge(silver_df.alias("s"), <chave>)`
   com:
   ```sql
   ON t.VendorID    = s.VendorID
   AND t.{pickup}   = s.{pickup}
   AND t.{dropoff}  = s.{dropoff}
   ```
2. `.withSchemaEvolution()` no builder do merge — colunas novas vindas
   da bronze sao adicionadas ao schema da silver automaticamente.
   Substitui o antigo
   `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`,
   bloqueado em Serverless (ADR-003) com
   `SERVERLESS_DELTA_SCHEMA_AUTO_MERGE_ENABLED`. Disponivel em
   delta-spark 3.3+ / DBR 15.3+, runtime do Serverless atual.
3. `whenMatchedUpdateAll()` + `whenNotMatchedInsertAll()`. Com schema
   evolution habilitada no builder, `*All()` propaga o schema completo,
   inclusive colunas novas.

Por que nao declarar schema na DDL e usar MERGE direto desde o
primeiro run? Para manter o mesmo padrao da bronze (CREATE TABLE sem
schema) e deixar a silver evoluir junto com a fonte, sem duplicar
declaracao de colunas.

Re-rodar com a mesma bronze nao duplica corridas — o dedup do batch
e a chave do MERGE garantem isso. Re-publicacao corrigida pelo TLC
sobrescreve o registro existente (UPDATE) em vez de duplicar.

### Politica de falha por taxi

**Fail-fast**: o loop principal trata cada taxi em `try/except`,
mas a clausula `except` sempre faz `raise` apos logar. Yellow
falhando interrompe green e o job inteiro vai a erro.

Diferente da bronze (onde a tolerancia parcial e proposital), aqui
qualquer falha mata o job. Silver e camada de contrato: publicar
parcial induziria gold/consumidores a verem snapshot inconsistente.

### Saida e codigos de retorno

`main()` imprime JSON com o breakdown dos taxis processados com
sucesso:

```json
{
  "per_taxi": {
    "yellow": {"status": "ok"},
    "green":  {"status": "ok"}
  }
}
```

Retorno:

- `0` quando todos os taxis processaram com sucesso;
- exit nao-zero (propagado pela excecao) em qualquer falha. A silver
  nao publica parcial — se yellow falhou antes de green ser tentado,
  green nem aparece no JSON.

Nao ha contagem de linhas no JSON: `silver_df.count()` foi removido
para evitar action adicional. Quando observabilidade fina for
necessaria, usar `lastOperationMetrics` da Delta (mais barato e
preciso que `count()` no DF).

## Exemplos de execucao

Yellow + Green (default):

```bash
transform_silver --catalog=nyc_taxi_dev --environment=dev
```

Apenas green:

```bash
transform_silver --taxi-type=green --catalog=nyc_taxi_dev --environment=dev
```

## Tabelas resultantes

```
nyc_taxi_dev.silver.yellow_taxi_trips   -- tpep_pickup_datetime, tpep_dropoff_datetime
nyc_taxi_dev.silver.green_taxi_trips    -- lpep_pickup_datetime, lpep_dropoff_datetime
```

Ambas com `pickup_date` derivado e Liquid Clustering pelo mesmo campo.
Schema completo de cada tabela = schema do bronze respectivo +
`pickup_date` + `_silver_processed_ts` (e `_ingestion_ts` renomeado
para `_bronze_ingestion_ts`).

## Quarentena

`PipelineConfig` ja preve quarentena simetrica por taxi:

- `silver_quarantine_table_pattern = "{taxi_type}_taxi_trips_quarantine"`
- `cfg.silver_quarantine_table_fqn_for("yellow"|"green")`

A implementacao atual aplica DQ inline (filtros no `WHERE`) e descarta
silenciosamente as linhas invalidas — nao escreve em quarentena ainda.
A nomenclatura e o FQN ficam pre-definidos para quando o ruleset for
materializado.

## Riscos tecnicos atuais

- **DQ inline nao registra o que foi descartado**: nao da para
  auditar retroativamente o "porque" de uma linha sumir. Mitigacao
  futura: materializar quarentena por taxi.
- **Sem contagem de linhas no resumo**: removida no refactor mais
  recente para simplificar. Observabilidade fina exige consulta a
  `DESCRIBE HISTORY` ou `lastOperationMetrics` do MERGE.
- **`total_amount >= 0`** exclui estornos legitimos do TLC; pode ser
  inadequado para casos de auditoria financeira.
- **Sem leitura incremental** (CDF/watermark): cada execucao varre
  toda a bronze do taxi. Funciona pelo MERGE idempotente, mas custa
  O(bronze) a cada run.
- **Listas de enums e ranges manuais**: se o TLC mudar o data
  dictionary, as constantes ficam desatualizadas silenciosamente ate
  alguem perceber o aumento de rejeicoes. Mitigacao parcial: o teste
  `test_enum_lists_match_data_dictionary` faz sanity check no CI.
- **Fail-fast pode mascarar progresso parcial**: se yellow processou
  com sucesso e green falhou, yellow ja esta commitado no Delta mas
  o exit code e nao-zero. Operadores precisam saber disso ao
  investigar.

## Proximos passos sugeridos

- Materializar quarentena (usar `silver_quarantine_table_fqn_for`) e
  preservar o motivo da rejeicao por linha.
- Migrar para leitura incremental (CDF da bronze ou watermark por
  `_ingestion_ts`) quando o volume crescer.
- Usar `lastOperationMetrics` do Delta para metricas exatas
  (inserted/updated/deleted) sem o custo de `count()`.
- Reavaliar a regra `total_amount >= 0` quando ruleset DQ formal
  entrar.
- Adicionar checks de schema drift entre o esperado na silver e o
  observado na bronze.
- Tooling para automatizar a sincronia entre as constantes de DQ e
  os PDFs do TLC (parser ou pelo menos diff regular).
