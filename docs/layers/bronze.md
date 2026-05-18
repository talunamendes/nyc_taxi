# Bronze Layer

## Papel da camada

A camada `bronze` converte os arquivos Parquet da landing em **uma tabela
Delta append-only por tipo de taxi** (`yellow_taxi`, `green_taxi`),
preservando o dado bruto e adicionando colunas de linhagem.

Cada bronze e auto-suficiente: schemaLocation, checkpointLocation, schema
e tabela Delta sao independentes por taxi. Ver ADR-013 para a justificativa
de manter datasets separados ao longo do pipeline.

Arquivo de referencia: `src/nyc_taxi/lakehouse/bronze/main.py`.

## Implementacao atual

### Entradas de CLI

O entrypoint `ingest_bronze` recebe:

- `--taxi-type` (`yellow`, `green` ou `both`; default `both`)
- `--catalog`
- `--environment` (`dev`, `stg`, `prd`)

Nao existe filtro por ano/mes na bronze. O Auto Loader decide o que e novo
com base no checkpoint de cada taxi.

### Bootstrap Databricks

`_get_spark()` cria `SparkSession`. Esta camada nao usa `DBUtils`
diretamente. Fora de cluster Databricks, levanta `RuntimeError`.

### Resolucao da lista de taxis

`_resolve_taxi_types(args.taxi_type, cfg)` expande `both` em
`cfg.supported_taxi_types`; valor unico vira tupla de 1 elemento. O loop
principal itera pela tupla, em ordem.

### Garantia de tabela (por taxi)

`ensure_bronze_table(cfg, taxi_type, spark)` executa, por taxi:

1. `CREATE TABLE IF NOT EXISTS cfg.bronze_table_fqn_for(taxi_type)` com:
   - `delta.autoOptimize.optimizeWrite = true`
   - `delta.autoOptimize.autoCompact = true`
   - `delta.enableChangeDataFeed = true`
   - `delta.columnMapping.mode = name`
   - `delta.minReaderVersion = 2`, `delta.minWriterVersion = 5`
   - `delta.feature.timestampNtz = supported`
   - `COMMENT` referenciando o tipo de taxi
2. `ALTER TABLE ... SET TAGS` com `layer`, `domain`, `taxi_type`,
   `criticality`, `pii`.

O schema NAO e definido no DDL: vem do primeiro write do Auto Loader.

### Ingestao com Auto Loader (por taxi)

`run_bronze_ingestion(cfg, taxi_type, spark)` usa
`readStream.format("cloudFiles")` com:

- `cloudFiles.format = parquet`
- `cloudFiles.schemaLocation = cfg.bronze_schema_location(taxi_type)`
  (ex.: `<schemas_volume>/bronze_yellow_taxi`)
- `cloudFiles.schemaEvolutionMode = addNewColumns`
- `cloudFiles.inferColumnTypes = true`
- `cloudFiles.includeExistingFiles = true`

A leitura aponta para o **subpath do taxi** na landing
(`cfg.landing_taxi_path(taxi_type)`), evitando misturar datasets.

Apos o `.load(...)`, sao adicionadas as seguintes colunas de linhagem:

- `_ingestion_ts` (current_timestamp)
- `_source_file` (de `_metadata.file_path`)
- `_source_file_modification_time` (de `_metadata.file_modification_time`)
- `_taxi_type` (lit do taxi_type da execucao; util para auditoria
  cross-table sem precisar inspecionar o nome da tabela)
- `_source_year`, `_source_month` (regex sobre `_metadata.file_path`)

Write:

- `checkpointLocation = cfg.bronze_checkpoint_location(taxi_type)`
  (ex.: `<checkpoints_volume>/bronze_yellow_taxi`)
- `mergeSchema = true`
- `trigger(availableNow=True)`
- `.toTable(cfg.bronze_table_fqn_for(taxi_type))`

Metricas por taxi vem de `query.lastProgress`:

- `rows_ingested` = `numInputRows`
- `files_processed` = `sources[0].metrics.numFilesOutstanding`

### Schema evolution (comportamento real)

Quando um Parquet novo traz coluna nao conhecida:

1. Auto Loader atualiza `schemaLocation` (registro do schema inferido);
2. a execucao encerra com `UnknownFieldException`;
3. o retry seguinte enxerga schema novo e prossegue com `mergeSchema=true`.

Por isso o job precisa de `max_retries >= 1` no Workflow (configurado como
`max_retries: 2`).

### Politica de falha por taxi

O loop principal trata cada taxi em `try/except` isolado:

- yellow falhando NAO interrompe green, e vice-versa;
- o resultado por taxi entra no JSON com `status: "ok"` ou
  `status: "failed"` + `error`.

### Saida e codigos de retorno

`main()` imprime JSON com agregado e breakdown:

```json
{
  "rows_ingested": <soma>,
  "files_processed": <soma>,
  "per_taxi": {
    "yellow": {"status": "ok", "rows_ingested": ..., "files_processed": ...},
    "green":  {"status": "ok", "rows_ingested": ..., "files_processed": ...}
  }
}
```

Retorno:

- `0` quando ao menos um taxi teve sucesso (falha parcial e tolerada);
- `1` apenas quando TODOS os taxis falharam.

## Exemplos de execucao

Yellow + Green (default):

```bash
ingest_bronze --catalog=nyc_taxi_dev --environment=dev
```

Backfill apenas de green:

```bash
ingest_bronze --taxi-type=green --catalog=nyc_taxi_dev --environment=dev
```

## Tabelas resultantes

```
nyc_taxi_dev.bronze.yellow_taxi
nyc_taxi_dev.bronze.green_taxi
```

## Riscos tecnicos atuais

- Dependencia operacional de retry para schema evolution.
- Nao existe alerta nativo para drift de schema (coluna nova entra
  silenciosamente).
- `files_processed` usa metrica de `numFilesOutstanding`, que pode nao
  representar literalmente "arquivos processados".
- Schemas/checkpoints por taxi sao isolados, mas estao no mesmo volume;
  perda do volume afeta os dois taxis simultaneamente.

## Proximos passos sugeridos

- Revisar metrica de arquivos para refletir "processados" de forma
  inequivoca.
- Criar alertas para ingestao zerada e mudanca de schema, por taxi.
- Backup/versionamento de `schemaLocation` e `checkpointLocation` (por
  taxi).
- Avaliar Delta Live Tables para gestao declarativa quando o conjunto
  de bronzes crescer.
