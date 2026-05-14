# Bronze Layer

## Papel da camada

A camada `bronze` converte os arquivos da landing em tabela Delta append-only,
preservando o dado bruto e adicionando colunas de linhagem.

Arquivo de referencia: `src/nyc_taxi/lakehouse/bronze/main.py`.

## Implementacao atual

### Entradas de CLI

O entrypoint `ingest_bronze` recebe apenas:

- `--catalog`
- `--environment` (`dev`, `stg`, `prd`)

Nao existe filtro por ano/mes na bronze. O Auto Loader decide o que e novo.

### Bootstrap Databricks

`_get_spark()` cria `SparkSession`. Esta camada nao usa `DBUtils` diretamente.

### Garantia de tabela

`ensure_bronze_table()` executa `CREATE TABLE IF NOT EXISTS` para
`cfg.bronze_table_fqn` com:

- `delta.autoOptimize.optimizeWrite = true`
- `delta.autoOptimize.autoCompact = true`
- `delta.enableChangeDataFeed = true`
- `delta.columnMapping.mode = name`
- versoes minimas de reader/writer para suportar column mapping
- tags UC (`layer`, `domain`, `criticality`, `pii`)

O schema nao e definido no DDL: ele vem do primeiro write do Auto Loader.

### Ingestao com Auto Loader

`run_bronze_ingestion()` usa `readStream.format("cloudFiles")` com:

- `cloudFiles.format = parquet`
- `cloudFiles.schemaLocation = <schemas_volume>/bronze_yellow_trips`
- `cloudFiles.schemaEvolutionMode = addNewColumns`
- `cloudFiles.inferColumnTypes = true`
- `cloudFiles.includeExistingFiles = true`

Depois do `.load(cfg.landing_volume_path)`, adiciona:

- `_ingestion_ts`
- `_source_file`
- `_source_file_modification_time`
- `_source_year`
- `_source_month`

Write:

- `checkpointLocation = <checkpoints_volume>/bronze_yellow_trips`
- `mergeSchema = true`
- `trigger(availableNow=True)`
- `.toTable(cfg.bronze_table_fqn)`

### Schema evolution (comportamento real)

Com coluna nova no Parquet:

1. Auto Loader atualiza `schemaLocation`;
2. a execucao pode encerrar com `UnknownFieldException`;
3. retry seguinte tende a concluir com schema novo e `mergeSchema=true`.

Por isso o job precisa de `max_retries >= 1` no Workflow.

### Saida e retorno

`main()` imprime JSON com:

- `rows_ingested` (de `lastProgress.numInputRows`)
- `files_processed` (hoje extraido de `sources[0].metrics.numFilesOutstanding`)

Retorna `0` no caminho feliz; excecao nao tratada resulta em falha do job.

## Exemplo de execucao

```bash
ingest_bronze --catalog=nyc_taxi_dev --environment=dev
```

## Riscos tecnicos atuais

- Dependencia operacional de retry para schema evolution.
- Nao existe alerta nativo para drift de schema (coluna nova entra silenciosamente).
- `files_processed` usa metrica de `numFilesOutstanding`, que pode nao representar
  literalmente "arquivos processados".
- Leitura de todo `landing_volume_path` pode misturar datasets se o volume crescer.

## Proximos passos sugeridos

- Revisar metrica de arquivos para refletir "processados" de forma inequivoca.
- Criar alertas para ingestao zerada e mudanca de schema.
- Isolar datasets por subpath quando houver mais de uma fonte.
- Backup/versionamento de `schemaLocation` e `checkpointLocation`.