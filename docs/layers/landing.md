# Landing Layer

## Papel da camada

A camada `landing` baixa os arquivos Parquet do NYC TLC (Yellow e Green Taxi)
do CDN do TLC e persiste em UC Volume com isolamento por tipo de taxi e
particionamento Hive-style:

- `.../<taxi_type>/year=YYYY/month=MM/<taxi_type>_tripdata_YYYY-MM.parquet`
- sidecar `_ingestion_metadata.json` por mes com metadados de ingestao
  (`taxi_type`, URL fonte, tamanho, MD5, timestamps, duracao).

A separacao por subpath (`yellow/`, `green/`) e o que permite o Auto Loader
da bronze ler cada dataset em uma tabela independente (ver ADR-013).

Arquivo de referencia: `src/nyc_taxi/lakehouse/landing/main.py`.

## Implementacao atual

### Entradas de CLI

O entrypoint `ingest_landing` suporta dois modos mutuamente exclusivos:

- Modo explicito:
  - `--target-year`
  - `--target-months` (ex.: `1,2,3`)
- Modo discovery:
  - `--discover`
  - `--discover-from` (formato `YYYY-MM`, obrigatorio)

Argumentos comuns:

- `--taxi-type` (`yellow`, `green` ou `both`; default `both`)
- `--catalog`
- `--environment` (`dev`, `stg`, `prd`)

Se a combinacao for invalida (ex.: `--discover` junto com `--target-year`,
ou nenhum modo escolhido), `_validate_args` falha antes de iniciar
Spark/DBUtils.

### Bootstrap Databricks

`_get_spark_and_dbutils()` cria:

- `SparkSession`
- `DBUtils`

Fora de cluster Databricks, a funcao levanta `RuntimeError`. Para testes
locais o caller deve mockar (vide `tests/test_landing_main.py`).

### Resolucao da janela alvo

`_resolve_taxi_types(args.taxi_type, cfg)` expande `both` para a tupla de
taxis suportados (`yellow`, `green`); valor unico vira tupla de 1 elemento.

Para cada taxi selecionado, `_resolve_target_window(args, taxi_type, cfg, dbutils)`
calcula a lista de `(year, month)`:

- Modo explicito: expande `target_year x target_months` (mesma janela para
  todos os taxis).
- Modo discovery: chama `discover_missing_months(cfg, taxi_type, ...)`.

No discovery, a janela vai de `discover_from` ate `today - cfg.tlc_publication_lag_months`.
Meses ja presentes no subpath do taxi (`cfg.landing_taxi_path(taxi_type)`)
sao removidos da lista final. Cada taxi tem discovery independente.

`main()` achata os resultados em uma lista plana de `(taxi_type, year, month)`
para um unico loop de ingestao.

### Ingestao por (taxi, ano, mes)

Para cada item da lista, `ingest_month(taxi_type, year, month, cfg, dbutils)`:

1. monta URL via `cfg.tlc_url_template.format(taxi_type=..., year=..., month=...)`;
2. monta `partition_path = {cfg.landing_taxi_path(taxi_type)}/year=YYYY/month=MM`;
3. verifica idempotencia (`file_already_ingested`);
4. cria o diretorio (`dbutils.fs.mkdirs`);
5. baixa o arquivo com retry exponencial (`download_with_retry`);
6. calcula MD5 (`compute_md5`);
7. grava `_ingestion_metadata.json` com `taxi_type`, URL fonte, tamanho,
   hash, duracao e identificacao do pipeline.

Falha em um `(taxi, year, month)` NAO interrompe os demais itens.

### Saida e codigos de retorno

`main()` imprime JSON no stdout, sempre com `per_taxi` para correlacao:

- caso normal:
  `{"ingested": X, "skipped": Y, "failed": Z, "per_taxi": {"yellow": {...}, "green": {...}}}`
- janela vazia para todos os taxis:
  `{"ingested": 0, "skipped": 0, "failed": 0, "nothing_to_do": true}`

Retorno:

- `0` quando ao menos um item teve sucesso (ingested ou skipped) OU
  quando a janela total esta vazia (nada a fazer != erro);
- `1` apenas quando TODOS os itens tentados falharam.

## Exemplos de execucao

Yellow + Green, modo explicito (default):

```bash
ingest_landing --target-year=2023 --target-months=1,2,3 \
  --catalog=nyc_taxi_dev --environment=dev
```

Apenas green em backfill:

```bash
ingest_landing --target-year=2023 --target-months=4,5 --taxi-type=green \
  --catalog=nyc_taxi_dev --environment=dev
```

Discovery (ambos os taxis):

```bash
ingest_landing --discover --discover-from=2023-01 \
  --catalog=nyc_taxi_dev --environment=dev
```

## Saida no UC Volume (esquematica)

```
/Volumes/nyc_taxi_dev/landing/nyc_taxi_raw/
  yellow/
    year=2023/month=01/
      yellow_tripdata_2023-01.parquet
      _ingestion_metadata.json
    year=2023/month=02/...
  green/
    year=2023/month=01/
      green_tripdata_2023-01.parquet
      _ingestion_metadata.json
    year=2023/month=02/...
```

## Riscos tecnicos atuais

- Idempotencia por heuristica de tamanho minimo em `file_already_ingested`,
  nao por checksum.
- `discover` considera apenas existencia de particao/arquivo, nao valida
  integridade.
- `ensure_uc_objects()` existe, mas esta comentada no fluxo principal.
- Regra de negocio e IO (HTTP + DBUtils) ainda estao acoplados no mesmo
  modulo.
- Mesma janela explicita aplicada a todos os taxis selecionados; nao ha
  override por taxi sem rodar o job duas vezes.

## Proximos passos sugeridos

- Tornar idempotencia deterministica usando metadado + checksum.
- Adicionar teto de discovery (ex.: `--discover-max-months`).
- Extrair adaptadores de storage/download para facilitar testes.
- Definir estrategia explicita para criacao de objetos UC por ambiente.
- Permitir janela explicita por taxi (ex.: `--target-months-yellow` /
  `--target-months-green`) quando o caso justificar.
