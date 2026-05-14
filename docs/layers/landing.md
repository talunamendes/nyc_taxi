# Landing Layer

## Papel da camada

A camada `landing` baixa os arquivos Parquet de Yellow Taxi no CDN do TLC e
persiste em UC Volume no formato Hive-style:

- `.../year=YYYY/month=MM/<arquivo>.parquet`
- sidecar `_ingestion_metadata.json` com metadados de ingestao.

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

- `--catalog`
- `--environment` (`dev`, `stg`, `prd`)

Se a combinacao for invalida, `_validate_args` falha antes de iniciar Spark/DBUtils.

### Bootstrap Databricks

`_get_spark_and_dbutils()` cria:

- `SparkSession`
- `DBUtils`

Fora de cluster Databricks, a funcao levanta `RuntimeError`.

### Resolucao da janela alvo

- Modo explicito: expande `target_year x target_months`.
- Modo discovery: chama `discover_missing_months()`.

No discovery, a janela vai de `discover_from` ate `today - cfg.tlc_publication_lag_months`.
Meses ja presentes no volume sao removidos da lista final.

### Ingestao por mes

Para cada `(year, month)` selecionado:

1. monta URL via `cfg.tlc_url_template`;
2. verifica idempotencia (`file_already_ingested`);
3. cria particao (`dbutils.fs.mkdirs`);
4. baixa arquivo com retry exponencial (`download_with_retry`);
5. calcula MD5 (`compute_md5`);
6. grava `_ingestion_metadata.json` com tamanho, hash e duracao.

Falha em um mes nao interrompe os demais meses.

### Saida e codigos de retorno

O job imprime JSON no stdout:

- `{"ingested": X, "skipped": Y, "failed": Z}`; ou
- `{"ingested": 0, "skipped": 0, "failed": 0, "nothing_to_do": true}` quando a janela fica vazia.

Retorno:

- `0` para sucesso total ou parcial;
- `1` apenas quando todos os meses tentados falham.

## Exemplo de execucao

Modo explicito:

```bash
ingest_landing --target-year=2023 --target-months=1,2,3 --catalog=nyc_taxi_dev --environment=dev
```

Modo discovery:

```bash
ingest_landing --discover --discover-from=2023-01 --catalog=nyc_taxi_dev --environment=dev
```

## Riscos tecnicos atuais

- Idempotencia por heuristica de tamanho minimo em `file_already_ingested`.
- `discover` considera apenas existencia de particao/arquivo, nao valida integridade.
- `ensure_uc_objects()` existe, mas esta comentada no fluxo principal.
- Regra de negocio e IO (HTTP + DBUtils) ainda estao acoplados no mesmo modulo.

## Proximos passos sugeridos

- Tornar idempotencia deterministica usando metadado + checksum.
- Adicionar teto de discovery (ex.: `--discover-max-months`).
- Extrair adaptadores de storage/download para facilitar testes.
- Definir estrategia explicita para criacao de objetos UC por ambiente.