"""
02 — Ingestão Bronze (Landing Volume -> Delta).

Entry point para Databricks `python_wheel` task.

Configuração no job:
    task:
      task_key: bronze
      python_wheel_task:
        package_name: <pacote>
        entry_point: ingest_bronze
        parameters:
          - "--environment=dev"
          - "--taxi-type=both"  # yellow | green | both (default: both)
      # Retry essencial para schema evolution (ver docstring de run_bronze_ingestion).
      max_retries: 2
      min_retry_interval_millis: 30000

Responsabilidades:
- Detectar novos Parquets na landing zone via Auto Loader (cloudFiles).
- Carregar em uma tabela Delta append-only por tipo de taxi
  (`yellow_taxi`, `green_taxi`).
- Adicionar colunas de linhagem de ingestão (timestamp, arquivo de origem,
  ano/mês derivados do path Hive-style da landing, taxi_type).
- Idempotência via checkpoint do Auto Loader: arquivos já processados não
  são relidos em execuções subsequentes (um checkpoint por taxi).
- Schema evolution permissiva: o schema vem do próprio Parquet (via Auto
  Loader), novas colunas são adicionadas automaticamente.

Decisões:
- Uma tabela por tipo de taxi (yellow_taxi, green_taxi) ao invés de uma
  tabela unificada com coluna `taxi_type`: os schemas do TLC divergem
  (ex.: yellow tem `tpep_*` e green tem `lpep_*`). Forçar union exigiria
  reconciliação de colunas que não pertence à bronze (cuja regra é
  preservar o bruto).
- CREATE TABLE sem lista de colunas: o schema é inteiramente inferido do
  Parquet pelo primeiro write. Evita duplicação entre DDL e arquivo de
  origem, alinhado com `schemaEvolutionMode=addNewColumns`. Properties
  são declaradas explicitamente — elas NÃO são inferidas e precisam estar
  configuradas desde a criação.
- Sem clustering/partitioning na bronze (vide ADR-006): bronze é
  append-only, lida sequencialmente pela silver. Clustering aqui seria
  trabalho perdido. Mantemos só `optimizeWrite`/`autoCompact` para
  resolver small files baratos. Clustering entra a partir da silver.
- Schema evolution habilitada em duas camadas — Auto Loader
  (`addNewColumns`) e Delta (`mergeSchema=true`) — para que mudanças no
  schema do Parquet NÃO quebrem o pipeline permanentemente.
- `delta.columnMapping.mode = 'name'` desde o início: necessário para
  rename/drop futuros sem rewrite. Bumpa min reader/writer versions, mas
  o trade-off já é assumido.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Sequence

from nyc_taxi.core.config import DEFAULT_CONFIG, SUPPORTED_TAXI_TYPES, PipelineConfig
from nyc_taxi.core.logging_utils import get_logger, log_with_context

logger = get_logger(__name__)

# Mesma convenção da landing: `both` é o default e expande para todos
# os taxis suportados.
TAXI_TYPE_ALL: str = "both"


def _get_spark() -> Any:
    """Obtém SparkSession do contexto Databricks (wheel task)."""
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "pyspark not available; this entry point must run on a Databricks cluster"
        ) from exc

    return SparkSession.builder.getOrCreate()


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parseia argumentos da CLI."""
    parser = argparse.ArgumentParser(
        prog="ingest_bronze",
        description=(
            "Ingest NYC TLC Parquet files from landing volume into bronze "
            "Delta tables (one per taxi type: yellow_taxi, green_taxi)"
        ),
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=DEFAULT_CONFIG.catalog,
        help="Catalog Unity (default: %(default)s)",
    )
    parser.add_argument(
        "--environment",
        type=str,
        choices=["dev", "stg", "prd"],
        default=DEFAULT_CONFIG.environment,
        help="Environment do pipeline (default: %(default)s)",
    )
    parser.add_argument(
        "--taxi-type",
        type=str,
        choices=[*SUPPORTED_TAXI_TYPES, TAXI_TYPE_ALL],
        default=TAXI_TYPE_ALL,
        help=(
            "Tipo de taxi a ingerir na bronze. 'yellow' ou 'green' processa "
            "apenas uma tabela; 'both' (default) processa as duas em sequência."
        ),
    )
    return parser.parse_args(argv)


def _resolve_taxi_types(taxi_type_arg: str, cfg: PipelineConfig) -> tuple[str, ...]:
    """Mapeia o valor de `--taxi-type` para a lista de tipos a processar."""
    if taxi_type_arg == TAXI_TYPE_ALL:
        return cfg.supported_taxi_types
    return (taxi_type_arg,)


def ensure_bronze_table(cfg: PipelineConfig, taxi_type: str, spark: Any) -> None:
    """
    Cria a tabela bronze de um tipo de taxi (idempotente) com properties
    de governance e auto-otimização. Sem lista de colunas — schema vem
    do write do Auto Loader.

    Args:
        cfg: configuração do pipeline.
        taxi_type: 'yellow' ou 'green'. Determina o nome da tabela
            (`yellow_taxi`, `green_taxi`) e o COMMENT.
        spark: SparkSession.

    Por que sem schema declarado?
        O schema correto de cada taxi é exatamente o do Parquet do TLC,
        e os schemas de yellow e green divergem (ex.: yellow tem `tpep_*`,
        green tem `lpep_*`). Declarar a lista de colunas aqui duplicaria
        essa informação e abriria espaço para desincronização (TLC publica
        coluna nova, DDL fica desatualizada).

    Por que sem CLUSTER BY / PARTITIONED BY?
        Vide ADR-006. Bronze é append-only e lida sequencialmente pela
        silver — não há padrão de filtro seletivo que se beneficie de
        organização física. `optimizeWrite`/`autoCompact` resolvem small
        files sem o custo de rewrite de clustering.
    """
    table_fqn = cfg.bronze_table_fqn_for(taxi_type)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn}
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableChangeDataFeed' = 'true',
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5',
            'delta.feature.timestampNtz' = 'supported'
        )
        COMMENT 'Bronze layer: NYC {taxi_type.capitalize()} Taxi raw ingestion (append-only).'
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {table_fqn}
        SET TAGS (
            'layer' = 'bronze',
            'domain' = 'mobility',
            'taxi_type' = '{taxi_type}',
            'criticality' = 'tier-2',
            'pii' = 'none'
        )
        """
    )

    log_with_context(
        logger,
        logging.INFO,
        "Bronze table ensured",
        taxi_type=taxi_type,
        table=table_fqn,
    )


def run_bronze_ingestion(
    cfg: PipelineConfig, taxi_type: str, spark: Any
) -> dict[str, int]:
    """
    Roda Auto Loader em modo batch (availableNow) para um tipo de taxi
    e retorna métricas. Cada taxi tem schemaLocation e checkpointLocation
    próprios — re-execuções de yellow não afetam green e vice-versa.

    Idempotência: o checkpoint do Auto Loader registra quais arquivos já
    foram processados — re-execuções só pegam o que é novo.

    Schema evolution (comportamento sob mudança de schema):
        Quando um Parquet novo traz coluna que a bronze não conhece, o
        Auto Loader em `schemaEvolutionMode=addNewColumns`:
            1. Detecta a coluna nova.
            2. Atualiza o `schemaLocation` (registro do schema inferido).
            3. **Termina o stream com `UnknownFieldException`.**

        Isso é intencional do Databricks: força um restart "limpo" do
        stream com o schema novo. Por isso o job DEVE ter `max_retries >= 1`
        no Workflow — a segunda execução já enxerga o schema atualizado,
        adiciona a coluna à tabela via `mergeSchema=true` e prossegue.

        Registros antigos ficam NULL na coluna nova (default do Delta).
        Nenhum rewrite é necessário.

    Returns:
        Dict com métricas extraídas do `lastProgress` do streaming query.
    """
    try:
        from pyspark.sql import functions as F  # type: ignore[import-not-found]  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "pyspark.sql.functions not available; running outside Databricks cluster?"
        ) from exc

    schema_location = cfg.bronze_schema_location(taxi_type)
    checkpoint_location = cfg.bronze_checkpoint_location(taxi_type)
    source_path = cfg.landing_taxi_path(taxi_type)
    table_fqn = cfg.bronze_table_fqn_for(taxi_type)

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        # `addNewColumns`: colunas inéditas no Parquet são adicionadas ao
        # schema gerenciado pelo Auto Loader. Combinado com `mergeSchema=true`
        # no writeStream abaixo, a tabela Delta também evolui.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Tipos inferidos do Parquet: respeita o que o TLC publica de fato.
        # Se inferíssemos como STRING (default do Auto Loader), perderíamos
        # tipagem útil já presente no arquivo.
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        # Subpath por taxi — cada Auto Loader vê apenas o seu dataset.
        .load(source_path)
        # Linhagem de ingestão. `_metadata` é coluna virtual exposta pelo
        # Auto Loader; não exige read explícito do FS.
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
        # `taxi_type` é constante por execução, mas materializar facilita
        # auditoria/queries cross-table no futuro.
        .withColumn("_taxi_type", F.lit(taxi_type))
        .withColumn(
            "_source_year",
            F.regexp_extract(F.col("_metadata.file_path"), r"year=(\d{4})", 1).cast("int"),
        )
        .withColumn(
            "_source_month",
            F.regexp_extract(F.col("_metadata.file_path"), r"month=(\d{2})", 1).cast("int"),
        )
    )

    query = (
        stream.writeStream.option("checkpointLocation", checkpoint_location)
        # `mergeSchema=true`: aceita schema mais largo (novas colunas) sem
        # erro. Junto com `addNewColumns` no Auto Loader, fecha o ciclo de
        # schema evolution sem intervenção manual.
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(table_fqn)
    )
    query.awaitTermination()

    # Métricas vêm do `lastProgress` do query — mais barato e mais preciso
    # que `count()` antes/depois (que custa O(N) e pode dar valor errado
    # com writes concorrentes).
    progress = query.lastProgress or {}
    metrics = {
        "rows_ingested": int(progress.get("numInputRows", 0)),
        "files_processed": int(
            progress.get("sources", [{}])[0].get("metrics", {}).get("numFilesOutstanding", 0)
        ),
    }

    log_with_context(
        logger,
        logging.INFO,
        "Bronze ingestion completed",
        taxi_type=taxi_type,
        table=table_fqn,
        **metrics,
    )
    return metrics


def main(argv: Sequence[str] | None = None) -> int:
    """
    Entry point chamado pela Databricks python_wheel task.

    Processa um ou ambos os tipos de taxi em sequência. Falha em um
    tipo NÃO interrompe o outro — para que green possa concluir mesmo
    que yellow tenha disparado UnknownFieldException de schema evolution.
    """
    args = _parse_args(argv)
    config = PipelineConfig(environment=args.environment, catalog=args.catalog)
    taxi_types = _resolve_taxi_types(args.taxi_type, config)

    log_with_context(
        logger,
        logging.INFO,
        "Starting ingest_bronze",
        catalog=config.catalog,
        environment=config.environment,
        taxi_types=list(taxi_types),
    )

    spark = _get_spark()

    per_taxi: dict[str, dict[str, Any]] = {}
    failures: list[str] = []

    for taxi_type in taxi_types:
        try:
            ensure_bronze_table(config, taxi_type, spark)
            metrics = run_bronze_ingestion(config, taxi_type, spark)
            per_taxi[taxi_type] = {"status": "ok", **metrics}
        except Exception as exc:  # noqa: BLE001
            # Logar e seguir: queremos que green ainda rode mesmo que
            # yellow falhe (e vice-versa). O exit code reflete o agregado.
            log_with_context(
                logger,
                logging.ERROR,
                "Bronze ingestion failed for taxi type",
                taxi_type=taxi_type,
                error=str(exc),
            )
            per_taxi[taxi_type] = {"status": "failed", "error": str(exc)}
            failures.append(taxi_type)

    summary: dict[str, Any] = {
        "rows_ingested": sum(
            int(v.get("rows_ingested", 0)) for v in per_taxi.values()
        ),
        "files_processed": sum(
            int(v.get("files_processed", 0)) for v in per_taxi.values()
        ),
        "per_taxi": per_taxi,
    }

    # Sumário JSON no stdout — visível nos logs do job e consumível por
    # tasks downstream via `dbutils.jobs.taskValues`.
    print(json.dumps(summary))

    if failures:
        # Reraise estilo "fail fast" só quando TODOS os taxis falharam;
        # falha parcial mantém exit 0 alinhado à política da landing.
        if len(failures) == len(taxi_types):
            log_with_context(
                logger,
                logging.ERROR,
                "All taxi types failed bronze ingestion",
                failures=failures,
            )
            return 1
        log_with_context(
            logger,
            logging.WARNING,
            "Bronze ingestion completed with partial failures",
            failures=failures,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())