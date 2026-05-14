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
      # Retry essencial para schema evolution (ver docstring de run_bronze_ingestion).
      max_retries: 2
      min_retry_interval_millis: 30000

Responsabilidades:
- Detectar novos Parquets na landing zone via Auto Loader (cloudFiles).
- Carregar em tabela Delta append-only.
- Adicionar colunas de linhagem de ingestão (timestamp, arquivo de origem,
  ano/mês derivados do path Hive-style da landing).
- Idempotência via checkpoint do Auto Loader: arquivos já processados não
  são relidos em execuções subsequentes.
- Schema evolution permissiva: o schema vem do próprio Parquet (via Auto
  Loader), novas colunas são adicionadas automaticamente.

Decisões:
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

from nyc_taxi.core.config import DEFAULT_CONFIG, PipelineConfig
from nyc_taxi.core.logging_utils import get_logger, log_with_context

logger = get_logger(__name__)


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
        description="Ingest NYC TLC Parquet files from landing volume into bronze Delta table",
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
    return parser.parse_args(argv)


def ensure_bronze_table(cfg: PipelineConfig, spark: Any) -> None:
    """
    Cria a tabela bronze (idempotente) com properties de governance e
    auto-otimização. Sem lista de colunas — schema vem do write.

    Por que sem schema declarado?
        O schema correto da Yellow Taxi é exatamente o do Parquet do TLC.
        Declarar a lista de colunas aqui duplicaria essa informação e abriria
        espaço para desincronização (TLC publica coluna nova, DDL fica
        desatualizada). A propriedade do Delta de criar tabela "empty schema"
        com properties permite que o primeiro write popule o schema a partir
        do Parquet, mantendo properties desde o início — coisas que NÃO
        seriam inferidas se omitíssemos o CREATE.

    Por que sem CLUSTER BY / PARTITIONED BY?
        Vide ADR-006. Bronze é append-only e lida sequencialmente pela
        silver — não há padrão de filtro seletivo que se beneficie de
        organização física. `optimizeWrite`/`autoCompact` resolvem small
        files sem o custo de rewrite de clustering.
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.bronze_table_fqn}
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
        COMMENT 'Bronze layer: NYC Yellow Taxi raw ingestion (append-only).'
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {cfg.bronze_table_fqn}
        SET TAGS (
            'layer' = 'bronze',
            'domain' = 'mobility',
            'criticality' = 'tier-2',
            'pii' = 'none'
        )
        """
    )

    log_with_context(logger, logging.INFO, "Bronze table ensured", table=cfg.bronze_table_fqn)


def run_bronze_ingestion(cfg: PipelineConfig, spark: Any) -> dict[str, int]:
    """
    Roda Auto Loader em modo batch (availableNow) e retorna métricas.

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

    schema_location = f"{cfg.schemas_volume_path}/bronze_yellow_trips"
    checkpoint_location = f"{cfg.checkpoints_volume_path}/bronze_yellow_trips"

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
        .load(cfg.landing_volume_path)
        # Linhagem de ingestão. `_metadata` é coluna virtual exposta pelo
        # Auto Loader; não exige read explícito do FS.
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_modification_time", F.col("_metadata.file_modification_time"))
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
        .toTable(cfg.bronze_table_fqn)
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

    log_with_context(logger, logging.INFO, "Bronze ingestion completed", **metrics)
    return metrics


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point chamado pela Databricks python_wheel task."""
    args = _parse_args(argv)
    config = PipelineConfig(environment=args.environment, catalog=args.catalog)

    log_with_context(
        logger,
        logging.INFO,
        "Starting ingest_bronze",
        catalog=config.catalog,
        environment=config.environment,
        source_path=config.landing_volume_path,
        table=config.bronze_table_fqn,
    )

    spark = _get_spark()
    ensure_bronze_table(config, spark)
    metrics = run_bronze_ingestion(config, spark)

    # Sumário JSON no stdout — visível nos logs do job e consumível por
    # tasks downstream via `dbutils.jobs.taskValues`.
    print(json.dumps(metrics))
    return 0


if __name__ == "__main__":
    sys.exit(main())