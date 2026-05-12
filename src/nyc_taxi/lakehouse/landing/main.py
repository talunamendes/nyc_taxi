"""
01 — Ingestão para Landing Zone (UC Volume).

Entry point para Databricks `python_wheel` task.

Configuração no job (Databricks Workflows):
    task:
      task_key: landing
      python_wheel_task:
        package_name: <nome_do_pacote>
        entry_point: landing
        parameters:
          - "--target-year=2023"
          - "--target-months=1,2,3,4,5"

O entry point `source_to_landing` é registrado no pyproject.toml apontando
para a função `main()` deste módulo:

    [project.scripts]
    ingest_landing = "src.nyc_taxi.lakehouse.landing:main"

Responsabilidades:
- Baixar Parquets do NYC TLC (CDN)
- Persistir em UC Volume com particionamento Hive-style year=/month=
- Registrar metadados de ingestão (checksum, tamanho, timestamp)
- Idempotência: se arquivo já existe e checksum bate, pula

Decisões:
- Volume substitui S3 por limitação do Databricks Free Edition
- Particionamento Hive desde a landing facilita Auto Loader downstream
- Checksum MD5 registrado em JSON ao lado para auditoria

Falhas conhecidas:
- TLC pode publicar com atraso (>30d). Ver runbook 001.
- Domínio CDN pode não estar na allowlist do Free Edition. Ver runbook 005.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Protocol, Sequence

import requests  # type: ignore[import-untyped]

from nyc_taxi.core.config import DEFAULT_CONFIG, PipelineConfig
from nyc_taxi.core.logging_utils import get_logger, log_with_context

logger = get_logger(__name__)


# -----------------------------------------------------------------------------
# Protocolos de tipagem para objetos Databricks/pyspark
# -----------------------------------------------------------------------------


class _SparkSessionProtocol(Protocol):
    def sql(self, query: str) -> object: ...


class _DbutilsEntryProtocol(Protocol):
    size: int


class _DbutilsFsProtocol(Protocol):
    def ls(self, path: str) -> list[_DbutilsEntryProtocol]: ...

    def mkdirs(self, path: str) -> object: ...

    def put(self, path: str, contents: str, overwrite: bool = ...) -> object: ...


class _DbutilsProtocol(Protocol):
    fs: _DbutilsFsProtocol


# -----------------------------------------------------------------------------
# Bootstrap de spark / dbutils
# -----------------------------------------------------------------------------
#
# Em notebook Databricks, `spark` e `dbutils` são variáveis pré-injetadas no
# escopo do driver. Em python_wheel task, NÃO são — o entry point é uma
# função Python comum. Precisamos obtê-las explicitamente.


def _get_spark_and_dbutils() -> tuple[_SparkSessionProtocol, _DbutilsProtocol]:
    """
    Obtém spark e dbutils do contexto de execução.

    Funciona em python_wheel task rodando em cluster Databricks (DBR 10.4+).
    Para testes locais sem cluster, importar este módulo falha — o que é
    desejado: o caller deve mockar.

    Returns:
        Tupla (spark, dbutils).

    Raises:
        RuntimeError: se rodando fora de um cluster Databricks.
    """
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "pyspark not available; this entry point must run on a Databricks cluster"
        ) from exc

    spark = SparkSession.builder.getOrCreate()

    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "pyspark.dbutils not available; running outside Databricks cluster?"
        ) from exc

    return spark, DBUtils(spark)


# -----------------------------------------------------------------------------
# CLI parsing
# -----------------------------------------------------------------------------


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """
    Parseia argumentos da linha de comando.

    Substitui `dbutils.widgets.*` do notebook. Defaults vêm de DEFAULT_CONFIG;
    sobrescritos pelos `parameters` definidos no Workflow.
    """
    parser = argparse.ArgumentParser(
        prog="ingest_landing",
        description="Ingest NYC TLC Yellow Taxi parquets to UC Volume landing zone",
    )
    parser.add_argument(
        "--target-year",
        type=int,
        default=DEFAULT_CONFIG.target_year,
        help="Ano alvo (default: %(default)s)",
    )
    parser.add_argument(
        "--target-months",
        type=str,
        default=",".join(map(str, DEFAULT_CONFIG.target_months)),
        help="Meses alvo separados por vírgula (default: %(default)s)",
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


def _parse_months(months_args: str) -> tuple[int, ...]:
    """Converte string 'M1,M2,M3' em tupla de ints."""
    try:
        return tuple(int(m.strip()) for m in months_args.split(",") if m.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid --target-months format: {months_args!r}") from exc


# -----------------------------------------------------------------------------
# Garantir catalog/schema/volume
# -----------------------------------------------------------------------------


def ensure_uc_objects(cfg: PipelineConfig, spark: _SparkSessionProtocol) -> None:
    """Cria catalog, schema e volume se não existirem (idempotente)."""

    print(f"CATALOG = {cfg.catalog}")
    print(f"LANDING SCHEMA = {cfg.catalog}.{cfg.landing_schema}")
    print(f"BRONZE SCHEMA  = {cfg.catalog}.{cfg.bronze_schema}")
    print(f"SILVER SCHEMA  = {cfg.catalog}.{cfg.bronze_schema}")
    print(f"GOLD SCHEMA  = {cfg.catalog}.{cfg.gold_schema}")
    print(f"VOLUME RAW  = {cfg.catalog}.{cfg.landing_schema}.{cfg.landing_volume}")
    print(f"GOLD SCHEMA  = {cfg.catalog}.{cfg.landing_schema}.{cfg.checkpoints_volume}")

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.landing_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.silver_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.gold_schema}")
    # spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.obs_schema}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.landing_schema}.{cfg.landing_volume}"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.landing_schema}.{cfg.checkpoints_volume}"
    )
    # spark.sql(
    #     f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.landing_schema}.{cfg.schemas_volume}"
    # )
    log_with_context(
        logger,
        logging.INFO,
        "UC objects ensured",
        catalog=cfg.catalog,
        environment=cfg.environment,
    )


# -----------------------------------------------------------------------------
# Funções utilitárias
# -----------------------------------------------------------------------------


def compute_md5(path: str, chunk_size: int = 8192) -> str:
    """Calcula MD5 de um arquivo em streaming."""
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


def download_with_retry(url: str, dest_path: str, cfg: PipelineConfig) -> None:
    """
    Download com retry exponencial.

    Raises:
        RuntimeError: se download falha após todas as tentativas.
    """
    last_exception: Exception | None = None

    for attempt in range(cfg.download_retry_attempts):
        try:
            with requests.get(
                url, stream=True, timeout=cfg.download_timeout_seconds
            ) as response:
                response.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1_048_576):  # 1MB
                        if chunk:
                            f.write(chunk)

            log_with_context(
                logger,
                logging.INFO,
                "Download successful",
                url=url,
                dest=dest_path,
                attempt=attempt + 1,
            )
            return

        except (requests.RequestException, IOError) as e:
            last_exception = e
            wait = 2 ** attempt
            log_with_context(
                logger,
                logging.WARNING,
                "Download failed, retrying",
                url=url,
                attempt=attempt + 1,
                wait_seconds=wait,
                error=str(e),
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Download failed after {cfg.download_retry_attempts} attempts: {last_exception}"
    )


def file_already_ingested(
    volume_path: str, dbutils: _DbutilsProtocol, expected_size_min: int = 10_000_000
) -> bool:
    """
    Verifica se arquivo já foi ingerido (idempotência).

    Heurística: existe e tem tamanho mínimo plausível (>10MB).
    Em produção, verificar checksum contra metadado.
    """
    try:
        result = dbutils.fs.ls(volume_path)
        return any(f.size >= expected_size_min for f in result)
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Loop de ingestão
# -----------------------------------------------------------------------------


def ingest_month(
    year: int, month: int, cfg: PipelineConfig, dbutils: _DbutilsProtocol
) -> dict[str, object]:
    """
    Ingere um mês de Yellow Taxi.

    Returns:
        Dict com metadados: status, file_path, size_bytes, md5, etc.
    """
    url = cfg.tlc_url_template.format(year=year, month=month)
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    partition_path = f"{cfg.landing_volume_path}/year={year}/month={month:02d}"
    dest_path = f"{partition_path}/{file_name}"
    metadata_path = f"{partition_path}/_ingestion_metadata.json"

    # Idempotência
    if file_already_ingested(partition_path, dbutils):
        log_with_context(
            logger,
            logging.INFO,
            "Skipping already-ingested file",
            year=year,
            month=month,
        )
        return {"status": "skipped", "year": year, "month": month}

    # Criar diretório
    dbutils.fs.mkdirs(partition_path)

    # Download
    started_at = datetime.now(timezone.utc)
    download_with_retry(url, dest_path, cfg)

    # Metadados
    file_stat = dbutils.fs.ls(dest_path)[0]
    md5 = compute_md5(dest_path)

    metadata = {
        "source_url": url,
        "file_name": file_name,
        "size_bytes": file_stat.size,
        "md5": md5,
        "ingested_at": started_at.isoformat(),
        "ingestion_duration_seconds": (datetime.now(timezone.utc) - started_at).total_seconds(),
        "pipeline_name": cfg.pipeline_name,
        "pipeline_environment": cfg.environment,
    }

    # Persistir metadados ao lado
    dbutils.fs.put(metadata_path, json.dumps(metadata, indent=2), overwrite=True)

    log_with_context(
        logger,
        logging.INFO,
        "Month ingested successfully",
        year=year,
        month=month,
        size_mb=file_stat.size / 1_048_576,
        md5=md5,
    )

    return {"status": "ingested", **metadata}


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    """
    Entry point chamado pela Databricks python_wheel task.

    Returns:
        0 se ao menos um mês foi processado (ingested ou skipped).
        1 se todos os meses falharam.
    """
    args = _parse_args(argv)

    months = _parse_months(args.target_months)

    config = PipelineConfig(
        environment=args.environment,
        catalog=args.catalog,
        target_year=args.target_year,
        target_months=months,
    )

    log_with_context(
        logger,
        logging.INFO,
        "Starting ingest_landing",
        target_year=config.target_year,
        target_months=list(config.target_months),
        catalog=config.catalog,
        environment=config.environment,
    )

    # Bootstrap spark/dbutils (substitui injeção automática do notebook)
    spark, dbutils = _get_spark_and_dbutils()

    # Garantir objetos UC
    # ensure_uc_objects(config, spark)

    # Loop de ingestão por mês
    results: list[dict[str, object]] = []
    for month in config.target_months:
        try:
            result = ingest_month(config.target_year, month, config, dbutils)
            results.append(result)
        except Exception as e:  # noqa: BLE001
            log_with_context(
                logger,
                logging.ERROR,
                "Failed to ingest month",
                year=config.target_year,
                month=month,
                error=str(e),
            )
            results.append(
                {
                    "status": "failed",
                    "year": config.target_year,
                    "month": month,
                    "error": str(e),
                }
            )
            # Não quebra o loop — continua tentando outros meses

    # Sumário
    ingested = sum(1 for r in results if r["status"] == "ingested")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] == "failed")

    log_with_context(
        logger,
        logging.INFO,
        "Landing ingestion summary",
        ingested=ingested,
        skipped=skipped,
        failed=failed,
        total=len(results),
    )

    # Emite sumário em JSON no stdout — substitui dbutils.notebook.exit().
    # Útil para inspecionar nos logs do job.
    print(json.dumps({"ingested": ingested, "skipped": skipped, "failed": failed}))

    # Política de exit code: falha o job apenas se NENHUM mês teve sucesso.
    # Em python_wheel task, exit code != 0 sinaliza falha à task.
    if failed > 0 and (ingested + skipped) == 0:
        log_with_context(
            logger,
            logging.ERROR,
            "All months failed to ingest",
            results=results,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())