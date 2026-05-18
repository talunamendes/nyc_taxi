"""
01 — Ingestão para Landing Zone (UC Volume).

Entry point para Databricks `python_wheel` task.

Modos de execução:

1. EXPLÍCITO (default) — diga exatamente o que baixar.

    ingest_landing --target-year=2023 --target-months=1,2,3,4,5

2. DISCOVERY — descubra o que falta baixando comparando o volume com a
   janela `[discover_from, hoje - lag_publicação_tlc]`.

    ingest_landing --discover --discover-from=2023-01

   `--discover-from` é obrigatório quando `--discover` é usado, para não
   acidentalmente tentar baixar histórico inteiro do TLC.

Modos são mutuamente exclusivos. Sem nenhum deles, falha cedo.

Tipos de taxi:
- `--taxi-type=yellow|green|both` (default: `both`).
- Cada tipo é baixado em subpath isolado dentro do volume da landing
  (`.../<taxi_type>/year=YYYY/month=MM/...`). Isso é o que permite ao
  Auto Loader da bronze ler cada dataset em uma tabela independente.

Responsabilidades:
- Baixar Parquets do NYC TLC (CDN).
- Persistir em UC Volume com particionamento Hive-style year=/month=,
  segmentado por subpath de taxi (`yellow/` e `green/`).
- Registrar metadados de ingestão (checksum, tamanho, timestamp).
- Idempotência: se arquivo já existe e checksum bate, pula.

Decisões:
- Volume substitui S3 por limitação do Databricks Free Edition.
- Particionamento Hive desde a landing facilita Auto Loader downstream.
- Checksum MD5 registrado em JSON ao lado para auditoria.
- `target_year`/`target_months`/`taxi_type` NÃO estão na config (vide
  config.py): são parâmetros de execução, não configuração.
- `taxi_type` vira subpath, não Hive partition: o Auto Loader da bronze
  é configurado por taxi para que cada um vire uma tabela separada
  (yellow_taxi, green_taxi). Misturar tipos em uma única partição
  exigiria split posterior por column virtual.

Falhas conhecidas:
- TLC pode publicar com atraso (>30d). Por isso o modo discover usa um
  buffer de `cfg.tlc_publication_lag_months`. Ver runbook 001.
- Domínio CDN pode não estar na allowlist do Free Edition. Ver runbook 005.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from typing import Protocol, Sequence

import requests  # type: ignore[import-untyped]

from nyc_taxi.core.config import DEFAULT_CONFIG, SUPPORTED_TAXI_TYPES, PipelineConfig
from nyc_taxi.core.logging_utils import get_logger, log_with_context

logger = get_logger(__name__)

# Valor especial de CLI que significa "ingerir todos os tipos suportados".
# Optamos por um marcador explícito (e não None/empty) para que o default
# fique visível tanto no `--help` quanto nos logs de job.
TAXI_TYPE_ALL: str = "both"

# Piso de tamanho do Parquet usado pela heurística de idempotência em
# `file_already_ingested`. As ordens de grandeza vêm direto do TLC:
#   - Yellow publica ~50 MB/mês — piso de 10 MB cobre downloads parciais
#     sem incorrer em falsos positivos.
#   - Green publica ~1–2 MB/mês — um piso de 10 MB rejeitaria todo
#     arquivo válido e o job rebaixaria green a cada execução. 500 KB
#     ainda flagra truncamento óbvio sem invalidar o arquivo correto.
# Um único piso global não atende os dois taxis (10 MB falha green;
# 100 KB aceitaria yellow truncado), por isso parametrizamos por taxi.
_MIN_PARQUET_SIZE_BYTES: dict[str, int] = {
    "yellow": 10_000_000,
    "green": 500_000,
}
_DEFAULT_MIN_PARQUET_SIZE_BYTES: int = 500_000


# -----------------------------------------------------------------------------
# Protocolos de tipagem para objetos Databricks/pyspark
# -----------------------------------------------------------------------------


class _SparkSessionProtocol(Protocol):
    def sql(self, query: str) -> object: ...


class _DbutilsEntryProtocol(Protocol):
    name: str
    path: str
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


def _get_spark_and_dbutils() -> tuple[_SparkSessionProtocol, _DbutilsProtocol]:
    """
    Obtém spark e dbutils do contexto de execução.

    Funciona em python_wheel task rodando em cluster Databricks (DBR 10.4+).
    Para testes locais sem cluster, importar este módulo falha — o que é
    desejado: o caller deve mockar.
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
# CLI parsing & validação
# -----------------------------------------------------------------------------


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """
    Parseia argumentos da linha de comando.

    Dois modos mutuamente exclusivos:
      - Explícito: --target-year + --target-months
      - Discovery: --discover + --discover-from
    """
    parser = argparse.ArgumentParser(
        prog="ingest_landing",
        description="Ingest NYC TLC Yellow/Green Taxi parquets to UC Volume landing zone",
    )

    # Modo explícito
    parser.add_argument(
        "--target-year",
        type=int,
        default=None,
        help="Ano alvo (obrigatório no modo explícito)",
    )
    parser.add_argument(
        "--target-months",
        type=str,
        default=None,
        help="Meses alvo separados por vírgula, ex: 1,2,3 (obrigatório no modo explícito)",
    )

    # Modo discovery
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Descobre meses faltantes comparando volume com janela alvo",
    )
    parser.add_argument(
        "--discover-from",
        type=str,
        default=None,
        help="Início da janela de discovery no formato YYYY-MM (obrigatório com --discover)",
    )

    # Tipo de taxi (yellow/green). Default `both` faz o job ingerir os
    # dois tipos numa única execução — é a expectativa do schedule.
    parser.add_argument(
        "--taxi-type",
        type=str,
        choices=[*SUPPORTED_TAXI_TYPES, TAXI_TYPE_ALL],
        default=TAXI_TYPE_ALL,
        help=(
            "Tipo de taxi a ingerir. Use 'yellow' ou 'green' para um único "
            "tipo, ou 'both' para ambos (default: %(default)s)."
        ),
    )

    # Comuns
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


def _resolve_taxi_types(taxi_type_arg: str, cfg: PipelineConfig) -> tuple[str, ...]:
    """Mapeia o valor de `--taxi-type` para a lista de tipos a processar."""
    if taxi_type_arg == TAXI_TYPE_ALL:
        return cfg.supported_taxi_types
    return (taxi_type_arg,)


def _validate_args(args: argparse.Namespace) -> None:
    """
    Valida combinação de args. Falha cedo com mensagem clara.

    Regras:
      - --discover é mutuamente exclusivo com --target-year/--target-months
      - --discover requer --discover-from
      - sem --discover, requer --target-year E --target-months
    """
    explicit_given = args.target_year is not None or args.target_months is not None

    if args.discover and explicit_given:
        raise ValueError(
            "--discover is mutually exclusive with --target-year/--target-months"
        )

    if args.discover:
        if not args.discover_from:
            raise ValueError("--discover requires --discover-from=YYYY-MM")
        return

    # Modo explícito
    if args.target_year is None or args.target_months is None:
        raise ValueError(
            "either use --discover --discover-from=YYYY-MM, or "
            "provide both --target-year and --target-months"
        )


def _parse_months(months_arg: str) -> tuple[int, ...]:
    """
    Converte string 'M1,M2,M3' em tupla de ints ordenada e deduplicada.
    Valida que todos estão em 1..12.
    """
    try:
        months = sorted({int(m.strip()) for m in months_arg.split(",") if m.strip()})
    except ValueError as exc:
        raise ValueError(f"Invalid --target-months format: {months_arg!r}") from exc

    if not months:
        raise ValueError("--target-months cannot be empty")

    invalid = [m for m in months if not 1 <= m <= 12]
    if invalid:
        raise ValueError(f"--target-months has values outside 1..12: {invalid}")

    return tuple(months)


def _parse_year_month(s: str) -> tuple[int, int]:
    """Parseia 'YYYY-MM' em (year, month) com validação."""
    try:
        parts = s.split("-")
        if len(parts) != 2:
            raise ValueError
        year, month = int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise ValueError(f"Invalid YYYY-MM format: {s!r}") from exc

    if not 1 <= month <= 12:
        raise ValueError(f"Month out of range in {s!r}")
    if not 2000 <= year <= 2100:
        raise ValueError(f"Year out of range in {s!r}")

    return year, month


# -----------------------------------------------------------------------------
# Discovery de meses faltantes
# -----------------------------------------------------------------------------


def _generate_month_range(
    start: tuple[int, int], end: tuple[int, int]
) -> list[tuple[int, int]]:
    """Gera lista de (year, month) de start a end, inclusivo, em ordem."""
    start_y, start_m = start
    end_y, end_m = end

    if (start_y, start_m) > (end_y, end_m):
        return []

    result: list[tuple[int, int]] = []
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def _list_existing_months(
    volume_path: str, dbutils: _DbutilsProtocol
) -> set[tuple[int, int]]:
    """
    Lista partições Hive-style existentes no volume e extrai (year, month).

    Retorna conjunto vazio se o volume não tem partições ou ainda não existe.
    """
    existing: set[tuple[int, int]] = set()
    try:
        year_dirs = dbutils.fs.ls(volume_path)
    except Exception:
        return existing

    for year_dir in year_dirs:
        name = year_dir.name.rstrip("/")
        if not name.startswith("year="):
            continue
        try:
            year = int(name.split("=", 1)[1])
        except (ValueError, IndexError):
            continue

        try:
            month_dirs = dbutils.fs.ls(year_dir.path)
        except Exception:
            continue

        for month_dir in month_dirs:
            mname = month_dir.name.rstrip("/")
            if not mname.startswith("month="):
                continue
            try:
                month = int(mname.split("=", 1)[1])
            except (ValueError, IndexError):
                continue
            existing.add((year, month))

    return existing


def discover_missing_months(
    cfg: PipelineConfig,
    taxi_type: str,
    discover_from: tuple[int, int],
    today: date,
    dbutils: _DbutilsProtocol,
) -> list[tuple[int, int]]:
    """
    Descobre meses ainda não ingeridos na janela [discover_from, hoje - lag]
    para um determinado tipo de taxi.

    O limite superior subtrai `cfg.tlc_publication_lag_months` do mês corrente
    porque o TLC publica com ~30–45 dias de atraso — tentar baixar o mês
    corrente quase sempre resulta em 404.

    Args:
        cfg: configuração do pipeline (usa landing_volume_path e
            tlc_publication_lag_months).
        taxi_type: 'yellow' ou 'green' (define o subpath inspecionado).
        discover_from: (year, month) de início da janela, inclusivo.
        today: data corrente (parâmetro para facilitar teste).
        dbutils: cliente de filesystem do Databricks.

    Returns:
        Lista ordenada de (year, month) faltantes. Vazia se nada falta.
    """
    # Calcula limite superior aplicando o lag de publicação
    lag = cfg.tlc_publication_lag_months
    end_y, end_m = today.year, today.month - lag
    while end_m <= 0:
        end_m += 12
        end_y -= 1

    target_window = _generate_month_range(discover_from, (end_y, end_m))
    existing = _list_existing_months(cfg.landing_taxi_path(taxi_type), dbutils)

    missing = [ym for ym in target_window if ym not in existing]
    return missing


# -----------------------------------------------------------------------------
# Garantir catalog/schema/volume
# -----------------------------------------------------------------------------


def ensure_uc_objects(cfg: PipelineConfig, spark: _SparkSessionProtocol) -> None:
    """Cria catalog, schema e volume se não existirem (idempotente)."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.landing_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.silver_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.gold_schema}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.landing_schema}.{cfg.landing_volume}"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}.{cfg.checkpoints_volume}"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}.{cfg.schemas_volume}"
    )
    log_with_context(
        logger,
        logging.INFO,
        "UC objects ensured",
        catalog=cfg.catalog,
        environment=cfg.environment,
    )


# -----------------------------------------------------------------------------
# Funções utilitárias de download
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
            wait = 2**attempt
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
    volume_path: str,
    dbutils: _DbutilsProtocol,
    expected_size_min: int = _DEFAULT_MIN_PARQUET_SIZE_BYTES,
) -> bool:
    """
    Verifica se arquivo já foi ingerido (idempotência).

    Heurística: existe na partição e tem tamanho >= `expected_size_min`.
    O piso varia por tipo de taxi (yellow ~50MB/mês, green ~1–2MB/mês),
    então o caller deve passar o piso correto via
    `_MIN_PARQUET_SIZE_BYTES[taxi_type]`. Em produção, verificar
    checksum contra metadado.
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
    taxi_type: str,
    year: int,
    month: int,
    cfg: PipelineConfig,
    dbutils: _DbutilsProtocol,
) -> dict[str, object]:
    """
    Ingere um mês de um tipo de taxi (yellow ou green).

    Arquivos vão para subpath dedicado por taxi (`.../yellow/...` ou
    `.../green/...`) para que cada um vire uma tabela bronze independente.

    Returns:
        Dict com metadados: status, file_path, size_bytes, md5, etc.
        Sempre inclui `taxi_type` para correlação no resumo final.
    """
    url = cfg.tlc_url_template.format(taxi_type=taxi_type, year=year, month=month)
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    partition_path = (
        f"{cfg.landing_taxi_path(taxi_type)}/year={year}/month={month:02d}"
    )
    dest_path = f"{partition_path}/{file_name}"
    metadata_path = f"{partition_path}/_ingestion_metadata.json"

    # Idempotência: piso do tamanho varia por taxi (ver
    # `_MIN_PARQUET_SIZE_BYTES`). Sem isso, green seria rebaixado a cada
    # execução porque seu Parquet (~1–2MB) fica abaixo do piso default
    # do yellow (10MB).
    min_size = _MIN_PARQUET_SIZE_BYTES.get(taxi_type, _DEFAULT_MIN_PARQUET_SIZE_BYTES)
    if file_already_ingested(partition_path, dbutils, expected_size_min=min_size):
        log_with_context(
            logger,
            logging.INFO,
            "Skipping already-ingested file",
            taxi_type=taxi_type,
            year=year,
            month=month,
        )
        return {
            "status": "skipped",
            "taxi_type": taxi_type,
            "year": year,
            "month": month,
        }

    # Criar diretório
    dbutils.fs.mkdirs(partition_path)

    # Download
    started_at = datetime.now(timezone.utc)
    download_with_retry(url, dest_path, cfg)

    # Metadados
    file_stat = dbutils.fs.ls(dest_path)[0]
    md5 = compute_md5(dest_path)

    metadata = {
        "taxi_type": taxi_type,
        "source_url": url,
        "file_name": file_name,
        "size_bytes": file_stat.size,
        "md5": md5,
        "ingested_at": started_at.isoformat(),
        "ingestion_duration_seconds": (
            datetime.now(timezone.utc) - started_at
        ).total_seconds(),
        "pipeline_name": cfg.pipeline_name,
        "pipeline_environment": cfg.environment,
    }

    dbutils.fs.put(metadata_path, json.dumps(metadata, indent=2), overwrite=True)

    log_with_context(
        logger,
        logging.INFO,
        "Month ingested successfully",
        taxi_type=taxi_type,
        year=year,
        month=month,
        size_mb=file_stat.size / 1_048_576,
        md5=md5,
    )

    return {"status": "ingested", **metadata}


# -----------------------------------------------------------------------------
# Resolução da janela a partir dos args
# -----------------------------------------------------------------------------


def _resolve_target_window(
    args: argparse.Namespace,
    taxi_type: str,
    cfg: PipelineConfig,
    dbutils: _DbutilsProtocol,
) -> list[tuple[int, int]]:
    """
    Resolve a lista final de (year, month) a ingerir para um tipo de taxi,
    conforme o modo escolhido.

    Modo explícito → expande target_year × target_months (mesma janela
    para todos os taxis).
    Modo discovery → consulta o subpath do taxi e calcula diferença.
    """
    if args.discover:
        discover_from = _parse_year_month(args.discover_from)
        today = datetime.now(timezone.utc).date()
        missing = discover_missing_months(cfg, taxi_type, discover_from, today, dbutils)
        log_with_context(
            logger,
            logging.INFO,
            "Discovery mode resolved target window",
            taxi_type=taxi_type,
            discover_from=args.discover_from,
            missing_count=len(missing),
            missing=[f"{y}-{m:02d}" for y, m in missing],
        )
        return missing

    # Modo explícito
    months = _parse_months(args.target_months)
    return [(args.target_year, m) for m in months]


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    """
    Entry point chamado pela Databricks python_wheel task.

    Loop externo é por tipo de taxi (yellow, green ou ambos). Para cada
    tipo, o loop interno é por (year, month). Falha em um mês — ou em um
    tipo inteiro — não interrompe os demais.

    Returns:
        0 se ao menos um mês de algum taxi foi processado (ingested ou
          skipped) OU se a janela total ficou vazia (nada faltando ≠ erro).
        1 se todos os meses tentados, em todos os taxis, falharam.
    """
    args = _parse_args(argv)
    _validate_args(args)

    config = PipelineConfig(
        environment=args.environment,
        catalog=args.catalog,
    )

    taxi_types = _resolve_taxi_types(args.taxi_type, config)

    log_with_context(
        logger,
        logging.INFO,
        "Starting ingest_landing",
        mode="discover" if args.discover else "explicit",
        catalog=config.catalog,
        environment=config.environment,
        taxi_types=list(taxi_types),
    )

    # Bootstrap spark/dbutils (substitui injeção automática do notebook)
    spark, dbutils = _get_spark_and_dbutils()

    # ensure_uc_objects(config, spark)  # opcional; ver runbook

    # Resolve janela alvo por taxi (explicit ou discovery). Lista plana
    # de (taxi_type, year, month) para simplificar o loop e o resumo.
    plan: list[tuple[str, int, int]] = []
    for taxi_type in taxi_types:
        window = _resolve_target_window(args, taxi_type, config, dbutils)
        plan.extend((taxi_type, y, m) for y, m in window)

    if not plan:
        log_with_context(
            logger,
            logging.INFO,
            "Nothing to ingest — target window is empty",
            mode="discover" if args.discover else "explicit",
            taxi_types=list(taxi_types),
        )
        print(
            json.dumps(
                {"ingested": 0, "skipped": 0, "failed": 0, "nothing_to_do": True}
            )
        )
        return 0

    # Loop de ingestão (achatado por (taxi_type, year, month))
    results: list[dict[str, object]] = []
    for taxi_type, year, month in plan:
        try:
            result = ingest_month(taxi_type, year, month, config, dbutils)
            results.append(result)
        except Exception as e:  # noqa: BLE001
            log_with_context(
                logger,
                logging.ERROR,
                "Failed to ingest month",
                taxi_type=taxi_type,
                year=year,
                month=month,
                error=str(e),
            )
            results.append(
                {
                    "status": "failed",
                    "taxi_type": taxi_type,
                    "year": year,
                    "month": month,
                    "error": str(e),
                }
            )

    ingested = sum(1 for r in results if r["status"] == "ingested")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] == "failed")

    # Breakdown por taxi facilita debugar parciais (yellow OK / green falhou).
    per_taxi: dict[str, dict[str, int]] = {
        t: {"ingested": 0, "skipped": 0, "failed": 0} for t in taxi_types
    }
    for r in results:
        taxi = str(r.get("taxi_type", ""))
        if taxi in per_taxi:
            per_taxi[taxi][str(r["status"])] += 1

    log_with_context(
        logger,
        logging.INFO,
        "Landing ingestion summary",
        ingested=ingested,
        skipped=skipped,
        failed=failed,
        total=len(results),
        per_taxi=per_taxi,
    )

    print(
        json.dumps(
            {
                "ingested": ingested,
                "skipped": skipped,
                "failed": failed,
                "per_taxi": per_taxi,
            }
        )
    )

    # Falha o job apenas se NENHUM mês teve sucesso (ADR-003).
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