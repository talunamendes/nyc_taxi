"""
Structured logging para o pipeline.

Princípio: logs como dados. Estruturado em JSON, com campos consistentes,
para que possam ser indexados e correlacionados em Datadog/ELK em produção.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JSONFormatter(logging.Formatter):
    """Formatter que emite logs em JSON estruturado."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Incluir campos extras passados via `extra={...}`
        if hasattr(record, "extra_fields"):
            log_obj.update(record.extra_fields)  # type: ignore[attr-defined]

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Retorna um logger configurado com JSON formatter.

    Args:
        name: nome do logger (geralmente __name__ do módulo)
        level: nível mínimo de log

    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Evitar duplicação de handlers em re-imports
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    **extra: Any,
) -> None:
    """
    Helper para logar com campos extras estruturados.

    Example:
        log_with_context(
            logger, logging.INFO,
            "Bronze ingestion completed",
            rows_ingested=3_500_000,
            duration_seconds=42.5,
            source_file="yellow_tripdata_2023-05.parquet",
        )
    """
    logger.log(level, message, extra={"extra_fields": extra})
