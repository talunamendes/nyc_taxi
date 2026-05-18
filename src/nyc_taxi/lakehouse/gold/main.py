"""
04 — Camada Gold (Silver -> View de consumo unificada).

Entry point para Databricks `python_wheel` task.

Configuração no job:
    task:
      task_key: gold
      depends_on:
        - task_key: silver
      python_wheel_task:
        package_name: nyc_taxi
        entry_point: gold_layer
        parameters:
          - "--catalog=${var.catalog}"
          - "--environment=${bundle.target}"

Responsabilidades:
- Publicar `gold.vw_taxi_trips`: VIEW que une `silver.yellow_taxi_trips`
  e `silver.green_taxi_trips` e expõe as colunas obrigatórias do
  contrato de consumo:

    > "É necessário garantir que as colunas VendorID, passenger_count,
    >  total_amount, tpep_pickup_datetime e tpep_dropoff_datetime
    >  estejam presentes na camada de consumo."

  Yellow publica `tpep_*` nativo; green publica `lpep_*`. A view aliasa
  os timestamps de green para `tpep_*`, normalizando o nome ao contrato
  de consumo. `taxi_type` literal (yellow | green) é adicionado para
  preservar lineage por linha.

Não-responsabilidades:
- **NÃO materializa fatos pré-agregados**. As perguntas analíticas
  (médias mensal e horária) são respondidas por SQL ad-hoc rodando
  diretamente contra esta view — ver `analysis/perguntas_analiticas.sql`.
  Justificativa formal em [ADR-014](../../../docs/adr/ADR-014-gold-data-model-per-question.md):
  pré-agregação pagaria dividendo apenas em volume/tráfego/governança
  que não existem no escopo atual (~18M linhas, leitor único, sem
  auditoria regulatória). Materializar metricas aqui seria
  over-engineering, contra a diretriz do projeto.

Decisões:
- **VIEW e não TABLE**: a transformação é `UNION ALL` + alias de
  coluna, ambas metadata-only no Spark/Delta. View não duplica
  armazenamento e fica sempre fresca em relação às silvers.
- **`CREATE OR REPLACE VIEW`**: idempotente; redeploy reescreve a
  definição sem `DROP` prévio. Re-execução do job é sempre seguro.
- **Schema explícito no DDL (`CREATE VIEW (col COMMENT '...')`)**:
  o contrato é declarado em SQL, não inferido do `SELECT`. Mudanças
  futuras no schema da view são PR conspícuo.
- **Sem `SET TAGS`**: `ALTER VIEW SET TAGS` não é uniforme entre
  runtimes do Databricks. Metadados de governança ficam no `COMMENT`.
- **Fail-fast**: qualquer erro no `CREATE VIEW` interrompe o job
  (mesma política do silver — gold é camada de contrato).
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


# Colunas obrigatórias do contrato de consumo:
#   "É necessário garantir que as colunas VendorID, passenger_count,
#   total_amount, tpep_pickup_datetime e tpep_dropoff_datetime estejam
#   presentes na camada de consumo. As outras colunas podem ser ignoradas."
#
# A view `vw_taxi_trips` materializa esse contrato unindo yellow + green
# com alias dos timestamps de green (`lpep_*`) para os nomes canônicos
# do contrato (`tpep_*`). `taxi_type` extra (yellow|green) preserva a
# linhagem de origem para quando o consumidor quiser filtrar.
_CONSUMPTION_REQUIRED_COLUMNS: tuple[str, ...] = (
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
)


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
        prog="publish_gold",
        description=(
            "Publish NYC Taxi gold consumption view (vw_taxi_trips) that "
            "exposes the columns required by the consumption contract over yellow + green."
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
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# DDL — view de consumo
# ---------------------------------------------------------------------------


def ensure_trips_consumption_view(cfg: PipelineConfig, spark: Any) -> None:
    """
    View `gold.vw_taxi_trips` — contrato de consumo da camada gold.

    Expoe sobre yellow + green as 5 colunas obrigatorias do contrato
    de consumo (`_CONSUMPTION_REQUIRED_COLUMNS`) com `tpep_*` como nome
    canonico do timestamp (alias de `lpep_*` para green). Adiciona
    `taxi_type` literal para preservar lineage por linha.

    Decisões:
    - **VIEW e nao TABLE**: nenhuma transformacao alem de UNION + alias.
      View nao duplica armazenamento, fica sempre fresca em relacao ao
      silver e o custo de cada `SELECT` e' o full scan que o consumidor
      faria de qualquer forma na silver.
    - **CREATE OR REPLACE VIEW**: idempotente; permite evoluir a
      definicao (ex.: novo taxi_type) sem `DROP` previo.
    - **Sem `SET TAGS`**: Unity Catalog hoje nao suporta `ALTER VIEW
      SET TAGS` em todas as runtimes. Tags ficam declaradas no
      `COMMENT` para auditoria.
    - **Schema explicito no SELECT**: yellow e green tem schemas
      diferentes (ADR-013). A view e' o ponto onde alinhamos para
      o contrato de consumo — qualquer mudanca aqui e' PR conspicuo.
    """
    view_fqn = cfg.gold_trips_view_fqn
    yellow_fqn = cfg.silver_table_fqn_for("yellow")
    green_fqn = cfg.silver_table_fqn_for("green")

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {view_fqn} (
            VendorID              COMMENT 'Codigo do provedor TLC (1,2,6,7).',
            passenger_count       COMMENT 'Numero de passageiros declarado pelo motorista.',
            total_amount          COMMENT 'Valor total cobrado (USD), incluindo taxas e gorjeta.',
            tpep_pickup_datetime  COMMENT 'Inicio da corrida (yellow nativo; green = lpep_pickup_datetime).',
            tpep_dropoff_datetime COMMENT 'Fim da corrida (yellow nativo; green = lpep_dropoff_datetime).',
            taxi_type             COMMENT 'Origem da linha: yellow ou green.'
        )
        COMMENT 'Gold: view de consumo unificada (yellow+green) com as colunas obrigatorias do contrato de consumo. Tags: layer=gold, domain=mobility, fact=trips_unified, taxi_type=all, criticality=tier-2, pii=none.'
        AS
        SELECT
            VendorID,
            passenger_count,
            total_amount,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            'yellow' AS taxi_type
        FROM {yellow_fqn}
        UNION ALL
        SELECT
            VendorID,
            passenger_count,
            total_amount,
            lpep_pickup_datetime  AS tpep_pickup_datetime,
            lpep_dropoff_datetime AS tpep_dropoff_datetime,
            'green' AS taxi_type
        FROM {green_fqn}
        """
    )

    log_with_context(
        logger,
        logging.INFO,
        "Gold consumption view ensured",
        view=view_fqn,
        required_columns=list(_CONSUMPTION_REQUIRED_COLUMNS),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    """
    Entry point chamado pela Databricks python_wheel task.

    Publica `gold.vw_taxi_trips`. Falha aqui interrompe o job — gold e'
    camada de contrato; nao publicar a view induziria consumidores a
    falhar ao tentar consultar.
    """
    args = _parse_args(argv)
    config = PipelineConfig(environment=args.environment, catalog=args.catalog)

    log_with_context(
        logger,
        logging.INFO,
        "Starting publish_gold",
        catalog=config.catalog,
        environment=config.environment,
        target_view=config.gold_trips_view_fqn,
    )

    spark = _get_spark()

    try:
        ensure_trips_consumption_view(config, spark)
    except Exception:
        # Fail-fast: gold e' camada de consumo final. Diferente da bronze
        # (onde a tolerancia parcial e' proposital), aqui qualquer falha
        # mata o job para que consumidores nao leiam estado parcial.
        log_with_context(
            logger,
            logging.ERROR,
            "Gold view publication failed",
            view=config.gold_trips_view_fqn,
        )
        raise

    summary: dict[str, Any] = {
        "objects": {config.gold_trips_view_fqn: {"status": "ok"}},
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
