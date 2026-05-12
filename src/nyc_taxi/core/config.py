"""
Configurações centralizadas do pipeline.

Princípio: nenhum valor mágico hardcoded nos notebooks. Tudo vem daqui ou de
parâmetros do job. Permite trocar entre dev/stg/prd alterando apenas env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class PipelineConfig:
    """
    Configuração imutável do pipeline NYC Yellow Taxi.

    Valores default servem para Databricks Free Edition. Em produção, sobrescrever
    via env vars ou parâmetros do job.
    """

    # === Identificação ===
    pipeline_name: str = "nyc_yellow_taxi"
    environment: Literal["dev", "stg", "prd"] = "dev"

    # === Catálogo Unity ===
    catalog: str = "nyc_taxi_dev"
    landing_schema: str = "landing"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    #obs_schema: str = "observability"

    # === Volumes (UC Volume substitui S3 no Free Edition) ===
    landing_volume: str = "nyc_taxi_raw"
    checkpoints_volume: str = "_checkpoints"
    schemas_volume: str = "_schemas"

    # === Tabelas ===
    bronze_table: str = "yellow_trips"
    silver_table: str = "yellow_trips"
    silver_quarantine_table: str = "yellow_trips_quarantine"
    gold_monthly_table: str = "fct_yellow_trips_monthly"
    gold_hourly_may_table: str = "fct_yellow_trips_hourly_may2023"

    # === Janela de processamento ===
    target_year: int = 2023
    target_months: tuple[int, ...] = field(default_factory=lambda: (1,2,3,4,5,))

    # === Fonte externa ===
    tlc_url_template: str = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    )
    download_timeout_seconds: int = 300
    download_retry_attempts: int = 3

    # === DQ thresholds ===
    dq_min_rows_per_month: int = 1_500_000
    dq_max_rows_per_month: int = 5_000_000
    dq_max_null_pct_critical: float = 0.001
    dq_max_rejection_rate: float = 0.10  # se >10% rejeitado, falha pipeline

    # === Performance / file sizing ===
    target_file_size_bytes: int = 134_217_728  # 128MB
    data_skipping_indexed_cols: int = 8

    # === SLA ===
    sla_freshness_hours: int = 24

    # === Audit ===
    cost_center: str = "mobility-analytics"
    owner_team: str = "data-platform"

    # === Computed properties ===
    @property
    def landing_volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.landing_volume}"

    @property
    def checkpoints_volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.checkpoints_volume}"

    @property
    def schemas_volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.schemas_volume}"

    @property
    def bronze_table_fqn(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_table}"

    @property
    def silver_table_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_table}"

    @property
    def silver_quarantine_table_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_quarantine_table}"

    @property
    def gold_monthly_table_fqn(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.{self.gold_monthly_table}"

    @property
    def gold_hourly_may_table_fqn(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.{self.gold_hourly_may_table}"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Constrói config sobrescrevendo defaults com env vars relevantes."""
        return cls(
            environment=os.getenv("ENV", "dev"),  # type: ignore[arg-type]
            catalog=os.getenv("NYC_TAXI_CATALOG", "nyc_taxi_dev"),
        )


# Singleton conveniente para importação direta
DEFAULT_CONFIG = PipelineConfig()
