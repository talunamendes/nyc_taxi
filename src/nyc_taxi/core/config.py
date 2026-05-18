"""
Configurações centralizadas do pipeline.

Princípio: configuração é o que fica estável entre execuções (catalog,
schemas, URL template, thresholds de DQ). Parâmetros que mudam a cada
execução (janela de ingestão, ano, meses, tipo de taxi) NÃO são
configuração — vêm via CLI do job. Por isso `target_year`,
`target_months` e `taxi_type` não estão aqui.

Permite trocar entre dev/stg/prd alterando apenas env vars ou parâmetros
do job, sem editar código.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

# Tipos de taxi do TLC suportados pelo pipeline.
# O CDN expõe os Parquets como `<taxi_type>_tripdata_YYYY-MM.parquet`, então
# acrescentar um novo tipo (ex.: `fhv`) significa apenas estender essa tupla
# — não há código por-tipo espalhado pelas camadas.
TaxiType = Literal["yellow", "green"]
SUPPORTED_TAXI_TYPES: tuple[TaxiType, ...] = ("yellow", "green")


@dataclass(frozen=True)
class PipelineConfig:
    """
    Configuração imutável do pipeline NYC Taxi (Yellow + Green).

    Valores default servem para Databricks Free Edition. Em produção, sobrescrever
    via env vars ou parâmetros do job.
    """

    # === Identificação ===
    pipeline_name: str = "nyc_taxi"
    environment: Literal["dev", "stg", "prd"] = "dev"

    # === Catálogo Unity ===
    catalog: str = "nyc_taxi_dev"
    landing_schema: str = "landing"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    # obs_schema: str = "observability"

    # === Volumes (UC Volume substitui S3 no Free Edition) ===
    landing_volume: str = "nyc_taxi_raw"
    checkpoints_volume: str = "_checkpoints"
    schemas_volume: str = "_schemas"

    # === Tabelas ===
    # Cada tipo de taxi vira uma tabela bronze independente (yellow_taxi,
    # green_taxi). O padrão `{taxi_type}_taxi` mantém o naming consistente
    # caso novos tipos sejam adicionados.
    bronze_table_pattern: str = "{taxi_type}_taxi"
    # Silver mantém UMA tabela por tipo de taxi (yellow_taxi_trips,
    # green_taxi_trips) preservando o schema nativo de cada fonte. Ver
    # ADR-013 (escolha do data model da silver).
    silver_table_pattern: str = "{taxi_type}_taxi_trips"
    # Quarentena é simétrica à silver: uma tabela por tipo de taxi,
    # preservando o schema nativo da origem (yellow tem `airport_fee`,
    # green tem `trip_type`, etc.). Misturar quarentenas em uma tabela
    # única traria o mesmo problema de schema interseccional descrito
    # no ADR-013.
    silver_quarantine_table_pattern: str = "{taxi_type}_taxi_trips_quarantine"
    # Gold: view unica de consumo. Decisao formalizada no ADR-014.
    #
    # A gold expoe uma unica VIEW (`vw_taxi_trips`) que une yellow + green
    # e padroniza os timestamps publicados pelo TLC: green usa `lpep_*` na
    # silver; a view aliasa para `tpep_*` para uniformizar o contrato de
    # consumo. As perguntas analiticas (Pergunta A e B) sao respondidas
    # por SQL ad-hoc rodando contra essa view —
    # ver `analysis/perguntas_analiticas.sql`.
    #
    # Por que view e nao fato pre-agregado? A pre-agregacao paga dividendo
    # quando ha (a) volume que torna `GROUP BY` ao vivo caro, (b) trafico
    # de consumo repetitivo, ou (c) governanca/auditoria que exige
    # snapshot imutavel da metrica. Nenhuma das tres condicoes existe
    # no escopo atual (~18M linhas, leitor unico, sem auditoria
    # regulatoria). Materializar metricas aqui seria over-engineering,
    # contra a diretriz do projeto. Volumes/trafico maiores devem
    # promover novos fatos via ADR — ver ADR-014, secao "Quando essa
    # decisao deve ser revisitada".
    gold_trips_view: str = "vw_taxi_trips"

    # === Fonte externa ===
    # O CDN do TLC publica arquivos como `<taxi_type>_tripdata_YYYY-MM.parquet`.
    # Mantemos `{taxi_type}` como placeholder para o caller decidir.
    tlc_url_template: str = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    )
    supported_taxi_types: tuple[TaxiType, ...] = field(
        default_factory=lambda: SUPPORTED_TAXI_TYPES
    )
    download_timeout_seconds: int = 300
    download_retry_attempts: int = 3

    # === Atraso esperado da publicação do TLC ===
    # TLC publica dados com ~30–45 dias de atraso. O modo --discover usa
    # essa margem para evitar tentativas de baixar meses ainda inexistentes.
    tlc_publication_lag_months: int = 2

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
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.checkpoints_volume}"

    @property
    def schemas_volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.schemas_volume}"

    def landing_taxi_path(self, taxi_type: str) -> str:
        """
        Subpath isolado por tipo de taxi dentro do volume da landing.

        Isolar por subpath é o que permite o Auto Loader da bronze ler
        cada dataset separadamente, mantendo schemaLocation e checkpoint
        independentes por tabela (yellow_taxi vs green_taxi).
        """
        return f"{self.landing_volume_path}/{taxi_type}"

    def bronze_table_name(self, taxi_type: str) -> str:
        return self.bronze_table_pattern.format(taxi_type=taxi_type)

    def bronze_table_fqn_for(self, taxi_type: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_table_name(taxi_type)}"

    def bronze_schema_location(self, taxi_type: str) -> str:
        """Diretório do schemaLocation do Auto Loader para esta bronze."""
        return f"{self.schemas_volume_path}/bronze_{self.bronze_table_name(taxi_type)}"

    def bronze_checkpoint_location(self, taxi_type: str) -> str:
        """Diretório do checkpoint do Auto Loader para esta bronze."""
        return f"{self.checkpoints_volume_path}/bronze_{self.bronze_table_name(taxi_type)}"

    @property
    def bronze_table_fqn(self) -> str:
        """
        Compatibilidade com camadas downstream (gold) que hoje só
        consomem yellow. Quando forem refatoradas para multi-taxi,
        passar a usar `bronze_table_fqn_for(taxi_type)` diretamente.
        """
        return self.bronze_table_fqn_for("yellow")

    def silver_table_name(self, taxi_type: str) -> str:
        return self.silver_table_pattern.format(taxi_type=taxi_type)

    def silver_table_fqn_for(self, taxi_type: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_table_name(taxi_type)}"

    @property
    def silver_table_fqn(self) -> str:
        """
        Compatibilidade com gold (ainda yellow-only). Aponta para
        `yellow_taxi_trips`. Quando a gold for refatorada, ler via
        `silver_table_fqn_for(taxi_type)` por escolha explícita.
        """
        return self.silver_table_fqn_for("yellow")

    def silver_quarantine_table_name(self, taxi_type: str) -> str:
        return self.silver_quarantine_table_pattern.format(taxi_type=taxi_type)

    def silver_quarantine_table_fqn_for(self, taxi_type: str) -> str:
        """
        FQN da tabela de quarentena do taxi. Uma quarentena por tipo
        (yellow_taxi_trips_quarantine, green_taxi_trips_quarantine),
        simétrica ao modelo da silver descrito no ADR-013.
        """
        return (
            f"{self.catalog}.{self.silver_schema}."
            f"{self.silver_quarantine_table_name(taxi_type)}"
        )

    @property
    def silver_quarantine_table_fqn(self) -> str:
        """Compat: quarentena do yellow (default histórico)."""
        return self.silver_quarantine_table_fqn_for("yellow")

    @property
    def gold_trips_view_fqn(self) -> str:
        """FQN da view de consumo cross-taxi com as colunas obrigatorias do contrato."""
        return f"{self.catalog}.{self.gold_schema}.{self.gold_trips_view}"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Constrói config sobrescrevendo defaults com env vars relevantes."""
        return cls(
            environment=os.getenv("ENV", "dev"),  # type: ignore[arg-type]
            catalog=os.getenv("NYC_TAXI_CATALOG", "nyc_taxi_dev"),
        )


# Singleton conveniente para importação direta
DEFAULT_CONFIG = PipelineConfig()