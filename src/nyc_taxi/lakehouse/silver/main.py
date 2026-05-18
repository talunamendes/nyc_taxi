"""
03 — Transformação Silver (Bronze -> Delta conformado).

Entry point para Databricks `python_wheel` task.

Configuração no job:
    task:
      task_key: silver
      depends_on:
        - task_key: bronze
      python_wheel_task:
        package_name: <pacote>
        entry_point: transform_silver
        parameters:
          - "--environment=dev"
          - "--taxi-type=both"  # yellow | green | both (default: both)

Responsabilidades:
- Ler a bronze de cada tipo de taxi (yellow_taxi, green_taxi).
- Materializar **uma tabela silver POR TIPO de taxi** (ADR-013):
  yellow_taxi_trips, green_taxi_trips. Cada tabela preserva o
  schema nativo da fonte (yellow mantém `tpep_*`, green mantém
  `lpep_*`).
- Projetar **apenas** o conjunto fixo de colunas do TLC (Trip Record —
  mesmo contrato vigente jan–mai/2023 nos Parquets públicos),
  conferido nos dicionários `docs/nyc/data_dictionary_trip_records_*.pdf`,
  mais colunas específicas do pipeline (`pickup_date`, `_bronze_ingestion_ts`,
  `_silver_processed_ts`). Colunas extras só na bronze (ex.: TLC publicou
  campo novo ou metadados de ingestão como `_source_file`) **não** seguem.
- Aplicar limpeza, validação e deduplicação inline ancoradas nos
  data dictionaries do TLC (`docs/nyc/data_dictionary_*.pdf`).
- MERGE INTO idempotente **sem schema evolution**: o schema físico é
  fechado no contrato 2023; evoluções na bronze não alteram colunas Delta.

Decisões:
- **CREATE TABLE com schema explícito** (diferente da bronze): a silver
  é camada de contrato. Declaramos nome + tipo de cada coluna no DDL
  para que a tabela exista já com o schema certo desde o primeiro run,
  sem depender do primeiro write para fechar a definição física.
  `_SILVER_COLUMN_TYPES` é a fonte única de verdade dos tipos.
- **Liquid Clustering multi-coluna** (ADR-012): `ensure_yellow_silver_table` /
  `ensure_green_silver_table` declaram no `CREATE TABLE` chaves típicas
  de prune para analítica temporal + zonas TLC (`pickup_date`, `PULocationID`).
- **Sem schema evolution** no MERGE (`mergeSchema` omitido/`false`; sem
  `withSchemaEvolution()`). Novas colunas que apareçam na bronze
  permanecem só ali — a silver mantém o contrato fechado.
- **Projeção explícita da bronze**: `build_silver_dataframe` faz
  `select(*colunas_TLC, _ingestion_ts)` antes de qualquer transformação.
  Garante que metadados de ingestão (`_source_file`, etc.) e colunas
  futuras só na bronze nunca cheguem no MERGE.
- **DQ inline com drop**, sem quarantine: o escopo atual demanda
  solução simples.
  Registros inválidos são removidos antes do MERGE. As regras vêm dos
  data dictionaries oficiais do TLC (yellow e green) — sem ad-hoc.
- **Falha em qualquer taxi interrompe o job** (exit != 0). Silver é
  camada de contrato: publicação parcial induziria gold/consumidores
  a verem um snapshot inconsistente. Diferente da política do bronze,
  onde a tolerância é proposital (cada bronze é independente).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Iterable, Sequence

from nyc_taxi.core.config import (
    DEFAULT_CONFIG,
    SUPPORTED_TAXI_TYPES,
    PipelineConfig,
)
from nyc_taxi.core.logging_utils import get_logger, log_with_context

logger = get_logger(__name__)

# Mesma convenção das camadas anteriores: `both` é o default e expande
# para todos os taxis suportados.
TAXI_TYPE_ALL: str = "both"

# Nomes nativos das colunas de pickup/dropoff por tipo de taxi. Yellow
# publica `tpep_*`, green publica `lpep_*`. A silver preserva esses
# nomes (ADR-013), então este map é a única fonte de verdade.
_PICKUP_COLS: dict[str, tuple[str, str]] = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green": ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
}

# Coluna derivada do timestamp nativo de pickup (data local). Também faz
# parte das chaves de Liquid Clustering junto com `PULocationID`.
_CLUSTER_COL: str = "pickup_date"

# Chaves declaradas por tipo no `CLUSTER BY (...)`. São colunas físicas já
# presentes no Trip Record TLC + lineage (pickup_date vem da transformação).
_YELLOW_SILVER_CLUSTER_COLS: tuple[str, ...] = ("pickup_date", "PULocationID")
_GREEN_SILVER_CLUSTER_COLS: tuple[str, ...] = ("pickup_date", "PULocationID")

# Coluna de ingestão criada apenas na bronze; na silver aparece renomeada
# para preservar lineage sem depender de metadados bruto (`_source_file`, etc.).
_BRONZE_RAW_INGESTION_COL: str = "_ingestion_ts"


# Contrato físico fechado: colunas definidas nos data dictionaries oficiais
# TLC (Trip Record Data Dictionary — PDFs em
# docs/nyc/data_dictionary_trip_records_{yellow,green}.pdf), mesmo conjunto
# dos Parquets públicos TLC jan–mai/2023. Qualquer campo novo só na bronze.
_SILVER_TLC_COLUMNS: dict[str, tuple[str, ...]] = {
    "yellow": (
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ),
    "green": (
        "VendorID",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "store_and_fwd_flag",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "trip_type",
        "congestion_surcharge",
    ),
}


def _silver_physical_column_names(taxi_type: str) -> tuple[str, ...]:
    """Ordem física gravada na tabela Delta (Trip Record + lineage silver)."""
    if taxi_type not in _SILVER_TLC_COLUMNS:
        raise ValueError(
            f"Unsupported taxi_type for silver: {taxi_type!r}. "
            f"Expected one of {sorted(_SILVER_TLC_COLUMNS)}."
        )
    tlc_cols = _SILVER_TLC_COLUMNS[taxi_type]
    return (
        *tlc_cols,
        "_bronze_ingestion_ts",
        _CLUSTER_COL,
        "_silver_processed_ts",
    )


# Tipos DDL declarados na tabela silver. Fonte única de verdade — usada
# tanto pelo `CREATE TABLE` em `ensure_*_silver_table` quanto pelo `cast`
# final em `build_silver_dataframe`, garantindo que o DataFrame entregue
# ao MERGE casa exatamente com o schema da tabela (sem `mergeSchema`).
#
# Decisões de tipo:
# - IDs e contagens (VendorID, RatecodeID, PULocationID, DOLocationID,
#   payment_type, trip_type, passenger_count): BIGINT — o TLC publica em
#   int64 no Parquet; usar BIGINT evita overflow e casa com a inferência
#   da bronze (Auto Loader em `inferColumnTypes`).
# - Métricas e valores monetários: DOUBLE — TLC publica nesse precisão.
# - `store_and_fwd_flag`: STRING ("Y"/"N").
# - Timestamps TLC: TIMESTAMP_NTZ — o Auto Loader infere assim para os
#   Parquets do TLC (sem timezone). Casa com a property `delta.feature.
#   timestampNtz = supported` já declarada.
# - Lineage timestamps (`_bronze_ingestion_ts`, `_silver_processed_ts`):
#   TIMESTAMP — vêm de `current_timestamp()` (com TZ da sessão Spark).
# - `pickup_date`: DATE — chave do Liquid Clustering (ADR-012).
_SILVER_COLUMN_TYPES: dict[str, str] = {
    # Chave de negócio
    "VendorID": "BIGINT",
    # Timestamps TLC (yellow e green divergem nos nomes; tipo é o mesmo)
    "tpep_pickup_datetime": "TIMESTAMP_NTZ",
    "tpep_dropoff_datetime": "TIMESTAMP_NTZ",
    "lpep_pickup_datetime": "TIMESTAMP_NTZ",
    "lpep_dropoff_datetime": "TIMESTAMP_NTZ",
    # Dimensões da corrida
    "passenger_count": "BIGINT",
    "trip_distance": "DOUBLE",
    "RatecodeID": "BIGINT",
    "store_and_fwd_flag": "STRING",
    "PULocationID": "BIGINT",
    "DOLocationID": "BIGINT",
    "payment_type": "BIGINT",
    # Valores monetários
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
    # Exclusivos por tipo
    "airport_fee": "DOUBLE",  # yellow-only
    "ehail_fee": "DOUBLE",    # green-only
    "trip_type": "BIGINT",    # green-only
    # Lineage silver
    "_bronze_ingestion_ts": "TIMESTAMP",
    "pickup_date": "DATE",
    "_silver_processed_ts": "TIMESTAMP",
}


def _format_cluster_by(columns: tuple[str, ...]) -> str:
    """Lista separada por vírgulas para `CLUSTER BY (...)`."""
    return ", ".join(columns)


def _require_bronze_trip_record_columns(columns: Iterable[str], taxi_type: str) -> None:
    """Falha cedo quando a bronze não traz todas as colunas do contrato TLC."""
    present = frozenset(columns)
    missing = [c for c in _SILVER_TLC_COLUMNS[taxi_type] if c not in present]
    if missing:
        raise ValueError(
            f"Bronze is missing TLC trip-record column(s) for {taxi_type!r}: {missing}. "
            "Repair bronze or align source files with the TLC dictionary."
        )


def _require_bronze_ingestion_column(columns: Iterable[str]) -> None:
    if _BRONZE_RAW_INGESTION_COL not in columns:
        raise ValueError(
            f"Bronze is missing '{_BRONZE_RAW_INGESTION_COL}'. "
            "Silver expects bronze lineage from ingest_bronze."
        )


# ---------------------------------------------------------------------------
# Regras de DQ (limpeza + validação)
#
# As constantes abaixo vêm dos data dictionaries oficiais do TLC em
# `docs/nyc/data_dictionary_trip_records_{yellow,green}.pdf`. Mudou no TLC?
# Atualizar aqui é a única alteração de código necessária — o filtro
# referencia essas listas.
# ---------------------------------------------------------------------------

# União dos VendorIDs válidos. Yellow publica {1, 2, 6, 7}; green publica
# {1, 2, 6}. Usamos a união pra que o filtro seja simétrico entre tabelas.
# Um VendorID=7 num registro green seria bronze corrompida, mas a frequência
# esperada é zero — não vale ramificar a regra por taxi só por isso.
_VALID_VENDOR_IDS: tuple[int, ...] = (1, 2, 6, 7)
_VALID_RATECODE_IDS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 99)
_VALID_PAYMENT_TYPES: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6)
_VALID_STORE_FWD_FLAGS: tuple[str, ...] = ("Y", "N")
_VALID_TRIP_TYPES: tuple[int, ...] = (1, 2)  # green-only (street-hail | dispatch)

# Limites sane (conservadores) para flagrar bronze corrompida sem virar
# fraud detector. Aim é descartar garbage óbvio; modelagem de outliers
# vai na gold/feature store.
_MAX_TRIP_DISTANCE_MILES: int = 200  # NYC + arredores; > 200mi é garbage
_MAX_PASSENGER_COUNT: int = 9  # vans/limusines do TLC vão até ~6, 9 é folga

# Range das Taxi Zones publicadas pelo TLC: 1–263 são zonas reais,
# 264 = "Unknown" e 265 = "Outside of NYC". Tudo fora disso é inválido.
_MIN_LOCATION_ID: int = 1
_MAX_LOCATION_ID: int = 265


def _validation_expression(taxi_type: str, fn: Any) -> Any:
    """
    Constrói a expressão Column booleana que mantém apenas registros
    válidos segundo os data dictionaries do TLC.

    Filosofia:
    - **Chaves de negócio** (VendorID, pickup, dropoff): rejeitar NULL
      ou fora do dicionário — sem chave não há corrida.
    - **Coerência temporal**: dropoff > pickup.
    - **Valores monetários**: total_amount >= 0 obrigatório (descarta
      voided/disputed); demais campos monetários nulláveis (NULL é
      legítimo) mas >= 0 quando informados.
    - **Distância**: nullável; >= 0 e <= 200mi quando informada.
    - **Passageiros**: nullável; 1–9 quando informado.
    - **Enums do TLC** (RatecodeID, payment_type, store_and_fwd_flag,
      trip_type): nulláveis (NULL = "missing data"), mas valores fora
      do dicionário são bronze corrompida e são rejeitados.
    - **Taxi Zones**: nulláveis; 1–265 quando informadas.

    Recebe `fn` (`pyspark.sql.functions`) como argumento explícito para
    permitir testar a montagem da expressão sem precisar de Spark.
    """
    if taxi_type not in _PICKUP_COLS:
        raise ValueError(
            f"Unsupported taxi_type for silver: {taxi_type!r}. "
            f"Expected one of {sorted(_PICKUP_COLS)}."
        )

    pickup_col, dropoff_col = _PICKUP_COLS[taxi_type]

    expr = (
        # Chave de negócio + VendorID no enum do TLC. `isin` em NULL
        # retorna NULL (falsy em WHERE), então drop de NULL é implícito.
        fn.col("VendorID").isin(list(_VALID_VENDOR_IDS))
        & fn.col(pickup_col).isNotNull()
        & fn.col(dropoff_col).isNotNull()
        # Coerência temporal
        & (fn.col(dropoff_col) > fn.col(pickup_col))
        # Valores monetários
        & (fn.col("total_amount") >= 0)
        & (fn.col("fare_amount").isNull() | (fn.col("fare_amount") >= 0))
        & (fn.col("tip_amount").isNull() | (fn.col("tip_amount") >= 0))
        & (fn.col("tolls_amount").isNull() | (fn.col("tolls_amount") >= 0))
        # Distância
        & (
            fn.col("trip_distance").isNull()
            | (
                (fn.col("trip_distance") >= 0)
                & (fn.col("trip_distance") <= _MAX_TRIP_DISTANCE_MILES)
            )
        )
        # Passageiros
        & (
            fn.col("passenger_count").isNull()
            | (
                (fn.col("passenger_count") > 0)
                & (fn.col("passenger_count") <= _MAX_PASSENGER_COUNT)
            )
        )
        # Enums do TLC (NULL aceito como missing)
        & (
            fn.col("RatecodeID").isNull()
            | fn.col("RatecodeID").isin(list(_VALID_RATECODE_IDS))
        )
        & (
            fn.col("payment_type").isNull()
            | fn.col("payment_type").isin(list(_VALID_PAYMENT_TYPES))
        )
        & (
            fn.col("store_and_fwd_flag").isNull()
            | fn.col("store_and_fwd_flag").isin(list(_VALID_STORE_FWD_FLAGS))
        )
        # Taxi Zones
        & (
            fn.col("PULocationID").isNull()
            | fn.col("PULocationID").between(_MIN_LOCATION_ID, _MAX_LOCATION_ID)
        )
        & (
            fn.col("DOLocationID").isNull()
            | fn.col("DOLocationID").between(_MIN_LOCATION_ID, _MAX_LOCATION_ID)
        )
    )

    if taxi_type == "green":
        # `trip_type` é exclusivo de green (street-hail vs dispatch).
        expr = expr & (
            fn.col("trip_type").isNull()
            | fn.col("trip_type").isin(list(_VALID_TRIP_TYPES))
        )

    return expr


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
        prog="transform_silver",
        description=(
            "Transform NYC TLC bronze tables into one conformed silver Delta "
            "table per taxi type (yellow_taxi_trips, green_taxi_trips), "
            "preserving native columns and adding pipeline metadata."
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
            "Tipo de taxi a transformar. 'yellow' ou 'green' processa "
            "apenas uma tabela; 'both' (default) processa as duas em "
            "sequência."
        ),
    )
    return parser.parse_args(argv)


def _resolve_taxi_types(taxi_type_arg: str, cfg: PipelineConfig) -> tuple[str, ...]:
    """Mapeia o valor de `--taxi-type` para a lista de tipos a processar."""
    if taxi_type_arg == TAXI_TYPE_ALL:
        return cfg.supported_taxi_types
    return (taxi_type_arg,)


def ensure_yellow_silver_table(cfg: PipelineConfig, spark: Any) -> None:
    """
    DDL explícito da silver **yellow**: todas as colunas do Trip Record TLC
    (yellow) + lineage (`_bronze_ingestion_ts`, `pickup_date`,
    `_silver_processed_ts`), mesmas `TBLPROPERTIES` Delta que a camada usa
    hoje e Liquid Clustering em `_YELLOW_SILVER_CLUSTER_COLS`.

    Tipos alinhados a `_SILVER_COLUMN_TYPES` / `build_silver_dataframe`.
    """
    taxi_type = "yellow"
    table_fqn = cfg.silver_table_fqn_for(taxi_type)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            VendorID {_SILVER_COLUMN_TYPES["VendorID"]},
            tpep_pickup_datetime {_SILVER_COLUMN_TYPES["tpep_pickup_datetime"]},
            tpep_dropoff_datetime {_SILVER_COLUMN_TYPES["tpep_dropoff_datetime"]},
            passenger_count {_SILVER_COLUMN_TYPES["passenger_count"]},
            trip_distance {_SILVER_COLUMN_TYPES["trip_distance"]},
            RatecodeID {_SILVER_COLUMN_TYPES["RatecodeID"]},
            store_and_fwd_flag {_SILVER_COLUMN_TYPES["store_and_fwd_flag"]},
            PULocationID {_SILVER_COLUMN_TYPES["PULocationID"]},
            DOLocationID {_SILVER_COLUMN_TYPES["DOLocationID"]},
            payment_type {_SILVER_COLUMN_TYPES["payment_type"]},
            fare_amount {_SILVER_COLUMN_TYPES["fare_amount"]},
            extra {_SILVER_COLUMN_TYPES["extra"]},
            mta_tax {_SILVER_COLUMN_TYPES["mta_tax"]},
            tip_amount {_SILVER_COLUMN_TYPES["tip_amount"]},
            tolls_amount {_SILVER_COLUMN_TYPES["tolls_amount"]},
            improvement_surcharge {_SILVER_COLUMN_TYPES["improvement_surcharge"]},
            total_amount {_SILVER_COLUMN_TYPES["total_amount"]},
            congestion_surcharge {_SILVER_COLUMN_TYPES["congestion_surcharge"]},
            airport_fee {_SILVER_COLUMN_TYPES["airport_fee"]},
            _bronze_ingestion_ts {_SILVER_COLUMN_TYPES["_bronze_ingestion_ts"]},
            {_CLUSTER_COL} {_SILVER_COLUMN_TYPES[_CLUSTER_COL]},
            _silver_processed_ts {_SILVER_COLUMN_TYPES["_silver_processed_ts"]}
        )
        USING DELTA
        CLUSTER BY ({_format_cluster_by(_YELLOW_SILVER_CLUSTER_COLS)})
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableChangeDataFeed' = 'true',
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5',
            'delta.feature.timestampNtz' = 'supported'
        )
        COMMENT 'Silver layer: NYC Yellow Taxi conformed trips.'
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {table_fqn}
        SET TAGS (
            'layer' = 'silver',
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
        "Silver table ensured",
        taxi_type=taxi_type,
        table=table_fqn,
    )


def ensure_green_silver_table(cfg: PipelineConfig, spark: Any) -> None:
    """
    DDL explícito da silver **green**: todas as colunas do Trip Record TLC
    (green) + lineage, mesmas propriedades Delta e clustering em
    `_GREEN_SILVER_CLUSTER_COLS`.
    """
    taxi_type = "green"
    table_fqn = cfg.silver_table_fqn_for(taxi_type)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            VendorID {_SILVER_COLUMN_TYPES["VendorID"]},
            lpep_pickup_datetime {_SILVER_COLUMN_TYPES["lpep_pickup_datetime"]},
            lpep_dropoff_datetime {_SILVER_COLUMN_TYPES["lpep_dropoff_datetime"]},
            store_and_fwd_flag {_SILVER_COLUMN_TYPES["store_and_fwd_flag"]},
            RatecodeID {_SILVER_COLUMN_TYPES["RatecodeID"]},
            PULocationID {_SILVER_COLUMN_TYPES["PULocationID"]},
            DOLocationID {_SILVER_COLUMN_TYPES["DOLocationID"]},
            passenger_count {_SILVER_COLUMN_TYPES["passenger_count"]},
            trip_distance {_SILVER_COLUMN_TYPES["trip_distance"]},
            fare_amount {_SILVER_COLUMN_TYPES["fare_amount"]},
            extra {_SILVER_COLUMN_TYPES["extra"]},
            mta_tax {_SILVER_COLUMN_TYPES["mta_tax"]},
            tip_amount {_SILVER_COLUMN_TYPES["tip_amount"]},
            tolls_amount {_SILVER_COLUMN_TYPES["tolls_amount"]},
            ehail_fee {_SILVER_COLUMN_TYPES["ehail_fee"]},
            improvement_surcharge {_SILVER_COLUMN_TYPES["improvement_surcharge"]},
            total_amount {_SILVER_COLUMN_TYPES["total_amount"]},
            payment_type {_SILVER_COLUMN_TYPES["payment_type"]},
            trip_type {_SILVER_COLUMN_TYPES["trip_type"]},
            congestion_surcharge {_SILVER_COLUMN_TYPES["congestion_surcharge"]},
            _bronze_ingestion_ts {_SILVER_COLUMN_TYPES["_bronze_ingestion_ts"]},
            {_CLUSTER_COL} {_SILVER_COLUMN_TYPES[_CLUSTER_COL]},
            _silver_processed_ts {_SILVER_COLUMN_TYPES["_silver_processed_ts"]}
        )
        USING DELTA
        CLUSTER BY ({_format_cluster_by(_GREEN_SILVER_CLUSTER_COLS)})
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableChangeDataFeed' = 'true',
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5',
            'delta.feature.timestampNtz' = 'supported'
        )
        COMMENT 'Silver layer: NYC Green Taxi conformed trips.'
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {table_fqn}
        SET TAGS (
            'layer' = 'silver',
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
        "Silver table ensured",
        taxi_type=taxi_type,
        table=table_fqn,
    )


def build_silver_dataframe(
    cfg: PipelineConfig, taxi_type: str, spark: Any
) -> Any:
    """
    Lê a bronze de um taxi, projeta **explicitamente** as colunas do Trip
    Record TLC + lineage de ingestão, aplica DQ inline, deduplica pela
    chave de negócio, adiciona lineage da silver e faz `cast` final para
    o schema declarado em `ensure_yellow_silver_table` /
    `ensure_green_silver_table`.

    Projeção explícita da bronze:
        `select(*_SILVER_TLC_COLUMNS[taxi_type], _ingestion_ts)`. Colunas
        extras só na bronze (metadados de arquivo como `_source_file`,
        `_source_year`, `_taxi_type` ou colunas TLC futuras adicionadas
        via schema evolution na bronze) **não** chegam ao MERGE. Silver
        é contrato fixo.

    Colunas adicionadas pela silver:
        - `pickup_date`: componente temporal do clustering líquido
          (conjunto declarado por tipo em `ensure_*_silver_table`).
        - `_silver_processed_ts`: lineage da camada.
        - `_bronze_ingestion_ts`: renomeio de `_ingestion_ts` para
          deixar explícito de onde veio o timestamp.

    Validação (drop de registros inválidos):
        Regras em `_validation_expression`. Resumo:
        - VendorID, pickup, dropoff: not null + enums do TLC.
        - dropoff > pickup.
        - total_amount >= 0; demais valores monetários >= 0 quando
          informados (NULL aceito).
        - trip_distance e passenger_count em ranges sane.
        - Enums (RatecodeID, payment_type, store_and_fwd_flag,
          trip_type) dentro do dicionário oficial.
        - Taxi Zone IDs em 1..265.

    Deduplicação:
        Pela tripla `(VendorID, pickup, dropoff)`. Re-ingestão do
        mesmo Parquet pelo TLC não duplica corridas. `taxi_type` não
        entra na chave — cada tabela já é segregada fisicamente
        (ADR-013).

    Cast final:
        Para cada coluna física, `F.col(name).cast(_SILVER_COLUMN_TYPES
        [name])`. Garante que o DataFrame casa exatamente com o schema
        DDL da tabela silver — sem isso, o MERGE rejeita quando a bronze
        inferiu INT contra a silver declarada como BIGINT (ou TIMESTAMP
        vs TIMESTAMP_NTZ).
    """
    if taxi_type not in _PICKUP_COLS:
        raise ValueError(
            f"Unsupported taxi_type for silver: {taxi_type!r}. "
            f"Expected one of {sorted(_PICKUP_COLS)}."
        )

    pickup_col, dropoff_col = _PICKUP_COLS[taxi_type]
    bronze_df = spark.table(cfg.bronze_table_fqn_for(taxi_type))
    _require_bronze_trip_record_columns(bronze_df.columns, taxi_type)
    _require_bronze_ingestion_column(bronze_df.columns)

    try:
        from pyspark.sql import functions as F  # type: ignore[import-not-found]  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "pyspark.sql.functions not available; running outside Databricks cluster?"
        ) from exc

    # Projeção explícita do contrato silver sobre a bronze: TLC trip
    # record + `_ingestion_ts`. Metadados de ingestão da bronze e
    # eventuais colunas novas que tenham aparecido lá ficam de fora.
    bronze_projected = bronze_df.select(
        *_SILVER_TLC_COLUMNS[taxi_type],
        _BRONZE_RAW_INGESTION_COL,
    )

    cleaned_df = bronze_projected.where(_validation_expression(taxi_type, F))
    enriched = (
        cleaned_df.withColumnRenamed(_BRONZE_RAW_INGESTION_COL, "_bronze_ingestion_ts")
        .withColumn(_CLUSTER_COL, F.to_date(F.col(pickup_col)))
        .withColumn("_silver_processed_ts", F.current_timestamp())
        .dropDuplicates(["VendorID", pickup_col, dropoff_col])
    )

    # Cast final alinhando com o schema DDL declarado em
    # `ensure_*_silver_table`. Sem isso, divergências de tipo entre bronze
    # e silver derrubam o MERGE (que roda sem schema evolution).
    casted = enriched
    for col_name in _silver_physical_column_names(taxi_type):
        casted = casted.withColumn(
            col_name, F.col(col_name).cast(_SILVER_COLUMN_TYPES[col_name])
        )

    return casted.select(*_silver_physical_column_names(taxi_type))


def merge_into_silver(
    cfg: PipelineConfig, taxi_type: str, spark: Any, silver_df: Any
) -> None:
    """
    Upsert idempotente (`MERGE INTO`) na tabela silver **sem** schema
    evolution.

    A tabela silver já é criada com schema explícito + CLUSTER BY em
    `ensure_yellow_silver_table` / `ensure_green_silver_table`, então
    primeiro run e subsequentes seguem o
    mesmo caminho: `MERGE INTO` casando pela chave de negócio
    `(VendorID, pickup, dropoff)`. Não há `withSchemaEvolution()` nem
    `mergeSchema=true` em momento algum — silver é contrato fechado no
    trip record TLC 2023.

    O DataFrame entregue por `build_silver_dataframe` já vem com a
    projeção explícita das colunas da bronze e com `cast` para o tipo
    declarado em `_SILVER_COLUMN_TYPES`, casando exatamente com o schema
    da tabela.

    Histórico: Serverless pode bloquear `spark.databricks.delta.schema.autoMerge`
    (ADR-003); esta camada também não usa o builder só para driblar esse
    limite — a política de produto aqui é **contrato fixo**.
    """
    try:
        from delta.tables import DeltaTable  # type: ignore[import-untyped]  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "delta.tables not available; running outside Databricks cluster?"
        ) from exc

    if taxi_type not in _PICKUP_COLS:
        raise ValueError(
            f"Unsupported taxi_type for silver: {taxi_type!r}. "
            f"Expected one of {sorted(_PICKUP_COLS)}."
        )

    pickup_col, dropoff_col = _PICKUP_COLS[taxi_type]
    table_fqn = cfg.silver_table_fqn_for(taxi_type)

    silver_table = DeltaTable.forName(spark, table_fqn)
    (
        silver_table.alias("t")
        .merge(
            silver_df.alias("s"),
            f"t.VendorID = s.VendorID "
            f"AND t.{pickup_col} = s.{pickup_col} "
            f"AND t.{dropoff_col} = s.{dropoff_col}",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    log_with_context(
        logger,
        logging.INFO,
        "Silver merge completed",
        taxi_type=taxi_type,
        table=table_fqn,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """
    Entry point chamado pela Databricks python_wheel task.

    Processa um ou ambos os tipos de taxi em sequência. Qualquer falha
    interrompe o job: silver é camada de contrato e publicar parcial
    induziria gold/consumidores a verem snapshot inconsistente.
    """
    args = _parse_args(argv)
    config = PipelineConfig(environment=args.environment, catalog=args.catalog)
    taxi_types = _resolve_taxi_types(args.taxi_type, config)

    log_with_context(
        logger,
        logging.INFO,
        "Starting transform_silver",
        catalog=config.catalog,
        environment=config.environment,
        taxi_types=list(taxi_types),
        source_tables=[config.bronze_table_fqn_for(t) for t in taxi_types],
        target_tables=[config.silver_table_fqn_for(t) for t in taxi_types],
    )

    spark = _get_spark()
    processed: list[str] = []

    for taxi_type in taxi_types:
        try:
            if taxi_type == "yellow":
                ensure_yellow_silver_table(config, spark)
            elif taxi_type == "green":
                ensure_green_silver_table(config, spark)
            else:
                raise ValueError(
                    f"Unsupported taxi_type for silver: {taxi_type!r}. "
                    f"Expected 'yellow' or 'green'."
                )
            silver_df = build_silver_dataframe(config, taxi_type, spark)
            merge_into_silver(config, taxi_type, spark, silver_df)
            processed.append(taxi_type)
        except Exception:
            log_with_context(
                logger,
                logging.ERROR,
                "Silver transform failed",
                taxi_type=taxi_type,
                processed_before_failure=processed,
            )
            # Fail-fast: silver é camada de contrato. Diferente da bronze
            # (onde a tolerância é proposital), aqui qualquer falha mata
            # o job para que a gold não consuma snapshot parcial.
            raise

    summary: dict[str, Any] = {
        "per_taxi": {t: {"status": "ok"} for t in processed},
    }
    print(json.dumps(summary))

    return 0


if __name__ == "__main__":
    sys.exit(main())
