from __future__ import annotations

import io
import pathlib
import sys
import types
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

# Ensure local src/ layout is importable in test runs.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi.core.config import PipelineConfig
from nyc_taxi.lakehouse.silver import main as silver_main


class _FakeSparkForSql:
    """Spark fake que só registra `spark.sql(...)` — suficiente para
    validar a DDL emitida por `ensure_yellow_silver_table` /
    `ensure_green_silver_table`."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> object:
        self.queries.append(query)
        return object()


class _FakeColumn:
    """Column fake — encadeável para `_validation_expression` + `build_silver_dataframe`."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __and__(self, other: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) & ({other})")

    def __or__(self, other: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) | ({other})")

    def __gt__(self, other: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) > ({other})")

    def __ge__(self, other: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) >= ({other})")

    def __lt__(self, other: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) < ({other})")

    def __le__(self, other: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) <= ({other})")

    def __sub__(self, other: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) - ({other})")

    def __invert__(self) -> "_FakeColumn":
        return _FakeColumn(f"~({self.name})")

    def isin(self, values: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) IN {list(values)}")

    def isNull(self) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) IS NULL")

    def isNotNull(self) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) IS NOT NULL")

    def between(self, lo: Any, hi: Any) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) BETWEEN {lo} AND {hi}")

    def cast(self, _to: str) -> "_FakeColumn":
        return _FakeColumn(f"({self.name}) AS {_to}")

    def __repr__(self) -> str:
        return self.name


class _FakeFunctions:
    """Mimic `pyspark.sql.functions` — só o que o silver usa em testes locais."""

    def __init__(self) -> None:
        self.col_names: list[str] = []

    def col(self, name: str) -> _FakeColumn:
        self.col_names.append(name)
        return _FakeColumn(name)

    @staticmethod
    def to_date(column: Any) -> _FakeColumn:
        return _FakeColumn(f"to_date({column!r})")

    @staticmethod
    def current_timestamp() -> _FakeColumn:
        return _FakeColumn("current_timestamp()")


def _silver_build_stub_pyspark_modules() -> dict[str, types.ModuleType]:
    """
    Stub mínimo de `pyspark.sql`/`pyspark.sql.functions` para rodar os testes de
    `build_silver_dataframe` sem cluster (CI local).
    """
    fake_functions = types.ModuleType("pyspark.sql.functions")

    wrapped = _FakeFunctions()

    def _col(name: str) -> _FakeColumn:
        return wrapped.col(name)

    fake_functions.col = _col  # type: ignore[attr-defined]
    fake_functions.to_date = wrapped.to_date  # type: ignore[attr-defined]
    fake_functions.current_timestamp = wrapped.current_timestamp  # type: ignore[attr-defined]

    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.functions = fake_functions  # type: ignore[attr-defined]

    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.sql = fake_sql_module  # type: ignore[attr-defined]

    return {
        "pyspark": fake_pyspark,
        "pyspark.sql": fake_sql_module,
        "pyspark.sql.functions": fake_functions,
    }


class TestEnsureSilverTable(unittest.TestCase):
    """
    `ensure_yellow_silver_table` / `ensure_green_silver_table` declaram o
    schema físico explicitamente (TLC trip record 2023 + lineage silver)
    e o Liquid Clustering
    no `CREATE TABLE`. Silver é camada de contrato — schema fica
    fechado a partir do primeiro DDL, sem depender do primeiro write.
    """

    def test_yellow_ddl_declares_explicit_schema_and_clustering(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForSql()

        silver_main.ensure_yellow_silver_table(cfg, spark)

        expected_fqn = cfg.silver_table_fqn_for("yellow")
        self.assertTrue(expected_fqn.endswith(".yellow_taxi_trips"))

        ddl = spark.queries[0]
        self.assertIn(f"CREATE TABLE IF NOT EXISTS {expected_fqn}", ddl)
        # Schema é declarado coluna a coluna na DDL.
        self.assertIn("VendorID BIGINT", ddl)
        self.assertIn("tpep_pickup_datetime TIMESTAMP_NTZ", ddl)
        self.assertIn("tpep_dropoff_datetime TIMESTAMP_NTZ", ddl)
        self.assertIn("airport_fee DOUBLE", ddl)
        self.assertIn("pickup_date DATE", ddl)
        self.assertIn("_bronze_ingestion_ts TIMESTAMP", ddl)
        self.assertIn("_silver_processed_ts TIMESTAMP", ddl)
        # Colunas exclusivas de green NÃO aparecem na DDL do yellow.
        self.assertNotIn("lpep_pickup_datetime", ddl)
        self.assertNotIn("ehail_fee", ddl)
        self.assertNotIn("trip_type ", ddl)
        # Liquid Clustering declarado no próprio CREATE TABLE.
        yellow_cluster_by = silver_main._format_cluster_by(
            silver_main._YELLOW_SILVER_CLUSTER_COLS
        )
        self.assertIn(f"CLUSTER BY ({yellow_cluster_by})", ddl)

        # Properties precisam estar no CREATE (não são inferidas).
        self.assertIn("'delta.columnMapping.mode' = 'name'", ddl)
        self.assertIn("'delta.autoOptimize.optimizeWrite' = 'true'", ddl)
        self.assertIn("'delta.enableChangeDataFeed' = 'true'", ddl)

        # SET TAGS sai como segundo statement com taxi_type correto.
        tags_sql = spark.queries[1]
        self.assertIn(f"ALTER TABLE {expected_fqn}", tags_sql)
        self.assertIn("'layer' = 'silver'", tags_sql)
        self.assertIn("'taxi_type' = 'yellow'", tags_sql)

    def test_green_ddl_declares_explicit_schema_and_clustering(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForSql()

        silver_main.ensure_green_silver_table(cfg, spark)

        expected_fqn = cfg.silver_table_fqn_for("green")
        self.assertTrue(expected_fqn.endswith(".green_taxi_trips"))

        ddl = spark.queries[0]
        self.assertIn(f"CREATE TABLE IF NOT EXISTS {expected_fqn}", ddl)
        # Colunas nativas de green presentes.
        self.assertIn("lpep_pickup_datetime TIMESTAMP_NTZ", ddl)
        self.assertIn("lpep_dropoff_datetime TIMESTAMP_NTZ", ddl)
        self.assertIn("ehail_fee DOUBLE", ddl)
        self.assertIn("trip_type BIGINT", ddl)
        # Colunas exclusivas de yellow NÃO aparecem na DDL do green.
        self.assertNotIn("tpep_pickup_datetime", ddl)
        self.assertNotIn("airport_fee", ddl)
        # Liquid Clustering.
        green_cluster_by = silver_main._format_cluster_by(
            silver_main._GREEN_SILVER_CLUSTER_COLS
        )
        self.assertIn(f"CLUSTER BY ({green_cluster_by})", ddl)
        self.assertIn("'taxi_type' = 'green'", spark.queries[1])


class TestResolveTaxiTypes(unittest.TestCase):
    def test_both_expands_to_all_supported(self) -> None:
        cfg = PipelineConfig()
        self.assertEqual(
            silver_main._resolve_taxi_types("both", cfg),
            cfg.supported_taxi_types,
        )

    def test_single_value_is_wrapped_in_tuple(self) -> None:
        cfg = PipelineConfig()
        self.assertEqual(silver_main._resolve_taxi_types("green", cfg), ("green",))
        self.assertEqual(silver_main._resolve_taxi_types("yellow", cfg), ("yellow",))


# ---------------------------------------------------------------------------
# Fake DataFrame chain — registra os métodos chamados sem precisar de Spark
# real. Suficiente para validar filtros, colunas adicionadas e dedup keys.
# ---------------------------------------------------------------------------


class _RecordingDataFrame:
    """Captura a sequência de transformações aplicadas pelo silver."""

    def __init__(self, name: str, stub_columns: list[str] | None = None):
        self.name = name
        self.stub_columns: list[str] = list(stub_columns or [])
        self.with_column_calls: list[str] = []
        self.with_column_renamed_calls: list[tuple[str, str]] = []
        self.where_calls: list[Any] = []
        self.drop_duplicates_keys: list[str] | None = None
        self.selected_columns: tuple[str, ...] | None = None
        # Histórico completo de `select(...)` — preciso porque o silver faz
        # duas seleções (projeção explícita da bronze + projeção final
        # alinhada ao schema da tabela).
        self.select_call_history: list[tuple[str, ...]] = []

    @property
    def columns(self) -> list[str]:
        return list(self.stub_columns)

    def select(self, *cols: str) -> "_RecordingDataFrame":
        self.selected_columns = cols
        self.select_call_history.append(cols)
        return self

    def withColumn(self, name: str, _expr: Any) -> "_RecordingDataFrame":
        self.with_column_calls.append(name)
        if name not in self.stub_columns:
            self.stub_columns.append(name)
        return self

    def withColumnRenamed(self, old: str, new: str) -> "_RecordingDataFrame":
        self.with_column_renamed_calls.append((old, new))
        if old in self.stub_columns:
            self.stub_columns[self.stub_columns.index(old)] = new
        return self

    def where(self, condition: Any) -> "_RecordingDataFrame":
        self.where_calls.append(condition)
        return self

    def dropDuplicates(self, keys: list[str]) -> "_RecordingDataFrame":
        self.drop_duplicates_keys = list(keys)
        return self


class _FakeSparkWithTables:
    """Bronze stubs com colunas TLC + lineage — para projetar silver fixa."""

    def __init__(self, bronze_columns_by_fqn: dict[str, list[str]]) -> None:
        self._templates = dict(bronze_columns_by_fqn)
        self.tables: dict[str, _RecordingDataFrame] = {}

    def table(self, fqn: str) -> _RecordingDataFrame:
        if fqn not in self.tables:
            tmpl = list(self._templates[fqn])
            self.tables[fqn] = _RecordingDataFrame(fqn, tmpl)
        return self.tables[fqn]


class TestBuildSilverDataframe(unittest.TestCase):
    """
    Contrato da `build_silver_dataframe`:
    - Projeta **explicitamente** TLC trip record + `_ingestion_ts` da
      bronze (primeiro `select`); metadados extras (`_source_file`, etc.)
      ficam de fora desde a primeira projeção.
    - Renomeia só `_ingestion_ts` para `_bronze_ingestion_ts` (lineage).
    - Adiciona `pickup_date` e `_silver_processed_ts`.
    - Aplica filtros mínimos via `where`; dedup pela tripla de negócio.
    - Faz `cast` final para os tipos declarados em `_SILVER_COLUMN_TYPES`
      antes do `select` final, garantindo schema compatível com o MERGE
      sem schema evolution.
    """

    def test_yellow_projects_bronze_explicitly_and_casts_to_silver_schema(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        yellow_fqn = cfg.bronze_table_fqn_for("yellow")
        tlc = list(silver_main._SILVER_TLC_COLUMNS["yellow"])
        # bronze inclui metadados extras que NÃO podem entrar no contrato
        # silver — a projeção explícita precisa filtrá-los.
        lineage = [silver_main._BRONZE_RAW_INGESTION_COL, "_source_file"]
        spark = _FakeSparkWithTables({yellow_fqn: tlc + lineage})

        with patch.dict(sys.modules, _silver_build_stub_pyspark_modules()):
            silver_df = silver_main.build_silver_dataframe(cfg, "yellow", spark)
        recorded = spark.tables[yellow_fqn]

        # 1) Primeira projeção: SELECT explícito das colunas TLC + ingestão
        #    da bronze. Metadados (`_source_file`) ficam fora.
        self.assertGreaterEqual(len(recorded.select_call_history), 2)
        bronze_projection = recorded.select_call_history[0]
        self.assertEqual(
            bronze_projection,
            (*silver_main._SILVER_TLC_COLUMNS["yellow"], silver_main._BRONZE_RAW_INGESTION_COL),
        )
        self.assertNotIn("_source_file", bronze_projection)

        # 2) Última projeção: schema físico da silver (TLC + lineage silver).
        expected_select = tuple(silver_main._silver_physical_column_names("yellow"))
        self.assertEqual(silver_df.selected_columns, expected_select)
        self.assertNotIn("_source_file", expected_select)

        # 3) Dedup pela chave de negócio.
        self.assertEqual(
            silver_df.drop_duplicates_keys,
            ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"],
        )

        # 4) Enrichment + cast: cada coluna física aparece em withColumn
        #    (cast para o tipo declarado em `_SILVER_COLUMN_TYPES`).
        self.assertIn("pickup_date", silver_df.with_column_calls)
        self.assertIn("_silver_processed_ts", silver_df.with_column_calls)
        for physical_col in expected_select:
            self.assertIn(physical_col, silver_df.with_column_calls)

        self.assertIn(
            (silver_main._BRONZE_RAW_INGESTION_COL, "_bronze_ingestion_ts"),
            silver_df.with_column_renamed_calls,
        )
        self.assertEqual(len(recorded.where_calls), 1)

    def test_green_projects_bronze_explicitly_and_casts_to_silver_schema(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        green_fqn = cfg.bronze_table_fqn_for("green")
        tlc = list(silver_main._SILVER_TLC_COLUMNS["green"])
        spark = _FakeSparkWithTables(
            {green_fqn: tlc + [silver_main._BRONZE_RAW_INGESTION_COL]}
        )

        with patch.dict(sys.modules, _silver_build_stub_pyspark_modules()):
            silver_df = silver_main.build_silver_dataframe(cfg, "green", spark)
        recorded = spark.tables[green_fqn]

        # Primeira projeção: SELECT explícito das colunas TLC de green.
        bronze_projection = recorded.select_call_history[0]
        self.assertEqual(
            bronze_projection,
            (*silver_main._SILVER_TLC_COLUMNS["green"], silver_main._BRONZE_RAW_INGESTION_COL),
        )

        self.assertEqual(
            silver_df.drop_duplicates_keys,
            ["VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime"],
        )
        expected_select = tuple(silver_main._silver_physical_column_names("green"))
        self.assertEqual(silver_df.selected_columns, expected_select)
        self.assertIn("pickup_date", silver_df.with_column_calls)
        # Cast nas colunas exclusivas de green.
        self.assertIn("ehail_fee", silver_df.with_column_calls)
        self.assertIn("trip_type", silver_df.with_column_calls)

    def test_unsupported_taxi_type_raises_value_error(self) -> None:
        cfg = PipelineConfig()
        with self.assertRaises(ValueError):
            silver_main.build_silver_dataframe(cfg, "fhv", object())

    def test_missing_tlc_columns_in_bronze_raises(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        yellow_fqn = cfg.bronze_table_fqn_for("yellow")
        incomplete = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            silver_main._BRONZE_RAW_INGESTION_COL,
        ]
        spark = _FakeSparkWithTables({yellow_fqn: incomplete})
        with self.assertRaises(ValueError) as ctx:
            silver_main.build_silver_dataframe(cfg, "yellow", spark)
        self.assertIn("Bronze is missing TLC trip-record column", str(ctx.exception))

    def test_missing_ingestion_ts_in_bronze_raises(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        yellow_fqn = cfg.bronze_table_fqn_for("yellow")
        spark = _FakeSparkWithTables({yellow_fqn: list(silver_main._SILVER_TLC_COLUMNS["yellow"])})
        with self.assertRaises(ValueError) as ctx:
            silver_main.build_silver_dataframe(cfg, "yellow", spark)
        self.assertIn(silver_main._BRONZE_RAW_INGESTION_COL, str(ctx.exception))


# `_FakeColumn` / `_FakeFunctions` / `_silver_build_stub_pyspark_modules`:
# topo do arquivo (compartilhados por validação e build).


class TestValidationExpression(unittest.TestCase):
    """
    `_validation_expression` traduz as regras dos data dictionaries do
    TLC em uma Column boolean. Testamos quais colunas são referenciadas
    para garantir cobertura — a semântica do filtro é validada via teste
    de integração no Databricks.
    """

    def _build(self, taxi_type: str) -> tuple[Any, _FakeFunctions]:
        fn = _FakeFunctions()
        expr = silver_main._validation_expression(taxi_type, fn)
        return expr, fn

    def test_yellow_references_tpep_timestamps_and_core_columns(self) -> None:
        _, fn = self._build("yellow")
        referenced = set(fn.col_names)

        # Chaves de negócio + timestamps yellow nativos
        self.assertIn("VendorID", referenced)
        self.assertIn("tpep_pickup_datetime", referenced)
        self.assertIn("tpep_dropoff_datetime", referenced)
        # Valores monetários (data dictionary do TLC)
        self.assertIn("total_amount", referenced)
        self.assertIn("fare_amount", referenced)
        self.assertIn("tip_amount", referenced)
        self.assertIn("tolls_amount", referenced)
        # Dimensões da corrida
        self.assertIn("trip_distance", referenced)
        self.assertIn("passenger_count", referenced)
        # Enums + zonas
        self.assertIn("RatecodeID", referenced)
        self.assertIn("payment_type", referenced)
        self.assertIn("store_and_fwd_flag", referenced)
        self.assertIn("PULocationID", referenced)
        self.assertIn("DOLocationID", referenced)
        # Yellow NÃO usa colunas de green
        self.assertNotIn("lpep_pickup_datetime", referenced)
        self.assertNotIn("lpep_dropoff_datetime", referenced)
        self.assertNotIn("trip_type", referenced)

    def test_green_references_lpep_timestamps_and_trip_type(self) -> None:
        _, fn = self._build("green")
        referenced = set(fn.col_names)

        self.assertIn("lpep_pickup_datetime", referenced)
        self.assertIn("lpep_dropoff_datetime", referenced)
        # `trip_type` é exclusivo de green (street-hail vs dispatch)
        self.assertIn("trip_type", referenced)
        # Green NÃO usa colunas de yellow
        self.assertNotIn("tpep_pickup_datetime", referenced)

    def test_unsupported_taxi_type_raises(self) -> None:
        fn = _FakeFunctions()
        with self.assertRaises(ValueError):
            silver_main._validation_expression("fhv", fn)

    def test_enum_lists_match_data_dictionary(self) -> None:
        """
        Sanity check das listas de enums contra os data dictionaries do
        TLC em `docs/nyc/`. Se o TLC publicar um valor novo, esta lista
        precisa acompanhar.
        """
        self.assertEqual(
            set(silver_main._VALID_RATECODE_IDS), {1, 2, 3, 4, 5, 6, 99}
        )
        self.assertEqual(
            set(silver_main._VALID_PAYMENT_TYPES), {0, 1, 2, 3, 4, 5, 6}
        )
        self.assertEqual(
            set(silver_main._VALID_STORE_FWD_FLAGS), {"Y", "N"}
        )
        self.assertEqual(set(silver_main._VALID_TRIP_TYPES), {1, 2})

    def test_fixed_tlc_column_sets_match_known_divergence_yellow_green(self) -> None:
        """Smoke do contrato 2023 (PDFs TLC): divergências conhecidas yellow vs green."""
        y = silver_main._SILVER_TLC_COLUMNS["yellow"]
        g = silver_main._SILVER_TLC_COLUMNS["green"]
        self.assertEqual(len(y), 19)
        self.assertEqual(len(g), 20)
        self.assertIn("airport_fee", y)
        self.assertNotIn("ehail_fee", y)
        self.assertNotIn("trip_type", y)
        self.assertIn("ehail_fee", g)
        self.assertIn("trip_type", g)
        self.assertNotIn("airport_fee", g)
        self.assertEqual(len(silver_main._silver_physical_column_names("yellow")), 22)
        self.assertEqual(len(silver_main._silver_physical_column_names("green")), 23)


class TestSilverColumnTypes(unittest.TestCase):
    """
    `_SILVER_COLUMN_TYPES` é a fonte única de verdade dos tipos físicos
    da silver. Tem que cobrir toda coluna que `_silver_physical_column_names`
    devolve para yellow e green — sem isso a DDL ou o cast quebram.
    """

    def test_every_physical_column_has_declared_type(self) -> None:
        for taxi_type in ("yellow", "green"):
            for col in silver_main._silver_physical_column_names(taxi_type):
                self.assertIn(
                    col,
                    silver_main._SILVER_COLUMN_TYPES,
                    msg=f"{col} (taxi={taxi_type}) has no declared type",
                )

    def test_physical_ddl_fragment_from_contract_has_correct_arity(self) -> None:
        """
        Sanity: número de campos físicos fecha com o join `nome tipo` que
        as funções `ensure_*_silver_table` espelham (sem duplicar a DDL
        literal nos testes).
        """
        for taxi_type in ("yellow", "green"):
            cols = silver_main._silver_physical_column_names(taxi_type)
            ddl_fragment = ",\n            ".join(
                f"{n} {silver_main._SILVER_COLUMN_TYPES[n]}" for n in cols
            )
            self.assertEqual(ddl_fragment.count(","), len(cols) - 1)

        yellow_cols = silver_main._silver_physical_column_names("yellow")
        green_cols = silver_main._silver_physical_column_names("green")
        self.assertEqual(len(yellow_cols), 22)
        self.assertEqual(len(green_cols), 23)

        y_frag = ",".join(
            f"{n} {silver_main._SILVER_COLUMN_TYPES[n]}"
            for n in silver_main._silver_physical_column_names("yellow")
        )
        g_frag = ",".join(
            f"{n} {silver_main._SILVER_COLUMN_TYPES[n]}"
            for n in silver_main._silver_physical_column_names("green")
        )
        self.assertIn("VendorID BIGINT", y_frag)
        self.assertIn("trip_type BIGINT", g_frag)


class TestMain(unittest.TestCase):
    """
    `main` na nova política: falha em qualquer taxi interrompe o job
    (raise propagado). Silver é camada de contrato — diferente da
    bronze, onde a falha parcial é tolerada por design.
    """

    def test_main_processes_both_taxis_by_default(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                catalog="nyc_taxi_dev",
                environment="dev",
                taxi_type="both",
            )

        ensure_calls: list[str] = []
        build_calls: list[str] = []
        merge_calls: list[str] = []

        def fake_yellow(_cfg: object, _spark: object) -> None:
            ensure_calls.append("yellow")

        def fake_green(_cfg: object, _spark: object) -> None:
            ensure_calls.append("green")

        def fake_build(
            _cfg: object, taxi_type: str, _spark: object
        ) -> object:
            build_calls.append(taxi_type)
            return object()

        def fake_merge(
            _cfg: object, taxi_type: str, _spark: object, _silver_df: object
        ) -> None:
            merge_calls.append(taxi_type)

        with (
            patch("nyc_taxi.lakehouse.silver.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.silver.main._get_spark", return_value=object()),
            patch(
                "nyc_taxi.lakehouse.silver.main.ensure_yellow_silver_table",
                side_effect=fake_yellow,
            ),
            patch(
                "nyc_taxi.lakehouse.silver.main.ensure_green_silver_table",
                side_effect=fake_green,
            ),
            patch(
                "nyc_taxi.lakehouse.silver.main.build_silver_dataframe",
                side_effect=fake_build,
            ),
            patch(
                "nyc_taxi.lakehouse.silver.main.merge_into_silver",
                side_effect=fake_merge,
            ),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = silver_main.main([])

        self.assertEqual(exit_code, 0)
        self.assertEqual(ensure_calls, ["yellow", "green"])
        self.assertEqual(build_calls, ["yellow", "green"])
        self.assertEqual(merge_calls, ["yellow", "green"])
        payload = captured.getvalue().strip()
        self.assertIn('"yellow"', payload)
        self.assertIn('"green"', payload)
        self.assertIn('"status": "ok"', payload)

    def test_main_runs_for_single_taxi_when_requested(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                catalog="nyc_taxi_dev",
                environment="dev",
                taxi_type="green",
            )

        ensure_calls: list[str] = []

        def fake_yellow_skipped(_cfg: object, _spark: object) -> None:
            self.fail("yellow ensure should not run when taxi_type=green")

        def fake_green(_cfg: object, _spark: object) -> None:
            ensure_calls.append("green")

        with (
            patch("nyc_taxi.lakehouse.silver.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.silver.main._get_spark", return_value=object()),
            patch(
                "nyc_taxi.lakehouse.silver.main.ensure_yellow_silver_table",
                side_effect=fake_yellow_skipped,
            ),
            patch(
                "nyc_taxi.lakehouse.silver.main.ensure_green_silver_table",
                side_effect=fake_green,
            ),
            patch(
                "nyc_taxi.lakehouse.silver.main.build_silver_dataframe",
                return_value=object(),
            ),
            patch(
                "nyc_taxi.lakehouse.silver.main.merge_into_silver",
                return_value=None,
            ),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = silver_main.main([])

        self.assertEqual(exit_code, 0)
        self.assertEqual(ensure_calls, ["green"])
        self.assertIn('"green"', captured.getvalue())
        self.assertNotIn('"yellow"', captured.getvalue())

    def test_main_fails_fast_on_any_taxi_failure(self) -> None:
        """
        Silver é camada de contrato: se um taxi falhar, o job inteiro
        falha (raise propaga). Diferente da bronze, onde falha parcial
        é aceita.
        """

        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                catalog="nyc_taxi_dev",
                environment="dev",
                taxi_type="both",
            )

        merge_seen: list[str] = []

        def fake_merge(
            _cfg: object, taxi_type: str, _spark: object, _silver_df: object
        ) -> None:
            merge_seen.append(taxi_type)
            if taxi_type == "yellow":
                raise RuntimeError("yellow boom")

        with (
            patch("nyc_taxi.lakehouse.silver.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.silver.main._get_spark", return_value=object()),
            patch("nyc_taxi.lakehouse.silver.main.ensure_yellow_silver_table"),
            patch("nyc_taxi.lakehouse.silver.main.ensure_green_silver_table"),
            patch(
                "nyc_taxi.lakehouse.silver.main.build_silver_dataframe",
                return_value=object(),
            ),
            patch(
                "nyc_taxi.lakehouse.silver.main.merge_into_silver",
                side_effect=fake_merge,
            ),
            self.assertRaises(RuntimeError),
        ):
            silver_main.main([])

        # green NUNCA roda — falha no yellow já interrompe o job.
        self.assertEqual(merge_seen, ["yellow"])


if __name__ == "__main__":
    unittest.main()