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
from nyc_taxi.lakehouse.bronze import main as bronze_main


class _FakeExpr:
    def cast(self, _dtype: str) -> "_FakeExpr":
        return self


class _FakeFunctionsModule:
    @staticmethod
    def current_timestamp() -> _FakeExpr:
        return _FakeExpr()

    @staticmethod
    def col(_name: str) -> _FakeExpr:
        return _FakeExpr()

    @staticmethod
    def lit(_value: object) -> _FakeExpr:
        return _FakeExpr()

    @staticmethod
    def regexp_extract(_expr: _FakeExpr, _pattern: str, _idx: int) -> _FakeExpr:
        return _FakeExpr()


class _FakeQuery:
    def __init__(self, last_progress: dict[str, object] | None = None) -> None:
        self.lastProgress = last_progress
        self.await_termination_called = False

    def awaitTermination(self) -> None:
        self.await_termination_called = True


class _FakeWriteStream:
    def __init__(self, query: _FakeQuery) -> None:
        self._query = query
        self.options: dict[str, str] = {}
        self.trigger_kwargs: dict[str, object] = {}
        self.to_table_name: str | None = None

    def option(self, key: str, value: str) -> "_FakeWriteStream":
        self.options[key] = value
        return self

    def trigger(self, **kwargs: object) -> "_FakeWriteStream":
        self.trigger_kwargs = kwargs
        return self

    def toTable(self, table: str) -> _FakeQuery:
        self.to_table_name = table
        return self._query


class _FakeDataFrame:
    def __init__(self, query: _FakeQuery) -> None:
        self.with_column_calls: list[str] = []
        self.writeStream = _FakeWriteStream(query)

    def withColumn(self, column_name: str, _expr: object) -> "_FakeDataFrame":
        self.with_column_calls.append(column_name)
        return self


class _FakeReadStream:
    def __init__(self, df: _FakeDataFrame) -> None:
        self._df = df
        self.format_name: str | None = None
        self.options: dict[str, str] = {}
        self.load_path: str | None = None

    def format(self, value: str) -> "_FakeReadStream":
        self.format_name = value
        return self

    def option(self, key: str, value: str) -> "_FakeReadStream":
        self.options[key] = value
        return self

    def load(self, path: str) -> _FakeDataFrame:
        self.load_path = path
        return self._df


class _FakeSparkForIngestion:
    def __init__(self, last_progress: dict[str, object] | None = None) -> None:
        self.query = _FakeQuery(last_progress)
        self.df = _FakeDataFrame(self.query)
        self.readStream = _FakeReadStream(self.df)


class _FakeSparkForSql:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> object:
        self.queries.append(query)
        return object()


def _patched_pyspark_modules() -> dict[str, types.ModuleType]:
    """
    Constrói o par de módulos pyspark/pyspark.sql para injeção em
    `sys.modules`. Centralizado para evitar repetição de `setattr` nos
    testes.
    """
    fake_sql_module = types.ModuleType("pyspark.sql")
    setattr(fake_sql_module, "functions", _FakeFunctionsModule())
    fake_pyspark_module = types.ModuleType("pyspark")
    setattr(fake_pyspark_module, "sql", fake_sql_module)
    return {
        "pyspark": fake_pyspark_module,
        "pyspark.sql": fake_sql_module,
    }


class TestBronzeMain(unittest.TestCase):
    def test_ensure_bronze_table_executes_create_and_alter(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForSql()

        bronze_main.ensure_bronze_table(cfg, "yellow", spark)

        expected_fqn = cfg.bronze_table_fqn_for("yellow")
        self.assertEqual(len(spark.queries), 2)
        self.assertIn(
            f"CREATE TABLE IF NOT EXISTS {expected_fqn}",
            spark.queries[0],
        )
        self.assertIn("USING DELTA", spark.queries[0])
        self.assertIn(f"ALTER TABLE {expected_fqn}", spark.queries[1])
        self.assertIn("SET TAGS", spark.queries[1])
        self.assertIn("'taxi_type' = 'yellow'", spark.queries[1])

    def test_ensure_bronze_table_targets_green_table_when_requested(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForSql()

        bronze_main.ensure_bronze_table(cfg, "green", spark)

        expected_fqn = cfg.bronze_table_fqn_for("green")
        self.assertTrue(expected_fqn.endswith(".green_taxi"))
        self.assertIn(f"CREATE TABLE IF NOT EXISTS {expected_fqn}", spark.queries[0])
        self.assertIn("'taxi_type' = 'green'", spark.queries[1])

    def test_run_bronze_ingestion_returns_metrics_and_configures_stream(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForIngestion(
            {
                "numInputRows": "321",
                "sources": [{"metrics": {"numFilesOutstanding": "9"}}],
            }
        )

        with patch.dict(sys.modules, _patched_pyspark_modules()):
            metrics = bronze_main.run_bronze_ingestion(cfg, "yellow", spark)

        self.assertEqual(metrics, {"rows_ingested": 321, "files_processed": 9})
        self.assertEqual(spark.readStream.format_name, "cloudFiles")
        self.assertEqual(
            spark.readStream.options["cloudFiles.schemaLocation"],
            cfg.bronze_schema_location("yellow"),
        )
        self.assertEqual(spark.readStream.load_path, cfg.landing_taxi_path("yellow"))
        self.assertIn("_source_month", spark.df.with_column_calls)
        self.assertIn("_taxi_type", spark.df.with_column_calls)
        self.assertEqual(
            spark.df.writeStream.options["checkpointLocation"],
            cfg.bronze_checkpoint_location("yellow"),
        )
        self.assertEqual(spark.df.writeStream.options["mergeSchema"], "true")
        self.assertEqual(spark.df.writeStream.trigger_kwargs, {"availableNow": True})
        self.assertEqual(
            spark.df.writeStream.to_table_name, cfg.bronze_table_fqn_for("yellow")
        )
        self.assertTrue(spark.query.await_termination_called)

    def test_run_bronze_ingestion_uses_green_specific_paths(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForIngestion(
            {
                "numInputRows": "7",
                "sources": [{"metrics": {"numFilesOutstanding": "2"}}],
            }
        )

        with patch.dict(sys.modules, _patched_pyspark_modules()):
            bronze_main.run_bronze_ingestion(cfg, "green", spark)

        self.assertEqual(spark.readStream.load_path, cfg.landing_taxi_path("green"))
        self.assertEqual(
            spark.readStream.options["cloudFiles.schemaLocation"],
            cfg.bronze_schema_location("green"),
        )
        self.assertEqual(
            spark.df.writeStream.options["checkpointLocation"],
            cfg.bronze_checkpoint_location("green"),
        )
        self.assertEqual(
            spark.df.writeStream.to_table_name, cfg.bronze_table_fqn_for("green")
        )

    def test_run_bronze_ingestion_defaults_metrics_when_last_progress_is_empty(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForIngestion(last_progress=None)

        with patch.dict(sys.modules, _patched_pyspark_modules()):
            metrics = bronze_main.run_bronze_ingestion(cfg, "yellow", spark)

        self.assertEqual(metrics, {"rows_ingested": 0, "files_processed": 0})

    def test_main_processes_both_taxis_by_default(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                catalog="nyc_taxi_dev",
                environment="dev",
                taxi_type="both",
            )

        ensure_calls: list[str] = []
        run_calls: list[str] = []

        def fake_ensure(_cfg: object, taxi_type: str, _spark: object) -> None:
            ensure_calls.append(taxi_type)

        def fake_run(_cfg: object, taxi_type: str, _spark: object) -> dict[str, int]:
            run_calls.append(taxi_type)
            return {"rows_ingested": 10 if taxi_type == "yellow" else 5, "files_processed": 1}

        with (
            patch("nyc_taxi.lakehouse.bronze.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.bronze.main._get_spark", return_value=object()),
            patch(
                "nyc_taxi.lakehouse.bronze.main.ensure_bronze_table",
                side_effect=fake_ensure,
            ),
            patch(
                "nyc_taxi.lakehouse.bronze.main.run_bronze_ingestion",
                side_effect=fake_run,
            ),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = bronze_main.main([])

        self.assertEqual(exit_code, 0)
        self.assertEqual(ensure_calls, ["yellow", "green"])
        self.assertEqual(run_calls, ["yellow", "green"])
        payload = captured.getvalue().strip()
        self.assertIn('"rows_ingested": 15', payload)
        self.assertIn('"yellow"', payload)
        self.assertIn('"green"', payload)

    def test_main_returns_one_when_all_taxi_types_fail(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                catalog="nyc_taxi_dev",
                environment="dev",
                taxi_type="both",
            )

        def fake_run(_cfg: object, _taxi_type: str, _spark: object) -> dict[str, int]:
            raise RuntimeError("boom")

        with (
            patch("nyc_taxi.lakehouse.bronze.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.bronze.main._get_spark", return_value=object()),
            patch("nyc_taxi.lakehouse.bronze.main.ensure_bronze_table"),
            patch(
                "nyc_taxi.lakehouse.bronze.main.run_bronze_ingestion",
                side_effect=fake_run,
            ),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = bronze_main.main([])

        self.assertEqual(exit_code, 1)
        payload = captured.getvalue().strip()
        self.assertIn('"status": "failed"', payload)

    def test_main_returns_zero_on_partial_failure(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                catalog="nyc_taxi_dev",
                environment="dev",
                taxi_type="both",
            )

        def fake_run(_cfg: object, taxi_type: str, _spark: object) -> dict[str, int]:
            if taxi_type == "green":
                raise RuntimeError("green boom")
            return {"rows_ingested": 42, "files_processed": 3}

        with (
            patch("nyc_taxi.lakehouse.bronze.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.bronze.main._get_spark", return_value=object()),
            patch("nyc_taxi.lakehouse.bronze.main.ensure_bronze_table"),
            patch(
                "nyc_taxi.lakehouse.bronze.main.run_bronze_ingestion",
                side_effect=fake_run,
            ),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = bronze_main.main([])

        # Partial failure mantém exit 0, alinhado com a política da landing.
        self.assertEqual(exit_code, 0)
        payload = captured.getvalue().strip()
        self.assertIn('"rows_ingested": 42', payload)
        self.assertIn('"status": "ok"', payload)
        self.assertIn('"status": "failed"', payload)


if __name__ == "__main__":
    unittest.main()
