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


class TestBronzeMain(unittest.TestCase):
    def test_ensure_bronze_table_executes_create_and_alter(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForSql()

        bronze_main.ensure_bronze_table(cfg, spark)

        self.assertEqual(len(spark.queries), 2)
        self.assertIn(
            f"CREATE TABLE IF NOT EXISTS {cfg.bronze_table_fqn}",
            spark.queries[0],
        )
        self.assertIn("USING DELTA", spark.queries[0])
        self.assertIn(f"ALTER TABLE {cfg.bronze_table_fqn}", spark.queries[1])
        self.assertIn("SET TAGS", spark.queries[1])

    def test_run_bronze_ingestion_returns_metrics_and_configures_stream(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForIngestion(
            {
                "numInputRows": "321",
                "sources": [{"metrics": {"numFilesOutstanding": "9"}}],
            }
        )

        fake_sql_module = types.ModuleType("pyspark.sql")
        setattr(fake_sql_module, "functions", _FakeFunctionsModule())
        fake_pyspark_module = types.ModuleType("pyspark")
        setattr(fake_pyspark_module, "sql", fake_sql_module)

        with patch.dict(
            sys.modules,
            {
                "pyspark": fake_pyspark_module,
                "pyspark.sql": fake_sql_module,
            },
        ):
            metrics = bronze_main.run_bronze_ingestion(cfg, spark)

        self.assertEqual(metrics, {"rows_ingested": 321, "files_processed": 9})
        self.assertEqual(spark.readStream.format_name, "cloudFiles")
        self.assertEqual(
            spark.readStream.options["cloudFiles.schemaLocation"],
            f"{cfg.schemas_volume_path}/bronze_yellow_trips",
        )
        self.assertEqual(spark.readStream.load_path, cfg.landing_volume_path)
        self.assertIn("_source_month", spark.df.with_column_calls)
        self.assertEqual(
            spark.df.writeStream.options["checkpointLocation"],
            f"{cfg.checkpoints_volume_path}/bronze_yellow_trips",
        )
        self.assertEqual(spark.df.writeStream.options["mergeSchema"], "true")
        self.assertEqual(spark.df.writeStream.trigger_kwargs, {"availableNow": True})
        self.assertEqual(spark.df.writeStream.to_table_name, cfg.bronze_table_fqn)
        self.assertTrue(spark.query.await_termination_called)

    def test_run_bronze_ingestion_defaults_metrics_when_last_progress_is_empty(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForIngestion(last_progress=None)

        fake_sql_module = types.ModuleType("pyspark.sql")
        setattr(fake_sql_module, "functions", _FakeFunctionsModule())
        fake_pyspark_module = types.ModuleType("pyspark")
        setattr(fake_pyspark_module, "sql", fake_sql_module)

        with patch.dict(
            sys.modules,
            {
                "pyspark": fake_pyspark_module,
                "pyspark.sql": fake_sql_module,
            },
        ):
            metrics = bronze_main.run_bronze_ingestion(cfg, spark)

        self.assertEqual(metrics, {"rows_ingested": 0, "files_processed": 0})

    def test_main_orchestrates_and_prints_metrics(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(catalog="nyc_taxi_dev", environment="dev")

        fake_spark = object()

        with (
            patch("nyc_taxi.lakehouse.bronze.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.bronze.main._get_spark", return_value=fake_spark),
            patch("nyc_taxi.lakehouse.bronze.main.ensure_bronze_table") as ensure_mock,
            patch(
                "nyc_taxi.lakehouse.bronze.main.run_bronze_ingestion",
                return_value={"rows_ingested": 123, "files_processed": 4},
            ) as run_mock,
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = bronze_main.main([])

        self.assertEqual(exit_code, 0)
        ensure_mock.assert_called_once()
        run_mock.assert_called_once()
        self.assertEqual(
            captured.getvalue().strip(),
            '{"rows_ingested": 123, "files_processed": 4}',
        )


if __name__ == "__main__":
    unittest.main()
