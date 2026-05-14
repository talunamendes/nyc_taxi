from __future__ import annotations

import io
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

# Ensure local src/ layout is importable in test runs.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi.lakehouse.landing import main as landing_main


class _FakeEntry:
    def __init__(self, size: int) -> None:
        self.size = size


class _FakeDbutilsFs:
    def __init__(self, entries: list[_FakeEntry], should_raise: bool = False) -> None:
        self._entries = entries
        self._should_raise = should_raise

    def ls(self, _path: str) -> list[_FakeEntry]:
        if self._should_raise:
            raise RuntimeError("path not found")
        return self._entries


class _FakeDbutils:
    def __init__(self, fs: _FakeDbutilsFs) -> None:
        self.fs = fs


class TestLandingMain(unittest.TestCase):
    def test_compute_md5_known_content(self) -> None:
        with tempfile.NamedTemporaryFile("wb", delete=True) as tmp:
            tmp.write(b"abc")
            tmp.flush()
            self.assertEqual(
                landing_main.compute_md5(tmp.name),
                "900150983cd24fb0d6963f7d28e17f72",
            )

    def test_file_already_ingested_returns_true_when_any_file_is_large_enough(self) -> None:
        dbutils = _FakeDbutils(_FakeDbutilsFs([_FakeEntry(1000), _FakeEntry(12_000_000)]))
        self.assertTrue(landing_main.file_already_ingested("/Volumes/x", dbutils))  # type: ignore[arg-type]

    def test_file_already_ingested_returns_false_when_ls_fails(self) -> None:
        dbutils = _FakeDbutils(_FakeDbutilsFs([], should_raise=True))
        self.assertFalse(landing_main.file_already_ingested("/Volumes/x", dbutils))  # type: ignore[arg-type]

    def test_main_returns_zero_when_at_least_one_month_succeeds(self) -> None:
        calls: list[int] = []

        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                target_year=2023,
                target_months="1,2,3",
                discover=False,
                discover_from=None,
                catalog="nyc_taxi_dev",
                environment="dev",
            )

        def fake_get_spark_and_dbutils() -> tuple[object, object]:
            return object(), object()

        def fake_ingest_month(
            _year: int, month: int, _cfg: object, _dbutils: object
        ) -> dict[str, str]:
            calls.append(month)
            if month == 2:
                raise RuntimeError("error month 2")
            if month == 1:
                return {"status": "ingested"}
            return {"status": "skipped"}

        with (
            patch("nyc_taxi.lakehouse.landing.main._parse_args", fake_parse_args),
            patch(
                "nyc_taxi.lakehouse.landing.main._get_spark_and_dbutils",
                fake_get_spark_and_dbutils,
            ),
            patch("nyc_taxi.lakehouse.landing.main.ingest_month", fake_ingest_month),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = landing_main.main([])

        self.assertEqual(exit_code, 0)
        self.assertEqual(calls, [1, 2, 3])
        self.assertEqual(
            captured.getvalue().strip(),
            '{"ingested": 1, "skipped": 1, "failed": 1}',
        )

    def test_main_returns_one_when_all_months_fail(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                target_year=2023,
                target_months="4,5",
                discover=False,
                discover_from=None,
                catalog="nyc_taxi_dev",
                environment="dev",
            )

        def fake_get_spark_and_dbutils() -> tuple[object, object]:
            return object(), object()

        def fake_ingest_month(
            _year: int, _month: int, _cfg: object, _dbutils: object
        ) -> dict[str, str]:
            raise RuntimeError("all failed")

        with (
            patch("nyc_taxi.lakehouse.landing.main._parse_args", fake_parse_args),
            patch(
                "nyc_taxi.lakehouse.landing.main._get_spark_and_dbutils",
                fake_get_spark_and_dbutils,
            ),
            patch("nyc_taxi.lakehouse.landing.main.ingest_month", fake_ingest_month),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = landing_main.main([])

        self.assertEqual(exit_code, 1)
        self.assertEqual(
            captured.getvalue().strip(),
            '{"ingested": 0, "skipped": 0, "failed": 2}',
        )

    def test_main_discover_returns_zero_when_target_window_is_empty(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                target_year=None,
                target_months=None,
                discover=True,
                discover_from="2023-01",
                catalog="nyc_taxi_dev",
                environment="dev",
            )

        def fake_get_spark_and_dbutils() -> tuple[object, object]:
            return object(), object()

        with (
            patch("nyc_taxi.lakehouse.landing.main._parse_args", fake_parse_args),
            patch(
                "nyc_taxi.lakehouse.landing.main._get_spark_and_dbutils",
                fake_get_spark_and_dbutils,
            ),
            patch(
                "nyc_taxi.lakehouse.landing.main._resolve_target_window",
                return_value=[],
            ),
            patch(
                "nyc_taxi.lakehouse.landing.main.ingest_month",
                side_effect=AssertionError("ingest_month should not be called"),
            ),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = landing_main.main([])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            captured.getvalue().strip(),
            '{"ingested": 0, "skipped": 0, "failed": 0, "nothing_to_do": true}',
        )


if __name__ == "__main__":
    unittest.main()
