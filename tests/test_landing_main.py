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
        # Yellow tem ~50MB/mês — o piso de 10MB cobre o caso típico.
        dbutils = _FakeDbutils(_FakeDbutilsFs([_FakeEntry(1000), _FakeEntry(12_000_000)]))
        self.assertTrue(
            landing_main.file_already_ingested(
                "/Volumes/x",
                dbutils,  # type: ignore[arg-type]
                expected_size_min=landing_main._MIN_PARQUET_SIZE_BYTES["yellow"],
            )
        )

    def test_file_already_ingested_uses_lower_threshold_for_green(self) -> None:
        # Green publica ~1–2MB/mês. Com o piso default do yellow (10MB),
        # esses arquivos seriam tratados como ausentes e rebaixados a
        # cada execução. O piso de green precisa permitir o tamanho real.
        dbutils = _FakeDbutils(_FakeDbutilsFs([_FakeEntry(1_400_000)]))
        self.assertTrue(
            landing_main.file_already_ingested(
                "/Volumes/x",
                dbutils,  # type: ignore[arg-type]
                expected_size_min=landing_main._MIN_PARQUET_SIZE_BYTES["green"],
            )
        )

    def test_file_already_ingested_default_threshold_skips_green_sized_files(self) -> None:
        # Sanity check: o default do callsite continua plausível —
        # arquivos abaixo de 500KB são considerados truncados/ausentes.
        dbutils = _FakeDbutils(_FakeDbutilsFs([_FakeEntry(100_000)]))
        self.assertFalse(landing_main.file_already_ingested("/Volumes/x", dbutils))  # type: ignore[arg-type]

    def test_file_already_ingested_returns_false_when_ls_fails(self) -> None:
        dbutils = _FakeDbutils(_FakeDbutilsFs([], should_raise=True))
        self.assertFalse(landing_main.file_already_ingested("/Volumes/x", dbutils))  # type: ignore[arg-type]

    def test_min_parquet_size_bytes_has_an_entry_per_supported_taxi(self) -> None:
        # Garante que o map per-taxi não saia do passo com SUPPORTED_TAXI_TYPES.
        # Se um taxi novo for adicionado, este teste falha pedindo o piso correto.
        from nyc_taxi.core.config import SUPPORTED_TAXI_TYPES

        self.assertEqual(
            set(landing_main._MIN_PARQUET_SIZE_BYTES.keys()),
            set(SUPPORTED_TAXI_TYPES),
        )

    def test_main_returns_zero_when_at_least_one_month_succeeds(self) -> None:
        calls: list[tuple[str, int]] = []

        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                target_year=2023,
                target_months="1,2,3",
                discover=False,
                discover_from=None,
                taxi_type="yellow",
                catalog="nyc_taxi_dev",
                environment="dev",
            )

        def fake_get_spark_and_dbutils() -> tuple[object, object]:
            return object(), object()

        def fake_ingest_month(
            taxi_type: str,
            _year: int,
            month: int,
            _cfg: object,
            _dbutils: object,
        ) -> dict[str, Any]:
            calls.append((taxi_type, month))
            if month == 2:
                raise RuntimeError("error month 2")
            if month == 1:
                return {"status": "ingested", "taxi_type": taxi_type}
            return {"status": "skipped", "taxi_type": taxi_type}

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
        self.assertEqual(calls, [("yellow", 1), ("yellow", 2), ("yellow", 3)])
        payload = captured.getvalue().strip()
        self.assertIn('"ingested": 1', payload)
        self.assertIn('"skipped": 1', payload)
        self.assertIn('"failed": 1', payload)
        self.assertIn('"per_taxi"', payload)

    def test_main_iterates_over_both_taxis_when_taxi_type_is_both(self) -> None:
        calls: list[tuple[str, int]] = []

        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                target_year=2023,
                target_months="1",
                discover=False,
                discover_from=None,
                taxi_type="both",
                catalog="nyc_taxi_dev",
                environment="dev",
            )

        def fake_get_spark_and_dbutils() -> tuple[object, object]:
            return object(), object()

        def fake_ingest_month(
            taxi_type: str,
            _year: int,
            month: int,
            _cfg: object,
            _dbutils: object,
        ) -> dict[str, Any]:
            calls.append((taxi_type, month))
            return {"status": "ingested", "taxi_type": taxi_type}

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
        self.assertEqual(calls, [("yellow", 1), ("green", 1)])
        payload = captured.getvalue().strip()
        self.assertIn('"ingested": 2', payload)
        self.assertIn('"yellow"', payload)
        self.assertIn('"green"', payload)

    def test_main_returns_one_when_all_months_fail(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                target_year=2023,
                target_months="4,5",
                discover=False,
                discover_from=None,
                taxi_type="yellow",
                catalog="nyc_taxi_dev",
                environment="dev",
            )

        def fake_get_spark_and_dbutils() -> tuple[object, object]:
            return object(), object()

        def fake_ingest_month(
            _taxi_type: str,
            _year: int,
            _month: int,
            _cfg: object,
            _dbutils: object,
        ) -> dict[str, Any]:
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
        payload = captured.getvalue().strip()
        self.assertIn('"ingested": 0', payload)
        self.assertIn('"skipped": 0', payload)
        self.assertIn('"failed": 2', payload)

    def test_main_discover_returns_zero_when_target_window_is_empty(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(
                target_year=None,
                target_months=None,
                discover=True,
                discover_from="2023-01",
                taxi_type="yellow",
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


class _StubDbutilsFs:
    """Fs stub que retorna o mesmo conjunto de entries para qualquer
    `ls(path)` — suficiente para validar a heurística de idempotência
    em `ingest_month`."""

    def __init__(self, entries: list[_FakeEntry]) -> None:
        self._entries = entries

    def ls(self, _path: str) -> list[_FakeEntry]:
        return self._entries

    def mkdirs(self, _path: str) -> None:  # pragma: no cover - não esperado
        raise AssertionError("mkdirs should not be called when file is already ingested")

    def put(self, _path: str, _contents: str, **_kwargs: Any) -> None:  # pragma: no cover
        raise AssertionError("put should not be called when file is already ingested")


class TestIngestMonthIdempotency(unittest.TestCase):
    """
    Garante que green é tratado igual a yellow no skip de re-download:
    com o piso correto por taxi (`_MIN_PARQUET_SIZE_BYTES`), arquivos
    já presentes no volume não são rebaixados.
    """

    def _run(self, taxi_type: str, file_size: int) -> dict[str, Any]:
        from nyc_taxi.core.config import PipelineConfig

        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        dbutils = _FakeDbutils(_StubDbutilsFs([_FakeEntry(file_size)]))

        download_calls: list[str] = []

        def fake_download(url: str, _dest: str, _cfg: object) -> None:
            download_calls.append(url)

        with patch(
            "nyc_taxi.lakehouse.landing.main.download_with_retry",
            side_effect=fake_download,
        ):
            result = landing_main.ingest_month(
                taxi_type, 2023, 1, cfg, dbutils,  # type: ignore[arg-type]
            )

        result["_download_calls"] = download_calls  # type: ignore[assignment]
        return result

    def test_yellow_skips_when_existing_file_is_above_yellow_threshold(self) -> None:
        # 12MB > piso de 10MB do yellow → skip.
        result = self._run("yellow", file_size=12_000_000)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["_download_calls"], [])

    def test_green_skips_when_existing_file_is_above_green_threshold(self) -> None:
        # 1.4MB > piso de 500KB do green → skip. Antes do fix esse caso
        # rebaixava porque o piso comum era 10MB.
        result = self._run("green", file_size=1_400_000)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["_download_calls"], [])


if __name__ == "__main__":
    unittest.main()
