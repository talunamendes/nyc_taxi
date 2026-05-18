from __future__ import annotations

import io
import pathlib
import sys
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

# Ensure local src/ layout is importable in test runs.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi.core.config import PipelineConfig
from nyc_taxi.lakehouse.gold import main as gold_main


class _FakeSparkForSql:
    """Spark fake que so registra `spark.sql(...)` — suficiente para
    validar a DDL emitida por `ensure_trips_consumption_view`."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query: str) -> object:
        self.queries.append(query)
        return object()


class TestEnsureTripsConsumptionView(unittest.TestCase):
    """
    `ensure_trips_consumption_view` deve criar a view de consumo com as
    5 colunas obrigatorias do contrato (`_CONSUMPTION_REQUIRED_COLUMNS`),
    unindo yellow + green e aliasando `lpep_*` -> `tpep_*` na metade green.

    Esta e a unica responsabilidade publicada pela gold (ADR-014).
    """

    def test_view_exposes_required_columns_and_unions_taxis(self) -> None:
        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        spark = _FakeSparkForSql()

        gold_main.ensure_trips_consumption_view(cfg, spark)

        # Apenas um statement: CREATE OR REPLACE VIEW (idempotente).
        self.assertEqual(len(spark.queries), 1)
        ddl = spark.queries[0]

        # FQN alvo correto.
        self.assertIn(f"CREATE OR REPLACE VIEW {cfg.gold_trips_view_fqn}", ddl)

        # Todas as 5 colunas obrigatorias do contrato aparecem na
        # assinatura da view (e nao apenas no SELECT, garantindo contrato
        # visivel).
        for col in gold_main._CONSUMPTION_REQUIRED_COLUMNS:
            self.assertIn(col, ddl)

        # Une yellow + green (UNION ALL preserva duplicatas legitimas
        # entre datasets fisicamente segregados).
        self.assertIn(cfg.silver_table_fqn_for("yellow"), ddl)
        self.assertIn(cfg.silver_table_fqn_for("green"), ddl)
        self.assertIn("UNION ALL", ddl)

        # Alias lpep_* -> tpep_* aplicado SOMENTE na metade green do
        # UNION (yellow ja publica `tpep_*` nativo, ADR-013).
        self.assertIn("lpep_pickup_datetime  AS tpep_pickup_datetime", ddl)
        self.assertIn("lpep_dropoff_datetime AS tpep_dropoff_datetime", ddl)

        # Linhagem por linha: `taxi_type` literal.
        self.assertIn("'yellow' AS taxi_type", ddl)
        self.assertIn("'green' AS taxi_type", ddl)


class TestConsumptionRequiredColumnsContract(unittest.TestCase):
    """
    `_CONSUMPTION_REQUIRED_COLUMNS` e' a fonte unica de verdade do
    contrato da view de consumo. Se alguem renomear/remover uma coluna
    sem atualizar essa tupla, este teste pega.
    """

    def test_required_columns_match_consumption_contract(self) -> None:
        # Conjunto literal das colunas obrigatorias do contrato.
        expected = {
            "VendorID",
            "passenger_count",
            "total_amount",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        }
        self.assertEqual(set(gold_main._CONSUMPTION_REQUIRED_COLUMNS), expected)


class TestMain(unittest.TestCase):
    """
    `main` publica APENAS a view (ADR-014). Sem fatos pre-agregados,
    sem overwrite. Falha = exit nao-zero (fail-fast).
    """

    def test_main_publishes_view_and_returns_zero(self) -> None:
        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(catalog="nyc_taxi_dev", environment="dev")

        ensure_called: list[bool] = []

        def fake_ensure_view(_cfg: object, _spark: object) -> None:
            ensure_called.append(True)

        with (
            patch("nyc_taxi.lakehouse.gold.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.gold.main._get_spark", return_value=object()),
            patch(
                "nyc_taxi.lakehouse.gold.main.ensure_trips_consumption_view",
                side_effect=fake_ensure_view,
            ),
            redirect_stdout(io.StringIO()) as captured,
        ):
            exit_code = gold_main.main([])

        cfg = PipelineConfig(catalog="nyc_taxi_dev", environment="dev")
        self.assertEqual(exit_code, 0)
        self.assertEqual(ensure_called, [True])

        payload = captured.getvalue().strip()
        self.assertIn(cfg.gold_trips_view_fqn, payload)
        self.assertIn('"status": "ok"', payload)

    def test_main_fails_fast_when_view_creation_fails(self) -> None:
        """Gold e camada de contrato: falha na publicacao da view aborta
        o job inteiro (raise propaga)."""

        def fake_parse_args(_argv: Any) -> SimpleNamespace:
            return SimpleNamespace(catalog="nyc_taxi_dev", environment="dev")

        def fake_ensure_view(_cfg: object, _spark: object) -> None:
            raise RuntimeError("view boom")

        with (
            patch("nyc_taxi.lakehouse.gold.main._parse_args", fake_parse_args),
            patch("nyc_taxi.lakehouse.gold.main._get_spark", return_value=object()),
            patch(
                "nyc_taxi.lakehouse.gold.main.ensure_trips_consumption_view",
                side_effect=fake_ensure_view,
            ),
            self.assertRaises(RuntimeError),
        ):
            gold_main.main([])


if __name__ == "__main__":
    unittest.main()
