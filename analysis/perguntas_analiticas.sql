-- Respostas as perguntas analiticas suportadas pelo pipeline NYC Taxi.
--
-- Pre-condicoes: o job `nyc_taxi_job` foi executado pelo menos uma vez,
-- materializando as silvers e a view de consumo da gold:
--
--   <catalog>.silver.yellow_taxi_trips
--   <catalog>.silver.green_taxi_trips
--   <catalog>.gold.vw_taxi_trips              -- view com as colunas
--                                                obrigatorias do contrato
--                                                de consumo
--
-- Substitua `<catalog>` pelo Unity Catalog em uso (default: `nyc_taxi_dev`).
--
-- A gold publica apenas a view. As perguntas analiticas sao respondidas
-- aqui, por SQL ad-hoc rodando contra a view. Decisao detalhada em
-- docs/adr/ADR-014-gold-data-model-per-question.md.

-- ============================================================================
-- Contrato de consumo
--    A view `gold.vw_taxi_trips` garante que as colunas VendorID,
--    passenger_count, total_amount, tpep_pickup_datetime e
--    tpep_dropoff_datetime estejam presentes na camada de consumo,
--    unindo yellow + green com alias `lpep_*` -> `tpep_*`. `taxi_type`
--    extra preserva a origem.
-- ============================================================================

SELECT
    VendorID,
    passenger_count,
    total_amount,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    taxi_type
FROM <catalog>.gold.vw_taxi_trips
WHERE tpep_pickup_datetime >= TIMESTAMP('2023-01-01')
  AND tpep_pickup_datetime <  TIMESTAMP('2023-06-01')
LIMIT 100;


-- ============================================================================
-- Pergunta A
--    "Qual a media de valor total (total_amount) recebido em um mes
--    considerando todos os yellow taxis da frota?"
-- ============================================================================

SELECT
    date_trunc('month', tpep_pickup_datetime) AS month_ref,
    AVG(total_amount)                          AS avg_total_amount,
    COUNT(*)                                   AS trips_count
FROM <catalog>.gold.vw_taxi_trips
WHERE taxi_type = 'yellow'
GROUP BY 1
ORDER BY 1;


-- ============================================================================
-- Pergunta B
--    "Qual a media de passageiros (passenger_count) por cada hora do dia
--    que pegaram taxi no mes de maio considerando todos os taxis da frota?"
-- ============================================================================

SELECT
    hour(tpep_pickup_datetime) AS hour_of_day,
    AVG(passenger_count)       AS avg_passenger_count,
    COUNT(*)                   AS trips_count
FROM <catalog>.gold.vw_taxi_trips
WHERE tpep_pickup_datetime >= TIMESTAMP('2023-05-01')
  AND tpep_pickup_datetime <  TIMESTAMP('2023-06-01')
  AND passenger_count IS NOT NULL
GROUP BY 1
ORDER BY 1;
