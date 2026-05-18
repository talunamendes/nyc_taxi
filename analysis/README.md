# analysis/

Scripts SQL/PySpark com consultas analiticas sobre o dataset NYC Taxi.

## Conteudo

- [`perguntas_analiticas.sql`](./perguntas_analiticas.sql) — respostas em SQL
  puro para as perguntas analiticas suportadas pelo pipeline. Cada pergunta
  e' uma consulta canonica que le da view de consumo (`gold.vw_taxi_trips`).

## Como executar

As consultas rodam diretamente no SQL Editor do Databricks, em qualquer
cluster com acesso ao Unity Catalog onde o job `nyc_taxi_job` foi
implantado. Substitua `<catalog>` pelo nome do catalogo em uso
(default: `nyc_taxi_dev`).

Pre-condicoes: o pipeline `nyc_taxi_job` precisa ter rodado com sucesso
pelo menos uma vez, materializando:

- `<catalog>.silver.yellow_taxi_trips`
- `<catalog>.silver.green_taxi_trips`
- `<catalog>.gold.vw_taxi_trips`

## Perguntas analiticas

### Pergunta A
Qual a media de valor total (`total_amount`) recebido em um mes
considerando todos os yellow taxis da frota?

### Pergunta B
Qual a media de passageiros (`passenger_count`) por cada hora do dia
que pegaram taxi no mes de maio considerando todos os taxis da frota?

## Como interpretar

A gold publica apenas a view `vw_taxi_trips`. As perguntas analiticas
sao respondidas por SQL ad-hoc aqui — sem fatos pre-agregados. A
justificativa formal dessa decisao (e os gatilhos para promover
metricas a fatos no futuro) esta em
[`docs/adr/ADR-014-gold-data-model-per-question.md`](../docs/adr/ADR-014-gold-data-model-per-question.md).

Detalhes de implementacao em
[`docs/layers/gold.md`](../docs/layers/gold.md).
