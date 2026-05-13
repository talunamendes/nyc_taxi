# NYC Taxi

## Arquitetura

```mermaid
flowchart LR
  A[NYC TLC Trip Data CDN<br/>Yellow Taxi 2023-01..05] --> B[Landing<br/>UC Volume]
  B --> C[Bronze]
  C --> D[Silver]
  D --> E[Gold / Camada de Consumo]

  F[Declarative Automation Bundles<br/>validate/deploy/run/destroy] --> G[Databricks Job Serverless]
  G --> B
  G --> C
  G --> D
  G --> E
```

## TL;DR

- Esta solução ingere dados de corridas Yellow Taxi (jan-mai/2023) para um lakehouse em Databricks.
- O pipeline é organizado em camadas (`landing`, `bronze`, `silver`, `gold`) e empacotado como wheel Python.
- A orquestração/deploy usa Declarative Automation Bundles (DAB) (`databricks bundle`) com execução em Serverless Jobs.
- O projeto inclui scripts DDL para bootstrap de catálogo, schemas e volumes no Unity Catalog.

## Quickstart (Databricks Free Edition)

Pré-requisitos: Databricks CLI autenticado (`databricks auth login`) e `uv` instalado.

```bash
make uv-build
make dab-deploy ENV=dev CATALOG=nyc_taxi_dev
make dab-run ENV=dev WORKFLOW=nyc_taxi_job CATALOG=nyc_taxi_dev
```

Se precisar criar objetos do Unity Catalog antes do deploy, use:

- `docs/sql/001_create_catalog.sql`
- `docs/sql/002_create_schemas.sql`
- `docs/sql/003_create_volumes.sql`

Guia detalhado: `docs/FREE_EDITION_SETUP.md`.
Documentação oficial: [Databricks Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/).

## Resposta Direta às Perguntas do Case

### 1) Solução de ingestão e disponibilização para usuário final

- **Ingestão**: implementada em `src/nyc_taxi/lakehouse/landing/main.py` (download, idempotência e metadados).
- **Uso de PySpark**: previsto no pipeline lakehouse e estrutura de execução em Databricks Jobs.
- **Metadados**: Unity Catalog (catálogo, schemas e volumes).
- **Camada de consumo**: prevista na camada `gold` (`src/nyc_taxi/lakehouse/gold/main.py`).
- **Modelagem inicial de tabelas/objetos**: bootstrap com DDL em `docs/sql`.
- **Colunas obrigatórias na camada de consumo**:
  - `VendorID`
  - `passenger_count`
  - `total_amount`
  - `tpep_pickup_datetime`
  - `tpep_dropoff_datetime`

### 2) Perguntas analíticas do case (SQL de referência)

**Pergunta A**: média de `total_amount` por mês (frota Yellow).

```sql
SELECT
  date_trunc('month', tpep_pickup_datetime) AS month_ref,
  AVG(total_amount) AS avg_total_amount
FROM <catalog>.gold.<tabela_consumo>
GROUP BY 1
ORDER BY 1;
```

**Pergunta B**: média de `passenger_count` por hora do dia em maio/2023.

```sql
SELECT
  hour(tpep_pickup_datetime) AS hour_of_day,
  AVG(passenger_count) AS avg_passenger_count
FROM <catalog>.gold.<tabela_consumo>
WHERE tpep_pickup_datetime >= TIMESTAMP('2023-05-01')
  AND tpep_pickup_datetime < TIMESTAMP('2023-06-01')
GROUP BY 1
ORDER BY 1;
```

## Artefatos de Decisão e Governança

- **ADRs**: `docs/adr/README.md`
- **Threat Model**: `docs/threat-model.md`
- **TCO Model**: `docs/tco-model.md`

## Estrutura do Repositório

Esta organização segue a estrutura base gerada pelo template `default-python` do Declarative Automation Bundles (DAB), com extensões específicas para o case.

- `src/nyc_taxi/`: código fonte do pipeline
- `resources/`: definição de workflow/job com Declarative Automation Bundles (DAB)
- `tests/`: testes unitários
- `docs/adr/`: registros de decisões arquiteturais
- `docs/sql/`: DDL de bootstrap
