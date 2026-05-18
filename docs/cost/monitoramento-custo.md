# Monitoramento de Custo com System Tables

O Databricks expõe dados de billing via system tables, permitindo calcular e monitorar o custo real de compute serverless e de jobs sem depender de estimativas manuais. As tabelas principais são:

- `system.billing.usage` — registro granular de consumo de DBUs por workload.
- `system.billing.list_prices` — preços de lista por SKU e período, usados para converter DBUs em custo monetário.
- `system.lakeflow.jobs` — metadados dos jobs (nome, estado mais recente).
- `system.lakeflow.job_run_timeline` — histórico de execuções com estado (`SUCCEEDED`, `FAILED`, `TIMED_OUT` etc.) e períodos de início/fim.

> **Requisito**: o usuário precisa ser metastore admin + account admin, ou ter permissões `USE` e `SELECT` nos schemas de sistema.

> **Escopo regional**: as queries retornam dados apenas de workspaces na mesma região cloud. Para monitorar outras regiões, execute a query no workspace da região correspondente.

### Custo de Compute Serverless

O SKU de serverless é identificado pelo filtro `sku_name LIKE '%SERVERLESS%'`. O campo `billing_origin_product` distingue o tipo de workload: `JOBS` para jobs serverless e `INTERACTIVE` para notebooks.

**Jobs serverless com maior consumo de DBUs (últimos 30 dias):**

```sql
SELECT
  usage_metadata.job_id,
  usage_metadata.job_name,
  SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE
  usage_metadata.job_id IS NOT NULL
  AND usage_unit = 'DBU'
  AND usage_date >= DATEADD(day, -30, current_date)
  AND sku_name LIKE '%JOBS_SERVERLESS_COMPUTE%'
GROUP BY 1, 2
ORDER BY total_dbu DESC
```

**Custo monetário por workspace (últimos 30 dias):**

```sql
SELECT
  t1.workspace_id,
  SUM(t1.usage_quantity * list_prices.pricing.default) AS list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices ON
  t1.cloud = list_prices.cloud AND
  t1.sku_name = list_prices.sku_name AND
  t1.usage_start_time >= list_prices.price_start_time AND
  (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
WHERE
  t1.sku_name LIKE '%SERVERLESS%'
  AND billing_origin_product IN ('JOBS', 'INTERACTIVE')
  AND t1.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY t1.workspace_id
```

> **Nota**: a arquitetura distribuída do serverless pode gerar múltiplos registros por execução com o mesmo `job_id`/`job_run_id`. O custo real é sempre a **soma** dos registros.

### Custo de Jobs (Serverless e Jobs Compute)

As queries abaixo cobrem jobs executados em **jobs compute** e **serverless compute**. Jobs em SQL warehouses ou all-purpose compute não são contabilizados como `billing_origin_product = 'JOBS'`.

**Jobs mais caros (últimos 30 dias):**

```sql
WITH list_cost_per_job AS (
  SELECT
    t1.workspace_id,
    t1.usage_metadata.job_id,
    COUNT(DISTINCT t1.usage_metadata.job_run_id) AS runs,
    SUM(t1.usage_quantity * list_prices.pricing.default) AS list_cost,
    FIRST(identity_metadata.run_as, TRUE) AS run_as,
    MAX(t1.usage_end_time) AS last_seen_date
  FROM system.billing.usage t1
  INNER JOIN system.billing.list_prices ON
    t1.cloud = list_prices.cloud AND
    t1.sku_name = list_prices.sku_name AND
    t1.usage_start_time >= list_prices.price_start_time AND
    (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  WHERE
    t1.billing_origin_product = 'JOBS'
    AND t1.usage_date >= CURRENT_DATE() - INTERVAL 30 DAY
  GROUP BY ALL
),
most_recent_jobs AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs QUALIFY rn = 1
)
SELECT
  t2.name,
  t1.job_id,
  t1.workspace_id,
  t1.runs,
  t1.run_as,
  SUM(t1.list_cost) AS list_cost,
  t1.last_seen_date
FROM list_cost_per_job t1
LEFT JOIN most_recent_jobs t2 USING (workspace_id, job_id)
GROUP BY ALL
ORDER BY list_cost DESC
```

**Análise de tendência de gasto (semana atual vs. semana anterior):**

```sql
WITH job_run_timeline_with_cost AS (
  SELECT
    t1.*,
    t1.usage_metadata.job_id AS job_id,
    t1.usage_quantity * list_prices.pricing.default AS list_cost
  FROM system.billing.usage t1
  INNER JOIN system.billing.list_prices ON
    t1.cloud = list_prices.cloud AND
    t1.sku_name = list_prices.sku_name AND
    t1.usage_start_time >= list_prices.price_start_time AND
    (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
  WHERE
    t1.billing_origin_product = 'JOBS'
    AND t1.usage_date >= CURRENT_DATE() - INTERVAL 14 DAY
)
SELECT
  job_id,
  workspace_id,
  sku_name,
  SUM(CASE WHEN usage_end_time BETWEEN date_add(current_date(), -8) AND date_add(current_date(), -1) THEN list_cost ELSE 0 END) AS last_7_day_spend,
  SUM(CASE WHEN usage_end_time BETWEEN date_add(current_date(), -15) AND date_add(current_date(), -8) THEN list_cost ELSE 0 END) AS prev_7_day_spend
FROM job_run_timeline_with_cost
GROUP BY ALL
ORDER BY (last_7_day_spend - prev_7_day_spend) DESC
```

### Saúde Operacional: Falhas e Reprocessamentos

Erros e retries têm custo direto — o job consome DBUs mesmo quando falha. As queries abaixo permitem quantificar esse desperdício.

**Jobs com maior número de falhas e custo associado (últimos 30 dias):**

```sql
WITH terminal_statuses AS (
  SELECT
    workspace_id,
    job_id,
    CASE WHEN result_state IN ('ERROR', 'FAILED', 'TIMED_OUT') THEN 1 ELSE 0 END AS is_failure,
    period_end_time AS last_seen_date
  FROM system.lakeflow.job_run_timeline
  WHERE
    result_state IS NOT NULL
    AND period_end_time >= CURRENT_DATE() - INTERVAL 30 DAYS
),
most_recent_jobs AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs QUALIFY rn = 1
)
SELECT
  FIRST(t2.name) AS name,
  t1.workspace_id,
  t1.job_id,
  COUNT(*) AS runs,
  SUM(t1.is_failure) AS failures,
  (1 - COALESCE(TRY_DIVIDE(SUM(t1.is_failure), COUNT(*)), 0)) * 100 AS success_ratio,
  MAX(t1.last_seen_date) AS last_seen_date
FROM terminal_statuses t1
LEFT JOIN most_recent_jobs t2 USING (workspace_id, job_id)
GROUP BY ALL
ORDER BY failures DESC
```

### Alertas de Orçamento

É possível configurar alertas no Databricks SQL para notificar quando o gasto superar um limiar. Basta agendar as queries abaixo e definir a condição de disparo quando retornarem linhas.

**Alerta: qualquer workspace excede orçamento nos últimos 30 dias** (substituir `{budget}` pelo valor escolhido):

```sql
SELECT
  t1.workspace_id,
  SUM(t1.usage_quantity * list_prices.pricing.default) AS list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices ON
  t1.cloud = list_prices.cloud AND
  t1.sku_name = list_prices.sku_name AND
  t1.usage_start_time >= list_prices.price_start_time AND
  (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
WHERE
  t1.sku_name LIKE '%SERVERLESS%'
  AND billing_origin_product IN ('JOBS', 'INTERACTIVE')
  AND t1.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY t1.workspace_id
HAVING list_cost > {budget}
```

**Alerta: um job específico excede orçamento** (substituir `{budget}`):

```sql
SELECT
  t1.workspace_id,
  t1.usage_metadata.job_id,
  SUM(t1.usage_quantity * list_prices.pricing.default) AS list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices ON
  t1.cloud = list_prices.cloud AND
  t1.sku_name = list_prices.sku_name AND
  t1.usage_start_time >= list_prices.price_start_time AND
  (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
WHERE
  t1.sku_name LIKE '%SERVERLESS%'
  AND billing_origin_product IN ('JOBS')
  AND t1.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY t1.workspace_id, t1.usage_metadata.job_id
HAVING list_cost > {budget}
```

## Estratégias de Otimização

- Evitar reprocessamento completo (idempotência por partição).
- Ajustar frequência de execução ao SLA real de negócio.
- Limitar retenção na landing e privilegiar camadas curadas.
- Automatizar deploy/rollback para reduzir custo operacional.
- Monitorar jobs com alta taxa de falhas via `system.lakeflow.job_run_timeline` para identificar e eliminar custo de execuções infrutíferas.
- Usar alertas em `system.billing.usage` para reagir a anomalias de gasto antes do fim do ciclo de cobrança.

## Referências

https://docs.databricks.com/aws/en/admin/system-tables/serverless-billing

https://docs.databricks.com/aws/en/admin/system-tables/jobs-cost

https://docs.databricks.com/aws/en/dashboards/automate/import-export#import

https://github.com/databricks/tmm/blob/main/System-Tables-Demo/Lakeflow/LakeFlow%20System%20Tables%20Dashboard%20v0.1.lvdash.json