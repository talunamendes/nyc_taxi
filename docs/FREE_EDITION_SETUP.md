# Databricks Free Edition Setup

This guide makes the project runnable by contributors using their own Databricks Free Edition workspace.

## 1) Prerequisites

- Databricks CLI installed and authenticated (`databricks auth login`)
- `uv` installed
- Serverless Jobs enabled in your Databricks workspace

## 2) Bootstrap Unity Catalog objects (DDL)

Use the scripts in `docs/sql`:

- `docs/sql/001_create_catalog.sql`
- `docs/sql/002_create_schemas.sql`
- `docs/sql/003_create_volumes.sql`

Run scripts in Databricks SQL Editor (or notebook SQL cells) in order: `001`, `002`, then `003`.

If your workspace blocks catalog creation permissions, skip `001` and use an existing catalog name in all commands.

## 3) Deploy bundle to your workspace

Build the wheel:

```bash
make uv-build
```

Deploy to `dev` target, overriding variables:

```bash
make dab-deploy ENV=dev \
  CATALOG=nyc_taxi_dev
```

Or directly with explicit variables:

```bash
databricks bundle deploy -t dev \
  --var="catalog=nyc_taxi_dev" \
  --var="wheel_file=<nome_do_arquivo_whl_em_dist>"
```

## 4) Run workflow

Use Makefile:

```bash
make dab-run ENV=dev WORKFLOW=nyc_taxi_job \
  CATALOG=nyc_taxi_dev
```

## Notes

- The repository no longer hardcodes workspace host or user paths.
- The bundle uses your authenticated CLI workspace/profile.
- The workflow is configured to run on Serverless Jobs (no `cluster_id` required).
- `wheel_file` is resolved dynamically by Makefile from `dist/*.whl`.
