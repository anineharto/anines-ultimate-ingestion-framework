# Databricks Asset Bundle: db-bundles

This project is initialized as a Databricks Asset Bundle with:

- `databricks.yml` bundle configuration
- `resources/db_bundles_job.yml` sample job definition
- `src/notebook.py` starter notebook script

## Prerequisites

1. Databricks CLI v0.218+ installed
2. `uv` installed for Python package management
3. Databricks authentication configured:

```bash
databricks configure
```

## Install dependencies

```bash
uv sync
```

## Validate and deploy

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run db_bundles_job -t dev
```

Before deploying, set values for:

- `workspace.host` in `databricks.yml`
- `var.cluster_id` in `databricks.yml` (or pass with `--var`)
