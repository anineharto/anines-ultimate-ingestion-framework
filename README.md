# db-bundles

Monorepo for Databricks bundles and shared Python libraries.

## Repo layout

- `bundles/extract_load`: Databricks bundle/service
- `libs/extractor-and-loader`: Pydantic models for extract/load configs
- `libs/manage-extract-and-load-configs`: CLI to render, validate, and upload configs to Blob Storage

## Prerequisites

- `uv` installed
- Azure CLI installed
- Databricks CLI installed (if you also work with bundle deploy commands)

## 1) Install the CLI as a uv tool

From repo root:

```bash
uv tool install --editable "./libs/manage-extract-and-load-configs"
```

After this, you can run:

```bash
manage-extract-and-load-configs --help
```

## 2) Authenticate with Azure (Default auth)

The CLI uses `DefaultAzureCredential`. For local development, sign in with Azure CLI:

```bash
az login
```

If you need a specific tenant/subscription:

```bash
az account set --subscription "<subscription-id-or-name>"
```

## 3) Configure storage target

Create a `.env` file in repo root (already supported by the CLI) with:

```bash
MANAGE_EL_STORAGE_ACCOUNT_URL="https://<storage-account>.blob.core.windows.net"
MANAGE_EL_STORAGE_CONTAINER="<container-name>"
MANAGE_EL_ENV="sandbox"
```

Branch naming is auto-resolved from the current git branch.
Environment config is auto-resolved from `MANAGE_EL_ENV`, looking for
`environment_configs/<env>.yml` near your `--source-path`.

## 4) Upload configs

Validated JSON blobs are uploaded to:

`<feature-branch-name>/<config-name>.json`

Example:

```bash
manage-extract-and-load-configs upload \
  --source-path bundles/extract_load/fixtures/extract_and_load_configs
```

Uploads always overwrite existing blobs.

## 5) Destroy uploaded configs

```bash
manage-extract-and-load-configs destroy \
  --source-path bundles/extract_load/fixtures/extract_and_load_configs
```

## Notes

- Variables files support dotted keys for Jinja templates, for example:
  - `source_a.dataset_b.server: dev-sql.company.internal`
- Config templates are rendered and validated before upload.
