# manage-extract-and-load-configs

Typer CLI to upload extract and load configs to blob storage.
During upload it renders Jinja placeholders using a variables file and validates
the rendered result with `extract-and-load` Pydantic models before upload.
Validated configs are uploaded as `.json` blobs.
Blob path format is always `<feature-branch-name>/<config-name>.json`.

## Install

```bash
uv sync
```

## Usage

```bash
uv run manage-extract-and-load-configs upload \
  --source-path ../../bundles/extract_load/fixtures/extract_load_configs \
  --variables-path ../../bundles/extract_load/fixtures/environment_configs/sandbox.yml
```

Uploads always overwrite existing blobs.

Delete uploaded config blobs that match local YAML files:

```bash
uv run manage-extract-and-load-configs destroy \
  --source-path ../../bundles/extract_load/fixtures/extract_load_configs
```

Set preconfigured storage settings with:

```bash
export EL_STORAGE_ACCOUNT_URL="https://<storage-account>.blob.core.windows.net"
export EL_CONFIG_CONTAINER="..."
```

Authentication uses Azure default credentials (`DefaultAzureCredential`).

Variables files can use dotted keys that map to Jinja paths, for example:

```yaml
source_a.dataset_b.server: sandbox-sql.company.internal
```
