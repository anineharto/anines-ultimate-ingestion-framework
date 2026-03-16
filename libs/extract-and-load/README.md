# extract-and-load

Pydantic models and YAML loading helpers for extract/load config files.

## Install

```bash
uv sync
```

## Example

```python
from pathlib import Path

from extract_and_load.models import ExtractLoadConfig

extract_load = ExtractLoadConfig.from_yaml_file(
    Path("../../bundles/extract_load/fixtures/extract_load_configs/source_a/el.source_a_dataset_b.yml")
)
```

## Read from ADLS

```python
from extract_and_load.models import ExtractLoadConfig

cfg_a = ExtractLoadConfig.from_adls(
    account_url="https://<storage-account>.dfs.core.windows.net",
    file_system="extract-load-configs",
    file_path="main/el.source_a_dataset_b.json",
)

cfg_b = ExtractLoadConfig.from_adls_uri(
    "abfss://extract-load-configs@<storage-account>.dfs.core.windows.net/main/el.source_a_dataset_b.json"
)
```

`BaseConfig.from_adls(...)` accepts optional parameters and falls back to `.env`:

- `EL_STORAGE_ACCOUNT_URL`
- `EL_CONFIG_CONTAINER`
- `EL_ADLS_FILE_PATH`
