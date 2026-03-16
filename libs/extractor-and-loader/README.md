# extractor-and-loader

Pydantic models and YAML loading helpers for extract/load config files.

## Install

```bash
uv sync
```

## Example

```python
from pathlib import Path

from extractor_and_loader.loaders import load_extract_load_config

extract_load = load_extract_load_config(
    Path("../../bundles/extract_load/fixtures/extract_load_configs/source_a/el.source_a_dataset_b.yml")
)
```
