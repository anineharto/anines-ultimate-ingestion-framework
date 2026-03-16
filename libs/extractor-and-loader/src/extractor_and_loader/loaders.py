from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from extractor_and_loader.models import ExtractLoadConfig


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_obj:
        data = yaml.safe_load(file_obj)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in YAML file: {path}")
    return data


def load_extract_load_config(path: Path) -> ExtractLoadConfig:
    data = _read_yaml(path)
    return ExtractLoadConfig.model_validate(data)
