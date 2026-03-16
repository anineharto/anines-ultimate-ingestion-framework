from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from extract_and_load.models.base_config import BaseConfig, CommonConfig, Metadata
from extract_and_load.models.extract_config import ExtractConfig


class ExtractLoadSpec(BaseModel):
    common: CommonConfig
    extract: ExtractConfig


class ExtractLoadConfig(BaseConfig):
    kind: Literal["ExtractLoadConfig", "ExtractAndLoadConfig"]
    metadata: Metadata
    spec: ExtractLoadSpec
