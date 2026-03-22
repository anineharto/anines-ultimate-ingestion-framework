from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from extract_and_load.models.base_config import BaseConfig, CommonConfig, Metadata
from extract_and_load.models.extract_config import ExtractConfig
from extract_and_load.models.load_config import LoadConfig


class ExtractLoadSpec(BaseModel):
    common: CommonConfig
    extract: ExtractConfig
    load: LoadConfig | None = None


class ExtractLoadConfig(BaseConfig):
    kind: Literal["ExtractLoadConfig"]
    metadata: Metadata
    spec: ExtractLoadSpec
