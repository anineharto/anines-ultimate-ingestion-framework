from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from extractor_and_loader.models.common import CommonConfig, Metadata
from extractor_and_loader.models.extract import ExtractConfig


class ExtractLoadSpec(BaseModel):
    common: CommonConfig
    extract: ExtractConfig


class ExtractLoadConfig(BaseModel):
    kind: Literal["ExtractLoadConfig", "ExtractAndLoadConfig"]
    metadata: Metadata
    spec: ExtractLoadSpec
