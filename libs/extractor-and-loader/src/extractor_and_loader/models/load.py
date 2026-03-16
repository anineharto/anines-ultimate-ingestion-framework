from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from extractor_and_loader.models.common import Metadata


class GenericLoadConfig(BaseModel):
    # Placeholder for future load kinds (e.g. DeltaLoadConfig, BlobLoadConfig).
    kind: str
    metadata: Metadata
    spec: dict[str, Any]


LoadConfig = GenericLoadConfig
