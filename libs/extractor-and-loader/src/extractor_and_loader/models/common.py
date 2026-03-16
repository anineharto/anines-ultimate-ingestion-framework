from __future__ import annotations

from pydantic import BaseModel


class Metadata(BaseModel):
    name: str
    description: str | None = None


class CommonConfig(BaseModel):
    source: str
    dataset: str
