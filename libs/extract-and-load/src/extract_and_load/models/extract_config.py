from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from extract_and_load.models.base_config import Metadata


class SqlExtractConfig(BaseModel):
    kind: Literal["SqlExtractConfig"]
    metadata: Metadata

    class Spec(BaseModel):
        server: str
        database: str
        table: str
        query: str

        @field_validator("query", mode="before")
        @classmethod
        def normalize_query(cls, value: object) -> str:
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                query_value = value.get("query")
                if isinstance(query_value, str):
                    return query_value
            raise ValueError("query must be a string or a mapping with a 'query' string key")
    
    spec: Spec


class DummyApiExtractConfig(BaseModel):
    kind: Literal["DummyApiExtractConfig"]
    metadata: Metadata

    class Spec(BaseModel):
        feed: str

    spec: Spec


ExtractConfig = Annotated[
    SqlExtractConfig
    | DummyApiExtractConfig,
    # | SsbApiExtractConfig,
    # | SftpExtractConfig,
    # | DynamicsCrmExtractConfig,
    Field(discriminator="kind"),
]
