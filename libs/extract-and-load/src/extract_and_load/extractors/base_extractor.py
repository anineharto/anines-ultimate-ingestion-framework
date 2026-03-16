from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
import uuid

from extract_and_load.models.extract_load_config import ExtractLoadConfig


class BaseExtractor(ABC):
    BATCH_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(
        self,
        extract_and_load_config: ExtractLoadConfig,
        batch_timestamp: str,
    ) -> None:
        self._validate_batch_timestamp(batch_timestamp)
        self.extract_and_load_config = extract_and_load_config
        self.batch_timestamp = batch_timestamp
        self.source = extract_and_load_config.spec.common.source
        self.dataset = extract_and_load_config.spec.common.dataset

    @classmethod
    def _validate_batch_timestamp(cls, batch_timestamp: str) -> None:
        try:
            datetime.strptime(batch_timestamp, cls.BATCH_TIMESTAMP_FORMAT)
        except ValueError as exc:
            raise ValueError(
                "batch_timestamp must use format yyyy-MM-ddTHH:mm:ss.ffffffZ"
            ) from exc

    def build_adls_file_path(self) -> str:
        guid = str(uuid.uuid4())
        filename = f"{guid}.parquet"
        return f"{self.source}/{self.dataset}/batch_timestamp={self.batch_timestamp}/{filename}"

    @abstractmethod
    def run(self) -> Any:
        raise NotImplementedError
