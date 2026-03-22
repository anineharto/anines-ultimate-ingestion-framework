from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
import uuid

from extract_and_load.models.extract_load_config import ExtractLoadConfig


class BaseExtractor(ABC):
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(
        self,
        extract_and_load_config: ExtractLoadConfig,
        current_batch_timestamp: str,
        last_batch_timestamp: str,
    ) -> None:
        self._validate_timestamp("current_batch_timestamp", current_batch_timestamp)
        self._validate_timestamp("last_batch_timestamp", last_batch_timestamp)
        self._validate_timestamp_order(
            last_batch_timestamp=last_batch_timestamp,
            current_batch_timestamp=current_batch_timestamp,
        )
        self.extract_and_load_config = extract_and_load_config
        self.current_batch_timestamp = current_batch_timestamp
        self.last_batch_timestamp = last_batch_timestamp
        self.source = extract_and_load_config.spec.common.source
        self.dataset = extract_and_load_config.spec.common.dataset

    @classmethod
    def _parse_timestamp(cls, timestamp_value: str) -> datetime:
        return datetime.strptime(timestamp_value, cls.TIMESTAMP_FORMAT)

    @classmethod
    def _validate_timestamp(cls, field_name: str, timestamp_value: str) -> None:
        try:
            cls._parse_timestamp(timestamp_value)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must use format yyyy-MM-ddTHH:mm:ss.ffffffZ"
            ) from exc

    @classmethod
    def _validate_timestamp_order(
        cls,
        *,
        last_batch_timestamp: str,
        current_batch_timestamp: str,
    ) -> None:
        if cls._parse_timestamp(last_batch_timestamp) > cls._parse_timestamp(
            current_batch_timestamp
        ):
            raise ValueError(
                "last_batch_timestamp must be earlier than or equal to current_batch_timestamp"
            )

    def build_adls_file_path(self) -> str:
        guid = str(uuid.uuid4())
        filename = f"{guid}.parquet"
        return (
            f"{self.source}/{self.dataset}/"
            f"batch_timestamp={self.current_batch_timestamp}/{filename}"
        )

    @abstractmethod
    def run(self) -> Any:
        raise NotImplementedError
