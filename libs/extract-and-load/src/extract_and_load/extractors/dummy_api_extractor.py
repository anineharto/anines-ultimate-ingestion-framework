from __future__ import annotations

import json
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient

from extract_and_load.extractors.base_extractor import BaseExtractor
from extract_and_load.models.extract_config import DummyApiExtractConfig
from extract_and_load.models.extract_load_config import ExtractLoadConfig
from extract_and_load.utils import read_env_file_values


class DummyApiExtractor(BaseExtractor):
    API_URL_TEMPLATE = "https://jsonplaceholder.typicode.com/{feed}"
    ACCOUNT_URL_ENV = "EL_STORAGE_ACCOUNT_URL"
    LANDING_CONTAINER_ENV = "EL_LANDING_CONTAINER"

    def __init__(
        self,
        extract_and_load_config: ExtractLoadConfig,
        current_batch_timestamp: str,
        last_batch_timestamp: str,
        *,
        account_url: str | None = None,
        container_name: str | None = None,
    ) -> None:
        super().__init__(
            extract_and_load_config=extract_and_load_config,
            current_batch_timestamp=current_batch_timestamp,
            last_batch_timestamp=last_batch_timestamp,
        )
        extract_config: DummyApiExtractConfig = self.extract_and_load_config.spec.extract
        if not isinstance(extract_config, DummyApiExtractConfig):
            raise ValueError(
                "DummyApiExtractor requires extract kind 'DummyApiExtractConfig' with spec.feed."
            )

        env_values = read_env_file_values()
        self.account_url = account_url or env_values.get(self.ACCOUNT_URL_ENV)
        self.container_name = container_name or env_values.get(self.LANDING_CONTAINER_ENV)
        if not self.account_url:
            raise ValueError(
                f"Missing storage account URL. Set {self.ACCOUNT_URL_ENV} in .env or pass account_url."
            )
        if not self.container_name:
            raise ValueError(
                "Missing landing container name. "
                f"Set {self.LANDING_CONTAINER_ENV} in .env or pass container_name."
            )
        
        self.feed = extract_config.spec.feed

    def fetch_dummy_records(self) -> list[dict]:
        api_url = self.API_URL_TEMPLATE.format(feed=self.feed)
        with urlopen(api_url) as response:  # nosec B310 - public dummy endpoint by design
            payload = response.read().decode("utf-8")
        parsed = json.loads(payload)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            return [parsed]
        raise ValueError("Unexpected API response. Expected object or list of objects.")

    @classmethod
    def _extract_record_timestamp(cls, record: dict) -> datetime | None:
        timestamp_keys = ("updated_at", "modified_at", "event_timestamp", "timestamp", "created_at")
        for key in timestamp_keys:
            value = record.get(key)
            if not isinstance(value, str):
                continue
            try:
                return cls._parse_timestamp(value)
            except ValueError:
                continue
        return None

    def _filter_records_newer_than_last_batch_timestamp(
        self, records: list[dict]
    ) -> list[dict]:
        parsed_records = [
            (record, self._extract_record_timestamp(record))
            for record in records
        ]
        if not any(record_timestamp is not None for _, record_timestamp in parsed_records):
            # Dummy endpoint payloads can omit temporal fields; keep full payload in that case.
            return records

        last_batch_cutoff = self._parse_timestamp(self.last_batch_timestamp)
        return [
            record
            for record, record_timestamp in parsed_records
            if record_timestamp is not None and record_timestamp > last_batch_cutoff
        ]

    @staticmethod
    def _to_parquet_bytes(records: list[dict]) -> bytes:
        table = pa.Table.from_pylist(records)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()

    def upload_parquet_to_adls(
        self,
        *,
        records: list[dict],
        filename: str = "filename.parquet",
        overwrite: bool = True,
    ) -> str:
        parquet_bytes = self._to_parquet_bytes(records)
        adls_file_path = self.build_adls_file_path(filename)
        file_client = DataLakeFileClient(
            account_url=self.account_url,
            file_system_name=self.container_name,
            file_path=adls_file_path,
            credential=DefaultAzureCredential(),
        )
        file_client.upload_data(parquet_bytes, overwrite=overwrite)
        return (
            f"{self.account_url.rstrip('/')}/{self.container_name}/{adls_file_path.lstrip('/')}"
        )

    def run(self, *, filename: str = "filename.parquet", overwrite: bool = True) -> str:
        records = self.fetch_dummy_records()
        records = self._filter_records_newer_than_last_batch_timestamp(records)
        return self.upload_parquet_to_adls(
            records=records,
            filename=filename,
            overwrite=overwrite,
        )
