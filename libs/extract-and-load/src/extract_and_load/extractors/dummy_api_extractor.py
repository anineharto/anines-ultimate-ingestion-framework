from __future__ import annotations

import json
from io import BytesIO
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient

from extract_and_load.extractors.base_extractor import BaseExtractor
from extract_and_load.models.extract_config import ApiExtractConfig
from extract_and_load.models.extract_load_config import ExtractLoadConfig
from extract_and_load.utils import read_env_file_values


class DummyApiExtractor(BaseExtractor):
    API_URL_TEMPLATE = "https://jsonplaceholder.typicode.com/{feed}"
    ACCOUNT_URL_ENV = "EL_STORAGE_ACCOUNT_URL"
    LANDING_CONTAINER_ENV = "EL_LANDING_CONTAINER"

    def __init__(
        self,
        extract_and_load_config: ExtractLoadConfig,
        batch_timestamp: str,
        *,
        account_url: str | None = None,
        container_name: str | None = None,
    ) -> None:
        super().__init__(extract_and_load_config, batch_timestamp)
        extract_config: ApiExtractConfig = self.extract_and_load_config.spec.extract
        if not isinstance(extract_config, ApiExtractConfig):
            raise ValueError(
                "DummyApiExtractor requires extract kind 'ApiExtractConfig' with spec.feed."
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
        return self.upload_parquet_to_adls(
            records=records,
            filename=filename,
            overwrite=overwrite,
        )
