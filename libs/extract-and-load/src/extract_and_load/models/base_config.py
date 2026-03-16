from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient
from pydantic import BaseModel
from extract_and_load.utils import read_env_file_values
import yaml


class BaseConfig(BaseModel):
    ADLS_ACCOUNT_URL_ENV: ClassVar[str] = "EL_STORAGE_ACCOUNT_URL"
    ADLS_FILE_SYSTEM_ENV: ClassVar[str] = "EL_CONFIG_CONTAINER"
    ADLS_FILE_PATH_ENV: ClassVar[str] = "EL_ADLS_FILE_PATH"

    @classmethod
    def from_yaml_file(cls, path: Path) -> "BaseConfig":
        with path.open("r", encoding="utf-8") as file_obj:
            data = yaml.safe_load(file_obj)
        if not isinstance(data, dict):
            raise ValueError(f"Expected mapping in YAML file: {path}")
        return cls.model_validate(data)

    @staticmethod
    def _parse_adls_uri(adls_uri: str) -> tuple[str, str, str]:
        parsed = urlparse(adls_uri)

        if parsed.scheme == "abfss":
            if "@" not in parsed.netloc:
                raise ValueError(
                    "Invalid abfss URI. Expected format: "
                    "abfss://<filesystem>@<account>.dfs.core.windows.net/<path>"
                )
            filesystem, host = parsed.netloc.split("@", 1)
            account_url = f"https://{host}"
            file_path = parsed.path.lstrip("/")
            if not filesystem or not file_path:
                raise ValueError("Invalid abfss URI. Filesystem and file path are required.")
            return account_url, filesystem, file_path

        if parsed.scheme == "https":
            path_parts = [part for part in parsed.path.split("/") if part]
            if len(path_parts) < 2:
                raise ValueError(
                    "Invalid ADLS https URI. Expected format: "
                    "https://<account>.dfs.core.windows.net/<filesystem>/<path>"
                )
            filesystem = path_parts[0]
            file_path = "/".join(path_parts[1:])
            account_url = f"{parsed.scheme}://{parsed.netloc}"
            return account_url, filesystem, file_path

        raise ValueError("Unsupported ADLS URI scheme. Use abfss:// or https://")

    @classmethod
    def from_adls(
        cls,
        *,
        account_url: str | None = None,
        file_system: str | None = None,
        file_path: str | None = None,
    ) -> "BaseConfig":
        env_values = read_env_file_values()
        resolved_account_url = account_url or env_values.get(cls.ADLS_ACCOUNT_URL_ENV)
        resolved_file_system = file_system or env_values.get(cls.ADLS_FILE_SYSTEM_ENV)
        resolved_file_path = file_path or env_values.get(cls.ADLS_FILE_PATH_ENV)

        if not resolved_account_url:
            raise ValueError(
                f"Missing ADLS account URL. Pass account_url or set {cls.ADLS_ACCOUNT_URL_ENV} in .env."
            )
        if not resolved_file_system:
            raise ValueError(
                f"Missing ADLS file system. Pass file_system or set {cls.ADLS_FILE_SYSTEM_ENV} in .env."
            )
        if not resolved_file_path:
            raise ValueError(
                f"Missing ADLS file path. Pass file_path or set {cls.ADLS_FILE_PATH_ENV} in .env."
            )

        client = DataLakeFileClient(
            account_url=resolved_account_url,
            file_system_name=resolved_file_system,
            file_path=resolved_file_path,
            credential=DefaultAzureCredential(),
        )
        file_bytes = client.download_file().readall()
        parsed: Any = yaml.safe_load(file_bytes.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("Expected mapping content in ADLS config file")
        return cls.model_validate(parsed)

    @classmethod
    def from_adls_uri(cls, adls_uri: str) -> "BaseConfig":
        account_url, file_system, file_path = cls._parse_adls_uri(adls_uri)
        return cls.from_adls(
            account_url=account_url,
            file_system=file_system,
            file_path=file_path,
        )


class Metadata(BaseModel):
    name: str
    description: str | None = None


class CommonConfig(BaseModel):
    source: str
    dataset: str
