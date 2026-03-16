from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from dotenv import dotenv_values, find_dotenv
from extract_and_load.models import ExtractLoadConfig
from jinja2 import Environment, StrictUndefined
import yaml


@dataclass
class UploadResult:
    uploaded_count: int
    total_bytes: int
    destination_container: str


@dataclass
class DeleteResult:
    deleted_count: int
    total_candidates: int
    destination_container: str


STORAGE_ACCOUNT_URL_ENV = "EL_STORAGE_ACCOUNT_URL"
STORAGE_CONTAINER_ENV = "EL_CONFIG_CONTAINER"
RUNTIME_ENV_ENV = "EL_ENV"


def _resolve_dotenv_path(anchor_path: Path | None = None) -> str:
    if anchor_path is not None:
        base_dir = anchor_path if anchor_path.is_dir() else anchor_path.parent
        for parent in [base_dir, *base_dir.parents]:
            candidate = parent / ".env"
            if candidate.exists() and candidate.is_file():
                return str(candidate)

    # Fallback to current working directory search.
    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        return dotenv_path

    raise ValueError(
        "Missing .env file. Expected .env near the project/source path or in the current working directory tree."
    )


def _read_env_file_values(anchor_path: Path | None = None) -> dict[str, str]:
    dotenv_path = _resolve_dotenv_path(anchor_path)

    loaded_values = dotenv_values(dotenv_path)
    values: dict[str, str] = {}
    for key, value in loaded_values.items():
        if key is None or value is None:
            continue
        values[key] = value
    return values


def _get_preconfigured_storage_settings(anchor_path: Path | None = None) -> tuple[str, str]:
    env_values = _read_env_file_values(anchor_path)
    account_url = env_values.get(STORAGE_ACCOUNT_URL_ENV)
    container = env_values.get(STORAGE_CONTAINER_ENV)
    if not account_url:
        raise ValueError(
            f"Missing required environment variable '{STORAGE_ACCOUNT_URL_ENV}' for storage account URL."
        )
    if not container:
        raise ValueError(
            f"Missing required environment variable '{STORAGE_CONTAINER_ENV}' for target container."
        )
    return account_url, container


def _detect_feature_branch_name(search_from_path: Path) -> str:
    cwd = search_from_path if search_from_path.is_dir() else search_from_path.parent
    command_candidates = [
        ["git", "symbolic-ref", "--short", "HEAD"],
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
    ]
    for command in command_candidates:
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)
        branch = result.stdout.strip()
        if result.returncode == 0 and branch and branch != "HEAD":
            return branch

    raise ValueError(
        "Unable to detect feature branch name from git. Run this command from a git repository on a named branch."
    )


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_obj:
        loaded = yaml.safe_load(file_obj)
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected YAML mapping in '{path}'.")
    return loaded


def _resolve_variables_path(source_path: Path, variables_path: Path | None) -> Path:
    if variables_path is not None:
        return variables_path.resolve()

    env_values = _read_env_file_values(source_path)
    env_name = env_values.get(RUNTIME_ENV_ENV)
    if not env_name:
        raise ValueError(
            f"Missing '{RUNTIME_ENV_ENV}' in .env (or pass --variables-path explicitly)."
        )

    base_dir = source_path if source_path.is_dir() else source_path.parent
    for parent in [base_dir, *base_dir.parents]:
        env_dir = parent / "environment_configs"
        if not env_dir.exists() or not env_dir.is_dir():
            continue

        named_candidates = [
            env_dir / f"{env_name}.yml",
            env_dir / f"{env_name}.yaml",
        ]
        for candidate_path in named_candidates:
            if candidate_path.exists() and candidate_path.is_file():
                return candidate_path.resolve()

        # In the nearest environment_configs folder, fall back to single file if unique.
        env_files = sorted(env_dir.glob("*.yml")) + sorted(env_dir.glob("*.yaml"))
        unique_env_files = list(dict.fromkeys(path.resolve() for path in env_files))
        if len(unique_env_files) == 1:
            return unique_env_files[0]

        break

    raise ValueError(
        f"Unable to resolve environment config for '{env_name}'. "
        "Expected a file like environment_configs/<env>.yml near the source path."
    )


def _merge_mappings(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_mappings(merged[key], value)
        else:
            merged[key] = value
    return merged


def _set_dotted_key(target: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [part for part in dotted_key.split(".") if part]
    if not parts:
        return

    cursor = target
    for part in parts[:-1]:
        existing = cursor.get(part)
        if existing is None:
            cursor[part] = {}
            cursor = cursor[part]
            continue
        if not isinstance(existing, dict):
            raise ValueError(
                f"Cannot expand dotted key '{dotted_key}': '{part}' already exists and is not a mapping."
            )
        cursor = existing

    last = parts[-1]
    existing_leaf = cursor.get(last)
    if isinstance(existing_leaf, dict) and isinstance(value, dict):
        cursor[last] = _merge_mappings(existing_leaf, value)
    else:
        cursor[last] = value


def _build_jinja_context(jinja_variables: dict[str, Any]) -> dict[str, Any]:
    context: dict[str, Any] = {}
    dotted_items: list[tuple[str, Any]] = []

    for key, value in jinja_variables.items():
        if "." in key:
            dotted_items.append((key, value))
            continue
        context[key] = value

    for dotted_key, value in dotted_items:
        _set_dotted_key(context, dotted_key, value)

    return context


def _render_and_validate_extract_load_config(
    *,
    template_path: Path,
    jinja_variables: dict[str, Any],
) -> bytes:
    template_text = template_path.read_text(encoding="utf-8")
    render_context = _build_jinja_context(jinja_variables)

    environment = Environment(undefined=StrictUndefined)
    rendered_text = environment.from_string(template_text).render(**render_context)

    parsed = yaml.safe_load(rendered_text)
    if not isinstance(parsed, dict):
        raise ValueError(f"Rendered config must be a YAML mapping: '{template_path}'.")

    validated = ExtractLoadConfig.model_validate(parsed)
    normalized_json = json.dumps(validated.model_dump(exclude_none=True), indent=2)
    return f"{normalized_json}\n".encode("utf-8")


def _build_blob_name(feature_branch_name: str, source_file: Path) -> str:
    config_name = source_file.stem
    return f"{feature_branch_name}/{config_name}.json"


def collect_yaml_files(source_path: Path) -> list[Path]:
    if source_path.is_file():
        if source_path.suffix.lower() in {".yml", ".yaml"}:
            return [source_path]
        return []

    files: list[Path] = []
    for file_path in source_path.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() in {".yml", ".yaml"}:
            files.append(file_path)
    return sorted(files)


def upload_configs_to_blob(
    *,
    source_path: Path,
    variables_path: Path | None = None,
    dry_run: bool = False,
) -> UploadResult:
    source_path = source_path.resolve()
    files = collect_yaml_files(source_path)
    if not files:
        raise ValueError(f"No YAML files found under '{source_path}'.")

    account_url, container = _get_preconfigured_storage_settings(source_path)
    feature_branch_name = _detect_feature_branch_name(source_path)

    resolved_variables_path = _resolve_variables_path(source_path, variables_path)
    jinja_variables = _read_yaml_mapping(resolved_variables_path)

    uploaded_count = 0
    total_bytes = 0

    if not dry_run:
        blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=DefaultAzureCredential(),
        )
        container_client = blob_service_client.get_container_client(container)
    else:
        container_client = None

    for file_path in files:
        blob_name = _build_blob_name(feature_branch_name, file_path)
        try:
            rendered_bytes = _render_and_validate_extract_load_config(
                template_path=file_path,
                jinja_variables=jinja_variables,
            )
        except Exception as exc:
            raise ValueError(f"Invalid rendered config in '{file_path}': {exc}") from exc

        total_bytes += len(rendered_bytes)

        if not dry_run and container_client is not None:
            container_client.upload_blob(name=blob_name, data=rendered_bytes, overwrite=True)
        uploaded_count += 1

    return UploadResult(
        uploaded_count=uploaded_count,
        total_bytes=total_bytes,
        destination_container=container,
    )


def delete_configs_from_blob(
    *,
    source_path: Path,
    dry_run: bool = False,
    ignore_missing: bool = True,
) -> DeleteResult:
    source_path = source_path.resolve()
    files = collect_yaml_files(source_path)
    if not files:
        raise ValueError(f"No YAML files found under '{source_path}'.")

    account_url, container = _get_preconfigured_storage_settings(source_path)
    feature_branch_name = _detect_feature_branch_name(source_path)
    deleted_count = 0

    if not dry_run:
        blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=DefaultAzureCredential(),
        )
        container_client = blob_service_client.get_container_client(container)
    else:
        container_client = None

    for file_path in files:
        blob_name = _build_blob_name(feature_branch_name, file_path)

        if dry_run:
            deleted_count += 1
            continue

        if container_client is None:
            continue

        try:
            container_client.delete_blob(blob_name)
            deleted_count += 1
        except ResourceNotFoundError:
            if not ignore_missing:
                raise

    return DeleteResult(
        deleted_count=deleted_count,
        total_candidates=len(files),
        destination_container=container,
    )
