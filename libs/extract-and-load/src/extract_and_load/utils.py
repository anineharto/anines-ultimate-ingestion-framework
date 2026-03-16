from __future__ import annotations

from dotenv import dotenv_values, find_dotenv


def read_env_file_values() -> dict[str, str]:
    dotenv_path = find_dotenv(usecwd=True)
    if not dotenv_path:
        return {}

    loaded_values = dotenv_values(dotenv_path)
    values: dict[str, str] = {}
    for key, value in loaded_values.items():
        if key is None or value is None:
            continue
        values[key] = value
    return values
