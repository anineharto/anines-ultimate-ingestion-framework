# Databricks notebook source
# %%
from __future__ import annotations

from extract_and_load.models import ExtractLoadConfig

# %%
dbutils_obj = globals().get("dbutils")
if dbutils_obj is not None:
    dbutils_obj.widgets.text("extract_load_config_name", "el.source_a_dataset_b")
    dbutils_obj.widgets.text("feature_branch_name", "main")
    extract_load_config_name = dbutils_obj.widgets.get("extract_load_config_name").strip()
    feature_branch_name = dbutils_obj.widgets.get("feature_branch_name").strip()
else:
    # Local fallback for quick development.
    feature_branch_name = "main"
    extract_load_config_name = "el.source_a_dataset_b"

# %%
extract_load_config_path = f"{feature_branch_name}/{extract_load_config_name}"
extract_load_config = ExtractLoadConfig.from_adls(file_path=extract_load_config_path)
# %%
extract_load_config
# %%
