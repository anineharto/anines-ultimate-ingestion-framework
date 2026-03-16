# Databricks notebook source
# %%
from extract_and_load.models import DummyApiExtractConfig, ExtractLoadConfig
from extract_and_load.extractors import DummyApiExtractor, BaseExtractor

# %%
dbutils_obj = globals().get("dbutils")
if dbutils_obj is not None:
    dbutils_obj.widgets.text("extract_load_config_name", "el.source_a_dataset_b")
    dbutils_obj.widgets.text("batch_timestamp", "2026-03-16T12:00:00.000000Z")
    dbutils_obj.widgets.text("feature_branch_name", "main")
    extract_load_config_name = dbutils_obj.widgets.get("extract_load_config_name").strip()
    feature_branch_name = dbutils_obj.widgets.get("feature_branch_name").strip()
    batch_timestamp = dbutils_obj.widgets.get("batch_timestamp").strip()
else:
    # Local fallback for quick development.
    feature_branch_name = "main"
    extract_load_config_name = "el.source_a_dataset_b"
    batch_timestamp = "2026-03-16T12:00:00.000000Z"

#%%
extractor_factory = {
    DummyApiExtractConfig: DummyApiExtractor,
}

extract_load_config_path = f"{feature_branch_name}/{extract_load_config_name}"
extract_load_config = ExtractLoadConfig.from_adls(file_path=extract_load_config_path)
extract_config = extract_load_config.spec.extract
if not isinstance(extract_config, DummyApiExtractConfig):
    raise ValueError(
        f"Invalid extract config kind: {type(extract_config).__name__}. Expected ApiExtractConfig."
    )

extractor: BaseExtractor = extractor_factory[type(extract_config)](
    extract_and_load_config=extract_load_config,
    batch_timestamp=batch_timestamp,
)
output_path = extractor.run()
# %%
