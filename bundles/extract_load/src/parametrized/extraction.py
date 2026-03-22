# Databricks notebook source
# %%
from extract_and_load.models import DummyApiExtractConfig, ExtractLoadConfig
from extract_and_load.extractors import DummyApiExtractor, BaseExtractor

# %%
dbutils_obj = globals().get("dbutils")
if dbutils_obj is not None:
    dbutils_obj.widgets.text("extract_load_config_name", "el.source_a_dataset_b")
    dbutils_obj.widgets.text("current_batch_timestamp", "2026-03-16T12:00:00.000000Z")
    dbutils_obj.widgets.text("last_batch_timestamp", "1970-01-01T00:00:00.123456Z")
    dbutils_obj.widgets.text("branch_name", "main")
    extract_load_config_name = dbutils_obj.widgets.get("extract_load_config_name").strip()
    branch_name = dbutils_obj.widgets.get("branch_name").strip()
    current_batch_timestamp = dbutils_obj.widgets.get("current_batch_timestamp").strip()
    last_batch_timestamp = dbutils_obj.widgets.get("last_batch_timestamp").strip()
else:
    # Local fallback for quick development.
    branch_name = "main"
    extract_load_config_name = "el.source_a_dataset_b"
    current_batch_timestamp = "2026-03-16T12:00:00.000000Z"
    last_batch_timestamp = "1970-01-01T00:00:00.123456Z"

#%%
extractor_factory = {
    DummyApiExtractConfig: DummyApiExtractor,
    # SsbApiExtractConfig: SsbApiExtractor,
    # SftpExtractConfig: SftpExtractor,
    # DynamicsCrmExtractConfig: DynamicsCrmExtractor,
}

extract_load_config_path = f"{branch_name}/{extract_load_config_name}"
extract_load_config = ExtractLoadConfig.from_adls(file_path=extract_load_config_path)
extract_config = extract_load_config.spec.extract
if not isinstance(extract_config, DummyApiExtractConfig):
    raise ValueError(
        f"Invalid extract config kind: {type(extract_config).__name__}. Expected DummyApiExtractConfig."
    )

extractor: BaseExtractor = extractor_factory[type(extract_config)](
    extract_and_load_config=extract_load_config,
    current_batch_timestamp=current_batch_timestamp,
    last_batch_timestamp=last_batch_timestamp,
)
output_path = extractor.run()
# %%
