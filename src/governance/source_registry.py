"""
source_registry.py

Central access service for the validated Source Registry.

Responsibilities
----------------
- obtain validated Sources and Datasets from validate_source_registry.py;
- resolve source metadata through Source ID;
- derive SQLite table names;
- expose dataset selection and lookup functions;
- resolve upstream dataset dependencies through Input Dataset ID;
- build safe relative and absolute dataset paths.

This module does not duplicate validation rules and contains no business
Dataset ID, Source ID, file name, provider, URL, or storage path.
"""

from pathlib import Path

import pandas as pd

from src.governance.validate_source_registry import (
    derive_table_name,
    validate_source_registry,
)
from src.utils.logger import logger

SOURCE_KEY = "Source ID"
DATASET_KEY = "Dataset ID"
INPUT_DATASET_KEY = "Input Dataset ID"
OUTPUT_FILE_KEY = "Output File Name"
STORAGE_DIRECTORY_KEY = "Storage Directory"
PRODUCTION_METHOD_KEY = "Production Method"
OUTPUT_ROLE_KEY = "Output Role"
DOWNLOAD_URL_KEY = "Download URL"
API_ENDPOINT_KEY = "API Endpoint"
DERIVED_TABLE_KEY = "Derived Table Name"

ACTIVE_KEY = "Active"
LOAD_ENABLED_KEY = "Load Enabled"
QUALITY_ENABLED_KEY = "Quality Enabled"
CHRONOLOGY_ENABLED_KEY = "Chronology Enabled"

DOWNLOAD_METHOD = "Download"
PREPARATION_METHOD = "Preparation"
API_METHOD = "API"
PRIMARY_ROLE = "Primary"
EXCEPTION_ROLE = "Exception"


def get_project_root():
    """Return the project root derived from this module location."""
    root = Path(__file__).resolve().parents[2]
    if not (root / "src").is_dir():
        raise RuntimeError(f"Unable to determine project root from {__file__}.")
    return root


def normalize_lookup_value(value, field_name):
    """Normalize a required lookup value."""
    if value is None:
        raise ValueError(f"{field_name} is required.")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    return normalized


def normalize_optional_value(value):
    """Normalize an optional text value."""
    if value is None or pd.isna(value):
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_output_file_name(output_file):
    """Extract and validate a file name from a name or complete path."""
    value = normalize_lookup_value(output_file, "output_file")
    file_name = Path(value).name
    if file_name in {"", ".", ".."}:
        raise ValueError("output_file must identify a file.")
    return file_name


def normalized_casefold_series(series):
    """Return normalized case-insensitive strings for matching."""
    return series.fillna("").astype(str).str.strip().str.casefold()


def validate_resolved_registry(resolved_registry):
    """Validate the structural contract of the resolved registry."""
    required_columns = {
        SOURCE_KEY,
        DATASET_KEY,
        INPUT_DATASET_KEY,
        OUTPUT_FILE_KEY,
        STORAGE_DIRECTORY_KEY,
        PRODUCTION_METHOD_KEY,
        OUTPUT_ROLE_KEY,
        ACTIVE_KEY,
        LOAD_ENABLED_KEY,
        QUALITY_ENABLED_KEY,
        CHRONOLOGY_ENABLED_KEY,
        DERIVED_TABLE_KEY,
    }
    missing_columns = sorted(required_columns - set(resolved_registry.columns))
    if missing_columns:
        raise ValueError(
            "Resolved Source Registry is missing required columns: "
            f"{missing_columns}"
        )
    if resolved_registry.empty:
        raise ValueError("Resolved Source Registry is empty.")

    uniqueness_columns = (
        DATASET_KEY,
        OUTPUT_FILE_KEY,
        DERIVED_TABLE_KEY,
    )
    for column in uniqueness_columns:
        values = normalized_casefold_series(resolved_registry[column])
        duplicate_mask = values.duplicated(keep=False)
        if duplicate_mask.any():
            duplicates = (
                resolved_registry.loc[duplicate_mask, column]
                .drop_duplicates()
                .tolist()
            )
            raise ValueError(
                f"Duplicate {column} values after registry resolution: {duplicates}"
            )


def build_resolved_registry():
    """Return one validated row per dataset enriched with source metadata."""
    logger.info("Source Registry resolution started")
    try:
        validation = validate_source_registry(raise_on_failure=True)
        sources = validation["sources"].copy()
        datasets = validation["datasets"].copy()

        resolved_registry = datasets.merge(
            sources,
            on=SOURCE_KEY,
            how="left",
            validate="many_to_one",
            suffixes=("_Dataset", "_Source"),
            indicator=True,
        )

        unmatched = resolved_registry[resolved_registry["_merge"] != "both"]
        if not unmatched.empty:
            dataset_ids = unmatched[DATASET_KEY].astype(str).tolist()
            raise ValueError(f"Datasets without a matching source: {dataset_ids}")

        resolved_registry = resolved_registry.drop(columns=["_merge"])
        resolved_registry[DERIVED_TABLE_KEY] = resolved_registry[
            OUTPUT_FILE_KEY
        ].apply(derive_table_name)

        validate_resolved_registry(resolved_registry)

        resolved_registry = resolved_registry.sort_values(
            by=[SOURCE_KEY, DATASET_KEY],
            kind="stable",
        ).reset_index(drop=True)

        logger.info(
            "Source Registry datasets resolved: %s",
            len(resolved_registry),
        )
        logger.info("Source Registry resolution completed successfully")
        return resolved_registry
    except Exception as error:
        logger.exception("Source Registry resolution failed: %s", error)
        raise


def filter_enabled_datasets(flag_column):
    """Return active datasets enabled for a supported processing scope."""
    flag = normalize_lookup_value(flag_column, "flag_column")
    allowed = {
        LOAD_ENABLED_KEY,
        QUALITY_ENABLED_KEY,
        CHRONOLOGY_ENABLED_KEY,
    }
    if flag not in allowed:
        raise ValueError(
            f"Unsupported dataset flag: {flag}. Allowed flags: {sorted(allowed)}"
        )
    registry = build_resolved_registry()
    selected = registry[registry[ACTIVE_KEY] & registry[flag]].copy()
    selected = selected.reset_index(drop=True)
    logger.info("Source Registry datasets selected for %s: %s", flag, len(selected))
    return selected


def get_active_datasets():
    """Return all active datasets."""
    registry = build_resolved_registry()
    selected = registry[registry[ACTIVE_KEY]].copy().reset_index(drop=True)
    logger.info("Active datasets selected: %s", len(selected))
    return selected


def get_load_enabled_datasets():
    """Return active datasets enabled for database loading."""
    return filter_enabled_datasets(LOAD_ENABLED_KEY)


def get_quality_enabled_datasets():
    """Return active datasets enabled for quality controls."""
    return filter_enabled_datasets(QUALITY_ENABLED_KEY)


def get_chronology_enabled_datasets():
    """Return active datasets enabled for chronology controls."""
    return filter_enabled_datasets(CHRONOLOGY_ENABLED_KEY)


def get_datasets_by_production_method(production_method, active_only=True):
    """Return datasets selected by production method."""
    method = normalize_lookup_value(production_method, "production_method")
    registry = build_resolved_registry()
    mask = normalized_casefold_series(registry[PRODUCTION_METHOD_KEY]).eq(
        method.casefold()
    )
    if active_only:
        mask = mask & registry[ACTIVE_KEY]
    selected = registry[mask].copy().reset_index(drop=True)
    logger.info(
        "Datasets selected for production method %s: %s",
        method,
        len(selected),
    )
    return selected


def get_download_enabled_datasets():
    """Return active Download datasets with a populated Download URL."""
    selected = get_datasets_by_production_method(DOWNLOAD_METHOD, active_only=True)
    if DOWNLOAD_URL_KEY not in selected.columns:
        raise KeyError("Download URL is missing from the resolved registry.")
    url_mask = selected[DOWNLOAD_URL_KEY].notna() & selected[
        DOWNLOAD_URL_KEY
    ].astype(str).str.strip().ne("")
    selected = selected[url_mask].copy().reset_index(drop=True)
    logger.info("Download-enabled datasets selected: %s", len(selected))
    return selected


def get_preparation_enabled_datasets():
    """Return active datasets produced by internal preparation."""
    return get_datasets_by_production_method(PREPARATION_METHOD, active_only=True)


def get_api_enabled_datasets():
    """Return active datasets produced through an API."""
    return get_datasets_by_production_method(API_METHOD, active_only=True)


def get_datasets_by_output_role(output_role, active_only=True):
    """Return datasets selected by their normalized technical output role."""
    role = normalize_lookup_value(output_role, "output_role")
    allowed_roles = {PRIMARY_ROLE, EXCEPTION_ROLE}
    canonical_roles = {value.casefold(): value for value in allowed_roles}
    if role.casefold() not in canonical_roles:
        raise ValueError(
            f"Unsupported output role: {role}. Allowed roles: {sorted(allowed_roles)}"
        )

    registry = build_resolved_registry()
    mask = normalized_casefold_series(registry[OUTPUT_ROLE_KEY]).eq(role.casefold())
    if active_only:
        mask = mask & registry[ACTIVE_KEY]

    selected = registry[mask].copy().reset_index(drop=True)
    logger.info("Datasets selected for output role %s: %s", role, len(selected))
    return selected


def get_api_output_datasets(source_id=None, input_dataset_id=None, active_only=True):
    """
    Return API output datasets, optionally restricted to one source/input group.

    The result is ordered with Primary before Exception so consumers can resolve
    outputs deterministically without relying on Dataset Name or Dataset ID.
    """
    selected = get_datasets_by_production_method(API_METHOD, active_only=active_only)

    if source_id is not None:
        normalized_source_id = normalize_lookup_value(source_id, "source_id")
        selected = selected[
            normalized_casefold_series(selected[SOURCE_KEY]).eq(
                normalized_source_id.casefold()
            )
        ]

    if input_dataset_id is not None:
        normalized_input_id = normalize_lookup_value(
            input_dataset_id, "input_dataset_id"
        )
        selected = selected[
            normalized_casefold_series(selected[INPUT_DATASET_KEY]).eq(
                normalized_input_id.casefold()
            )
        ]

    role_order = {PRIMARY_ROLE: 0, EXCEPTION_ROLE: 1}
    selected = selected.copy()
    selected["_role_order"] = selected[OUTPUT_ROLE_KEY].map(role_order).fillna(99)
    selected = (
        selected.sort_values(["_role_order", DATASET_KEY], kind="stable")
        .drop(columns=["_role_order"])
        .reset_index(drop=True)
    )
    logger.info("API output datasets selected: %s", len(selected))
    return selected


def get_api_output_configurations(source_id=None, input_dataset_id=None):
    """Return API output configurations keyed by Output Role."""
    datasets = get_api_output_datasets(
        source_id=source_id,
        input_dataset_id=input_dataset_id,
        active_only=True,
    )
    if datasets.empty:
        raise ValueError("No active API output datasets were found.")

    configurations = {}
    for _, row in datasets.iterrows():
        role = normalize_lookup_value(row[OUTPUT_ROLE_KEY], OUTPUT_ROLE_KEY)
        if role in configurations:
            raise ValueError(f"Multiple API outputs use Output Role: {role}")
        configurations[role] = row.to_dict()

    if PRIMARY_ROLE not in configurations:
        raise ValueError("The API output group has no Primary dataset.")

    return configurations


def get_api_endpoint(configuration):
    """Return the validated API endpoint inherited from the dataset source."""
    if not isinstance(configuration, dict):
        raise TypeError("configuration must be a dictionary.")
    if API_ENDPOINT_KEY not in configuration:
        raise KeyError("API Endpoint is missing from the resolved configuration.")
    endpoint = normalize_lookup_value(configuration[API_ENDPOINT_KEY], API_ENDPOINT_KEY)
    if not endpoint.lower().startswith(("https://", "http://")):
        raise ValueError("API Endpoint must use HTTP or HTTPS.")
    return endpoint


def _lookup_single(registry, column, value, field_name):
    """Return one registry row using a case-insensitive exact match."""
    normalized = normalize_lookup_value(value, field_name)
    matches = registry[
        normalized_casefold_series(registry[column]).eq(normalized.casefold())
    ]
    if matches.empty:
        raise KeyError(f"No dataset configuration found for {field_name}: {normalized}")
    if len(matches) > 1:
        raise ValueError(
            f"Multiple dataset configurations found for {field_name}: {normalized}"
        )
    return matches.iloc[0].copy()


def get_dataset_by_id(dataset_id):
    """Return a resolved dataset configuration by Dataset ID."""
    return _lookup_single(
        build_resolved_registry(),
        DATASET_KEY,
        dataset_id,
        "dataset_id",
    )


def get_dataset_by_output_file(output_file):
    """Return a resolved dataset configuration by output file name."""
    return _lookup_single(
        build_resolved_registry(),
        OUTPUT_FILE_KEY,
        normalize_output_file_name(output_file),
        "output_file",
    )


def get_dataset_configuration(output_file=None, dataset_id=None):
    """Return a resolved configuration dictionary using exactly one lookup."""
    lookup_count = sum(value is not None for value in (output_file, dataset_id))
    if lookup_count != 1:
        raise ValueError("Provide exactly one lookup value: output_file or dataset_id.")
    if output_file is not None:
        row = get_dataset_by_output_file(output_file)
    else:
        row = get_dataset_by_id(dataset_id)
    return row.to_dict()


def get_input_dataset(dataset_id):
    """Return the direct upstream dataset configuration, or None when absent."""
    dataset = get_dataset_by_id(dataset_id)
    input_dataset_id = normalize_optional_value(dataset.get(INPUT_DATASET_KEY))
    if input_dataset_id is None:
        return None
    return get_dataset_by_id(input_dataset_id)


def get_input_dataset_configuration(dataset_id):
    """Return the direct upstream configuration dictionary, or None."""
    input_dataset = get_input_dataset(dataset_id)
    return None if input_dataset is None else input_dataset.to_dict()


def get_dependency_chain(dataset_id):
    """Return upstream Dataset IDs from direct parent to root."""
    current = get_dataset_by_id(dataset_id)
    chain = []
    visited = {normalize_lookup_value(current[DATASET_KEY], DATASET_KEY)}

    while True:
        input_dataset_id = normalize_optional_value(current.get(INPUT_DATASET_KEY))
        if input_dataset_id is None:
            break
        if input_dataset_id in visited:
            raise RuntimeError(
                "Circular dataset dependency encountered while resolving: "
                f"{input_dataset_id}"
            )
        visited.add(input_dataset_id)
        chain.append(input_dataset_id)
        current = get_dataset_by_id(input_dataset_id)

    return chain


def build_dataset_relative_path(configuration):
    """Build a safe project-relative path from a configuration dictionary."""
    if not isinstance(configuration, dict):
        raise TypeError("configuration must be a dictionary.")
    missing_fields = [
        field
        for field in (STORAGE_DIRECTORY_KEY, OUTPUT_FILE_KEY)
        if field not in configuration
    ]
    if missing_fields:
        raise KeyError(f"Missing configuration fields: {missing_fields}")

    storage_directory = normalize_lookup_value(
        configuration[STORAGE_DIRECTORY_KEY],
        STORAGE_DIRECTORY_KEY,
    )
    output_file_name = normalize_output_file_name(configuration[OUTPUT_FILE_KEY])
    storage_path = Path(storage_directory)

    if storage_path.is_absolute():
        raise ValueError("Storage Directory must be relative to the project root.")
    if ".." in storage_path.parts:
        raise ValueError("Storage Directory cannot contain a parent reference.")

    return storage_path / output_file_name


def build_dataset_absolute_path(configuration):
    """Build and validate the absolute path of a configured dataset."""
    root = get_project_root().resolve()
    absolute_path = (root / build_dataset_relative_path(configuration)).resolve()
    try:
        absolute_path.relative_to(root)
    except ValueError as error:
        raise ValueError("Resolved dataset path is outside the project root.") from error
    return absolute_path


def get_dataset_path(dataset_id):
    """Return the absolute output path of a dataset by Dataset ID."""
    return build_dataset_absolute_path(get_dataset_by_id(dataset_id).to_dict())


def get_input_dataset_path(dataset_id):
    """Return the absolute path of the direct upstream dataset, or None."""
    input_configuration = get_input_dataset_configuration(dataset_id)
    if input_configuration is None:
        return None
    return build_dataset_absolute_path(input_configuration)


def main():
    """Validate and resolve the complete Source Registry."""
    registry = build_resolved_registry()
    logger.info(
        "Source Registry module execution completed: %s datasets available",
        len(registry),
    )


if __name__ == "__main__":
    main()
