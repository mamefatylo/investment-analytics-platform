"""
source_registry.py

Central access service for the validated Source Registry.

Purpose
-------
Provide a single trusted access layer between the validated
Source Registry and the rest of the platform.

Responsibilities
----------------
- consume validated Sources and Datasets;
- enrich Dataset configurations with Source metadata;
- expose Dataset lookup and filtering services;
- expose Production Method configurations;
- expose Access Endpoint, Connector and Parameters;
- resolve upstream Dataset dependencies;
- derive SQLite table names;
- build safe project-relative and absolute Dataset paths.

Architecture
------------
source_registry.xlsx
        |
        v
validate_source_registry.py
        |
        v
source_registry.py
        |
        +--> acquisition engines
        +--> preparation modules
        +--> database loader
        +--> quality framework

Design principles
-----------------
- Source Registry is the configuration authority.
- Validation rules are not duplicated here.
- One registry Dataset row represents one official Dataset.
- No business Source ID or Dataset ID is embedded here.
- No provider name is embedded here.
- No endpoint is embedded here.
- No connector name is embedded here.
- No output file or storage directory is embedded here.
- No provider-specific acquisition logic is embedded here.
- No secret is handled here.

Backup Source and Comments are documentation-only metadata.
They may be present in the resolved registry but never affect execution.
"""

from pathlib import Path
from typing import Any

import pandas as pd

from src.governance.validate_source_registry import (
    API_METHOD,
    DOWNLOAD_METHOD,
    PACKAGE_METHOD,
    PREPARATION_METHOD,
    derive_table_name,
    validate_source_registry,
)
from src.utils.logger import logger


# ============================================================================
# SOURCE CONTRACT
# ============================================================================

SOURCE_KEY = "Source ID"
PROVIDER_KEY = "Provider"
SOURCE_URL_KEY = "URL"
RELIABILITY_KEY = "Reliability"
SOURCE_STATUS_KEY = "Status"

BACKUP_SOURCE_KEY = "Backup Source"
COMMENTS_KEY = "Comments"


# ============================================================================
# DATASET CONTRACT
# ============================================================================

DATASET_KEY = "Dataset ID"
DATASET_NAME_KEY = "Dataset Name"

PRODUCTION_METHOD_KEY = "Production Method"
CONNECTOR_KEY = "Connector"
ACCESS_ENDPOINT_KEY = "Access Endpoint"
PARAMETERS_KEY = "Parameters"

INPUT_DATASET_KEY = "Input Dataset IDs"

OUTPUT_FILE_KEY = "Output File Name"
STORAGE_DIRECTORY_KEY = "Storage Directory"
FREQUENCY_KEY = "Frequency"

LOAD_ENABLED_KEY = "Load Enabled"
QUALITY_ENABLED_KEY = "Quality Enabled"
CHRONOLOGY_ENABLED_KEY = "Chronology Enabled"
ACTIVE_KEY = "Active"

DERIVED_TABLE_KEY = "Derived Table Name"


# ============================================================================
# INTERNAL CONTRACTS
# ============================================================================

PROCESSING_FLAGS = frozenset(
    {
        LOAD_ENABLED_KEY,
        QUALITY_ENABLED_KEY,
        CHRONOLOGY_ENABLED_KEY,
    }
)

PRODUCTION_METHOD_VALUES = frozenset(
    {
        DOWNLOAD_METHOD,
        API_METHOD,
        PACKAGE_METHOD,
        PREPARATION_METHOD,
    }
)


# ============================================================================
# PROJECT ROOT
# ============================================================================

def get_project_root() -> Path:
    """
    Return the project root derived from this module location.

    The resolved project root must contain the src directory.
    """

    root = (
        Path(__file__)
        .resolve()
        .parents[2]
    )

    if not (root / "src").is_dir():

        raise RuntimeError(
            "Unable to determine project root "
            f"from module location: {__file__}"
        )

    return root


# ============================================================================
# NORMALIZATION
# ============================================================================

def normalize_lookup_value(
    value: Any,
    field_name: str,
) -> str:
    """
    Normalize a required lookup value.

    Missing and blank values are rejected.
    """

    if value is None:

        raise ValueError(
            f"{field_name} is required."
        )

    if not isinstance(
        value,
        (
            dict,
            list,
            tuple,
            set,
        ),
    ):

        try:

            if pd.isna(value):

                raise ValueError(
                    f"{field_name} is required."
                )

        except TypeError:

            pass

    normalized = str(
        value
    ).strip()

    if not normalized:

        raise ValueError(
            f"{field_name} cannot be empty."
        )

    return normalized


def normalize_optional_value(
    value: Any,
):
    """
    Normalize an optional registry value.

    Strings are stripped.
    Missing scalar values become None.
    Structured values are preserved.
    """

    if value is None:
        return None

    if isinstance(
        value,
        str,
    ):

        normalized = value.strip()

        return normalized or None

    if isinstance(
        value,
        (
            dict,
            list,
            tuple,
            set,
        ),
    ):

        return value

    try:

        if pd.isna(value):
            return None

    except (
        TypeError,
        ValueError,
    ):

        pass

    return value


def normalized_casefold_series(
    series: pd.Series,
) -> pd.Series:
    """
    Normalize a Series for case-insensitive exact matching.
    """

    if not isinstance(
        series,
        pd.Series,
    ):

        raise TypeError(
            "series must be a pandas Series."
        )

    return (
        series
        .fillna("")
        .astype(str)
        .str.strip()
        .str.casefold()
    )


def normalize_output_file_name(
    output_file: Any,
) -> str:
    """
    Normalize an Output File Name.

    For lookup purposes, only the file name is retained.
    """

    value = normalize_lookup_value(
        output_file,
        OUTPUT_FILE_KEY,
    )

    file_name = Path(
        value
    ).name

    if file_name in {
        "",
        ".",
        "..",
    }:

        raise ValueError(
            "Output File Name must identify a file."
        )

    if not Path(
        file_name
    ).suffix:

        raise ValueError(
            "Output File Name must include "
            "a file extension."
        )

    return file_name


# ============================================================================
# RESOLVED REGISTRY CONTRACT
# ============================================================================

def validate_resolved_registry(
    resolved_registry: pd.DataFrame,
) -> None:
    """
    Validate runtime invariants required by registry consumers.

    Detailed workbook validation belongs exclusively to
    validate_source_registry.py.
    """

    if not isinstance(
        resolved_registry,
        pd.DataFrame,
    ):

        raise TypeError(
            "resolved_registry must be "
            "a pandas DataFrame."
        )

    required_columns = {
        SOURCE_KEY,
        PROVIDER_KEY,
        DATASET_KEY,
        DATASET_NAME_KEY,
        PRODUCTION_METHOD_KEY,
        CONNECTOR_KEY,
        ACCESS_ENDPOINT_KEY,
        PARAMETERS_KEY,
        INPUT_DATASET_KEY,
        OUTPUT_FILE_KEY,
        STORAGE_DIRECTORY_KEY,
        FREQUENCY_KEY,
        ACTIVE_KEY,
        LOAD_ENABLED_KEY,
        QUALITY_ENABLED_KEY,
        CHRONOLOGY_ENABLED_KEY,
        DERIVED_TABLE_KEY,
    }

    missing_columns = sorted(
        required_columns
        - set(
            resolved_registry.columns
        )
    )

    if missing_columns:

        raise ValueError(
            "Resolved Source Registry is missing "
            "required columns: "
            f"{missing_columns}"
        )

    if resolved_registry.empty:

        raise ValueError(
            "Resolved Source Registry is empty."
        )

    uniqueness_columns = (
        DATASET_KEY,
        OUTPUT_FILE_KEY,
        DERIVED_TABLE_KEY,
    )

    for column in uniqueness_columns:

        values = normalized_casefold_series(
            resolved_registry[
                column
            ]
        )

        blank_mask = (
            values.eq("")
        )

        if blank_mask.any():

            dataset_ids = (
                resolved_registry.loc[
                    blank_mask,
                    DATASET_KEY,
                ]
                .astype(str)
                .tolist()
            )

            raise ValueError(
                f"{column} contains blank values "
                f"for Datasets: {dataset_ids}"
            )

        duplicate_mask = (
            values.duplicated(
                keep=False
            )
        )

        if duplicate_mask.any():

            duplicates = (
                resolved_registry.loc[
                    duplicate_mask,
                    column,
                ]
                .drop_duplicates()
                .tolist()
            )

            raise ValueError(
                f"Duplicate {column} values "
                "after registry resolution: "
                f"{duplicates}"
            )

    boolean_columns = (
        ACTIVE_KEY,
        LOAD_ENABLED_KEY,
        QUALITY_ENABLED_KEY,
        CHRONOLOGY_ENABLED_KEY,
    )

    for column in boolean_columns:

        invalid_mask = (
            ~resolved_registry[
                column
            ]
            .map(
                lambda value:
                    isinstance(
                        value,
                        bool,
                    )
            )
        )

        if invalid_mask.any():

            dataset_ids = (
                resolved_registry.loc[
                    invalid_mask,
                    DATASET_KEY,
                ]
                .astype(str)
                .tolist()
            )

            raise TypeError(
                f"{column} contains non-boolean "
                "resolved values for Datasets: "
                f"{dataset_ids}"
            )

    for index, parameters in (
        resolved_registry[
            PARAMETERS_KEY
        ].items()
    ):

        if not isinstance(
            parameters,
            dict,
        ):

            dataset_id = (
                resolved_registry.at[
                    index,
                    DATASET_KEY,
                ]
            )

            raise TypeError(
                "Resolved Parameters must be "
                "a dictionary for Dataset "
                f"{dataset_id}."
            )


        for index, input_dataset_ids in (
            resolved_registry[
                INPUT_DATASET_KEY
                ].items()
        ):
            
            dataset_id = (
                resolved_registry.at[
                    index,
                    DATASET_KEY,
                ]
            )
            
            if not isinstance(
                input_dataset_ids,
                list,
            ):
                
                raise TypeError(
                    "Resolved Input Dataset IDs "
                    "must be a list for Dataset "
                    f"{dataset_id}."
                )

        normalized_ids = []
        comparison_keys = set()

        for input_dataset_id in input_dataset_ids:

            normalized_id = normalize_lookup_value(
                input_dataset_id,
                INPUT_DATASET_KEY,
            )

            comparison_key = (
                normalized_id.casefold()
            )

            if comparison_key in comparison_keys:

                raise ValueError(
                    "Resolved Input Dataset IDs "
                    "contains duplicate dependencies "
                    f"for Dataset {dataset_id}: "
                    f"{normalized_id}"
                )

            comparison_keys.add(
                comparison_key
            )

            normalized_ids.append(
                normalized_id
            )

# ============================================================================
# REGISTRY RESOLUTION
# ============================================================================

def build_resolved_registry() -> pd.DataFrame:
    """
    Return one validated row per Dataset enriched with Source metadata.

    The workbook is validated before resolution.

    Parameters are already normalized as dictionaries by
    validate_source_registry.py.

    Returns
    -------
    pandas.DataFrame
        Fully validated and normalized Dataset configurations.
    """

    logger.info(
        "Source Registry resolution started"
    )

    try:

        validation = validate_source_registry(
            raise_on_failure=True
        )

        if not validation.get(
            "is_valid",
            False,
        ):

            raise RuntimeError(
                "Source Registry validation "
                "did not succeed."
            )

        sources = (
            validation[
                "sources"
            ]
            .copy(
                deep=True
            )
        )

        datasets = (
            validation[
                "datasets"
            ]
            .copy(
                deep=True
            )
        )

        resolved_registry = datasets.merge(
            sources,
            on=SOURCE_KEY,
            how="left",
            validate="many_to_one",
            suffixes=(
                "_Dataset",
                "_Source",
            ),
            indicator=True,
        )

        unmatched = resolved_registry[
            resolved_registry[
                "_merge"
            ].ne(
                "both"
            )
        ]

        if not unmatched.empty:

            dataset_ids = (
                unmatched[
                    DATASET_KEY
                ]
                .astype(str)
                .tolist()
            )

            raise RuntimeError(
                "Datasets without matching "
                "Source configuration: "
                f"{dataset_ids}"
            )

        resolved_registry = (
            resolved_registry
            .drop(
                columns=[
                    "_merge",
                ]
            )
        )

        resolved_registry[
            DERIVED_TABLE_KEY
        ] = (
            resolved_registry[
                OUTPUT_FILE_KEY
            ]
            .apply(
                derive_table_name
            )
        )

        validate_resolved_registry(
            resolved_registry
        )

        resolved_registry = (
            resolved_registry
            .sort_values(
                by=[
                    SOURCE_KEY,
                    DATASET_KEY,
                ],
                kind="stable",
            )
            .reset_index(
                drop=True
            )
        )

        logger.info(
            "Source Registry Datasets "
            "resolved: %s",
            len(
                resolved_registry
            ),
        )

        logger.info(
            "Source Registry resolution "
            "completed successfully"
        )

        return resolved_registry.copy(
            deep=True
        )

    except Exception as error:

        logger.exception(
            "Source Registry resolution "
            "failed: %s",
            error,
        )

        raise


# ============================================================================
# REGISTRY INPUT SAFETY
# ============================================================================

def resolve_registry_input(
    registry: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Return a safe resolved registry.

    When registry is None, the Source Registry is validated and resolved.

    When a resolved registry is supplied, the runtime contract is
    validated and a defensive copy is returned.

    This allows orchestration processes to resolve the registry once
    per run and reuse it without repeatedly reading the Excel file.
    """

    if registry is None:

        return build_resolved_registry()

    if not isinstance(
        registry,
        pd.DataFrame,
    ):

        raise TypeError(
            "registry must be a pandas DataFrame "
            "or None."
        )

    resolved = registry.copy(
        deep=True
    )

    validate_resolved_registry(
        resolved
    )

    return resolved


# ============================================================================
# GENERIC FILTERING
# ============================================================================

def filter_enabled_datasets(
    flag_column: str,
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active Datasets enabled for one processing scope.
    """

    flag = normalize_lookup_value(
        flag_column,
        "flag_column",
    )

    if flag not in PROCESSING_FLAGS:

        raise ValueError(
            "Unsupported Dataset flag: "
            f"{flag}. Allowed flags: "
            f"{sorted(PROCESSING_FLAGS)}"
        )

    resolved = resolve_registry_input(
        registry
    )

    selected = (
        resolved[
            resolved[
                ACTIVE_KEY
            ]
            & resolved[
                flag
            ]
        ]
        .copy(
            deep=True
        )
        .reset_index(
            drop=True
        )
    )

    logger.info(
        "Source Registry Datasets selected "
        "for %s: %s",
        flag,
        len(selected),
    )

    return selected


def get_active_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return all active Datasets.
    """

    resolved = resolve_registry_input(
        registry
    )

    selected = (
        resolved[
            resolved[
                ACTIVE_KEY
            ]
        ]
        .copy(
            deep=True
        )
        .reset_index(
            drop=True
        )
    )

    logger.info(
        "Active Datasets selected: %s",
        len(selected),
    )

    return selected


def get_load_enabled_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active Datasets enabled for database loading.
    """

    return filter_enabled_datasets(
        LOAD_ENABLED_KEY,
        registry=registry,
    )


def get_quality_enabled_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active Datasets enabled for quality processing.
    """

    return filter_enabled_datasets(
        QUALITY_ENABLED_KEY,
        registry=registry,
    )


def get_chronology_enabled_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active Datasets enabled for chronology processing.
    """

    return filter_enabled_datasets(
        CHRONOLOGY_ENABLED_KEY,
        registry=registry,
    )


# ============================================================================
# PRODUCTION METHOD SELECTION
# ============================================================================

def normalize_production_method_lookup(
    production_method: Any,
) -> str:
    """
    Normalize and validate a Production Method lookup.
    """

    requested = normalize_lookup_value(
        production_method,
        PRODUCTION_METHOD_KEY,
    )

    canonical = {
        method.casefold(): method
        for method
        in PRODUCTION_METHOD_VALUES
    }

    key = requested.casefold()

    if key not in canonical:

        raise ValueError(
            "Unsupported Production Method: "
            f"{requested}. Allowed values: "
            f"{sorted(PRODUCTION_METHOD_VALUES)}"
        )

    return canonical[
        key
    ]


def get_datasets_by_production_method(
    production_method: Any,
    *,
    active_only: bool = True,
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return Datasets selected by Production Method.
    """

    method = normalize_production_method_lookup(
        production_method
    )

    resolved = resolve_registry_input(
        registry
    )

    mask = (
        normalized_casefold_series(
            resolved[
                PRODUCTION_METHOD_KEY
            ]
        )
        .eq(
            method.casefold()
        )
    )

    if active_only:

        mask = (
            mask
            & resolved[
                ACTIVE_KEY
            ]
        )

    selected = (
        resolved[
            mask
        ]
        .copy(
            deep=True
        )
        .reset_index(
            drop=True
        )
    )

    logger.info(
        "Datasets selected for "
        "Production Method %s: %s",
        method,
        len(selected),
    )

    return selected


def get_download_enabled_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active Download acquisition Datasets.
    """

    return get_datasets_by_production_method(
        DOWNLOAD_METHOD,
        active_only=True,
        registry=registry,
    )


def get_api_enabled_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active API acquisition Datasets.
    """

    return get_datasets_by_production_method(
        API_METHOD,
        active_only=True,
        registry=registry,
    )


def get_package_enabled_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active Package acquisition Datasets.
    """

    return get_datasets_by_production_method(
        PACKAGE_METHOD,
        active_only=True,
        registry=registry,
    )


def get_preparation_enabled_datasets(
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return active Preparation Datasets.

    Preparation is executed under src/preparation rather than
    src/acquisition.
    """

    return get_datasets_by_production_method(
        PREPARATION_METHOD,
        active_only=True,
        registry=registry,
    )


# ============================================================================
# CONNECTOR SELECTION
# ============================================================================

def get_datasets_by_connector(
    connector: Any,
    *,
    production_method: Any | None = None,
    active_only: bool = True,
    registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Return Datasets associated with one technical Connector.

    The function contains no knowledge of available connector names.
    Connector discovery and implementation resolution belong to
    src.acquisition.connectors.resolver.
    """

    requested_connector = (
        normalize_lookup_value(
            connector,
            CONNECTOR_KEY,
        )
    )

    if production_method is None:

        resolved = resolve_registry_input(
            registry
        )

        if active_only:

            resolved = (
                resolved[
                    resolved[
                        ACTIVE_KEY
                    ]
                ]
                .copy(
                    deep=True
                )
            )

    else:

        resolved = (
            get_datasets_by_production_method(
                production_method,
                active_only=active_only,
                registry=registry,
            )
        )

    mask = (
        normalized_casefold_series(
            resolved[
                CONNECTOR_KEY
            ]
        )
        .eq(
            requested_connector.casefold()
        )
    )

    selected = (
        resolved[
            mask
        ]
        .copy(
            deep=True
        )
        .reset_index(
            drop=True
        )
    )

    logger.info(
        "Datasets selected for "
        "Connector %s: %s",
        requested_connector,
        len(selected),
    )

    return selected


# ============================================================================
# DATASET LOOKUP
# ============================================================================

def _lookup_single(
    registry: pd.DataFrame,
    column: str,
    value: Any,
    field_name: str,
) -> pd.Series:
    """
    Return exactly one resolved Dataset configuration.
    """

    if not isinstance(
        registry,
        pd.DataFrame,
    ):

        raise TypeError(
            "registry must be a pandas DataFrame."
        )

    requested = normalize_lookup_value(
        value,
        field_name,
    )

    if column not in registry.columns:

        raise KeyError(
            f"Registry column is missing: {column}"
        )

    matches = registry[
        normalized_casefold_series(
            registry[
                column
            ]
        )
        .eq(
            requested.casefold()
        )
    ]

    if matches.empty:

        raise KeyError(
            "No Dataset configuration found "
            f"for {field_name}: {requested}"
        )

    if len(matches) > 1:

        raise ValueError(
            "Multiple Dataset configurations "
            f"found for {field_name}: "
            f"{requested}"
        )

    return (
        matches
        .iloc[0]
        .copy(
            deep=True
        )
    )


def get_dataset_by_id(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> pd.Series:
    """
    Return one resolved Dataset by Dataset ID.
    """

    resolved = resolve_registry_input(
        registry
    )

    return _lookup_single(
        resolved,
        DATASET_KEY,
        dataset_id,
        "dataset_id",
    )


def get_dataset_by_output_file(
    output_file: Any,
    registry: pd.DataFrame | None = None,
) -> pd.Series:
    """
    Return one resolved Dataset by Output File Name.
    """

    resolved = resolve_registry_input(
        registry
    )

    file_name = normalize_output_file_name(
        output_file
    )

    return _lookup_single(
        resolved,
        OUTPUT_FILE_KEY,
        file_name,
        "output_file",
    )


def get_dataset_configuration(
    *,
    dataset_id: Any | None = None,
    output_file: Any | None = None,
    registry: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """
    Return one resolved Dataset configuration as a dictionary.

    Exactly one lookup argument must be supplied.
    """

    lookup_count = sum(
        value is not None
        for value in (
            dataset_id,
            output_file,
        )
    )

    if lookup_count != 1:

        raise ValueError(
            "Provide exactly one lookup: "
            "dataset_id or output_file."
        )

    if dataset_id is not None:

        row = get_dataset_by_id(
            dataset_id,
            registry=registry,
        )

    else:

        row = get_dataset_by_output_file(
            output_file,
            registry=registry,
        )

    return row.to_dict()

# ============================================================================
# EXECUTION CONFIGURATION
# ============================================================================

def validate_configuration_mapping(
    configuration: dict[str, Any],
) -> None:
    """
    Validate a Dataset configuration mapping.
    """

    if not isinstance(
        configuration,
        dict,
    ):

        raise TypeError(
            "configuration must be a dictionary."
        )


def get_access_endpoint(
    configuration: dict[str, Any],
) -> str | None:
    """
    Return the optional Dataset Access Endpoint.

    Whether the endpoint is mandatory is determined by
    validate_source_registry.py according to Production Method.
    """

    validate_configuration_mapping(
        configuration
    )

    if (
        ACCESS_ENDPOINT_KEY
        not in configuration
    ):

        raise KeyError(
            "Access Endpoint is missing "
            "from Dataset configuration."
        )

    endpoint = normalize_optional_value(
        configuration[
            ACCESS_ENDPOINT_KEY
        ]
    )

    if (
        endpoint is not None
        and not isinstance(
            endpoint,
            str,
        )
    ):

        raise TypeError(
            "Access Endpoint must resolve "
            "to a string or None."
        )

    return endpoint


def get_connector(
    configuration: dict[str, Any],
) -> str | None:
    """
    Return the optional technical Connector identifier.

    Connector implementation resolution belongs to
    src.acquisition.connectors.resolver.
    """

    validate_configuration_mapping(
        configuration
    )

    if (
        CONNECTOR_KEY
        not in configuration
    ):

        raise KeyError(
            "Connector is missing from "
            "Dataset configuration."
        )

    connector = normalize_optional_value(
        configuration[
            CONNECTOR_KEY
        ]
    )

    if (
        connector is not None
        and not isinstance(
            connector,
            str,
        )
    ):

        raise TypeError(
            "Connector must resolve "
            "to a string or None."
        )

    return connector


def get_parameters(
    configuration: dict[str, Any],
) -> dict[str, Any]:
    
    """
    Return a defensive copy of normalized Dataset Parameters.

    validate_source_registry.py guarantees that resolved
    Parameters are dictionaries.
    """

    validate_configuration_mapping(
        configuration
    )

    if (
        PARAMETERS_KEY
        not in configuration
    ):

        raise KeyError(
            "Parameters are missing from "
            "Dataset configuration."
        )

    parameters = configuration[
        PARAMETERS_KEY
    ]

    if parameters is None:
        return {}

    if not isinstance(
        parameters,
        dict,
    ):

        raise TypeError(
            "Resolved Dataset Parameters "
            "must be a dictionary."
        )

    return parameters.copy()


def get_production_method(
    configuration: dict[str, Any],
) -> str:
    """
    Return the normalized Production Method.
    """

    validate_configuration_mapping(
        configuration
    )

    if (
        PRODUCTION_METHOD_KEY
        not in configuration
    ):

        raise KeyError(
            "Production Method is missing "
            "from Dataset configuration."
        )

    return normalize_production_method_lookup(
        configuration[
            PRODUCTION_METHOD_KEY
        ]
    )


# ============================================================================
# DATASET DEPENDENCIES
# ============================================================================

def get_input_dataset_ids(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> list[str]:
    """
    Return direct upstream Dataset IDs.

    The returned order matches the order declared in the
    Source Registry.

    Returns an empty list when no upstream Dataset exists.
    """

    resolved = resolve_registry_input(
        registry
    )

    dataset = get_dataset_by_id(
        dataset_id,
        registry=resolved,
    )

    input_dataset_ids = dataset.get(
        INPUT_DATASET_KEY
    )

    if not isinstance(
        input_dataset_ids,
        list,
    ):

        raise TypeError(
            "Resolved Input Dataset IDs "
            "must be a list."
        )

    normalized_ids = []

    for input_dataset_id in input_dataset_ids:

        normalized_ids.append(
            normalize_lookup_value(
                input_dataset_id,
                INPUT_DATASET_KEY,
            )
        )

    return normalized_ids


def get_input_datasets(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> list[pd.Series]:
    """
    Return direct upstream Dataset rows.

    Returns an empty list when the Dataset has no direct
    upstream dependencies.
    """

    resolved = resolve_registry_input(
        registry
    )

    input_dataset_ids = get_input_dataset_ids(
        dataset_id,
        registry=resolved,
    )

    return [
        get_dataset_by_id(
            input_dataset_id,
            registry=resolved,
        )
        for input_dataset_id
        in input_dataset_ids
    ]


def get_input_dataset_configurations(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    """
    Return direct upstream Dataset configurations.

    Returns
    -------
    list[dict]
        One configuration per direct upstream Dataset,
        preserving Source Registry dependency order.

        An empty list is returned when no dependency exists.
    """

    input_datasets = get_input_datasets(
        dataset_id,
        registry=registry,
    )

    return [
        dataset.to_dict()
        for dataset
        in input_datasets
    ]


def get_dependency_graph(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> dict[str, list[str]]:
    """
    Return the reachable upstream dependency graph.

    The result maps every visited Dataset ID to its direct
    upstream Dataset IDs.

    Example
    -------
    For:

        DS-003 -> DS-001
        DS-003 -> DS-002
        DS-002 -> DS-001
        DS-001 -> DS-000

    the result is conceptually:

        {
            "DS-003": ["DS-001", "DS-002"],
            "DS-001": ["DS-000"],
            "DS-000": [],
            "DS-002": ["DS-001"],
        }

    Circular dependencies should already be rejected by
    validate_source_registry.py. Runtime cycle detection remains
    as defense in depth for externally supplied registries.
    """

    resolved = resolve_registry_input(
        registry
    )

    root = get_dataset_by_id(
        dataset_id,
        registry=resolved,
    )

    root_id = normalize_lookup_value(
        root[
            DATASET_KEY
        ],
        DATASET_KEY,
    )

    graph: dict[
        str,
        list[str],
    ] = {}

    visiting = set()
    visited = set()

    def visit(
        current_dataset_id: str,
    ) -> None:

        current_key = (
            current_dataset_id.casefold()
        )

        if current_key in visiting:

            raise RuntimeError(
                "Circular Dataset dependency "
                "encountered while resolving: "
                f"{current_dataset_id}"
            )

        if current_key in visited:
            return

        visiting.add(
            current_key
        )

        input_ids = get_input_dataset_ids(
            current_dataset_id,
            registry=resolved,
        )

        graph[
            current_dataset_id
        ] = input_ids.copy()

        for input_dataset_id in input_ids:

            get_dataset_by_id(
                input_dataset_id,
                registry=resolved,
            )

            visit(
                input_dataset_id
            )

        visiting.remove(
            current_key
        )

        visited.add(
            current_key
        )

    visit(
        root_id
    )

    return graph


def get_dependency_order(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> list[str]:
    """
    Return all transitive upstream Dataset IDs in dependency-first order.

    Each Dataset ID appears exactly once.

    The requested Dataset itself is not included.

    Example
    -------
    For:

        DS-003 -> DS-001
        DS-003 -> DS-002
        DS-002 -> DS-001
        DS-001 -> DS-000

    the result is:

        ["DS-000", "DS-001", "DS-002"]

    This ordering is suitable for dependency-aware orchestration.
    """

    resolved = resolve_registry_input(
        registry
    )

    root = get_dataset_by_id(
        dataset_id,
        registry=resolved,
    )

    root_id = normalize_lookup_value(
        root[
            DATASET_KEY
        ],
        DATASET_KEY,
    )

    ordered = []
    visited = set()
    visiting = set()

    def visit(
        current_dataset_id: str,
    ) -> None:
        """
        Recursively visit upstream dependencies.
        """

        current_key = (
            current_dataset_id
            .casefold()
        )

        if current_key in visiting:

            raise RuntimeError(
                "Circular Dataset dependency "
                "encountered while resolving: "
                f"{current_dataset_id}"
            )

        if current_key in visited:
            return

        visiting.add(
            current_key
        )

        input_ids = get_input_dataset_ids(
            current_dataset_id,
            registry=resolved,
        )

        for input_dataset_id in input_ids:

            visit(
                input_dataset_id
            )

        visiting.remove(
            current_key
        )

        visited.add(
            current_key
        )

        if current_key != root_id.casefold():

            ordered.append(
                current_dataset_id
            )

    visit(
        root_id
    )

    return ordered

# ============================================================================
# DATASET PATHS
# ============================================================================

def build_dataset_relative_path(
    configuration: dict[str, Any],
) -> Path:
    """
    Build a safe project-relative Dataset path.

    The Storage Directory must be project-relative and cannot
    contain parent-directory references.
    """

    validate_configuration_mapping(
        configuration
    )

    required_fields = (
        STORAGE_DIRECTORY_KEY,
        OUTPUT_FILE_KEY,
    )

    missing_fields = [
        field
        for field in required_fields
        if field not in configuration
    ]

    if missing_fields:

        raise KeyError(
            "Missing Dataset path fields: "
            f"{missing_fields}"
        )

    storage_directory = (
        normalize_lookup_value(
            configuration[
                STORAGE_DIRECTORY_KEY
            ],
            STORAGE_DIRECTORY_KEY,
        )
    )

    output_file_name = (
        normalize_output_file_name(
            configuration[
                OUTPUT_FILE_KEY
            ]
        )
    )

    storage_path = Path(
        storage_directory
    )

    if storage_path.is_absolute():

        raise ValueError(
            "Storage Directory must be "
            "relative to project root."
        )

    if ".." in storage_path.parts:

        raise ValueError(
            "Storage Directory cannot contain "
            "a parent reference."
        )

    relative_path = (
        storage_path
        / output_file_name
    )

    if relative_path.is_absolute():

        raise ValueError(
            "Dataset path must be "
            "project-relative."
        )

    if ".." in relative_path.parts:

        raise ValueError(
            "Dataset relative path cannot "
            "contain a parent reference."
        )

    return relative_path


def build_dataset_absolute_path(
    configuration: dict[str, Any],
) -> Path:
    """
    Build and validate the absolute Dataset path.

    The final resolved path must remain inside the project root.
    """

    root = (
        get_project_root()
        .resolve()
    )

    relative_path = (
        build_dataset_relative_path(
            configuration
        )
    )

    absolute_path = (
        root
        / relative_path
    ).resolve()

    try:

        absolute_path.relative_to(
            root
        )

    except ValueError as error:

        raise ValueError(
            "Resolved Dataset path is outside "
            "the project root."
        ) from error

    return absolute_path


def get_dataset_path(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> Path:
    """
    Return the absolute output path of a Dataset by Dataset ID.
    """

    resolved = resolve_registry_input(
        registry
    )

    configuration = (
        get_dataset_by_id(
            dataset_id,
            registry=resolved,
        )
        .to_dict()
    )

    return build_dataset_absolute_path(
        configuration
    )


def get_input_dataset_paths(
    dataset_id: Any,
    registry: pd.DataFrame | None = None,
) -> list:
    """
    Return absolute paths of all direct upstream Datasets.

    The path order matches Input Dataset IDs declared in the
    Source Registry.

    Returns an empty list when no upstream Dataset exists.
    """

    resolved = resolve_registry_input(
        registry
    )

    configurations = (
        get_input_dataset_configurations(
            dataset_id,
            registry=resolved,
        )
    )

    return [
        build_dataset_absolute_path(
            configuration
        )
        for configuration
        in configurations
    ]

# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Validate and resolve the complete Source Registry.

    This entry point performs no acquisition or transformation.
    """

    registry = build_resolved_registry()

    method_counts = (
        registry[
            PRODUCTION_METHOD_KEY
        ]
        .value_counts()
        .to_dict()
    )

    active_count = int(
        registry[
            ACTIVE_KEY
        ].sum()
    )

    logger.info(
        "Source Registry module execution "
        "completed: %s Datasets available",
        len(
            registry
        ),
    )

    logger.info(
        "Source Registry active Datasets: %s",
        active_count,
    )

    logger.info(
        "Production Method distribution: %s",
        method_counts,
    )


if __name__ == "__main__":
    main()