"""
validate_source_registry.py

Validate the Source Registry before acquisition, preparation,
database loading, quality processing, or downstream analytics.

The Source Registry is the configuration authority for:
- sources;
- datasets;
- production methods;
- connectors;
- access endpoints;
- dataset parameters;
- dataset dependencies;
- output locations;
- processing flags.

Execution contracts
-------------------
Download
    Requires an Access Endpoint.
    Connector is optional.
    Input Dataset IDs must contain zero dependencies.

API
    Requires an Access Endpoint.
    Requires a Connector.
    Input Dataset IDs may contain zero or one dependency.

Package
    Requires a Connector.
    Access Endpoint is optional.
    Input Dataset IDs may contain zero or one dependency.

Preparation
    Requires one or more Input Dataset IDs.
    Connector is not allowed.
    Access Endpoint is not allowed.

Design principles
-----------------
- The Source Registry is the source of truth.
- Acquisition produces source-faithful datasets.
- Preparation performs internal transformations.
- Quality rules are controlled by Dataset flags.
- Backup Source and Comments are documentation-only metadata.
- No business Source ID, Dataset ID, provider, endpoint,
  connector, file name, or storage path is embedded here.
"""

import json
import re
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from src.config import SOURCE_REGISTRY_FILE
from src.utils.logger import logger


# ============================================================================
# WORKBOOK CONTRACT
# ============================================================================

SOURCES_SHEET = "Sources"
DATASETS_SHEET = "Datasets"

REQUIRED_SHEETS = {
    SOURCES_SHEET,
    DATASETS_SHEET,
}


# ============================================================================
# SOURCE CONTRACT
# ============================================================================

SOURCE_ID_COLUMN = "Source ID"
PROVIDER_COLUMN = "Provider"
SOURCE_URL_COLUMN = "URL"
RELIABILITY_COLUMN = "Reliability"
SOURCE_STATUS_COLUMN = "Status"

BACKUP_SOURCE_COLUMN = "Backup Source"
COMMENTS_COLUMN = "Comments"

REQUIRED_SOURCE_COLUMNS = [
    SOURCE_ID_COLUMN,
    PROVIDER_COLUMN,
    SOURCE_URL_COLUMN,
    RELIABILITY_COLUMN,
    SOURCE_STATUS_COLUMN,
]

REQUIRED_SOURCE_VALUE_COLUMNS = [
    SOURCE_ID_COLUMN,
    PROVIDER_COLUMN,
    RELIABILITY_COLUMN,
    SOURCE_STATUS_COLUMN,
]

SOURCE_TEXT_COLUMNS = [
    SOURCE_ID_COLUMN,
    PROVIDER_COLUMN,
    SOURCE_URL_COLUMN,
    RELIABILITY_COLUMN,
    SOURCE_STATUS_COLUMN,
    BACKUP_SOURCE_COLUMN,
    COMMENTS_COLUMN,
]


# ============================================================================
# DATASET CONTRACT
# ============================================================================

DATASET_ID_COLUMN = "Dataset ID"
DATASET_SOURCE_ID_COLUMN = "Source ID"
DATASET_NAME_COLUMN = "Dataset Name"

PRODUCTION_METHOD_COLUMN = "Production Method"
CONNECTOR_COLUMN = "Connector"
ACCESS_ENDPOINT_COLUMN = "Access Endpoint"
PARAMETERS_COLUMN = "Parameters"
INPUT_DATASET_COLUMN = "Input Dataset IDs"

OUTPUT_FILE_COLUMN = "Output File Name"
STORAGE_DIRECTORY_COLUMN = "Storage Directory"
FREQUENCY_COLUMN = "Frequency"

LOAD_ENABLED_COLUMN = "Load Enabled"
QUALITY_ENABLED_COLUMN = "Quality Enabled"
CHRONOLOGY_ENABLED_COLUMN = "Chronology Enabled"
ACTIVE_COLUMN = "Active"

REQUIRED_DATASET_COLUMNS = [
    DATASET_ID_COLUMN,
    DATASET_SOURCE_ID_COLUMN,
    DATASET_NAME_COLUMN,
    PRODUCTION_METHOD_COLUMN,
    CONNECTOR_COLUMN,
    ACCESS_ENDPOINT_COLUMN,
    PARAMETERS_COLUMN,
    INPUT_DATASET_COLUMN,
    OUTPUT_FILE_COLUMN,
    STORAGE_DIRECTORY_COLUMN,
    FREQUENCY_COLUMN,
    LOAD_ENABLED_COLUMN,
    QUALITY_ENABLED_COLUMN,
    CHRONOLOGY_ENABLED_COLUMN,
    ACTIVE_COLUMN,
]

REQUIRED_DATASET_VALUE_COLUMNS = [
    DATASET_ID_COLUMN,
    DATASET_SOURCE_ID_COLUMN,
    DATASET_NAME_COLUMN,
    PRODUCTION_METHOD_COLUMN,
    OUTPUT_FILE_COLUMN,
    INPUT_DATASET_COLUMN,
    STORAGE_DIRECTORY_COLUMN,
    FREQUENCY_COLUMN,
    LOAD_ENABLED_COLUMN,
    QUALITY_ENABLED_COLUMN,
    CHRONOLOGY_ENABLED_COLUMN,
    ACTIVE_COLUMN,
]

DATASET_TEXT_COLUMNS = [
    DATASET_ID_COLUMN,
    DATASET_SOURCE_ID_COLUMN,
    DATASET_NAME_COLUMN,
    PRODUCTION_METHOD_COLUMN,
    CONNECTOR_COLUMN,
    ACCESS_ENDPOINT_COLUMN,
    PARAMETERS_COLUMN,
    INPUT_DATASET_COLUMN,
    OUTPUT_FILE_COLUMN,
    STORAGE_DIRECTORY_COLUMN,
    FREQUENCY_COLUMN,
]

BOOLEAN_COLUMNS = [
    LOAD_ENABLED_COLUMN,
    QUALITY_ENABLED_COLUMN,
    CHRONOLOGY_ENABLED_COLUMN,
    ACTIVE_COLUMN,
]


# ============================================================================
# CONTROLLED VOCABULARIES
# ============================================================================

DOWNLOAD_METHOD = "Download"
API_METHOD = "API"
PACKAGE_METHOD = "Package"
PREPARATION_METHOD = "Preparation"

PRODUCTION_METHODS = {
    DOWNLOAD_METHOD,
    API_METHOD,
    PACKAGE_METHOD,
    PREPARATION_METHOD,
}

SOURCE_STATUSES = {
    "Active",
    "Inactive",
}

RELIABILITY_VALUES = {
    "High",
    "Medium",
    "Low",
}

SUPPORTED_FILE_EXTENSIONS = {
    ".csv",
    ".json",
    ".parquet",
    ".xls",
    ".xlsx",
}

ALLOWED_URL_SCHEMES = {
    "http",
    "https",
}

TABLE_NAME_PATTERN = re.compile(
    r"^[a-z_][a-z0-9_]*$"
)


# ============================================================================
# RESULT FACTORY
# ============================================================================

def build_validation_result(
    control_id,
    control_name,
    status,
    issues,
    notes,
):
    """Return a standardized validation result."""

    return {
        "control_id": str(control_id),
        "control_name": str(control_name),
        "status": str(status),
        "issues": int(issues),
        "notes": str(notes),
    }


# ============================================================================
# NORMALIZATION
# ============================================================================

def normalize_optional_text(value):
    """Return stripped text or None."""

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    normalized = str(value).strip()
    return normalized or None


def normalize_column_names(dataframe):
    """Normalize workbook column names."""

    result = dataframe.copy()
    result.columns = result.columns.astype(str).str.strip()
    return result


def normalize_text_columns(dataframe, columns):
    """Normalize selected text columns when present."""

    result = dataframe.copy()

    for column in columns:
        if column in result.columns:
            result[column] = result[column].apply(
                normalize_optional_text
            )

    return result


def normalize_boolean_value(value):
    """Normalize a Dataset boolean value."""

    if isinstance(value, bool):
        return value

    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value in (0, 1)
    ):
        return bool(value)

    if (
        isinstance(value, float)
        and not pd.isna(value)
        and value in (0.0, 1.0)
    ):
        return bool(int(value))

    if isinstance(value, str):

        normalized = value.strip().upper()

        if normalized in {"TRUE", "1"}:
            return True

        if normalized in {"FALSE", "0"}:
            return False

    raise ValueError(
        f"Invalid boolean value: {value}"
    )


def normalize_boolean_columns(dataframe):
    """Normalize all Dataset processing flags."""

    result = dataframe.copy()

    for column in BOOLEAN_COLUMNS:
        if column in result.columns:
            result[column] = result[column].apply(
                normalize_boolean_value
            )

    return result


def normalize_choice(
    value,
    field_name,
    allowed_values,
):
    """Normalize a controlled-vocabulary value."""

    normalized = normalize_optional_text(value)

    if normalized is None:
        raise ValueError(
            f"{field_name} cannot be empty."
        )

    canonical = {
        str(item).casefold(): item
        for item in allowed_values
    }

    key = normalized.casefold()

    if key not in canonical:
        raise ValueError(
            f"{field_name} must be one of "
            f"{sorted(allowed_values)}. "
            f"Received: {value}"
        )

    return canonical[key]


def normalize_production_method(value):
    """Normalize Production Method."""

    return normalize_choice(
        value,
        PRODUCTION_METHOD_COLUMN,
        PRODUCTION_METHODS,
    )


def normalize_production_methods(dataframe):
    """Normalize Production Method values."""

    result = dataframe.copy()

    if PRODUCTION_METHOD_COLUMN in result.columns:
        result[PRODUCTION_METHOD_COLUMN] = (
            result[PRODUCTION_METHOD_COLUMN]
            .apply(normalize_production_method)
        )

    return result


# ============================================================================
# PARAMETERS
# ============================================================================

def parse_parameters(value):
    """
    Parse Dataset Parameters.

    Empty cells become an empty dictionary.
    Populated values must contain a JSON object.
    """

    normalized = normalize_optional_text(value)

    if normalized is None:
        return {}

    try:
        parameters = json.loads(normalized)
    except json.JSONDecodeError as error:
        raise ValueError(
            "Parameters must contain valid JSON."
        ) from error

    if not isinstance(parameters, dict):
        raise ValueError(
            "Parameters JSON must contain a JSON object."
        )

    return parameters


def normalize_parameters(dataframe):
    """Convert Parameters JSON strings to dictionaries."""

    result = dataframe.copy()

    if PARAMETERS_COLUMN in result.columns:
        result[PARAMETERS_COLUMN] = (
            result[PARAMETERS_COLUMN]
            .apply(parse_parameters)
        )

    return result

# ============================================================================
# DATASET DEPENDENCIES NORMALIZATION
# ============================================================================

def parse_input_dataset_ids(value):
    """
    Parse Input Dataset IDs.

    Contract
    --------
    The registry cell must contain a JSON array.

    Examples
    --------
    No dependency:
        []

    One dependency:
        ["DS-001"]

    Multiple dependencies:
        ["DS-001", "DS-002"]

    Returns
    -------
    list[str]
        Normalized Dataset IDs preserving declared order.
    """

    normalized = normalize_optional_text(
        value
    )

    if normalized is None:
        raise ValueError(
            "Input Dataset IDs must contain "
            "a JSON array. Use [] when there "
            "are no dependencies."
        )

    try:
        parsed = json.loads(
            normalized
        )
    except json.JSONDecodeError as error:
        raise ValueError(
            "Input Dataset IDs must contain "
            "valid JSON."
        ) from error

    if not isinstance(
        parsed,
        list,
    ):
        raise ValueError(
            "Input Dataset IDs must contain "
            "a JSON array."
        )

    normalized_ids = []
    comparison_keys = set()

    for position, value in enumerate(
        parsed
    ):

        dataset_id = normalize_optional_text(
            value
        )

        if dataset_id is None:
            raise ValueError(
                "Input Dataset IDs cannot contain "
                "empty Dataset IDs. "
                f"Invalid position: {position}"
            )

        comparison_key = (
            dataset_id.casefold()
        )

        if comparison_key in comparison_keys:
            raise ValueError(
                "Input Dataset IDs cannot contain "
                "duplicate Dataset IDs: "
                f"{dataset_id}"
            )

        comparison_keys.add(
            comparison_key
        )

        normalized_ids.append(
            dataset_id
        )

    return normalized_ids


def normalize_input_dataset_ids(
    dataframe,
):
    """
    Convert Input Dataset IDs JSON cells to lists.
    """

    result = dataframe.copy()

    if INPUT_DATASET_COLUMN in result.columns:
        result[
            INPUT_DATASET_COLUMN
        ] = (
            result[
                INPUT_DATASET_COLUMN
            ]
            .apply(
                parse_input_dataset_ids
            )
        )

    return result


def validate_input_dataset_ids_format(
    datasets,
):
    """
    Validate the JSON/list contract of Input Dataset IDs.
    """

    issues = []

    if INPUT_DATASET_COLUMN not in datasets.columns:
        return build_validation_result(
            "SR-019A",
            "Input Dataset IDs Format",
            "FAIL",
            1,
            (
                "Missing column: "
                f"{INPUT_DATASET_COLUMN}"
            ),
        )

    for index, value in datasets[
        INPUT_DATASET_COLUMN
    ].items():

        try:
            parse_input_dataset_ids(
                value
            )

        except ValueError as error:
            issues.append(
                {
                    "excel_row":
                        index + 2,
                    "dataset_id":
                        datasets.at[
                            index,
                            DATASET_ID_COLUMN,
                        ],
                    "reason":
                        str(error),
                }
            )

    return build_validation_result(
        "SR-019A",
        "Input Dataset IDs Format",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Input Dataset IDs contain "
            "valid JSON arrays."
            if not issues
            else f"Issues: {issues}"
        ),
    )
# ============================================================================
# URL VALIDATION
# ============================================================================

def is_valid_http_url(value):
    """Return True for a valid HTTP or HTTPS URL."""

    normalized = normalize_optional_text(value)

    if normalized is None:
        return False

    if any(
        character.isspace()
        for character in normalized
    ):
        return False

    try:
        parsed = urlparse(normalized)
    except ValueError:
        return False

    return (
        parsed.scheme.lower() in ALLOWED_URL_SCHEMES
        and bool(parsed.netloc)
    )


# ============================================================================
# SQLITE TABLE NAME DERIVATION
# ============================================================================

def derive_table_name(output_file_name):
    """Derive a deterministic SQLite table name."""

    normalized = normalize_optional_text(
        output_file_name
    )

    if normalized is None:
        raise ValueError(
            "Output File Name cannot be empty."
        )

    file_path = Path(normalized)

    if file_path.name != normalized:
        raise ValueError(
            "Output File Name must not contain "
            "a directory path."
        )

    if not file_path.suffixes:
        raise ValueError(
            "Output File Name must contain "
            "a file extension."
        )

    table_name = normalized

    for suffix in reversed(file_path.suffixes):
        if table_name.lower().endswith(
            suffix.lower()
        ):
            table_name = table_name[:-len(suffix)]

    table_name = re.sub(
        r"[^a-z0-9_]+",
        "_",
        table_name.strip().lower(),
    )

    table_name = re.sub(
        r"_+",
        "_",
        table_name,
    ).strip("_")

    if not table_name:
        raise ValueError(
            "Unable to derive a table name "
            f"from {output_file_name}."
        )

    if table_name[0].isdigit():
        table_name = f"dataset_{table_name}"

    return table_name


def is_valid_table_name(table_name):
    """Return True for a valid derived table name."""

    return bool(
        TABLE_NAME_PATTERN.fullmatch(
            str(table_name)
        )
    )


# ============================================================================
# WORKBOOK LOADING
# ============================================================================

def validate_registry_file():
    """Validate registry file availability."""

    if not SOURCE_REGISTRY_FILE.exists():
        raise FileNotFoundError(
            "Source Registry not found: "
            f"{SOURCE_REGISTRY_FILE}"
        )

    if not SOURCE_REGISTRY_FILE.is_file():
        raise ValueError(
            "Source Registry path is not a file: "
            f"{SOURCE_REGISTRY_FILE}"
        )


def load_registry_sheets():
    """Read and normalize Sources and Datasets."""

    validate_registry_file()

    try:

        workbook = pd.ExcelFile(
            SOURCE_REGISTRY_FILE,
            engine="openpyxl",
        )

        missing_sheets = (
            REQUIRED_SHEETS
            - set(workbook.sheet_names)
        )

        if missing_sheets:
            raise ValueError(
                "Missing Source Registry sheets: "
                f"{sorted(missing_sheets)}"
            )

        sources = pd.read_excel(
            SOURCE_REGISTRY_FILE,
            sheet_name=SOURCES_SHEET,
            engine="openpyxl",
        )

        datasets = pd.read_excel(
            SOURCE_REGISTRY_FILE,
            sheet_name=DATASETS_SHEET,
            engine="openpyxl",
        )

    except PermissionError as error:

        raise PermissionError(
            "The Source Registry is open or locked. "
            "Close it and rerun validation."
        ) from error

    sources = normalize_text_columns(
        normalize_column_names(sources),
        SOURCE_TEXT_COLUMNS,
    )

    datasets = normalize_text_columns(
        normalize_column_names(datasets),
        DATASET_TEXT_COLUMNS,
    )

    return sources, datasets


# ============================================================================
# GENERIC STRUCTURAL CONTROLS
# ============================================================================

def validate_required_columns(
    dataframe,
    required_columns,
    sheet_name,
    control_id,
):
    """Validate required columns."""

    missing = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    return build_validation_result(
        control_id,
        f"{sheet_name} Required Columns",
        "PASS" if not missing else "FAIL",
        len(missing),
        (
            "All required columns are present."
            if not missing
            else f"Missing columns: {missing}"
        ),
    )


def validate_population(
    dataframe,
    sheet_name,
    control_id,
):
    """Validate that a sheet contains records."""

    empty = dataframe.empty

    return build_validation_result(
        control_id,
        f"{sheet_name} Population",
        "FAIL" if empty else "PASS",
        int(empty),
        (
            f"{sheet_name} sheet is empty."
            if empty
            else f"{len(dataframe)} records found."
        ),
    )


def validate_unique_values(
    dataframe,
    column,
    control_id,
):
    """Validate case-insensitive uniqueness."""

    if column not in dataframe.columns:
        return build_validation_result(
            control_id,
            f"{column} Uniqueness",
            "FAIL",
            1,
            f"Missing column: {column}",
        )

    normalized_values = (
        dataframe[column]
        .apply(normalize_optional_text)
    )

    blank_rows = normalized_values[
        normalized_values.isna()
    ].index.tolist()

    values = (
        normalized_values
        .dropna()
        .astype(str)
        .str.casefold()
    )

    duplicates = (
        values[
            values.duplicated(
                keep=False
            )
        ]
        .drop_duplicates()
        .tolist()
    )

    issues = (
        len(blank_rows)
        + len(duplicates)
    )

    notes = []

    if blank_rows:
        notes.append(
            "Blank values at Excel rows: "
            f"{[index + 2 for index in blank_rows]}"
        )

    if duplicates:
        notes.append(
            f"Duplicate values: {duplicates}"
        )

    return build_validation_result(
        control_id,
        f"{column} Uniqueness",
        "PASS" if not issues else "FAIL",
        issues,
        (
            f"{column} values are populated and unique."
            if not issues
            else "; ".join(notes)
        ),
    )


# ============================================================================
# SOURCE CONTROLS
# ============================================================================

def validate_required_source_values(sources):
    """Validate values mandatory for every Source."""

    issues = []

    for column in REQUIRED_SOURCE_VALUE_COLUMNS:

        if column not in sources.columns:
            issues.append(
                {
                    "column": column,
                    "reason": "missing column",
                }
            )
            continue

        for index, value in sources[column].items():

            if normalize_optional_text(value) is None:
                issues.append(
                    {
                        "excel_row": index + 2,
                        "column": column,
                    }
                )

    return build_validation_result(
        "SR-006",
        "Required Source Values",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "No required Source values are missing."
            if not issues
            else f"Missing values: {issues}"
        ),
    )


def validate_source_urls(sources):
    """
    Validate populated official Source URLs.

    URL is optional because some Sources represent multiple public
    websites or manually researched sources.
    """

    issues = []
    populated = 0

    for index, value in sources[
        SOURCE_URL_COLUMN
    ].items():

        normalized = normalize_optional_text(
            value
        )

        if normalized is None:
            continue

        populated += 1

        if not is_valid_http_url(
            normalized
        ):
            issues.append(
                {
                    "excel_row": index + 2,
                    "value": normalized,
                }
            )

    return build_validation_result(
        "SR-007",
        "Source URL Validation",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            f"{populated} populated Source URLs are valid."
            if not issues
            else f"Invalid URLs: {issues}"
        ),
    )


def validate_source_controlled_values(
    sources,
):
    """Validate Source Reliability and Status."""

    issues = []

    reliability_lookup = {
        value.casefold(): value
        for value in RELIABILITY_VALUES
    }

    status_lookup = {
        value.casefold(): value
        for value in SOURCE_STATUSES
    }

    for index, row in sources.iterrows():

        reliability = normalize_optional_text(
            row.get(
                RELIABILITY_COLUMN
            )
        )

        status = normalize_optional_text(
            row.get(
                SOURCE_STATUS_COLUMN
            )
        )

        if (
            reliability is not None
            and reliability.casefold()
            not in reliability_lookup
        ):
            issues.append(
                {
                    "excel_row": index + 2,
                    "column": RELIABILITY_COLUMN,
                    "value": reliability,
                }
            )

        if (
            status is not None
            and status.casefold()
            not in status_lookup
        ):
            issues.append(
                {
                    "excel_row": index + 2,
                    "column": SOURCE_STATUS_COLUMN,
                    "value": status,
                }
            )

    return build_validation_result(
        "SR-008",
        "Source Controlled Values",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Source controlled values are valid."
            if not issues
            else f"Invalid values: {issues}"
        ),
    )


# ============================================================================
# DATASET CONTROLS
# ============================================================================

def validate_required_dataset_values(
    datasets,
):
    """Validate values mandatory for every Dataset."""

    issues = []

    for column in REQUIRED_DATASET_VALUE_COLUMNS:

        if column not in datasets.columns:
            issues.append(
                {
                    "column": column,
                    "reason": "missing column",
                }
            )
            continue

        for index, value in datasets[
            column
        ].items():

            if normalize_optional_text(
                value
            ) is None:
                issues.append(
                    {
                        "excel_row": index + 2,
                        "column": column,
                    }
                )

    return build_validation_result(
        "SR-011",
        "Required Dataset Values",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "No required Dataset values are missing."
            if not issues
            else f"Missing values: {issues}"
        ),
    )


def validate_source_relationships(
    sources,
    datasets,
):
    """Validate Dataset-to-Source relationships."""

    known_sources = {
        normalize_optional_text(value)
        for value in sources[
            SOURCE_ID_COLUMN
        ]
        if normalize_optional_text(
            value
        ) is not None
    }

    referenced_sources = {
        normalize_optional_text(value)
        for value in datasets[
            DATASET_SOURCE_ID_COLUMN
        ]
        if normalize_optional_text(
            value
        ) is not None
    }

    unknown = sorted(
        referenced_sources
        - known_sources
    )

    return build_validation_result(
        "SR-012",
        "Source Referential Integrity",
        "PASS" if not unknown else "FAIL",
        len(unknown),
        (
            "All Dataset Source IDs exist in Sources."
            if not unknown
            else f"Unknown Source IDs: {unknown}"
        ),
    )


def validate_boolean_columns(
    datasets,
):
    """Validate Dataset processing flags."""

    issues = []

    for column in BOOLEAN_COLUMNS:

        if column not in datasets.columns:
            continue

        for index, value in datasets[
            column
        ].items():

            try:
                normalize_boolean_value(
                    value
                )
            except ValueError:
                issues.append(
                    {
                        "excel_row": index + 2,
                        "column": column,
                        "value": value,
                    }
                )

    return build_validation_result(
        "SR-013",
        "Boolean Configuration",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Dataset flags are valid booleans."
            if not issues
            else f"Invalid values: {issues}"
        ),
    )


def validate_dataset_parameters(
    datasets,
):
    """Validate Dataset Parameters JSON."""

    issues = []

    for index, value in datasets[
        PARAMETERS_COLUMN
    ].items():

        try:
            parse_parameters(
                value
            )
        except ValueError as error:
            issues.append(
                {
                    "excel_row": index + 2,
                    "dataset_id": datasets.at[
                        index,
                        DATASET_ID_COLUMN,
                    ],
                    "reason": str(error),
                }
            )

    return build_validation_result(
        "SR-014",
        "Dataset Parameters",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Parameters contain valid JSON objects."
            if not issues
            else f"Issues: {issues}"
        ),
    )

# ============================================================================
# OUTPUT CONTRACT
# ============================================================================

def validate_output_file_names(datasets):
    """Validate Dataset Output File Names."""

    issues = []

    if OUTPUT_FILE_COLUMN not in datasets.columns:
        return build_validation_result(
            "SR-015",
            "Output File Names",
            "FAIL",
            1,
            f"Missing column: {OUTPUT_FILE_COLUMN}",
        )

    for index, value in datasets[
        OUTPUT_FILE_COLUMN
    ].items():

        normalized = normalize_optional_text(
            value
        )

        if normalized is None:

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": "empty file name",
                }
            )

            continue

        path = Path(
            normalized
        )

        if path.name != normalized:

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": (
                        "directory path not allowed"
                    ),
                }
            )

        elif not path.suffix:

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": "missing extension",
                }
            )

        elif (
            path.suffix.lower()
            not in SUPPORTED_FILE_EXTENSIONS
        ):

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": (
                        "unsupported extension "
                        f"{path.suffix.lower()}"
                    ),
                }
            )

    return build_validation_result(
        "SR-015",
        "Output File Names",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Output File Names are valid."
            if not issues
            else f"Issues: {issues}"
        ),
    )


def validate_storage_directories(datasets):
    """Validate project-relative Dataset storage paths."""

    issues = []

    if (
        STORAGE_DIRECTORY_COLUMN
        not in datasets.columns
    ):

        return build_validation_result(
            "SR-016",
            "Storage Directory Validation",
            "FAIL",
            1,
            (
                "Missing column: "
                f"{STORAGE_DIRECTORY_COLUMN}"
            ),
        )

    for index, value in datasets[
        STORAGE_DIRECTORY_COLUMN
    ].items():

        normalized = normalize_optional_text(
            value
        )

        if normalized is None:

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": "empty directory",
                }
            )

            continue

        path = Path(
            normalized
        )

        if path.is_absolute():

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": (
                        "absolute path not allowed"
                    ),
                }
            )

        elif ".." in path.parts:

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": (
                        "parent reference not allowed"
                    ),
                }
            )

        elif normalized in {
            ".",
            "..",
        }:

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": "invalid directory",
                }
            )

    return build_validation_result(
        "SR-016",
        "Storage Directory Validation",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Storage Directories are valid."
            if not issues
            else f"Issues: {issues}"
        ),
    )


# ============================================================================
# DERIVED TABLE NAMES
# ============================================================================

def validate_derived_table_names(datasets):
    """Validate deterministic SQLite table names."""

    required_columns = [
        DATASET_ID_COLUMN,
        OUTPUT_FILE_COLUMN,
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in datasets.columns
    ]

    if missing_columns:

        details = pd.DataFrame(
            columns=[
                DATASET_ID_COLUMN,
                OUTPUT_FILE_COLUMN,
                "Derived Table Name",
                "Valid Table Name",
            ]
        )

        return (
            build_validation_result(
                "SR-017",
                "Derived Table Names",
                "FAIL",
                len(missing_columns),
                (
                    "Missing columns: "
                    f"{missing_columns}"
                ),
            ),
            details,
        )

    details = datasets[
        required_columns
    ].copy()

    names = []
    valid_flags = []
    issues = []

    for index, value in details[
        OUTPUT_FILE_COLUMN
    ].items():

        try:

            table_name = derive_table_name(
                value
            )

            valid = is_valid_table_name(
                table_name
            )

            names.append(
                table_name
            )

            valid_flags.append(
                valid
            )

            if not valid:

                issues.append(
                    {
                        "excel_row": index + 2,
                        "table_name": table_name,
                    }
                )

        except ValueError as error:

            names.append(
                None
            )

            valid_flags.append(
                False
            )

            issues.append(
                {
                    "excel_row": index + 2,
                    "reason": str(error),
                }
            )

    details[
        "Derived Table Name"
    ] = names

    details[
        "Valid Table Name"
    ] = valid_flags

    normalized_names = (
        details[
            "Derived Table Name"
        ]
        .dropna()
        .astype(str)
        .str.strip()
        .str.casefold()
    )

    duplicates = (
        normalized_names[
            normalized_names.duplicated(
                keep=False
            )
        ]
        .drop_duplicates()
        .tolist()
    )

    if duplicates:

        issues.append(
            {
                "reason": (
                    "duplicate derived table names"
                ),
                "values": duplicates,
            }
        )

    return (
        build_validation_result(
            "SR-017",
            "Derived Table Names",
            "PASS" if not issues else "FAIL",
            len(issues),
            (
                "All derived table names are "
                "valid and unique."
                if not issues
                else f"Issues: {issues}"
            ),
        ),
        details,
    )


# ============================================================================
# PROCESSING CONFIGURATION
# ============================================================================

def validate_configuration_rules(datasets):
    """Validate Dataset processing flag dependencies."""

    missing_columns = [
        column
        for column in BOOLEAN_COLUMNS
        if column not in datasets.columns
    ]

    if missing_columns:

        return build_validation_result(
            "SR-018",
            "Configuration Consistency",
            "FAIL",
            len(missing_columns),
            (
                "Missing columns: "
                f"{missing_columns}"
            ),
        )

    issues = []

    for index, row in datasets.iterrows():

        dataset_id = (
            normalize_optional_text(
                row.get(
                    DATASET_ID_COLUMN
                )
            )
            or f"Excel row {index + 2}"
        )

        try:

            load_enabled = (
                normalize_boolean_value(
                    row[
                        LOAD_ENABLED_COLUMN
                    ]
                )
            )

            quality_enabled = (
                normalize_boolean_value(
                    row[
                        QUALITY_ENABLED_COLUMN
                    ]
                )
            )

            chronology_enabled = (
                normalize_boolean_value(
                    row[
                        CHRONOLOGY_ENABLED_COLUMN
                    ]
                )
            )

            active = (
                normalize_boolean_value(
                    row[
                        ACTIVE_COLUMN
                    ]
                )
            )

        except ValueError:

            # Invalid booleans are already reported
            # by validate_boolean_columns().
            continue

        if (
            quality_enabled
            and not load_enabled
        ):

            issues.append(
                (
                    f"{dataset_id}: "
                    "Quality Enabled requires "
                    "Load Enabled."
                )
            )

        if (
            chronology_enabled
            and not quality_enabled
        ):

            issues.append(
                (
                    f"{dataset_id}: "
                    "Chronology Enabled requires "
                    "Quality Enabled."
                )
            )

        if (
            not active
            and (
                load_enabled
                or quality_enabled
                or chronology_enabled
            )
        ):

            issues.append(
                (
                    f"{dataset_id}: "
                    "inactive Dataset has enabled "
                    "processing flags."
                )
            )

    return build_validation_result(
        "SR-018",
        "Configuration Consistency",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "Dataset processing flags are consistent."
            if not issues
            else f"Issues: {issues}"
        ),
    )
# ============================================================================
# PRODUCTION METHOD CONTRACT
# ============================================================================

def validate_production_methods(datasets):
    """
    Validate configuration required by each Production Method.

    Dependency cardinality
    ----------------------
    Download
        Exactly zero dependencies.

    API
        Zero or one dependency.

    Package
        Zero or one dependency.

    Preparation
        One or more dependencies.
    """

    required_columns = {
        DATASET_ID_COLUMN,
        PRODUCTION_METHOD_COLUMN,
        CONNECTOR_COLUMN,
        ACCESS_ENDPOINT_COLUMN,
        INPUT_DATASET_COLUMN,
    }

    missing_columns = sorted(
        required_columns
        - set(
            datasets.columns
        )
    )

    if missing_columns:

        return build_validation_result(
            "SR-019",
            "Production Method Consistency",
            "FAIL",
            len(missing_columns),
            (
                "Missing columns: "
                f"{missing_columns}"
            ),
        )

    issues = []

    for index, row in datasets.iterrows():

        excel_row = index + 2

        dataset_id = normalize_optional_text(
            row.get(
                DATASET_ID_COLUMN
            )
        )

        try:
            method = normalize_production_method(
                row.get(
                    PRODUCTION_METHOD_COLUMN
                )
            )

        except ValueError as error:
            issues.append(
                {
                    "excel_row":
                        excel_row,
                    "dataset_id":
                        dataset_id,
                    "reason":
                        str(error),
                }
            )
            continue

        connector = normalize_optional_text(
            row.get(
                CONNECTOR_COLUMN
            )
        )

        endpoint = normalize_optional_text(
            row.get(
                ACCESS_ENDPOINT_COLUMN
            )
        )

        try:
            input_dataset_ids = (
                parse_input_dataset_ids(
                    row.get(
                        INPUT_DATASET_COLUMN
                    )
                )
            )

        except ValueError:
            # SR-019A reports the malformed dependency cell.
            # Avoid duplicate diagnostics here.
            continue

        dependency_count = len(
            input_dataset_ids
        )

        # --------------------------------------------------------------------
        # DOWNLOAD
        # --------------------------------------------------------------------

        if method == DOWNLOAD_METHOD:

            if endpoint is None:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Download requires "
                                "Access Endpoint."
                            ),
                    }
                )

            elif not is_valid_http_url(
                endpoint
            ):

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Download Access Endpoint "
                                "must be HTTP/HTTPS."
                            ),
                    }
                )

            if dependency_count != 0:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Download requires zero "
                                "Input Dataset IDs."
                            ),
                    }
                )

        # --------------------------------------------------------------------
        # API
        # --------------------------------------------------------------------

        elif method == API_METHOD:

            if endpoint is None:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "API requires "
                                "Access Endpoint."
                            ),
                    }
                )

            elif not is_valid_http_url(
                endpoint
            ):

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "API Access Endpoint must "
                                "be HTTP/HTTPS."
                            ),
                    }
                )

            if connector is None:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "API requires Connector."
                            ),
                    }
                )

            if dependency_count > 1:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "API currently supports "
                                "at most one Input Dataset ID."
                            ),
                    }
                )

        # --------------------------------------------------------------------
        # PACKAGE
        # --------------------------------------------------------------------

        elif method == PACKAGE_METHOD:

            if connector is None:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Package requires Connector."
                            ),
                    }
                )

            if (
                endpoint is not None
                and not is_valid_http_url(
                    endpoint
                )
            ):

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Package Access Endpoint "
                                "must be HTTP/HTTPS when "
                                "provided."
                            ),
                    }
                )

            if dependency_count > 1:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Package currently supports "
                                "at most one Input Dataset ID."
                            ),
                    }
                )

        # --------------------------------------------------------------------
        # PREPARATION
        # --------------------------------------------------------------------

        elif method == PREPARATION_METHOD:

            if dependency_count < 1:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Preparation requires "
                                "at least one "
                                "Input Dataset ID."
                            ),
                    }
                )

            if endpoint is not None:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Preparation cannot declare "
                                "Access Endpoint."
                            ),
                    }
                )

            if connector is not None:

                issues.append(
                    {
                        "excel_row":
                            excel_row,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Preparation cannot declare "
                                "Connector."
                            ),
                    }
                )

    return build_validation_result(
        "SR-019",
        "Production Method Consistency",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Production Method contracts "
            "are valid."
            if not issues
            else f"Issues: {issues}"
        ),
    )
    
# ============================================================================
# DATASET DEPENDENCIES
# ============================================================================

def validate_dataset_dependencies(datasets):
    """
    Validate the complete Dataset dependency graph.

    Controls
    --------
    - Input Dataset IDs must contain valid JSON arrays.
    - Each referenced Dataset ID must exist.
    - Dependency IDs are case-insensitive.
    - A Dataset cannot depend on itself.
    - Duplicate dependencies are prohibited.
    - The dependency graph must be acyclic.
    """

    required_columns = {
        DATASET_ID_COLUMN,
        INPUT_DATASET_COLUMN,
    }

    missing_columns = sorted(
        required_columns
        - set(
            datasets.columns
        )
    )

    if missing_columns:

        return build_validation_result(
            "SR-020",
            "Dataset Dependency Consistency",
            "FAIL",
            len(missing_columns),
            (
                "Missing columns: "
                f"{missing_columns}"
            ),
        )

    dataset_lookup = {}

    for value in datasets[
        DATASET_ID_COLUMN
    ]:

        dataset_id = normalize_optional_text(
            value
        )

        if dataset_id is None:
            continue

        dataset_lookup[
            dataset_id.casefold()
        ] = dataset_id

    dependencies = {}
    issues = []

    for index, row in datasets.iterrows():

        dataset_id = normalize_optional_text(
            row.get(
                DATASET_ID_COLUMN
            )
        )

        if dataset_id is None:
            continue

        dataset_key = (
            dataset_id.casefold()
        )

        try:
            input_dataset_ids = (
                parse_input_dataset_ids(
                    row.get(
                        INPUT_DATASET_COLUMN
                    )
                )
            )

        except ValueError as error:

            issues.append(
                {
                    "excel_row":
                        index + 2,
                    "dataset_id":
                        dataset_id,
                    "reason":
                        str(error),
                }
            )

            dependencies[
                dataset_key
            ] = []

            continue

        dependency_keys = []

        for input_dataset_id in input_dataset_ids:

            input_key = (
                input_dataset_id.casefold()
            )

            if input_key not in dataset_lookup:

                issues.append(
                    {
                        "excel_row":
                            index + 2,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "Unknown Input Dataset ID: "
                                f"{input_dataset_id}"
                            ),
                    }
                )

                continue

            if input_key == dataset_key:

                issues.append(
                    {
                        "excel_row":
                            index + 2,
                        "dataset_id":
                            dataset_id,
                        "reason":
                            (
                                "A Dataset cannot depend "
                                "on itself."
                            ),
                    }
                )

                continue

            dependency_keys.append(
                input_key
            )

        dependencies[
            dataset_key
        ] = dependency_keys

    # ------------------------------------------------------------------------
    # CYCLE DETECTION
    # ------------------------------------------------------------------------

    visit_state = {
        dataset_key: 0
        for dataset_key
        in dependencies
    }

    traversal_stack = []

    reported_cycles = set()

    def visit(
        dataset_key,
    ):
        """
        Depth-first cycle detection.

        States
        ------
        0 = unvisited
        1 = currently visiting
        2 = fully visited
        """

        visit_state[
            dataset_key
        ] = 1

        traversal_stack.append(
            dataset_key
        )

        for dependency_key in dependencies.get(
            dataset_key,
            [],
        ):

            if (
                dependency_key
                not in visit_state
            ):
                continue

            state = visit_state[
                dependency_key
            ]

            if state == 0:

                visit(
                    dependency_key
                )

            elif state == 1:

                try:
                    cycle_start = (
                        traversal_stack.index(
                            dependency_key
                        )
                    )

                except ValueError:
                    cycle_start = 0

                cycle_keys = (
                    traversal_stack[
                        cycle_start:
                    ]
                    + [
                        dependency_key
                    ]
                )

                cycle_names = [
                    dataset_lookup.get(
                        key,
                        key,
                    )
                    for key in cycle_keys
                ]

                canonical_cycle = tuple(
                    sorted(
                        set(
                            cycle_keys
                        )
                    )
                )

                if (
                    canonical_cycle
                    not in reported_cycles
                ):

                    reported_cycles.add(
                        canonical_cycle
                    )

                    issues.append(
                        {
                            "dataset_id":
                                dataset_lookup.get(
                                    dataset_key,
                                    dataset_key,
                                ),
                            "reason":
                                (
                                    "Circular dependency "
                                    "detected: "
                                    + " -> ".join(
                                        cycle_names
                                    )
                                ),
                        }
                    )

        traversal_stack.pop()

        visit_state[
            dataset_key
        ] = 2

    for dataset_key in list(
        dependencies
    ):

        if visit_state[
            dataset_key
        ] == 0:

            visit(
                dataset_key
            )

    return build_validation_result(
        "SR-020",
        "Dataset Dependency Consistency",
        "PASS" if not issues else "FAIL",
        len(issues),
        (
            "All Dataset dependencies are "
            "valid and acyclic."
            if not issues
            else f"Issues: {issues}"
        ),
    )
# ============================================================================
# RESULT SAFETY
# ============================================================================

def append_validation_result(
    results,
    result,
    function_name,
):
    """Validate and append one control result."""

    if result is None:

        raise RuntimeError(
            "Validation function returned None: "
            f"{function_name}"
        )

    if not isinstance(
        result,
        dict,
    ):

        raise TypeError(
            f"{function_name} must return dict."
        )

    required_keys = {
        "control_id",
        "control_name",
        "status",
        "issues",
        "notes",
    }

    missing_keys = (
        required_keys
        - set(result)
    )

    if missing_keys:

        raise ValueError(
            f"{function_name} result is missing "
            f"keys: {sorted(missing_keys)}"
        )

    if result[
        "status"
    ] not in {
        "PASS",
        "FAIL",
    }:

        raise ValueError(
            f"{function_name} returned "
            "unsupported status: "
            f"{result['status']}"
        )

    try:

        issue_count = int(
            result[
                "issues"
            ]
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise TypeError(
            f"{function_name} returned an "
            "invalid issues count."
        ) from error

    if issue_count < 0:

        raise ValueError(
            f"{function_name} returned a "
            "negative issues count."
        )

    results.append(
        result
    )
# ============================================================================
# ORCHESTRATOR
# ============================================================================

def validate_source_registry(
    raise_on_failure=True,
):
    """
    Validate and normalize the complete Source Registry.

    Parameters
    ----------
    raise_on_failure : bool
        Raise ValueError when one or more validation
        controls fail.

    Returns
    -------
    dict
        validation_report
            DataFrame containing all executed controls.

        sources
            Normalized Sources DataFrame.

        datasets
            Normalized Datasets DataFrame.

        derived_tables
            Dataset-to-table-name resolution details.

        is_valid
            True when every executed control passed.
    """

    logger.info(
        "Source Registry validation started"
    )

    try:

        sources, datasets = (
            load_registry_sheets()
        )

        results = []

        # --------------------------------------------------------------------
        # SCHEMA
        # --------------------------------------------------------------------

        source_schema = (
            validate_required_columns(
                sources,
                REQUIRED_SOURCE_COLUMNS,
                SOURCES_SHEET,
                "SR-001",
            )
        )

        dataset_schema = (
            validate_required_columns(
                datasets,
                REQUIRED_DATASET_COLUMNS,
                DATASETS_SHEET,
                "SR-002",
            )
        )

        append_validation_result(
            results,
            source_schema,
            "validate_required_columns_sources",
        )

        append_validation_result(
            results,
            dataset_schema,
            "validate_required_columns_datasets",
        )

        # --------------------------------------------------------------------
        # POPULATION
        # --------------------------------------------------------------------

        append_validation_result(
            results,
            validate_population(
                sources,
                SOURCES_SHEET,
                "SR-003",
            ),
            "validate_population_sources",
        )

        append_validation_result(
            results,
            validate_population(
                datasets,
                DATASETS_SHEET,
                "SR-004",
            ),
            "validate_population_datasets",
        )

        source_schema_valid = (
            source_schema[
                "status"
            ]
            == "PASS"
        )

        dataset_schema_valid = (
            dataset_schema[
                "status"
            ]
            == "PASS"
        )

        derived_tables = pd.DataFrame(
            columns=[
                DATASET_ID_COLUMN,
                OUTPUT_FILE_COLUMN,
                "Derived Table Name",
                "Valid Table Name",
            ]
        )

        # --------------------------------------------------------------------
        # SOURCE CONTROLS
        # --------------------------------------------------------------------

        if source_schema_valid:

            append_validation_result(
                results,
                validate_unique_values(
                    sources,
                    SOURCE_ID_COLUMN,
                    "SR-005",
                ),
                "validate_unique_source_ids",
            )

            append_validation_result(
                results,
                validate_required_source_values(
                    sources
                ),
                "validate_required_source_values",
            )

            append_validation_result(
                results,
                validate_source_urls(
                    sources
                ),
                "validate_source_urls",
            )

            append_validation_result(
                results,
                validate_source_controlled_values(
                    sources
                ),
                "validate_source_controlled_values",
            )

        # --------------------------------------------------------------------
        # DATASET CONTROLS
        # --------------------------------------------------------------------

        if dataset_schema_valid:

            append_validation_result(
                results,
                validate_unique_values(
                    datasets,
                    DATASET_ID_COLUMN,
                    "SR-009",
                ),
                "validate_unique_dataset_ids",
            )

            append_validation_result(
                results,
                validate_unique_values(
                    datasets,
                    OUTPUT_FILE_COLUMN,
                    "SR-010",
                ),
                "validate_unique_output_files",
            )

            append_validation_result(
                results,
                validate_required_dataset_values(
                    datasets
                ),
                "validate_required_dataset_values",
            )

            if source_schema_valid:

                append_validation_result(
                    results,
                    validate_source_relationships(
                        sources,
                        datasets,
                    ),
                    "validate_source_relationships",
                )

            append_validation_result(
                results,
                validate_boolean_columns(
                    datasets
                ),
                "validate_boolean_columns",
            )

            append_validation_result(
                results,
                validate_dataset_parameters(
                    datasets
                ),
                "validate_dataset_parameters",
            )

            append_validation_result(
                results,
                validate_output_file_names(
                    datasets
                ),
                "validate_output_file_names",
            )

            append_validation_result(
                results,
                validate_storage_directories(
                    datasets
                ),
                "validate_storage_directories",
            )

            (
                table_result,
                derived_tables,
            ) = validate_derived_table_names(
                datasets
            )

            append_validation_result(
                results,
                table_result,
                "validate_derived_table_names",
            )
            append_validation_result(
                results,
                validate_configuration_rules(
                    datasets
                ),
                "validate_configuration_rules",
            )
            
            append_validation_result(
                results,
                validate_input_dataset_ids_format(
                    datasets
                ),
                "validate_input_dataset_ids_format",
            )
            
            append_validation_result(
                results,
                validate_production_methods(
                    datasets
                ),
                "validate_production_methods",
            )
            
            append_validation_result(
                results,
                validate_dataset_dependencies(
                    datasets
                ),
                "validate_dataset_dependencies",
            )

    
        # --------------------------------------------------------------------
        # REPORT
        # --------------------------------------------------------------------

        report = pd.DataFrame(
            results
        )

        if report.empty:

            raise RuntimeError(
                "Source Registry validation "
                "executed no controls."
            )

        failures = (
            report[
                report[
                    "status"
                ].eq(
                    "FAIL"
                )
            ]
            .copy()
        )

        is_valid = (
            failures.empty
        )

        # --------------------------------------------------------------------
        # NORMALIZED OUTPUT
        # --------------------------------------------------------------------

        normalized_sources = (
            sources.copy(
                deep=True
            )
        )

        normalized_datasets = (
            datasets.copy(
                deep=True
            )
        )

        if (
            dataset_schema_valid
            and is_valid
        ):

            normalized_datasets = (
                normalize_boolean_columns(
                    normalized_datasets
                )
            )

            normalized_datasets = (
                normalize_production_methods(
                    normalized_datasets
                )
            )

            normalized_datasets = (
                normalize_parameters(
                    normalized_datasets
                )
            )

            normalized_datasets = (
                normalize_input_dataset_ids(
                    normalized_datasets
                )
            )

        # --------------------------------------------------------------------
        # EXECUTION SUMMARY
        # --------------------------------------------------------------------

        logger.info(
            "Source Registry sources validated: %s",
            len(
                sources
            ),
        )

        logger.info(
            "Source Registry datasets validated: %s",
            len(
                datasets
            ),
        )

        logger.info(
            "Source Registry controls executed: %s",
            len(
                report
            ),
        )

        logger.info(
            "Source Registry failed controls: %s",
            len(
                failures
            ),
        )

        # --------------------------------------------------------------------
        # FAILURE POLICY
        # --------------------------------------------------------------------

        if not is_valid:

            summary = (
                failures[
                    [
                        "control_id",
                        "control_name",
                        "notes",
                    ]
                ]
                .to_dict(
                    orient="records"
                )
            )

            if raise_on_failure:

                raise ValueError(
                    "Source Registry validation "
                    "failed: "
                    f"{summary}"
                )

            logger.warning(
                "Source Registry validation "
                "completed with failures: %s",
                summary,
            )

        else:

            logger.info(
                "Source Registry validation "
                "completed successfully"
            )

        # --------------------------------------------------------------------
        # RESULT
        # --------------------------------------------------------------------

        return {
            "validation_report":
                report.copy(
                    deep=True
                ),
            "sources":
                normalized_sources.copy(
                    deep=True
                ),
            "datasets":
                normalized_datasets.copy(
                    deep=True
                ),
            "derived_tables":
                derived_tables.copy(
                    deep=True
                ),
            "is_valid":
                is_valid,
        }

    except Exception as error:

        logger.exception(
            "Source Registry validation "
            "failed: %s",
            error,
        )

        raise


# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Validate the complete Source Registry.

    The command exits through an exception when the registry
    violates one or more blocking controls.
    """

    validation = (
        validate_source_registry(
            raise_on_failure=True
        )
    )

    logger.info(
        "Source Registry valid: %s",
        validation[
            "is_valid"
        ],
    )


if __name__ == "__main__":
    main()


    