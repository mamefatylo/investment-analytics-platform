"""
validate_source_registry.py

Validate the Sources and Datasets sheets before acquisition, loading, or
quality processing. The validator enforces structure, referential integrity,
production rules, file conventions, and acyclic dataset dependencies.
"""

import re
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from src.config import SOURCE_REGISTRY_FILE
from src.utils.logger import logger

SOURCES_SHEET = "Sources"
DATASETS_SHEET = "Datasets"
REQUIRED_SHEETS = {SOURCES_SHEET, DATASETS_SHEET}

REQUIRED_SOURCE_COLUMNS = ["Source ID", "Download URL", "API Endpoint"]
REQUIRED_DATASET_COLUMNS = [
    "Dataset ID",
    "Source ID",
    "Dataset Name",
    "Output File Name",
    "Storage Directory",
    "Production Method",
    "Input Dataset ID",
    "Output Role",
    "Load Enabled",
    "Quality Enabled",
    "Chronology Enabled",
    "Active",
]
REQUIRED_DATASET_VALUE_COLUMNS = [
    column for column in REQUIRED_DATASET_COLUMNS
    if column != "Input Dataset ID"
]
SOURCE_TEXT_COLUMNS = ["Source ID", "Download URL", "API Endpoint"]
DATASET_TEXT_COLUMNS = [
    "Dataset ID",
    "Source ID",
    "Dataset Name",
    "Output File Name",
    "Storage Directory",
    "Production Method",
    "Input Dataset ID",
    "Output Role",
]
BOOLEAN_COLUMNS = [
    "Load Enabled",
    "Quality Enabled",
    "Chronology Enabled",
    "Active",
]
SUPPORTED_FILE_EXTENSIONS = {".csv", ".json", ".parquet", ".xls", ".xlsx"}
ALLOWED_URL_SCHEMES = {"http", "https"}
PRODUCTION_METHODS = {"Download", "API", "Preparation"}
OUTPUT_ROLES = {"Primary", "Exception"}
TABLE_NAME_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


def build_validation_result(control_id, control_name, status, issues, notes):
    return {
        "control_id": str(control_id),
        "control_name": str(control_name),
        "status": str(status),
        "issues": int(issues),
        "notes": str(notes),
    }


def normalize_optional_text(value):
    if pd.isna(value):
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_column_names(dataframe):
    result = dataframe.copy()
    result.columns = result.columns.astype(str).str.strip()
    return result


def normalize_text_columns(dataframe, columns):
    result = dataframe.copy()
    for column in columns:
        if column in result.columns:
            result[column] = result[column].apply(normalize_optional_text)
    return result


def normalize_boolean_value(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
        return bool(value)
    if isinstance(value, float) and value in (0.0, 1.0):
        return bool(int(value))
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in {"TRUE", "1"}:
            return True
        if normalized in {"FALSE", "0"}:
            return False
    raise ValueError(f"Invalid boolean value: {value}")


def normalize_boolean_columns(dataframe):
    result = dataframe.copy()
    for column in BOOLEAN_COLUMNS:
        if column in result.columns:
            result[column] = result[column].apply(normalize_boolean_value)
    return result


def normalize_production_method(value):
    normalized = normalize_optional_text(value)
    if normalized is None:
        raise ValueError("Production Method cannot be empty.")
    canonical = {method.casefold(): method for method in PRODUCTION_METHODS}
    key = normalized.casefold()
    if key not in canonical:
        raise ValueError(
            "Production Method must be one of: "
            f"{sorted(PRODUCTION_METHODS)}. Received: {value}"
        )
    return canonical[key]


def normalize_production_methods(dataframe):
    result = dataframe.copy()
    if "Production Method" in result.columns:
        result["Production Method"] = result["Production Method"].apply(
            normalize_production_method
        )
    return result


def derive_table_name(output_file_name):
    normalized = normalize_optional_text(output_file_name)
    if normalized is None:
        raise ValueError("Output File Name cannot be empty.")
    file_path = Path(normalized)
    if file_path.name != normalized:
        raise ValueError("Output File Name must not contain a directory path.")
    if not file_path.suffixes:
        raise ValueError("Output File Name must contain a file extension.")
    table_name = normalized
    for suffix in reversed(file_path.suffixes):
        if table_name.lower().endswith(suffix.lower()):
            table_name = table_name[:-len(suffix)]
    table_name = re.sub(r"[^a-z0-9_]+", "_", table_name.strip().lower())
    table_name = re.sub(r"_+", "_", table_name).strip("_")
    if not table_name:
        raise ValueError(f"Unable to derive a table name from {output_file_name}.")
    if table_name[0].isdigit():
        table_name = f"dataset_{table_name}"
    return table_name


def is_valid_table_name(table_name):
    return bool(TABLE_NAME_PATTERN.fullmatch(str(table_name)))


def is_valid_http_url(value):
    """Return True for an empty value or a valid HTTP/HTTPS URL."""
    normalized = normalize_optional_text(value)
    if normalized is None:
        return True
    if any(character.isspace() for character in normalized):
        return False
    try:
        parsed = urlparse(normalized)
    except ValueError:
        return False
    return parsed.scheme.lower() in ALLOWED_URL_SCHEMES and bool(parsed.netloc)


def is_valid_download_url(value):
    """Backward-compatible URL validator used by existing modules."""
    return is_valid_http_url(value)


def validate_registry_file():
    if not SOURCE_REGISTRY_FILE.exists():
        raise FileNotFoundError(f"Source Registry not found: {SOURCE_REGISTRY_FILE}")
    if not SOURCE_REGISTRY_FILE.is_file():
        raise ValueError(f"Source Registry path is not a file: {SOURCE_REGISTRY_FILE}")


def load_registry_sheets():
    validate_registry_file()
    try:
        workbook = pd.ExcelFile(SOURCE_REGISTRY_FILE, engine="openpyxl")
        missing_sheets = REQUIRED_SHEETS - set(workbook.sheet_names)
        if missing_sheets:
            raise ValueError(f"Missing Source Registry sheets: {sorted(missing_sheets)}")
        sources = pd.read_excel(
            SOURCE_REGISTRY_FILE, sheet_name=SOURCES_SHEET, engine="openpyxl"
        )
        datasets = pd.read_excel(
            SOURCE_REGISTRY_FILE, sheet_name=DATASETS_SHEET, engine="openpyxl"
        )
    except PermissionError as error:
        raise PermissionError(
            "The Source Registry is open or locked. Close it and rerun validation."
        ) from error
    sources = normalize_text_columns(normalize_column_names(sources), SOURCE_TEXT_COLUMNS)
    datasets = normalize_text_columns(normalize_column_names(datasets), DATASET_TEXT_COLUMNS)
    return sources, datasets


def validate_required_columns(dataframe, required_columns, sheet_name, control_id):
    missing = [column for column in required_columns if column not in dataframe.columns]
    return build_validation_result(
        control_id,
        f"{sheet_name} Required Columns",
        "PASS" if not missing else "FAIL",
        len(missing),
        "All required columns are present." if not missing else f"Missing columns: {missing}",
    )


def validate_population(dataframe, sheet_name, control_id):
    empty = dataframe.empty
    return build_validation_result(
        control_id,
        f"{sheet_name} Population",
        "FAIL" if empty else "PASS",
        int(empty),
        f"{sheet_name} sheet is empty." if empty else f"{len(dataframe)} records found.",
    )


def validate_unique_values(dataframe, column, control_id):
    if column not in dataframe.columns:
        return build_validation_result(
            control_id, f"{column} Uniqueness", "FAIL", 1, f"Missing column: {column}"
        )
    values = dataframe[column].dropna().astype(str).str.strip().str.casefold()
    duplicates = values[values.duplicated(keep=False)].drop_duplicates().tolist()
    return build_validation_result(
        control_id,
        f"{column} Uniqueness",
        "PASS" if not duplicates else "FAIL",
        len(duplicates),
        f"{column} values are unique." if not duplicates else f"Duplicate values: {duplicates}",
    )


def validate_download_urls(sources):
    if "Download URL" not in sources.columns:
        return build_validation_result(
            "SR-006", "Download URL Validation", "FAIL", 1, "Download URL column is missing."
        )
    issues = []
    populated = 0
    for index, value in sources["Download URL"].items():
        if normalize_optional_text(value) is None:
            continue
        populated += 1
        if not is_valid_download_url(value):
            issues.append({"excel_row": index + 2, "value": value})
    return build_validation_result(
        "SR-006",
        "Download URL Validation",
        "PASS" if not issues else "FAIL",
        len(issues),
        f"{populated} populated Download URL values are valid."
        if not issues else f"Invalid URLs: {issues}",
    )


def validate_required_dataset_values(datasets):
    issues = []
    for column in REQUIRED_DATASET_VALUE_COLUMNS:
        if column not in datasets.columns:
            issues.append({"column": column, "reason": "missing column"})
            continue
        for index, value in datasets[column].items():
            if pd.isna(value) or (isinstance(value, str) and not value.strip()):
                issues.append({"excel_row": index + 2, "column": column})
    return build_validation_result(
        "SR-007",
        "Required Dataset Values",
        "PASS" if not issues else "FAIL",
        len(issues),
        "No required dataset values are missing." if not issues else f"Missing values: {issues}",
    )


def validate_source_relationships(sources, datasets):
    if "Source ID" not in sources.columns or "Source ID" not in datasets.columns:
        return build_validation_result(
            "SR-010", "Source Referential Integrity", "FAIL", 1, "Source ID column is missing."
        )
    known = set(sources["Source ID"].dropna().astype(str).str.strip())
    referenced = set(datasets["Source ID"].dropna().astype(str).str.strip())
    unknown = sorted(referenced - known)
    return build_validation_result(
        "SR-010",
        "Source Referential Integrity",
        "PASS" if not unknown else "FAIL",
        len(unknown),
        "All Dataset Source IDs exist in Sources." if not unknown else f"Unknown Source IDs: {unknown}",
    )


def validate_boolean_columns(datasets):
    issues = []
    for column in BOOLEAN_COLUMNS:
        if column not in datasets.columns:
            issues.append({"column": column, "reason": "missing column"})
            continue
        for index, value in datasets[column].items():
            try:
                normalize_boolean_value(value)
            except ValueError:
                issues.append({"excel_row": index + 2, "column": column, "value": value})
    return build_validation_result(
        "SR-011",
        "Boolean Configuration",
        "PASS" if not issues else "FAIL",
        len(issues),
        "All configuration flags are valid booleans." if not issues else f"Invalid values: {issues}",
    )


def validate_output_file_names(datasets):
    if "Output File Name" not in datasets.columns:
        return build_validation_result(
            "SR-012", "Output File Names", "FAIL", 1, "Output File Name column is missing."
        )
    issues = []
    for index, value in datasets["Output File Name"].items():
        normalized = normalize_optional_text(value)
        if normalized is None:
            issues.append({"excel_row": index + 2, "reason": "empty file name"})
            continue
        path = Path(normalized)
        if path.name != normalized:
            issues.append({"excel_row": index + 2, "reason": "directory path not allowed"})
        elif not path.suffix:
            issues.append({"excel_row": index + 2, "reason": "missing extension"})
        elif path.suffix.lower() not in SUPPORTED_FILE_EXTENSIONS:
            issues.append({"excel_row": index + 2, "reason": f"unsupported extension {path.suffix.lower()}"})
    return build_validation_result(
        "SR-012",
        "Output File Names",
        "PASS" if not issues else "FAIL",
        len(issues),
        "All output file names are valid." if not issues else f"Invalid file names: {issues}",
    )


def validate_storage_directories(datasets):
    if "Storage Directory" not in datasets.columns:
        return build_validation_result(
            "SR-013", "Storage Directory Validation", "FAIL", 1, "Storage Directory column is missing."
        )
    issues = []
    for index, value in datasets["Storage Directory"].items():
        normalized = normalize_optional_text(value)
        if normalized is None:
            issues.append({"excel_row": index + 2, "reason": "empty directory"})
            continue
        path = Path(normalized)
        if path.is_absolute():
            issues.append({"excel_row": index + 2, "reason": "absolute path not allowed"})
        elif ".." in path.parts:
            issues.append({"excel_row": index + 2, "reason": "parent reference not allowed"})
        elif path.name in {"", ".", ".."}:
            issues.append({"excel_row": index + 2, "reason": "invalid directory"})
    return build_validation_result(
        "SR-013",
        "Storage Directory Validation",
        "PASS" if not issues else "FAIL",
        len(issues),
        "All storage directories are valid." if not issues else f"Invalid directories: {issues}",
    )


def validate_derived_table_names(datasets):
    required = ["Dataset ID", "Output File Name"]
    missing = [column for column in required if column not in datasets.columns]
    if missing:
        details = pd.DataFrame(columns=required + ["Derived Table Name", "Valid Table Name"])
        return build_validation_result(
            "SR-014", "Derived Table Names", "FAIL", len(missing), f"Missing columns: {missing}"
        ), details

    details = datasets[required].copy()
    names = []
    valid_flags = []
    issues = []
    for index, value in details["Output File Name"].items():
        try:
            name = derive_table_name(value)
            valid = is_valid_table_name(name)
            names.append(name)
            valid_flags.append(valid)
            if not valid:
                issues.append({"excel_row": index + 2, "table_name": name})
        except ValueError as error:
            names.append(None)
            valid_flags.append(False)
            issues.append({"excel_row": index + 2, "reason": str(error)})
    details["Derived Table Name"] = names
    details["Valid Table Name"] = valid_flags
    valid_names = details["Derived Table Name"].dropna()
    duplicates = valid_names[valid_names.duplicated(keep=False)].drop_duplicates().tolist()
    if duplicates:
        issues.append({"reason": "duplicate derived table names", "values": duplicates})
    return build_validation_result(
        "SR-014",
        "Derived Table Names",
        "PASS" if not issues else "FAIL",
        len(issues),
        "All derived table names are valid and unique." if not issues else f"Errors: {issues}",
    ), details


def validate_configuration_rules(datasets):
    missing = [column for column in BOOLEAN_COLUMNS if column not in datasets.columns]
    if missing:
        return build_validation_result(
            "SR-015", "Configuration Consistency", "FAIL", len(missing), f"Missing columns: {missing}"
        )
    try:
        normalized = normalize_boolean_columns(datasets)
    except ValueError as error:
        return build_validation_result("SR-015", "Configuration Consistency", "FAIL", 1, str(error))
    issues = []
    for _, row in normalized.iterrows():
        dataset_id = row.get("Dataset ID")
        if row["Quality Enabled"] and not row["Load Enabled"]:
            issues.append(f"{dataset_id}: Quality Enabled requires Load Enabled.")
        if row["Chronology Enabled"] and not row["Quality Enabled"]:
            issues.append(f"{dataset_id}: Chronology Enabled requires Quality Enabled.")
        if not row["Active"] and (
            row["Load Enabled"] or row["Quality Enabled"] or row["Chronology Enabled"]
        ):
            issues.append(f"{dataset_id}: inactive dataset has an enabled processing flag.")
    return build_validation_result(
        "SR-015",
        "Configuration Consistency",
        "PASS" if not issues else "FAIL",
        len(issues),
        "Dataset configuration rules are consistent." if not issues else f"Issues: {issues}",
    )


def validate_production_methods(sources, datasets):
    """
    Validate production methods against source access configuration.

    Download requires a valid Download URL on the related source.
    API requires a valid API Endpoint on the related source.
    Preparation does not require an external endpoint.
    """
    if "Production Method" not in datasets.columns:
        return build_validation_result(
            "SR-016",
            "Production Method Consistency",
            "FAIL",
            1,
            "Production Method column is missing.",
        )

    issues = []
    normalized_methods = {}
    for index, value in datasets["Production Method"].items():
        try:
            normalized_methods[index] = normalize_production_method(value)
        except ValueError as error:
            normalized_methods[index] = None
            issues.append({"excel_row": index + 2, "reason": str(error)})

    source_access = {}
    required_source_columns = {"Source ID", "Download URL", "API Endpoint"}
    if required_source_columns.issubset(sources.columns):
        source_access = (
            sources[["Source ID", "Download URL", "API Endpoint"]]
            .drop_duplicates(subset=["Source ID"])
            .set_index("Source ID")
            .to_dict(orient="index")
        )

    for index, row in datasets.iterrows():
        method = normalized_methods.get(index)
        if method is None:
            continue

        source_id = normalize_optional_text(row.get("Source ID"))
        access = source_access.get(source_id, {})

        if method == "Download":
            download_url = access.get("Download URL")
            if normalize_optional_text(download_url) is None:
                issues.append({
                    "excel_row": index + 2,
                    "dataset_id": row.get("Dataset ID"),
                    "reason": "Download production requires a populated Download URL.",
                })
            elif not is_valid_http_url(download_url):
                issues.append({
                    "excel_row": index + 2,
                    "dataset_id": row.get("Dataset ID"),
                    "reason": "Download production references an invalid Download URL.",
                })

        if method == "API":
            api_endpoint = access.get("API Endpoint")
            if normalize_optional_text(api_endpoint) is None:
                issues.append({
                    "excel_row": index + 2,
                    "dataset_id": row.get("Dataset ID"),
                    "reason": "API production requires a populated API Endpoint.",
                })
            elif not is_valid_http_url(api_endpoint):
                issues.append({
                    "excel_row": index + 2,
                    "dataset_id": row.get("Dataset ID"),
                    "reason": "API production references an invalid API Endpoint.",
                })

    return build_validation_result(
        "SR-016",
        "Production Method Consistency",
        "PASS" if not issues else "FAIL",
        len(issues),
        "All production methods and source access settings are valid."
        if not issues
        else f"Issues: {issues}",
    )


def validate_dataset_dependencies(datasets):
    input_column = "Input Dataset ID"
    required_columns = {"Dataset ID", input_column, "Production Method"}
    missing = sorted(required_columns - set(datasets.columns))
    if missing:
        return build_validation_result(
            "SR-017", "Dataset Dependency Consistency", "FAIL", len(missing), f"Missing columns: {missing}"
        )

    dataset_ids = {
        normalize_optional_text(value)
        for value in datasets["Dataset ID"]
        if normalize_optional_text(value) is not None
    }
    dependencies = {}
    issues = []

    for index, row in datasets.iterrows():
        excel_row = index + 2
        dataset_id = normalize_optional_text(row["Dataset ID"])
        input_dataset_id = normalize_optional_text(row[input_column])
        try:
            method = normalize_production_method(row["Production Method"])
        except ValueError:
            method = None

        if dataset_id is None:
            continue
        dependencies[dataset_id] = input_dataset_id

        if input_dataset_id is not None and input_dataset_id not in dataset_ids:
            issues.append({
                "excel_row": excel_row,
                "dataset_id": dataset_id,
                "reason": f"Unknown Input Dataset ID: {input_dataset_id}",
            })
        if input_dataset_id == dataset_id:
            issues.append({
                "excel_row": excel_row,
                "dataset_id": dataset_id,
                "reason": "A dataset cannot depend on itself.",
            })
        if method == "Download" and input_dataset_id is not None:
            issues.append({
                "excel_row": excel_row,
                "dataset_id": dataset_id,
                "reason": "Download datasets cannot declare Input Dataset ID.",
            })
        if method == "Preparation" and input_dataset_id is None:
            issues.append({
                "excel_row": excel_row,
                "dataset_id": dataset_id,
                "reason": "Preparation datasets require Input Dataset ID.",
            })

    reported_cycles = set()
    for start_dataset in dependencies:
        path = []
        positions = {}
        current = start_dataset
        while current is not None and current in dependencies:
            if current in positions:
                cycle = path[positions[current]:] + [current]
                signature = tuple(sorted(set(cycle)))
                if signature not in reported_cycles:
                    reported_cycles.add(signature)
                    issues.append({
                        "dataset_id": start_dataset,
                        "reason": "Circular dependency detected: " + " -> ".join(cycle),
                    })
                break
            positions[current] = len(path)
            path.append(current)
            current = dependencies.get(current)

    return build_validation_result(
        "SR-017",
        "Dataset Dependency Consistency",
        "PASS" if not issues else "FAIL",
        len(issues),
        "All dataset dependencies are valid and acyclic." if not issues else f"Issues: {issues}",
    )


def normalize_output_role(value):
    """Normalize Output Role to its canonical value."""
    normalized = normalize_optional_text(value)
    if normalized is None:
        raise ValueError("Output Role cannot be empty.")
    canonical = {role.casefold(): role for role in OUTPUT_ROLES}
    key = normalized.casefold()
    if key not in canonical:
        raise ValueError(
            f"Output Role must be one of: {sorted(OUTPUT_ROLES)}. Received: {value}"
        )
    return canonical[key]


def normalize_output_roles(dataframe):
    """Normalize the Output Role column."""
    result = dataframe.copy()
    if "Output Role" in result.columns:
        result["Output Role"] = result["Output Role"].apply(normalize_output_role)
    return result


def validate_output_roles(datasets):
    """
    Validate technical output roles.

    Rules:
    - every role is Primary or Exception;
    - non-API datasets must be Primary;
    - each API production group has exactly one Primary output;
    - each API production group has at most one Exception output.
    """
    required_columns = {
        "Dataset ID",
        "Source ID",
        "Input Dataset ID",
        "Production Method",
        "Output Role",
    }
    missing = sorted(required_columns - set(datasets.columns))
    if missing:
        return build_validation_result(
            "SR-018",
            "Output Role Consistency",
            "FAIL",
            len(missing),
            f"Missing columns: {missing}",
        )

    issues = []
    normalized = datasets.copy()
    roles = {}
    methods = {}

    for index, row in datasets.iterrows():
        try:
            roles[index] = normalize_output_role(row["Output Role"])
        except ValueError as error:
            roles[index] = None
            issues.append({"excel_row": index + 2, "reason": str(error)})

        try:
            methods[index] = normalize_production_method(row["Production Method"])
        except ValueError:
            methods[index] = None

    normalized["_output_role"] = pd.Series(roles)
    normalized["_production_method"] = pd.Series(methods)

    for index, row in normalized.iterrows():
        method = row["_production_method"]
        role = row["_output_role"]
        if method is not None and method != "API" and role not in {None, "Primary"}:
            issues.append({
                "excel_row": index + 2,
                "dataset_id": row.get("Dataset ID"),
                "reason": "Download and Preparation datasets must use Output Role Primary.",
            })

    api_rows = normalized[normalized["_production_method"].eq("API")].copy()
    if not api_rows.empty:
        api_rows["_source_group"] = api_rows["Source ID"].apply(normalize_optional_text)
        api_rows["_input_group"] = api_rows["Input Dataset ID"].apply(normalize_optional_text)

        for (source_id, input_dataset_id), group in api_rows.groupby(
            ["_source_group", "_input_group"],
            dropna=False,
        ):
            primary_count = int(group["_output_role"].eq("Primary").sum())
            exception_count = int(group["_output_role"].eq("Exception").sum())
            group_ids = group["Dataset ID"].astype(str).tolist()

            if primary_count != 1:
                issues.append({
                    "dataset_ids": group_ids,
                    "reason": (
                        "Each API source/input group requires exactly one Primary output. "
                        f"Found {primary_count}."
                    ),
                })

            if exception_count > 1:
                issues.append({
                    "dataset_ids": group_ids,
                    "reason": (
                        "Each API source/input group allows at most one Exception output. "
                        f"Found {exception_count}."
                    ),
                })

    return build_validation_result(
        "SR-018",
        "Output Role Consistency",
        "PASS" if not issues else "FAIL",
        len(issues),
        "All output roles are valid and consistent."
        if not issues
        else f"Issues: {issues}",
    )


def append_validation_result(results, result, function_name):
    if result is None:
        raise RuntimeError(f"Validation function returned None: {function_name}")
    if not isinstance(result, dict):
        raise TypeError(
            f"Validation function {function_name} returned {type(result).__name__}; expected dict."
        )
    required_keys = {"control_id", "control_name", "status", "issues", "notes"}
    missing_keys = required_keys - set(result)
    if missing_keys:
        raise ValueError(
            f"Validation result from {function_name} is missing keys: {sorted(missing_keys)}"
        )
    results.append(result)


def validate_source_registry(raise_on_failure=True):
    logger.info("Source Registry validation started")
    try:
        sources, datasets = load_registry_sheets()
        results = []

        source_schema = validate_required_columns(
            sources, REQUIRED_SOURCE_COLUMNS, SOURCES_SHEET, "SR-001"
        )
        dataset_schema = validate_required_columns(
            datasets, REQUIRED_DATASET_COLUMNS, DATASETS_SHEET, "SR-002"
        )
        append_validation_result(results, source_schema, "validate_required_columns_sources")
        append_validation_result(results, dataset_schema, "validate_required_columns_datasets")
        append_validation_result(
            results, validate_population(sources, SOURCES_SHEET, "SR-003"), "validate_population_sources"
        )
        append_validation_result(
            results, validate_population(datasets, DATASETS_SHEET, "SR-004"), "validate_population_datasets"
        )

        source_schema_valid = source_schema["status"] == "PASS"
        dataset_schema_valid = dataset_schema["status"] == "PASS"
        derived_tables = pd.DataFrame(
            columns=["Dataset ID", "Output File Name", "Derived Table Name", "Valid Table Name"]
        )

        if source_schema_valid:
            append_validation_result(
                results, validate_unique_values(sources, "Source ID", "SR-005"), "validate_unique_source_ids"
            )
            append_validation_result(results, validate_download_urls(sources), "validate_download_urls")

        if dataset_schema_valid:
            append_validation_result(
                results, validate_required_dataset_values(datasets), "validate_required_dataset_values"
            )
            append_validation_result(
                results, validate_unique_values(datasets, "Dataset ID", "SR-008"), "validate_unique_dataset_ids"
            )
            append_validation_result(
                results, validate_unique_values(datasets, "Output File Name", "SR-009"), "validate_unique_output_files"
            )
            if source_schema_valid:
                append_validation_result(
                    results, validate_source_relationships(sources, datasets), "validate_source_relationships"
                )
            append_validation_result(results, validate_boolean_columns(datasets), "validate_boolean_columns")
            append_validation_result(results, validate_output_file_names(datasets), "validate_output_file_names")
            append_validation_result(results, validate_storage_directories(datasets), "validate_storage_directories")
            table_result, derived_tables = validate_derived_table_names(datasets)
            append_validation_result(results, table_result, "validate_derived_table_names")
            append_validation_result(
                results, validate_configuration_rules(datasets), "validate_configuration_rules"
            )
            if source_schema_valid:
                append_validation_result(
                    results, validate_production_methods(sources, datasets), "validate_production_methods"
                )
            append_validation_result(
                results, validate_dataset_dependencies(datasets), "validate_dataset_dependencies"
            )
            append_validation_result(
                results, validate_output_roles(datasets), "validate_output_roles"
            )

        report = pd.DataFrame(results)
        failures = report[report["status"] == "FAIL"]
        is_valid = failures.empty

        normalized_datasets = datasets.copy()
        if dataset_schema_valid:
            normalized_datasets = normalize_boolean_columns(normalized_datasets)
            normalized_datasets = normalize_production_methods(normalized_datasets)
            normalized_datasets = normalize_output_roles(normalized_datasets)

        logger.info("Source Registry sources validated: %s", len(sources))
        logger.info("Source Registry datasets validated: %s", len(datasets))
        logger.info("Source Registry controls executed: %s", len(report))
        logger.info("Source Registry failed controls: %s", len(failures))

        if is_valid:
            populated_urls = int(sources["Download URL"].notna().sum()) if "Download URL" in sources.columns else 0
            logger.info("Source Registry Download URLs validated: %s", populated_urls)
            logger.info("Source Registry validation completed successfully")
        elif raise_on_failure:
            summary = failures[["control_id", "control_name", "notes"]].to_dict(orient="records")
            raise ValueError(f"Source Registry validation failed: {summary}")

        return {
            "validation_report": report,
            "sources": sources,
            "datasets": normalized_datasets,
            "derived_tables": derived_tables,
            "is_valid": is_valid,
        }
    except Exception as error:
        logger.exception("Source Registry validation failed: %s", error)
        raise


def main():
    validate_source_registry(raise_on_failure=True)


if __name__ == "__main__":
    main()
