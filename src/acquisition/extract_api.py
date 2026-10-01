"""
extract_api.py

Generic registry-driven API acquisition engine.

Purpose
-------
Execute API Datasets declared in the validated Source Registry.

Architecture
------------
Source Registry
        |
        v
validate_source_registry.py
        |
        v
source_registry.py
        |
        v
extract_api.py
        |
        +--> select active API Dataset
        |
        +--> resolve optional upstream Dataset
        |
        +--> resolve Connector dynamically
        |
        +--> filter runtime options through Connector capability contract
        |
        +--> build ConnectorContext
        |
        +--> connector.acquire()
        |
        +--> validate ConnectorResult
        |
        +--> stage output
        |
        +--> verify staged output
        |
        +--> atomic publication
        |
        +--> acquisition logging

Design principles
-----------------
- no provider-specific logic;
- no business Source ID hardcoding;
- no business Dataset ID hardcoding;
- no endpoint hardcoding;
- no connector-name hardcoding;
- no output-path hardcoding;
- no secrets;
- one registry row represents one official Dataset;
- acquisition remains source-faithful;
- connectors handle provider-specific protocol;
- the engine handles orchestration and publication;
- runtime capabilities are declared by connectors;
- Dataset outputs are published atomically;
- failures are isolated by Dataset unless fail-fast is requested.

Supported output formats
------------------------
API connector DataFrames can currently be published as:
- CSV
- JSON
- Parquet

Excel output is deliberately excluded from this engine.
"""

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd

from src.acquisition.acquisition_logger import log_acquisition
from src.acquisition.connectors.base import (
    BaseConnector,
    ConnectorContext,
    ConnectorResult,
)
from src.acquisition.connectors.resolver import (
    resolve_connector,
)
from src.governance.source_registry import (
    DATASET_KEY,
    build_dataset_absolute_path,
    build_resolved_registry,
    get_access_endpoint,
    get_api_enabled_datasets,
    get_connector,
    get_input_dataset_configurations,
    get_parameters,
)
from src.utils.logger import logger


# ============================================================================
# EXECUTION STATUS
# ============================================================================

SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"


# ============================================================================
# OUTPUT CONTRACT
# ============================================================================

CSV_EXTENSION = ".csv"
JSON_EXTENSION = ".json"
PARQUET_EXTENSION = ".parquet"

SUPPORTED_OUTPUT_EXTENSIONS = frozenset(
    {
        CSV_EXTENSION,
        JSON_EXTENSION,
        PARQUET_EXTENSION,
    }
)


# ============================================================================
# RUNTIME DEFAULTS
# ============================================================================

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_REQUEST_DELAY_SECONDS = 0.0

DEFAULT_USER_AGENT = (
    "investment-data-pipeline/1.0"
)


# ============================================================================
# EXECUTION MODEL
# ============================================================================

@dataclass(frozen=True)
class DatasetExecution:
    """
    Fully resolved API Dataset execution contract.

    Attributes
    ----------
    configuration
        Resolved Source Registry Dataset configuration.

    connector_name
        Technical connector identifier from the registry.

    connector
        Dynamically resolved connector implementation.

    endpoint
        API Access Endpoint resolved from the registry.

    parameters
        Provider-specific non-secret Dataset Parameters.

    input_configuration
        Optional upstream Dataset configuration.

    input_path
        Optional upstream Dataset path.

    output_path
        Final Dataset output path.
    """

    configuration: dict[str, Any]
    connector_name: str
    connector: BaseConnector
    endpoint: str
    parameters: dict[str, Any]
    input_configuration: dict[str, Any] | None
    input_path: Path | None
    output_path: Path


# ============================================================================
# COMMAND LINE
# ============================================================================

def parse_arguments():
    """
    Parse API engine command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Execute active API Datasets "
            "configured in the Source Registry."
        )
    )

    parser.add_argument(
        "--dataset-id",
        default=None,
        help=(
            "Execute one active API Dataset only. "
            "When omitted, all active API Datasets "
            "are executed."
        ),
    )

    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=(
            DEFAULT_CONNECT_TIMEOUT_SECONDS
        ),
    )

    parser.add_argument(
        "--read-timeout",
        type=float,
        default=(
            DEFAULT_READ_TIMEOUT_SECONDS
        ),
    )

    parser.add_argument(
        "--max-attempts",
        type=int,
        default=(
            DEFAULT_MAX_ATTEMPTS
        ),
    )

    parser.add_argument(
        "--backoff-seconds",
        type=float,
        default=(
            DEFAULT_BACKOFF_SECONDS
        ),
    )

    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=(
            DEFAULT_REQUEST_DELAY_SECONDS
        ),
    )

    parser.add_argument(
        "--user-agent",
        default=(
            DEFAULT_USER_AGENT
        ),
    )

    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help=(
            "Stop immediately when a Dataset fails. "
            "Default behavior continues with other "
            "independent API Datasets."
        ),
    )

    return parser.parse_args()


# ============================================================================
# NORMALIZATION
# ============================================================================

def optional_text(
    value: Any,
) -> str | None:
    """
    Normalize optional scalar text.
    """

    if value is None:
        return None

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
                return None

        except (
            TypeError,
            ValueError,
        ):

            pass

    normalized = str(
        value
    ).strip()

    return normalized or None


def positive_number(
    value: Any,
    field_name: str,
) -> float:
    """
    Normalize a strictly positive numeric value.
    """

    if isinstance(
        value,
        bool,
    ):

        raise ValueError(
            f"{field_name} must be "
            "a positive number."
        )

    try:

        normalized = float(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise ValueError(
            f"{field_name} must be numeric."
        ) from error

    if normalized <= 0:

        raise ValueError(
            f"{field_name} must be "
            "greater than zero."
        )

    return normalized


def non_negative_number(
    value: Any,
    field_name: str,
) -> float:
    """
    Normalize a non-negative numeric value.
    """

    if isinstance(
        value,
        bool,
    ):

        raise ValueError(
            f"{field_name} must be "
            "a non-negative number."
        )

    try:

        normalized = float(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise ValueError(
            f"{field_name} must be numeric."
        ) from error

    if normalized < 0:

        raise ValueError(
            f"{field_name} cannot be negative."
        )

    return normalized


def positive_integer(
    value: Any,
    field_name: str,
) -> int:
    """
    Normalize a strictly positive integer.
    """

    if isinstance(
        value,
        bool,
    ):

        raise ValueError(
            f"{field_name} must be "
            "a positive integer."
        )

    try:

        normalized = int(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise ValueError(
            f"{field_name} must be "
            "a positive integer."
        ) from error

    if normalized <= 0:

        raise ValueError(
            f"{field_name} must be "
            "greater than zero."
        )

    return normalized


# ============================================================================
# RUNTIME OPTIONS
# ============================================================================

def build_runtime_options(
    *,
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
    request_delay_seconds: float,
    user_agent: str,
) -> dict[str, Any]:
    """
    Build the complete API-engine runtime option pool.

    Individual connectors receive only the subset declared in
    BaseConnector.SUPPORTED_RUNTIME_OPTIONS.
    """

    normalized_user_agent = optional_text(
        user_agent
    )

    if normalized_user_agent is None:

        raise ValueError(
            "user_agent cannot be empty."
        )

    return {
        "connect_timeout":
            positive_number(
                connect_timeout,
                "connect_timeout",
            ),
        "read_timeout":
            positive_number(
                read_timeout,
                "read_timeout",
            ),
        "max_attempts":
            positive_integer(
                max_attempts,
                "max_attempts",
            ),
        "backoff_seconds":
            positive_number(
                backoff_seconds,
                "backoff_seconds",
            ),
        "request_delay_seconds":
            non_negative_number(
                request_delay_seconds,
                "request_delay_seconds",
            ),
        "user_agent":
            normalized_user_agent,
    }


# ============================================================================
# INPUT DATASET READING
# ============================================================================

def read_tabular_dataset(
    path: Path,
) -> pd.DataFrame:
    """
    Read an upstream Dataset according to its extension.

    This operation performs technical deserialization only.
    No analytical transformation is applied.
    """

    if not isinstance(
        path,
        Path,
    ):

        raise TypeError(
            "path must be a pathlib.Path."
        )

    if not path.exists():

        raise FileNotFoundError(
            "Input Dataset not found: "
            f"{path}"
        )

    if not path.is_file():

        raise ValueError(
            "Input Dataset path is not "
            f"a file: {path}"
        )

    extension = (
        path.suffix
        .lower()
    )

    if extension == ".csv":

        return pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )

    if extension == ".json":

        return pd.read_json(
            path,
        )

    if extension == ".parquet":

        return pd.read_parquet(
            path,
        )

    if extension == ".xls":

        return pd.read_excel(
            path,
            engine="xlrd",
        )

    if extension == ".xlsx":

        return pd.read_excel(
            path,
            engine="openpyxl",
        )

    raise ValueError(
        "Unsupported upstream Dataset "
        f"extension: {extension}"
    )


# ============================================================================
# DATASET SELECTION
# ============================================================================

def select_api_datasets(
    registry: pd.DataFrame,
    dataset_id: str | None = None,
) -> pd.DataFrame:
    """
    Select active API Datasets for execution.

    When dataset_id is omitted, all active API Datasets are selected.

    When dataset_id is supplied, exactly one active API Dataset
    must resolve.
    """

    if not isinstance(
        registry,
        pd.DataFrame,
    ):

        raise TypeError(
            "registry must be "
            "a pandas DataFrame."
        )

    api_datasets = (
        get_api_enabled_datasets(
            registry=registry
        )
    )

    requested_dataset_id = (
        optional_text(
            dataset_id
        )
    )

    if requested_dataset_id is None:

        return (
            api_datasets
            .copy(
                deep=True
            )
            .reset_index(
                drop=True
            )
        )

    matches = api_datasets[
        api_datasets[
            DATASET_KEY
        ]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.casefold()
        .eq(
            requested_dataset_id.casefold()
        )
    ]

    if matches.empty:

        raise KeyError(
            "Requested Dataset is not "
            "an active API Dataset: "
            f"{requested_dataset_id}"
        )

    if len(matches) > 1:

        raise RuntimeError(
            "Requested Dataset resolved to "
            "multiple API configurations: "
            f"{requested_dataset_id}"
        )

    return (
        matches
        .copy(
            deep=True
        )
        .reset_index(
            drop=True
        )
    )


# ============================================================================
# DATASET EXECUTION RESOLUTION
# ============================================================================

def resolve_dataset_execution(
    configuration: dict[str, Any],
    registry: pd.DataFrame,
) -> DatasetExecution:
    """
    Resolve one API Dataset execution.

    API connectors currently support zero or one direct upstream
    Dataset. The Source Registry dependency model itself supports
    multiple dependencies for other Production Methods.
    """

    if not isinstance(
        configuration,
        dict,
    ):
        raise TypeError(
            "configuration must be a dictionary."
        )

    if not isinstance(
        registry,
        pd.DataFrame,
    ):
        raise TypeError(
            "registry must be a pandas DataFrame."
        )

    dataset_id = optional_text(
        configuration.get(
            DATASET_KEY
        )
    )

    if dataset_id is None:
        raise ValueError(
            "Dataset configuration is missing Dataset ID."
        )

    connector_name = get_connector(
        configuration
    )

    if connector_name is None:
        raise ValueError(
            f"{dataset_id}: Connector is required."
        )

    endpoint = get_access_endpoint(
        configuration
    )

    if endpoint is None:
        raise ValueError(
            f"{dataset_id}: Access Endpoint is required."
        )

    parameters = get_parameters(
        configuration
    )

    connector = resolve_connector(
        connector_name
    )

    if not isinstance(
        connector,
        BaseConnector,
    ):
        raise TypeError(
            "Resolved connector does not implement "
            "BaseConnector."
        )

    # ------------------------------------------------------------------------
    # UPSTREAM DATASET RESOLUTION
    # ------------------------------------------------------------------------

    input_configurations = (
        get_input_dataset_configurations(
            dataset_id,
            registry=registry,
        )
    )

    if not isinstance(
        input_configurations,
        list,
    ):
        raise TypeError(
            "Input Dataset configurations must "
            "resolve to a list."
        )

    if len(input_configurations) > 1:
        raise ValueError(
            f"{dataset_id}: API execution currently "
            "supports at most one upstream Dataset. "
            f"Resolved inputs: {len(input_configurations)}"
        )

    input_configuration = (
        input_configurations[0]
        if input_configurations
        else None
    )

    input_path = None

    if input_configuration is not None:
        input_path = build_dataset_absolute_path(
            input_configuration
        )

    # ------------------------------------------------------------------------
    # OUTPUT DATASET RESOLUTION
    # ------------------------------------------------------------------------

    output_path = build_dataset_absolute_path(
        configuration
    )

    return DatasetExecution(
        configuration=configuration.copy(),
        connector_name=connector_name,
        connector=connector,
        endpoint=endpoint,
        parameters=parameters,
        input_configuration=input_configuration,
        input_path=input_path,
        output_path=output_path,
    )
# ============================================================================
# CONNECTOR INPUT
# ============================================================================

def load_execution_input(
    execution: DatasetExecution,
) -> pd.DataFrame | None:
    """
    Load an optional upstream Dataset.
    """

    if not isinstance(
        execution,
        DatasetExecution,
    ):

        raise TypeError(
            "execution must be "
            "a DatasetExecution."
        )

    if execution.input_path is None:

        return None

    logger.info(
        "Loading API upstream Dataset: %s",
        execution.input_path,
    )

    input_data = read_tabular_dataset(
        execution.input_path
    )

    logger.info(
        "API upstream Dataset loaded: "
        "Rows=%s; Columns=%s",
        len(input_data),
        len(input_data.columns),
    )

    return input_data


# ============================================================================
# CONNECTOR CONTEXT
# ============================================================================

def build_connector_context(
    execution: DatasetExecution,
    *,
    input_data: pd.DataFrame | None,
    runtime_options: dict[str, Any],
) -> ConnectorContext:
    """
    Build the ConnectorContext for one Dataset execution.

    Runtime options are filtered through the connector capability
    contract without provider-specific branching.
    """

    connector_runtime_options = (
        execution.connector
        .filter_runtime_options(
            runtime_options
        )
    )

    logger.debug(
        "API Connector runtime options "
        "resolved: Connector=%s; Options=%s",
        execution.connector_name,
        sorted(
            connector_runtime_options
            .keys()
        ),
    )

    return ConnectorContext(
        endpoint=execution.endpoint,
        parameters=execution.parameters,
        input_data=input_data,
        runtime_options=(
            connector_runtime_options
        ),
    )


# ============================================================================
# CONNECTOR RESULT CONTRACT
# ============================================================================

def validate_connector_result(
    result: ConnectorResult,
) -> None:
    """
    Validate the common connector output contract.

    Zero observation rows are permitted when the connector returns
    a meaningful tabular schema.

    A DataFrame with zero columns is rejected because it cannot form
    a stable published Dataset contract.
    """

    if not isinstance(
        result,
        ConnectorResult,
    ):

        raise TypeError(
            "Connector acquire() must "
            "return ConnectorResult."
        )

    if not isinstance(
        result.data,
        pd.DataFrame,
    ):

        raise TypeError(
            "ConnectorResult.data must "
            "be a pandas DataFrame."
        )

    if not isinstance(
        result.metadata,
        dict,
    ):

        raise TypeError(
            "ConnectorResult.metadata must "
            "be a dictionary."
        )

    if len(
        result.data.columns
    ) == 0:

        raise ValueError(
            "ConnectorResult.data has "
            "no columns."
       )

    duplicate_columns = (
        result.data.columns[
            result.data.columns
            .duplicated(
                keep=False
            )
        ]
        .astype(str)
        .tolist()
    )

    if duplicate_columns:

        raise ValueError(
            "ConnectorResult contains "
            "duplicate columns: "
            f"{duplicate_columns}"
        )


# ============================================================================
# OUTPUT SERIALIZATION
# ============================================================================

def serialize_dataframe(
    dataframe: pd.DataFrame,
    path: Path,
) -> None:
    """
    Serialize a DataFrame to a staging file.

    Publication is performed separately.
    """

    if not isinstance(
        dataframe,
        pd.DataFrame,
    ):

        raise TypeError(
            "dataframe must be "
            "a pandas DataFrame."
        )

    if not isinstance(
        path,
        Path,
    ):

        raise TypeError(
            "path must be a pathlib.Path."
        )

    extension = (
        path.suffix
        .lower()
    )

    if extension == CSV_EXTENSION:

        dataframe.to_csv(
            path,
            index=False,
            encoding="utf-8",
        )

        return

    if extension == JSON_EXTENSION:

        dataframe.to_json(
            path,
            orient="records",
            force_ascii=False,
            date_format="iso",
        )

        return

    if extension == PARQUET_EXTENSION:

        dataframe.to_parquet(
            path,
            index=False,
        )

        return

    raise ValueError(
        "Unsupported API Dataset "
        f"output extension: {extension}"
    )


# ============================================================================
# STAGING
# ============================================================================

def create_staging_path(
    output_path: Path,
) -> Path:
    """
    Create a staging file beside the final Dataset.

    Using the destination directory ensures staging and publication
    normally occur on the same filesystem.
    """

    if not isinstance(
        output_path,
        Path,
    ):

        raise TypeError(
            "output_path must be "
            "a pathlib.Path."
        )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with NamedTemporaryFile(
        mode="wb",
        prefix=(
            f".{output_path.stem}_"
        ),
        suffix=output_path.suffix,
        dir=output_path.parent,
        delete=False,
    ) as temporary_file:

        temporary_path = Path(
            temporary_file.name
        )

    return temporary_path


def stage_dataframe(
    dataframe: pd.DataFrame,
    output_path: Path,
) -> Path:
    """
    Serialize a Dataset into a temporary staging file.
    """

    extension = (
        output_path.suffix
        .lower()
    )

    if (
        extension
        not in SUPPORTED_OUTPUT_EXTENSIONS
    ):

        raise ValueError(
            "API engine cannot publish "
            "output extension: "
            f"{extension}"
        )

    staging_path = (
        create_staging_path(
            output_path
        )
    )

    try:

        serialize_dataframe(
            dataframe,
            staging_path,
        )

        if not staging_path.exists():

            raise RuntimeError(
                "Dataset staging file "
                "was not created."
            )

        if not staging_path.is_file():

            raise RuntimeError(
                "Dataset staging path "
                "is not a file."
            )

        return staging_path

    except Exception:

        staging_path.unlink(
            missing_ok=True
        )

        raise


# ============================================================================
# STAGING VERIFICATION
# ============================================================================

def read_staged_dataframe(
    path: Path,
) -> pd.DataFrame:
    """
    Read a staged Dataset for structural verification.
    """

    extension = (
        path.suffix
        .lower()
    )

    if extension == CSV_EXTENSION:

        return pd.read_csv(
            path,
            encoding="utf-8",
        )

    if extension == JSON_EXTENSION:

        return pd.read_json(
            path,
            orient="records",
        )

    if extension == PARQUET_EXTENSION:

        return pd.read_parquet(
            path,
        )

    raise ValueError(
        "Unsupported staged Dataset "
        f"extension: {extension}"
    )


def verify_staged_dataframe(
    original: pd.DataFrame,
    staging_path: Path,
) -> None:
    """
    Verify staged Dataset structure before publication.

    Column order and row count must survive serialization.

    Exact pandas dtypes are deliberately not compared because
    text-based serialization can legitimately alter dtype inference.
    """

    if not staging_path.exists():

        raise FileNotFoundError(
            "Staging file not found: "
            f"{staging_path}"
        )

    verification = (
        read_staged_dataframe(
            staging_path
        )
    )

    expected_columns = [
        str(column)
        for column
        in original.columns
    ]

    actual_columns = [
        str(column)
        for column
        in verification.columns
    ]

    if actual_columns != expected_columns:

        raise RuntimeError(
            "Staged Dataset columns do not "
            "match ConnectorResult columns. "
            f"Expected={expected_columns}; "
            f"Actual={actual_columns}"
        )

    if len(verification) != len(original):

        raise RuntimeError(
            "Staged Dataset row count does "
            "not match ConnectorResult. "
            f"Expected={len(original)}; "
            f"Actual={len(verification)}"
        )


# ============================================================================
# ATOMIC PUBLICATION
# ============================================================================

def publish_staged_file(
    staging_path: Path,
    output_path: Path,
) -> None:
    """
    Atomically replace the published Dataset.

    Staging is performed beside the target file so publication can
    use a same-filesystem replacement under normal operation.
    """

    if not staging_path.exists():

        raise FileNotFoundError(
            "Staging file does not exist: "
            f"{staging_path}"
        )

    if not staging_path.is_file():

        raise ValueError(
            "Staging path is not a file: "
            f"{staging_path}"
        )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    os.replace(
        staging_path,
        output_path,
    )


# ============================================================================
# AUDIT METADATA
# ============================================================================

def sanitize_metadata(
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """
    Remove potentially sensitive metadata values recursively.

    Connectors must never return secrets. This is an additional
    defense-in-depth guard before metadata enters acquisition logs.
    """

    if not isinstance(
        metadata,
        dict,
    ):

        raise TypeError(
            "metadata must be a dictionary."
        )

    sensitive_fragments = (
        "api_key",
        "apikey",
        "token",
        "secret",
        "password",
        "authorization",
        "credential",
    )

    sanitized = {}

    for key, value in metadata.items():

        string_key = str(
            key
        )

        normalized_key = (
            string_key
            .casefold()
        )

        if any(
            fragment
            in normalized_key
            for fragment
            in sensitive_fragments
        ):

            sanitized[
                string_key
            ] = "[REDACTED]"

            continue

        if isinstance(
            value,
            dict,
        ):

            sanitized[
                string_key
            ] = sanitize_metadata(
                value
            )

        elif isinstance(
            value,
            (list, tuple),
        ):

            sanitized[
                string_key
            ] = [
                item
                for item in value
            ]

        else:

            sanitized[
                string_key
            ] = value

    return sanitized


def build_success_notes(
    execution: DatasetExecution,
    result: ConnectorResult,
) -> str:
    """
    Build non-secret structured success notes.
    """

    dataset_id = (
        execution.configuration[
            DATASET_KEY
        ]
    )

    payload = {
        "dataset_id":
            dataset_id,
        "connector":
            execution.connector_name,
        "records_produced":
            len(
                result.data
            ),
        "metadata":
            sanitize_metadata(
                result.metadata
            ),
    }

    return json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
    )


def build_failure_notes(
    execution: DatasetExecution,
    error: Exception,
) -> str:
    """
    Build non-secret structured failure notes.
    """

    dataset_id = (
        execution.configuration[
            DATASET_KEY
        ]
    )

    payload = {
        "dataset_id":
            dataset_id,
        "connector":
            execution.connector_name,
        "error_type":
            type(
                error
            ).__name__,
        "error":
            str(
                error
            ),
    }

    return json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
    )


# ============================================================================
# ACQUISITION LOGGING
# ============================================================================

def log_dataset_event(
    *,
    output_path: Path,
    status: str,
    records_produced: int | None,
    notes: str,
):
    """
    Persist one Dataset acquisition event.
    """

    return log_acquisition(
        output_file=output_path,
        storage_location=(
            output_path.parent
        ),
        status=status,
        records_produced=(
            records_produced
        ),
        notes=notes,
    )


# ============================================================================
# SINGLE DATASET EXECUTION
# ============================================================================

def execute_api_dataset(
    execution: DatasetExecution,
    *,
    runtime_options: dict[str, Any],
) -> Path:
    """
    Execute one fully resolved API Dataset.

    Execution sequence
    ------------------
    1. Load optional upstream Dataset.
    2. Filter runtime options through connector capabilities.
    3. Build ConnectorContext.
    4. Validate connector context.
    5. Execute connector acquisition.
    6. Validate ConnectorResult.
    7. Stage output.
    8. Verify staged Dataset.
    9. Atomically publish output.
    10. Persist acquisition event.
    """

    if not isinstance(
        execution,
        DatasetExecution,
    ):

        raise TypeError(
            "execution must be "
            "a DatasetExecution."
        )

    dataset_id = (
        execution.configuration[
            DATASET_KEY
        ]
    )

    staging_path = None

    logger.info(
        "API Dataset execution started: %s",
        dataset_id,
    )

    logger.info(
        "API Connector resolved: %s",
        execution.connector_name,
    )

    logger.info(
        "API output resolved: %s",
        execution.output_path,
    )

    try:

        input_data = (
            load_execution_input(
                execution
            )
        )

        context = (
            build_connector_context(
                execution,
                input_data=input_data,
                runtime_options=(
                    runtime_options
                ),
            )
        )

        execution.connector.validate_context(
            context
        )

        result = (
            execution.connector.acquire(
                context
            )
        )

        validate_connector_result(
            result
        )

        staging_path = stage_dataframe(
            result.data,
            execution.output_path,
        )

        verify_staged_dataframe(
            result.data,
            staging_path,
        )

        publish_staged_file(
            staging_path,
            execution.output_path,
        )

        staging_path = None

        notes = build_success_notes(
            execution,
            result,
        )

        run_id = log_dataset_event(
            output_path=(
                execution.output_path
            ),
            status=SUCCESS_STATUS,
            records_produced=(
                len(
                    result.data
                )
            ),
            notes=notes,
        )

        logger.info(
            "API Dataset acquisition logged: "
            "Dataset=%s; RunID=%s",
            dataset_id,
            run_id,
        )

        logger.info(
            "API Dataset execution completed: "
            "Dataset=%s; Records=%s",
            dataset_id,
            len(
                result.data
            ),
        )

        return execution.output_path

    except Exception as error:

        if staging_path is not None:

            staging_path.unlink(
                missing_ok=True
            )

        logger.exception(
            "API Dataset execution failed: "
            "Dataset=%s; Error=%s",
            dataset_id,
            error,
        )

        try:

            failure_notes = (
                build_failure_notes(
                    execution,
                    error,
                )
            )

            log_dataset_event(
                output_path=(
                    execution.output_path
                ),
                status=FAILED_STATUS,
                records_produced=None,
                notes=failure_notes,
            )

        except Exception as logging_error:

            logger.exception(
                "Unable to persist failed "
                "API acquisition event: %s",
                logging_error,
            )

        raise


# ============================================================================
# API ENGINE
# ============================================================================

def extract_api(
    *,
    dataset_id: str | None = None,
    connect_timeout: float = (
        DEFAULT_CONNECT_TIMEOUT_SECONDS
    ),
    read_timeout: float = (
        DEFAULT_READ_TIMEOUT_SECONDS
    ),
    max_attempts: int = (
        DEFAULT_MAX_ATTEMPTS
    ),
    backoff_seconds: float = (
        DEFAULT_BACKOFF_SECONDS
    ),
    request_delay_seconds: float = (
        DEFAULT_REQUEST_DELAY_SECONDS
    ),
    user_agent: str = (
        DEFAULT_USER_AGENT
    ),
    fail_fast: bool = False,
) -> list:
    """
    Execute generic API acquisition.

    When dataset_id is supplied, exactly one active API Dataset
    is executed.

    When dataset_id is omitted, all active API Datasets are
    executed in deterministic Source Registry order.

    Independent Dataset failures are collected by default.
    fail_fast=True stops execution immediately.

    Returns
    -------
    list[pathlib.Path]
        Successfully published Dataset paths.

    Raises
    ------
    RuntimeError
        When one or more Datasets fail during a multi-Dataset run.
    """

    runtime_options = (
        build_runtime_options(
            connect_timeout=(
                connect_timeout
            ),
            read_timeout=(
                read_timeout
            ),
            max_attempts=(
                max_attempts
            ),
            backoff_seconds=(
                backoff_seconds
            ),
            request_delay_seconds=(
                request_delay_seconds
            ),
            user_agent=(
                user_agent
            ),
        )
    )

    registry = (
        build_resolved_registry()
    )

    datasets = (
        select_api_datasets(
            registry,
            dataset_id=dataset_id,
        )
    )

    if datasets.empty:

        logger.info(
            "No active API Datasets "
            "are configured."
        )

        return []

    successful_paths = []
    failures = []

    logger.info(
        "Generic API acquisition started: "
        "Datasets=%s",
        len(
            datasets
        ),
    )

    for _, row in datasets.iterrows():

        configuration = (
            row.to_dict()
        )

        current_dataset_id = (
            optional_text(
                configuration.get(
                    DATASET_KEY
                )
            )
        )

        try:

            execution = (
                resolve_dataset_execution(
                    configuration,
                    registry,
                )
            )

            output_path = (
                execute_api_dataset(
                    execution,
                    runtime_options=(
                        runtime_options
                    ),
                )
            )

            successful_paths.append(
                output_path
            )

        except Exception as error:

            failure = {
                "dataset_id":
                    current_dataset_id,
                "error_type":
                    type(
                        error
                    ).__name__,
                "error":
                    str(
                        error
                    ),
            }

            failures.append(
                failure
            )

            if fail_fast:

                logger.error(
                    "API acquisition stopped "
                    "because fail_fast is enabled."
                )

                raise

    if failures:

        logger.error(
            "Generic API acquisition completed "
            "with failures: "
            "Success=%s; Failed=%s",
            len(
                successful_paths
            ),
            len(
                failures
            ),
        )

        raise RuntimeError(
            "One or more API Dataset "
            "acquisitions failed: "
            f"{failures}"
        )

    logger.info(
        "Generic API acquisition completed "
        "successfully: Datasets=%s",
        len(
            successful_paths
        ),
    )

    return successful_paths


# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Execute the generic API acquisition engine.
    """

    arguments = parse_arguments()

    paths = extract_api(
        dataset_id=(
            arguments.dataset_id
        ),
        connect_timeout=(
            arguments.connect_timeout
        ),
        read_timeout=(
            arguments.read_timeout
        ),
        max_attempts=(
            arguments.max_attempts
        ),
        backoff_seconds=(
            arguments.backoff_seconds
        ),
        request_delay_seconds=(
            arguments.request_delay_seconds
        ),
        user_agent=(
            arguments.user_agent
        ),
        fail_fast=(
            arguments.fail_fast
        ),
    )

    logger.info(
        "API engine published "
        "%s Dataset(s)",
        len(
            paths
        ),
    )


if __name__ == "__main__":
    main()