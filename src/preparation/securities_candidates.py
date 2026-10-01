"""
securities_candidates.py

Build a Security Candidates Dataset from its configured upstream Dataset.

Purpose
-------
Transform a source-faithful benchmark holdings Dataset into an internal
Security Candidates Dataset used by subsequent identifier-mapping stages.

Architecture
------------
Source Registry
        |
        v
Upstream Benchmark Holdings Dataset
        |
        v
securities_candidates.py
        |
        +--> resolve Preparation configuration
        +--> resolve exactly one upstream Dataset
        +--> parse SpreadsheetML workbook
        +--> identify configured worksheet
        +--> locate holdings table
        +--> apply configured asset-class selection
        +--> retain candidate identity attributes
        +--> validate mandatory candidate keys
        +--> deduplicate according to configured business keys
        +--> stage CSV
        +--> verify staged CSV
        +--> atomic publication
        +--> preparation logging
        |
        v
Security Candidates Dataset

Design principles
-----------------
- no business Dataset ID hardcoding;
- no Source ID hardcoding;
- no provider hardcoding;
- no business output file-name hardcoding;
- no storage-path hardcoding;
- no upstream Dataset ID hardcoding;
- configurable preparation rules come from Dataset Parameters;
- workbook parsing remains technical;
- candidate selection is Preparation business logic;
- the upstream Dataset is never modified;
- output publication is atomic.
"""

import argparse
import os
import re
from html import unescape
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd

from src.acquisition.acquisition_logger import (
    log_preparation,
)
from src.governance.source_registry import (
    ACTIVE_KEY,
    DATASET_KEY,
    PREPARATION_METHOD,
    PRODUCTION_METHOD_KEY,
    build_dataset_absolute_path,
    build_resolved_registry,
    get_dataset_by_id,
    get_input_dataset_configurations,
    get_parameters,
    get_preparation_enabled_datasets,
)
from src.utils.logger import logger


# ============================================================================
# EXECUTION STATUS
# ============================================================================

SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"


# ============================================================================
# PREPARATION PARAMETER CONTRACT
# ============================================================================

WORKSHEET_PARAMETER = "worksheet"

ASSET_CLASS_COLUMN_PARAMETER = (
    "asset_class_column"
)

INCLUDED_ASSET_CLASSES_PARAMETER = (
    "included_asset_classes"
)

DEDUPLICATION_COLUMNS_PARAMETER = (
    "deduplication_columns"
)

REQUIRED_PARAMETERS = frozenset(
    {
        WORKSHEET_PARAMETER,
        ASSET_CLASS_COLUMN_PARAMETER,
        INCLUDED_ASSET_CLASSES_PARAMETER,
        DEDUPLICATION_COLUMNS_PARAMETER,
    }
)

SUPPORTED_PARAMETERS = (
    REQUIRED_PARAMETERS
)


# ============================================================================
# HOLDINGS CONTRACT
# ============================================================================

SOURCE_COLUMNS = (
    "Ticker",
    "Name",
    "Sector",
    "Asset Class",
    "Market Value",
    "Weight (%)",
    "Notional Value",
    "Quantity",
    "Price",
    "Location",
    "Exchange",
    "Currency",
    "FX Rate",
    "Accrual Date",
)

OUTPUT_COLUMNS = (
    "Ticker",
    "Name",
    "Location",
    "Exchange",
    "Currency",
    "Asset Class",
)

MANDATORY_CANDIDATE_COLUMNS = (
    "Ticker",
    "Exchange",
)


# ============================================================================
# SPREADSHEETML CONTRACT
# ============================================================================

WORKSHEET_PATTERN = re.compile(
    r"<(?:[A-Za-z_][\w.-]*:)?Worksheet\b"
    r"[^>]*(?:[A-Za-z_][\w.-]*:)?Name\s*=\s*"
    r"(?P<quote>[\"']){worksheet}(?P=quote)[^>]*>"
    r"(?P<body>.*?)"
    r"</(?:[A-Za-z_][\w.-]*:)?Worksheet\s*>",
    flags=(
        re.IGNORECASE
        | re.DOTALL
    ),
)

ROW_PATTERN = re.compile(
    r"<(?:[A-Za-z_][\w.-]*:)?Row\b[^>]*>"
    r"(?P<body>.*?)"
    r"</(?:[A-Za-z_][\w.-]*:)?Row\s*>",
    flags=(
        re.IGNORECASE
        | re.DOTALL
    ),
)

CELL_PATTERN = re.compile(
    r"<(?:[A-Za-z_][\w.-]*:)?Cell\b"
    r"(?P<attributes>[^>]*)>"
    r"(?P<body>.*?)"
    r"</(?:[A-Za-z_][\w.-]*:)?Cell\s*>",
    flags=(
        re.IGNORECASE
        | re.DOTALL
    ),
)

DATA_PATTERN = re.compile(
    r"<(?:[A-Za-z_][\w.-]*:)?Data\b[^>]*>"
    r"(?P<value>.*?)"
    r"</(?:[A-Za-z_][\w.-]*:)?Data\s*>",
    flags=(
        re.IGNORECASE
        | re.DOTALL
    ),
)

INDEX_PATTERN = re.compile(
    r'(?:[A-Za-z_][\w.-]*:)?Index\s*=\s*["\']'
    r'(?P<index>\d+)["\']',
    flags=re.IGNORECASE,
)

TAG_PATTERN = re.compile(
    r"<[^>]+>"
)


# ============================================================================
# COMMAND LINE
# ============================================================================

def parse_arguments():
    """
    Parse Security Candidates preparation arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Build a Security Candidates Dataset "
            "configured with "
            "Production Method=Preparation."
        )
    )

    parser.add_argument(
        "--dataset-id",
        default=None,
        help=(
            "Preparation Dataset ID. "
            "Required when several active "
            "Preparation Datasets are configured."
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


def required_text(
    value: Any,
    field_name: str,
) -> str:
    """
    Normalize a required text value.
    """

    normalized = optional_text(
        value
    )

    if normalized is None:

        raise ValueError(
            f"{field_name} is required."
        )

    return normalized


def normalize_text_list(
    value: Any,
    field_name: str,
) -> list[str]:
    """
    Normalize a non-empty list of unique strings.
    """

    if not isinstance(
        value,
        list,
    ):

        raise ValueError(
            f"{field_name} must be "
            "a JSON array."
        )

    if not value:

        raise ValueError(
            f"{field_name} cannot be empty."
        )

    result = []

    seen = set()

    for item in value:

        normalized = required_text(
            item,
            field_name,
        )

        key = (
            normalized
            .casefold()
        )

        if key in seen:

            raise ValueError(
                f"{field_name} contains "
                "duplicate value: "
                f"{normalized}"
            )

        seen.add(
            key
        )

        result.append(
            normalized
        )

    return result


# ============================================================================
# PREPARATION PARAMETERS
# ============================================================================

def validate_preparation_parameters(
    parameters: dict[str, Any],
) -> None:
    """
    Validate Security Candidates preparation Parameters.
    """

    if not isinstance(
        parameters,
        dict,
    ):

        raise TypeError(
            "Preparation Parameters must "
            "be a dictionary."
        )

    missing = sorted(
        REQUIRED_PARAMETERS
        - set(
            parameters
        )
    )

    if missing:

        raise ValueError(
            "Security Candidates Parameters "
            f"are missing: {missing}"
        )

    unsupported = sorted(
        set(
            parameters
        )
        - SUPPORTED_PARAMETERS
    )

    if unsupported:

        raise ValueError(
            "Unsupported Security Candidates "
            f"Parameters: {unsupported}"
        )

    required_text(
        parameters[
            WORKSHEET_PARAMETER
        ],
        WORKSHEET_PARAMETER,
    )

    asset_class_column = required_text(
        parameters[
            ASSET_CLASS_COLUMN_PARAMETER
        ],
        ASSET_CLASS_COLUMN_PARAMETER,
    )

    if (
        asset_class_column
        not in SOURCE_COLUMNS
    ):

        raise ValueError(
            "Configured asset_class_column "
            "is not part of the holdings "
            "contract: "
            f"{asset_class_column}"
        )

    normalize_text_list(
        parameters[
            INCLUDED_ASSET_CLASSES_PARAMETER
        ],
        INCLUDED_ASSET_CLASSES_PARAMETER,
    )

    deduplication_columns = (
        normalize_text_list(
            parameters[
                DEDUPLICATION_COLUMNS_PARAMETER
            ],
            DEDUPLICATION_COLUMNS_PARAMETER,
        )
    )

    unsupported_deduplication_columns = (
        sorted(
            set(
                deduplication_columns
            )
            - set(
                OUTPUT_COLUMNS
            )
        )
    )

    if unsupported_deduplication_columns:

        raise ValueError(
            "Deduplication columns must belong "
            "to the Security Candidates output "
            "contract. Unsupported columns: "
            f"{unsupported_deduplication_columns}"
        )

    missing_mandatory_keys = sorted(
        set(
            MANDATORY_CANDIDATE_COLUMNS
        )
        - set(
            deduplication_columns
        )
    )

    if missing_mandatory_keys:

        raise ValueError(
            "Security Candidates "
            "deduplication_columns must "
            "contain the mandatory business "
            "key columns: "
            f"{missing_mandatory_keys}"
        )


# ============================================================================
# PREPARATION CONFIGURATION
# ============================================================================

def validate_preparation_configuration(
    configuration: dict[str, Any],
) -> None:
    """
    Validate runtime invariants of one Preparation configuration.

    Complete Source Registry validation remains the responsibility
    of validate_source_registry.py.
    """

    if not isinstance(
        configuration,
        dict,
    ):

        raise TypeError(
            "configuration must be "
            "a dictionary."
        )

    dataset_id = required_text(
        configuration.get(
            DATASET_KEY
        ),
        DATASET_KEY,
    )

    method = required_text(
        configuration.get(
            PRODUCTION_METHOD_KEY
        ),
        PRODUCTION_METHOD_KEY,
    )

    if (
        method.casefold()
        != PREPARATION_METHOD.casefold()
    ):

        raise ValueError(
            f"{dataset_id} is not configured "
            "for Preparation."
        )

    active = configuration.get(
        ACTIVE_KEY
    )

    if not isinstance(
        active,
        bool,
    ):

        raise TypeError(
            f"{dataset_id}: Active must "
            "be boolean."
        )

    if not active:

        raise ValueError(
            f"{dataset_id} is inactive."
        )

    parameters = get_parameters(
        configuration
    )

    validate_preparation_parameters(
        parameters
    )


def select_preparation_configuration(
    dataset_id: Any | None = None,
    *,
    registry: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """
    Select the Preparation Dataset to execute.

    When dataset_id is omitted, exactly one active Preparation
    Dataset must exist.

    No Dataset ID is embedded in this module.
    """

    resolved = (
        build_resolved_registry()
        if registry is None
        else registry.copy(
            deep=True
        )
    )

    requested_dataset_id = optional_text(
        dataset_id
    )

    if requested_dataset_id is not None:

        configuration = (
            get_dataset_by_id(
                requested_dataset_id,
                registry=resolved,
            )
            .to_dict()
        )

        validate_preparation_configuration(
            configuration
        )

        return configuration

    candidates = (
        get_preparation_enabled_datasets(
            registry=resolved
        )
    )

    if candidates.empty:

        raise ValueError(
            "No active Preparation Dataset "
            "was found in the Source Registry."
        )

    if len(
        candidates
    ) != 1:

        identifiers = (
            candidates[
                DATASET_KEY
            ]
            .astype(str)
            .tolist()
        )

        raise ValueError(
            "Several active Preparation Datasets "
            "were found. Provide --dataset-id. "
            f"Available values: {identifiers}"
        )

    configuration = (
        candidates
        .iloc[0]
        .to_dict()
    )

    validate_preparation_configuration(
        configuration
    )

    return configuration


# ============================================================================
# DEPENDENCY RESOLUTION
# ============================================================================

def resolve_preparation_paths(
    configuration: dict[str, Any],
    *,
    registry: pd.DataFrame,
) -> tuple[Path, Path]:
    """
    Resolve the upstream Dataset and output path.

    Security Candidates requires exactly one upstream Dataset.

    The Source Registry supports multiple Preparation dependencies,
    but this specific transformation deliberately has a one-input
    contract.
    """

    dataset_id = required_text(
        configuration.get(
            DATASET_KEY
        ),
        DATASET_KEY,
    )

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
            "Input Dataset configurations "
            "must resolve to a list."
        )

    if len(
        input_configurations
    ) != 1:

        raise ValueError(
            f"{dataset_id}: Security Candidates "
            "requires exactly one upstream Dataset. "
            "Resolved inputs: "
            f"{len(input_configurations)}"
        )

    input_path = (
        build_dataset_absolute_path(
            input_configurations[
                0
            ]
        )
    )

    output_path = (
        build_dataset_absolute_path(
            configuration
        )
    )

    if input_path == output_path:

        raise ValueError(
            "Input and output Dataset paths "
            "cannot be identical."
        )

    return (
        input_path,
        output_path,
    )


# ============================================================================
# SPREADSHEETML INPUT
# ============================================================================

def read_text_file(
    file_path: Path,
) -> str:
    """
    Read the upstream workbook as text.

    The benchmark artifact is expected to contain SpreadsheetML
    XML content. Several common encodings are attempted without
    modifying the source artifact.
    """

    if not isinstance(
        file_path,
        Path,
    ):

        raise TypeError(
            "file_path must be "
            "a pathlib.Path."
        )

    if not file_path.exists():

        raise FileNotFoundError(
            "Input Dataset not found: "
            f"{file_path}"
        )

    if not file_path.is_file():

        raise ValueError(
            "Input Dataset path is not "
            f"a file: {file_path}"
        )

    if (
        file_path.stat().st_size
        <= 0
    ):

        raise ValueError(
            "Input Dataset is empty: "
            f"{file_path}"
        )

    raw = file_path.read_bytes()

    for encoding in (
        "utf-8-sig",
        "utf-16",
        "latin-1",
    ):

        try:

            text = raw.decode(
                encoding
            )

            if text.strip():

                return text

        except UnicodeDecodeError:

            continue

    raise ValueError(
        "Unable to decode input Dataset: "
        f"{file_path}"
    )


def extract_worksheet_content(
    workbook_text: str,
    worksheet_name: str,
) -> str:
    """
    Extract one configured SpreadsheetML worksheet.
    """

    escaped_name = re.escape(
        required_text(
            worksheet_name,
            WORKSHEET_PARAMETER,
        )
    )

    pattern = re.compile(
        WORKSHEET_PATTERN.pattern.format(
            worksheet=escaped_name
        ),
        flags=WORKSHEET_PATTERN.flags,
    )

    match = pattern.search(
        workbook_text
    )

    if match is None:

        raise ValueError(
            "Configured worksheet not found: "
            f"{worksheet_name}"
        )

    return match.group(
        "body"
    )


def clean_cell_value(
    value: Any,
) -> str:
    """
    Normalize SpreadsheetML cell text.
    """

    if value is None:

        return ""

    without_tags = TAG_PATTERN.sub(
        "",
        str(
            value
        ),
    )

    return (
        unescape(
            without_tags
        )
        .replace(
            "\xa0",
            " ",
        )
        .strip()
    )


def extract_row_values(
    row_content: str,
) -> list[str]:
    """
    Extract SpreadsheetML cell values preserving sparse positions.
    """

    values = []
    next_position = 1

    for cell_match in CELL_PATTERN.finditer(
        row_content
    ):

        attributes = (
            cell_match.group(
                "attributes"
            )
            or ""
        )

        index_match = INDEX_PATTERN.search(
            attributes
        )

        if index_match is not None:

            explicit_position = int(
                index_match.group(
                    "index"
                )
            )

            if explicit_position < 1:
                raise ValueError(
                    "SpreadsheetML cell Index "
                    "must be positive."
                )

            while next_position < explicit_position:
                values.append("")
                next_position += 1

        data_match = DATA_PATTERN.search(
            cell_match.group(
                "body"
            )
        )

        value = clean_cell_value(
            data_match.group(
                "value"
            )
            if data_match is not None
            else ""
        )

        values.append(
            value
        )

        next_position += 1

    return values

def extract_worksheet_rows(
    worksheet_content: str,
) -> list[list[str]]:
    """
    Extract non-empty SpreadsheetML rows.
    """

    rows = [
        extract_row_values(
            match.group(
                "body"
            )
        )
        for match
        in ROW_PATTERN.finditer(
            worksheet_content
        )
    ]

    rows = [
        row
        for row in rows
        if any(
            value != ""
            for value in row
        )
    ]

    if not rows:

        raise ValueError(
            "No worksheet rows were extracted."
        )

    return rows


def find_header_row(
    rows: list[list[str]],
) -> int:
    """
    Locate the holdings-table header.
    """

    expected = list(
        SOURCE_COLUMNS
    )

    for index, row in enumerate(
        rows
    ):

        if (
            row[
                :len(
                    expected
                )
            ]
            == expected
        ):

            return index

    raise ValueError(
        "The expected holdings header "
        "was not found. "
        "Expected columns: "
        f"{expected}"
    )


def reconstruct_holdings(
    rows: list[list[str]],
    header_index: int,
) -> pd.DataFrame:
    """
    Reconstruct the holdings table below the detected header.
    """

    if not isinstance(
        header_index,
        int,
    ):

        raise TypeError(
            "header_index must be integer."
        )

    records = []

    expected_length = len(
        SOURCE_COLUMNS
    )

    for row in rows[
        header_index + 1:
    ]:

        normalized = list(
            row[
                :expected_length
            ]
        )

        if (
            len(
                normalized
            )
            < expected_length
        ):

            normalized.extend(
                [""] * (
                    expected_length
                    - len(
                        normalized
                    )
                )
            )

        if not any(
            value != ""
            for value in normalized
        ):

            continue

        records.append(
            normalized
        )

    holdings = pd.DataFrame(
        records,
        columns=SOURCE_COLUMNS,
    )

    if holdings.empty:

        raise ValueError(
            "No holdings records "
            "were reconstructed."
        )

    return holdings


# ============================================================================
# SECURITY CANDIDATE TRANSFORMATION
# ============================================================================

def build_candidates_dataframe(
    holdings: pd.DataFrame,
    *,
    asset_class_column: str,
    included_asset_classes: list[str],
    deduplication_columns: list[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Build Security Candidates from source holdings.

    Business rules
    --------------
    - retain configured asset classes;
    - retain the Security Candidates output attributes;
    - normalize textual candidate values;
    - require mandatory candidate keys;
    - reject tickers without any alphanumeric character;
    - deduplicate according to configured business-key columns.

    Returns
    -------
    tuple
        candidates
        transformation_metadata
    """

    if not isinstance(
        holdings,
        pd.DataFrame,
    ):

        raise TypeError(
            "holdings must be "
            "a pandas DataFrame."
        )

    required_source_columns = (
        set(
            OUTPUT_COLUMNS
        )
        | {
            asset_class_column,
        }
    )

    missing_columns = sorted(
        required_source_columns
        - set(
            holdings.columns
        )
    )

    if missing_columns:

        raise ValueError(
            "Holdings data is missing "
            f"columns: {missing_columns}"
        )

    included_lookup = {
        value.casefold()
        for value
        in included_asset_classes
    }

    asset_classes = (
        holdings[
            asset_class_column
        ]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    inclusion_mask = (
        asset_classes
        .str.casefold()
        .isin(
            included_lookup
        )
    )

    candidates = (
        holdings.loc[
            inclusion_mask,
            list(
                OUTPUT_COLUMNS
            ),
        ]
        .copy()
    )

    selected_before_key_validation = (
        len(
            candidates
        )
    )

    if candidates.empty:

        raise ValueError(
            "No holdings match configured "
            "included_asset_classes: "
            f"{included_asset_classes}"
        )

    for column in OUTPUT_COLUMNS:

        candidates[
            column
        ] = (
            candidates[
                column
            ]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    ticker_valid = (
        candidates[
            "Ticker"
        ]
        .str.contains(
            r"[A-Za-z0-9]",
            regex=True,
            na=False,
        )
    )

    exchange_valid = (
        candidates[
            "Exchange"
        ]
        .ne("")
    )

    rejected_ticker_count = int(
        (
            ~ticker_valid
        ).sum()
    )

    rejected_exchange_count = int(
        (
            ticker_valid
            & ~exchange_valid
        ).sum()
    )

    if rejected_ticker_count:

        logger.warning(
            "Security Candidates rejected "
            "because ticker contains no "
            "alphanumeric character: %s",
            rejected_ticker_count,
        )

    if rejected_exchange_count:

        logger.warning(
            "Security Candidates rejected "
            "because Exchange is missing: %s",
            rejected_exchange_count,
        )

    candidates = (
        candidates.loc[
            ticker_valid
            & exchange_valid
        ]
        .copy()
    )

    if candidates.empty:

        raise ValueError(
            "No candidates remain after "
            "mandatory-key validation."
        )

    before_deduplication = len(
        candidates
    )

    candidates = (
        candidates
        .drop_duplicates(
            subset=(
                deduplication_columns
            ),
            keep="first",
        )
        .reset_index(
            drop=True
        )
    )

    duplicates_removed = (
        before_deduplication
        - len(
            candidates
        )
    )

    if candidates.duplicated(
        subset=(
            deduplication_columns
        ),
        keep=False,
    ).any():

        raise RuntimeError(
            "Security Candidate deduplication "
            "did not produce unique "
            "business keys."
        )

    candidates = candidates[
        list(
            OUTPUT_COLUMNS
        )
    ].copy()

    metadata = {
        "holdings_records":
            len(
                holdings
            ),
        "asset_class_selected_records":
            selected_before_key_validation,
        "rejected_invalid_ticker_records":
            rejected_ticker_count,
        "rejected_missing_exchange_records":
            rejected_exchange_count,
        "duplicates_removed":
            duplicates_removed,
        "output_records":
            len(
                candidates
            ),
    }

    return (
        candidates,
        metadata,
    )


# ============================================================================
# ATOMIC CSV PUBLICATION
# ============================================================================

def create_staging_path(
    output_path: Path,
) -> Path:
    """
    Create a staging CSV beside the final output.
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
        suffix=(
            output_path.suffix
        ),
        dir=(
            output_path.parent
        ),
        delete=False,
    ) as temporary_file:

        temporary_path = Path(
            temporary_file.name
        )

    return temporary_path


def write_csv_atomically(
    dataframe: pd.DataFrame,
    output_path: Path,
) -> None:
    """
    Stage, verify and atomically publish Security Candidates.
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
        output_path,
        Path,
    ):

        raise TypeError(
            "output_path must be "
            "a pathlib.Path."
        )

    if (
        output_path.suffix.casefold()
        != ".csv"
    ):

        raise ValueError(
            "Security Candidates output "
            "must be a CSV file."
        )

    staging_path = create_staging_path(
        output_path
    )

    try:

        dataframe.to_csv(
            staging_path,
            index=False,
            encoding="utf-8",
        )

        if not staging_path.exists():

            raise RuntimeError(
                "Security Candidates staging "
                "file was not created."
            )

        if not staging_path.is_file():

            raise RuntimeError(
                "Security Candidates staging "
                "path is not a file."
            )

        verification = pd.read_csv(
            staging_path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )

        expected_columns = list(
            OUTPUT_COLUMNS
        )

        actual_columns = list(
            verification.columns
        )

        if (
            actual_columns
            != expected_columns
        ):

            raise RuntimeError(
                "Generated Security Candidates "
                "CSV columns do not match "
                "the output contract. "
                f"Expected={expected_columns}; "
                f"Actual={actual_columns}"
            )

        if (
            len(
                verification
            )
            != len(
                dataframe
            )
        ):

            raise RuntimeError(
                "Generated Security Candidates "
                "CSV row count mismatch. "
                f"Expected={len(dataframe)}; "
                f"Actual={len(verification)}"
            )

        os.replace(
            staging_path,
            output_path,
        )

        staging_path = None

    except PermissionError as error:

        raise PermissionError(
            "Security Candidates output is "
            "open or locked. Close the file "
            "and rerun the preparation."
        ) from error

    finally:

        if (
            staging_path is not None
            and staging_path.exists()
        ):

            try:

                staging_path.unlink()

            except OSError as cleanup_error:

                logger.warning(
                    "Unable to remove temporary "
                    "Security Candidates file %s: %s",
                    staging_path,
                    cleanup_error,
                )


# ============================================================================
# PREPARATION EXECUTION
# ============================================================================

def build_security_candidates(
    dataset_id: Any | None = None,
) -> Path:
    """
    Execute the Security Candidates preparation.
    """

    registry = build_resolved_registry()

    configuration = (
        select_preparation_configuration(
            dataset_id,
            registry=registry,
        )
    )

    dataset_identifier = required_text(
        configuration.get(
            DATASET_KEY
        ),
        DATASET_KEY,
    )

    parameters = get_parameters(
        configuration
    )

    validate_preparation_parameters(
        parameters
    )

    (
        input_path,
        output_path,
    ) = resolve_preparation_paths(
        configuration,
        registry=registry,
    )

    worksheet_name = required_text(
        parameters[
            WORKSHEET_PARAMETER
        ],
        WORKSHEET_PARAMETER,
    )

    asset_class_column = required_text(
        parameters[
            ASSET_CLASS_COLUMN_PARAMETER
        ],
        ASSET_CLASS_COLUMN_PARAMETER,
    )

    included_asset_classes = (
        normalize_text_list(
            parameters[
                INCLUDED_ASSET_CLASSES_PARAMETER
            ],
            INCLUDED_ASSET_CLASSES_PARAMETER,
        )
    )

    deduplication_columns = (
        normalize_text_list(
            parameters[
                DEDUPLICATION_COLUMNS_PARAMETER
            ],
            DEDUPLICATION_COLUMNS_PARAMETER,
        )
    )

    logger.info(
        "Security Candidates preparation "
        "started: Dataset=%s",
        dataset_identifier,
    )

    logger.info(
        "Preparation input resolved: %s",
        input_path,
    )

    logger.info(
        "Preparation output resolved: %s",
        output_path,
    )

    try:

        workbook_text = read_text_file(
            input_path
        )

        worksheet_content = (
            extract_worksheet_content(
                workbook_text,
                worksheet_name,
            )
        )

        rows = extract_worksheet_rows(
            worksheet_content
        )

        header_index = find_header_row(
            rows
        )

        holdings = reconstruct_holdings(
            rows,
            header_index,
        )

        (
            candidates,
            transformation_metadata,
        ) = build_candidates_dataframe(
            holdings,
            asset_class_column=(
                asset_class_column
            ),
            included_asset_classes=(
                included_asset_classes
            ),
            deduplication_columns=(
                deduplication_columns
            ),
        )

        write_csv_atomically(
            candidates,
            output_path,
        )

        notes = (
            f"Input={input_path.name}; "
            f"Worksheet={worksheet_name}; "
            f"HoldingsRows="
            f"{transformation_metadata['holdings_records']}; "
            f"SelectedRows="
            f"{transformation_metadata['asset_class_selected_records']}; "
            f"InvalidTickerRejected="
            f"{transformation_metadata['rejected_invalid_ticker_records']}; "
            f"MissingExchangeRejected="
            f"{transformation_metadata['rejected_missing_exchange_records']}; "
            f"DuplicatesRemoved="
            f"{transformation_metadata['duplicates_removed']}; "
            f"OutputRows="
            f"{transformation_metadata['output_records']}"
        )

        run_id = log_preparation(
            output_file=output_path,
            storage_location=(
                output_path.parent
            ),
            status=SUCCESS_STATUS,
            records_produced=(
                len(
                    candidates
                )
            ),
            notes=notes,
        )

        logger.info(
            "Preparation Run ID: %s",
            run_id,
        )

        logger.info(
            "Security Candidates preparation "
            "completed successfully: "
            "Dataset=%s; Records=%s",
            dataset_identifier,
            len(
                candidates
            ),
        )

        return output_path

    except Exception as error:

        logger.exception(
            "Security Candidates preparation "
            "failed: Dataset=%s; Error=%s",
            dataset_identifier,
            error,
        )

        try:

            log_preparation(
                output_file=output_path,
                storage_location=(
                    output_path.parent
                ),
                status=FAILED_STATUS,
                records_produced=None,
                notes=(
                    f"Dataset={dataset_identifier}; "
                    f"Input={input_path.name}; "
                    f"ErrorType="
                    f"{type(error).__name__}; "
                    f"Error={error}"
                ),
            )

        except Exception as logging_error:

            logger.exception(
                "Unable to persist failed "
                "Security Candidates preparation: %s",
                logging_error,
            )

        raise


# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Execute Security Candidates preparation.
    """

    arguments = parse_arguments()

    output_path = build_security_candidates(
        dataset_id=(
            arguments.dataset_id
        )
    )

    logger.info(
        "Security Candidates Dataset "
        "published: %s",
        output_path,
    )


if __name__ == "__main__":
    main()