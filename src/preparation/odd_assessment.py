"""
Build the ODD assessment register for SRC-007.

SRC-007 has no single provider endpoint. The register is a
Preparation of the Security Master: one row per listed
instrument, with the evidence columns left blank for manual
completion. Eligibility is not decided here.
"""

import argparse
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd

from src.acquisition.acquisition_logger import log_preparation
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


SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"

MASTER_COLUMNS = frozenset(
    {
        "Ticker",
        "Name",
        "Exchange",
        "figi",
    }
)

IDENTITY_COLUMNS = (
    "Ticker",
    "Exchange",
)

OUTPUT_COLUMNS = (
    "Ticker",
    "Exchange",
    "figi",
    "ISIN",
    "Company",
    "SDG",
    "Activity",
    "Positive Contribution",
    "Negative Contribution",
    "Evidence",
    "Source",
    "Controversy",
    "Data Quality",
    "Status",
)

EVIDENCE_COLUMNS = (
    "SDG",
    "Activity",
    "Positive Contribution",
    "Negative Contribution",
    "Evidence",
    "Source",
    "Controversy",
    "Data Quality",
    "Status",
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Build the ODD assessment register configured "
            "with Production Method=Preparation."
        )
    )
    parser.add_argument("--dataset-id", default=None)
    return parser.parse_args()


def optional_text(value: Any) -> str | None:
    if value is None:
        return None

    if not isinstance(value, (dict, list, tuple, set)):
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass

    text = str(value).strip()
    return text or None


def required_text(value: Any, field_name: str) -> str:
    text = optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def validate_preparation_configuration(
    configuration: dict[str, Any],
) -> None:
    if not isinstance(configuration, dict):
        raise TypeError("configuration must be a dictionary.")

    dataset_id = required_text(
        configuration.get(DATASET_KEY),
        DATASET_KEY,
    )
    method = required_text(
        configuration.get(PRODUCTION_METHOD_KEY),
        PRODUCTION_METHOD_KEY,
    )

    if method.casefold() != PREPARATION_METHOD.casefold():
        raise ValueError(
            f"{dataset_id} is not configured for Preparation."
        )

    if not isinstance(configuration.get(ACTIVE_KEY), bool):
        raise TypeError(f"{dataset_id}: Active must be boolean.")

    if not configuration[ACTIVE_KEY]:
        raise ValueError(f"{dataset_id} is inactive.")

    parameters = get_parameters(configuration)
    if not isinstance(parameters, dict):
        raise TypeError(
            "ODD assessment Parameters must be a dictionary."
        )

    if parameters:
        raise ValueError(
            "ODD assessment expects empty Parameters. "
            "Evidence is completed in the register, "
            "not supplied by the registry."
        )


def select_preparation_configuration(
    dataset_id: str | None = None,
    *,
    registry: pd.DataFrame | None = None,
) -> dict[str, Any]:
    resolved = (
        build_resolved_registry()
        if registry is None
        else registry.copy(deep=True)
    )
    requested = optional_text(dataset_id)

    if requested is not None:
        configuration = get_dataset_by_id(
            requested,
            registry=resolved,
        ).to_dict()
        validate_preparation_configuration(configuration)
        return configuration

    candidates = get_preparation_enabled_datasets(
        registry=resolved
    )
    if len(candidates) != 1:
        raise ValueError(
            "Provide --dataset-id when active Preparation "
            "Dataset count is not exactly one."
        )

    configuration = candidates.iloc[0].to_dict()
    validate_preparation_configuration(configuration)
    return configuration


def read_tabular_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)

    extension = path.suffix.casefold()
    if extension == ".csv":
        return pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )

    raise ValueError(
        f"Unsupported ODD assessment input extension: {extension}"
    )


def resolve_input_dataset(
    configuration: dict[str, Any],
    *,
    registry: pd.DataFrame,
) -> tuple[pd.DataFrame, Path, Path]:
    dataset_id = required_text(
        configuration.get(DATASET_KEY),
        DATASET_KEY,
    )
    inputs = get_input_dataset_configurations(
        dataset_id,
        registry=registry,
    )

    if len(inputs) != 1:
        raise ValueError(
            f"{dataset_id}: ODD assessment requires exactly "
            "one upstream Dataset. "
            f"Resolved inputs: {len(inputs)}"
        )

    input_path = build_dataset_absolute_path(inputs[0])
    master = read_tabular_dataset(input_path)
    output_path = build_dataset_absolute_path(configuration)

    if output_path == input_path:
        raise ValueError(
            "ODD assessment output path cannot equal "
            "its input path."
        )

    return master, input_path, output_path


def normalize_master(master: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(MASTER_COLUMNS - set(master.columns))
    if missing:
        raise ValueError(
            "Security Master is missing columns: "
            f"{missing}"
        )

    frame = master.copy(deep=True)
    for column in MASTER_COLUMNS:
        frame[column] = (
            frame[column].fillna("").astype(str).str.strip()
        )

    if frame.empty:
        raise ValueError("Security Master Dataset is empty.")

    blank_identity = (
        frame["Ticker"].eq("") | frame["Exchange"].eq("")
    )
    if blank_identity.any():
        raise ValueError(
            "Security Master contains a blank Ticker or Exchange."
        )

    blank_company = frame["Name"].eq("")
    if blank_company.any():
        raise ValueError(
            "Security Master contains a blank company name."
        )

    duplicate = frame.duplicated(
        subset=list(IDENTITY_COLUMNS),
        keep=False,
    )
    if duplicate.any():
        raise ValueError(
            "Security Master identity is not unique: "
            f"{frame.loc[duplicate, list(IDENTITY_COLUMNS)].to_dict(orient='records')}"
        )

    return frame.reset_index(drop=True)


def empty_assessment_row(security: pd.Series) -> dict[str, str]:
    row = {column: "" for column in OUTPUT_COLUMNS}
    row["Ticker"] = security["Ticker"]
    row["Exchange"] = security["Exchange"]
    row["figi"] = security["figi"]
    row["Company"] = security["Name"]
    return row


def identity_key(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["Ticker"].astype(str).str.strip()
        + "\x1f"
        + frame["Exchange"].astype(str).str.strip()
    )


def read_existing_register(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=list(OUTPUT_COLUMNS))

    register = read_tabular_dataset(path)
    if list(register.columns) != list(OUTPUT_COLUMNS):
        raise ValueError(
            "Existing ODD assessment columns do not match "
            f"the register contract: {list(register.columns)}"
        )

    for column in OUTPUT_COLUMNS:
        register[column] = (
            register[column].fillna("").astype(str).str.strip()
        )

    return register


def build_odd_assessment_dataframe(
    master: pd.DataFrame,
    existing: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    master = normalize_master(master)
    skeleton = pd.DataFrame(
        [
            empty_assessment_row(security)
            for _, security in master.iterrows()
        ],
        columns=list(OUTPUT_COLUMNS),
    )

    if existing.empty:
        register = skeleton
        preserved_rows = 0
        added_rows = len(skeleton)
    else:
        known = set(identity_key(existing))
        additions = skeleton.loc[
            ~identity_key(skeleton).isin(known)
        ].copy()
        register = pd.concat(
            [existing, additions],
            ignore_index=True,
        )
        preserved_rows = len(existing)
        added_rows = len(additions)

    register = (
        register
        .sort_values(
            ["Ticker", "Exchange", "SDG"],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )

    covered = set(identity_key(register))
    master_keys = set(identity_key(master))
    missing_identities = master_keys - covered
    if missing_identities:
        raise RuntimeError(
            "ODD assessment is missing Security Master identities."
        )

    evidence = register[list(EVIDENCE_COLUMNS)]
    filled_rows = int(evidence.ne("").any(axis=1).sum())

    metadata = {
        "master_records": len(master),
        "preserved_rows": preserved_rows,
        "added_rows": added_rows,
        "output_records": len(register),
        "filled_evidence_rows": filled_rows,
        "blank_evidence_rows": len(register) - filled_rows,
        "orphan_rows": int((~identity_key(register).isin(master_keys)).sum()),
    }
    return register, metadata


def write_csv_atomically(
    frame: pd.DataFrame,
    output_path: Path,
) -> None:
    if output_path.suffix.casefold() != ".csv":
        raise ValueError("ODD assessment output must be CSV.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None

    try:
        with NamedTemporaryFile(
            mode="wb",
            prefix=f".{output_path.stem}_",
            suffix=output_path.suffix,
            dir=output_path.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)

        frame.to_csv(
            temporary,
            index=False,
            encoding="utf-8",
        )
        verified = pd.read_csv(
            temporary,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )

        if list(verified.columns) != list(OUTPUT_COLUMNS):
            raise RuntimeError(
                "Generated ODD assessment failed column verification."
            )

        if len(verified) != len(frame):
            raise RuntimeError(
                "Generated ODD assessment failed row verification."
            )

        os.replace(temporary, output_path)
        temporary = None
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)


def build_odd_assessment(
    dataset_id: str | None = None,
) -> Path:
    registry = build_resolved_registry()
    configuration = select_preparation_configuration(
        dataset_id,
        registry=registry,
    )
    dataset_id = required_text(
        configuration.get(DATASET_KEY),
        DATASET_KEY,
    )
    master, input_path, output_path = resolve_input_dataset(
        configuration,
        registry=registry,
    )

    logger.info(
        "ODD assessment preparation started: Dataset=%s",
        dataset_id,
    )

    try:
        existing = read_existing_register(output_path)
        register, metadata = build_odd_assessment_dataframe(
            master,
            existing,
        )
        write_csv_atomically(register, output_path)
        notes = json.dumps(
            {
                "dataset_id": dataset_id,
                "input": input_path.name,
                **metadata,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        run_id = log_preparation(
            output_file=output_path,
            storage_location=output_path.parent,
            status=SUCCESS_STATUS,
            records_produced=len(register),
            notes=notes,
        )
        logger.info(
            "ODD assessment completed: Records=%s; Added=%s; "
            "Filled=%s; Blank=%s",
            metadata["output_records"],
            metadata["added_rows"],
            metadata["filled_evidence_rows"],
            metadata["blank_evidence_rows"],
        )
        logger.info("Preparation Run ID: %s", run_id)
        return output_path
    except Exception as error:
        logger.exception(
            "ODD assessment preparation failed: Dataset=%s; Error=%s",
            dataset_id,
            error,
        )
        try:
            log_preparation(
                output_file=output_path,
                storage_location=output_path.parent,
                status=FAILED_STATUS,
                records_produced=None,
                notes=json.dumps(
                    {
                        "dataset_id": dataset_id,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        except Exception as logging_error:
            logger.exception(
                "Unable to persist failed ODD assessment "
                "preparation: %s",
                logging_error,
            )
        raise


def main() -> None:
    arguments = parse_arguments()
    output = build_odd_assessment(
        dataset_id=arguments.dataset_id
    )
    logger.info("ODD assessment Dataset published: %s", output)


if __name__ == "__main__":
    main()
