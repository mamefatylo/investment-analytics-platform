"""Build the security master from the candidate list.

The Yahoo symbol and the listing currency come from the exchange table.
The preparation does not call an external identifier service.
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
from src.preparation.exchange_mapping import (
    build_yahoo_symbol,
    get_listing_currency,
)
from src.utils.logger import logger

SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"

CANDIDATE_REQUIRED_COLUMNS = frozenset({
    "Ticker",
    "Name",
    "Location",
    "Exchange",
    "Currency",
    "Asset Class",
})
OUTPUT_COLUMNS = (
    "Ticker",
    "Name",
    "Location",
    "Exchange",
    "Currency",
    "listing_currency",
    "Asset Class",
    "yahoo_symbol",
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Build a Security Master dataset configured as Preparation.",
    )
    parser.add_argument("--dataset-id", default=None)
    return parser.parse_args()


def optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return None
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


def validate_preparation_configuration(configuration: dict[str, Any]) -> None:
    if not isinstance(configuration, dict):
        raise TypeError("configuration must be a dictionary.")
    dataset_id = required_text(configuration.get(DATASET_KEY), DATASET_KEY)
    method = required_text(configuration.get(PRODUCTION_METHOD_KEY), PRODUCTION_METHOD_KEY)
    if method.casefold() != PREPARATION_METHOD.casefold():
        raise ValueError(f"{dataset_id} is not configured for Preparation.")
    if not isinstance(configuration.get(ACTIVE_KEY), bool):
        raise TypeError(f"{dataset_id}: Active must be boolean.")
    if not configuration[ACTIVE_KEY]:
        raise ValueError(f"{dataset_id} is inactive.")
    parameters = get_parameters(configuration)
    if not isinstance(parameters, dict):
        raise TypeError("Security Master Parameters must be a dictionary.")
    if parameters:
        raise ValueError("Security Master currently expects empty Parameters.")


def select_preparation_configuration(dataset_id=None, *, registry=None) -> dict[str, Any]:
    resolved = build_resolved_registry() if registry is None else registry.copy(deep=True)
    requested = optional_text(dataset_id)
    if requested is not None:
        configuration = get_dataset_by_id(requested, registry=resolved).to_dict()
        validate_preparation_configuration(configuration)
        return configuration
    candidates = get_preparation_enabled_datasets(registry=resolved)
    master_rows = candidates.loc[
        candidates["Output File Name"].astype(str).str.casefold().eq("securities_master.csv")
    ]
    if len(master_rows) != 1:
        raise ValueError(
            "Provide --dataset-id when the Security Master preparation is not unique. "
            f"Found {len(master_rows)}."
        )
    configuration = master_rows.iloc[0].to_dict()
    validate_preparation_configuration(configuration)
    return configuration


def read_tabular_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    extension = path.suffix.casefold()
    if extension == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")
    if extension == ".json":
        return pd.read_json(path)
    if extension == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported Security Master input extension: {extension}")


def resolve_input_dataset(configuration, *, registry):
    dataset_id = required_text(configuration.get(DATASET_KEY), DATASET_KEY)
    inputs = get_input_dataset_configurations(dataset_id, registry=registry)
    if len(inputs) != 1:
        raise ValueError(
            f"{dataset_id}: Security Master requires exactly one upstream dataset. "
            f"Resolved inputs: {len(inputs)}"
        )
    input_path = build_dataset_absolute_path(inputs[0])
    candidates = read_tabular_dataset(input_path)
    missing = sorted(CANDIDATE_REQUIRED_COLUMNS - set(candidates.columns))
    if missing:
        raise ValueError(f"Security Candidates is missing columns: {missing}")
    output_path = build_dataset_absolute_path(configuration)
    if output_path == input_path:
        raise ValueError("Security Master output path cannot equal its input path.")
    return candidates.copy(), input_path, output_path


def build_security_master_dataframe(candidates: pd.DataFrame):
    frame = candidates.copy(deep=True)
    for column in CANDIDATE_REQUIRED_COLUMNS:
        frame[column] = frame[column].fillna("").astype(str).str.strip()
    if frame.empty:
        raise ValueError("Security Candidates dataset is empty.")

    records = []
    for _, candidate in frame.iterrows():
        ticker = required_text(candidate["Ticker"], "Ticker")
        exchange = required_text(candidate["Exchange"], "Exchange")
        listing_currency = get_listing_currency(exchange) or ""
        yahoo_symbol = build_yahoo_symbol(ticker, exchange) or ""
        records.append({
            "Ticker": ticker,
            "Name": candidate["Name"],
            "Location": candidate["Location"],
            "Exchange": exchange,
            "Currency": candidate["Currency"],
            "listing_currency": listing_currency,
            "Asset Class": candidate["Asset Class"],
            "yahoo_symbol": yahoo_symbol,
        })
    master = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    if len(master) != len(frame):
        raise RuntimeError("Security Master row count does not match Security Candidates.")

    missing_symbol = master.loc[master["yahoo_symbol"].eq(""), ["Ticker", "Exchange"]]
    if not missing_symbol.empty:
        raise RuntimeError(
            "Security Master contains unresolved Yahoo symbols: "
            f"{missing_symbol.to_dict(orient='records')}"
        )
    missing_currency = master.loc[master["listing_currency"].eq(""), ["Ticker", "Exchange"]]
    if not missing_currency.empty:
        raise RuntimeError(
            "Security Master contains unresolved listing currencies: "
            f"{missing_currency.to_dict(orient='records')}"
        )
    duplicated = master["yahoo_symbol"].duplicated(keep=False)
    if duplicated.any():
        raise RuntimeError(
            "Security Master contains duplicate Yahoo symbols: "
            f"{master.loc[duplicated, ['Ticker', 'Exchange', 'yahoo_symbol']].to_dict(orient='records')}"
        )
    metadata = {
        "candidate_records": len(frame),
        "output_records": len(master),
        "yahoo_symbol_records": int(master["yahoo_symbol"].ne("").sum()),
    }
    return master, metadata


def write_csv_atomically(frame: pd.DataFrame, output_path: Path) -> None:
    if output_path.suffix.casefold() != ".csv":
        raise ValueError("Security Master output must be CSV.")
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
        frame.to_csv(temporary, index=False, encoding="utf-8")
        verified = pd.read_csv(temporary, dtype=str, keep_default_na=False, encoding="utf-8")
        if list(verified.columns) != list(OUTPUT_COLUMNS) or len(verified) != len(frame):
            raise RuntimeError("Generated Security Master failed staging verification.")
        os.replace(temporary, output_path)
        temporary = None
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)


def build_security_master(dataset_id=None) -> Path:
    registry = build_resolved_registry()
    configuration = select_preparation_configuration(dataset_id, registry=registry)
    dataset_id = required_text(configuration.get(DATASET_KEY), DATASET_KEY)
    candidates, input_path, output_path = resolve_input_dataset(configuration, registry=registry)
    logger.info("Security Master preparation started: Dataset=%s", dataset_id)
    try:
        master, metadata = build_security_master_dataframe(candidates)
        write_csv_atomically(master, output_path)
        notes = json.dumps(
            {"inputs": [input_path.name], **metadata},
            ensure_ascii=False,
            sort_keys=True,
        )
        run_id = log_preparation(
            output_file=output_path,
            storage_location=output_path.parent,
            status=SUCCESS_STATUS,
            records_produced=len(master),
            notes=notes,
        )
        logger.info(
            "Security Master completed: Records=%s; YahooSymbols=%s",
            metadata["output_records"],
            metadata["yahoo_symbol_records"],
        )
        logger.info("Preparation Run ID: %s", run_id)
        return output_path
    except Exception as error:
        logger.exception(
            "Security Master preparation failed: Dataset=%s; Error=%s",
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
                "Unable to persist failed Security Master preparation: %s",
                logging_error,
            )
        raise


def main():
    args = parse_arguments()
    output = build_security_master(dataset_id=args.dataset_id)
    logger.info("Security Master dataset published: %s", output)


if __name__ == "__main__":
    main()
