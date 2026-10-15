"""
Acquire the datasets that are due, then rebuild the published layer.

The registry column Frequency decides which raw files are downloaded
again. A successful download replaces the previous raw file. Datasets
that are not due keep the file already on disk.

Order
-----
1. Active datasets whose frequency is due, in dependency order.
2. Load every Load Enabled dataset into SQLite.
3. Fetch a quote currency only for a master symbol that is not stored yet.
4. Rebuild the CHF prices, the CHF universe and the treated SARON.
5. Rebuild the three portfolios on the latest SARON window.
6. Rebuild the quality control center from the loaded tables.
"""

import argparse
import sqlite3
from datetime import datetime, timezone

from src.acquisition.extract_api import extract_api
from src.acquisition.extract_download import extract_download
from src.acquisition.extract_package import extract_package
from src.config import DATABASE_FILE
from src.database.load_database import load_database
from src.governance.source_registry import (
    DATASET_KEY,
    FREQUENCY_KEY,
    INPUT_DATASET_KEY,
    OUTPUT_FILE_KEY,
    PRODUCTION_METHOD_KEY,
    build_dataset_absolute_path,
    get_active_datasets,
)
from src.portfolio_construction.build_portfolios import build_portfolios
from src.preparation.odd_assessment import build_odd_assessment
from src.preparation.quality_treatment import build_quality_treatment
from src.preparation.quote_currency import complete_quote_currencies
from src.preparation.securities_candidates import build_security_candidates
from src.preparation.securities_master import build_security_master
from src.quality.data_control_center import build_data_control_center
from src.utils.logger import logger


PREPARATION_BUILDERS = {
    "security_candidates.csv": build_security_candidates,
    "securities_master.csv": build_security_master,
    "odd_assessment.csv": build_odd_assessment,
}
ON_DEMAND = "on demand"


def utc_now():
    return datetime.now(timezone.utc)


def parse_moment(value):
    moment = datetime.fromisoformat(str(value))
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def period_marker(frequency, moment):
    """Calendar bucket for a registry frequency. The clock is UTC."""
    label = str(frequency).strip().casefold()
    if label == "daily":
        return ("day", moment.date().isoformat())
    if label in {"monthly", "periodic"}:
        return ("month", f"{moment.year:04d}-{moment.month:02d}")
    if label == "quarterly":
        quarter = (moment.month - 1) // 3 + 1
        return ("quarter", f"{moment.year:04d}-Q{quarter}")
    if label == "annual":
        return ("year", f"{moment.year:04d}")
    if label == ON_DEMAND:
        return None
    raise ValueError(f"Unknown registry frequency: {frequency}")


def last_success_by_file():
    """Latest successful publication of each output file name."""
    if not DATABASE_FILE.exists():
        return {}
    connection = sqlite3.connect(DATABASE_FILE)
    try:
        rows = connection.execute(
            """
            SELECT output_file, execution_date
            FROM acquisition_log
            WHERE status = 'Success'
            """
        ).fetchall()
    finally:
        connection.close()
    latest = {}
    for output_file, execution_date in rows:
        name = str(output_file).replace("\\", "/").rsplit("/", 1)[-1].casefold()
        moment = parse_moment(execution_date)
        current = latest.get(name)
        if current is None or moment > current:
            latest[name] = moment
    return latest


def input_ids(row):
    values = row[INPUT_DATASET_KEY]
    if not isinstance(values, list):
        raise TypeError(f"{row[DATASET_KEY]} input dataset ids must be a list")
    return [str(value).strip() for value in values]


def topological_datasets(datasets):
    """Active datasets ordered so each input is published before its reader."""
    by_id = {str(row[DATASET_KEY]): row for _, row in datasets.iterrows()}
    pending = {dataset_id: 0 for dataset_id in by_id}
    readers = {dataset_id: [] for dataset_id in by_id}
    for dataset_id, row in by_id.items():
        for parent in input_ids(row):
            if parent not in by_id:
                continue
            pending[dataset_id] += 1
            readers[parent].append(dataset_id)
    ready = sorted(dataset_id for dataset_id, count in pending.items() if count == 0)
    ordered = []
    while ready:
        current = ready.pop(0)
        ordered.append(by_id[current])
        for child in sorted(readers[current]):
            pending[child] -= 1
            if pending[child] == 0:
                ready.append(child)
        ready.sort()
    if len(ordered) != len(by_id):
        raise RuntimeError("Active dataset dependencies contain a cycle")
    return ordered


def dataset_is_due(row, last_success, now, refreshed_ids, force):
    dataset_id = str(row[DATASET_KEY])
    frequency = row[FREQUENCY_KEY]
    if force:
        return True
    if str(frequency).strip().casefold() == ON_DEMAND:
        output_name = str(row[OUTPUT_FILE_KEY]).strip().casefold()
        if output_name not in last_success:
            return True
        return any(parent in refreshed_ids for parent in input_ids(row))
    output_name = str(row[OUTPUT_FILE_KEY]).strip().casefold()
    last = last_success.get(output_name)
    if last is None:
        return True
    return period_marker(frequency, last) != period_marker(frequency, now)


def datasets_to_acquire(now=None, force=False):
    """Return the active datasets this run must publish again."""
    moment = utc_now() if now is None else now
    last_success = last_success_by_file()
    selected = []
    refreshed = set()
    for row in topological_datasets(get_active_datasets()):
        if dataset_is_due(row, last_success, moment, refreshed, force):
            selected.append(row)
            refreshed.add(str(row[DATASET_KEY]))
    return selected


def acquire_dataset(row):
    dataset_id = str(row[DATASET_KEY])
    method = str(row[PRODUCTION_METHOD_KEY]).strip().casefold()
    if method == "download":
        return extract_download(dataset_id=dataset_id, fail_fast=True)
    if method == "api":
        return extract_api(dataset_id=dataset_id, fail_fast=True)
    if method == "package":
        return extract_package(dataset_id=dataset_id, fail_fast=True)
    if method == "preparation":
        output_name = str(row[OUTPUT_FILE_KEY]).strip().casefold()
        builder = PREPARATION_BUILDERS.get(output_name)
        if builder is None:
            raise ValueError(f"No preparation builder for {output_name}")
        return builder(dataset_id=dataset_id)
    raise ValueError(f"Unknown production method: {method}")


def upstream_file_missing(row, failed_ids, datasets):
    by_id = {str(item[DATASET_KEY]): item for item in datasets}
    for parent in input_ids(row):
        if parent not in failed_ids:
            continue
        parent_row = by_id.get(parent)
        if parent_row is None:
            return True
        if not build_dataset_absolute_path(parent_row.to_dict()).exists():
            return True
    return False


def acquire_due_datasets(force=False):
    """Download or rebuild every due dataset. One failure does not cancel the others."""
    datasets = topological_datasets(get_active_datasets())
    due = datasets_to_acquire(force=force)
    due_ids = {str(row[DATASET_KEY]) for row in due}
    failed = {}
    published = []
    logger.info(
        "Acquisition plan: %s",
        ", ".join(sorted(due_ids)) if due_ids else "nothing due",
    )
    for row in datasets:
        dataset_id = str(row[DATASET_KEY])
        if dataset_id not in due_ids:
            continue
        if upstream_file_missing(row, failed, datasets):
            failed[dataset_id] = "upstream file missing"
            logger.error("Skipped %s because an upstream file is missing", dataset_id)
            continue
        try:
            acquire_dataset(row)
            published.append(dataset_id)
            logger.info("Acquired %s", dataset_id)
        except Exception as error:
            failed[dataset_id] = f"{type(error).__name__}: {error}"
            logger.exception("Acquisition failed for %s", dataset_id)
    return published, failed


def refresh(force=False):
    """Acquire due raw files, then rebuild every published dataset."""
    published, failed = acquire_due_datasets(force=force)
    database = load_database()
    logger.info("Database refreshed: %s", database)
    quote_file = complete_quote_currencies()
    logger.info("Quote currencies: %s", quote_file)
    treated = build_quality_treatment()
    logger.info("Treated datasets: %s", len(treated))
    portfolios = build_portfolios()
    logger.info("Portfolio files: %s", len(portfolios))
    report = build_data_control_center()
    logger.info("Quality report: %s", report)
    return {
        "acquired": published,
        "failures": failed,
        "report": report,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Acquire due datasets from the registry frequency, then rebuild the published layer.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Acquire every active dataset, ignoring frequency.",
    )
    args = parser.parse_args()
    result = refresh(force=args.force)
    print(result["report"])
    if result["failures"]:
        for dataset_id, message in sorted(result["failures"].items()):
            print(f"FAILED {dataset_id}: {message}")
        raise SystemExit(1)
    print("Acquired:", ", ".join(result["acquired"]) if result["acquired"] else "nothing due")


if __name__ == "__main__":
    main()
