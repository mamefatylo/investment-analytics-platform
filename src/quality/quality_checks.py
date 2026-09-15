"""
quality_checks.py

Phase 4 - Data Quality Framework

Objectif
---------
Exécuter des contrôles qualité génériques sur
tous les datasets actifs marqués Quality Enabled.

Contrôles
----------
- Table Availability
- Dataset Not Empty
- Completeness
- Duplicate Rows
- Blank Values
- Column Integrity
- Chronology Readiness
"""

from datetime import datetime
import sqlite3

import pandas as pd

from src.config import DATABASE_FILE
from src.utils.logger import logger
from src.quality.quality_config import (
    OPTIONAL_COLUMNS,
)


# ============================================================================
# CONTROL DEFINITIONS
# ============================================================================

CONTROL_IDS = {
    "Table Availability": "QC-001",
    "Dataset Not Empty": "QC-002",
    "Completeness": "QC-003",
    "Duplicate Rows": "QC-004",
    "Blank Values": "QC-005",
    "Column Integrity": "QC-006",
    "Chronology Readiness": "QC-007",
}


# ============================================================================
# DATABASE
# ============================================================================

def create_connection():

    return sqlite3.connect(
        DATABASE_FILE
    )


def load_quality_enabled_datasets(
    connection,
) -> pd.DataFrame:

    return pd.read_sql(
        """
        SELECT
            dataset_id,
            source_id,
            table_name,
            chronology_enabled
        FROM dataset_registry
        WHERE active = 1
        AND quality_enabled = 1
        ORDER BY dataset_id
        """,
        connection,
    )


def load_dataset(
    connection,
    table_name: str,
) -> pd.DataFrame:

    return pd.read_sql(
        f'SELECT * FROM "{table_name}"',
        connection,
    )


# ============================================================================
# RESULT FACTORY
# ============================================================================

def build_result(
    dataset_id: str,
    source_id: str,
    table_name: str,
    control_name: str,
    status: str,
    issues: int,
    records_checked: int,
    notes: str,
) -> dict:

    return {
        "dataset_id":
            dataset_id,

        "source_id":
            source_id,

        "table_name":
            table_name,

        "control_id":
            CONTROL_IDS[
                control_name
            ],

        "control_name":
            control_name,

        "status":
            status,

        "issues":
            int(
                issues
            ),

        "records_checked":
            int(
                records_checked
            ),

        "execution_date":
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "notes":
            notes,
    }


# ============================================================================
# CONTROLS
# ============================================================================

def check_table_availability(
    connection,
    dataset,
) -> dict:

    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table'
        AND name=?
        """,
        (
            dataset.table_name,
        ),
    ).fetchone()

    exists = row is not None

    return build_result(
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        table_name=dataset.table_name,
        control_name="Table Availability",
        status=(
            "PASS"
            if exists
            else "FAIL"
        ),
        issues=0 if exists else 1,
        records_checked=0,
        notes="Physical table existence",
    )


def check_dataset_not_empty(
    dataset,
    dataframe,
) -> dict:

    records = len(
        dataframe
    )

    return build_result(
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        table_name=dataset.table_name,
        control_name="Dataset Not Empty",
        status=(
            "PASS"
            if records > 0
            else "FAIL"
        ),
        issues=0 if records > 0 else 1,
        records_checked=records,
        notes="Dataset contains records",
    )


def check_completeness(
    dataset,
    dataframe,
) -> dict:

    columns_to_check = [
        column
        for column in dataframe.columns
        if column not in OPTIONAL_COLUMNS
    ]

    if not columns_to_check:

        return build_result(
            dataset_id=dataset.dataset_id,
            source_id=dataset.source_id,
            table_name=dataset.table_name,
            control_name="Completeness",
            status="PASS",
            issues=0,
            records_checked=len(dataframe),
            notes="No mandatory columns configured",
        )

    missing = (
        dataframe[columns_to_check]
        .isna()
        .sum()
        .sum()
    )

    return build_result(
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        table_name=dataset.table_name,
        control_name="Completeness",
        status=(
            "PASS"
            if missing == 0
            else "WARNING"
        ),
        issues=int(missing),
        records_checked=len(dataframe),
        notes="Mandatory field completeness",
    )


def check_duplicate_rows(
    dataset,
    dataframe,
) -> dict:

    duplicates = (
        dataframe
        .duplicated()
        .sum()
    )

    status = (
                "PASS"
        if duplicates == 0
        else "WARNING"
    )

    return build_result(
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        table_name=dataset.table_name,
        control_name="Duplicate Rows",
        status=status,
        issues=duplicates,
        records_checked=len(
            dataframe
        ),
        notes="Duplicate row detection",
    )


def check_blank_values(
    dataset,
    dataframe,
) -> dict:

    blank_count = 0

    for column in dataframe.columns:

        blank_count += (
            dataframe[column]
            .astype(str)
            .str.strip()
            .eq("")
            .sum()
        )

    status = (
        "PASS"
        if blank_count == 0
        else "WARNING"
    )

    return build_result(
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        table_name=dataset.table_name,
        control_name="Blank Values",
        status=status,
        issues=blank_count,
        records_checked=len(
            dataframe
        ),
        notes="Blank value detection",
    )


def check_column_integrity(
    dataset,
    dataframe,
) -> dict:

    duplicate_columns = (
        dataframe.columns.duplicated().sum()
    )

    unnamed_columns = len(
        [
            column
            for column in dataframe.columns
            if str(column).startswith(
                "Unnamed"
            )
        ]
    )

    issues = (
        duplicate_columns
        + unnamed_columns
    )

    status = (
        "PASS"
        if issues == 0
        else "FAIL"
    )

    return build_result(
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        table_name=dataset.table_name,
        control_name="Column Integrity",
        status=status,
        issues=issues,
        records_checked=len(
            dataframe
        ),
        notes="Column structure validation",
    )


def check_chronology_readiness(
    dataset,
    dataframe,
) -> dict:

    chronology_enabled = bool(
        dataset.chronology_enabled
    )

    if not chronology_enabled:

        return build_result(
            dataset_id=dataset.dataset_id,
            source_id=dataset.source_id,
            table_name=dataset.table_name,
            control_name="Chronology Readiness",
            status="NOT_APPLICABLE",
            issues=0,
            records_checked=len(
                dataframe
            ),
            notes="Chronology disabled",
        )

    date_columns = [
        column
        for column in dataframe.columns
        if "date"
        in column.lower()
    ]

    return build_result(
        dataset_id=dataset.dataset_id,
        source_id=dataset.source_id,
        table_name=dataset.table_name,
        control_name="Chronology Readiness",
        status=(
            "PASS"
            if date_columns
            else "FAIL"
        ),
        issues=0 if date_columns else 1,
        records_checked=len(
            dataframe
        ),
        notes="Date column detection",
    )


# ============================================================================
# ORCHESTRATOR
# ============================================================================

def run_quality_checks() -> pd.DataFrame:

    logger.info(
        "Quality controls started"
    )

    results = []

    connection = create_connection()

    try:

        datasets = (
            load_quality_enabled_datasets(
                connection
            )
        )

        for dataset in (
            datasets.itertuples()
        ):

            results.append(
                check_table_availability(
                    connection,
                    dataset,
                )
            )

            dataframe = load_dataset(
                connection,
                dataset.table_name,
            )

            results.extend(
                [
                    check_dataset_not_empty(
                        dataset,
                        dataframe,
                    ),

                    check_completeness(
                        dataset,
                        dataframe,
                    ),

                    check_duplicate_rows(
                        dataset,
                        dataframe,
                    ),

                    check_blank_values(
                        dataset,
                        dataframe,
                    ),

                    check_column_integrity(
                        dataset,
                        dataframe,
                    ),

                    check_chronology_readiness(
                        dataset,
                        dataframe,
                    ),
                ]
            )

    finally:

        connection.close()

    controls = pd.DataFrame(
        results
    )

    logger.info(
        "Controls executed: %s",
        len(
            controls
        ),
    )

    logger.info(
        "Quality controls completed"
    )

    return controls


# ============================================================================
# MAIN
# ============================================================================

def main():

    controls = (
        run_quality_checks()
    )

    pd.set_option(
        "display.max_columns",
        None,
    )

    pd.set_option(
        "display.width",
        None,
    )

    print(
        controls
    )


if __name__ == "__main__":

    main()
 