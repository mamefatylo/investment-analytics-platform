"""
quality_checks.py

Phase 4 - Data Quality Framework

Objectif
---------
Exécuter les contrôles qualité sur les données
stockées dans la base SQLite du projet.

Table actuellement contrôlée :
    - securities_master

Contrôles :
    - Missing Values
    - Duplicates
    - Invalid Formats
    - Outliers
    - Currency Consistency
    - Data Chronology
"""

import sqlite3

import pandas as pd

from src.config import (
    DATABASE_FILE,
)

from src.utils.logger import logger

# ============================================================================
# DATABASE
# ============================================================================


def load_securities_master() -> pd.DataFrame:
    """
    Charge la table securities_master.
    """

    connection = sqlite3.connect(
        DATABASE_FILE
    )

    try:

        dataframe = pd.read_sql(
            """
            SELECT *
            FROM securities_master
            """,
            connection,
        )

        return dataframe

    finally:

        connection.close()


# ============================================================================
# CONTROL RESULT
# ============================================================================


def build_result(
    control_name: str,
    status: str,
    issues: int,
    records_checked: int,
    notes: str,
) -> dict:
    """
    Construit un résultat normalisé.
    """

    return {
        "control_name": control_name,
        "status": status,
        "issues": issues,
        "records_checked": records_checked,
        "notes": notes,
    }


# ============================================================================
# MISSING VALUES
# ============================================================================


def check_missing_values(
    dataframe: pd.DataFrame,
) -> dict:

    required_fields = [
        "ticker",
        "exchange_code",
        "status",
    ]

    missing = dataframe[
        required_fields
    ].isna().sum().sum()

    status = (
        "PASS"
        if missing == 0
        else "FAIL"
    )

    return build_result(
        control_name="Missing Values",
        status=status,
        issues=int(missing),
        records_checked=len(dataframe),
        notes=(
            "Mandatory fields validation"
        ),
    )


# ============================================================================
# DUPLICATES
# ============================================================================


def check_duplicates(
    dataframe: pd.DataFrame,
) -> dict:

    duplicates = dataframe.duplicated().sum()

    status = (
        "PASS"
        if duplicates == 0
        else "FAIL"
    )

    return build_result(
        control_name="Duplicates",
        status=status,
        issues=int(duplicates),
        records_checked=len(dataframe),
        notes="Duplicate rows",
    )


# ============================================================================
# INVALID STATUS
# ============================================================================


def check_invalid_formats(
    dataframe: pd.DataFrame,
) -> dict:

    valid_status = [
        "MATCH",
        "NO_MATCH",
        "MISSING_MAPPING",
        "ERROR",
    ]

    invalid = len(
        dataframe[
            ~dataframe[
                "status"
            ].isin(valid_status)
        ]
    )

    status = (
        "PASS"
        if invalid == 0
        else "FAIL"
    )

    return build_result(
        control_name="Invalid Formats",
        status=status,
        issues=int(invalid),
        records_checked=len(dataframe),
        notes="Status validation",
    )


# ============================================================================
# OUTLIERS
# ============================================================================


def check_outliers(
    dataframe: pd.DataFrame,
) -> dict:

    outliers = len(
        dataframe[
            (dataframe["status"] == "MATCH")
            &
            (
                dataframe["match_count"]
                <= 0
            )
        ]
    )

    status = (
        "PASS"
        if outliers == 0
        else "FAIL"
    )

    return build_result(
        control_name="Outliers",
        status=status,
        issues=int(outliers),
        records_checked=len(dataframe),
        notes=(
            "MATCH records must have "
            "match_count > 0"
        ),
    )


# ============================================================================
# CURRENCY CONSISTENCY
# ============================================================================


def check_currency_consistency(
    dataframe: pd.DataFrame,
) -> dict:

    invalid = len(
        dataframe[
            dataframe["currency"]
            .isna()
        ]
    )

    status = (
        "PASS"
        if invalid == 0
        else "FAIL"
    )

    return build_result(
        control_name="Currency Consistency",
        status=status,
        issues=int(invalid),
        records_checked=len(dataframe),
        notes="Currency populated",
    )


# ============================================================================
# DATA CHRONOLOGY
# ============================================================================


def check_data_chronology(
    dataframe: pd.DataFrame,
) -> dict:

    return build_result(
        control_name="Data Chronology",
        status="NOT_APPLICABLE",
        issues=0,
        records_checked=len(dataframe),
        notes=(
            "No time series available "
            "at current phase"
        ),
    )


# ============================================================================
# ORCHESTRATOR
# ============================================================================


def run_quality_checks() -> pd.DataFrame:
    """
    Exécute l'ensemble des contrôles.
    """

    logger.info(
        "Data quality checks started"
    )

    dataframe = load_securities_master()

    results = [
        check_missing_values(
            dataframe
        ),
        check_duplicates(
            dataframe
        ),
        check_invalid_formats(
            dataframe
        ),
        check_outliers(
            dataframe
        ),
        check_currency_consistency(
            dataframe
        ),
        check_data_chronology(
            dataframe
        ),
    ]

    result_dataframe = pd.DataFrame(
        results
    )

    logger.info(
        "Data quality checks completed"
    )

    return result_dataframe


# ============================================================================
# MAIN
# ============================================================================


def main():

    results = run_quality_checks()

    print(results)


if __name__ == "__main__":

    main()