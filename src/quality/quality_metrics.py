"""
quality_metrics.py

Phase 4 - Data Quality Framework

Objectif
---------
Calculer les indicateurs qualité génériques
à partir d'un dataset fourni en entrée.

KPIs
----
- Availability Rate
- Completeness Rate
- Uniqueness Rate
- Freshness Rate
- Chronology Rate
- Global Quality Score
- Global Quality Grade
- Global Status
"""

from datetime import datetime

import pandas as pd

from src.quality.quality_config import (
    GRADE_THRESHOLDS,
    PASS_THRESHOLD,
    QUALITY_WEIGHTS,
    WARNING_THRESHOLD,
    OPTIONAL_COLUMNS,
)

from src.utils.logger import logger


# ============================================================================
# KPI STATUS
# ============================================================================


def calculate_metric_status(
    score: float,
) -> str:

    if score >= PASS_THRESHOLD:
        return "PASS"

    if score >= WARNING_THRESHOLD:
        return "WARNING"

    return "FAIL"


# ============================================================================
# QUALITY GRADE
# ============================================================================


def calculate_quality_grade(
    score: float,
) -> str:

    if score >= GRADE_THRESHOLDS["A"]:
        return "A"

    if score >= GRADE_THRESHOLDS["B"]:
        return "B"

    if score >= GRADE_THRESHOLDS["C"]:
        return "C"

    return "D"


# ============================================================================
# AVAILABILITY
# ============================================================================


def calculate_availability_rate(
    dataframe: pd.DataFrame,
) -> float:

    return (
        100.0
        if not dataframe.empty
        else 0.0
    )


# ============================================================================
# COMPLETENESS
# ============================================================================


def calculate_completeness_rate(
    dataframe: pd.DataFrame,
) -> float:

    if dataframe.empty:
        return 0.0

    columns_to_check = [
        column
        for column in dataframe.columns
        if column not in OPTIONAL_COLUMNS
    ]

    if not columns_to_check:
        return 100.0

    total_cells = (
        len(dataframe)
        * len(columns_to_check)
    )

    missing_cells = (
        dataframe[columns_to_check]
        .isna()
        .sum()
        .sum()
    )

    score = (
        1
        - (
            missing_cells
            / total_cells
        )
    ) * 100

    return round(
        max(score, 0.0),
        2,
    )

# ============================================================================
# UNIQUENESS
# ============================================================================


def calculate_uniqueness_rate(
    dataframe: pd.DataFrame,
) -> float:

    if dataframe.empty:
        return 0.0

    duplicates = (
        dataframe
        .duplicated()
        .sum()
    )

    score = (
        1
        - (
            duplicates
            / len(dataframe)
        )
    ) * 100

    return round(
        max(
            score,
            0.0,
        ),
        2,
    )


# ============================================================================
# FRESHNESS
# ============================================================================


def calculate_freshness_rate() -> float:
    """
    Placeholder.

    La logique future utilisera :
    dataset_registry.last_acquisition_date
    """

    return 100.0


# ============================================================================
# CHRONOLOGY
# ============================================================================


def calculate_chronology_rate(
    dataframe: pd.DataFrame,
    chronology_enabled: bool,
) -> float:

    if not chronology_enabled:
        return 100.0

    date_columns = [
        column
        for column in dataframe.columns
        if any(
            keyword in column.lower()
            for keyword in (
                "date",
                "datetime",
                "timestamp",
            )
        )
    ]

    return (
        100.0
        if date_columns
        else 0.0
    )


# ============================================================================
# GLOBAL SCORE
# ============================================================================


def calculate_weighted_quality_score(
    metrics: dict,
) -> float:

    weighted_score = 0.0

    for metric_name, weight in (
        QUALITY_WEIGHTS.items()
    ):

        weighted_score += (
            metrics.get(
                metric_name,
                0.0,
            )
            * weight
        )

    return round(
        weighted_score,
        2,
    )


# ============================================================================
# DATASET METRICS
# ============================================================================


def calculate_dataset_metrics(
    dataframe: pd.DataFrame,
    chronology_enabled: bool = False,
) -> dict:

    return {
        "Availability Rate":
            calculate_availability_rate(
                dataframe
            ),

        "Completeness Rate":
            calculate_completeness_rate(
                dataframe
            ),

        "Uniqueness Rate":
            calculate_uniqueness_rate(
                dataframe
            ),

        "Freshness Rate":
            calculate_freshness_rate(),

        "Chronology Rate":
            calculate_chronology_rate(
                dataframe=dataframe,
                chronology_enabled=chronology_enabled,
            ),
    }


# ============================================================================
# ORCHESTRATOR
# ============================================================================


def calculate_quality_metrics(
    dataframe: pd.DataFrame,
    chronology_enabled: bool = False,
) -> dict:

    logger.info(
        "Quality metrics calculation started"
    )

    records_checked = len(
        dataframe
    )

    execution_date = (
        datetime.now()
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    metrics = (
        calculate_dataset_metrics(
            dataframe=dataframe,
            chronology_enabled=chronology_enabled,
        )
    )

    global_score = (
        calculate_weighted_quality_score(
            metrics
        )
    )

    global_grade = (
        calculate_quality_grade(
            global_score
        )
    )

    global_status = (
        calculate_metric_status(
            global_score
        )
    )

    records = []

    for metric_name, metric_value in (
        metrics.items()
    ):

        records.append(
            {
                "metric":
                    metric_name,

                "value":
                    metric_value,

                "status":
                    calculate_metric_status(
                        metric_value
                    ),

                "threshold":
                    PASS_THRESHOLD,

                "records_checked":
                    records_checked,

                "execution_date":
                    execution_date,
            }
        )

    metrics_dataframe = pd.DataFrame(
        records
    )

    logger.info(
        "Records checked: %s",
        records_checked,
    )

    logger.info(
        "Global Quality Score: %.2f",
        global_score,
    )

    logger.info(
        "Global Quality Grade: %s",
        global_grade,
    )

    logger.info(
        "Global Quality Status: %s",
        global_status,
    )

    logger.info(
        "Quality metrics calculation completed"
    )

    return {
        "metrics":
            metrics_dataframe,

        "global_quality_score":
            global_score,

        "global_quality_grade":
            global_grade,

        "global_quality_status":
            global_status,

        "records_checked":
            records_checked,

        "execution_date":
            execution_date,
    }


# ============================================================================
# MAIN
# ============================================================================


def main():

    print(
        "quality_metrics.py loaded successfully"
    )


if __name__ == "__main__":

    main()