"""
quality_metrics.py

Phase 4 - Data Quality Framework

Les KPI reprennent les six contrôles. Le score d'un jeu est la moyenne
des taux applicables. Le score global est la moyenne des jeux, chaque
jeu ayant le même poids.
"""

from datetime import datetime

import pandas as pd

from src.quality.quality_config import (
    CONTROL_ORDER,
    FAIL_RATE,
    GRADE_THRESHOLDS,
    PASS_RATE,
    STATUS_RANK,
)
from src.utils.logger import logger


def control_rate(issues: int, records_checked: int) -> float:
    if records_checked <= 0:
        return 0.0
    score = (1.0 - (issues / records_checked)) * 100.0
    return round(max(score, 0.0), 2)


def calculate_quality_grade(score: float) -> str:
    if score >= GRADE_THRESHOLDS["A"]:
        return "A"
    if score >= GRADE_THRESHOLDS["B"]:
        return "B"
    if score >= GRADE_THRESHOLDS["C"]:
        return "C"
    return "D"


def worst_status(statuses) -> str:
    ranked = [status for status in statuses if status != "NOT_APPLICABLE"]
    if not ranked:
        return "FAIL"
    return max(ranked, key=lambda status: STATUS_RANK.get(status, 2))


def calculate_quality_metrics(controls: pd.DataFrame) -> dict:
    """Calcule les KPI à partir des contrôles, sans relire les tables."""
    logger.info("Quality metrics calculation started")
    if controls.empty:
        raise ValueError("Quality controls are empty.")

    execution_date = str(controls["execution_date"].iloc[0])
    dataset_rows = []
    metric_rows = []

    for dataset_id, dataset_controls in controls.groupby("dataset_id", sort=True):
        dataset_controls = dataset_controls.set_index("control_name")
        source_id = dataset_controls["source_id"].iloc[0]
        table_name = dataset_controls["table_name"].iloc[0]
        records_checked = int(dataset_controls["records_checked"].max())
        applicable_rates = []

        for control_name in CONTROL_ORDER:
            row = dataset_controls.loc[control_name]
            applicable = bool(row["applicable"])
            issues = int(row["issues"])
            checked = int(row["records_checked"])
            rate = control_rate(issues, checked) if applicable else None
            if applicable:
                applicable_rates.append(rate)
            metric_rows.append(
                {
                    "dataset_id": dataset_id,
                    "source_id": source_id,
                    "table_name": table_name,
                    "metric": control_name,
                    "value": rate,
                    "status": row["status"],
                    "threshold": PASS_RATE,
                    "weight": 1.0 if applicable else 0.0,
                    "applicable": int(applicable),
                    "records_checked": checked,
                    "execution_date": execution_date,
                }
            )

        dataset_score = round(sum(applicable_rates) / len(applicable_rates), 2)
        dataset_status = worst_status(dataset_controls["status"])
        dataset_rows.append(
            {
                "dataset_id": dataset_id,
                "source_id": source_id,
                "table_name": table_name,
                "records_checked": records_checked,
                "dataset_quality_score": dataset_score,
                "dataset_quality_grade": calculate_quality_grade(dataset_score),
                "dataset_quality_status": dataset_status,
            }
        )

    dataset_metrics = pd.DataFrame(dataset_rows)
    detailed_metrics = pd.DataFrame(metric_rows)

    global_rows = []
    for control_name in CONTROL_ORDER:
        applicable = detailed_metrics[
            (detailed_metrics["metric"] == control_name)
            & (detailed_metrics["applicable"] == 1)
        ]
        if applicable.empty:
            value = None
            status = "NOT_APPLICABLE"
        else:
            value = round(float(applicable["value"].mean()), 2)
            status = worst_status(applicable["status"])
        global_rows.append(
            {
                "metric": control_name,
                "value": value if value is not None else 0.0,
                "status": status,
                "threshold": PASS_RATE,
                "records_checked": int(dataset_metrics["records_checked"].sum()),
                "execution_date": execution_date,
            }
        )

    metrics = pd.DataFrame(global_rows)
    global_score = round(float(dataset_metrics["dataset_quality_score"].mean()), 2)
    global_grade = calculate_quality_grade(global_score)
    global_status = worst_status(dataset_metrics["dataset_quality_status"])
    records_checked = int(dataset_metrics["records_checked"].sum())

    logger.info("Datasets checked: %s", len(dataset_metrics))
    logger.info("Global Quality Score: %.2f", global_score)
    logger.info("Global Quality Grade: %s", global_grade)
    logger.info("Global Quality Status: %s", global_status)
    logger.info("Quality metrics calculation completed")

    return {
        "metrics": metrics,
        "dataset_metrics": dataset_metrics,
        "detailed_metrics": detailed_metrics,
        "global_quality_score": global_score,
        "global_quality_grade": global_grade,
        "global_quality_status": global_status,
        "records_checked": records_checked,
        "datasets_checked": int(len(dataset_metrics)),
        "execution_date": execution_date,
        "fail_rate": FAIL_RATE,
    }


def main():
    print("quality_metrics.py loaded successfully")
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    main()
