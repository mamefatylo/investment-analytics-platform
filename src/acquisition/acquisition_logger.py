"""
acquisition_logger.py

Journalisation des acquisitions de données.

Chaque exécution d'un script d'acquisition crée
une entrée dans :

audit/acquisition_log.xlsx
"""

from datetime import datetime

import pandas as pd

from src.config import (
    ACQUISITION_LOG_FILE,
)

from src.utils.logger import (
    logger,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

LOG_COLUMNS = [
    "Run ID",
    "Acquisition Date",
    "Source ID",
    "Dataset",
    "Provider",
    "Period Covered",
    "Output File",
    "Storage Location",
    "Status",
    "Records Downloaded",
    "Notes",
]

VALID_STATUS = [
    "Planned",
    "Success",
    "Failed",
    "Partial Success",
]

# ============================================================================
# INITIALIZATION
# ============================================================================


def initialize_log() -> None:
    """
    Crée acquisition_log.xlsx s'il n'existe pas.
    """

    if not ACQUISITION_LOG_FILE.exists():

        ACQUISITION_LOG_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        pd.DataFrame(
            columns=LOG_COLUMNS
        ).to_excel(
            ACQUISITION_LOG_FILE,
            index=False,
        )

        logger.info(
            "Acquisition log initialized"
        )


# ============================================================================
# RUN ID GENERATION
# ============================================================================


def generate_run_id() -> str:
    """
    Génère un identifiant unique d'acquisition.

    Exemple :
    ACQ-20260903-141523
    """

    timestamp = datetime.now().strftime(
        "%Y%m%d-%H%M%S"
    )

    return f"ACQ-{timestamp}"


# ============================================================================
# LOGGING FUNCTION
# ============================================================================


def log_acquisition(
    source_id,
    dataset,
    provider,
    period_covered,
    output_file,
    storage_location,
    status,
    records_downloaded=None,
    notes="",
):
    """
    Ajoute une ligne dans acquisition_log.xlsx.
    """

    if status not in VALID_STATUS:

        raise ValueError(
            f"Status must be one of: "
            f"{VALID_STATUS}"
        )

    initialize_log()

    run_id = generate_run_id()

    record = {
        "Run ID": run_id,
        "Acquisition Date": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "Source ID": source_id,
        "Dataset": dataset,
        "Provider": provider,
        "Period Covered": period_covered,
        "Output File": str(output_file),
        "Storage Location": str(storage_location),
        "Status": status,
        "Records Downloaded": records_downloaded,
        "Notes": notes,
    }

    try:

        existing_log = pd.read_excel(
            ACQUISITION_LOG_FILE
        )

    except Exception:

        existing_log = pd.DataFrame(
            columns=LOG_COLUMNS
        )

    updated_log = pd.concat(
        [
            existing_log,
            pd.DataFrame([record]),
        ],
        ignore_index=True,
    )

    updated_log.to_excel(
        ACQUISITION_LOG_FILE,
        index=False,
    )

    logger.info(
        f"[AUDIT] {run_id} | "
        f"{source_id} | "
        f"{dataset} | "
        f"{status}"
    )

    return run_id