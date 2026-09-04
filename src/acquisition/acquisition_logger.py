"""
acquisition_logger.py

Journalisation des acquisitions de données.

Chaque exécution d'un script d'acquisition doit créer
une entrée dans :

audit/acquisition_log.xlsx
"""

from datetime import datetime

import pandas as pd

from src.config import ACQUISITION_LOG_FILE

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


def initialize_log():
    """
    Crée acquisition_log.xlsx s'il n'existe pas.
    """

    if not ACQUISITION_LOG_FILE.exists():

        # Création du dossier audit si nécessaire

        ACQUISITION_LOG_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        df = pd.DataFrame(columns=LOG_COLUMNS)

        df.to_excel(
            ACQUISITION_LOG_FILE,
            index=False,
        )


# ============================================================================
# RUN ID GENERATION
# ============================================================================


def generate_run_id():
    """
    Génère un identifiant unique d'acquisition.

    Exemple :
    ACQ-20260903-141523
    """

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

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

    Parameters
    ----------
    source_id : str
        Identifiant de la source (SRC-XXX)

    dataset : str
        Dataset concerné

    provider : str
        Fournisseur de données

    period_covered : str
        Période couverte par l'acquisition

    output_file : str
        Fichier généré

    storage_location : str
        Emplacement du fichier généré

    status : str
        Planned, Success, Failed ou Partial Success

    records_downloaded : int | None
        Nombre d'enregistrements téléchargés.
        None lorsque l'information n'est pas encore disponible.

    notes : str
        Commentaires éventuels
    """

    if status not in VALID_STATUS:
        raise ValueError(
            f"Status must be one of: {VALID_STATUS}"
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

    print(
        f"[LOGGED] {run_id} | "
        f"{dataset} | "
        f"{status}"
    )

    return run_id