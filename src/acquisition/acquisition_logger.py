"""
acquisition_logger.py

Journalisation transactionnelle des traitements produisant des datasets.

Objectif
--------
Enregistrer dans SQLite les exécutions des processus d'acquisition
et de préparation.

La table SQLite acquisition_log constitue la source officielle
de la traçabilité opérationnelle.

Les métadonnées métier restent dans le Source Registry et seront
rapprochées ultérieurement grâce au nom du fichier produit.

Ce module n'écrit aucun fichier Excel.
"""

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from uuid import uuid4

from src.database.schema import (
    ACQUISITION_LOG_TABLE,
    database_connection,
    initialize_database_schema,
)

from src.utils.logger import logger

# ============================================================================
# TECHNICAL CONTRACT
# ============================================================================

VALID_PROCESS_TYPES = frozenset(
    {
        "Acquisition",
        "Preparation",
    }
)

VALID_STATUSES = frozenset(
    {
        "Planned",
        "Success",
        "Partial Success",
        "Failed",
    }
)

# ============================================================================
# TIME AND RUN IDENTIFICATION
# ============================================================================


def utc_now():
    """
    Retourne la date et l'heure actuelles en UTC.
    """

    return datetime.now(
        timezone.utc
    )


def to_utc_iso(value):
    """
    Convertit une date en UTC au format ISO 8601.
    """

    if value.tzinfo is None:

        value = value.replace(
            tzinfo=timezone.utc
        )

    else:

        value = value.astimezone(
            timezone.utc
        )

    return value.isoformat(
        timespec="seconds"
    )


def generate_run_id(
    execution_datetime=None,
):
    """
    Génère un identifiant unique d'exécution.

    Exemple :
    RUN-20260914-153056-A1B2C3D4
    """

    value = (
        execution_datetime
        or utc_now()
    )

    timestamp = (
        value
        .astimezone(timezone.utc)
        .strftime("%Y%m%d-%H%M%S")
    )

    unique_suffix = (
        uuid4()
        .hex[:8]
        .upper()
    )

    return (
        f"RUN-{timestamp}-"
        f"{unique_suffix}"
    )


# ============================================================================
# INPUT NORMALIZATION
# ============================================================================


def normalize_required_text(
    value,
    field_name,
):
    """
    Vérifie qu'une valeur textuelle obligatoire
    est présente et non vide.
    """

    if value is None:

        raise ValueError(
            f"{field_name} is required."
        )

    normalized_value = str(
        value
    ).strip()

    if not normalized_value:

        raise ValueError(
            f"{field_name} cannot be empty."
        )

    return normalized_value


def normalize_output_file(
    value,
):
    """
    Normalise le nom du fichier produit.

    Seul le nom du fichier est conservé.
    Son dossier est enregistré séparément.
    """

    normalized_value = (
        normalize_required_text(
            value,
            "output_file",
        )
    )

    file_name = Path(
        normalized_value
    ).name

    if file_name in {
        "",
        ".",
        "..",
    }:

        raise ValueError(
            "output_file must identify a file."
        )

    if not Path(
        file_name
    ).suffix:

        raise ValueError(
            "output_file must include "
            "a file extension."
        )

    return file_name


def normalize_storage_location(
    value,
):
    """
    Normalise le dossier contenant le fichier produit.
    """

    normalized_value = (
        normalize_required_text(
            value,
            "storage_location",
        )
    )

    return str(
        Path(
            normalized_value
        )
    )


def normalize_choice(
    value,
    field_name,
    allowed_values,
):
    """
    Valide une valeur appartenant à un ensemble autorisé.
    """

    normalized_value = (
        normalize_required_text(
            value,
            field_name,
        )
    )

    if normalized_value not in allowed_values:

        raise ValueError(
            f"{field_name} must be one of: "
            f"{sorted(allowed_values)}."
        )

    return normalized_value


def normalize_records_produced(
    value,
):
    """
    Valide le nombre d'enregistrements produits.

    La valeur doit être calculée dynamiquement
    par le processus producteur.
    """

    if value is None:
        return None

    if isinstance(
        value,
        bool,
    ):

        raise ValueError(
            "records_produced cannot "
            "be a boolean."
        )

    try:

        normalized_value = int(
            value
        )

    except (
        TypeError,
        ValueError,
    ) as error:

        raise ValueError(
            "records_produced must be "
            "an integer or None."
        ) from error

    if normalized_value < 0:

        raise ValueError(
            "records_produced cannot "
            "be negative."
        )

    if (
        isinstance(
            value,
            float,
        )
        and not value.is_integer()
    ):

        raise ValueError(
            "records_produced must be "
            "a whole number."
        )

    return normalized_value


def normalize_notes(
    value,
):
    """
    Normalise les notes opérationnelles.
    """

    if value is None:
        return ""

    return str(
        value
    ).strip()


# ============================================================================
# OUTPUT VALIDATION
# ============================================================================


def validate_output_path(
    output_file,
    storage_location,
    status,
):
    """
    Vérifie qu'un fichier déclaré comme produit existe.

    Le contrôle est appliqué aux statuts :
    - Success ;
    - Partial Success.

    Les statuts Planned et Failed peuvent référencer
    un fichier qui n'existe pas.
    """

    if status not in {
        "Success",
        "Partial Success",
    }:

        return

    output_path = (
        Path(storage_location)
        / output_file
    )

    if not output_path.exists():

        raise FileNotFoundError(
            "A successful run cannot reference "
            "a missing output file: "
            f"{output_path}"
        )

    if not output_path.is_file():

        raise ValueError(
            "Output path is not a file: "
            f"{output_path}"
        )


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================


def insert_run(
    connection,
    record,
):
    """
    Insère une exécution dans acquisition_log.
    """

    insert_statement = f"""
        INSERT INTO {ACQUISITION_LOG_TABLE} (
            run_id,
            execution_date,
            process_type,
            output_file,
            storage_location,
            status,
            records_produced,
            notes,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    connection.execute(
        insert_statement,
        (
            record["run_id"],
            record["execution_date"],
            record["process_type"],
            record["output_file"],
            record["storage_location"],
            record["status"],
            record["records_produced"],
            record["notes"],
            record["created_at"],
        ),
    )


def verify_insert(
    connection,
    run_id,
):
    """
    Vérifie que l'exécution a été enregistrée.
    """

    row = connection.execute(
        f"""
        SELECT run_id
        FROM {ACQUISITION_LOG_TABLE}
        WHERE run_id = ?
        """,
        (
            run_id,
        ),
    ).fetchone()

    if row is None:

        raise RuntimeError(
            "Acquisition run was not persisted: "
            f"{run_id}"
        )


# ============================================================================
# GENERIC DATA RUN LOGGER
# ============================================================================


def log_data_run(
    output_file,
    storage_location,
    status,
    process_type,
    records_produced=None,
    notes="",
):
    """
    Enregistre une exécution dans SQLite.

    Parameters
    ----------
    output_file
        Nom ou chemin du fichier produit.

    storage_location
        Dossier contenant le fichier produit.

    status
        Planned, Success, Partial Success ou Failed.

    process_type
        Acquisition ou Preparation.

    records_produced
        Nombre d'enregistrements calculé par le producteur.
        La valeur peut être absente pour un fichier non tabulaire.

    notes
        Informations complémentaires.

    Returns
    -------
    str
        Identifiant unique de l'exécution.
    """

    normalized_output_file = (
        normalize_output_file(
            output_file
        )
    )

    normalized_storage_location = (
        normalize_storage_location(
            storage_location
        )
    )

    normalized_status = (
        normalize_choice(
            status,
            "status",
            VALID_STATUSES,
        )
    )

    normalized_process_type = (
        normalize_choice(
            process_type,
            "process_type",
            VALID_PROCESS_TYPES,
        )
    )

    normalized_records = (
        normalize_records_produced(
            records_produced
        )
    )

    normalized_notes = (
        normalize_notes(
            notes
        )
    )

    validate_output_path(
        normalized_output_file,
        normalized_storage_location,
        normalized_status,
    )

    execution_datetime = utc_now()

    execution_date = to_utc_iso(
        execution_datetime
    )

    run_id = generate_run_id(
        execution_datetime
    )

    record = {
        "run_id":
            run_id,
        "execution_date":
            execution_date,
        "process_type":
            normalized_process_type,
        "output_file":
            normalized_output_file,
        "storage_location":
            normalized_storage_location,
        "status":
            normalized_status,
        "records_produced":
            normalized_records,
        "notes":
            normalized_notes,
        "created_at":
            execution_date,
    }

    initialize_database_schema()

    try:

        with database_connection() as connection:

            insert_run(
                connection,
                record,
            )

            verify_insert(
                connection,
                run_id,
            )

    except sqlite3.IntegrityError as error:

        logger.exception(
            "Acquisition Log integrity error "
            f"for run {run_id}"
        )

        raise ValueError(
            "Acquisition run violates the "
            "database contract: "
            f"{run_id}"
        ) from error

    except sqlite3.DatabaseError:

        logger.exception(
            "Acquisition Log database error "
            f"for run {run_id}"
        )

        raise

    logger.info(
        f"[AUDIT] {run_id} | "
        f"{normalized_process_type} | "
        f"{normalized_output_file} | "
        f"{normalized_status} | "
        f"Records={normalized_records}"
    )

    return run_id


# ============================================================================
# PROCESS-SPECIFIC ENTRY POINTS
# ============================================================================


def log_acquisition(
    output_file,
    storage_location,
    status,
    records_produced=None,
    notes="",
):
    """
    Journalise un fichier produit par
    une acquisition externe.
    """

    return log_data_run(
        output_file=output_file,
        storage_location=storage_location,
        status=status,
        process_type="Acquisition",
        records_produced=records_produced,
        notes=notes,
    )


def log_preparation(
    output_file,
    storage_location,
    status,
    records_produced=None,
    notes="",
):
    """
    Journalise un fichier produit par
    une préparation interne.
    """

    return log_data_run(
        output_file=output_file,
        storage_location=storage_location,
        status=status,
        process_type="Preparation",
        records_produced=records_produced,
        notes=notes,
    )