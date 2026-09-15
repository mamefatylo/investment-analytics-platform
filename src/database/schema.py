"""
schema.py

Gestion versionnee du schema SQLite permanent de la plateforme.

Tables techniques
-----------------
- schema_migrations
- acquisition_log
- dataset_registry
- quality_run
- quality_control_result
- quality_metric_result

Les tables de donnees metier sont creees dynamiquement par load_database.py a
partir du Source Registry. Ce module ne contient aucun identifiant de source,
de dataset, de fichier ou de table metier.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import sqlite3

from src.config import DATABASE_DIR, DATABASE_FILE
from src.utils.logger import logger

DATABASE_TIMEOUT_SECONDS = 30
BUSY_TIMEOUT_MILLISECONDS = 30_000
CURRENT_SCHEMA_VERSION = 2

MIGRATIONS_TABLE = "schema_migrations"
ACQUISITION_LOG_TABLE = "acquisition_log"
DATASET_REGISTRY_TABLE = "dataset_registry"
QUALITY_RUN_TABLE = "quality_run"
QUALITY_CONTROL_RESULT_TABLE = "quality_control_result"
QUALITY_METRIC_RESULT_TABLE = "quality_metric_result"

PROCESS_TYPES = (
    "Acquisition",
    "Preparation",
)

RUN_STATUSES = (
    "Planned",
    "Success",
    "Partial Success",
    "Failed",
)

QUALITY_STATUSES = (
    "PASS",
    "WARNING",
    "FAIL",
    "NOT_APPLICABLE",
)

QUALITY_GRADES = (
    "A",
    "B",
    "C",
    "D",
)

EXPECTED_COLUMNS = {
    MIGRATIONS_TABLE: [
        "version",
        "description",
        "applied_at",
    ],
    ACQUISITION_LOG_TABLE: [
        "run_id",
        "execution_date",
        "process_type",
        "output_file",
        "storage_location",
        "status",
        "records_produced",
        "notes",
        "created_at",
    ],
    DATASET_REGISTRY_TABLE: [
        "dataset_id",
        "source_id",
        "dataset_name",
        "output_file_name",
        "table_name",
        "load_enabled",
        "quality_enabled",
        "chronology_enabled",
        "active",
        "output_file",
        "storage_location",
        "last_acquisition_date",
        "last_acquisition_status",
        "records_loaded",
        "last_database_load_date",
        "database_name",
        "table_available",
        "created_at",
        "updated_at",
    ],
    QUALITY_RUN_TABLE: [
        "run_id",
        "execution_date",
        "datasets_checked",
        "records_checked",
        "controls_executed",
        "issues_detected",
        "global_quality_score",
        "global_quality_grade",
        "global_status",
        "notes",
        "created_at",
    ],
    QUALITY_CONTROL_RESULT_TABLE: [
        "run_id",
        "dataset_id",
        "source_id",
        "table_name",
        "control_id",
        "control_name",
        "status",
        "issues",
        "records_checked",
        "notes",
        "execution_date",
        "created_at",
    ],
    QUALITY_METRIC_RESULT_TABLE: [
        "run_id",
        "dataset_id",
        "source_id",
        "table_name",
        "metric_name",
        "metric_value",
        "metric_status",
        "threshold",
        "weight",
        "applicable",
        "records_checked",
        "execution_date",
        "created_at",
    ],
}


def utc_timestamp():
    """Retourne un timestamp UTC ISO 8601."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_sql_literals(values):
    """Construit une liste SQL depuis des constantes techniques internes."""
    return ", ".join(
        "'" + str(value).replace("'", "''") + "'"
        for value in values
    )


def configure_connection(connection):
    """Configure la connexion SQLite."""
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MILLISECONDS}")
    connection.execute("PRAGMA synchronous = NORMAL")


def create_connection():
    """Cree et configure une connexion SQLite en mode WAL."""
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        DATABASE_FILE,
        timeout=DATABASE_TIMEOUT_SECONDS,
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        configure_connection(connection)
    except Exception:
        connection.close()
        raise
    return connection


@contextmanager
def database_connection():
    """Fournit une connexion transactionnelle geree."""
    connection = create_connection()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def create_migrations_table(connection):
    """Cree la table de suivi des migrations."""
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            version INTEGER PRIMARY KEY CHECK (version > 0),
            description TEXT NOT NULL CHECK (length(trim(description)) > 0),
            applied_at TEXT NOT NULL CHECK (length(trim(applied_at)) > 0)
        )
        """
    )


def get_applied_migration_versions(connection):
    """Retourne les versions de migration deja appliquees."""
    rows = connection.execute(
        f"SELECT version FROM {MIGRATIONS_TABLE} ORDER BY version"
    ).fetchall()
    return {int(row["version"]) for row in rows}


def record_migration(connection, version, description):
    """Enregistre une migration appliquee."""
    connection.execute(
        f"""
        INSERT INTO {MIGRATIONS_TABLE} (
            version,
            description,
            applied_at
        )
        VALUES (?, ?, ?)
        """,
        (int(version), str(description), utc_timestamp()),
    )


def apply_migration_001(connection):
    """Cree acquisition_log et dataset_registry."""
    process_types = build_sql_literals(PROCESS_TYPES)
    run_statuses = build_sql_literals(RUN_STATUSES)

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ACQUISITION_LOG_TABLE} (
            run_id TEXT PRIMARY KEY CHECK (length(trim(run_id)) > 0),
            execution_date TEXT NOT NULL CHECK (length(trim(execution_date)) > 0),
            process_type TEXT NOT NULL CHECK (process_type IN ({process_types})),
            output_file TEXT NOT NULL CHECK (length(trim(output_file)) > 0),
            storage_location TEXT NOT NULL CHECK (length(trim(storage_location)) > 0),
            status TEXT NOT NULL CHECK (status IN ({run_statuses})),
            records_produced INTEGER CHECK (
                records_produced IS NULL OR records_produced >= 0
            ),
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0)
        )
        """
    )

    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_acquisition_log_output_date
        ON {ACQUISITION_LOG_TABLE} (output_file, execution_date DESC)
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_acquisition_log_status_date
        ON {ACQUISITION_LOG_TABLE} (status, execution_date DESC)
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_acquisition_log_process_date
        ON {ACQUISITION_LOG_TABLE} (process_type, execution_date DESC)
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DATASET_REGISTRY_TABLE} (
            dataset_id TEXT PRIMARY KEY CHECK (length(trim(dataset_id)) > 0),
            source_id TEXT NOT NULL CHECK (length(trim(source_id)) > 0),
            dataset_name TEXT NOT NULL CHECK (length(trim(dataset_name)) > 0),
            output_file_name TEXT NOT NULL UNIQUE
                CHECK (length(trim(output_file_name)) > 0),
            table_name TEXT NOT NULL UNIQUE CHECK (length(trim(table_name)) > 0),
            load_enabled INTEGER NOT NULL CHECK (load_enabled IN (0, 1)),
            quality_enabled INTEGER NOT NULL CHECK (quality_enabled IN (0, 1)),
            chronology_enabled INTEGER NOT NULL CHECK (chronology_enabled IN (0, 1)),
            active INTEGER NOT NULL CHECK (active IN (0, 1)),
            output_file TEXT,
            storage_location TEXT,
            last_acquisition_date TEXT,
            last_acquisition_status TEXT CHECK (
                last_acquisition_status IS NULL
                OR last_acquisition_status IN ({run_statuses})
            ),
            records_loaded INTEGER CHECK (
                records_loaded IS NULL OR records_loaded >= 0
            ),
            last_database_load_date TEXT,
            database_name TEXT,
            table_available INTEGER NOT NULL DEFAULT 0
                CHECK (table_available IN (0, 1)),
            created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
            updated_at TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
            CHECK (quality_enabled = 0 OR load_enabled = 1),
            CHECK (chronology_enabled = 0 OR quality_enabled = 1),
            CHECK (
                active = 1
                OR (
                    load_enabled = 0
                    AND quality_enabled = 0
                    AND chronology_enabled = 0
                )
            )
        )
        """
    )

    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_dataset_registry_source
        ON {DATASET_REGISTRY_TABLE} (source_id)
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_dataset_registry_load_scope
        ON {DATASET_REGISTRY_TABLE} (active, load_enabled)
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_dataset_registry_quality_scope
        ON {DATASET_REGISTRY_TABLE} (active, quality_enabled)
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_dataset_registry_output_file
        ON {DATASET_REGISTRY_TABLE} (output_file)
        """
    )


def apply_migration_002(connection):
    """Cree les tables permanentes de resultats qualite."""
    quality_statuses = build_sql_literals(QUALITY_STATUSES)
    quality_grades = build_sql_literals(QUALITY_GRADES)

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_RUN_TABLE} (
            run_id TEXT PRIMARY KEY CHECK (length(trim(run_id)) > 0),
            execution_date TEXT NOT NULL CHECK (length(trim(execution_date)) > 0),
            datasets_checked INTEGER NOT NULL CHECK (datasets_checked >= 0),
            records_checked INTEGER NOT NULL CHECK (records_checked >= 0),
            controls_executed INTEGER NOT NULL CHECK (controls_executed >= 0),
            issues_detected INTEGER NOT NULL CHECK (issues_detected >= 0),
            global_quality_score REAL NOT NULL CHECK (
                global_quality_score >= 0 AND global_quality_score <= 100
            ),
            global_quality_grade TEXT NOT NULL CHECK (
                global_quality_grade IN ({quality_grades})
            ),
            global_status TEXT NOT NULL CHECK (
                global_status IN ({quality_statuses})
            ),
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0)
        )
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_CONTROL_RESULT_TABLE} (
            run_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            control_id TEXT NOT NULL,
            control_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ({quality_statuses})),
            issues INTEGER NOT NULL CHECK (issues >= 0),
            records_checked INTEGER NOT NULL CHECK (records_checked >= 0),
            notes TEXT NOT NULL DEFAULT '',
            execution_date TEXT NOT NULL CHECK (length(trim(execution_date)) > 0),
            created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
            PRIMARY KEY (run_id, dataset_id, control_id),
            FOREIGN KEY (run_id)
                REFERENCES {QUALITY_RUN_TABLE} (run_id)
                ON DELETE CASCADE,
            FOREIGN KEY (dataset_id)
                REFERENCES {DATASET_REGISTRY_TABLE} (dataset_id)
        )
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_METRIC_RESULT_TABLE} (
            run_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL,
            metric_status TEXT NOT NULL CHECK (
                metric_status IN ({quality_statuses})
            ),
            threshold REAL CHECK (
                threshold IS NULL OR (threshold >= 0 AND threshold <= 100)
            ),
            weight REAL NOT NULL DEFAULT 1.0 CHECK (weight >= 0),
            applicable INTEGER NOT NULL CHECK (applicable IN (0, 1)),
            records_checked INTEGER NOT NULL CHECK (records_checked >= 0),
            execution_date TEXT NOT NULL CHECK (length(trim(execution_date)) > 0),
            created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
            PRIMARY KEY (run_id, dataset_id, metric_name),
            FOREIGN KEY (run_id)
                REFERENCES {QUALITY_RUN_TABLE} (run_id)
                ON DELETE CASCADE,
            FOREIGN KEY (dataset_id)
                REFERENCES {DATASET_REGISTRY_TABLE} (dataset_id)
        )
        """
    )

    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_quality_run_execution_date
        ON {QUALITY_RUN_TABLE} (execution_date DESC)
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_quality_run_status_date
        ON {QUALITY_RUN_TABLE} (global_status, execution_date DESC)
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_quality_control_dataset_date
        ON {QUALITY_CONTROL_RESULT_TABLE} (
            dataset_id,
            execution_date DESC
        )
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_quality_control_status_date
        ON {QUALITY_CONTROL_RESULT_TABLE} (
            status,
            execution_date DESC
        )
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_quality_metric_dataset_date
        ON {QUALITY_METRIC_RESULT_TABLE} (
            dataset_id,
            execution_date DESC
        )
        """
    )
    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_quality_metric_name_date
        ON {QUALITY_METRIC_RESULT_TABLE} (
            metric_name,
            execution_date DESC
        )
        """
    )


MIGRATIONS = (
    (
        1,
        "Create acquisition log and dataset registry",
        apply_migration_001,
    ),
    (
        2,
        "Create quality run, control result, and metric result tables",
        apply_migration_002,
    ),
)


def apply_pending_migrations(connection):
    """Applique les migrations non encore enregistrees."""
    create_migrations_table(connection)
    applied_versions = get_applied_migration_versions(connection)

    for version, description, migration_function in MIGRATIONS:
        if version in applied_versions:
            logger.info("Database migration already applied: %s", version)
            continue

        logger.info("Applying database migration %s: %s", version, description)
        savepoint_name = f"migration_{int(version)}"
        connection.execute(f"SAVEPOINT {savepoint_name}")
        try:
            migration_function(connection)
            record_migration(connection, version, description)
            connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        except Exception:
            connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            raise

        logger.info("Database migration applied: %s", version)


def table_exists(connection, table_name):
    """Verifie qu'une table existe."""
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    return row is not None


def get_table_columns(connection, table_name):
    """Retourne les colonnes d'une table SQLite."""
    escaped_table_name = str(table_name).replace('"', '""')
    rows = connection.execute(
        f'PRAGMA table_info("{escaped_table_name}")'
    ).fetchall()
    return [str(row["name"]) for row in rows]


def validate_table_schema(connection, table_name, expected_columns):
    """Verifie qu'une table possede exactement les colonnes attendues."""
    if not table_exists(connection, table_name):
        raise RuntimeError(f"Required technical table does not exist: {table_name}")

    actual_columns = get_table_columns(connection, table_name)
    if actual_columns != expected_columns:
        raise RuntimeError(
            f"Invalid schema for {table_name}. "
            f"Expected columns: {expected_columns}. "
            f"Actual columns: {actual_columns}."
        )


def validate_database_schema(connection):
    """Verifie tout le schema technique et la version courante."""
    for table_name, expected_columns in EXPECTED_COLUMNS.items():
        validate_table_schema(connection, table_name, expected_columns)

    applied_versions = get_applied_migration_versions(connection)
    expected_versions = {version for version, _, _ in MIGRATIONS}
    missing_versions = sorted(expected_versions - applied_versions)
    if missing_versions:
        raise RuntimeError(f"Database migrations are missing: {missing_versions}")
    if CURRENT_SCHEMA_VERSION != max(expected_versions):
        raise RuntimeError(
            "CURRENT_SCHEMA_VERSION does not match the migration registry."
        )


def run_integrity_check(connection):
    """Execute le controle d'integrite SQLite."""
    row = connection.execute("PRAGMA integrity_check").fetchone()
    result = str(row[0]) if row is not None else ""
    if result.lower() != "ok":
        raise RuntimeError(f"SQLite integrity check failed: {result}")


def initialize_database_schema():
    """Initialise, migre et valide le schema SQLite."""
    logger.info("Database schema initialization started")
    try:
        with database_connection() as connection:
            apply_pending_migrations(connection)
            validate_database_schema(connection)
            run_integrity_check(connection)

        logger.info("Database schema version: %s", CURRENT_SCHEMA_VERSION)
        for table_name in EXPECTED_COLUMNS:
            if table_name != MIGRATIONS_TABLE:
                logger.info("Technical table available: %s", table_name)
        logger.info("Database schema initialization completed successfully")
    except Exception as error:
        logger.exception("Database schema initialization failed: %s", error)
        raise


def main():
    """Point d'entree du module."""
    initialize_database_schema()


if __name__ == "__main__":
    main()
