"""
load_database.py

Load active datasets marked Load Enabled into SQLite.

The Source Registry is the configuration authority for input paths, table
names, and dataset metadata. Each target table is staged, verified, and swapped
inside a SQLite transaction. Existing published tables remain available if a
load fails.

No business Dataset ID, Source ID, file name, table name, or storage path is
embedded in this module.
"""

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import DATABASE_FILE
from src.database.schema import initialize_database_schema
from src.governance.source_registry import (
    build_dataset_absolute_path,
    get_load_enabled_datasets,
)
from src.utils.logger import logger

DATASET_REGISTRY_TABLE = "dataset_registry"
SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".json", ".xlsx", ".xls"}
SQL_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


def utc_timestamp():
    """Return a timezone-aware UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def quote_identifier(identifier):
    """Safely quote a validated SQLite identifier."""
    value = str(identifier).strip()
    if not SQL_IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid SQLite identifier: {identifier}")
    return f'"{value}"'


def normalize_column_name(column_name):
    """Convert a source column name to a stable SQLite identifier."""
    value = str(column_name).strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        raise ValueError(f"Unable to normalize column name: {column_name}")
    if value[0].isdigit():
        value = f"column_{value}"
    if not SQL_IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid normalized column name: {value}")
    return value


def normalize_dataframe_columns(dataframe):
    """Normalize columns and reject collisions after normalization."""
    result = dataframe.copy()
    normalized = [normalize_column_name(column) for column in result.columns]
    duplicates = sorted({name for name in normalized if normalized.count(name) > 1})
    if duplicates:
        raise ValueError(
            "Column names collide after SQLite normalization: "
            f"{duplicates}"
        )
    result.columns = normalized
    return result


def validate_input_file(file_path):
    """Validate a configured input file before reading it."""
    if not file_path.exists():
        raise FileNotFoundError(f"Load-enabled dataset file not found: {file_path}")
    if not file_path.is_file():
        raise ValueError(f"Configured dataset path is not a file: {file_path}")
    if file_path.stat().st_size <= 0:
        raise ValueError(f"Configured dataset file is empty: {file_path}")
    if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported dataset extension {file_path.suffix.lower()}: {file_path}"
        )


def read_dataset(file_path):
    """Read a configured dataset according to its extension."""
    validate_input_file(file_path)
    extension = file_path.suffix.lower()

    if extension == ".csv":
        dataframe = pd.read_csv(file_path, low_memory=False)
    elif extension == ".parquet":
        dataframe = pd.read_parquet(file_path)
    elif extension == ".json":
        dataframe = pd.read_json(file_path)
    elif extension in {".xlsx", ".xls"}:
        engine = "openpyxl" if extension == ".xlsx" else "xlrd"
        dataframe = pd.read_excel(file_path, engine=engine)
    else:
        raise ValueError(f"Unsupported dataset extension: {extension}")

    if dataframe.columns.duplicated().any():
        duplicates = dataframe.columns[dataframe.columns.duplicated()].tolist()
        raise ValueError(f"Dataset contains duplicate columns: {duplicates}")

    return normalize_dataframe_columns(dataframe)


def infer_sqlite_type(series):
    """Infer a conservative SQLite affinity from a pandas Series."""
    if pd.api.types.is_bool_dtype(series.dtype):
        return "INTEGER"
    if pd.api.types.is_integer_dtype(series.dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series.dtype):
        return "REAL"
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return "TEXT"
    return "TEXT"


def serialize_value(value):
    """Convert pandas and Python values to SQLite-compatible values."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    return value


def create_staging_table(connection, table_name, dataframe):
    """Create and populate a staging table without publishing it."""
    staging_name = f"_staging_{table_name}"
    connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(staging_name)}")

    if len(dataframe.columns) == 0:
        raise ValueError(f"Dataset has no columns: {table_name}")

    column_definitions = ", ".join(
        f"{quote_identifier(column)} {infer_sqlite_type(dataframe[column])}"
        for column in dataframe.columns
    )
    connection.execute(
        f"CREATE TABLE {quote_identifier(staging_name)} ({column_definitions})"
    )

    if not dataframe.empty:
        columns = list(dataframe.columns)
        placeholders = ", ".join("?" for _ in columns)
        quoted_columns = ", ".join(quote_identifier(column) for column in columns)
        insert_sql = (
            f"INSERT INTO {quote_identifier(staging_name)} "
            f"({quoted_columns}) VALUES ({placeholders})"
        )
        rows = (
            tuple(serialize_value(value) for value in row)
            for row in dataframe.itertuples(index=False, name=None)
        )
        connection.executemany(insert_sql, rows)

    loaded_rows = connection.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(staging_name)}"
    ).fetchone()[0]
    if loaded_rows != len(dataframe):
        raise RuntimeError(
            f"Staging row count mismatch for {table_name}. "
            f"Expected {len(dataframe)}, found {loaded_rows}."
        )

    actual_columns = [
        row[1]
        for row in connection.execute(
            f"PRAGMA table_info({quote_identifier(staging_name)})"
        ).fetchall()
    ]
    if actual_columns != list(dataframe.columns):
        raise RuntimeError(
            f"Staging column mismatch for {table_name}. "
            f"Expected {list(dataframe.columns)}, found {actual_columns}."
        )

    return staging_name


def publish_staging_table(connection, staging_name, table_name):
    """Atomically replace a published table with its verified staging table."""
    backup_name = f"_backup_{table_name}"
    connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(backup_name)}")

    existing = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()

    if existing:
        connection.execute(
            f"ALTER TABLE {quote_identifier(table_name)} "
            f"RENAME TO {quote_identifier(backup_name)}"
        )

    connection.execute(
        f"ALTER TABLE {quote_identifier(staging_name)} "
        f"RENAME TO {quote_identifier(table_name)}"
    )
    connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(backup_name)}")


def table_columns(connection, table_name):
    """Return the physical columns of an existing SQLite table."""
    return [
        row[1]
        for row in connection.execute(
            f"PRAGMA table_info({quote_identifier(table_name)})"
        ).fetchall()
    ]


def register_dataset(connection, configuration, table_name, row_count):
    """Upsert dataset metadata using the columns available in dataset_registry."""
    columns = set(table_columns(connection, DATASET_REGISTRY_TABLE))
    if not columns:
        raise RuntimeError("dataset_registry is unavailable after schema initialization.")

    now = utc_timestamp()
    candidate_values: dict[str, Any] = {
        "table_name": table_name,
        "dataset_name": configuration.get("Dataset Name"),
        "source_id": configuration.get("Source ID"),
        "database_name": DATABASE_FILE.name,
        "active": int(bool(configuration.get("Active"))),
        "load_enabled": int(bool(configuration.get("Load Enabled"))),
        "quality_enabled": int(bool(configuration.get("Quality Enabled"))),
        "chronology_enabled": int(bool(configuration.get("Chronology Enabled"))),
        "row_count": row_count,
        "records_loaded": row_count,
        "output_file_name": configuration.get("Output File Name"),
        "output_file": configuration.get("Output File Name"),
        "dataset_id": configuration.get("Dataset ID"),
        "updated_at": now,
        "created_at": now,
    }

    required = {"table_name"}
    if not required.issubset(columns):
        raise RuntimeError("dataset_registry does not contain table_name.")

    table_info = connection.execute(
        f"PRAGMA table_info({quote_identifier(DATASET_REGISTRY_TABLE)})"
    ).fetchall()
    mandatory_columns = {
        row[1]
        for row in table_info
        if row[3] == 1 and row[4] is None and row[5] == 0
    }
    unsupported_mandatory_columns = sorted(
        column
        for column in mandatory_columns
        if column not in candidate_values
    )
    if unsupported_mandatory_columns:
        raise RuntimeError(
            "dataset_registry contains mandatory columns that the loader cannot populate: "
            f"{unsupported_mandatory_columns}"
        )

    empty_mandatory_columns = sorted(
        column
        for column in mandatory_columns
        if column in candidate_values and candidate_values[column] is None
    )
    if empty_mandatory_columns:
        raise RuntimeError(
            "Dataset configuration cannot populate mandatory dataset_registry fields: "
            f"{empty_mandatory_columns}"
        )

    existing = connection.execute(
        f"SELECT 1 FROM {quote_identifier(DATASET_REGISTRY_TABLE)} "
        "WHERE table_name=?",
        (table_name,),
    ).fetchone()

    writable_values = {
        key: value
        for key, value in candidate_values.items()
        if key in columns and not (existing and key == "created_at")
    }

    if existing:
        assignments = ", ".join(
            f"{quote_identifier(column)}=?"
            for column in writable_values
            if column != "table_name"
        )
        values = [
            writable_values[column]
            for column in writable_values
            if column != "table_name"
        ]
        if assignments:
            connection.execute(
                f"UPDATE {quote_identifier(DATASET_REGISTRY_TABLE)} "
                f"SET {assignments} WHERE table_name=?",
                (*values, table_name),
            )
    else:
        insert_values = {
            key: value for key, value in candidate_values.items() if key in columns
        }
        insert_columns = list(insert_values)
        column_sql = ", ".join(quote_identifier(column) for column in insert_columns)
        placeholders = ", ".join("?" for _ in insert_columns)
        connection.execute(
            f"INSERT INTO {quote_identifier(DATASET_REGISTRY_TABLE)} "
            f"({column_sql}) VALUES ({placeholders})",
            tuple(insert_values[column] for column in insert_columns),
        )


def load_one_dataset(connection, configuration):
    """Read, stage, verify, publish, and register one configured dataset."""
    table_name = str(configuration["Derived Table Name"]).strip()
    quote_identifier(table_name)
    file_path = build_dataset_absolute_path(configuration)

    logger.info("Database load started for table %s from %s", table_name, file_path)
    dataframe = read_dataset(file_path)
    staging_name = create_staging_table(connection, table_name, dataframe)
    publish_staging_table(connection, staging_name, table_name)

    published_rows = connection.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(table_name)}"
    ).fetchone()[0]
    if published_rows != len(dataframe):
        raise RuntimeError(
            f"Published row count mismatch for {table_name}. "
            f"Expected {len(dataframe)}, found {published_rows}."
        )

    register_dataset(connection, configuration, table_name, published_rows)
    logger.info("Database table loaded: %s | rows=%s", table_name, published_rows)
    return {
        "dataset_id": configuration.get("Dataset ID"),
        "table_name": table_name,
        "file_path": file_path,
        "rows": published_rows,
    }


def clean_orphan_work_tables(connection):
    """Remove stale staging and backup tables from interrupted prior runs."""
    names = connection.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND (name LIKE '_staging_%' OR name LIKE '_backup_%')"
    ).fetchall()
    for (name,) in names:
        connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(name)}")


def load_database():
    """Load every active dataset marked Load Enabled into SQLite."""
    logger.info("Database dataset loading started")
    initialize_database_schema()

    configurations = get_load_enabled_datasets()
    if configurations.empty:
        logger.info("No active Load Enabled datasets were found")
        return DATABASE_FILE

    DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_FILE)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")

    results = []
    try:
        connection.execute("BEGIN IMMEDIATE")
        clean_orphan_work_tables(connection)

        for _, row in configurations.iterrows():
            results.append(load_one_dataset(connection, row.to_dict()))

        integrity_result = connection.execute("PRAGMA quick_check").fetchone()[0]
        if integrity_result != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {integrity_result}")

        connection.commit()
        logger.info("Database datasets loaded: %s", len(results))
        logger.info(
            "Database rows loaded: %s",
            sum(result["rows"] for result in results),
        )
        logger.info("Database dataset loading completed successfully")
        return DATABASE_FILE
    except Exception as error:
        connection.rollback()
        logger.exception("Database dataset loading failed: %s", error)
        raise
    finally:
        connection.close()


def main():
    print(load_database())


if __name__ == "__main__":
    main()
