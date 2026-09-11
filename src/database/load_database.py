"""
load_database.py

Database Layer Initialization.

Objectif
---------
Charger les datasets acquis dans la base SQLite
de la plateforme.

Sources
--------
data/01_raw/securities/security_candidates.csv
data/01_raw/securities/securities_master.csv
data/01_raw/securities/securities_unmatched.csv

Database
--------
database/sdg_investment.db

Tables
-------
security_candidates
securities_master
securities_unmatched
"""

import sqlite3
from pathlib import Path

import pandas as pd

from src.config import (
    DATABASE_DIR,
    DATABASE_FILE,
    SECURITIES,
)

from src.utils.logger import logger

# ============================================================================
# INPUT FILES
# ============================================================================

SECURITY_CANDIDATES_FILE = (
    SECURITIES
    / "security_candidates.csv"
)

SECURITIES_MASTER_FILE = (
    SECURITIES
    / "securities_master.csv"
)

SECURITIES_UNMATCHED_FILE = (
    SECURITIES
    / "securities_unmatched.csv"
)

# ============================================================================
# DATABASE LOADER
# ============================================================================


def load_database() -> Path:
    """
    Charge les datasets acquis dans SQLite.
    """

    logger.info(
        "Database initialization started"
    )

    try:

        # --------------------------------------------------------------------
        # VALIDATE INPUT FILES
        # --------------------------------------------------------------------

        required_files = [
            SECURITY_CANDIDATES_FILE,
            SECURITIES_MASTER_FILE,
            SECURITIES_UNMATCHED_FILE,
        ]

        for file in required_files:

            if not file.exists():

                raise FileNotFoundError(
                    f"Missing input file: {file}"
                )

        logger.info(
            "All input files validated"
        )

        # --------------------------------------------------------------------
        # CREATE DATABASE DIRECTORY
        # --------------------------------------------------------------------

        DATABASE_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        # --------------------------------------------------------------------
        # LOAD DATASETS
        # --------------------------------------------------------------------

        security_candidates = pd.read_csv(
            SECURITY_CANDIDATES_FILE
        )

        securities_master = pd.read_csv(
            SECURITIES_MASTER_FILE
        )

        securities_unmatched = pd.read_csv(
            SECURITIES_UNMATCHED_FILE
        )

        logger.info(
            "Datasets loaded successfully"
        )

        # --------------------------------------------------------------------
        # SQLITE CONNECTION
        # --------------------------------------------------------------------

        connection = sqlite3.connect(
            DATABASE_FILE
        )

        # --------------------------------------------------------------------
        # LOAD TABLES
        # --------------------------------------------------------------------

        security_candidates.to_sql(
            "security_candidates",
            connection,
            if_exists="replace",
            index=False,
        )

        securities_master.to_sql(
            "securities_master",
            connection,
            if_exists="replace",
            index=False,
        )

        securities_unmatched.to_sql(
            "securities_unmatched",
            connection,
            if_exists="replace",
            index=False,
        )

        logger.info(
            "Tables loaded successfully"
        )

        # --------------------------------------------------------------------
        # VALIDATION
        # --------------------------------------------------------------------

        cursor = connection.cursor()

        tables = cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            """
        ).fetchall()

        logger.info(
            f"Tables available: {tables}"
        )

        connection.close()

        logger.info(
            f"Database created: "
            f"{DATABASE_FILE}"
        )

        logger.info(
            "Database initialization "
            "completed successfully"
        )

        return DATABASE_FILE

    except Exception as error:

        logger.exception(
            f"Database initialization failed: "
            f"{error}"
        )

        raise


# ============================================================================
# MAIN
# ============================================================================


def main():

    load_database()


if __name__ == "__main__":

    main()