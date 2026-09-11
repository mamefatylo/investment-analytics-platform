"""
securities_candidates.py

Construction du Security Candidates Dataset.

Source :
SRC-001 Benchmark Dataset

Objectif :

- extraire la worksheet Holdings ;
- reconstruire la table des positions ;
- conserver les positions Equity ;
- supprimer les doublons ;
- produire security_candidates.csv.
"""

from pathlib import Path
import re

import pandas as pd

from src.config import (
    BENCHMARK,
    SECURITIES,
)

from src.utils.logger import (
    logger,
)

# ============================================================================
# FILES
# ============================================================================

INPUT_FILE = (
    BENCHMARK
    / "benchmark_holdings.xls"
)

OUTPUT_FILE = (
    SECURITIES
    / "security_candidates.csv"
)

# ============================================================================
# HEADER DEFINITION
# ============================================================================

HEADER = [
    "Ticker",
    "Name",
    "Sector",
    "Asset Class",
    "Market Value",
    "Weight (%)",
    "Notional Value",
    "Quantity",
    "Price",
    "Location",
    "Exchange",
    "Currency",
    "FX Rate",
    "Accrual Date",
]

# ============================================================================
# PROCESSING
# ============================================================================


def build_security_candidates() -> Path:
    """
    Construit le Security Candidates Dataset
    à partir de la worksheet Holdings.
    """

    logger.info(
        "Security candidates extraction started"
    )

    try:

        # --------------------------------------------------------------------
        # FILE VALIDATION
        # --------------------------------------------------------------------

        if not INPUT_FILE.exists():

            raise FileNotFoundError(
                f"Benchmark file not found: "
                f"{INPUT_FILE}"
            )

        logger.info(
            f"Benchmark file found: {INPUT_FILE}"
        )

        # --------------------------------------------------------------------
        # LOAD FILE
        # --------------------------------------------------------------------

        with open(
            INPUT_FILE,
            "r",
            encoding="utf-8",
            errors="ignore",
        ) as file:

            content = file.read()

        if not content:

            raise ValueError(
                "Benchmark file is empty."
            )

        logger.info(
            "Benchmark file loaded"
        )

        # --------------------------------------------------------------------
        # HOLDINGS EXTRACTION
        # --------------------------------------------------------------------

        match = re.search(
            r'<ss:Worksheet ss:Name="Holdings">(.*?)</ss:Worksheet>',
            content,
            re.DOTALL,
        )

        if not match:

            raise ValueError(
                "Holdings worksheet not found."
            )

        holdings_content = match.group(1)

        logger.info(
            "Holdings worksheet extracted"
        )

        # --------------------------------------------------------------------
        # CELL EXTRACTION
        # --------------------------------------------------------------------

        values = re.findall(
            r"<ss:Data[^>]*>(.*?)</ss:Data>",
            holdings_content,
            re.DOTALL,
        )

        if not values:

            raise ValueError(
                "No worksheet data extracted."
            )

        logger.info(
            f"Extracted {len(values):,} cell values"
        )

        # --------------------------------------------------------------------
        # HEADER LOCATION
        # --------------------------------------------------------------------

        header_index = None

        for i in range(len(values)):

            if (
                values[
                    i : i + len(HEADER)
                ]
                == HEADER
            ):

                header_index = i
                break

        if header_index is None:

            raise ValueError(
                "Header not found."
            )

        logger.info(
            f"Header found at position "
            f"{header_index}"
        )

        # --------------------------------------------------------------------
        # DATAFRAME RECONSTRUCTION
        # --------------------------------------------------------------------

        data_values = values[
            header_index + len(HEADER):
        ]

        records = []

        for i in range(
            0,
            len(data_values),
            len(HEADER),
        ):

            row = data_values[
                i : i + len(HEADER)
            ]

            if len(row) != len(HEADER):
                continue

            records.append(row)

        df = pd.DataFrame(
            records,
            columns=HEADER,
        )

        if df.empty:

            raise ValueError(
                "No holdings records reconstructed."
            )

        logger.info(
            f"Worksheet reconstructed: "
            f"{len(df):,} rows | "
            f"{len(df.columns)} columns"
        )

        # --------------------------------------------------------------------
        # EQUITY FILTER
        # --------------------------------------------------------------------

        equities = df[
            df["Asset Class"] == "Equity"
        ].copy()

        if equities.empty:

            raise ValueError(
                "No equity positions found."
            )

        logger.info(
            f"Equity positions retained: "
            f"{len(equities):,}"
        )

        # --------------------------------------------------------------------
        # ATTRIBUTE SELECTION
        # --------------------------------------------------------------------

        equities = equities[
            [
                "Ticker",
                "Name",
                "Location",
                "Exchange",
                "Currency",
                "Asset Class",
            ]
        ]

        logger.info(
            "Security candidate attributes selected"
        )

        # --------------------------------------------------------------------
        # DEDUPLICATION
        # --------------------------------------------------------------------

        equities = equities.drop_duplicates(
            subset=[
                "Ticker",
                "Exchange",
            ]
        )

        logger.info(
            f"Unique securities: "
            f"{len(equities):,}"
        )

        # --------------------------------------------------------------------
        # EXPORT
        # --------------------------------------------------------------------

        SECURITIES.mkdir(
            parents=True,
            exist_ok=True,
        )

        equities.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        logger.info(
            f"Security candidates exported: "
            f"{OUTPUT_FILE}"
        )

        logger.info(
            "Security candidates extraction "
            "completed successfully"
        )

        return OUTPUT_FILE

    except Exception as error:

        logger.exception(
            "Security candidates extraction "
            f"failed: {error}"
        )

        raise


# ============================================================================
# MAIN
# ============================================================================


def main():

    build_security_candidates()


if __name__ == "__main__":

    main()