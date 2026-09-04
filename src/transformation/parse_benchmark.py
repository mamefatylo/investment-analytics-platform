"""
parse_benchmark.py

Analyse du fichier benchmark téléchargé depuis iShares.

Objectif :

- lire le fichier SpreadsheetML XML ;
- identifier les feuilles disponibles ;
- documenter sa structure ;
- préparer l'extraction future des holdings.

INPUT

data/01_raw/benchmark/benchmark_holdings.xls

OUTPUT

data/02_processed/benchmark/benchmark_metadata.csv
"""

import xml.etree.ElementTree as ET

import pandas as pd

from src.config import (
    BENCHMARK,
    PROCESSED_DIR,
)

# ============================================================================
# PATHS
# ============================================================================

INPUT_FILE = BENCHMARK / "benchmark_holdings.xls"

PROCESSED_BENCHMARK = (
    PROCESSED_DIR / "benchmark"
)

METADATA_FILE = (
    PROCESSED_BENCHMARK /
    "benchmark_metadata.csv"
)

# ============================================================================
# HELPERS
# ============================================================================


def get_worksheets(root):
    """
    Retourne la liste des feuilles présentes
    dans le workbook SpreadsheetML.
    """

    worksheets = []

    for element in root.iter():

        if "Worksheet" in element.tag:

            worksheet_name = element.attrib.get(
                "{urn:schemas-microsoft-com:office:spreadsheet}Name"
            )

            worksheets.append(
                worksheet_name
            )

    return worksheets


# ============================================================================
# MAIN FUNCTION
# ============================================================================


def parse_benchmark():

    print("=" * 60)
    print("BENCHMARK PARSING")
    print("=" * 60)

    if not INPUT_FILE.exists():

        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    PROCESSED_BENCHMARK.mkdir(
        parents=True,
        exist_ok=True,
    )

    tree = ET.parse(INPUT_FILE)

    root = tree.getroot()

    worksheets = get_worksheets(root)

    print(
        f"Worksheets detected: "
        f"{len(worksheets)}"
    )

    for worksheet in worksheets:

        print(
            f" - {worksheet}"
        )

    metadata = pd.DataFrame({
        "Worksheet": worksheets
    })

    metadata.to_csv(
        METADATA_FILE,
        index=False,
    )

    print(
        f"\nMetadata exported to:"
    )

    print(METADATA_FILE)

    return metadata


# ============================================================================
# EXECUTION
# ============================================================================

if __name__ == "__main__":

    parse_benchmark()