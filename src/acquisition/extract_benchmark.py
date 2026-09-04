"""
extract_benchmark.py

Téléchargement du Benchmark Dataset.

Source :
SRC-001
iShares MSCI Global Sustainable Development Goals ETF

Objectif :

- télécharger automatiquement le fichier benchmark ;
- sauvegarder le fichier dans data/01_raw/benchmark ;
- vérifier que le fichier a bien été récupéré ;
- détecter son format ;
- alimenter l'Acquisition Log.
"""

from datetime import datetime

import requests

from src.config import BENCHMARK
from src.acquisition.acquisition_logger import log_acquisition

# ============================================================================
# SOURCE CONFIGURATION
# ============================================================================

DOWNLOAD_URL = (
    "https://www.blackrock.com/varnish-api/"
    "blk-one01-product-data/product-data/api/v1/"
    "get-fund-document"
    "?appType=PRODUCT_PAGE"
    "&appSubType=ISHARES"
    "&targetSite=us-ishares"
    "&locale=en_US"
    "&portfolioId=283378"
    "&component=fundDownload"
    "&userType=individual"
)

OUTPUT_FILE = BENCHMARK / "benchmark_holdings.xls"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64)"
    )
}

# ============================================================================
# EXTRACTION
# ============================================================================


def extract_benchmark():
    """
    Télécharge le fichier benchmark iShares SDG
    et l'enregistre dans la couche RAW.
    """

    print("=" * 60)
    print("BENCHMARK ACQUISITION")
    print("=" * 60)

    try:

        # Création du dossier cible

        BENCHMARK.mkdir(
            parents=True,
            exist_ok=True,
        )

        # Téléchargement

        response = requests.get(
            DOWNLOAD_URL,
            headers=HEADERS,
            timeout=30,
        )

        response.raise_for_status()

        # Sauvegarde locale

        with open(
            OUTPUT_FILE,
            "wb",
        ) as file:

            file.write(response.content)

        # Contrôles

        file_size = OUTPUT_FILE.stat().st_size

        with open(
            OUTPUT_FILE,
            "rb",
        ) as file:

            preview = file.read(500)

        if b"<ss:Workbook" in preview:

            workbook_type = "SpreadsheetML XML"

        else:

            workbook_type = "Unknown Format"

        file_size_mb = round(
            file_size / (1024 * 1024),
            2,
        )

        # Journalisation

        run_id = log_acquisition(
            source_id="SRC-001",
            dataset="Benchmark Dataset",
            provider="iShares / BlackRock",
            period_covered=datetime.today().strftime(
                "%Y-%m-%d"
            ),
            output_file=OUTPUT_FILE.name,
            storage_location=str(BENCHMARK),
            status="Success",
            records_downloaded=None,
            notes=(
                f"Size={file_size_mb} MB | "
                f"Format={workbook_type}"
            ),
        )

        print(f"Run ID: {run_id}")
        print(f"File saved: {OUTPUT_FILE}")
        print(f"File size: {file_size_mb} MB")
        print(f"Detected format: {workbook_type}")

        return OUTPUT_FILE

    except Exception as error:

        log_acquisition(
            source_id="SRC-001",
            dataset="Benchmark Dataset",
            provider="iShares / BlackRock",
            period_covered=datetime.today().strftime(
                "%Y-%m-%d"
            ),
            output_file=OUTPUT_FILE.name,
            storage_location=str(BENCHMARK),
            status="Failed",
            records_downloaded=None,
            notes=str(error),
        )

        raise


# ============================================================================
# EXECUTION
# ============================================================================

if __name__ == "__main__":

    extract_benchmark()