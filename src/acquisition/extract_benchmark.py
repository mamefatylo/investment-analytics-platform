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
- contrôler le format du fichier téléchargé ;
- contrôler la présence de la worksheet Holdings ;
- alimenter l'Acquisition Log ;
- journaliser l'exécution technique.
"""

from datetime import datetime
from pathlib import Path

import requests

from src.acquisition.acquisition_logger import (
    log_acquisition,
)

from src.config import (
    BENCHMARK,
)

from src.utils.logger import (
    logger,
)

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

OUTPUT_FILE = (
    BENCHMARK
    / "benchmark_holdings.xls"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64)"
    )
}

EXPECTED_FORMAT = "SpreadsheetML XML"

# ============================================================================
# EXTRACTION
# ============================================================================


def extract_benchmark() -> Path:
    """
    Télécharge le benchmark iShares SDG
    et l'enregistre dans la couche RAW.
    """

    logger.info(
        "SRC-001 benchmark acquisition started"
    )

    try:

        # --------------------------------------------------------------------
        # CREATE TARGET DIRECTORY
        # --------------------------------------------------------------------

        BENCHMARK.mkdir(
            parents=True,
            exist_ok=True,
        )

        logger.info(
            "Benchmark directory verified"
        )

        # --------------------------------------------------------------------
        # DOWNLOAD FILE
        # --------------------------------------------------------------------

        response = requests.get(
            DOWNLOAD_URL,
            headers=HEADERS,
            timeout=30,
        )

        response.raise_for_status()

        logger.info(
            "Benchmark file downloaded"
        )

        # --------------------------------------------------------------------
        # CONTENT VALIDATION
        # --------------------------------------------------------------------

        if not response.content:

            raise ValueError(
                "Downloaded file is empty."
            )

        logger.info(
            "Downloaded content is not empty"
        )

        # --------------------------------------------------------------------
        # SAVE FILE
        # --------------------------------------------------------------------

        with open(
            OUTPUT_FILE,
            "wb",
        ) as file:

            file.write(
                response.content
            )

        logger.info(
            f"File saved: {OUTPUT_FILE}"
        )

        # --------------------------------------------------------------------
        # FILE CONTROLS
        # --------------------------------------------------------------------

        file_size = (
            OUTPUT_FILE
            .stat()
            .st_size
        )

        with open(
            OUTPUT_FILE,
            "rb",
        ) as file:

            content = file.read()

        # --------------------------------------------------------------------
        # FORMAT VALIDATION
        # --------------------------------------------------------------------

        if b"<ss:Workbook" not in content:

            raise ValueError(
                "Downloaded file is not a valid "
                "SpreadsheetML workbook."
            )

        # --------------------------------------------------------------------
        # HOLDINGS VALIDATION
        # --------------------------------------------------------------------

        if b"Holdings" not in content:

            raise ValueError(
                "Holdings worksheet not found "
                "in downloaded file."
            )

        workbook_type = EXPECTED_FORMAT

        logger.info(
            f"Format validated: {workbook_type}"
        )

        file_size_mb = round(
            file_size / (1024 * 1024),
            2,
        )

        # --------------------------------------------------------------------
        # ACQUISITION LOG
        # --------------------------------------------------------------------

        run_id = log_acquisition(
            source_id="SRC-001",
            dataset="Benchmark Dataset",
            provider="iShares / BlackRock",
            period_covered=datetime.today().strftime(
                "%Y-%m-%d"
            ),
            output_file=OUTPUT_FILE.name,
            storage_location=str(
                BENCHMARK
            ),
            status="Success",
            records_downloaded=None,
            notes=(
                f"Size={file_size_mb} MB | "
                f"Format={workbook_type}"
            ),
        )

        # --------------------------------------------------------------------
        # COMPLETION LOG
        # --------------------------------------------------------------------

        logger.info(
            f"Run ID: {run_id}"
        )

        logger.info(
            f"File size: {file_size_mb} MB"
        )

        logger.info(
            "Benchmark acquisition completed successfully"
        )

        return OUTPUT_FILE

    except Exception as error:

        logger.exception(
            f"SRC-001 benchmark acquisition failed: "
            f"{error}"
        )

        try:

            log_acquisition(
                source_id="SRC-001",
                dataset="Benchmark Dataset",
                provider="iShares / BlackRock",
                period_covered=datetime.today().strftime(
                    "%Y-%m-%d"
                ),
                output_file=OUTPUT_FILE.name,
                storage_location=str(
                    BENCHMARK
                ),
                status="Failed",
                records_downloaded=None,
                notes=str(error),
            )

        except Exception as log_error:

            logger.exception(
                f"Failed to write acquisition log: "
                f"{log_error}"
            )

        raise


# ============================================================================
# MAIN
# ============================================================================


def main():

    extract_benchmark()


if __name__ == "__main__":

    main()