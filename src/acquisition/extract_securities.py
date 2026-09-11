"""
SRC-002 - Securities Dataset

Objectif
---------
Enrichir les instruments financiers identifiés dans le benchmark
à l'aide de l'API OpenFIGI.

Entrée
------
data/01_raw/securities/security_candidates.csv

Sorties
--------
data/01_raw/securities/securities_master.csv
data/01_raw/securities/securities_unmatched.csv

Journalisation
---------------
logs/acquisition.log
audit/acquisition_log.xlsx
"""

import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import (
    SECURITIES,
)

from src.preparation.exchange_mapping import (
    EXCHANGE_MAPPING,
)

from src.acquisition.acquisition_logger import (
    log_acquisition,
)

from src.utils.logger import (
    logger,
)

# ============================================================================
# API CONFIGURATION
# ============================================================================

load_dotenv()

OPENFIGI_API_KEY = os.getenv(
    "OPENFIGI_API_KEY"
)

if not OPENFIGI_API_KEY:

    raise ValueError(
        "OPENFIGI_API_KEY not found in .env"
    )

HEADERS = {
    "Content-Type": "application/json",
    "X-OPENFIGI-APIKEY": OPENFIGI_API_KEY,
}

OPENFIGI_URL = (
    "https://api.openfigi.com/v3/mapping"
)

# ============================================================================
# INPUT / OUTPUT
# ============================================================================

INPUT_FILE = (
    SECURITIES
    / "security_candidates.csv"
)

OUTPUT_FILE = (
    SECURITIES
    / "securities_master.csv"
)

UNMATCHED_FILE = (
    SECURITIES
    / "securities_unmatched.csv"
)

# ============================================================================
# OUTPUT SCHEMA
# ============================================================================

BASE_RECORD = {
    "ticker": None,
    "name_source": None,
    "location": None,
    "exchange_source": None,
    "currency": None,
    "asset_class": None,
    "exchange_code": None,
    "figi": None,
    "composite_figi": None,
    "share_class_figi": None,
    "security_name": None,
    "security_type": None,
    "security_type_2": None,
    "market_sector": None,
    "security_description": None,
    "match_count": 0,
    "status": None,
}

# ============================================================================
# EXTRACTION
# ============================================================================

def extract_securities() -> Path:

    logger.info(
        "SRC-002 securities acquisition started"
    )

    try:

        # --------------------------------------------------------------------
        # INPUT VALIDATION
        # --------------------------------------------------------------------

        if not INPUT_FILE.exists():

            raise FileNotFoundError(
                f"Input file not found: "
                f"{INPUT_FILE}"
            )

        df = pd.read_csv(
            INPUT_FILE
        )

        if df.empty:

            raise ValueError(
                "Security candidates file is empty."
            )

        logger.info(
            f"Securities loaded: {len(df):,}"
        )

        logger.info(
            f"Unique exchanges: "
            f"{df['Exchange'].nunique()}"
        )

        results = []

        # --------------------------------------------------------------------
        # OPENFIGI LOOKUP
        # --------------------------------------------------------------------

        for _, row in df.iterrows():

            ticker = str(
                row["Ticker"]
            ).strip()

            exchange = row["Exchange"]

            openfigi_exchange = (
                EXCHANGE_MAPPING.get(
                    exchange
                )
            )

            record = BASE_RECORD.copy()

            record.update(
                {
                    "ticker": ticker,
                    "name_source": row["Name"],
                    "location": row["Location"],
                    "exchange_source": exchange,
                    "currency": row["Currency"],
                    "asset_class": row["Asset Class"],
                    "exchange_code": openfigi_exchange,
                }
            )

            # ------------------------------------------------------------
            # MAPPING VALIDATION
            # ------------------------------------------------------------

            if openfigi_exchange is None:

                record["status"] = (
                    "MISSING_MAPPING"
                )

                results.append(
                    record
                )

                logger.warning(
                    f"MISSING_MAPPING: "
                    f"{ticker} | "
                    f"{exchange}"
                )

                continue

            payload = [
                {
                    "idType": "TICKER",
                    "idValue": ticker,
                    "exchCode": openfigi_exchange,
                }
            ]

            try:

                # --------------------------------------------------------
                # OPENFIGI REQUEST WITH RETRY
                # --------------------------------------------------------

                data = None
                last_exception = None

                for attempt in range(3):

                    try:

                        response = requests.post(
                            OPENFIGI_URL,
                            headers=HEADERS,
                            json=payload,
                            timeout=30,
                        )

                        response.raise_for_status()

                        data = (
                            response.json()
                        )

                        break

                    except requests.exceptions.HTTPError as exc:

                        last_exception = (
                            exc
                        )

                        status_code = getattr(
                            exc.response,
                            "status_code",
                            None,
                        )

                        if status_code in [
                            429,
                            500,
                            502,
                            503,
                            504,
                        ]:

                            wait_time = (
                                attempt + 1
                            ) * 5

                            logger.warning(
                                f"RETRY "
                                f"{attempt + 1}/3 | "
                                f"{ticker} | "
                                f"HTTP {status_code} | "
                                f"waiting {wait_time}s"
                            )

                            time.sleep(
                                wait_time
                            )

                            continue

                        raise

                    except Exception as exc:

                        last_exception = (
                            exc
                        )

                        raise

                if data is None:

                    raise (
                        last_exception
                    )

                # --------------------------------------------------------
                # RESPONSE PROCESSING
                # --------------------------------------------------------

                matches = (
                    data[0].get(
                        "data",
                        [],
                    )
                    if data
                    else []
                )

                match_count = len(
                    matches
                )

                # --------------------------------------------------------
                # NO MATCH
                # --------------------------------------------------------

                if match_count == 0:

                    record.update(
                        {
                            "match_count": 0,
                            "status": "NO_MATCH",
                        }
                    )

                    results.append(
                        record
                    )

                    logger.warning(
                        f"NO_MATCH: "
                        f"{ticker}"
                    )

                    continue

                # --------------------------------------------------------
                # MATCH
                # --------------------------------------------------------

                match = matches[0]

                record.update(
                    {
                        "exchange_code":
                            match.get(
                                "exchCode"
                            ),
                        "figi":
                            match.get(
                                "figi"
                            ),
                        "composite_figi":
                            match.get(
                                "compositeFIGI"
                            ),
                        "share_class_figi":
                            match.get(
                                "shareClassFIGI"
                            ),
                        "security_name":
                            match.get(
                                "name"
                            ),
                        "security_type":
                            match.get(
                                "securityType"
                            ),
                        "security_type_2":
                            match.get(
                                "securityType2"
                            ),
                        "market_sector":
                            match.get(
                                "marketSector"
                            ),
                        "security_description":
                            match.get(
                                "securityDescription"
                            ),
                        "match_count":
                            match_count,
                        "status":
                            "MATCH",
                    }
                )

                results.append(
                    record
                )

                logger.info(
                    f"MATCH: "
                    f"{ticker} "
                    f"(matches={match_count})"
                )

            except Exception as exc:

                record.update(
                    {
                        "status": "ERROR",
                        "security_description":
                            str(exc),
                    }
                )

                results.append(
                    record
                )

                logger.error(
                    f"{ticker} | {exc}"
                )

        # --------------------------------------------------------------------
        # EXPORT
        # --------------------------------------------------------------------

        OUTPUT_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        securities_master = pd.DataFrame(
            results
        )

        securities_master.to_csv(
            OUTPUT_FILE,
            index=False,
        )

        logger.info(
            f"Security master exported: "
            f"{OUTPUT_FILE}"
        )

        # --------------------------------------------------------------------
        # UNMATCHED SECURITIES
        # --------------------------------------------------------------------

        unmatched = securities_master[
            securities_master["status"] != "MATCH"
        ].copy()

        unmatched.to_csv(
            UNMATCHED_FILE,
            index=False,
        )

        logger.info(
            f"Unmatched securities exported: "
            f"{UNMATCHED_FILE}"
        )

        # --------------------------------------------------------------------
        # METRICS
        # --------------------------------------------------------------------

        total_records = len(
            securities_master
        )

        if total_records == 0:

            raise ValueError(
                "No securities processed."
            )

        match_count = (
            securities_master["status"]
            == "MATCH"
        ).sum()

        no_match_count = (
            securities_master["status"]
            == "NO_MATCH"
        ).sum()

        missing_mapping_count = (
            securities_master["status"]
            == "MISSING_MAPPING"
        ).sum()

        error_count = (
            securities_master["status"]
            .astype(str)
            .str.startswith("ERROR")
            .sum()
        )

        coverage = (
            match_count
            / total_records
        ) * 100

        # --------------------------------------------------------------------
        # ACQUISITION LOG
        # --------------------------------------------------------------------

        status = "Success"

        if (
            error_count > 0
            or no_match_count > 0
            or missing_mapping_count > 0
        ):
            status = "Partial Success"

        notes = (
            f"Matches={match_count}; "
            f"NoMatch={no_match_count}; "
            f"MissingMapping={missing_mapping_count}; "
            f"Errors={error_count}; "
            f"Coverage={coverage:.2f}%"
        )

        run_id = log_acquisition(
            source_id="SRC-002",
            dataset="Securities Dataset",
            provider="OpenFIGI",
            period_covered="N/A",
            output_file=OUTPUT_FILE.name,
            storage_location=str(
                OUTPUT_FILE.parent
            ),
            status=status,
            records_downloaded=total_records,
            notes=notes,
        )

        logger.info(
            f"Run ID: {run_id}"
        )

        logger.info(
            "SRC-002 securities acquisition "
            "completed successfully"
        )

        return OUTPUT_FILE

    except Exception as error:

        logger.exception(
            f"SRC-002 securities acquisition "
            f"failed: {error}"
        )

        try:

            log_acquisition(
                source_id="SRC-002",
                dataset="Securities Dataset",
                provider="OpenFIGI",
                period_covered="N/A",
                output_file=OUTPUT_FILE.name,
                storage_location=str(
                    OUTPUT_FILE.parent
                ),
                status="Failed",
                records_downloaded=None,
                notes=str(error),
            )

        except Exception as log_error:

            logger.exception(
                f"Failed to write "
                f"acquisition log: "
                f"{log_error}"
            )

        raise


# ============================================================================
# MAIN
# ============================================================================

def main():

    extract_securities()


if __name__ == "__main__":

    main()