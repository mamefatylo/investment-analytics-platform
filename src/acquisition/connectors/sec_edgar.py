"""
sec_edgar.py

Source-faithful SEC EDGAR Company Facts connector.

Purpose
-------
Acquire all SEC XBRL Company Facts available for securities from the
configured upstream Dataset, without selecting or interpreting downstream
fundamental metrics.

Architecture
------------
Upstream Dataset
        |
        v
Generic API Engine
        |
        v
ConnectorContext
        |
        v
SecEdgarConnector
        |
        +--> SEC ticker-to-CIK reference
        |
        +--> SEC Company Facts API
        |
        v
Source-faithful normalized XBRL facts
        |
        v
ConnectorResult
        |
        v
Generic API Engine
        |
        v
Raw Dataset

Responsibilities
----------------
The connector is responsible for:
- validating SEC-specific Parameters;
- validating the upstream input contract;
- resolving SEC CIKs from tickers;
- requesting SEC Company Facts;
- respecting technical request pacing;
- retrying transient transport failures;
- preserving all XBRL concepts and observations returned by SEC;
- preserving securities not present in the SEC ticker reference;
- returning technical lineage and non-secret execution metadata.

The connector does not:
- read the Source Registry;
- contain business Source IDs or Dataset IDs;
- contain the Company Facts endpoint configured in the Registry;
- publish files or resolve output paths;
- select Revenue, EBIT, EBITDA, FCF, or other business metrics;
- standardize US-GAAP and IFRS concepts;
- construct processed Fundamentals.
"""

import os
import time
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from src.acquisition.connectors.base import (
    BaseConnector,
    ConnectorConfigurationError,
    ConnectorContext,
    ConnectorResponseError,
    ConnectorResult,
    ConnectorTransportError,
)
from src.utils.logger import logger

load_dotenv()

# ============================================================================
# SEC TECHNICAL REFERENCE
# ============================================================================

SEC_TICKER_REFERENCE_ENDPOINT = (
    "https://www.sec.gov/files/company_tickers.json"
)

RETRYABLE_STATUS_CODES = frozenset(
    {
        429,
        500,
        502,
        503,
        504,
    }
)


# ============================================================================
# TECHNICAL RUNTIME DEFAULTS
# ============================================================================

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_REQUEST_INTERVAL_SECONDS = 0.12


# ============================================================================
# PARAMETER CONTRACT
# ============================================================================

TICKER_COLUMN_PARAMETER = "ticker_column"

REQUIRED_PARAMETERS = (
    TICKER_COLUMN_PARAMETER,
)


# ============================================================================
# OUTPUT CONTRACT
# ============================================================================

OUTPUT_COLUMNS = (
    "input_position",
    "input_index",
    "input_ticker",
    "cik",
    "entity_name",
    "taxonomy",
    "concept",
    "label",
    "description",
    "unit",
    "start",
    "end",
    "value",
    "accession",
    "fiscal_year",
    "fiscal_period",
    "form",
    "filed",
    "frame",
    "provider_status",
)


# ============================================================================
# NORMALIZATION
# ============================================================================

def optional_text(
    value: Any,
) -> str | None:
    """Normalize optional scalar text."""

    if value is None:
        return None

    if not isinstance(
        value,
        (
            dict,
            list,
            tuple,
            set,
        ),
    ):
        try:
            if pd.isna(value):
                return None
        except (
            TypeError,
            ValueError,
        ):
            pass

    normalized = str(
        value
    ).strip()

    return normalized or None


def positive_integer(
    value: Any,
    field_name: str,
) -> int:
    """Normalize a strictly positive integer."""

    if isinstance(
        value,
        bool,
    ):
        raise ConnectorConfigurationError(
            f"{field_name} must be a positive integer."
        )

    try:
        normalized = int(
            value
        )
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be a positive integer."
        ) from error

    if normalized <= 0:
        raise ConnectorConfigurationError(
            f"{field_name} must be greater than zero."
        )

    return normalized


def positive_number(
    value: Any,
    field_name: str,
) -> float:
    """Normalize a strictly positive numeric value."""

    if isinstance(
        value,
        bool,
    ):
        raise ConnectorConfigurationError(
            f"{field_name} must be positive."
        )

    try:
        normalized = float(
            value
        )
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be numeric."
        ) from error

    if normalized <= 0:
        raise ConnectorConfigurationError(
            f"{field_name} must be greater than zero."
        )

    return normalized


def non_negative_number(
    value: Any,
    field_name: str,
) -> float:
    """Normalize a non-negative numeric value."""

    if isinstance(
        value,
        bool,
    ):
        raise ConnectorConfigurationError(
            f"{field_name} must be non-negative."
        )

    try:
        normalized = float(
            value
        )
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be numeric."
        ) from error

    if normalized < 0:
        raise ConnectorConfigurationError(
            f"{field_name} cannot be negative."
        )

    return normalized


# ============================================================================
# RUNTIME OPTIONS
# ============================================================================

def resolve_runtime_options(
    runtime_options: dict[str, Any],
) -> dict[str, Any]:
    """Resolve technical SEC execution settings."""

    if not isinstance(
        runtime_options,
        dict,
    ):
        raise ConnectorConfigurationError(
            "runtime_options must be a dictionary."
        )

    allowed_options = {
        "connect_timeout",
        "read_timeout",
        "max_attempts",
        "backoff_seconds",
        "request_interval_seconds",
    }

    unsupported = sorted(
        set(
            runtime_options
        )
        - allowed_options
    )

    if unsupported:
        raise ConnectorConfigurationError(
            "Unsupported SEC EDGAR runtime options: "
            f"{unsupported}"
        )

    return {
        "connect_timeout": positive_number(
            runtime_options.get(
                "connect_timeout",
                DEFAULT_CONNECT_TIMEOUT_SECONDS,
            ),
            "connect_timeout",
        ),
        "read_timeout": positive_number(
            runtime_options.get(
                "read_timeout",
                DEFAULT_READ_TIMEOUT_SECONDS,
            ),
            "read_timeout",
        ),
        "max_attempts": positive_integer(
            runtime_options.get(
                "max_attempts",
                DEFAULT_MAX_ATTEMPTS,
            ),
            "max_attempts",
        ),
        "backoff_seconds": positive_number(
            runtime_options.get(
                "backoff_seconds",
                DEFAULT_BACKOFF_SECONDS,
            ),
            "backoff_seconds",
        ),
        "request_interval_seconds": non_negative_number(
            runtime_options.get(
                "request_interval_seconds",
                DEFAULT_REQUEST_INTERVAL_SECONDS,
            ),
            "request_interval_seconds",
        ),
    }


def resolve_sec_user_agent() -> str:
    """
    Resolve the SEC EDGAR User-Agent from the environment.
    """

    user_agent = optional_text(
        os.getenv(
            "SEC_EDGAR_USER_AGENT"
        )
    )

    if user_agent is None:
        raise ConnectorConfigurationError(
            "SEC_EDGAR_USER_AGENT environment "
            "variable is required."
        )

    return user_agent

# ============================================================================
# INPUT CONTRACT
# ============================================================================

def validate_input_data(
    input_data: pd.DataFrame,
    ticker_column: str,
) -> None:
    """Validate the upstream Dataset used to resolve SEC filers."""

    if not isinstance(
        input_data,
        pd.DataFrame,
    ):
        raise ConnectorConfigurationError(
            "SEC EDGAR requires input_data as a pandas DataFrame."
        )

    if input_data.empty:
        raise ConnectorConfigurationError(
            "SEC EDGAR input_data is empty."
        )

    if ticker_column not in input_data.columns:
        raise ConnectorConfigurationError(
            "Configured SEC ticker column was not found in input_data: "
            f"{ticker_column}"
        )


# ============================================================================
# HTTP
# ============================================================================

def request_json(
    *,
    session: requests.Session,
    url: str,
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
) -> Any:
    """Execute one retry-aware SEC GET request and return decoded JSON."""

    timeout = (
        connect_timeout,
        read_timeout,
    )

    for attempt in range(
        1,
        max_attempts + 1,
    ):
        try:
            response = session.get(
                url,
                timeout=timeout,
            )
        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    "SEC EDGAR transport request failed after all retries."
                ) from error

            wait_seconds = (
                backoff_seconds
                * (2 ** (attempt - 1))
            )
            time.sleep(
                wait_seconds
            )
            continue

        if response.status_code in RETRYABLE_STATUS_CODES:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    "SEC EDGAR returned retryable HTTP status after all attempts: "
                    f"{response.status_code}"
                )

            retry_after = optional_text(
                response.headers.get(
                    "Retry-After"
                )
            )

            if retry_after is not None:
                try:
                    wait_seconds = max(
                        float(
                            retry_after
                        ),
                        0.0,
                    )
                except ValueError:
                    wait_seconds = (
                        backoff_seconds
                        * (2 ** (attempt - 1))
                    )
            else:
                wait_seconds = (
                    backoff_seconds
                    * (2 ** (attempt - 1))
                )

            time.sleep(
                wait_seconds
            )
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            raise ConnectorTransportError(
                "SEC EDGAR request failed with "
                f"HTTP {response.status_code}: {url}"
            ) from error

        try:
            return response.json()
        except requests.JSONDecodeError as error:
            raise ConnectorResponseError(
                "SEC EDGAR response is not valid JSON."
            ) from error

    raise ConnectorTransportError(
        "SEC EDGAR request exhausted all configured attempts."
    )


# ============================================================================
# SEC TICKER REFERENCE
# ============================================================================

def build_ticker_cik_lookup(
    payload: Any,
) -> dict[str, str]:
    """Build a case-insensitive SEC ticker-to-CIK lookup."""

    if not isinstance(
        payload,
        dict,
    ):
        raise ConnectorResponseError(
            "SEC ticker reference must be a JSON object."
        )

    lookup = {}

    for item in payload.values():
        if not isinstance(
            item,
            dict,
        ):
            continue

        ticker = optional_text(
            item.get(
                "ticker"
            )
        )
        cik_value = item.get(
            "cik_str"
        )

        if ticker is None or cik_value is None:
            continue

        try:
            cik = f"{int(cik_value):010d}"
        except (
            TypeError,
            ValueError,
        ):
            continue

        lookup[
            ticker.casefold()
        ] = cik

    if not lookup:
        raise ConnectorResponseError(
            "SEC ticker reference resolved no ticker/CIK pairs."
        )

    return lookup


# ============================================================================
# RAW NORMALIZATION
# ============================================================================

def status_record(
    *,
    input_position: int,
    input_index: Any,
    input_ticker: str | None,
    cik: str | None,
    status: str,
) -> dict[str, Any]:
    """Preserve an input security for which no Company Facts rows exist."""

    return {
        "input_position": input_position,
        "input_index": input_index,
        "input_ticker": input_ticker,
        "cik": cik,
        "entity_name": None,
        "taxonomy": None,
        "concept": None,
        "label": None,
        "description": None,
        "unit": None,
        "start": None,
        "end": None,
        "value": None,
        "accession": None,
        "fiscal_year": None,
        "fiscal_period": None,
        "form": None,
        "filed": None,
        "frame": None,
        "provider_status": status,
    }


def normalize_companyfacts(
    payload: Any,
    *,
    input_position: int,
    input_index: Any,
    input_ticker: str,
    cik: str,
) -> list[dict[str, Any]]:
    """Flatten all SEC Company Facts concepts and observations."""

    if not isinstance(
        payload,
        dict,
    ):
        raise ConnectorResponseError(
            "SEC Company Facts response must be a JSON object."
        )

    entity_name = optional_text(
        payload.get(
            "entityName"
        )
    )

    facts = payload.get(
        "facts"
    )

    if not isinstance(
        facts,
        dict,
    ):
        raise ConnectorResponseError(
            "SEC Company Facts response is missing its facts object."
        )

    records = []

    for taxonomy, concepts in facts.items():
        if not isinstance(
            concepts,
            dict,
        ):
            continue

        for concept, concept_data in concepts.items():
            if not isinstance(
                concept_data,
                dict,
            ):
                continue

            label = optional_text(
                concept_data.get(
                    "label"
                )
            )
            description = optional_text(
                concept_data.get(
                    "description"
                )
            )
            units = concept_data.get(
                "units"
            )

            if not isinstance(
                units,
                dict,
            ):
                continue

            for unit, observations in units.items():
                if not isinstance(
                    observations,
                    list,
                ):
                    continue

                for observation in observations:
                    if not isinstance(
                        observation,
                        dict,
                    ):
                        continue

                    records.append(
                        {
                            "input_position": input_position,
                            "input_index": input_index,
                            "input_ticker": input_ticker,
                            "cik": cik,
                            "entity_name": entity_name,
                            "taxonomy": taxonomy,
                            "concept": concept,
                            "label": label,
                            "description": description,
                            "unit": unit,
                            "start": observation.get("start"),
                            "end": observation.get("end"),
                            "value": observation.get("val"),
                            "accession": observation.get("accn"),
                            "fiscal_year": observation.get("fy"),
                            "fiscal_period": observation.get("fp"),
                            "form": observation.get("form"),
                            "filed": observation.get("filed"),
                            "frame": observation.get("frame"),
                            "provider_status": "success",
                        }
                    )

    if records:
        return records

    return [
        status_record(
            input_position=input_position,
            input_index=input_index,
            input_ticker=input_ticker,
            cik=cik,
            status="no_companyfacts",
        )
    ]


def build_output_frame(
    records: list[dict[str, Any]],
) -> pd.DataFrame:
    """Build the source-faithful normalized SEC output frame."""

    dataframe = pd.DataFrame(
        records,
        columns=OUTPUT_COLUMNS,
    )

    if dataframe.empty:
        raise ConnectorResponseError(
            "SEC EDGAR produced no normalized response records."
        )

    return dataframe.reset_index(
        drop=True
    )


# ============================================================================
# CONNECTOR
# ============================================================================

class SecEdgarConnector(
    BaseConnector
):
    """Source-faithful SEC EDGAR Company Facts connector."""

    SUPPORTED_RUNTIME_OPTIONS = frozenset(
        {
            "connect_timeout",
            "read_timeout",
            "max_attempts",
            "backoff_seconds",
            "request_interval_seconds",
        }
    )

    @classmethod
    def validate_parameters(
        cls,
        parameters: dict[str, Any],
    ) -> None:
        """Validate SEC-specific Dataset Parameters."""

        if not isinstance(
            parameters,
            dict,
        ):
            raise ConnectorConfigurationError(
                "SEC EDGAR Parameters must be a dictionary."
            )

        missing = [
            parameter
            for parameter in REQUIRED_PARAMETERS
            if optional_text(
                parameters.get(
                    parameter
                )
            ) is None
        ]

        if missing:
            raise ConnectorConfigurationError(
                "SEC EDGAR Parameters are missing: "
                f"{missing}"
            )

        unsupported = sorted(
            set(
                parameters
            )
            - {
                TICKER_COLUMN_PARAMETER,
            }
        )

        if unsupported:
            raise ConnectorConfigurationError(
                "Unsupported SEC EDGAR Dataset Parameters: "
                f"{unsupported}"
            )

    @classmethod
    def validate_context(
        cls,
        context: ConnectorContext,
    ) -> None:
        """Validate the complete SEC EDGAR execution context."""

        super().validate_context(
            context
        )

        endpoint = optional_text(
            context.endpoint
        )

        if endpoint is None:
            raise ConnectorConfigurationError(
                "SEC EDGAR requires Access Endpoint."
            )

        if context.input_data is None:
            raise ConnectorConfigurationError(
                "SEC EDGAR requires an upstream input Dataset."
            )

        ticker_column = optional_text(
            context.parameters.get(
                TICKER_COLUMN_PARAMETER
            )
        )

        if ticker_column is None:
            raise ConnectorConfigurationError(
                "SEC EDGAR requires ticker_column."
            )

        validate_input_data(
            context.input_data,
            ticker_column,
        )

        resolve_runtime_options(
            context.runtime_options
        )

    @classmethod
    def acquire(
        cls,
        context: ConnectorContext,
    ) -> ConnectorResult:
        """Acquire all SEC Company Facts available for the input securities."""

        cls.validate_context(
            context
        )

        endpoint = optional_text(
            context.endpoint
        )
        ticker_column = optional_text(
            context.parameters.get(
                TICKER_COLUMN_PARAMETER
            )
        )

        if endpoint is None or ticker_column is None:
            raise ConnectorConfigurationError(
                "SEC EDGAR execution configuration is incomplete."
            )

        runtime = resolve_runtime_options(
            context.runtime_options
        )
        user_agent = resolve_sec_user_agent()
        input_data = context.input_data.copy(
            deep=True
        )

        records = []
        sec_covered = 0
        ticker_not_found = 0
        companyfacts_requests = 0

        with requests.Session() as session:
            session.headers.update(
                {
                    "User-Agent": user_agent,
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                }
            )

            ticker_payload = request_json(
                session=session,
                url=SEC_TICKER_REFERENCE_ENDPOINT,
                connect_timeout=runtime["connect_timeout"],
                read_timeout=runtime["read_timeout"],
                max_attempts=runtime["max_attempts"],
                backoff_seconds=runtime["backoff_seconds"],
            )

            ticker_lookup = build_ticker_cik_lookup(
                ticker_payload
            )

            for input_position, (
                input_index,
                row,
            ) in enumerate(
                input_data.iterrows()
            ):
                ticker = optional_text(
                    row[
                        ticker_column
                    ]
                )

                if ticker is None:
                    records.append(
                        status_record(
                            input_position=input_position,
                            input_index=input_index,
                            input_ticker=None,
                            cik=None,
                            status="missing_ticker",
                        )
                    )
                    continue

                cik = ticker_lookup.get(
                    ticker.casefold()
                )

                if cik is None:
                    ticker_not_found += 1
                    records.append(
                        status_record(
                            input_position=input_position,
                            input_index=input_index,
                            input_ticker=ticker,
                            cik=None,
                            status="ticker_not_found",
                        )
                    )
                    continue

                sec_covered += 1

                companyfacts_url = (
                    endpoint.rstrip("/")
                    + "/CIK"
                    + cik
                    + ".json"
                )

                payload = request_json(
                    session=session,
                    url=companyfacts_url,
                    connect_timeout=runtime["connect_timeout"],
                    read_timeout=runtime["read_timeout"],
                    max_attempts=runtime["max_attempts"],
                    backoff_seconds=runtime["backoff_seconds"],
                )

                companyfacts_requests += 1

                records.extend(
                    normalize_companyfacts(
                        payload,
                        input_position=input_position,
                        input_index=input_index,
                        input_ticker=ticker,
                        cik=cik,
                    )
                )

                interval = runtime[
                    "request_interval_seconds"
                ]

                if interval > 0:
                    time.sleep(
                        interval
                    )

        dataframe = build_output_frame(
            records
        )

        success_records = int(
            dataframe[
                "provider_status"
            ]
            .eq(
                "success"
            )
            .sum()
        )

        metadata = {
            "input_records": len(input_data),
            "sec_covered_securities": sec_covered,
            "ticker_not_found": ticker_not_found,
            "companyfacts_requests": companyfacts_requests,
            "normalized_records": len(dataframe),
            "fact_records": success_records,
        }

        logger.info(
            "SEC EDGAR acquisition completed: "
            "Input=%s; SEC-covered=%s; "
            "TickerNotFound=%s; Requests=%s; "
            "Records=%s; Facts=%s",
            metadata["input_records"],
            metadata["sec_covered_securities"],
            metadata["ticker_not_found"],
            metadata["companyfacts_requests"],
            metadata["normalized_records"],
            metadata["fact_records"],
        )

        return ConnectorResult(
            data=dataframe,
            metadata=metadata,
        )
