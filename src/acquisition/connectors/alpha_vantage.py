"""
alpha_vantage.py

Source-faithful Alpha Vantage time-series connector with provider-native
symbol resolution.

Purpose
-------
Acquire Alpha Vantage market-data responses without applying downstream
investment or analytical transformations.

Resolution strategy
-------------------
For each upstream instrument:
1. attempt the configured symbol directly;
2. if Alpha Vantage rejects the symbol, call SYMBOL_SEARCH using available
   instrument identity fields;
3. resolve only when one search candidate is clearly supported by provider
   evidence;
4. preserve all provider search candidates in execution metadata for audit;
5. preserve ambiguous and unresolved instruments as technical status rows;
6. acquire the configured time series only for resolved symbols.

The generic acquisition engine, Source Registry reader, output publishing,
logging, SQLite loading, returns, currency conversion and investment logic
remain outside this connector.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from src.acquisition.connectors.base import (
    BaseConnector,
    ConnectorAuthenticationError,
    ConnectorConfigurationError,
    ConnectorContext,
    ConnectorResponseError,
    ConnectorResult,
    ConnectorTransportError,
)
from src.utils.logger import logger


# ============================================================================
# AUTHENTICATION
# ============================================================================

API_KEY_ENVIRONMENT_VARIABLE = "ALPHA_VANTAGE_API_KEY"
API_KEY_QUERY_PARAMETER = "apikey"


# ============================================================================
# DATASET PARAMETERS
# ============================================================================

FUNCTION_PARAMETER = "function"
SYMBOL_COLUMN_PARAMETER = "symbol_column"
NAME_COLUMN_PARAMETER = "name_column"
LOCATION_COLUMN_PARAMETER = "location_column"
EXCHANGE_COLUMN_PARAMETER = "exchange_column"
OUTPUT_SIZE_PARAMETER = "outputsize"
DATATYPE_PARAMETER = "datatype"

REQUIRED_PARAMETERS = {
    FUNCTION_PARAMETER,
    SYMBOL_COLUMN_PARAMETER,
}

INPUT_COLUMN_PARAMETERS = {
    SYMBOL_COLUMN_PARAMETER,
    NAME_COLUMN_PARAMETER,
    LOCATION_COLUMN_PARAMETER,
    EXCHANGE_COLUMN_PARAMETER,
}

RESERVED_PARAMETERS = {
    API_KEY_QUERY_PARAMETER,
    "symbol",
    "keywords",
}

REQUIRED_DATATYPE = "json"
SEARCH_FUNCTION = "SYMBOL_SEARCH"


# ============================================================================
# PROVIDER RESPONSE CONTRACT
# ============================================================================

METADATA_KEY = "Meta Data"
SEARCH_RESULTS_KEY = "bestMatches"

PROVIDER_ERROR_KEYS = (
    "Error Message",
    "Information",
    "Note",
)


# ============================================================================
# TECHNICAL DEFAULTS
# ============================================================================

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_REQUEST_DELAY_SECONDS = 0.0
DEFAULT_USER_AGENT = "investment-data-pipeline/1.0"

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


# ============================================================================
# RESOLUTION STATUS
# ============================================================================

STATUS_DIRECT = "DIRECT"
STATUS_SEARCH_RESOLVED = "SEARCH_RESOLVED"
STATUS_SEARCH_AMBIGUOUS = "SEARCH_AMBIGUOUS"
STATUS_SEARCH_NOT_FOUND = "SEARCH_NOT_FOUND"


@dataclass(frozen=True)
class Instrument:
    input_position: int
    input_index: Any
    ticker: str
    name: str | None
    location: str | None
    exchange: str | None


@dataclass(frozen=True)
class SymbolResolution:
    status: str
    resolved_symbol: str | None
    match_score: str | None = None
    provider_name: str | None = None
    provider_region: str | None = None
    provider_currency: str | None = None
    search_keyword: str | None = None
    candidate_count: int = 0
    search_candidates: tuple[dict[str, str | None], ...] = ()


# ============================================================================
# NORMALIZATION
# ============================================================================

def optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, (dict, list, tuple, set)):
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
    normalized = str(value).strip()
    return normalized or None


def positive_integer(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ConnectorConfigurationError(f"{field_name} must be a positive integer.")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as error:
        raise ConnectorConfigurationError(f"{field_name} must be a positive integer.") from error
    if normalized <= 0:
        raise ConnectorConfigurationError(f"{field_name} must be greater than zero.")
    return normalized


def non_negative_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ConnectorConfigurationError(f"{field_name} must be non-negative.")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as error:
        raise ConnectorConfigurationError(f"{field_name} must be numeric.") from error
    if normalized < 0:
        raise ConnectorConfigurationError(f"{field_name} cannot be negative.")
    return normalized


def positive_number(value: Any, field_name: str) -> float:
    normalized = non_negative_number(value, field_name)
    if normalized <= 0:
        raise ConnectorConfigurationError(f"{field_name} must be greater than zero.")
    return normalized


def normalize_provider_field_name(field_name: Any) -> str:
    normalized = optional_text(field_name)
    if normalized is None:
        raise ConnectorResponseError("Alpha Vantage observation contains an empty field name.")
    if "." in normalized:
        prefix, remainder = normalized.split(".", 1)
        if prefix.strip().isdigit():
            normalized = remainder.strip()
    normalized = normalized.strip().lower().replace(" ", "_").replace("-", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    normalized = normalized.strip("_")
    if not normalized:
        raise ConnectorResponseError(f"Unable to normalize Alpha Vantage field name: {field_name}")
    return normalized


# ============================================================================
# AUTHENTICATION
# ============================================================================

def load_api_key() -> str:
    load_dotenv()
    api_key = optional_text(os.getenv(API_KEY_ENVIRONMENT_VARIABLE))
    if api_key is None:
        raise ConnectorAuthenticationError(
            f"{API_KEY_ENVIRONMENT_VARIABLE} is missing from the environment."
        )
    return api_key


# ============================================================================
# INPUT INSTRUMENTS
# ============================================================================

def _configured_column(parameters: dict[str, Any], key: str) -> str | None:
    return optional_text(parameters.get(key))


def normalize_instruments(
    input_data: pd.DataFrame,
    parameters: dict[str, Any],
) -> list[Instrument]:
    if not isinstance(input_data, pd.DataFrame):
        raise ConnectorConfigurationError("Alpha Vantage requires input_data as a pandas DataFrame.")
    if input_data.empty:
        raise ConnectorConfigurationError("Alpha Vantage input_data is empty.")

    symbol_column = _configured_column(parameters, SYMBOL_COLUMN_PARAMETER)
    if symbol_column is None:
        raise ConnectorConfigurationError("Alpha Vantage Parameters require symbol_column.")

    configured = {
        "ticker": symbol_column,
        "name": _configured_column(parameters, NAME_COLUMN_PARAMETER),
        "location": _configured_column(parameters, LOCATION_COLUMN_PARAMETER),
        "exchange": _configured_column(parameters, EXCHANGE_COLUMN_PARAMETER),
    }

    missing = sorted(
        column for column in configured.values()
        if column is not None and column not in input_data.columns
    )
    if missing:
        raise ConnectorConfigurationError(
            f"Configured Alpha Vantage input columns were not found: {missing}"
        )

    instruments: list[Instrument] = []
    for input_position, (input_index, row) in enumerate(input_data.iterrows()):
        ticker = optional_text(row[symbol_column])
        if ticker is None:
            raise ConnectorConfigurationError(
                f"Alpha Vantage input contains an empty symbol at input index {input_index}."
            )
        instruments.append(
            Instrument(
                input_position=input_position,
                input_index=input_index,
                ticker=ticker,
                name=optional_text(row[configured["name"]]) if configured["name"] else None,
                location=optional_text(row[configured["location"]]) if configured["location"] else None,
                exchange=optional_text(row[configured["exchange"]]) if configured["exchange"] else None,
            )
        )

    comparison_keys = [item.ticker.casefold() for item in instruments]
    if len(comparison_keys) != len(set(comparison_keys)):
        duplicates = (
            pd.Series(comparison_keys)[pd.Series(comparison_keys).duplicated(keep=False)]
            .drop_duplicates().tolist()
        )
        raise ConnectorConfigurationError(f"Alpha Vantage input contains duplicate symbols: {duplicates}")

    return instruments


# ============================================================================
# DATASET REQUEST PARAMETERS
# ============================================================================

def build_request_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(parameters, dict):
        raise ConnectorConfigurationError("Alpha Vantage Parameters must be a dictionary.")

    function = optional_text(parameters.get(FUNCTION_PARAMETER))
    symbol_column = optional_text(parameters.get(SYMBOL_COLUMN_PARAMETER))
    if function is None:
        raise ConnectorConfigurationError("Alpha Vantage Parameters require function.")
    if symbol_column is None:
        raise ConnectorConfigurationError("Alpha Vantage Parameters require symbol_column.")

    forbidden = sorted(key for key in RESERVED_PARAMETERS if key in parameters)
    if forbidden:
        raise ConnectorConfigurationError(
            f"Alpha Vantage Parameters must not contain connector-managed values: {forbidden}"
        )

    configured_datatype = optional_text(parameters.get(DATATYPE_PARAMETER))
    if configured_datatype is not None and configured_datatype.casefold() != REQUIRED_DATATYPE:
        raise ConnectorConfigurationError("Alpha Vantage connector requires datatype=json.")

    request_parameters = {
        key: value
        for key, value in parameters.items()
        if key not in INPUT_COLUMN_PARAMETERS | {DATATYPE_PARAMETER}
    }
    request_parameters[FUNCTION_PARAMETER] = function
    request_parameters[DATATYPE_PARAMETER] = REQUIRED_DATATYPE
    return request_parameters


# ============================================================================
# RUNTIME OPTIONS
# ============================================================================

def resolve_runtime_options(runtime_options: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(runtime_options, dict):
        raise ConnectorConfigurationError("runtime_options must be a dictionary.")

    allowed_options = {
        "connect_timeout",
        "read_timeout",
        "max_attempts",
        "backoff_seconds",
        "request_delay_seconds",
        "user_agent",
    }
    unsupported = sorted(set(runtime_options) - allowed_options)
    if unsupported:
        raise ConnectorConfigurationError(
            f"Unsupported Alpha Vantage runtime options: {unsupported}"
        )

    user_agent = optional_text(runtime_options.get("user_agent", DEFAULT_USER_AGENT))
    if user_agent is None:
        raise ConnectorConfigurationError("user_agent cannot be empty.")

    return {
        "connect_timeout": positive_number(
            runtime_options.get("connect_timeout", DEFAULT_CONNECT_TIMEOUT_SECONDS),
            "connect_timeout",
        ),
        "read_timeout": positive_number(
            runtime_options.get("read_timeout", DEFAULT_READ_TIMEOUT_SECONDS),
            "read_timeout",
        ),
        "max_attempts": positive_integer(
            runtime_options.get("max_attempts", DEFAULT_MAX_ATTEMPTS),
            "max_attempts",
        ),
        "backoff_seconds": positive_number(
            runtime_options.get("backoff_seconds", DEFAULT_BACKOFF_SECONDS),
            "backoff_seconds",
        ),
        "request_delay_seconds": non_negative_number(
            runtime_options.get("request_delay_seconds", DEFAULT_REQUEST_DELAY_SECONDS),
            "request_delay_seconds",
        ),
        "user_agent": user_agent,
    }


# ============================================================================
# HTTP
# ============================================================================

def resolve_retry_delay(
    response: requests.Response,
    *,
    attempt: int,
    backoff_seconds: float,
) -> float:
    retry_after = optional_text(response.headers.get("Retry-After"))
    if retry_after is not None:
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            pass
    return backoff_seconds * (2 ** (attempt - 1))


def extract_provider_message(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary.")
    for key in PROVIDER_ERROR_KEYS:
        message = optional_text(payload.get(key))
        if message is not None:
            return key, message
    return None, None


def request_json(
    *,
    session: requests.Session,
    endpoint: str,
    query: dict[str, Any],
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
    allow_provider_error: bool = False,
) -> dict[str, Any]:
    normalized_endpoint = optional_text(endpoint)
    if normalized_endpoint is None:
        raise ConnectorConfigurationError("Alpha Vantage endpoint cannot be empty.")
    if not isinstance(query, dict):
        raise TypeError("query must be a dictionary.")

    timeout = (connect_timeout, read_timeout)
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(normalized_endpoint, params=query, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as error:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    "Alpha Vantage transport request failed after all configured attempts."
                ) from error
            wait_seconds = backoff_seconds * (2 ** (attempt - 1))
            time.sleep(wait_seconds)
            continue

        if response.status_code in RETRYABLE_STATUS_CODES:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    f"Alpha Vantage returned retryable HTTP status after all attempts: {response.status_code}"
                )
            time.sleep(resolve_retry_delay(
                response,
                attempt=attempt,
                backoff_seconds=backoff_seconds,
            ))
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            if response.status_code in {401, 403}:
                raise ConnectorAuthenticationError("Alpha Vantage authentication was rejected.") from error
            raise ConnectorTransportError(
                f"Alpha Vantage request failed with HTTP {response.status_code}."
            ) from error

        try:
            payload = response.json()
        except ValueError as error:
            raise ConnectorResponseError("Alpha Vantage response is not valid JSON.") from error
        if not isinstance(payload, dict):
            raise ConnectorResponseError("Alpha Vantage response must be a JSON object.")

        message_type, provider_message = extract_provider_message(payload)
        if provider_message is None:
            return payload

        if message_type == "Error Message":
            if allow_provider_error:
                return payload
            raise ConnectorResponseError(
                f"Alpha Vantage returned an API error: {provider_message}"
            )

        if message_type in {"Information", "Note"}:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    f"Alpha Vantage returned {message_type} after all configured attempts: {provider_message}"
                )
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))
            continue

    raise ConnectorTransportError("Alpha Vantage request exhausted all configured attempts.")


def maybe_wait(runtime: dict[str, Any]) -> None:
    delay = runtime["request_delay_seconds"]
    if delay > 0:
        time.sleep(delay)


# ============================================================================
# TIME-SERIES DISCOVERY / NORMALIZATION
# ============================================================================

def find_time_series_key(payload: dict[str, Any]) -> str:
    candidates = []
    for key, value in payload.items():
        if key == METADATA_KEY or not isinstance(value, dict) or not value:
            continue
        child_values = list(value.values())
        if child_values and all(isinstance(child, dict) for child in child_values):
            candidates.append(key)
    if not candidates:
        raise ConnectorResponseError(
            "Unable to discover an Alpha Vantage time-series container. "
            f"Top-level keys: {sorted(str(key) for key in payload)}"
        )
    if len(candidates) > 1:
        raise ConnectorResponseError(
            f"Alpha Vantage response contains multiple possible time-series containers: {sorted(candidates)}"
        )
    return candidates[0]


def validate_response_contract(
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    message_type, provider_message = extract_provider_message(payload)
    if provider_message is not None:
        raise ConnectorResponseError(
            f"Alpha Vantage response contains {message_type}: {provider_message}"
        )
    metadata = payload.get(METADATA_KEY, {}) or {}
    if not isinstance(metadata, dict):
        raise ConnectorResponseError("Alpha Vantage Meta Data must be a JSON object.")
    time_series_key = find_time_series_key(payload)
    time_series = payload[time_series_key]
    if not time_series:
        raise ConnectorResponseError("Alpha Vantage time-series container is empty.")
    return time_series_key, metadata, time_series


def normalize_time_series(
    *,
    instrument: Instrument,
    resolution: SymbolResolution,
    payload: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    time_series_key, provider_metadata, time_series = validate_response_contract(payload)
    records: list[dict[str, Any]] = []
    schema: tuple[str, ...] | None = None

    for observation_position, (timestamp, observation) in enumerate(time_series.items()):
        normalized_timestamp = optional_text(timestamp)
        if normalized_timestamp is None or not isinstance(observation, dict):
            raise ConnectorResponseError("Invalid Alpha Vantage time-series observation.")

        normalized_observation: dict[str, Any] = {}
        for provider_field, value in observation.items():
            normalized_field = normalize_provider_field_name(provider_field)
            if normalized_field in normalized_observation:
                raise ConnectorResponseError(
                    f"Alpha Vantage fields collapse to duplicate name: {normalized_field}"
                )
            normalized_observation[normalized_field] = value

        current_schema = tuple(sorted(normalized_observation))
        if schema is None:
            schema = current_schema
        elif current_schema != schema:
            raise ConnectorResponseError(
                "Alpha Vantage observation schema changed within one response."
            )

        record = _lineage_record(instrument, resolution)
        record.update({
            "observation_position": observation_position,
            "timestamp": normalized_timestamp,
        })
        record.update(normalized_observation)
        records.append(record)

    if not records:
        raise ConnectorResponseError("Alpha Vantage response contains no observations.")

    dataframe = pd.DataFrame(records).reset_index(drop=True)
    return dataframe, {
        "time_series_key": time_series_key,
        "provider_metadata": provider_metadata.copy(),
        "observation_records": len(dataframe),
    }


# ============================================================================
# SYMBOL SEARCH / RESOLUTION
# ============================================================================

def normalize_search_results(payload: dict[str, Any]) -> list[dict[str, str | None]]:
    message_type, provider_message = extract_provider_message(payload)
    if provider_message is not None:
        raise ConnectorResponseError(
            f"Alpha Vantage search returned {message_type}: {provider_message}"
        )
    matches = payload.get(SEARCH_RESULTS_KEY, [])
    if not isinstance(matches, list):
        raise ConnectorResponseError("Alpha Vantage bestMatches must be a list.")

    results: list[dict[str, str | None]] = []
    for item in matches:
        if not isinstance(item, dict):
            continue
        symbol = optional_text(item.get("1. symbol"))
        if symbol is None:
            continue
        results.append({
            "symbol": symbol,
            "name": optional_text(item.get("2. name")),
            "type": optional_text(item.get("3. type")),
            "region": optional_text(item.get("4. region")),
            "market_open": optional_text(item.get("5. marketOpen")),
            "market_close": optional_text(item.get("6. marketClose")),
            "timezone": optional_text(item.get("7. timezone")),
            "currency": optional_text(item.get("8. currency")),
            "match_score": optional_text(item.get("9. matchScore")),
        })
    return results


def _words(value: str | None) -> set[str]:
    if value is None:
        return set()
    cleaned = "".join(char if char.isalnum() else " " for char in value.casefold())
    return {word for word in cleaned.split() if len(word) > 1}


def _candidate_rank(instrument: Instrument, candidate: dict[str, str | None]) -> tuple[int, float]:
    score = 0
    candidate_symbol = optional_text(candidate.get("symbol")) or ""
    base_symbol = candidate_symbol.split(".", 1)[0]
    if base_symbol.casefold() == instrument.ticker.casefold():
        score += 6

    input_name_words = _words(instrument.name)
    provider_name_words = _words(candidate.get("name"))
    overlap = len(input_name_words & provider_name_words)
    score += min(overlap, 4)

    location_words = _words(instrument.location)
    region_words = _words(candidate.get("region"))
    if location_words & region_words:
        score += 3

    try:
        provider_score = float(candidate.get("match_score") or 0.0)
    except (TypeError, ValueError):
        provider_score = 0.0

    return score, provider_score


def choose_search_candidate(
    instrument: Instrument,
    candidates: list[dict[str, str | None]],
    *,
    search_keyword: str,
) -> SymbolResolution:
    if not candidates:
        return SymbolResolution(
            status=STATUS_SEARCH_NOT_FOUND,
            resolved_symbol=None,
            search_keyword=search_keyword,
            candidate_count=0,
            search_candidates=(),
        )

    ranked = sorted(
        ((candidate, *_candidate_rank(instrument, candidate)) for candidate in candidates),
        key=lambda item: (item[1], item[2]),
        reverse=True,
    )
    best, best_evidence, best_provider_score = ranked[0]
    second_evidence = ranked[1][1] if len(ranked) > 1 else -1

    # Resolve only with meaningful identity evidence and a unique evidence lead.
    if best_evidence < 6 or best_evidence == second_evidence:
        return SymbolResolution(
            status=STATUS_SEARCH_AMBIGUOUS,
            resolved_symbol=None,
            match_score=best.get("match_score"),
            provider_name=best.get("name"),
            provider_region=best.get("region"),
            provider_currency=best.get("currency"),
            search_keyword=search_keyword,
            candidate_count=len(candidates),
            search_candidates=tuple(dict(candidate) for candidate in candidates),
        )

    return SymbolResolution(
        status=STATUS_SEARCH_RESOLVED,
        resolved_symbol=best["symbol"],
        match_score=best.get("match_score"),
        provider_name=best.get("name"),
        provider_region=best.get("region"),
        provider_currency=best.get("currency"),
        search_keyword=search_keyword,
        candidate_count=len(candidates),
        search_candidates=tuple(dict(candidate) for candidate in candidates),
    )


def search_symbol(
    *,
    session: requests.Session,
    endpoint: str,
    api_key: str,
    instrument: Instrument,
    runtime: dict[str, Any],
) -> tuple[SymbolResolution, int]:
    # Company/security name generally disambiguates international tickers better
    # than ticker-only search; fall back to ticker when name is unavailable.
    keyword = instrument.name or instrument.ticker
    query = {
        "function": SEARCH_FUNCTION,
        "keywords": keyword,
        "datatype": REQUIRED_DATATYPE,
        API_KEY_QUERY_PARAMETER: api_key,
    }
    payload = request_json(
        session=session,
        endpoint=endpoint,
        query=query,
        connect_timeout=runtime["connect_timeout"],
        read_timeout=runtime["read_timeout"],
        max_attempts=runtime["max_attempts"],
        backoff_seconds=runtime["backoff_seconds"],
    )
    candidates = normalize_search_results(payload)
    return choose_search_candidate(
        instrument,
        candidates,
        search_keyword=keyword,
    ), 1


# ============================================================================
# ACQUISITION HELPERS
# ============================================================================

def _lineage_record(
    instrument: Instrument,
    resolution: SymbolResolution,
) -> dict[str, Any]:
    return {
        "input_position": instrument.input_position,
        "input_index": instrument.input_index,
        "input_ticker": instrument.ticker,
        "input_name": instrument.name,
        "input_location": instrument.location,
        "input_exchange": instrument.exchange,
        "resolved_symbol": resolution.resolved_symbol,
        "symbol_resolution_status": resolution.status,
        "match_score": resolution.match_score,
        "provider_name": resolution.provider_name,
        "provider_region": resolution.provider_region,
        "provider_currency": resolution.provider_currency,
        "search_keyword": resolution.search_keyword,
        "search_candidate_count": resolution.candidate_count,
    }


def status_frame(
    instrument: Instrument,
    resolution: SymbolResolution,
) -> pd.DataFrame:
    record = _lineage_record(instrument, resolution)
    record.update({"observation_position": None, "timestamp": None})
    return pd.DataFrame([record])


def acquire_time_series(
    *,
    session: requests.Session,
    endpoint: str,
    api_key: str,
    instrument: Instrument,
    resolution: SymbolResolution,
    request_parameters: dict[str, Any],
    runtime: dict[str, Any],
    allow_invalid_symbol: bool = False,
) -> tuple[pd.DataFrame | None, dict[str, Any], bool]:
    symbol = resolution.resolved_symbol
    if symbol is None:
        raise ConnectorConfigurationError("Resolved Alpha Vantage symbol cannot be empty.")

    query = dict(request_parameters)
    query["symbol"] = symbol
    query[API_KEY_QUERY_PARAMETER] = api_key
    payload = request_json(
        session=session,
        endpoint=endpoint,
        query=query,
        connect_timeout=runtime["connect_timeout"],
        read_timeout=runtime["read_timeout"],
        max_attempts=runtime["max_attempts"],
        backoff_seconds=runtime["backoff_seconds"],
        allow_provider_error=allow_invalid_symbol,
    )

    message_type, provider_message = extract_provider_message(payload)
    if provider_message is not None:
        if allow_invalid_symbol and message_type == "Error Message":
            return None, {"http_requests": 1, "provider_error": provider_message}, False
        raise ConnectorResponseError(
            f"Alpha Vantage returned {message_type}: {provider_message}"
        )

    frame, response_metadata = normalize_time_series(
        instrument=instrument,
        resolution=resolution,
        payload=payload,
    )
    return frame, {
        "http_requests": 1,
        "observations": len(frame),
        "time_series_key": response_metadata["time_series_key"],
        "provider_metadata": response_metadata["provider_metadata"],
    }, True


# ============================================================================
# CONNECTOR
# ============================================================================

class AlphaVantageConnector(BaseConnector):
    """Source-faithful Alpha Vantage connector with symbol resolution."""

    SUPPORTED_RUNTIME_OPTIONS = frozenset({
        "connect_timeout",
        "read_timeout",
        "max_attempts",
        "backoff_seconds",
        "request_delay_seconds",
        "user_agent",
    })

    @classmethod
    def validate_parameters(cls, parameters: dict[str, Any]) -> None:
        if not isinstance(parameters, dict):
            raise ConnectorConfigurationError("Alpha Vantage Parameters must be a dictionary.")

        missing = sorted(
            key for key in REQUIRED_PARAMETERS
            if optional_text(parameters.get(key)) is None
        )
        if missing:
            raise ConnectorConfigurationError(
                f"Alpha Vantage Parameters are missing: {missing}"
            )

        forbidden = sorted(key for key in RESERVED_PARAMETERS if key in parameters)
        if forbidden:
            raise ConnectorConfigurationError(
                f"Alpha Vantage Parameters must not contain connector-managed values: {forbidden}"
            )

        configured_datatype = optional_text(parameters.get(DATATYPE_PARAMETER))
        if configured_datatype is not None and configured_datatype.casefold() != REQUIRED_DATATYPE:
            raise ConnectorConfigurationError("Alpha Vantage connector requires datatype=json.")

        output_size = optional_text(parameters.get(OUTPUT_SIZE_PARAMETER))
        if output_size is not None and output_size.casefold() not in {"compact", "full"}:
            raise ConnectorConfigurationError("Alpha Vantage outputsize must be compact or full.")

        build_request_parameters(parameters)

    @classmethod
    def validate_context(cls, context: ConnectorContext) -> None:
        super().validate_context(context)
        if optional_text(context.endpoint) is None:
            raise ConnectorConfigurationError("Alpha Vantage requires Access Endpoint.")
        if context.input_data is None:
            raise ConnectorConfigurationError("Alpha Vantage requires an upstream input Dataset.")
        normalize_instruments(context.input_data, context.parameters)
        resolve_runtime_options(context.runtime_options)

    @classmethod
    def acquire(cls, context: ConnectorContext) -> ConnectorResult:
        cls.validate_context(context)

        endpoint = optional_text(context.endpoint)
        if endpoint is None or context.input_data is None:
            raise ConnectorConfigurationError("Alpha Vantage execution context is incomplete.")

        parameters = context.parameters.copy()
        instruments = normalize_instruments(context.input_data.copy(deep=True), parameters)
        request_parameters = build_request_parameters(parameters)
        runtime = resolve_runtime_options(context.runtime_options)
        api_key = load_api_key()

        frames: list[pd.DataFrame] = []
        total_requests = 0
        total_observations = 0
        resolution_counts = {
            STATUS_DIRECT: 0,
            STATUS_SEARCH_RESOLVED: 0,
            STATUS_SEARCH_AMBIGUOUS: 0,
            STATUS_SEARCH_NOT_FOUND: 0,
        }
        instrument_metadata: dict[str, Any] = {}

        with requests.Session() as session:
            session.headers.update({
                "Accept": "application/json",
                "User-Agent": runtime["user_agent"],
            })

            for instrument in instruments:
                logger.info("Alpha Vantage acquisition started: %s", instrument.ticker)

                # First try the upstream ticker directly. This avoids consuming an
                # additional SYMBOL_SEARCH request when the provider accepts it.
                direct_resolution = SymbolResolution(
                    status=STATUS_DIRECT,
                    resolved_symbol=instrument.ticker,
                )
                frame, metadata, direct_ok = acquire_time_series(
                    session=session,
                    endpoint=endpoint,
                    api_key=api_key,
                    instrument=instrument,
                    resolution=direct_resolution,
                    request_parameters=request_parameters,
                    runtime=runtime,
                    allow_invalid_symbol=True,
                )
                total_requests += int(metadata.get("http_requests", 0))
                maybe_wait(runtime)

                resolution = direct_resolution
                search_requests = 0

                if not direct_ok:
                    resolution, search_requests = search_symbol(
                        session=session,
                        endpoint=endpoint,
                        api_key=api_key,
                        instrument=instrument,
                        runtime=runtime,
                    )
                    total_requests += search_requests
                    maybe_wait(runtime)

                    if resolution.status == STATUS_SEARCH_RESOLVED:
                        frame, metadata, _ = acquire_time_series(
                            session=session,
                            endpoint=endpoint,
                            api_key=api_key,
                            instrument=instrument,
                            resolution=resolution,
                            request_parameters=request_parameters,
                            runtime=runtime,
                        )
                        total_requests += int(metadata.get("http_requests", 0))
                        maybe_wait(runtime)
                    else:
                        frame = status_frame(instrument, resolution)
                        metadata = {"observations": 0}

                resolution_counts[resolution.status] += 1
                observations = int(metadata.get("observations", 0))
                total_observations += observations
                frames.append(frame if frame is not None else status_frame(instrument, resolution))

                instrument_metadata[instrument.ticker] = {
                    "resolution_status": resolution.status,
                    "resolved_symbol": resolution.resolved_symbol,
                    "match_score": resolution.match_score,
                    "provider_name": resolution.provider_name,
                    "provider_region": resolution.provider_region,
                    "search_keyword": resolution.search_keyword,
                    "search_candidate_count": resolution.candidate_count,
                    "search_candidates": [dict(candidate) for candidate in resolution.search_candidates],
                    "search_requests": search_requests,
                    "observations": observations,
                }

                logger.info(
                    "Alpha Vantage acquisition completed: %s; Status=%s; Resolved=%s; Observations=%s",
                    instrument.ticker,
                    resolution.status,
                    resolution.resolved_symbol,
                    observations,
                )

        dataframe = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

        fact_rows = dataframe["timestamp"].notna().sum() if "timestamp" in dataframe.columns else 0
        if int(fact_rows) != total_observations:
            raise ConnectorResponseError(
                "Alpha Vantage normalized observation count does not match accumulated metadata. "
                f"DataFrame={int(fact_rows)}; Accumulated={total_observations}"
            )

        if not dataframe.empty:
            observed = dataframe[dataframe["timestamp"].notna()]
            duplicates = observed.duplicated(
                subset=["input_position", "resolved_symbol", "observation_position"],
                keep=False,
            )
            if duplicates.any():
                raise ConnectorResponseError(
                    "Alpha Vantage output contains duplicate technical observation positions."
                )

        result_metadata = {
            "instrument_count": len(instruments),
            "http_requests": total_requests,
            "observation_records": total_observations,
            "resolution_counts": resolution_counts,
            "instruments": instrument_metadata,
        }

        logger.info(
            "Alpha Vantage acquisition completed: Instruments=%s; Requests=%s; Observations=%s; Resolution=%s",
            len(instruments),
            total_requests,
            total_observations,
            resolution_counts,
        )

        return ConnectorResult(data=dataframe, metadata=result_metadata)
