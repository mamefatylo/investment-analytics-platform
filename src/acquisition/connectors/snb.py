"""
snb.py

Source-faithful Swiss National Bank data-portal connector.

Purpose
-------
Acquire one published SNB cube through the data.snb.ch REST API
without applying downstream analytical transformations.

Responsibilities
----------------
The connector is responsible for:
- validating SNB-specific Parameters;
- reading listing currencies from the upstream Dataset, or an explicit series list;
- requesting only the SNB codes configured for that selection;
- constructing the cube lastUpdate and data requests;
- interpreting the SNB JSON cube response;
- preserving series headers, metadata, and observation values;
- returning technically normalized tabular data;
- returning non-secret execution metadata.

The connector does not:
- read the Source Registry;
- contain business Source IDs;
- contain business Dataset IDs;
- contain the SNB endpoint;
- contain the cube identifier;
- contain output file names;
- contain storage directories;
- resolve output paths;
- publish files;
- write acquisition logs;
- load SQLite tables;
- rescale quotations (for example per 100 currency units);
- convert monthly observations to daily observations;
- fill missing observations;
- drop currencies absent from the provider response;
- invent an SNB code for a currency that is not configured;
- perform analytical filtering.

Business interpretation belongs downstream in preparation
and quality layers.
"""

import re
import time
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests

from src.acquisition.connectors.base import (
    BaseConnector,
    ConnectorConfigurationError,
    ConnectorContext,
    ConnectorResponseError,
    ConnectorResult,
    ConnectorTransportError,
)
from src.utils.logger import logger


# ============================================================================
# SNB DATASET PARAMETER CONTRACT
# ============================================================================

CUBE_ID_PARAMETER = "cube_id"
LANGUAGE_PARAMETER = "language"
FILE_TYPE_PARAMETER = "file_type"
FREQUENCY_CODE_PARAMETER = "frequency_code"
CURRENCY_COLUMN_PARAMETER = "currency_column"
REPORTING_CURRENCY_PARAMETER = "reporting_currency"
SNB_CURRENCY_CODES_PARAMETER = "snb_currency_codes"
SERIES_CODES_PARAMETER = "series_codes"
FROM_DATE_PARAMETER = "from_date"
TO_DATE_PARAMETER = "to_date"

REQUIRED_FILE_TYPE = "json"

SUPPORTED_LANGUAGES = frozenset(
    {
        "en",
        "de",
        "fr",
        "it",
    }
)

SUPPORTED_DATASET_PARAMETERS = frozenset(
    {
        CUBE_ID_PARAMETER,
        LANGUAGE_PARAMETER,
        FILE_TYPE_PARAMETER,
        FREQUENCY_CODE_PARAMETER,
        CURRENCY_COLUMN_PARAMETER,
        REPORTING_CURRENCY_PARAMETER,
        SNB_CURRENCY_CODES_PARAMETER,
        SERIES_CODES_PARAMETER,
        FROM_DATE_PARAMETER,
        TO_DATE_PARAMETER,
    }
)

CUBE_ID_PATTERN = re.compile(
    r"^[a-z][a-z0-9_]*$"
)

ISO_CURRENCY_PATTERN = re.compile(
    r"^[A-Z]{3}$"
)

SNB_CODE_PATTERN = re.compile(
    r"^[A-Z0-9]+$"
)

FREQUENCY_CODE_PATTERN = re.compile(
    r"^[A-Z0-9]+$"
)

COLUMN_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*$"
)

DIM_SEL_PATTERN = re.compile(
    r"^[A-Za-z0-9(),_]+$"
)

DATE_FILTER_PATTERN = re.compile(
    r"^\d{4}(-\d{2}(-\d{2})?)?$"
)


# ============================================================================
# PROVIDER CONTRACT
# ============================================================================

LAST_UPDATE_RESOURCE = "lastUpdate"
DATA_RESOURCE = "data"

EDITION_DATE_FIELD = "editionDate"
PUBLIC_SINCE_DATE_FIELD = "publicSinceDate"

TIMESERIES_FIELD = "timeseries"
HEADER_FIELD = "header"
METADATA_FIELD = "metadata"
VALUES_FIELD = "values"

HEADER_DIM_FIELD = "dim"
HEADER_ITEM_FIELD = "dimItem"

SERIES_KEY_FIELD = "key"
FREQUENCY_FIELD = "frequency"
SCALE_FIELD = "scale"
UNIT_FIELD = "unit"

OBSERVATION_DATE_FIELD = "date"
OBSERVATION_VALUE_FIELD = "value"

QUERY_DIM_SEL = "dimSel"
QUERY_FROM_DATE = "fromDate"
QUERY_TO_DATE = "toDate"

RETRYABLE_STATUS_CODES = frozenset(
    {
        408,
        429,
        500,
        502,
        503,
        504,
    }
)


# ============================================================================
# OUTPUT CONTRACT
# ============================================================================

CUBE_ID_COLUMN = "cube_id"
SERIES_KEY_COLUMN = "series_key"
FREQUENCY_COLUMN = "frequency"
SCALE_COLUMN = "scale"
UNIT_COLUMN = "unit"
VALUE_POSITION_COLUMN = "value_position"
DATE_COLUMN = "date"
VALUE_COLUMN = "value"

BASE_COLUMNS = (
    CUBE_ID_COLUMN,
    SERIES_KEY_COLUMN,
    FREQUENCY_COLUMN,
    SCALE_COLUMN,
    UNIT_COLUMN,
    VALUE_POSITION_COLUMN,
    DATE_COLUMN,
    VALUE_COLUMN,
)


# ============================================================================
# RUNTIME DEFAULTS
# ============================================================================

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 2.0
DEFAULT_REQUEST_DELAY_SECONDS = 0.0
DEFAULT_USER_AGENT = "investment-data-pipeline/1.0"


# ============================================================================
# NORMALIZATION
# ============================================================================

def optional_text(
    value: Any,
) -> str | None:
    """Normalize optional scalar text."""

    if value is None:
        return None

    if isinstance(
        value,
        (
            dict,
            list,
            tuple,
            set,
        ),
    ):
        return None

    try:
        if pd.isna(value):
            return None
    except (
        TypeError,
        ValueError,
    ):
        pass

    normalized = str(value).strip()

    return normalized or None


def positive_integer(
    value: Any,
    field_name: str,
) -> int:
    """Normalize a strictly positive integer."""

    if isinstance(value, bool):
        raise ConnectorConfigurationError(
            f"{field_name} must be a positive integer."
        )

    try:
        normalized = int(value)
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

    if isinstance(value, bool):
        raise ConnectorConfigurationError(
            f"{field_name} must be a positive number."
        )

    try:
        normalized = float(value)
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be a positive number."
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

    if isinstance(value, bool):
        raise ConnectorConfigurationError(
            f"{field_name} must be a non-negative number."
        )

    try:
        normalized = float(value)
    except (
        TypeError,
        ValueError,
    ) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be a non-negative number."
        ) from error

    if normalized < 0:
        raise ConnectorConfigurationError(
            f"{field_name} cannot be negative."
        )

    return normalized


def require_parameter_text(
    parameters: dict[str, Any],
    parameter_name: str,
) -> str:
    """Return one required non-empty parameter."""

    normalized = optional_text(
        parameters.get(parameter_name)
    )

    if normalized is None:
        raise ConnectorConfigurationError(
            f"SNB Parameter {parameter_name} is required."
        )

    return normalized


def header_column_name(
    position: int,
    kind: str,
) -> str:
    """Return a positional header column name."""

    return f"header_{kind}_{position}"


def output_columns(
    header_count: int,
) -> list[str]:
    """
    Return the stable column order for one response.

    Header columns follow the provider header order. They are inserted
    before the observation fields so each row keeps its series identity
    next to the published dimension labels.
    """

    header_columns = []

    for position in range(1, header_count + 1):
        header_columns.append(
            header_column_name(
                position,
                "dim",
            )
        )
        header_columns.append(
            header_column_name(
                position,
                "item",
            )
        )

    return [
        CUBE_ID_COLUMN,
        SERIES_KEY_COLUMN,
        FREQUENCY_COLUMN,
        SCALE_COLUMN,
        UNIT_COLUMN,
        *header_columns,
        VALUE_POSITION_COLUMN,
        DATE_COLUMN,
        VALUE_COLUMN,
    ]


# ============================================================================
# PARAMETER CONTRACT
# ============================================================================

def normalize_cube_id(
    parameters: dict[str, Any],
) -> str:
    """Validate the cube identifier used as a URL path segment."""

    cube_id = require_parameter_text(
        parameters,
        CUBE_ID_PARAMETER,
    ).casefold()

    if CUBE_ID_PATTERN.fullmatch(cube_id) is None:
        raise ConnectorConfigurationError(
            "SNB cube_id must contain only lowercase "
            "letters, numbers and underscores, and "
            "must begin with a letter."
        )

    return cube_id


def normalize_language(
    parameters: dict[str, Any],
) -> str:
    """Validate the portal language used in the data path."""

    language = require_parameter_text(
        parameters,
        LANGUAGE_PARAMETER,
    ).casefold()

    if language not in SUPPORTED_LANGUAGES:
        raise ConnectorConfigurationError(
            "SNB language must be one of: "
            f"{sorted(SUPPORTED_LANGUAGES)}."
        )

    return language


def normalize_file_type(
    parameters: dict[str, Any],
) -> str:
    """
    Validate the response format.

    The connector parses JSON. file_type may be omitted.
    An explicit value must be json.
    """

    configured = optional_text(
        parameters.get(FILE_TYPE_PARAMETER)
    )

    if configured is None:
        return REQUIRED_FILE_TYPE

    normalized = configured.casefold()

    if normalized != REQUIRED_FILE_TYPE:
        raise ConnectorConfigurationError(
            "SNB connector requires file_type=json."
        )

    return normalized


def normalize_frequency_code(
    parameters: dict[str, Any],
) -> str:
    """Validate the D0 code requested from the cube."""

    frequency_code = require_parameter_text(
        parameters,
        FREQUENCY_CODE_PARAMETER,
    )

    if FREQUENCY_CODE_PATTERN.fullmatch(frequency_code) is None:
        raise ConnectorConfigurationError(
            "SNB frequency_code must contain only "
            "uppercase letters and numbers."
        )

    return frequency_code


def normalize_currency_column(
    parameters: dict[str, Any],
) -> str:
    """Validate the upstream listing-currency column name."""

    column_name = require_parameter_text(
        parameters,
        CURRENCY_COLUMN_PARAMETER,
    )

    if COLUMN_NAME_PATTERN.fullmatch(column_name) is None:
        raise ConnectorConfigurationError(
            "SNB currency_column must be a column name."
        )

    return column_name


def normalize_reporting_currency(
    parameters: dict[str, Any],
) -> str:
    """Validate the currency that does not require a rate."""

    reporting_currency = require_parameter_text(
        parameters,
        REPORTING_CURRENCY_PARAMETER,
    )

    if ISO_CURRENCY_PATTERN.fullmatch(reporting_currency) is None:
        raise ConnectorConfigurationError(
            "SNB reporting_currency must be a "
            "three-letter ISO code."
        )

    return reporting_currency


def normalize_snb_currency_codes(
    parameters: dict[str, Any],
) -> dict[str, str]:
    """Validate the ISO currency to SNB dimension-code map."""

    configured = parameters.get(
        SNB_CURRENCY_CODES_PARAMETER
    )

    if not isinstance(configured, dict) or not configured:
        raise ConnectorConfigurationError(
            "SNB snb_currency_codes must be "
            "a non-empty object."
        )

    normalized: dict[str, str] = {}

    for raw_currency, raw_code in configured.items():
        currency = optional_text(raw_currency)
        code = optional_text(raw_code)

        if (
            currency is None
            or ISO_CURRENCY_PATTERN.fullmatch(currency) is None
        ):
            raise ConnectorConfigurationError(
                "SNB snb_currency_codes keys must be "
                "three-letter ISO codes."
            )

        if (
            code is None
            or SNB_CODE_PATTERN.fullmatch(code) is None
        ):
            raise ConnectorConfigurationError(
                "SNB snb_currency_codes values must "
                f"be provider dimension codes: {currency}"
            )

        normalized[currency] = code

    duplicate_codes = sorted(
        code
        for code in set(normalized.values())
        if list(normalized.values()).count(code) > 1
    )

    if duplicate_codes:
        raise ConnectorConfigurationError(
            "SNB snb_currency_codes contains "
            f"duplicate provider codes: {duplicate_codes}"
        )

    return normalized


def build_dim_sel(
    frequency_code: str,
    snb_codes: list[str],
) -> str:
    """Build the provider dimension filter from resolved codes."""

    if not snb_codes:
        raise ConnectorConfigurationError(
            "SNB request has no currency codes."
        )

    dim_sel = (
        f"D0({frequency_code}),D1({','.join(snb_codes)})"
    )

    if DIM_SEL_PATTERN.fullmatch(dim_sel) is None:
        raise ConnectorConfigurationError(
            "SNB could not build a valid dimSel "
            f"from the resolved codes: {dim_sel}"
        )

    return dim_sel


CURRENCY_SELECTION_PARAMETERS = frozenset(
    {
        FREQUENCY_CODE_PARAMETER,
        CURRENCY_COLUMN_PARAMETER,
        REPORTING_CURRENCY_PARAMETER,
        SNB_CURRENCY_CODES_PARAMETER,
    }
)


def uses_direct_series(parameters: dict[str, Any]) -> bool:
    """True when the dataset names SNB series instead of listing currencies."""
    return SERIES_CODES_PARAMETER in parameters


def normalize_series_codes(parameters: dict[str, Any]) -> list[str]:
    """Validate the explicit D0 series codes for a non-FX cube."""
    raw_codes = parameters.get(SERIES_CODES_PARAMETER)
    if not isinstance(raw_codes, list) or not raw_codes:
        raise ConnectorConfigurationError(
            "SNB series_codes must be a non-empty list."
        )
    codes: list[str] = []
    for raw_code in raw_codes:
        code = optional_text(raw_code)
        if code is None or SNB_CODE_PATTERN.fullmatch(code) is None:
            raise ConnectorConfigurationError(
                "SNB series_codes must be provider dimension codes: "
                f"{raw_code}"
            )
        codes.append(code)
    if len(set(codes)) != len(codes):
        raise ConnectorConfigurationError(
            "SNB series_codes contains duplicate codes."
        )
    return codes


def resolve_direct_series(parameters: dict[str, Any]) -> dict[str, Any]:
    """Select named series, without an upstream currency file."""
    codes = normalize_series_codes(parameters)
    dim_sel = f"D0({','.join(codes)})"
    if DIM_SEL_PATTERN.fullmatch(dim_sel) is None:
        raise ConnectorConfigurationError(
            "SNB could not build a valid dimSel "
            f"from the series codes: {dim_sel}"
        )
    return {
        "reporting_currency": None,
        "requested_currencies": [],
        "requested_snb_codes": codes,
        "unmapped_currencies": [],
        "dim_sel": dim_sel,
    }


def resolve_requested_currencies(
    input_data: pd.DataFrame,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """
    Select SNB codes for the upstream listing currencies.

    The reporting currency is not requested.
    A listing currency with no configured SNB code is reported
    and is not sent to the provider.
    """

    if input_data is None or input_data.empty:
        raise ConnectorConfigurationError(
            "SNB cube acquisition requires "
            "a non-empty upstream Dataset."
        )

    column_name = normalize_currency_column(parameters)
    reporting_currency = normalize_reporting_currency(
        parameters
    )
    code_map = normalize_snb_currency_codes(parameters)

    if column_name not in input_data.columns:
        raise ConnectorConfigurationError(
            "SNB upstream Dataset is missing "
            f"column: {column_name}"
        )

    listing_currencies: list[str] = []

    for value in input_data[column_name].tolist():
        currency = optional_text(value)

        if currency is None:
            raise ConnectorConfigurationError(
                "SNB upstream Dataset contains "
                f"a blank {column_name}."
            )

        if ISO_CURRENCY_PATTERN.fullmatch(currency) is None:
            raise ConnectorConfigurationError(
                "SNB upstream listing currency must "
                f"be a three-letter ISO code: {currency}"
            )

        listing_currencies.append(currency)

    distinct_currencies = sorted(set(listing_currencies))
    requested: list[tuple[str, str]] = []
    unmapped: list[str] = []

    for currency in distinct_currencies:
        if currency == reporting_currency:
            continue

        code = code_map.get(currency)

        if code is None:
            unmapped.append(currency)
            continue

        requested.append((currency, code))

    if not requested:
        raise ConnectorConfigurationError(
            "SNB request has no currency codes "
            "after applying the upstream Dataset."
        )

    return {
        "reporting_currency": reporting_currency,
        "requested_currencies": [
            currency for currency, _code in requested
        ],
        "requested_snb_codes": [
            code for _currency, code in requested
        ],
        "unmapped_currencies": unmapped,
        "dim_sel": build_dim_sel(
            normalize_frequency_code(parameters),
            [code for _currency, code in requested],
        ),
    }


def normalize_date_filter(
    parameters: dict[str, Any],
    parameter_name: str,
) -> str | None:
    """Validate an optional fromDate or toDate filter."""

    configured = optional_text(
        parameters.get(parameter_name)
    )

    if configured is None:
        return None

    if DATE_FILTER_PATTERN.fullmatch(configured) is None:
        raise ConnectorConfigurationError(
            f"SNB {parameter_name} must use "
            "YYYY, YYYY-MM or YYYY-MM-DD."
        )

    return configured


def build_query_parameters(
    parameters: dict[str, Any],
    dim_sel: str,
) -> dict[str, str]:
    """Build the provider query string from a resolved dimSel."""

    if DIM_SEL_PATTERN.fullmatch(dim_sel) is None:
        raise ConnectorConfigurationError(
            "SNB dimSel is not a valid provider filter."
        )

    query = {
        QUERY_DIM_SEL: dim_sel,
    }

    from_date = normalize_date_filter(
        parameters,
        FROM_DATE_PARAMETER,
    )

    to_date = normalize_date_filter(
        parameters,
        TO_DATE_PARAMETER,
    )

    if from_date is not None:
        query[QUERY_FROM_DATE] = from_date

    if to_date is not None:
        query[QUERY_TO_DATE] = to_date

    return query


# ============================================================================
# RUNTIME OPTIONS
# ============================================================================

def resolve_runtime_options(
    runtime_options: dict[str, Any],
) -> dict[str, Any]:
    """Resolve technical execution options provided by the engine."""

    if not isinstance(runtime_options, dict):
        raise ConnectorConfigurationError(
            "runtime_options must be a dictionary."
        )

    allowed_options = {
        "connect_timeout",
        "read_timeout",
        "max_attempts",
        "backoff_seconds",
        "request_delay_seconds",
        "user_agent",
    }

    unsupported = sorted(
        set(runtime_options) - allowed_options
    )

    if unsupported:
        raise ConnectorConfigurationError(
            "Unsupported SNB runtime options: "
            f"{unsupported}"
        )

    user_agent = optional_text(
        runtime_options.get(
            "user_agent",
            DEFAULT_USER_AGENT,
        )
    )

    if user_agent is None:
        raise ConnectorConfigurationError(
            "user_agent cannot be empty."
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
        "request_delay_seconds": non_negative_number(
            runtime_options.get(
                "request_delay_seconds",
                DEFAULT_REQUEST_DELAY_SECONDS,
            ),
            "request_delay_seconds",
        ),
        "user_agent": user_agent,
    }


def resolve_retry_delay(
    response: requests.Response,
    *,
    attempt: int,
    backoff_seconds: float,
) -> float:
    """Prefer Retry-After, otherwise use exponential backoff."""

    retry_after = optional_text(
        response.headers.get("Retry-After")
    )

    if retry_after is not None:
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            pass

    return backoff_seconds * (2 ** (attempt - 1))


# ============================================================================
# URLS
# ============================================================================

def normalize_endpoint(
    endpoint: str | None,
) -> str:
    """Return the cube API root without a trailing slash."""

    normalized = optional_text(endpoint)

    if normalized is None:
        raise ConnectorConfigurationError(
            "SNB requires Access Endpoint."
        )

    if not normalized.lower().startswith(
        ("http://", "https://")
    ):
        raise ConnectorConfigurationError(
            "SNB Access Endpoint must be HTTP/HTTPS."
        )

    if "?" in normalized or "#" in normalized:
        raise ConnectorConfigurationError(
            "SNB Access Endpoint must be the cube "
            "API root, without a query string."
        )

    return normalized.rstrip("/")


def build_resource_url(
    endpoint: str,
    cube_id: str,
    resource: str,
    *,
    language: str | None = None,
    file_type: str | None = None,
    query: dict[str, str] | None = None,
) -> str:
    """Build one cube resource URL from registry inputs."""

    parts = [
        endpoint,
        cube_id,
        resource,
    ]

    if file_type is not None:
        parts.append(file_type)

    if language is not None:
        parts.append(language)

    url = "/".join(parts)

    if query:
        url = f"{url}?{urlencode(query)}"

    return url


# ============================================================================
# HTTP
# ============================================================================

def request_json(
    session: requests.Session,
    url: str,
    *,
    runtime: dict[str, Any],
) -> dict[str, Any]:
    """GET one SNB JSON resource, with bounded retries."""

    max_attempts = int(runtime["max_attempts"])
    backoff_seconds = float(runtime["backoff_seconds"])
    timeout = (
        float(runtime["connect_timeout"]),
        float(runtime["read_timeout"]),
    )

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(
                url,
                timeout=timeout,
            )
        except requests.RequestException as error:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    "SNB request failed after all "
                    f"configured attempts: {url}"
                ) from error

            wait_seconds = backoff_seconds * (
                2 ** (attempt - 1)
            )

            logger.warning(
                "SNB transport retry %s/%s in %.2f seconds",
                attempt,
                max_attempts,
                wait_seconds,
            )

            time.sleep(wait_seconds)
            continue

        status_code = response.status_code

        if status_code in RETRYABLE_STATUS_CODES:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    "SNB returned retryable HTTP status "
                    f"after all configured attempts: {status_code}"
                )

            wait_seconds = resolve_retry_delay(
                response,
                attempt=attempt,
                backoff_seconds=backoff_seconds,
            )

            logger.warning(
                "SNB HTTP retry %s/%s after status %s "
                "in %.2f seconds",
                attempt,
                max_attempts,
                status_code,
                wait_seconds,
            )

            time.sleep(wait_seconds)
            continue

        if status_code == 304:
            raise ConnectorResponseError(
                "SNB returned HTTP 304 without an "
                "If-None-Match request."
            )

        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            detail = optional_text(response.text)
            suffix = (
                f" Provider response: {detail}"
                if detail
                else ""
            )
            raise ConnectorResponseError(
                "SNB request failed with "
                f"HTTP {status_code}.{suffix}"
            ) from error

        try:
            payload = response.json()
        except (
            ValueError,
            requests.JSONDecodeError,
        ) as error:
            raise ConnectorResponseError(
                "SNB response is not valid JSON."
            ) from error

        if not isinstance(payload, dict):
            raise ConnectorResponseError(
                "SNB response must be a JSON object."
            )

        return payload

    raise ConnectorTransportError(
        "SNB request exhausted all configured attempts."
    )


# ============================================================================
# RESPONSE CONTRACT
# ============================================================================

def require_object_text(
    payload: dict[str, Any],
    field_name: str,
    *,
    allow_blank: bool = False,
) -> str:
    """Return one required string field from a provider object."""

    if field_name not in payload:
        raise ConnectorResponseError(
            f"SNB response is missing field: {field_name}"
        )

    value = payload[field_name]

    if value is None:
        raise ConnectorResponseError(
            f"SNB field {field_name} is null."
        )

    if not isinstance(value, str):
        raise ConnectorResponseError(
            f"SNB field {field_name} must be a string."
        )

    if not allow_blank and not value.strip():
        raise ConnectorResponseError(
            f"SNB field {field_name} is empty."
        )

    return value


def parse_last_update(
    payload: dict[str, Any],
) -> dict[str, str]:
    """Read the cube edition dates published by lastUpdate."""

    return {
        "edition_date": require_object_text(
            payload,
            EDITION_DATE_FIELD,
        ),
        "public_since_date": require_object_text(
            payload,
            PUBLIC_SINCE_DATE_FIELD,
        ),
    }


def parse_header(
    header: Any,
) -> list[dict[str, str]]:
    """Read one series header in provider order."""

    if not isinstance(header, list) or not header:
        raise ConnectorResponseError(
            "SNB series header must be a non-empty list."
        )

    parsed = []

    for item in header:
        if not isinstance(item, dict):
            raise ConnectorResponseError(
                "SNB series header item must be an object."
            )

        parsed.append(
            {
                "dim": require_object_text(
                    item,
                    HEADER_DIM_FIELD,
                ),
                "item": require_object_text(
                    item,
                    HEADER_ITEM_FIELD,
                ),
            }
        )

    return parsed


def parse_observation_value(
    value: Any,
) -> Any:
    """
    Keep a published observation value without rescaling.

    Null remains null. Numbers remain numbers.
    """

    if value is None:
        return None

    if isinstance(value, bool) or isinstance(value, (dict, list)):
        raise ConnectorResponseError(
            "SNB observation value must be a number or null."
        )

    if isinstance(value, (int, float)):
        return value

    raise ConnectorResponseError(
        "SNB observation value must be a number or null."
    )


def parse_timeseries(
    payload: dict[str, Any],
    *,
    cube_id: str,
) -> tuple[pd.DataFrame, int]:
    """Flatten cube timeseries into one source-faithful table."""

    if TIMESERIES_FIELD not in payload:
        raise ConnectorResponseError(
            "SNB data response is missing timeseries."
        )

    timeseries = payload[TIMESERIES_FIELD]

    if not isinstance(timeseries, list):
        raise ConnectorResponseError(
            "SNB timeseries must be a list."
        )

    if not timeseries:
        raise ConnectorResponseError(
            "SNB returned no timeseries for the "
            "configured filter."
        )

    rows: list[dict[str, Any]] = []
    header_count = 0

    for series in timeseries:
        if not isinstance(series, dict):
            raise ConnectorResponseError(
                "SNB timeseries item must be an object."
            )

        header = parse_header(
            series.get(HEADER_FIELD)
        )
        header_count = max(header_count, len(header))

        metadata = series.get(METADATA_FIELD)

        if not isinstance(metadata, dict):
            raise ConnectorResponseError(
                "SNB series metadata must be an object."
            )

        series_key = require_object_text(
            metadata,
            SERIES_KEY_FIELD,
        )
        frequency = require_object_text(
            metadata,
            FREQUENCY_FIELD,
        )
        unit = require_object_text(
            metadata,
            UNIT_FIELD,
        )
        scale = require_object_text(
            metadata,
            SCALE_FIELD,
            allow_blank=True,
        )

        values = series.get(VALUES_FIELD)

        if not isinstance(values, list):
            raise ConnectorResponseError(
                "SNB series values must be a list."
            )

        for position, observation in enumerate(values, start=1):
            if not isinstance(observation, dict):
                raise ConnectorResponseError(
                    "SNB observation must be an object."
                )

            if OBSERVATION_DATE_FIELD not in observation:
                raise ConnectorResponseError(
                    "SNB observation is missing date."
                )

            date = require_object_text(
                observation,
                OBSERVATION_DATE_FIELD,
            )

            if OBSERVATION_VALUE_FIELD not in observation:
                observed_value = None
            else:
                observed_value = parse_observation_value(
                    observation[OBSERVATION_VALUE_FIELD]
                )

            row = {
                CUBE_ID_COLUMN: cube_id,
                SERIES_KEY_COLUMN: series_key,
                FREQUENCY_COLUMN: frequency,
                SCALE_COLUMN: scale,
                UNIT_COLUMN: unit,
                VALUE_POSITION_COLUMN: position,
                DATE_COLUMN: date,
                VALUE_COLUMN: observed_value,
            }

            for index, header_item in enumerate(header, start=1):
                row[
                    header_column_name(index, "dim")
                ] = header_item["dim"]
                row[
                    header_column_name(index, "item")
                ] = header_item["item"]

            rows.append(row)

    columns = output_columns(header_count)

    dataframe = pd.DataFrame(
        rows,
        columns=columns,
    )

    return dataframe, len(timeseries)


# ============================================================================
# CONNECTOR
# ============================================================================

class SnbConnector(BaseConnector):
    """
    Source-faithful SNB data-portal cube connector.

    One execution reads lastUpdate, then the filtered JSON cube,
    and returns the published observations without rescaling.
    """

    SUPPORTED_RUNTIME_OPTIONS = frozenset(
        {
            "connect_timeout",
            "read_timeout",
            "max_attempts",
            "backoff_seconds",
            "request_delay_seconds",
            "user_agent",
        }
    )

    @classmethod
    def validate_parameters(
        cls,
        parameters: dict[str, Any],
    ) -> None:
        """
        Validate SNB-specific Dataset Parameters.

        An FX cube requires cube_id, language, frequency_code,
        currency_column, reporting_currency and snb_currency_codes.

        A named series requires cube_id, language and series_codes.
        It does not use the FX currency parameters.

        file_type may be omitted or explicitly set to json.
        from_date and to_date are optional provider filters.
        """

        if not isinstance(parameters, dict):
            raise ConnectorConfigurationError(
                "SNB Parameters must be a dictionary."
            )

        unsupported_parameters = sorted(
            set(parameters) - SUPPORTED_DATASET_PARAMETERS
        )

        if unsupported_parameters:
            raise ConnectorConfigurationError(
                "Unsupported SNB Dataset Parameters: "
                f"{unsupported_parameters}"
            )

        normalize_cube_id(parameters)
        normalize_language(parameters)
        normalize_file_type(parameters)
        direct_series = uses_direct_series(parameters)
        currency_parameters = CURRENCY_SELECTION_PARAMETERS.intersection(parameters)
        if direct_series and currency_parameters:
            raise ConnectorConfigurationError(
                "SNB series_codes cannot be combined with "
                f"FX currency parameters: {sorted(currency_parameters)}"
            )
        if direct_series:
            normalize_series_codes(parameters)
        else:
            normalize_frequency_code(parameters)
            normalize_currency_column(parameters)
            normalize_reporting_currency(parameters)
            normalize_snb_currency_codes(parameters)
        normalize_date_filter(
            parameters,
            FROM_DATE_PARAMETER,
        )
        normalize_date_filter(
            parameters,
            TO_DATE_PARAMETER,
        )

    @classmethod
    def validate_context(
        cls,
        context: ConnectorContext,
    ) -> None:
        """
        Validate the complete SNB execution context.

        Cube acquisition requires an Access Endpoint. An FX cube
        also requires the upstream Dataset that carries listing
        currencies. A named series does not.
        """

        super().validate_context(context)

        normalize_endpoint(context.endpoint)

        if (
            not uses_direct_series(context.parameters)
            and context.input_data is None
        ):
            raise ConnectorConfigurationError(
                "SNB cube acquisition requires input_data."
            )

        resolve_runtime_options(
            context.runtime_options
        )

    @classmethod
    def acquire(
        cls,
        context: ConnectorContext,
    ) -> ConnectorResult:
        """
        Acquire the configured SNB cube selection.

        Returns
        -------
        ConnectorResult
            data
                One row per published observation.

            metadata
                Cube edition dates and non-secret request counts.
        """

        cls.validate_context(context)

        endpoint = normalize_endpoint(
            context.endpoint
        )
        parameters = context.parameters.copy()
        cube_id = normalize_cube_id(parameters)
        language = normalize_language(parameters)
        file_type = normalize_file_type(parameters)
        if uses_direct_series(parameters):
            selection = resolve_direct_series(parameters)
        else:
            selection = resolve_requested_currencies(
                context.input_data,
                parameters,
            )
        query = build_query_parameters(
            parameters,
            selection["dim_sel"],
        )
        runtime = resolve_runtime_options(
            context.runtime_options
        )

        last_update_url = build_resource_url(
            endpoint,
            cube_id,
            LAST_UPDATE_RESOURCE,
        )
        data_url = build_resource_url(
            endpoint,
            cube_id,
            DATA_RESOURCE,
            language=language,
            file_type=file_type,
            query=query,
        )

        logger.info(
            "SNB cube acquisition started: %s",
            cube_id,
        )

        with requests.Session() as session:
            session.headers.update(
                {
                    "Accept": "application/json",
                    "User-Agent": runtime["user_agent"],
                }
            )

            last_update_payload = request_json(
                session,
                last_update_url,
                runtime=runtime,
            )

            delay = float(
                runtime["request_delay_seconds"]
            )

            if delay > 0:
                time.sleep(delay)

            data_payload = request_json(
                session,
                data_url,
                runtime=runtime,
            )

        last_update = parse_last_update(
            last_update_payload
        )
        dataframe, series_count = parse_timeseries(
            data_payload,
            cube_id=cube_id,
        )

        logger.info(
            "SNB cube acquisition completed: %s; "
            "Series=%s; Observations=%s; Edition=%s",
            cube_id,
            series_count,
            len(dataframe),
            last_update["edition_date"],
        )

        return ConnectorResult(
            data=dataframe,
            metadata={
                "cube_id": cube_id,
                "language": language,
                "file_type": file_type,
                "dim_sel": selection["dim_sel"],
                "requested_currencies": (
                    selection["requested_currencies"]
                ),
                "requested_snb_codes": (
                    selection["requested_snb_codes"]
                ),
                "reporting_currency": (
                    selection["reporting_currency"]
                ),
                "unmapped_currencies": (
                    selection["unmapped_currencies"]
                ),
                "edition_date": last_update["edition_date"],
                "public_since_date": (
                    last_update["public_since_date"]
                ),
                "http_requests": 2,
                "series_count": series_count,
                "observations": len(dataframe),
            },
        )
