"""
sec_edgar.py

Source-faithful SEC EDGAR Company Facts connector (identity-first v1).

Purpose
-------
Acquire SEC XBRL Company Facts only for instruments whose SEC identity
is DIRECT_VALIDATED via submissions evidence. company_tickers.json is
candidate generation only; Company Facts never proves identity.

Architecture
------------
Upstream Dataset (DS-003)
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
        +--> company_tickers.json   (candidate CIK only)
        |
        +--> submissions/CIK….json  (identity authority)
        |
        +--> Company Facts          (context.endpoint; DIRECT only)
        |
        v
ConnectorResult
  data     = XBRL facts for DIRECT_VALIDATED only
  metadata = identity_resolution for every requested instrument

Responsibilities
----------------
- validate SEC Parameters / context (columns + secondary endpoint URLs);
- resolve SEC_EDGAR_USER_AGENT from the environment;
- generate CIK candidates from company_tickers.json (registry URL);
- validate identity via submissions (name, formerNames, tickers, exchanges);
- download Company Facts only when facts_eligible (DIRECT_VALIDATED);
- pace requests (~0.12s), retry transport failures, honor Retry-After;
- classify HTTP failures (Transport / Authentication / Response);
- preserve source-faithful XBRL observations;
- emit identity diagnostics in metadata only (no pseudo-status rows).

The connector does not:
- treat company_tickers title / ticker_reference_title as decision proof;
- invent SEARCH_RESOLVED or alternative auto-acquisition;
- select Revenue / EBIT / EBITDA / FCF;
- construct Fundamentals, promote RAW, or rewrite securities master;
- know Yahoo, Alpha Vantage, or SRC-004B;
- hardcode instrument-specific exceptions;
- hardcode SEC secondary URLs (ticker reference / submissions).
"""

from __future__ import annotations

import os
import re
import time
import unicodedata
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

load_dotenv()


# ============================================================================
# HTTP RETRY SURFACE
# ============================================================================

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
NAME_COLUMN_PARAMETER = "name_column"
EXCHANGE_COLUMN_PARAMETER = "exchange_column"
TICKER_REFERENCE_ENDPOINT_PARAMETER = "ticker_reference_endpoint"
SUBMISSIONS_ENDPOINT_PARAMETER = "submissions_endpoint"

REQUIRED_PARAMETERS = (
    TICKER_COLUMN_PARAMETER,
    NAME_COLUMN_PARAMETER,
    EXCHANGE_COLUMN_PARAMETER,
    TICKER_REFERENCE_ENDPOINT_PARAMETER,
    SUBMISSIONS_ENDPOINT_PARAMETER,
)

ALLOWED_PARAMETERS = frozenset(REQUIRED_PARAMETERS)


# ============================================================================
# IDENTITY STATUSES (closed set v1 — no SEARCH_RESOLVED)
# ============================================================================

DIRECT_VALIDATED_STATUS = "DIRECT_VALIDATED"
IDENTITY_AMBIGUOUS_STATUS = "IDENTITY_AMBIGUOUS"
IDENTITY_NOT_FOUND_STATUS = "IDENTITY_NOT_FOUND"

RESOLUTION_STATUSES = frozenset(
    {
        DIRECT_VALIDATED_STATUS,
        IDENTITY_AMBIGUOUS_STATUS,
        IDENTITY_NOT_FOUND_STATUS,
    }
)

FACTS_ELIGIBLE_STATUSES = frozenset(
    {
        DIRECT_VALIDATED_STATUS,
    }
)


# ============================================================================
# IDENTITY METHOD / REASON (closed sets)
# ============================================================================

IDENTITY_METHOD_TICKER_REFERENCE_SUBMISSIONS = (
    "TICKER_REFERENCE_SUBMISSIONS"
)
IDENTITY_METHOD_NONE = "NONE"

IDENTITY_REASON_EXACT_NO_CONTRADICTION = "EXACT_NO_CONTRADICTION"
IDENTITY_REASON_EXACT_TICKER_CONTRADICTION = (
    "EXACT_TICKER_CONTRADICTION"
)
IDENTITY_REASON_EXACT_EXCHANGE_CONTRADICTION = (
    "EXACT_EXCHANGE_CONTRADICTION"
)
IDENTITY_REASON_EXACT_NAME_CONTRADICTION = (
    "EXACT_NAME_CONTRADICTION"
)
IDENTITY_REASON_MULTIPLE_CIK_CANDIDATES = (
    "MULTIPLE_CIK_CANDIDATES"
)
IDENTITY_REASON_INCONSISTENT_SUBMISSIONS_IDENTITY = (
    "INCONSISTENT_SUBMISSIONS_IDENTITY"
)
IDENTITY_REASON_MISSING_INPUT_TICKER = "MISSING_INPUT_TICKER"
IDENTITY_REASON_TICKER_NOT_IN_REFERENCE = (
    "TICKER_NOT_IN_REFERENCE"
)
IDENTITY_REASON_SUBMISSIONS_IDENTITY_ABSENT = (
    "SUBMISSIONS_IDENTITY_ABSENT"
)
IDENTITY_REASON_NO_PROVIDER_IDENTITY = "NO_PROVIDER_IDENTITY"


# ============================================================================
# EVIDENCE STATES
# ============================================================================

EVIDENCE_MATCH = "MATCH"
EVIDENCE_CONTRADICTION = "CONTRADICTION"
EVIDENCE_UNKNOWN = "UNKNOWN"

EVIDENCE_STATES = frozenset(
    {
        EVIDENCE_MATCH,
        EVIDENCE_CONTRADICTION,
        EVIDENCE_UNKNOWN,
    }
)

CORPORATE_STOPWORDS = frozenset(
    {
        "ab",
        "ag",
        "asa",
        "as",
        "co",
        "company",
        "corp",
        "corporation",
        "inc",
        "incorporated",
        "ltd",
        "limited",
        "llc",
        "lp",
        "nv",
        "oyj",
        "plc",
        "publ",
        "sa",
        "sab",
        "spa",
        "se",
    }
)


# ============================================================================
# EXCHANGE ALIAS TABLE (exact membership; no substring)
# ============================================================================
#
# EXCHANGE_GROUPS map DS-003 exchange labels and SEC submissions
# exchange strings onto interoperability groups. Comparison uses
# exact alias-set membership after normalization. No fuzzy / substring
# matching.

EXCHANGE_GROUPS = {
    "NASDAQ": {
        "nasdaq",
        "nasdaqgs",
        "nasdaqglobalmarket",
        "nasdaqglobalselectmarket",
        "nasdaqcapitalmarket",
        "nms",
        "ngm",
        "ncm",
        "nas",
    },
    "NYSE": {
        "nyse",
        "newyorkstockexchange",
        "nyq",
    },
    "NYSE_MKT": {
        "nysemkt",
        "nyseamerican",
        "americanstockexchange",
        "ase",
        "amex",
    },
    "OTC": {
        "otc",
        "otcmarkets",
        "otcqx",
        "otcqb",
        "pink",
        "pinksheets",
    },
    "LONDON": {
        "london",
        "londonstockexchange",
        "lse",
    },
    "HONG_KONG": {
        "hongkong",
        "hongkongexchangesandclearingltd",
        "hongkongstockexchange",
        "hkex",
        "hkse",
        "hkg",
    },
    "TOKYO": {
        "tokyo",
        "tokyostockexchange",
        "japanexchangegroup",
        "jpx",
    },
    "TORONTO": {
        "toronto",
        "torontostockexchange",
        "tsx",
        "tor",
    },
    "SHANGHAI": {
        "shanghai",
        "shanghaistockexchange",
        "shh",
        "sse",
    },
    "SHENZHEN": {
        "shenzhen",
        "shenzhenstockexchange",
        "shz",
        "szse",
    },
    "KOREA": {
        "korea",
        "koreastockexchange",
        "krx",
        "ksc",
    },
    "KOSDAQ": {
        "kosdaq",
        "koe",
    },
    "INDIA_NSE": {
        "nse",
        "nationalstockexchangeofindia",
        "nationalstockexchangeindia",
        "nsi",
    },
    "INDIA_BSE": {
        "bse",
        "bombay",
        "bombaystockexchange",
        "bsc",
    },
    "SWITZERLAND": {
        "swiss",
        "six",
        "sixswissexchange",
        "swx",
        "ebs",
    },
    "PARIS": {
        "paris",
        "euronextparis",
        "par",
    },
    "BRUSSELS": {
        "brussels",
        "euronextbrussels",
        "bru",
    },
    "AMSTERDAM": {
        "amsterdam",
        "euronextamsterdam",
        "ams",
    },
    "FRANKFURT": {
        "frankfurt",
        "frankfurtstockexchange",
        "xetra",
        "ger",
        "fra",
    },
    "AUSTRALIA": {
        "australia",
        "asx",
        "australiansecuritiesexchange",
        "asxallmarkets",
    },
    "SINGAPORE": {
        "singapore",
        "singaporeexchange",
        "sgx",
        "ses",
    },
}


# ============================================================================
# OUTPUT CONTRACT (facts only — no identity pseudo-rows)
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
)


# ============================================================================
# NORMALIZATION HELPERS
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
            f"{field_name} must be positive."
        )

    try:
        normalized = float(value)
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

    if isinstance(value, bool):
        raise ConnectorConfigurationError(
            f"{field_name} must be non-negative."
        )

    try:
        normalized = float(value)
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


def normalize_cik(
    value: Any,
) -> str | None:
    """Normalize a CIK to zero-padded 10 digits."""

    if value is None:
        return None

    text = optional_text(value)
    if text is None:
        return None

    digits = re.sub(r"\D", "", text)
    if not digits:
        try:
            digits = str(int(value))
        except (
            TypeError,
            ValueError,
        ):
            return None

    try:
        return f"{int(digits):010d}"
    except (
        TypeError,
        ValueError,
    ):
        return None


def normalize_ascii_text(
    value: Any,
) -> str:
    """Casefold ASCII-ish text for token comparisons."""

    text = optional_text(value)
    if text is None:
        return ""

    text = unicodedata.normalize("NFKD", text)
    text = "".join(
        character
        for character in text
        if not unicodedata.combining(character)
    )
    text = text.casefold()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def normalize_exchange_token(
    value: Any,
) -> str:
    """Normalize an exchange alias token (letters/digits only)."""

    return re.sub(r"[^a-z0-9]", "", normalize_ascii_text(value))


# ============================================================================
# RUNTIME OPTIONS
# ============================================================================

def resolve_runtime_options(
    runtime_options: dict[str, Any],
) -> dict[str, Any]:
    """Resolve technical SEC execution settings."""

    if not isinstance(runtime_options, dict):
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

    unsupported = sorted(set(runtime_options) - allowed_options)
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
    """Resolve the SEC EDGAR User-Agent from the environment."""

    user_agent = optional_text(os.getenv("SEC_EDGAR_USER_AGENT"))
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
    name_column: str,
    exchange_column: str,
) -> None:
    """Validate the upstream Dataset used to resolve SEC filers."""

    if not isinstance(input_data, pd.DataFrame):
        raise ConnectorConfigurationError(
            "SEC EDGAR requires input_data as a pandas DataFrame."
        )

    if input_data.empty:
        raise ConnectorConfigurationError(
            "SEC EDGAR input_data is empty."
        )

    for column_name, label in (
        (ticker_column, "ticker"),
        (name_column, "name"),
        (exchange_column, "exchange"),
    ):
        if column_name not in input_data.columns:
            raise ConnectorConfigurationError(
                f"Configured SEC {label} column was not found in "
                f"input_data: {column_name}"
            )


# ============================================================================
# EXCHANGE EVIDENCE
# ============================================================================

def validate_exchange_groups() -> None:
    """Fail fast on empty aliases or cross-group collisions."""

    owners: dict[str, str] = {}
    for group, aliases in EXCHANGE_GROUPS.items():
        for alias in aliases:
            token = normalize_exchange_token(alias)
            if not token:
                raise ConnectorConfigurationError(
                    "EXCHANGE_GROUPS contains an empty alias "
                    f"after normalization in group {group}."
                )
            existing = owners.get(token)
            if existing is not None and existing != group:
                raise ConnectorConfigurationError(
                    "EXCHANGE_GROUPS alias collision after "
                    f"normalization: {token!r} belongs to both "
                    f"{existing} and {group}."
                )
            owners[token] = group


validate_exchange_groups()


EXCHANGE_TOKEN_TO_GROUP: dict[str, str] = {}
for _group, _aliases in EXCHANGE_GROUPS.items():
    for _alias in _aliases:
        _token = normalize_exchange_token(_alias)
        if _token:
            EXCHANGE_TOKEN_TO_GROUP[_token] = _group


def exchange_group(
    value: Any,
) -> str | None:
    """Map one exchange label to its interoperability group."""

    token = normalize_exchange_token(value)
    if not token:
        return None
    return EXCHANGE_TOKEN_TO_GROUP.get(token)


def classify_exchange_evidence(
    input_exchange: str | None,
    provider_exchanges: list[str],
) -> str:
    """
    Classify exchange evidence.

    MATCH if at least one SEC exchange maps to the same group as
    upstream. CONTRADICTION only when upstream maps and every mapped
    SEC exchange diverges. Otherwise UNKNOWN.
    """

    expected = exchange_group(input_exchange)
    if expected is None:
        return EVIDENCE_UNKNOWN

    provider_groups = {
        group
        for value in provider_exchanges
        for group in [exchange_group(value)]
        if group is not None
    }

    if not provider_groups:
        return EVIDENCE_UNKNOWN

    if expected in provider_groups:
        return EVIDENCE_MATCH

    return EVIDENCE_CONTRADICTION


# ============================================================================
# NAME EVIDENCE
# ============================================================================

def normalized_name_tokens(
    value: Any,
) -> set[str]:
    normalized = normalize_ascii_text(value)
    if not normalized:
        return set()
    return {
        token
        for token in normalized.split()
        if token not in CORPORATE_STOPWORDS
    }


def significant_name_tokens(
    value: Any,
) -> set[str]:
    tokens = normalized_name_tokens(value)
    if not tokens:
        return set()
    filtered = {token for token in tokens if len(token) > 1}
    return filtered or tokens


def token_is_truncation_of(
    shorter: str,
    longer: str,
) -> bool:
    return (
        longer.startswith(shorter)
        and len(longer) > len(shorter)
        and len(shorter) >= 5
    )


def tokens_are_false_friends(
    left: str,
    right: str,
) -> bool:
    if (
        right.startswith(left)
        and len(right) > len(left)
        and 3 <= len(left) <= 4
        and (len(right) - len(left)) >= 2
    ):
        return True
    if (
        left.startswith(right)
        and len(left) > len(right)
        and 3 <= len(right) <= 4
        and (len(left) - len(right)) >= 2
    ):
        return True
    return False


def abbreviation_versus_phrase(
    left_tokens: set[str],
    right_tokens: set[str],
) -> bool:
    if (
        len(left_tokens) == 1
        and len(right_tokens) >= 2
        and len(next(iter(left_tokens))) <= 4
    ):
        return True
    if (
        len(right_tokens) == 1
        and len(left_tokens) >= 2
        and len(next(iter(right_tokens))) <= 4
    ):
        return True
    return False


def name_side_is_informative(
    tokens: set[str],
) -> bool:
    if not tokens:
        return False
    if all(len(token) <= 1 for token in tokens):
        return False
    if len(tokens) == 1 and len(next(iter(tokens))) <= 2:
        return False
    return True


def remainder_tokens_compatible(
    left_only: set[str],
    right_only: set[str],
    left_all: set[str],
    right_all: set[str],
) -> bool:
    for token in left_only:
        compatible = any(
            token == other
            or token_is_truncation_of(token, other)
            or token_is_truncation_of(other, token)
            for other in right_all
        )
        if not compatible:
            return False

    for token in right_only:
        compatible = any(
            token == other
            or token_is_truncation_of(token, other)
            or token_is_truncation_of(other, token)
            for other in left_all
        )
        if not compatible:
            return False

    return True


def classify_name_evidence(
    expected_name: Any,
    candidate_name_value: Any,
) -> str:
    """
    Classify one name pair as MATCH, CONTRADICTION, or UNKNOWN.

    No global similarity-score gates. Prefer UNKNOWN over guessing.
    """

    left = significant_name_tokens(expected_name)
    right = significant_name_tokens(candidate_name_value)

    if not left or not right:
        return EVIDENCE_UNKNOWN

    if left == right or left <= right or right <= left:
        return EVIDENCE_MATCH

    shared = left & right
    if shared:
        left_only = left - shared
        right_only = right - shared
        if remainder_tokens_compatible(
            left_only,
            right_only,
            left,
            right,
        ):
            return EVIDENCE_MATCH
        return EVIDENCE_UNKNOWN

    if abbreviation_versus_phrase(left, right):
        return EVIDENCE_UNKNOWN

    if len(left) == 1 and len(right) == 1:
        left_token = next(iter(left))
        right_token = next(iter(right))
        if (
            token_is_truncation_of(left_token, right_token)
            or token_is_truncation_of(right_token, left_token)
        ):
            return EVIDENCE_MATCH
        if tokens_are_false_friends(left_token, right_token):
            return EVIDENCE_CONTRADICTION
        if (
            name_side_is_informative(left)
            and name_side_is_informative(right)
        ):
            return EVIDENCE_CONTRADICTION
        return EVIDENCE_UNKNOWN

    for left_token in left:
        for right_token in right:
            if tokens_are_false_friends(left_token, right_token):
                return EVIDENCE_CONTRADICTION

    if (
        name_side_is_informative(left)
        and name_side_is_informative(right)
    ):
        return EVIDENCE_CONTRADICTION

    return EVIDENCE_UNKNOWN


def classify_name_against_submissions(
    input_name: str | None,
    provider_name: str | None,
    provider_former_names: list[str],
) -> str:
    """
    Name evidence against current + former SEC names.

    MATCH if any SEC name strongly matches. CONTRADICTION only when
    every available SEC name is a clear contradiction. Otherwise
    UNKNOWN.
    """

    sec_names = [
        name
        for name in [provider_name, *provider_former_names]
        if optional_text(name) is not None
    ]

    if optional_text(input_name) is None or not sec_names:
        return EVIDENCE_UNKNOWN

    outcomes = [
        classify_name_evidence(input_name, name)
        for name in sec_names
    ]

    if EVIDENCE_MATCH in outcomes:
        return EVIDENCE_MATCH

    if outcomes and all(
        outcome == EVIDENCE_CONTRADICTION for outcome in outcomes
    ):
        return EVIDENCE_CONTRADICTION

    return EVIDENCE_UNKNOWN


# ============================================================================
# TICKER EVIDENCE
# ============================================================================

def classify_ticker_evidence(
    input_ticker: str | None,
    provider_tickers: list[str],
) -> str:
    """
    Classify ticker evidence against submissions.tickers.

    Empty / missing provider tickers → UNKNOWN (not CONTRADICTION).
    """

    ticker = optional_text(input_ticker)
    if ticker is None:
        return EVIDENCE_UNKNOWN

    normalized_provider = [
        value.casefold()
        for value in provider_tickers
        if optional_text(value) is not None
    ]

    if not normalized_provider:
        return EVIDENCE_UNKNOWN

    if ticker.casefold() in normalized_provider:
        return EVIDENCE_MATCH

    return EVIDENCE_CONTRADICTION


# ============================================================================
# HTTP CLASSIFICATION
# ============================================================================

def _response_body_preview(
    response: requests.Response,
) -> str:
    try:
        text = response.text or ""
    except Exception:
        return ""
    return text[:2000].casefold()


def is_explicit_rate_threshold_403(
    response: requests.Response,
) -> bool:
    """
    True only when a 403 body clearly indicates a temporary rate
    threshold (fail closed to Authentication otherwise).
    """

    body = _response_body_preview(response)
    if not body:
        return False

    rate_markers = (
        "rate threshold",
        "rate limit",
        "too many requests",
        "exceeded your rate",
        "temporary block",
    )
    return any(marker in body for marker in rate_markers)


def is_fair_access_or_user_agent_403(
    response: requests.Response,
) -> bool:
    """Detect SEC fair-access / undeclared-bot / User-Agent 403."""

    body = _response_body_preview(response)
    markers = (
        "user-agent",
        "user agent",
        "fair access",
        "undeclared automated",
        "automated access",
        "accessing-edgar-data",
        "please declare",
    )
    if any(marker in body for marker in markers):
        return True

    # Fail closed: non-rate 403 without clear rate semantics is Auth.
    return not is_explicit_rate_threshold_403(response)


def raise_for_sec_http_status(
    response: requests.Response,
    url: str,
) -> None:
    """
    Classify non-success SEC HTTP statuses.

    408 / 429 / 5xx → Transport (caller retries before raising)
    403 fair-access / User-Agent → Authentication
    403 explicit rate threshold → Transport
    other 4xx → Response
    """

    status = response.status_code

    if status in RETRYABLE_STATUS_CODES:
        raise ConnectorTransportError(
            "SEC EDGAR returned retryable HTTP status "
            f"{status}: {url}"
        )

    if status == 403:
        if is_explicit_rate_threshold_403(response):
            raise ConnectorTransportError(
                "SEC EDGAR returned HTTP 403 rate threshold: "
                f"{url}"
            )
        raise ConnectorAuthenticationError(
            "SEC EDGAR rejected the request (HTTP 403 fair-access "
            f"/ User-Agent): {url}"
        )

    if 400 <= status < 500:
        raise ConnectorResponseError(
            "SEC EDGAR request failed with "
            f"HTTP {status}: {url}"
        )

    if status >= 500:
        raise ConnectorTransportError(
            "SEC EDGAR request failed with "
            f"HTTP {status}: {url}"
        )

    response.raise_for_status()


def request_json(
    *,
    session: requests.Session,
    url: str,
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    backoff_seconds: float,
    allow_not_found: bool = False,
) -> Any | None:
    """
    Execute one retry-aware SEC GET and return decoded JSON.

    When allow_not_found is True, HTTP 404 returns None (identity
    absence path for submissions). Company Facts must not use this
    flag — post-validation 404 is a ResponseError.
    """

    timeout = (connect_timeout, read_timeout)

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(url, timeout=timeout)
        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    "SEC EDGAR transport request failed after "
                    "all retries."
                ) from error

            wait_seconds = backoff_seconds * (2 ** (attempt - 1))
            time.sleep(wait_seconds)
            continue

        if (
            allow_not_found
            and response.status_code == 404
        ):
            return None

        if response.status_code in RETRYABLE_STATUS_CODES:
            if attempt == max_attempts:
                raise ConnectorTransportError(
                    "SEC EDGAR returned retryable HTTP status "
                    "after all attempts: "
                    f"{response.status_code}"
                )

            retry_after = optional_text(
                response.headers.get("Retry-After")
            )
            if retry_after is not None:
                try:
                    wait_seconds = max(float(retry_after), 0.0)
                except ValueError:
                    wait_seconds = (
                        backoff_seconds * (2 ** (attempt - 1))
                    )
            else:
                wait_seconds = (
                    backoff_seconds * (2 ** (attempt - 1))
                )

            time.sleep(wait_seconds)
            continue

        if response.status_code != 200:
            raise_for_sec_http_status(response, url)

        try:
            return response.json()
        except ValueError as error:
            raise ConnectorResponseError(
                "SEC EDGAR response is not valid JSON."
            ) from error

    raise ConnectorTransportError(
        "SEC EDGAR request exhausted all configured attempts."
    )


def pace(
    request_interval_seconds: float,
) -> None:
    if request_interval_seconds > 0:
        time.sleep(request_interval_seconds)


# ============================================================================
# SEC TICKER REFERENCE (candidate generation only)
# ============================================================================

def build_ticker_cik_candidates(
    payload: Any,
) -> dict[str, list[dict[str, str | None]]]:
    """
    Build case-insensitive ticker → candidate CIK list.

    company_tickers.json is candidate generation only. Titles are
    retained as ticker_reference_title for audit/metadata and are
    never decision proof.
    """

    if not isinstance(payload, dict):
        raise ConnectorResponseError(
            "SEC ticker reference must be a JSON object."
        )

    lookup: dict[str, list[dict[str, str | None]]] = {}

    for item in payload.values():
        if not isinstance(item, dict):
            continue

        ticker = optional_text(item.get("ticker"))
        cik = normalize_cik(item.get("cik_str"))
        title = optional_text(item.get("title"))

        if ticker is None or cik is None:
            continue

        key = ticker.casefold()
        entry = {
            "cik": cik,
            "ticker_reference_title": title,
        }
        bucket = lookup.setdefault(key, [])

        # Deduplicate identical CIK rows; keep distinct CIKs.
        if any(existing["cik"] == cik for existing in bucket):
            continue
        bucket.append(entry)

    if not lookup:
        raise ConnectorResponseError(
            "SEC ticker reference resolved no ticker/CIK pairs."
        )

    return lookup


def submissions_url(
    submissions_endpoint: str,
    cik: str,
) -> str:
    """Build submissions URL from registry base + CIK path."""

    return (
        submissions_endpoint.rstrip("/")
        + "/CIK"
        + cik
        + ".json"
    )


def companyfacts_url(
    endpoint: str,
    cik: str,
) -> str:
    return endpoint.rstrip("/") + "/CIK" + cik + ".json"


def extract_submissions_identity(
    payload: Any,
    *,
    expected_cik: str,
) -> dict[str, Any] | None:
    """
    Extract identity fields from submissions JSON.

    Returns None when the payload is structurally unusable
    (INCONSISTENT_SUBMISSIONS_IDENTITY path).
    """

    if not isinstance(payload, dict):
        return None

    payload_cik = normalize_cik(payload.get("cik"))
    if (
        payload_cik is not None
        and payload_cik != expected_cik
    ):
        return None

    provider_name = optional_text(payload.get("name"))

    former_raw = payload.get("formerNames")
    former_names: list[str] = []
    if isinstance(former_raw, list):
        for item in former_raw:
            if isinstance(item, dict):
                name = optional_text(item.get("name"))
                if name is not None:
                    former_names.append(name)
            else:
                name = optional_text(item)
                if name is not None:
                    former_names.append(name)
    elif former_raw is not None:
        # Unexpected shape → unusable identity surface.
        return None

    tickers_raw = payload.get("tickers")
    provider_tickers: list[str] = []
    if tickers_raw is None:
        provider_tickers = []
    elif isinstance(tickers_raw, list):
        for item in tickers_raw:
            ticker = optional_text(item)
            if ticker is not None:
                provider_tickers.append(ticker)
    else:
        return None

    exchanges_raw = payload.get("exchanges")
    provider_exchanges: list[str] = []
    if exchanges_raw is None:
        provider_exchanges = []
    elif isinstance(exchanges_raw, list):
        for item in exchanges_raw:
            exchange = optional_text(item)
            if exchange is not None:
                provider_exchanges.append(exchange)
    else:
        return None

    return {
        "provider_name": provider_name,
        "provider_former_names": former_names,
        "provider_tickers": provider_tickers,
        "provider_exchanges": provider_exchanges,
        "provider_cik": payload_cik or expected_cik,
    }


# ============================================================================
# IDENTITY DECISION (pure)
# ============================================================================

def _empty_evidence() -> dict[str, str]:
    return {
        "ticker": EVIDENCE_UNKNOWN,
        "exchange": EVIDENCE_UNKNOWN,
        "name": EVIDENCE_UNKNOWN,
    }


def _base_resolution(
    *,
    input_ticker: str | None,
    candidate_cik: str | None,
    resolved_cik: str | None,
    status: str,
    identity_method: str,
    identity_reason: str,
    evidence: dict[str, str] | None = None,
    ticker_reference_title: str | None = None,
    provider_name: str | None = None,
    provider_former_names: list[str] | None = None,
    provider_tickers: list[str] | None = None,
    provider_exchanges: list[str] | None = None,
    candidate_ciks: list[str] | None = None,
    facts_empty: bool = False,
    identity_requests: int = 0,
) -> dict[str, Any]:
    facts_eligible = status in FACTS_ELIGIBLE_STATUSES
    return {
        "input_ticker": input_ticker,
        "candidate_cik": candidate_cik,
        "resolved_cik": resolved_cik,
        "candidate_ciks": list(candidate_ciks or []),
        "resolution_status": status,
        "facts_eligible": facts_eligible,
        "review_required": False,
        "identity_method": identity_method,
        "identity_reason": identity_reason,
        "evidence": evidence or _empty_evidence(),
        "provider_name": provider_name,
        "provider_former_names": list(provider_former_names or []),
        "provider_tickers": list(provider_tickers or []),
        "provider_exchanges": list(provider_exchanges or []),
        # Audit / metadata only — never used as decision proof.
        "ticker_reference_title": ticker_reference_title,
        "facts_empty": facts_empty,
        "identity_requests": identity_requests,
    }


def prioritize_contradiction_reason(
    evidence: dict[str, str],
) -> str:
    """Deterministic contradiction priority: ticker > exchange > name."""

    if evidence.get("ticker") == EVIDENCE_CONTRADICTION:
        return IDENTITY_REASON_EXACT_TICKER_CONTRADICTION
    if evidence.get("exchange") == EVIDENCE_CONTRADICTION:
        return IDENTITY_REASON_EXACT_EXCHANGE_CONTRADICTION
    if evidence.get("name") == EVIDENCE_CONTRADICTION:
        return IDENTITY_REASON_EXACT_NAME_CONTRADICTION
    return IDENTITY_REASON_EXACT_NO_CONTRADICTION


def decide_identity(
    *,
    input_ticker: str | None,
    input_name: str | None = None,
    input_exchange: str | None = None,
    candidate_entries: list[dict[str, str | None]] | None = None,
    submissions_identity: dict[str, Any] | None = None,
    submissions_absent: bool = False,
    identity_requests: int = 0,
) -> dict[str, Any]:
    """
    Pure identity decision for one instrument.

    candidate_entries: list of {cik, ticker_reference_title} from
    company_tickers (candidate generation only).

    submissions_identity: extracted submissions fields, or None.
    submissions_absent: True when submissions returned HTTP 404.

    ticker_reference_title is retained for audit only and never
    influences MATCH / CONTRADICTION / status.
    """

    entries = list(candidate_entries or [])
    titles = [
        optional_text(entry.get("ticker_reference_title"))
        for entry in entries
    ]
    title_audit = next(
        (title for title in titles if title is not None),
        None,
    )
    ciks = [
        cik
        for cik in (
            normalize_cik(entry.get("cik")) for entry in entries
        )
        if cik is not None
    ]
    # Preserve order while uniquing.
    unique_ciks: list[str] = []
    for cik in ciks:
        if cik not in unique_ciks:
            unique_ciks.append(cik)

    ticker = optional_text(input_ticker)
    if ticker is None:
        return _base_resolution(
            input_ticker=None,
            candidate_cik=None,
            resolved_cik=None,
            status=IDENTITY_NOT_FOUND_STATUS,
            identity_method=IDENTITY_METHOD_NONE,
            identity_reason=IDENTITY_REASON_MISSING_INPUT_TICKER,
            ticker_reference_title=title_audit,
            candidate_ciks=unique_ciks,
            identity_requests=identity_requests,
        )

    if not unique_ciks:
        return _base_resolution(
            input_ticker=ticker,
            candidate_cik=None,
            resolved_cik=None,
            status=IDENTITY_NOT_FOUND_STATUS,
            identity_method=IDENTITY_METHOD_NONE,
            identity_reason=(
                IDENTITY_REASON_TICKER_NOT_IN_REFERENCE
            ),
            ticker_reference_title=title_audit,
            candidate_ciks=[],
            identity_requests=identity_requests,
        )

    if len(unique_ciks) > 1:
        return _base_resolution(
            input_ticker=ticker,
            candidate_cik=None,
            resolved_cik=None,
            status=IDENTITY_AMBIGUOUS_STATUS,
            identity_method=(
                IDENTITY_METHOD_TICKER_REFERENCE_SUBMISSIONS
            ),
            identity_reason=(
                IDENTITY_REASON_MULTIPLE_CIK_CANDIDATES
            ),
            ticker_reference_title=title_audit,
            candidate_ciks=unique_ciks,
            identity_requests=identity_requests,
        )

    candidate_cik = unique_ciks[0]

    if submissions_absent:
        return _base_resolution(
            input_ticker=ticker,
            candidate_cik=candidate_cik,
            resolved_cik=None,
            status=IDENTITY_NOT_FOUND_STATUS,
            identity_method=(
                IDENTITY_METHOD_TICKER_REFERENCE_SUBMISSIONS
            ),
            identity_reason=(
                IDENTITY_REASON_SUBMISSIONS_IDENTITY_ABSENT
            ),
            ticker_reference_title=title_audit,
            candidate_ciks=unique_ciks,
            identity_requests=identity_requests,
        )

    if submissions_identity is None:
        return _base_resolution(
            input_ticker=ticker,
            candidate_cik=candidate_cik,
            resolved_cik=None,
            status=IDENTITY_AMBIGUOUS_STATUS,
            identity_method=(
                IDENTITY_METHOD_TICKER_REFERENCE_SUBMISSIONS
            ),
            identity_reason=(
                IDENTITY_REASON_INCONSISTENT_SUBMISSIONS_IDENTITY
            ),
            ticker_reference_title=title_audit,
            candidate_ciks=unique_ciks,
            identity_requests=identity_requests,
        )

    provider_name = optional_text(
        submissions_identity.get("provider_name")
    )
    provider_former_names = list(
        submissions_identity.get("provider_former_names") or []
    )
    provider_tickers = list(
        submissions_identity.get("provider_tickers") or []
    )
    provider_exchanges = list(
        submissions_identity.get("provider_exchanges") or []
    )

    evidence = {
        "ticker": classify_ticker_evidence(
            ticker,
            provider_tickers,
        ),
        "exchange": classify_exchange_evidence(
            input_exchange,
            provider_exchanges,
        ),
        "name": classify_name_against_submissions(
            input_name,
            provider_name,
            provider_former_names,
        ),
    }

    has_material_contradiction = any(
        value == EVIDENCE_CONTRADICTION
        for value in evidence.values()
    )

    # DIRECT: ticker MATCH, or ticker UNKNOWN when submissions.tickers
    # empty (UNKNOWN ≠ CONTRADICTION), and no exchange/name contradiction.
    ticker_ok = (
        evidence["ticker"] == EVIDENCE_MATCH
        or evidence["ticker"] == EVIDENCE_UNKNOWN
    )

    if ticker_ok and not has_material_contradiction:
        return _base_resolution(
            input_ticker=ticker,
            candidate_cik=candidate_cik,
            resolved_cik=candidate_cik,
            status=DIRECT_VALIDATED_STATUS,
            identity_method=(
                IDENTITY_METHOD_TICKER_REFERENCE_SUBMISSIONS
            ),
            identity_reason=(
                IDENTITY_REASON_EXACT_NO_CONTRADICTION
            ),
            evidence=evidence,
            ticker_reference_title=title_audit,
            provider_name=provider_name,
            provider_former_names=provider_former_names,
            provider_tickers=provider_tickers,
            provider_exchanges=provider_exchanges,
            candidate_ciks=unique_ciks,
            identity_requests=identity_requests,
        )

    return _base_resolution(
        input_ticker=ticker,
        candidate_cik=candidate_cik,
        resolved_cik=None,
        status=IDENTITY_AMBIGUOUS_STATUS,
        identity_method=(
            IDENTITY_METHOD_TICKER_REFERENCE_SUBMISSIONS
        ),
        identity_reason=prioritize_contradiction_reason(evidence),
        evidence=evidence,
        ticker_reference_title=title_audit,
        provider_name=provider_name,
        provider_former_names=provider_former_names,
        provider_tickers=provider_tickers,
        provider_exchanges=provider_exchanges,
        candidate_ciks=unique_ciks,
        identity_requests=identity_requests,
    )


# ============================================================================
# COMPANY FACTS NORMALIZATION
# ============================================================================

def normalize_companyfacts(
    payload: Any,
    *,
    input_position: int,
    input_index: Any,
    input_ticker: str,
    cik: str,
) -> list[dict[str, Any]]:
    """Flatten all SEC Company Facts concepts and observations."""

    if not isinstance(payload, dict):
        raise ConnectorResponseError(
            "SEC Company Facts response must be a JSON object."
        )

    payload_cik = normalize_cik(payload.get("cik"))
    if payload_cik is not None and payload_cik != cik:
        raise ConnectorResponseError(
            "SEC Company Facts CIK does not match "
            "the DIRECT_VALIDATED CIK."
        )

    entity_name = optional_text(payload.get("entityName"))
    facts = payload.get("facts")

    if not isinstance(facts, dict):
        raise ConnectorResponseError(
            "SEC Company Facts response is missing its facts object."
        )

    records: list[dict[str, Any]] = []

    for taxonomy, concepts in facts.items():
        if not isinstance(concepts, dict):
            continue

        for concept, concept_data in concepts.items():
            if not isinstance(concept_data, dict):
                continue

            label = optional_text(concept_data.get("label"))
            description = optional_text(
                concept_data.get("description")
            )
            units = concept_data.get("units")

            if not isinstance(units, dict):
                continue

            for unit, observations in units.items():
                if not isinstance(observations, list):
                    continue

                for observation in observations:
                    if not isinstance(observation, dict):
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
                        }
                    )

    return records


def build_output_frame(
    records: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Build the source-faithful normalized SEC output frame.

    Empty frames are allowed when no instrument is facts-eligible
    or every validated instrument returned structurally empty facts.
    """

    dataframe = pd.DataFrame(records, columns=list(OUTPUT_COLUMNS))
    return dataframe.reset_index(drop=True)


# ============================================================================
# CONNECTOR
# ============================================================================

class SecEdgarConnector(
    BaseConnector,
):
    """Identity-first SEC EDGAR Company Facts connector."""

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

        if not isinstance(parameters, dict):
            raise ConnectorConfigurationError(
                "SEC EDGAR Parameters must be a dictionary."
            )

        missing = [
            parameter
            for parameter in REQUIRED_PARAMETERS
            if optional_text(parameters.get(parameter)) is None
        ]
        if missing:
            raise ConnectorConfigurationError(
                "SEC EDGAR Parameters are missing: "
                f"{missing}"
            )

        unsupported = sorted(set(parameters) - ALLOWED_PARAMETERS)
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

        super().validate_context(context)

        endpoint = optional_text(context.endpoint)
        if endpoint is None:
            raise ConnectorConfigurationError(
                "SEC EDGAR requires Access Endpoint."
            )

        cls.validate_parameters(context.parameters)

        if context.input_data is None:
            raise ConnectorConfigurationError(
                "SEC EDGAR requires an upstream input Dataset."
            )

        ticker_column = optional_text(
            context.parameters.get(TICKER_COLUMN_PARAMETER)
        )
        name_column = optional_text(
            context.parameters.get(NAME_COLUMN_PARAMETER)
        )
        exchange_column = optional_text(
            context.parameters.get(EXCHANGE_COLUMN_PARAMETER)
        )
        ticker_reference_endpoint = optional_text(
            context.parameters.get(
                TICKER_REFERENCE_ENDPOINT_PARAMETER
            )
        )
        submissions_endpoint = optional_text(
            context.parameters.get(SUBMISSIONS_ENDPOINT_PARAMETER)
        )

        if (
            ticker_column is None
            or name_column is None
            or exchange_column is None
            or ticker_reference_endpoint is None
            or submissions_endpoint is None
        ):
            raise ConnectorConfigurationError(
                "SEC EDGAR execution configuration is incomplete."
            )

        validate_input_data(
            context.input_data,
            ticker_column,
            name_column,
            exchange_column,
        )
        resolve_runtime_options(context.runtime_options)

    @classmethod
    def acquire(
        cls,
        context: ConnectorContext,
    ) -> ConnectorResult:
        """
        Resolve identity for every input instrument, then acquire
        Company Facts only for DIRECT_VALIDATED instruments.
        """

        cls.validate_context(context)

        endpoint = optional_text(context.endpoint)
        ticker_column = optional_text(
            context.parameters.get(TICKER_COLUMN_PARAMETER)
        )
        name_column = optional_text(
            context.parameters.get(NAME_COLUMN_PARAMETER)
        )
        exchange_column = optional_text(
            context.parameters.get(EXCHANGE_COLUMN_PARAMETER)
        )
        ticker_reference_endpoint = optional_text(
            context.parameters.get(
                TICKER_REFERENCE_ENDPOINT_PARAMETER
            )
        )
        submissions_endpoint = optional_text(
            context.parameters.get(SUBMISSIONS_ENDPOINT_PARAMETER)
        )

        if (
            endpoint is None
            or ticker_column is None
            or name_column is None
            or exchange_column is None
            or ticker_reference_endpoint is None
            or submissions_endpoint is None
        ):
            raise ConnectorConfigurationError(
                "SEC EDGAR execution configuration is incomplete."
            )

        runtime = resolve_runtime_options(context.runtime_options)
        user_agent = resolve_sec_user_agent()
        input_data = context.input_data.copy(deep=True)

        records: list[dict[str, Any]] = []
        identity_resolution: list[dict[str, Any]] = []
        companyfacts_requests = 0
        submissions_requests = 0
        ticker_reference_requests = 0
        direct_validated = 0
        facts_empty_count = 0

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
                url=ticker_reference_endpoint,
                connect_timeout=runtime["connect_timeout"],
                read_timeout=runtime["read_timeout"],
                max_attempts=runtime["max_attempts"],
                backoff_seconds=runtime["backoff_seconds"],
            )
            ticker_reference_requests = 1
            pace(runtime["request_interval_seconds"])

            ticker_lookup = build_ticker_cik_candidates(
                ticker_payload
            )

            pending_facts: list[dict[str, Any]] = []

            for input_position, (
                input_index,
                row,
            ) in enumerate(input_data.iterrows()):
                ticker = optional_text(row[ticker_column])
                input_name = optional_text(row[name_column])
                input_exchange = optional_text(row[exchange_column])

                candidate_entries = (
                    list(
                        ticker_lookup.get(ticker.casefold(), [])
                    )
                    if ticker is not None
                    else []
                )

                identity_requests = 0
                submissions_identity = None
                submissions_absent = False

                unique_candidate_ciks = []
                for entry in candidate_entries:
                    cik = normalize_cik(entry.get("cik"))
                    if (
                        cik is not None
                        and cik not in unique_candidate_ciks
                    ):
                        unique_candidate_ciks.append(cik)

                # Fetch submissions only for a unique CIK candidate.
                if (
                    ticker is not None
                    and len(unique_candidate_ciks) == 1
                ):
                    cik = unique_candidate_ciks[0]
                    payload = request_json(
                        session=session,
                        url=submissions_url(
                            submissions_endpoint,
                            cik,
                        ),
                        connect_timeout=runtime["connect_timeout"],
                        read_timeout=runtime["read_timeout"],
                        max_attempts=runtime["max_attempts"],
                        backoff_seconds=runtime["backoff_seconds"],
                        allow_not_found=True,
                    )
                    submissions_requests += 1
                    identity_requests += 1
                    pace(runtime["request_interval_seconds"])

                    if payload is None:
                        submissions_absent = True
                    else:
                        submissions_identity = (
                            extract_submissions_identity(
                                payload,
                                expected_cik=cik,
                            )
                        )

                decision = decide_identity(
                    input_ticker=ticker,
                    input_name=input_name,
                    input_exchange=input_exchange,
                    candidate_entries=candidate_entries,
                    submissions_identity=submissions_identity,
                    submissions_absent=submissions_absent,
                    identity_requests=identity_requests,
                )

                resolution_row = {
                    "input_position": input_position,
                    "input_index": input_index,
                    **decision,
                }
                identity_resolution.append(resolution_row)

                if decision["facts_eligible"]:
                    direct_validated += 1
                    pending_facts.append(
                        {
                            "input_position": input_position,
                            "input_index": input_index,
                            "input_ticker": decision["input_ticker"],
                            "cik": decision["resolved_cik"],
                            "resolution_index": (
                                len(identity_resolution) - 1
                            ),
                        }
                    )

            for item in pending_facts:
                facts_url = companyfacts_url(endpoint, item["cik"])
                payload = request_json(
                    session=session,
                    url=facts_url,
                    connect_timeout=runtime["connect_timeout"],
                    read_timeout=runtime["read_timeout"],
                    max_attempts=runtime["max_attempts"],
                    backoff_seconds=runtime["backoff_seconds"],
                    allow_not_found=False,
                )
                companyfacts_requests += 1
                pace(runtime["request_interval_seconds"])

                fact_rows = normalize_companyfacts(
                    payload,
                    input_position=item["input_position"],
                    input_index=item["input_index"],
                    input_ticker=item["input_ticker"],
                    cik=item["cik"],
                )

                if not fact_rows:
                    facts_empty_count += 1
                    identity_resolution[
                        item["resolution_index"]
                    ]["facts_empty"] = True
                else:
                    records.extend(fact_rows)

        dataframe = build_output_frame(records)

        if not dataframe.empty:
            direct_tickers = {
                optional_text(row.get("input_ticker"))
                for row in identity_resolution
                if row.get("resolution_status")
                == DIRECT_VALIDATED_STATUS
            }
            direct_tickers.discard(None)
            data_tickers = {
                optional_text(value)
                for value in dataframe["input_ticker"].tolist()
            }
            data_tickers.discard(None)
            unexpected = sorted(data_tickers - direct_tickers)
            if unexpected:
                raise ConnectorResponseError(
                    "SEC Company Facts data contains input_ticker "
                    "values outside DIRECT_VALIDATED identity "
                    f"resolution: {unexpected}"
                )

        resolution_counts = {
            status: 0 for status in sorted(RESOLUTION_STATUSES)
        }
        for row in identity_resolution:
            status = row["resolution_status"]
            resolution_counts[status] = (
                resolution_counts.get(status, 0) + 1
            )

        metadata = {
            "input_records": len(input_data),
            "resolution_counts": resolution_counts,
            "identity_resolution": identity_resolution,
            "direct_validated": direct_validated,
            "facts_eligible": direct_validated,
            "ticker_reference_requests": ticker_reference_requests,
            "submissions_requests": submissions_requests,
            "companyfacts_requests": companyfacts_requests,
            "normalized_records": len(dataframe),
            "fact_records": len(dataframe),
            "facts_empty_instruments": facts_empty_count,
        }

        logger.info(
            "SEC EDGAR acquisition completed: "
            "Input=%s; DIRECT_VALIDATED=%s; "
            "Ambiguous=%s; NotFound=%s; "
            "Submissions=%s; FactsRequests=%s; FactRows=%s",
            metadata["input_records"],
            resolution_counts.get(DIRECT_VALIDATED_STATUS, 0),
            resolution_counts.get(IDENTITY_AMBIGUOUS_STATUS, 0),
            resolution_counts.get(IDENTITY_NOT_FOUND_STATUS, 0),
            metadata["submissions_requests"],
            metadata["companyfacts_requests"],
            metadata["fact_records"],
        )

        return ConnectorResult(
            data=dataframe,
            metadata=metadata,
        )
