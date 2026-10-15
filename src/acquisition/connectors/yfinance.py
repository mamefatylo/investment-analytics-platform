"""
yfinance.py

Source-faithful Yahoo Finance market-data connector with defensive
instrument identity validation.

Purpose
-------
Acquire Yahoo Finance market data while preventing acquisition under an
incorrect instrument identity.

Identity architecture
---------------------
The configured Yahoo symbol remains the requested provider identifier.

Identity decisions use independent evidence states, not numeric score
thresholds. Each provider-side dimension is classified as:

- MATCH
- CONTRADICTION
- UNKNOWN

Evidence dimensions:

- exact_symbol
- quote_type
- exchange
- name

For each instrument:

1. Look up the configured symbol first with non-fuzzy Yahoo Search.
   If that search does not return the exact symbol, read yfinance
   Ticker.info for that symbol only. Do not download prices during
   identity validation.
2. Evaluate the exact candidate with independent evidence states.
3. DIRECT_VALIDATED (and automatic download) requires exact_symbol
   MATCH and no material CONTRADICTION on quote_type, exchange, or
   name. UNKNOWN evidence does not reject.
4. If the exact symbol is missing or has a material CONTRADICTION,
   fuzzy symbol search or company-name search may discover a strong
   alternative for review only.
5. SEARCH_RESOLVED alternatives are diagnostic only:
   - review_required=True
   - download_eligible=False
   - no automatic modification of the upstream securities master

Each identity decision also records:

- identity_method: EXACT_SEARCH | EXACT_TICKER_INFO |
  ALTERNATIVE_SEARCH | NONE
- identity_reason: a small closed set of technical reasons
  (exact no-contradiction / exact contradictions by dimension /
  unique or multiple strong alternatives / no provider identity)

IDENTITY_NOT_FOUND means Yahoo returned no observable identity at all
(no exact candidate and no alternative candidates) after a successful
provider response. Candidates that exist but cannot be validated are
IDENTITY_AMBIGUOUS (or SEARCH_RESOLVED when a unique strong alternative
exists).

Technical Yahoo failures are never converted into empty candidate lists
or IDENTITY_NOT_FOUND. Transport, authentication, dependency, and
response-contract failures raise the corresponding connector exceptions
from base.py. A Yahoo HTTP 404 that correctly reports quote/symbol not
found is provider identity absence (not transport): exact Ticker.info
returns None so alternative discovery can continue; Search treats it as
an empty candidate list. Only successful emptiness or that absence
signal may yield an empty candidate list and NO_PROVIDER_IDENTITY.

The connector never:
- promotes RAW data to processed or universe;
- modifies the upstream security master;
- invokes another provider;
- performs provider fallback;
- applies portfolio or analytical rules;
- writes final datasets;
- writes acquisition logs.

Yahoo Search is therefore used as provider-side identity evidence and
discovery, not as an authoritative identifier-mapping service.
"""

import importlib
import re
import unicodedata
from typing import Any

import pandas as pd

from src.acquisition.connectors.base import (
    BaseConnector,
    ConnectorAuthenticationError,
    ConnectorConfigurationError,
    ConnectorContext,
    ConnectorDependencyError,
    ConnectorResponseError,
    ConnectorResult,
    ConnectorTransportError,
)
from src.utils.logger import logger


# ============================================================================
# CONFIGURATION
# ============================================================================

SYMBOL_COLUMN_PARAMETER = "symbol_column"
NAME_COLUMN_PARAMETER = "name_column"
EXCHANGE_COLUMN_PARAMETER = "exchange_column"

REQUIRED_PARAMETERS = {
    SYMBOL_COLUMN_PARAMETER,
    NAME_COLUMN_PARAMETER,
    EXCHANGE_COLUMN_PARAMETER,
}

SUPPORTED_DOWNLOAD_PARAMETERS = {
    "period",
    "start",
    "end",
    "interval",
    "actions",
    "auto_adjust",
    "back_adjust",
    "repair",
    "keepna",
    "prepost",
    "rounding",
    "ignore_tz",
}

CONNECTOR_MANAGED_PARAMETERS = {
    "tickers",
    "threads",
    "timeout",
    "progress",
    "group_by",
    "session",
    "multi_level_index",
}

SUPPORTED_DATASET_PARAMETERS = (
    SUPPORTED_DOWNLOAD_PARAMETERS
    | REQUIRED_PARAMETERS
)

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_THREADS = True
DEFAULT_SEARCH_RESULTS = 10

DEFAULT_RUNTIME_OPTIONS = {
    "timeout": DEFAULT_TIMEOUT_SECONDS,
    "threads": DEFAULT_THREADS,
    "search_results": DEFAULT_SEARCH_RESULTS,
}

DOWNLOAD_GROUP_BY = "ticker"
DOWNLOAD_PROGRESS = False
DOWNLOAD_MULTI_LEVEL_INDEX = True


# ============================================================================
# IDENTITY STATUSES
# ============================================================================

DIRECT_VALIDATED_STATUS = "DIRECT_VALIDATED"
SEARCH_RESOLVED_STATUS = "SEARCH_RESOLVED"
IDENTITY_AMBIGUOUS_STATUS = "IDENTITY_AMBIGUOUS"
IDENTITY_NOT_FOUND_STATUS = "IDENTITY_NOT_FOUND"

DOWNLOAD_ELIGIBLE_STATUSES = frozenset({
    DIRECT_VALIDATED_STATUS,
})

REVIEW_REQUIRED_STATUSES = frozenset({
    SEARCH_RESOLVED_STATUS,
})


# ============================================================================
# IDENTITY METHOD / REASON
# ============================================================================
#
# identity_method — how the selected Yahoo identity was obtained.
# identity_reason — stable technical explanation of the decision.

IDENTITY_METHOD_EXACT_SEARCH = "EXACT_SEARCH"
IDENTITY_METHOD_EXACT_TICKER_INFO = "EXACT_TICKER_INFO"
IDENTITY_METHOD_ALTERNATIVE_SEARCH = "ALTERNATIVE_SEARCH"
IDENTITY_METHOD_NONE = "NONE"

IDENTITY_METHODS = frozenset({
    IDENTITY_METHOD_EXACT_SEARCH,
    IDENTITY_METHOD_EXACT_TICKER_INFO,
    IDENTITY_METHOD_ALTERNATIVE_SEARCH,
    IDENTITY_METHOD_NONE,
})

IDENTITY_REASON_EXACT_NO_CONTRADICTION = "EXACT_NO_CONTRADICTION"
IDENTITY_REASON_EXACT_NAME_CONTRADICTION = "EXACT_NAME_CONTRADICTION"
IDENTITY_REASON_EXACT_EXCHANGE_CONTRADICTION = (
    "EXACT_EXCHANGE_CONTRADICTION"
)
IDENTITY_REASON_EXACT_QUOTE_TYPE_CONTRADICTION = (
    "EXACT_QUOTE_TYPE_CONTRADICTION"
)
IDENTITY_REASON_UNIQUE_STRONG_ALTERNATIVE = (
    "UNIQUE_STRONG_ALTERNATIVE"
)
IDENTITY_REASON_MULTIPLE_STRONG_ALTERNATIVES = (
    "MULTIPLE_STRONG_ALTERNATIVES"
)
IDENTITY_REASON_NO_STRONG_ALTERNATIVE = "NO_STRONG_ALTERNATIVE"
IDENTITY_REASON_INCONSISTENT_EXACT_CANDIDATES = (
    "INCONSISTENT_EXACT_CANDIDATES"
)
IDENTITY_REASON_NO_PROVIDER_IDENTITY = "NO_PROVIDER_IDENTITY"

IDENTITY_REASONS = frozenset({
    IDENTITY_REASON_EXACT_NO_CONTRADICTION,
    IDENTITY_REASON_EXACT_NAME_CONTRADICTION,
    IDENTITY_REASON_EXACT_EXCHANGE_CONTRADICTION,
    IDENTITY_REASON_EXACT_QUOTE_TYPE_CONTRADICTION,
    IDENTITY_REASON_UNIQUE_STRONG_ALTERNATIVE,
    IDENTITY_REASON_MULTIPLE_STRONG_ALTERNATIVES,
    IDENTITY_REASON_NO_STRONG_ALTERNATIVE,
    IDENTITY_REASON_INCONSISTENT_EXACT_CANDIDATES,
    IDENTITY_REASON_NO_PROVIDER_IDENTITY,
})

# When several material contradictions exist on the exact path,
# report one reason with this priority (highest first).
EXACT_CONTRADICTION_REASON_PRIORITY = (
    (
        "quote_type",
        IDENTITY_REASON_EXACT_QUOTE_TYPE_CONTRADICTION,
    ),
    (
        "exchange",
        IDENTITY_REASON_EXACT_EXCHANGE_CONTRADICTION,
    ),
    (
        "name",
        IDENTITY_REASON_EXACT_NAME_CONTRADICTION,
    ),
)


# ============================================================================
# EVIDENCE STATES
# ============================================================================

EVIDENCE_MATCH = "MATCH"
EVIDENCE_CONTRADICTION = "CONTRADICTION"
EVIDENCE_UNKNOWN = "UNKNOWN"

EVIDENCE_STATES = frozenset({
    EVIDENCE_MATCH,
    EVIDENCE_CONTRADICTION,
    EVIDENCE_UNKNOWN,
})

ACCEPTABLE_QUOTE_TYPES = frozenset({
    "equity",
    "etf",
})

# Corporate/legal suffixes are weak identity information.
# They are removed from name comparisons.
CORPORATE_STOPWORDS = frozenset({
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
    "nv",
    "oyj",
    "plc",
    "publ",
    "sa",
    "sab",
    "spa",
    "se",
})


# ============================================================================
# YAHOO EXCHANGE TAXONOMY
# ============================================================================
#
# EXCHANGE_GROUPS are provider interoperability aliases only
# (human-readable exchange labels ↔ Yahoo codes). They are not a
# business exchange master.
#
# The upstream Exchange field may contain human-readable exchange names,
# while Yahoo Search commonly returns provider codes such as HKG, JPX,
# NMS, TOR or SES.
#
# Exchange comparison therefore uses explicit equivalence groups.
#
# IMPORTANT:
# No substring matching is performed.
#
# "Hong Kong" must never match "Hanover" simply because text happens to
# share characters.

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
        "brusselsstockexchange",
        "euronextbrussels",
        "bru",
    },
    "AMSTERDAM": {
        "amsterdam",
        "euronextamsterdam",
        "ams",
    },
    "LISBON": {
        "lisbon",
        "lisbonstockexchange",
        "euronextlisbon",
        "lis",
    },
    "MILAN": {
        "milan",
        "borsaitaliana",
        "mil",
    },
    "STOCKHOLM": {
        "stockholm",
        "nasdaqstockholm",
        "stockholmstockexchange",
        "sto",
    },
    "COPENHAGEN": {
        "copenhagen",
        "nasdaqcopenhagen",
        "cph",
    },
    "OSLO": {
        "oslo",
        "oslobors",
        "osl",
    },
    "VIENNA": {
        "vienna",
        "viennastockexchange",
        "vie",
    },
    "AUSTRALIA": {
        "australian",
        "australiansecuritiesexchange",
        "asx",
    },
    "SINGAPORE": {
        "singapore",
        "singaporeexchange",
        "sgx",
        "ses",
    },
    "TAIWAN": {
        "taiwan",
        "taiwanstockexchange",
        "twse",
        "tai",
    },
    "MEXICO": {
        "mexico",
        "bolsamexicanadevalores",
        "bmv",
        "mex",
    },
    "JOHANNESBURG": {
        "johannesburg",
        "johannesburgstockexchange",
        "jse",
        "jnb",
    },
    "SAUDI": {
        "saudi",
        "saudistockexchange",
        "tadawul",
        "sau",
    },
    "TEL_AVIV": {
        "telaviv",
        "telavivstockexchange",
        "tase",
        "tlv",
    },
    "GERMANY": {
        "xetra",
        "frankfurt",
        "frankfurtstockexchange",
        "deutscheboerse",
        "ger",
        "fra",
    },
    "MADRID": {
        "madrid",
        "bolsademadrid",
        "bme",
        "mce",
    },
}


# ============================================================================
# GENERIC HELPERS
# ============================================================================

def optional_text(value: Any) -> str | None:
    if value is None:
        return None

    if not isinstance(
        value,
        (dict, list, tuple, set),
    ):
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass

    normalized = str(value).strip()

    return normalized or None


def positive_number(
    value: Any,
    field_name: str,
) -> float:
    if isinstance(value, bool):
        raise ConnectorConfigurationError(
            f"{field_name} must be positive."
        )

    try:
        normalized = float(value)
    except (TypeError, ValueError) as error:
        raise ConnectorConfigurationError(
            f"{field_name} must be numeric."
        ) from error

    if normalized <= 0:
        raise ConnectorConfigurationError(
            f"{field_name} must be greater than zero."
        )

    return normalized


def positive_integer(
    value: Any,
    field_name: str,
) -> int:
    if isinstance(value, bool):
        raise ConnectorConfigurationError(
            f"{field_name} must be a positive integer."
        )

    if isinstance(value, int):
        normalized = value

    elif isinstance(value, str):
        stripped = value.strip()

        if not stripped.isdigit():
            raise ConnectorConfigurationError(
                f"{field_name} must be a positive integer."
            )

        normalized = int(stripped)

    else:
        raise ConnectorConfigurationError(
            f"{field_name} must be a positive integer."
        )

    if normalized <= 0:
        raise ConnectorConfigurationError(
            f"{field_name} must be greater than zero."
        )

    return normalized


def normalize_threads(
    value: Any,
) -> bool | int:
    if isinstance(value, bool):
        return value

    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value > 0
    ):
        return value

    raise ConnectorConfigurationError(
        "threads must be True, False, or a positive integer."
    )


# ============================================================================
# TEXT NORMALIZATION
# ============================================================================

def normalize_ascii_text(
    value: Any,
) -> str:
    text = optional_text(value)

    if text is None:
        return ""

    text = unicodedata.normalize(
        "NFKD",
        text,
    )

    text = "".join(
        character
        for character in text
        if not unicodedata.combining(character)
    )

    text = text.casefold()

    text = re.sub(
        r"[^a-z0-9]+",
        " ",
        text,
    )

    return " ".join(
        text.split()
    )


def normalize_provider_field_name(
    value: Any,
) -> str:
    normalized = optional_text(value)

    if normalized is None:
        raise ConnectorResponseError(
            "yfinance returned an empty field name."
        )

    normalized = re.sub(
        r"[^a-z0-9]+",
        "_",
        normalized.casefold(),
    )

    normalized = re.sub(
        r"_+",
        "_",
        normalized,
    ).strip("_")

    if not normalized:
        raise ConnectorResponseError(
            f"Unable to normalize yfinance field name: {value}"
        )

    return normalized


def normalized_name_tokens(
    value: Any,
) -> set[str]:
    normalized = normalize_ascii_text(
        value
    )

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
    """
    Name tokens used for evidence classification.

    Single-character tokens are treated as truncation debris and
    ignored when longer tokens remain.
    """

    tokens = normalized_name_tokens(
        value
    )

    if not tokens:
        return set()

    filtered = {
        token
        for token in tokens
        if len(token) > 1
    }

    return filtered or tokens


def token_is_truncation_of(
    shorter: str,
    longer: str,
) -> bool:
    """
    True when shorter is a meaningful truncation of longer.

    Short stems (length <= 4) are rejected here so false friends such
    as "sap" / "saputo" are not treated as the same company name.
    """

    return (
        longer.startswith(shorter)
        and len(longer) > len(shorter)
        and len(shorter) >= 5
    )


def tokens_are_false_friends(
    left: str,
    right: str,
) -> bool:
    """
    Detect short proper-prefix collisions between distinct words.

    Example: "sap" versus "saputo".

    Single-character and two-letter stubs are excluded so truncated
    debris such as "a" / "apple" stays UNKNOWN rather than CONTRADICTION.
    """

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
    """
    Weak identity: a short single token against a multi-token name.

    Example: "IBM" versus "International Business Machines".
    This is UNKNOWN, not CONTRADICTION.
    """

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
    """
    True when a name side has enough content to support a firm
    MATCH or CONTRADICTION claim.

    Insufficient / non-comparable evidence includes missing names,
    single-character debris, and ultra-short lone stubs. Legal
    suffixes are already stripped before this check.
    """

    if not tokens:
        return False

    if all(
        len(token) <= 1
        for token in tokens
    ):
        return False

    if (
        len(tokens) == 1
        and len(next(iter(tokens))) <= 2
    ):
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
    Classify name identity as MATCH, CONTRADICTION, or UNKNOWN.

    Conservative rules of thumb
    ---------------------------
    - strong match → MATCH
    - both sides sufficiently informative and clearly incompatible
      company identities → CONTRADICTION
    - missing, weak, abbreviated, truncated, or otherwise
      non-comparable evidence → UNKNOWN

    Prefer UNKNOWN over guessing. Do not use global similarity-score
    gates. UNKNOWN must never be used to paper over a clear
    different-company signal when both names are informative.

    Examples
    --------
    - SAP SE versus Saputo Inc. → CONTRADICTION (false-friend stem)
    - Microsoft versus Apple → CONTRADICTION (informative, no shared core)
    - Truncated CapitaLand names versus full CapitaLand names →
      MATCH or UNKNOWN, never forced CONTRADICTION
    - IBM versus International Business Machines → UNKNOWN
    """

    left = significant_name_tokens(
        expected_name
    )

    right = significant_name_tokens(
        candidate_name_value
    )

    if not left or not right:
        return EVIDENCE_UNKNOWN

    if (
        left == right
        or left <= right
        or right <= left
    ):
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

        # Shared core with incompatible leftovers is uncertain,
        # not proof of distinct issuers.
        return EVIDENCE_UNKNOWN

    if abbreviation_versus_phrase(
        left,
        right,
    ):
        return EVIDENCE_UNKNOWN

    if len(left) == 1 and len(right) == 1:
        left_token = next(iter(left))
        right_token = next(iter(right))

        if (
            token_is_truncation_of(
                left_token,
                right_token,
            )
            or token_is_truncation_of(
                right_token,
                left_token,
            )
        ):
            return EVIDENCE_MATCH

        # Short proper-prefix collision between distinct words
        # (e.g. sap / saputo) is an obvious different-company case.
        if tokens_are_false_friends(
            left_token,
            right_token,
        ):
            return EVIDENCE_CONTRADICTION

        if (
            name_side_is_informative(left)
            and name_side_is_informative(right)
        ):
            return EVIDENCE_CONTRADICTION

        return EVIDENCE_UNKNOWN

    # Multi-token / mixed names with no shared core.
    for left_token in left:
        for right_token in right:
            if tokens_are_false_friends(
                left_token,
                right_token,
            ):
                return EVIDENCE_CONTRADICTION

    if (
        name_side_is_informative(left)
        and name_side_is_informative(right)
    ):
        return EVIDENCE_CONTRADICTION

    return EVIDENCE_UNKNOWN


# ============================================================================
# YFINANCE DEPENDENCY
# ============================================================================

def load_yfinance():
    try:
        module = importlib.import_module(
            "yfinance"
        )

    except (
        ModuleNotFoundError,
        ImportError,
    ) as error:
        raise ConnectorDependencyError(
            "The yfinance package could not be imported."
        ) from error

    if not callable(
        getattr(
            module,
            "download",
            None,
        )
    ):
        raise ConnectorDependencyError(
            "Installed yfinance does not expose download()."
        )

    if not callable(
        getattr(
            module,
            "Search",
            None,
        )
    ):
        raise ConnectorDependencyError(
            "Installed yfinance does not expose Search()."
        )

    if not callable(
        getattr(
            module,
            "Ticker",
            None,
        )
    ):
        raise ConnectorDependencyError(
            "Installed yfinance does not expose Ticker()."
        )

    return module


# ============================================================================
# INPUT VALIDATION
# ============================================================================

def validate_input_data(
    input_data: pd.DataFrame,
    *,
    symbol_column: str,
    name_column: str,
    exchange_column: str,
) -> None:
    if (
        not isinstance(
            input_data,
            pd.DataFrame,
        )
        or input_data.empty
    ):
        raise ConnectorConfigurationError(
            "yfinance requires non-empty input_data."
        )

    missing = sorted(
        {
            symbol_column,
            name_column,
            exchange_column,
        }
        - set(input_data.columns)
    )

    if missing:
        raise ConnectorConfigurationError(
            "yfinance input_data is missing configured columns: "
            f"{missing}"
        )

    missing_symbols = (
        input_data[symbol_column]
        .apply(optional_text)
        .isna()
    )

    if missing_symbols.any():
        raise ConnectorConfigurationError(
            "yfinance input contains empty configured symbols "
            "at input indices: "
            f"{input_data.index[missing_symbols].tolist()}"
        )


def build_instruments(
    input_data: pd.DataFrame,
    *,
    symbol_column: str,
    name_column: str,
    exchange_column: str,
) -> list[dict[str, Any]]:
    validate_input_data(
        input_data,
        symbol_column=symbol_column,
        name_column=name_column,
        exchange_column=exchange_column,
    )

    instruments: list[dict[str, Any]] = []
    seen: set[str] = set()

    for input_position, (
        input_index,
        row,
    ) in enumerate(
        input_data.iterrows()
    ):
        symbol = optional_text(
            row[symbol_column]
        )

        if symbol is None:
            raise ConnectorConfigurationError(
                "yfinance configured symbol cannot be empty."
            )

        key = symbol.casefold()

        if key in seen:
            raise ConnectorConfigurationError(
                "yfinance input contains duplicate configured symbols: "
                f"{symbol}"
            )

        seen.add(
            key
        )

        instruments.append({
            "input_position": input_position,
            "input_index": input_index,
            "configured_symbol": symbol,
            "input_name": optional_text(
                row[name_column]
            ),
            "input_exchange": optional_text(
                row[exchange_column]
            ),
            "resolved_symbol": None,
        })

    return instruments


# ============================================================================
# PARAMETER VALIDATION
# ============================================================================

def validate_date_range_parameters(
    parameters: dict[str, Any],
) -> None:
    period = optional_text(
        parameters.get("period")
    )

    start = optional_text(
        parameters.get("start")
    )

    end = optional_text(
        parameters.get("end")
    )

    if (
        period is not None
        and (
            start is not None
            or end is not None
        )
    ):
        raise ConnectorConfigurationError(
            "yfinance Parameters must use either period "
            "or start/end, not both."
        )


def build_download_parameters(
    parameters: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(
        parameters,
        dict,
    ):
        raise ConnectorConfigurationError(
            "yfinance Parameters must be a dictionary."
        )

    unsupported = sorted(
        set(parameters)
        - SUPPORTED_DATASET_PARAMETERS
    )

    if unsupported:
        raise ConnectorConfigurationError(
            "Unsupported yfinance Dataset Parameters: "
            f"{unsupported}"
        )

    forbidden = sorted(
        set(parameters)
        & CONNECTOR_MANAGED_PARAMETERS
    )

    if forbidden:
        raise ConnectorConfigurationError(
            "yfinance Dataset Parameters contain connector-managed "
            f"values: {forbidden}"
        )

    validate_date_range_parameters(
        parameters
    )

    return {
        key: value
        for key, value in parameters.items()
        if key in SUPPORTED_DOWNLOAD_PARAMETERS
    }


def resolve_runtime_options(
    runtime_options: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(
        runtime_options,
        dict,
    ):
        raise ConnectorConfigurationError(
            "runtime_options must be a dictionary."
        )

    allowed = {
        "timeout",
        "threads",
        "search_results",
    }

    unsupported = sorted(
        set(runtime_options)
        - allowed
    )

    if unsupported:
        raise ConnectorConfigurationError(
            "Unsupported yfinance runtime options: "
            f"{unsupported}"
        )

    return {
        "timeout": positive_number(
            runtime_options.get(
                "timeout",
                DEFAULT_RUNTIME_OPTIONS["timeout"],
            ),
            "timeout",
        ),
        "threads": normalize_threads(
            runtime_options.get(
                "threads",
                DEFAULT_RUNTIME_OPTIONS["threads"],
            )
        ),
        "search_results": positive_integer(
            runtime_options.get(
                "search_results",
                DEFAULT_RUNTIME_OPTIONS["search_results"],
            ),
            "search_results",
        ),
    }


# ============================================================================
# YAHOO PROVIDER FAILURE CLASSIFICATION
# ============================================================================
#
# One coherent place for Yahoo/yfinance exception semantics.
# Callers decide how identity_absent is handled (None / [] / raise).

YAHOO_FAILURE_IDENTITY_ABSENT = "identity_absent"
YAHOO_FAILURE_AUTHENTICATION = "authentication"
YAHOO_FAILURE_TRANSPORT = "transport"
YAHOO_FAILURE_RESPONSE = "response"


def yahoo_http_status_code(
    error: BaseException,
) -> int | None:
    """
    Best-effort HTTP status from urllib/requests-style exceptions.
    """

    for attribute in (
        "code",
        "status",
        "status_code",
    ):
        value = getattr(
            error,
            attribute,
            None,
        )

        if isinstance(
            value,
            int,
        ):
            return value

    response = getattr(
        error,
        "response",
        None,
    )

    if response is not None:
        for attribute in (
            "status_code",
            "status",
            "code",
        ):
            value = getattr(
                response,
                attribute,
                None,
            )

            if isinstance(
                value,
                int,
            ):
                return value

    match = re.search(
        r"\b([1-5][0-9]{2})\b",
        str(error),
    )

    if match is None:
        return None

    return int(
        match.group(1)
    )


def is_yahoo_quote_not_found_message(
    message: str,
) -> bool:
    """
    True when the message indicates Yahoo has no quote/symbol at the
    requested endpoint (identity absence, not transport failure).
    """

    strong_markers = (
        "quote not found",
        "symbol not found",
        "ticker not found",
    )

    if any(
        marker in message
        for marker in strong_markers
    ):
        return True

    weak_markers = (
        "not found",
        "no data found",
        "no fundamentals data",
    )

    return any(
        marker in message
        for marker in weak_markers
    )


def classify_yahoo_provider_failure(
    error: BaseException,
) -> str:
    """
    Classify a Yahoo/yfinance failure for connector mapping.

    Returns one of:
    - identity_absent: 404 + quote/symbol not found (provider correctly
      reports the identifier is absent at that endpoint)
    - authentication: 401 / 403
    - transport: 429 / rate limit, timeout, connection, network
    - response: malformed / unexpected / other non-transport HTTP
    """

    if isinstance(
        error,
        ConnectorAuthenticationError,
    ):
        return YAHOO_FAILURE_AUTHENTICATION

    if isinstance(
        error,
        ConnectorTransportError,
    ):
        return YAHOO_FAILURE_TRANSPORT

    if isinstance(
        error,
        (
            ConnectorConfigurationError,
            ConnectorDependencyError,
            ConnectorResponseError,
        ),
    ):
        return YAHOO_FAILURE_RESPONSE

    error_name = type(error).__name__
    message = str(error).casefold()
    status_code = yahoo_http_status_code(
        error
    )

    strong_not_found = any(
        marker in message
        for marker in (
            "quote not found",
            "symbol not found",
            "ticker not found",
        )
    )

    # Strong Yahoo wording is identity absence even if status extraction
    # fails. Weak "not found" wording requires an HTTP 404 signal.
    if strong_not_found or (
        (
            status_code == 404
            or "404" in message
        )
        and is_yahoo_quote_not_found_message(
            message
        )
    ):
        return YAHOO_FAILURE_IDENTITY_ABSENT

    if (
        status_code in {
            401,
            403,
        }
        or any(
            marker in message
            for marker in (
                "unauthorized",
                "forbidden",
                "authentication",
            )
        )
    ):
        # Prefer explicit auth status over vague message matches when
        # the status is clearly something else.
        if status_code is None or status_code in {
            401,
            403,
        }:
            return YAHOO_FAILURE_AUTHENTICATION

    if (
        status_code == 429
        or error_name == "YFRateLimitError"
        or any(
            marker in message
            for marker in (
                "too many requests",
                "rate limit",
            )
        )
    ):
        return YAHOO_FAILURE_TRANSPORT

    transport_names = {
        "ChunkedEncodingError",
        "ConnectTimeout",
        "ConnectionError",
        "ProxyError",
        "ReadTimeout",
        "RequestException",
        "SSLError",
        "Timeout",
        "TimeoutError",
        "URLError",
        "YFDataException",
    }

    transport_markers = (
        "timed out",
        "timeout",
        "connection",
        "network",
        "temporarily unavailable",
        "currently down",
        "will be right back",
    )

    if (
        error_name in transport_names
        or isinstance(
            error,
            (
                ConnectionError,
                TimeoutError,
                OSError,
            ),
        )
        or any(
            marker in message
            for marker in transport_markers
        )
    ):
        return YAHOO_FAILURE_TRANSPORT

    # HTTP 5xx is treated as transport (provider temporarily unusable).
    if (
        status_code is not None
        and 500 <= status_code <= 599
    ):
        return YAHOO_FAILURE_TRANSPORT

    # Remaining HTTPError (including bare 404 without quote-not-found
    # wording) is a response/contract issue, not blanket transport.
    return YAHOO_FAILURE_RESPONSE


def raise_yahoo_provider_failure(
    operation: str,
    error: BaseException,
) -> None:
    """
    Map a Yahoo/yfinance failure to a connector exception.

    Never returns. Technical provider failures must not be converted
    into empty candidate lists or IDENTITY_NOT_FOUND.

    For identity_absent (404 quote/symbol not found), raises
    ConnectorResponseError. Identity callers that should continue
    (Ticker.info → None, Search → []) must classify first and handle
    identity_absent without calling this helper.
    """

    kind = classify_yahoo_provider_failure(
        error
    )

    if kind == YAHOO_FAILURE_AUTHENTICATION:
        if isinstance(
            error,
            ConnectorAuthenticationError,
        ):
            raise error

        raise ConnectorAuthenticationError(
            f"Yahoo {operation} authentication was rejected."
        ) from error

    if kind == YAHOO_FAILURE_TRANSPORT:
        if isinstance(
            error,
            ConnectorTransportError,
        ):
            raise error

        raise ConnectorTransportError(
            f"Yahoo {operation} transport request failed."
        ) from error

    if kind == YAHOO_FAILURE_IDENTITY_ABSENT:
        raise ConnectorResponseError(
            f"Yahoo {operation} reported quote/symbol not found."
        ) from error

    if isinstance(
        error,
        (
            ConnectorConfigurationError,
            ConnectorDependencyError,
            ConnectorResponseError,
        ),
    ):
        raise error

    raise ConnectorResponseError(
        f"Yahoo {operation} returned an unusable response."
    ) from error


# ============================================================================
# YAHOO SEARCH
# ============================================================================


def normalize_search_candidate(
    candidate: Any,
) -> dict[str, Any] | None:
    if not isinstance(
        candidate,
        dict,
    ):
        return None

    symbol = optional_text(
        candidate.get("symbol")
    )

    if symbol is None:
        return None

    return {
        "symbol": symbol,
        "shortname": optional_text(
            candidate.get("shortname")
        ),
        "longname": optional_text(
            candidate.get("longname")
        ),
        "exchange": optional_text(
            candidate.get("exchange")
        ),
        "exchange_disp": optional_text(
            candidate.get("exchDisp")
        ),
        "quote_type": optional_text(
            candidate.get("quoteType")
        ),
        "type_disp": optional_text(
            candidate.get("typeDisp")
        ),
    }


def search_yahoo(
    *,
    yfinance_module,
    query: str,
    max_results: int,
    enable_fuzzy_query: bool,
) -> list[dict[str, Any]]:
    """
    Run one Yahoo Search query.

    A successful response with no quotes returns []. Quote/symbol not
    found (HTTP 404 style identity absence) also returns [] so the
    batch continues. Transport, authentication, and response-contract
    failures raise connector exceptions and must not be reported as
    IDENTITY_NOT_FOUND.
    """

    normalized_query = optional_text(
        query
    )

    if normalized_query is None:
        return []

    try:
        search = yfinance_module.Search(
            normalized_query,
            max_results=max_results,
            news_count=0,
            lists_count=0,
            include_cb=False,
            include_nav_links=False,
            include_research=False,
            enable_fuzzy_query=enable_fuzzy_query,
            recommended=0,
            raise_errors=True,
        )

        quotes = search.quotes

    except Exception as error:
        if (
            classify_yahoo_provider_failure(
                error
            )
            == YAHOO_FAILURE_IDENTITY_ABSENT
        ):
            return []

        raise_yahoo_provider_failure(
            "Search",
            error,
        )

    if quotes is None:
        return []

    if not isinstance(
        quotes,
        list,
    ):
        raise ConnectorResponseError(
            "Yahoo Search quotes must be a list."
        )

    output: list[dict[str, Any]] = []
    seen: set[str] = set()

    for candidate in quotes:
        normalized = normalize_search_candidate(
            candidate
        )

        if normalized is None:
            continue

        key = normalized[
            "symbol"
        ].casefold()

        if key in seen:
            continue

        seen.add(
            key
        )

        output.append(
            normalized
        )

    return output


def lookup_ticker_candidate(
    *,
    yfinance_module,
    symbol: str,
) -> dict[str, Any] | None:
    """
    Read provider identity for one exact Yahoo symbol.

    This uses Ticker info only. Historical prices are not downloaded.

    Returns None when Yahoo successfully indicates no usable identity
    for the symbol: empty/non-dict info, a different returned symbol,
    or HTTP 404 quote/symbol not found (provider identity absence).
    That None lets exact-path callers continue to alternative discovery.
    Transport, authentication, and other response-contract failures
    raise; they are not treated as identity absence.
    """

    try:
        ticker = yfinance_module.Ticker(
            symbol
        )
        info = getattr(
            ticker,
            "info",
            None,
        )

    except Exception as error:
        if (
            classify_yahoo_provider_failure(
                error
            )
            == YAHOO_FAILURE_IDENTITY_ABSENT
        ):
            return None

        raise_yahoo_provider_failure(
            "Ticker.info",
            error,
        )

    if info is None:
        return None

    if not isinstance(
        info,
        dict,
    ):
        raise ConnectorResponseError(
            "Yahoo Ticker.info must return a dictionary."
        )

    returned = optional_text(
        info.get("symbol")
    )

    if (
        returned is None
        or returned.casefold()
        != symbol.casefold()
    ):
        return None

    quote_type = optional_text(
        info.get("quoteType")
    )

    return {
        "symbol": returned,
        "shortname": optional_text(
            info.get("shortName")
        ),
        "longname": optional_text(
            info.get("longName")
        ),
        "exchange": optional_text(
            info.get("exchange")
        ),
        "exchange_disp": optional_text(
            info.get("fullExchangeName")
        ),
        "quote_type": quote_type,
        "type_disp": quote_type,
    }


def read_quote_currency(
    *,
    yfinance_module,
    symbol: str,
) -> str | None:
    """
    Return the quote currency Yahoo publishes for one symbol.

    The value is stored as Yahoo spells it, including a minor-unit
    code such as GBp. Nothing is converted to an ISO code here.
    """

    try:
        ticker = yfinance_module.Ticker(
            symbol
        )
        fast_info = getattr(
            ticker,
            "fast_info",
            None,
        )
        currency = None

        if fast_info is not None:
            currency = optional_text(
                fast_info.get(
                    "currency"
                )
            )

        if currency is None:
            info = getattr(
                ticker,
                "info",
                None,
            )
            if isinstance(
                info,
                dict,
            ):
                currency = optional_text(
                    info.get(
                        "currency"
                    )
                )

    except Exception as error:
        message = str(error).casefold()
        currency_absent = (
            isinstance(error, KeyError)
            or "quote not found" in message
            or "no data found" in message
            or classify_yahoo_provider_failure(error)
            == YAHOO_FAILURE_IDENTITY_ABSENT
        )
        if currency_absent:
            return None

        raise_yahoo_provider_failure(
            "quote currency",
            error,
        )

    return currency


def lookup_exact_candidates(
    *,
    yfinance_module,
    configured_symbol: str,
    max_results: int,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    str | None,
]:
    """
    Resolve the configured symbol without fuzzy Search ranking.

    Non-exact hits from this query are withheld until the exact
    candidate has been judged.

    Returns
    -------
    exact
        Candidates whose symbol equals the configured symbol.
    others
        Non-exact hits from the non-fuzzy symbol Search query.
    exact_identity_method
        EXACT_SEARCH when Search returned the exact symbol,
        EXACT_TICKER_INFO when Ticker.info supplied the only exact
        candidate, or None when no exact candidate exists.
    """

    exact: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []

    for candidate in search_yahoo(
        yfinance_module=yfinance_module,
        query=configured_symbol,
        max_results=max_results,
        enable_fuzzy_query=False,
    ):
        if (
            candidate["symbol"].casefold()
            == configured_symbol.casefold()
        ):
            exact.append(
                candidate
            )
        else:
            others.append(
                candidate
            )

    if exact:
        return (
            exact,
            others,
            IDENTITY_METHOD_EXACT_SEARCH,
        )

    ticker_candidate = lookup_ticker_candidate(
        yfinance_module=yfinance_module,
        symbol=configured_symbol,
    )

    if ticker_candidate is not None:
        exact.append(
            ticker_candidate
        )

        return (
            exact,
            others,
            IDENTITY_METHOD_EXACT_TICKER_INFO,
        )

    return (
        exact,
        others,
        None,
    )


def collect_alternative_candidates(
    *,
    yfinance_module,
    instrument: dict[str, Any],
    max_results: int,
    excluded_symbols: set[str],
) -> list[dict[str, Any]]:
    """
    Discover alternative listings after the exact symbol was judged.

    Company-name search and fuzzy symbol search are discovery only.
    They cannot validate the configured symbol.
    """

    queries: list[
        tuple[str, bool]
    ] = []

    input_name = optional_text(
        instrument.get(
            "input_name"
        )
    )

    if input_name is not None:
        queries.append(
            (
                input_name,
                False,
            )
        )

    queries.append(
        (
            instrument["configured_symbol"],
            True,
        )
    )

    candidates: list[dict[str, Any]] = []
    seen_symbols = set(
        excluded_symbols
    )
    used_queries: set[
        tuple[str, bool]
    ] = set()

    for (
        query_text,
        fuzzy,
    ) in queries:
        query_key = (
            query_text.casefold(),
            fuzzy,
        )

        if query_key in used_queries:
            continue

        used_queries.add(
            query_key
        )

        for candidate in search_yahoo(
            yfinance_module=yfinance_module,
            query=query_text,
            max_results=max_results,
            enable_fuzzy_query=fuzzy,
        ):
            key = candidate[
                "symbol"
            ].casefold()

            if key in seen_symbols:
                continue

            seen_symbols.add(
                key
            )

            candidates.append(
                candidate
            )

    return candidates


# ============================================================================
# IDENTITY VALIDATION
# ============================================================================

def candidate_name(
    candidate: dict[str, Any],
) -> str | None:
    return (
        optional_text(
            candidate.get("longname")
        )
        or optional_text(
            candidate.get("shortname")
        )
    )


def classify_quote_type_evidence(
    candidate: dict[str, Any],
) -> str:
    quote_type = normalize_ascii_text(
        candidate.get(
            "quote_type"
        )
    )

    if not quote_type:
        return EVIDENCE_UNKNOWN

    if quote_type in ACCEPTABLE_QUOTE_TYPES:
        return EVIDENCE_MATCH

    return EVIDENCE_CONTRADICTION


def normalize_exchange_token(
    value: Any,
) -> str:
    return re.sub(
        r"[^a-z0-9]",
        "",
        normalize_ascii_text(
            value
        ),
    )


def exchange_group(
    value: Any,
) -> str | None:
    token = normalize_exchange_token(
        value
    )

    if not token:
        return None

    matched_groups = [
        group
        for group, aliases in EXCHANGE_GROUPS.items()
        if token in aliases
    ]

    if len(matched_groups) == 1:
        return matched_groups[0]

    return None


def candidate_exchange_groups(
    candidate: dict[str, Any],
) -> set[str]:
    groups: set[str] = set()

    for value in (
        candidate.get("exchange"),
        candidate.get("exchange_disp"),
    ):
        group = exchange_group(
            value
        )

        if group is not None:
            groups.add(
                group
            )

    return groups


def classify_exchange_evidence(
    candidate: dict[str, Any],
    input_exchange: str | None,
) -> str:
    expected_group = exchange_group(
        input_exchange
    )

    candidate_groups = candidate_exchange_groups(
        candidate
    )

    if expected_group is None:
        return EVIDENCE_UNKNOWN

    if not candidate_groups:
        return EVIDENCE_UNKNOWN

    if expected_group in candidate_groups:
        return EVIDENCE_MATCH

    return EVIDENCE_CONTRADICTION


def classify_exact_symbol_evidence(
    candidate: dict[str, Any],
    configured_symbol: str,
) -> str:
    result_symbol = (
        optional_text(
            candidate.get("symbol")
        )
        or ""
    )

    if (
        result_symbol.casefold()
        == configured_symbol.casefold()
    ):
        return EVIDENCE_MATCH

    return EVIDENCE_CONTRADICTION


def candidate_evidence(
    candidate: dict[str, Any],
    *,
    configured_symbol: str,
    input_name: str | None,
    input_exchange: str | None,
) -> dict[str, Any]:
    actual_candidate_name = candidate_name(
        candidate
    )

    exact_symbol = classify_exact_symbol_evidence(
        candidate,
        configured_symbol,
    )

    quote_type = classify_quote_type_evidence(
        candidate
    )

    exchange = classify_exchange_evidence(
        candidate,
        input_exchange,
    )

    name = classify_name_evidence(
        input_name,
        actual_candidate_name,
    )

    return {
        "exact_symbol": exact_symbol,
        "quote_type": quote_type,
        "exchange": exchange,
        "name": name,
        "candidate_name": actual_candidate_name,
    }


def evidence_has_material_contradiction(
    evidence: dict[str, Any],
) -> bool:
    return any(
        evidence.get(field)
        == EVIDENCE_CONTRADICTION
        for field in (
            "quote_type",
            "exchange",
            "name",
        )
    )


def direct_candidate_is_validated(
    evidence: dict[str, Any],
) -> bool:
    """
    Exact configured symbol with no material identity contradiction.

    UNKNOWN evidence never rejects automatic download eligibility.
    """

    if evidence.get(
        "exact_symbol"
    ) != EVIDENCE_MATCH:
        return False

    return not evidence_has_material_contradiction(
        evidence
    )


def alternative_candidate_is_strong(
    evidence: dict[str, Any],
) -> bool:
    """
    Strong review-only alternative: clear MATCH on exchange and name.

    Uniqueness of evidence, not a numeric score margin, decides
    SEARCH_RESOLVED versus IDENTITY_AMBIGUOUS.
    """

    if evidence.get(
        "exact_symbol"
    ) == EVIDENCE_MATCH:
        return False

    if evidence.get(
        "quote_type"
    ) == EVIDENCE_CONTRADICTION:
        return False

    return (
        evidence.get("exchange")
        == EVIDENCE_MATCH
        and evidence.get("name")
        == EVIDENCE_MATCH
    )


def exact_contradiction_reason(
    evidence: dict[str, Any],
) -> str | None:
    """
    Map exact-path material contradictions to identity_reason.

    Priority when several contradictions exist:
    quote_type > exchange > name.
    """

    for field_name, reason in EXACT_CONTRADICTION_REASON_PRIORITY:
        if evidence.get(
            field_name
        ) == EVIDENCE_CONTRADICTION:
            return reason

    return None


def exact_identity_signature(
    candidate: dict[str, Any],
) -> str:
    return (
        (
            optional_text(
                candidate.get(
                    "symbol"
                )
            ) or ""
        ).casefold()
        + "|"
        + (
            optional_text(
                candidate_name(
                    candidate
                )
            ) or ""
        ).casefold()
        + "|"
        + (
            optional_text(
                candidate.get(
                    "exchange"
                )
            ) or ""
        ).casefold()
    )


def build_identity_decision(
    *,
    status: str,
    resolved_symbol: str | None,
    selected_candidate: dict[str, Any] | None,
    selected_evidence: dict[str, Any] | None,
    search_candidates: list[dict[str, Any]],
    identity_method: str,
    identity_reason: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "resolved_symbol": resolved_symbol,
        "selected_candidate": selected_candidate,
        "selected_evidence": selected_evidence,
        "search_candidates": search_candidates,
        "search_candidate_count": len(
            search_candidates
        ),
        "identity_method": identity_method,
        "identity_reason": identity_reason,
    }


def validate_or_resolve_identity(
    *,
    yfinance_module,
    instrument: dict[str, Any],
    max_results: int,
) -> dict[str, Any]:
    configured_symbol = instrument[
        "configured_symbol"
    ]

    (
        exact_list,
        symbol_query_others,
        exact_identity_method,
    ) = lookup_exact_candidates(
        yfinance_module=yfinance_module,
        configured_symbol=configured_symbol,
        max_results=max_results,
    )

    exact_evaluations: list[
        tuple[
            dict[str, Any],
            dict[str, Any],
        ]
    ] = []

    for candidate in exact_list:
        evidence = candidate_evidence(
            candidate,
            configured_symbol=configured_symbol,
            input_name=instrument[
                "input_name"
            ],
            input_exchange=instrument[
                "input_exchange"
            ],
        )

        if evidence[
            "exact_symbol"
        ] != EVIDENCE_MATCH:
            continue

        exact_evaluations.append(
            (
                candidate,
                evidence,
            )
        )

    exact_method = (
        exact_identity_method
        or IDENTITY_METHOD_NONE
    )

    # ------------------------------------------------------------------
    # DIRECT PATH
    # ------------------------------------------------------------------
    #
    # The exact configured Yahoo symbol has absolute priority over any
    # search alternative.

    if exact_evaluations:
        valid_exact = [
            item
            for item in exact_evaluations
            if direct_candidate_is_validated(
                item[1]
            )
        ]

        if len(valid_exact) == 1:
            candidate, evidence = valid_exact[
                0
            ]

            return build_identity_decision(
                status=DIRECT_VALIDATED_STATUS,
                resolved_symbol=candidate[
                    "symbol"
                ],
                selected_candidate=candidate,
                selected_evidence=evidence,
                search_candidates=exact_list,
                identity_method=exact_method,
                identity_reason=(
                    IDENTITY_REASON_EXACT_NO_CONTRADICTION
                ),
            )

        if len(valid_exact) > 1:
            signatures = {
                exact_identity_signature(
                    candidate
                )
                for candidate, _ in valid_exact
            }

            if len(signatures) == 1:
                candidate, evidence = valid_exact[
                    0
                ]

                return build_identity_decision(
                    status=DIRECT_VALIDATED_STATUS,
                    resolved_symbol=candidate[
                        "symbol"
                    ],
                    selected_candidate=candidate,
                    selected_evidence=evidence,
                    search_candidates=exact_list,
                    identity_method=exact_method,
                    identity_reason=(
                        IDENTITY_REASON_EXACT_NO_CONTRADICTION
                    ),
                )

        # Exact symbol existed but failed validation, or multiple
        # incompatible exact records were returned. Alternatives may
        # still be discovered below for review only.

    # ------------------------------------------------------------------
    # ALTERNATIVE DISCOVERY PATH
    # ------------------------------------------------------------------

    excluded_symbols = {
        candidate["symbol"].casefold()
        for candidate in exact_list
    }
    excluded_symbols.add(
        configured_symbol.casefold()
    )

    alternative_candidates: list[
        dict[str, Any]
    ] = []
    seen_alternative_symbols = set(
        excluded_symbols
    )

    for candidate in symbol_query_others:
        key = candidate[
            "symbol"
        ].casefold()

        if key in seen_alternative_symbols:
            continue

        seen_alternative_symbols.add(
            key
        )
        alternative_candidates.append(
            candidate
        )

    alternative_candidates.extend(
        collect_alternative_candidates(
            yfinance_module=yfinance_module,
            instrument=instrument,
            max_results=max_results,
            excluded_symbols=seen_alternative_symbols,
        )
    )

    candidates = (
        list(exact_list)
        + alternative_candidates
    )

    # IDENTITY_NOT_FOUND means Yahoo provided absolutely no observable
    # identity after a successful provider response: no exact candidate
    # and no alternative candidates. Technical Yahoo failures raise
    # connector exceptions earlier and never reach this branch.
    # An empty exact_evaluations list while candidates exist is never
    # NOT_FOUND — that is AMBIGUOUS (or SEARCH_RESOLVED).
    if not candidates:
        return build_identity_decision(
            status=IDENTITY_NOT_FOUND_STATUS,
            resolved_symbol=None,
            selected_candidate=None,
            selected_evidence=None,
            search_candidates=[],
            identity_method=IDENTITY_METHOD_NONE,
            identity_reason=(
                IDENTITY_REASON_NO_PROVIDER_IDENTITY
            ),
        )

    strong_alternatives: list[
        tuple[
            dict[str, Any],
            dict[str, Any],
        ]
    ] = []

    for candidate in alternative_candidates:
        evidence = candidate_evidence(
            candidate,
            configured_symbol=configured_symbol,
            input_name=instrument[
                "input_name"
            ],
            input_exchange=instrument[
                "input_exchange"
            ],
        )

        if evidence[
            "exact_symbol"
        ] == EVIDENCE_MATCH:
            continue

        if alternative_candidate_is_strong(
            evidence
        ):
            strong_alternatives.append(
                (
                    candidate,
                    evidence,
                )
            )

    if len(strong_alternatives) == 1:
        candidate, evidence = strong_alternatives[
            0
        ]

        return build_identity_decision(
            status=SEARCH_RESOLVED_STATUS,
            resolved_symbol=candidate[
                "symbol"
            ],
            selected_candidate=candidate,
            selected_evidence=evidence,
            search_candidates=candidates,
            identity_method=(
                IDENTITY_METHOD_ALTERNATIVE_SEARCH
            ),
            identity_reason=(
                IDENTITY_REASON_UNIQUE_STRONG_ALTERNATIVE
            ),
        )

    # ------------------------------------------------------------------
    # AMBIGUOUS
    # ------------------------------------------------------------------
    #
    # Conflicting alternatives, or exact-path failure without a unique
    # strong review candidate. Provider candidates existed, so this is
    # never IDENTITY_NOT_FOUND.

    exact_candidate = None
    exact_evidence = None

    if exact_evaluations:
        (
            exact_candidate,
            exact_evidence,
        ) = exact_evaluations[0]

    if len(strong_alternatives) > 1:
        ambiguous_method = IDENTITY_METHOD_ALTERNATIVE_SEARCH
        ambiguous_reason = IDENTITY_REASON_MULTIPLE_STRONG_ALTERNATIVES

    elif exact_evaluations:
        ambiguous_method = exact_method
        ambiguous_reason = None
        for _, evidence in exact_evaluations:
            ambiguous_reason = exact_contradiction_reason(evidence)
            if ambiguous_reason is not None:
                break
        if ambiguous_reason is None:
            ambiguous_reason = (
                IDENTITY_REASON_INCONSISTENT_EXACT_CANDIDATES
            )

    else:
        ambiguous_method = IDENTITY_METHOD_ALTERNATIVE_SEARCH
        ambiguous_reason = IDENTITY_REASON_NO_STRONG_ALTERNATIVE

    return build_identity_decision(
        status=IDENTITY_AMBIGUOUS_STATUS,
        resolved_symbol=None,
        selected_candidate=exact_candidate,
        selected_evidence=exact_evidence,
        search_candidates=candidates,
        identity_method=ambiguous_method,
        identity_reason=ambiguous_reason,
    )


# ============================================================================
# YAHOO DOWNLOAD
# ============================================================================

def download_market_data(
    *,
    yfinance_module,
    symbols: list[str],
    download_parameters: dict[str, Any],
    runtime: dict[str, Any],
) -> pd.DataFrame:
    if not symbols:
        raise ConnectorConfigurationError(
            "yfinance requires at least one symbol."
        )

    arguments = dict(
        download_parameters
    )

    arguments.update({
        "tickers": symbols,
        "threads": runtime[
            "threads"
        ],
        "timeout": runtime[
            "timeout"
        ],
        "progress": DOWNLOAD_PROGRESS,
        "group_by": DOWNLOAD_GROUP_BY,
        "multi_level_index": (
            DOWNLOAD_MULTI_LEVEL_INDEX
        ),
    })

    try:
        dataframe = yfinance_module.download(
            **arguments
        )

    except Exception as error:
        # Same classifier as identity paths. Quote-not-found on an
        # already DIRECT_VALIDATED symbol is ConnectorResponseError
        # (inconsistent Yahoo state), not silent success.
        raise_yahoo_provider_failure(
            "download",
            error,
        )

    if (
        dataframe is None
        or not isinstance(
            dataframe,
            pd.DataFrame,
        )
    ):
        raise ConnectorResponseError(
            "yfinance download must return a pandas DataFrame."
        )

    return dataframe.copy(
        deep=True
    )


# ============================================================================
# DOWNLOAD NORMALIZATION
# ============================================================================

def normalize_level_value(
    value: Any,
) -> str:
    normalized = optional_text(
        value
    )

    return (
        normalized.casefold()
        if normalized
        else ""
    )


def build_symbol_lookup(
    symbols: list[str],
) -> dict[str, str]:
    lookup: dict[str, str] = {}

    for symbol in symbols:
        normalized = optional_text(
            symbol
        )

        if normalized is None:
            raise ConnectorConfigurationError(
                "Symbol list contains an empty value."
            )

        key = normalized.casefold()

        if key in lookup:
            raise ConnectorConfigurationError(
                f"Duplicate normalized symbol: {normalized}"
            )

        lookup[
            key
        ] = normalized

    return lookup


def identify_multiindex_levels(
    columns: pd.MultiIndex,
    symbols: list[str],
) -> tuple[int, int]:
    if (
        not isinstance(
            columns,
            pd.MultiIndex,
        )
        or columns.nlevels != 2
    ):
        raise ConnectorResponseError(
            "yfinance output must have a two-level MultiIndex."
        )

    configured_keys = set(
        build_symbol_lookup(
            symbols
        )
    )

    matches: list[int] = []

    for level in range(
        2
    ):
        values = {
            normalize_level_value(
                value
            )
            for value
            in columns.get_level_values(
                level
            )
        }

        values.discard(
            ""
        )

        if (
            values
            and values.issubset(
                configured_keys
            )
        ):
            matches.append(
                level
            )

    if len(matches) != 1:
        raise ConnectorResponseError(
            "Unable to uniquely identify yfinance symbol level."
        )

    symbol_level = matches[
        0
    ]

    field_level = (
        1
        if symbol_level == 0
        else 0
    )

    return (
        symbol_level,
        field_level,
    )


def normalize_single_symbol_frame(
    dataframe: pd.DataFrame,
    symbol: str,
) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    if isinstance(
        dataframe.columns,
        pd.MultiIndex,
    ):
        raise ConnectorResponseError(
            "Unexpected MultiIndex in single-symbol normalizer."
        )

    normalized: dict[Any, str] = {}

    for column in dataframe.columns:
        field = normalize_provider_field_name(
            column
        )

        if field in normalized.values():
            raise ConnectorResponseError(
                f"Duplicate normalized field: {field}"
            )

        normalized[
            column
        ] = field

    frame = dataframe.rename(
        columns=normalized
    ).copy()

    frame.index.name = (
        "timestamp"
    )

    frame = frame.reset_index()

    if "timestamp" not in frame.columns:
        frame = frame.rename(
            columns={
                frame.columns[0]: "timestamp"
            }
        )

    frame.insert(
        0,
        "symbol",
        symbol,
    )

    return frame.reset_index(
        drop=True
    )


def normalize_multiindex_frame(
    dataframe: pd.DataFrame,
    symbols: list[str],
) -> pd.DataFrame:
    (
        symbol_level,
        field_level,
    ) = identify_multiindex_levels(
        dataframe.columns,
        symbols,
    )

    symbol_lookup = build_symbol_lookup(
        symbols
    )

    returned: list[str] = []

    for value in dataframe.columns.get_level_values(
        symbol_level
    ):
        key = normalize_level_value(
            value
        )

        if (
            key
            and key not in returned
        ):
            returned.append(
                key
            )

    frames: list[pd.DataFrame] = []

    for symbol_key in returned:
        if symbol_key not in symbol_lookup:
            raise ConnectorResponseError(
                f"Unexpected Yahoo symbol: {symbol_key}"
            )

        selected_columns = [
            column
            for column in dataframe.columns
            if normalize_level_value(
                column[
                    symbol_level
                ]
            )
            == symbol_key
        ]

        frame = dataframe.loc[
            :,
            selected_columns,
        ].copy()

        fields = [
            normalize_provider_field_name(
                column[
                    field_level
                ]
            )
            for column
            in selected_columns
        ]

        if len(fields) != len(
            set(fields)
        ):
            raise ConnectorResponseError(
                "Duplicate normalized fields for "
                f"{symbol_lookup[symbol_key]}"
            )

        frame.columns = (
            fields
        )

        frame.index.name = (
            "timestamp"
        )

        frame = frame.reset_index()

        if "timestamp" not in frame.columns:
            frame = frame.rename(
                columns={
                    frame.columns[0]: "timestamp"
                }
            )

        frame.insert(
            0,
            "symbol",
            symbol_lookup[
                symbol_key
            ],
        )

        frames.append(
            frame
        )

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )


def normalize_download_frame(
    dataframe: pd.DataFrame,
    symbols: list[str],
) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame()

    if isinstance(
        dataframe.columns,
        pd.MultiIndex,
    ):
        return normalize_multiindex_frame(
            dataframe,
            symbols,
        )

    if len(symbols) != 1:
        raise ConnectorResponseError(
            "Single-level columns returned for a multi-symbol request."
        )

    return normalize_single_symbol_frame(
        dataframe,
        symbols[0],
    )


# ============================================================================
# OBSERVATION VALIDATION
# ============================================================================

def market_value_columns(
    dataframe: pd.DataFrame,
) -> list[str]:
    return [
        column
        for column in (
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
        )
        if column in dataframe.columns
    ]


def filter_usable_observations(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    columns = market_value_columns(
        dataframe
    )

    if not columns:
        return dataframe.iloc[
            0:0
        ].copy()

    usable = dataframe[
        columns
    ].notna().any(
        axis=1
    )

    return dataframe.loc[
        usable
    ].reset_index(
        drop=True
    )


# ============================================================================
# LINEAGE
# ============================================================================

def attach_lineage(
    dataframe: pd.DataFrame,
    *,
    instruments: list[dict[str, Any]],
    resolution_status: dict[str, str],
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    actual_lookup: dict[
        str,
        dict[str, Any],
    ] = {}

    for instrument in instruments:
        resolved = optional_text(
            instrument.get(
                "resolved_symbol"
            )
        )

        if resolved is None:
            continue

        key = resolved.casefold()

        if key in actual_lookup:
            raise ConnectorResponseError(
                "Multiple instruments resolve to Yahoo symbol "
                f"{resolved}."
            )

        actual_lookup[
            key
        ] = instrument

    records: list[
        dict[str, Any]
    ] = []

    for _, row in dataframe.iterrows():
        actual = optional_text(
            row.get(
                "symbol"
            )
        )

        actual_key = (
            actual.casefold()
            if actual
            else ""
        )

        instrument = actual_lookup.get(
            actual_key
        )

        if instrument is None:
            raise ConnectorResponseError(
                "Unable to resolve Yahoo lineage for "
                f"{actual}."
            )

        record = row.to_dict()

        configured = instrument[
            "configured_symbol"
        ]

        record.update({
            "input_position": instrument[
                "input_position"
            ],
            "input_index": instrument[
                "input_index"
            ],
            "configured_symbol": configured,
            "resolved_symbol": actual,
            "symbol_resolution_status": (
                resolution_status[
                    configured.casefold()
                ]
            ),
        })

        records.append(
            record
        )

    output = pd.DataFrame(
        records
    )

    technical = [
        "input_position",
        "input_index",
        "configured_symbol",
        "resolved_symbol",
        "symbol_resolution_status",
    ]

    return output[
        technical
        + [
            column
            for column in output.columns
            if column not in technical
        ]
    ].reset_index(
        drop=True
    )


def validate_final_output(
    dataframe: pd.DataFrame,
) -> None:
    if dataframe.empty:
        return

    required = {
        "input_position",
        "input_index",
        "configured_symbol",
        "resolved_symbol",
        "symbol_resolution_status",
        "timestamp",
    }

    missing = sorted(
        required
        - set(dataframe.columns)
    )

    if missing:
        raise ConnectorResponseError(
            "Normalized Yahoo output missing columns: "
            f"{missing}"
        )

    if dataframe[
        "timestamp"
    ].isna().any():
        raise ConnectorResponseError(
            "Normalized Yahoo output contains missing timestamps."
        )

    duplicates = dataframe.duplicated(
        subset=[
            "configured_symbol",
            "timestamp",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ConnectorResponseError(
            "Normalized Yahoo output contains duplicate "
            "configured_symbol/timestamp keys."
        )


# ============================================================================
# CONNECTOR
# ============================================================================

class YFinanceConnector(
    BaseConnector
):
    """
    Yahoo Finance market-data connector with defensive instrument
    identity validation.
    """

    SUPPORTED_RUNTIME_OPTIONS = frozenset({
        "timeout",
        "threads",
        "search_results",
    })

    @classmethod
    def validate_parameters(
        cls,
        parameters: dict[str, Any],
    ) -> None:
        if not isinstance(
            parameters,
            dict,
        ):
            raise ConnectorConfigurationError(
                "yfinance Parameters must be a dictionary."
            )

        missing = sorted(
            parameter
            for parameter in REQUIRED_PARAMETERS
            if optional_text(
                parameters.get(
                    parameter
                )
            )
            is None
        )

        if missing:
            raise ConnectorConfigurationError(
                "yfinance Parameters are missing: "
                f"{missing}"
            )

        build_download_parameters(
            parameters
        )

    @classmethod
    def validate_context(
        cls,
        context: ConnectorContext,
    ) -> None:
        super().validate_context(
            context
        )

        if optional_text(
            context.endpoint
        ) is not None:
            raise ConnectorConfigurationError(
                "yfinance does not use Access Endpoint."
            )

        if context.input_data is None:
            raise ConnectorConfigurationError(
                "yfinance requires an upstream input Dataset."
            )

        symbol_column = optional_text(
            context.parameters.get(
                SYMBOL_COLUMN_PARAMETER
            )
        )

        name_column = optional_text(
            context.parameters.get(
                NAME_COLUMN_PARAMETER
            )
        )

        exchange_column = optional_text(
            context.parameters.get(
                EXCHANGE_COLUMN_PARAMETER
            )
        )

        if (
            not symbol_column
            or not name_column
            or not exchange_column
        ):
            raise ConnectorConfigurationError(
                "yfinance input column configuration is incomplete."
            )

        validate_input_data(
            context.input_data,
            symbol_column=symbol_column,
            name_column=name_column,
            exchange_column=exchange_column,
        )

        resolve_runtime_options(
            context.runtime_options
        )

    @classmethod
    def acquire(
        cls,
        context: ConnectorContext,
    ) -> ConnectorResult:
        cls.validate_context(
            context
        )

        parameters = (
            context.parameters.copy()
        )

        if context.input_data is None:
            raise ConnectorConfigurationError(
                "yfinance requires an upstream input Dataset."
            )

        input_data = (
            context.input_data.copy(
                deep=True
            )
        )

        symbol_column = optional_text(
            parameters.get(
                SYMBOL_COLUMN_PARAMETER
            )
        )

        name_column = optional_text(
            parameters.get(
                NAME_COLUMN_PARAMETER
            )
        )

        exchange_column = optional_text(
            parameters.get(
                EXCHANGE_COLUMN_PARAMETER
            )
        )

        if (
            symbol_column is None
            or name_column is None
            or exchange_column is None
        ):
            raise ConnectorConfigurationError(
                "yfinance input column configuration is incomplete."
            )

        download_parameters = build_download_parameters(
            parameters
        )

        runtime = resolve_runtime_options(
            context.runtime_options
        )

        yf = load_yfinance()

        instruments = build_instruments(
            input_data,
            symbol_column=symbol_column,
            name_column=name_column,
            exchange_column=exchange_column,
        )

        identity_metadata: dict[
            str,
            Any,
        ] = {}

        resolution_status: dict[
            str,
            str,
        ] = {}

        validated_symbols: list[
            str
        ] = []

        # ------------------------------------------------------------------
        # IDENTITY VALIDATION
        # ------------------------------------------------------------------

        for instrument in instruments:
            configured = instrument[
                "configured_symbol"
            ]

            decision = validate_or_resolve_identity(
                yfinance_module=yf,
                instrument=instrument,
                max_results=runtime[
                    "search_results"
                ],
            )

            status = decision[
                "status"
            ]

            resolved = decision[
                "resolved_symbol"
            ]

            resolution_status[
                configured.casefold()
            ] = status

            instrument[
                "resolved_symbol"
            ] = None

            download_eligible = (
                status
                in DOWNLOAD_ELIGIBLE_STATUSES
            )

            review_required = (
                status
                in REVIEW_REQUIRED_STATUSES
            )

            identity_metadata[
                configured
            ] = {
                "resolution_status": status,
                "resolved_symbol": resolved,
                "identity_method": decision[
                    "identity_method"
                ],
                "identity_reason": decision[
                    "identity_reason"
                ],
                "selected_candidate": decision[
                    "selected_candidate"
                ],
                "selected_evidence": decision[
                    "selected_evidence"
                ],
                "search_candidate_count": decision[
                    "search_candidate_count"
                ],
                "search_candidates": decision[
                    "search_candidates"
                ],
                "download_eligible": download_eligible,
                "review_required": review_required,
            }

            if (
                status
                == DIRECT_VALIDATED_STATUS
            ):
                if (
                    resolved is None
                    or resolved.casefold()
                    != configured.casefold()
                ):
                    raise ConnectorResponseError(
                        "DIRECT_VALIDATED identity must preserve "
                        "the configured Yahoo symbol."
                    )

                instrument[
                    "resolved_symbol"
                ] = configured

                validated_symbols.append(
                    configured
                )

            else:
                logger.warning(
                    "Yahoo identity not directly validated: %s; "
                    "Status=%s; Candidate=%s",
                    configured,
                    status,
                    resolved,
                )

        # ------------------------------------------------------------------
        # DUPLICATE SAFETY
        # ------------------------------------------------------------------

        resolved_lookup: dict[
            str,
            str,
        ] = {}

        for instrument in instruments:
            resolved = optional_text(
                instrument.get(
                    "resolved_symbol"
                )
            )

            if resolved is None:
                continue

            key = resolved.casefold()

            if key in resolved_lookup:
                raise ConnectorResponseError(
                    "Multiple configured instruments resolve to the "
                    "same Yahoo symbol before download."
                )

            resolved_lookup[
                key
            ] = instrument[
                "configured_symbol"
            ]

        # ------------------------------------------------------------------
        # MARKET-DATA ACQUISITION
        # ------------------------------------------------------------------

        if not validated_symbols:
            dataframe = pd.DataFrame()

        else:
            raw = download_market_data(
                yfinance_module=yf,
                symbols=validated_symbols,
                download_parameters=download_parameters,
                runtime=runtime,
            )

            dataframe = normalize_download_frame(
                raw,
                validated_symbols,
            )

            dataframe = filter_usable_observations(
                dataframe
            )

            dataframe = attach_lineage(
                dataframe,
                instruments=instruments,
                resolution_status=resolution_status,
            )

            validate_final_output(
                dataframe
            )

        # ------------------------------------------------------------------
        # DATA AVAILABILITY
        # ------------------------------------------------------------------

        returned_keys: set[str] = set()

        if not dataframe.empty:
            returned_keys = set(
                dataframe[
                    "configured_symbol"
                ]
                .astype(str)
                .str.casefold()
            )

        symbols_without_data: list[
            str
        ] = []

        for instrument in instruments:
            configured = instrument[
                "configured_symbol"
            ]

            status = resolution_status[
                configured.casefold()
            ]

            if (
                status
                == DIRECT_VALIDATED_STATUS
                and configured.casefold()
                not in returned_keys
            ):
                symbols_without_data.append(
                    configured
                )

        # ------------------------------------------------------------------
        # METADATA
        # ------------------------------------------------------------------

        statuses = (
            DIRECT_VALIDATED_STATUS,
            SEARCH_RESOLVED_STATUS,
            IDENTITY_AMBIGUOUS_STATUS,
            IDENTITY_NOT_FOUND_STATUS,
        )

        resolution_counts = {
            status: sum(
                1
                for value
                in resolution_status.values()
                if value == status
            )
            for status in statuses
        }

        validated_count = resolution_counts[
            DIRECT_VALIDATED_STATUS
        ]

        review_required_count = resolution_counts[
            SEARCH_RESOLVED_STATUS
        ]

        unresolved_identity_count = (
            resolution_counts[
                IDENTITY_AMBIGUOUS_STATUS
            ]
            + resolution_counts[
                IDENTITY_NOT_FOUND_STATUS
            ]
        )

        identity_validation_rate = (
            validated_count
            / len(instruments)
            * 100.0
            if instruments
            else 0.0
        )

        metadata = {
            "requested_symbol_count": len(
                instruments
            ),
            "identity_validated_count": validated_count,
            "identity_validation_rate": (
                identity_validation_rate
            ),
            "review_required_count": (
                review_required_count
            ),
            "unresolved_identity_count": (
                unresolved_identity_count
            ),
            "returned_symbol_count": len(
                returned_keys
            ),
            "symbols_without_data": (
                symbols_without_data
            ),
            "resolution_counts": (
                resolution_counts
            ),
            "identity_resolution": (
                identity_metadata
            ),
            "observation_records": len(
                dataframe
            ),
            "interval": download_parameters.get(
                "interval"
            ),
            "period": download_parameters.get(
                "period"
            ),
            "start": download_parameters.get(
                "start"
            ),
            "end": download_parameters.get(
                "end"
            ),
        }

        logger.info(
            "yfinance acquisition completed: "
            "Requested=%s; "
            "IdentityValidated=%s; "
            "Returned=%s; "
            "DirectValidated=%s; "
            "SearchResolved=%s; "
            "IdentityAmbiguous=%s; "
            "IdentityNotFound=%s; "
            "Observations=%s",
            metadata[
                "requested_symbol_count"
            ],
            metadata[
                "identity_validated_count"
            ],
            metadata[
                "returned_symbol_count"
            ],
            resolution_counts[
                DIRECT_VALIDATED_STATUS
            ],
            resolution_counts[
                SEARCH_RESOLVED_STATUS
            ],
            resolution_counts[
                IDENTITY_AMBIGUOUS_STATUS
            ],
            resolution_counts[
                IDENTITY_NOT_FOUND_STATUS
            ],
            metadata[
                "observation_records"
            ],
        )

        return ConnectorResult(
            data=dataframe,
            metadata=metadata,
        )
