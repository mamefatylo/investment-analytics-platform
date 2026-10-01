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

For each instrument:

1. Search Yahoo using the configured symbol and, when available, the
   upstream instrument name.
2. Look specifically for the exact configured Yahoo symbol.
3. Validate that exact symbol using independent identity evidence:
   - exact Yahoo symbol;
   - quote type;
   - exchange;
   - instrument/company name.
4. Only an exact symbol with sufficiently coherent identity is eligible
   for automatic market-data download.
5. If the exact configured symbol cannot be validated, Yahoo Search may
   identify a strong alternative candidate.
6. Alternative candidates are diagnostic only:
   - SEARCH_RESOLVED;
   - review_required=True;
   - download_eligible=False;
   - no automatic modification of the upstream securities master.

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
    ConnectorConfigurationError,
    ConnectorContext,
    ConnectorDependencyError,
    ConnectorResponseError,
    ConnectorResult,
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
# IDENTITY RULES
# ============================================================================

ACCEPTABLE_QUOTE_TYPES = frozenset({
    "equity",
    "etf",
})

# A configured exact Yahoo symbol already provides strong identifier
# evidence. Name comparison is therefore used primarily to detect
# contradictions rather than to demand almost identical wording.
DIRECT_MIN_NAME_SCORE = 0.60

# A different Yahoo symbol has no exact identifier evidence and therefore
# requires substantially stronger identity evidence.
SEARCH_MIN_NAME_SCORE = 0.85

# If two alternative candidates are almost equally good, the connector
# must not choose between them automatically.
SEARCH_MIN_MARGIN = 0.05


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
# The upstream Exchange field may contain human-readable exchange names,
# while Yahoo Search commonly returns provider codes such as HKG, JPX,
# NMS or STO.
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


def name_similarity(
    expected_name: Any,
    candidate_name_value: Any,
) -> float:
    left_tokens = normalized_name_tokens(
        expected_name
    )

    right_tokens = normalized_name_tokens(
        candidate_name_value
    )

    if (
        not left_tokens
        or not right_tokens
    ):
        return 0.0

    intersection = len(
        left_tokens & right_tokens
    )

    return (
        2.0
        * intersection
        / (
            len(left_tokens)
            + len(right_tokens)
        )
    )


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
) -> list[dict[str, Any]]:
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
            enable_fuzzy_query=True,
            recommended=0,
            raise_errors=True,
        )

        quotes = search.quotes

    except Exception as error:
        logger.warning(
            "Yahoo Search failed for %s: %s",
            normalized_query,
            error,
        )
        return []

    if not isinstance(
        quotes,
        list,
    ):
        return []

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


def collect_identity_candidates(
    *,
    yfinance_module,
    instrument: dict[str, Any],
    max_results: int,
) -> list[dict[str, Any]]:
    queries = [
        instrument["configured_symbol"]
    ]

    if instrument["input_name"]:
        queries.append(
            instrument["input_name"]
        )

    candidates: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    used_queries: set[str] = set()

    for query in queries:
        query_text = optional_text(
            query
        )

        if query_text is None:
            continue

        query_key = query_text.casefold()

        if query_key in used_queries:
            continue

        used_queries.add(
            query_key
        )

        for candidate in search  yfinance_module=yfinance_module,
            query=query_text,
            max_results=max_results,
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


def quote_type_is_acceptable(
    candidate: dict[str, Any],
) -> bool:
    quote_type = normalize_ascii_text(
        candidate.get(
            "quote_type"
        )
    )

    if not quote_type:
        return True

    return (
        quote_type
        in ACCEPTABLE_QUOTE_TYPES
    )


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
) -> setgroups: set[str] = set()

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


def exchange_matches_input(
    candidate: dict[str, Any],
    input_exchange: str | None,
) -> bool | None:
    expected_group = exchange_group(
        input_exchange
    )

    candidate_groups = candidate_exchange_groups(
        candidate
    )

    # Unknown upstream exchange is not treated as either a match
    # or a contradiction.
    if expected_group is None:
        return None

    # Unknown Yahoo exchange is also not treated as a contradiction.
    if not candidate_groups:
        return None

    return (
        expected_group
        in candidate_groups
    )


def candidate_evidence(
    candidate: dict[str, Any],
    *,
    configured_symbol: str,
    input_name: str | None,
    input_exchange: str | None,
) -> dict[str, Any]:
    result_symbol = (
        optional_text(
            candidate.get("symbol")
        )
        or ""
    )

    expected_name_present = (
        optional_text(
            input_name
        )
        is not None
    )

    actual_candidate_name = candidate_name(
        candidate
    )

    candidate_name_present = (
        actual_candidate_name
        is not None
    )

    name_evidence_available = (
        expected_name_present
        and candidate_name_present
    )

    if name_evidence_available:
        name_score: float | None = name_similarity(
            input_name,
            actual_candidate_name,
        )
    else:
        name_score = None

    return {
        "exact_symbol_match": (
            result_symbol.casefold()
            == configured_symbol.casefold()
        ),
        "exchange_match": exchange_matches_input(
            candidate,
            input_exchange,
        ),
        "name_score": name_score,
        "name_evidence_available": (
            name_evidence_available
        ),
        "quote_type_ok": quote_type_is_acceptable(
            candidate
        ),
    }


def direct_candidate_is_valid(
    evidence: dict[str, Any],
) -> bool:
    # Exact provider symbol is mandatory for automatic download.
    if not evidence[
        "exact_symbol_match"
    ]:
        return False

    # Explicitly incompatible Yahoo quote types are rejected.
    if not evidence[
        "quote_type_ok"
    ]:
        return False

    # Known exchange contradiction is fatal.
    if evidence[
        "exchange_match"
    ] is False:
        return False

    # When both sides provide a name, a strong name contradiction
    # is fatal even if the ticker itself exists.
    #
    # This is the protection against cases such as:
    #
    # configured symbol: SAP.TO
    # Yahoo instrument: Saputo Inc.
    #
    # while the upstream instrument is SAP SE.
    if evidence[
        "name_evidence_available"
    ]:
        name_score = evidence[
            "name_score"
        ]

        if (
            name_score is None
            or name_score
            < DIRECT_MIN_NAME_SCORE
        ):
            return False

    # Exact symbol alone is not enough.
    #
    # At least one independent identity signal must be available:
    # either a positively mapped exchange or usable name evidence.
    identity_support = (
        evidence[
            "exchange_match"
        ] is True
        or evidence[
            "name_evidence_available"
        ]
    )

    return identity_support


def alternative_candidate_score(
    evidence: dict[str, Any],
) -> float | None:
    # Alternative discovery concerns only different Yahoo symbols.
    if evidence[
        "exact_symbol_match"
    ]:
        return None

    if not evidence[
        "quote_type_ok"
    ]:
        return None

    # Explicit exchange contradiction is fatal for an alternative.
    if evidence[
        "exchange_match"
    ] is False:
        return None

    # Alternative resolution requires positive exchange evidence.
    #
    # Unlike the direct path, unknown exchange mapping is not sufficient
    # because the provider symbol itself differs from the configured one.
    if evidence[
        "exchange_match"
    ] is not True:
        return None

    # Alternative resolution also requires actual name evidence.
    if not evidence[
        "name_evidence_available"
    ]:
        return None

    name_score = evidence[
        "name_score"
    ]

    if (
        name_score is None
        or name_score
        < SEARCH_MIN_NAME_SCORE
    ):
        return None

    return float(
        name_score
    )


def validate_or_resolve_identity(
    *,
    yfinance_module,
    instrument: dict[str, Any],
    max_results: int,
) -> dict[str, Any]:
    configured_symbol = instrument[
        "configured_symbol"
    ]

    candidates = collect_identity_candidates(
        yfinance_module=yfinance_module,
        instrument=instrument,
        max_results=max_results,
    )

    if not candidates:
        return {
            "status": IDENTITY_NOT_FOUND_STATUS,
            "resolved_symbol": None,
            "selected_candidate": None,
            "selected_evidence": None,
            "search_candidates": [],
            "search_candidate_count": 0,
        }

    evaluated: list[
        tuple[
            dict[str, Any],
            dict[str, Any],
        ]
    ] = []

    exact_evaluations: list[
        tuple[
            dict[str, Any],
            dict[str, Any],
        ]
    ] = []

    for candidate in candidates:
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

        item = (
            candidate,
            evidence,
        )

        evaluated.append(
            item
        )

        if evidence[
            "exact_symbol_match"
        ]:
            exact_evaluations.append(
                item
            )

    # ------------------------------------------------------------------
    # DIRECT PATH
    # ------------------------------------------------------------------
    #
    # The exact configured Yahoo symbol has absolute priority.
    #
    # A different listing can never beat a valid exact symbol merely
    # because its textual name similarity happens to be slightly higher.

    if exact_evaluations:
        valid_exact = [
            item
            for item in exact_evaluations
            if direct_candidate_is_valid(
                item[1]
            )
        ]

        if len(valid_exact) == 1:
            candidate, evidence = valid_exact[
                0
            ]

            return {
                "status": DIRECT_VALIDATED_STATUS,
                "resolved_symbol": candidate[
                    "symbol"
                ],
                "selected_candidate": candidate,
                "selected_evidence": evidence,
                "search_candidates": candidates,
                "search_candidate_count": len(
                    candidates
                ),
            }

        # If Yahoo somehow returns more than one exact-symbol record,
        # automatic validation is allowed only when the valid records
        # are themselves identity-equivalent.
        if len(valid_exact) > 1:
            signatures = {
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
                for candidate, _ in valid_exact
            }

            if len(signatures) == 1:
                candidate, evidence = valid_exact[
                    0
                ]

                return {
                    "status": DIRECT_VALIDATED_STATUS,
                    "resolved_symbol": candidate[
                        "symbol"
                    ],
                    "selected_candidate": candidate,
                    "selected_evidence": evidence,
                    "search_candidates": candidates,
                    "search_candidate_count": len(
                        candidates
                    ),
                }

        # Important:
        #
        # An exact Yahoo symbol may exist but be inconsistent with the
        # upstream identity. This does NOT allow another listing to become
        # automatically downloadable.
        #
        # Alternatives may still be discovered below, but only as review
        # candidates.

    # ------------------------------------------------------------------
    # ALTERNATIVE DISCOVERY PATH
    # ------------------------------------------------------------------

    alternatives: list[
        tuple[
            float,
            dict[str, Any],
            dict[str, Any],
        ]
    ] = []

    for candidate, evidence in evaluated:
        score = alternative_candidate_score(
            evidence
        )

        if score is None:
            continue

        alternatives.append(
            (
                score,
                candidate,
                evidence,
            )
        )

    if alternatives:
        alternatives.sort(
            key=lambda item: (
                item[0],
                item[1]["symbol"].casefold(),
            ),
            reverse=True,
        )

        (
            best_score,
            best_candidate,
            best_evidence,
        ) = alternatives[0]

        second_score = (
            alternatives[1][0]
            if len(alternatives) > 1
            else None
        )

        sufficiently_unique = (
            second_score is None
            or (
                best_score
                - second_score
            )
            >= SEARCH_MIN_MARGIN
        )

        if sufficiently_unique:
            return {
                "status": SEARCH_RESOLVED_STATUS,
                "resolved_symbol": best_candidate[
                    "symbol"
                ],
                "selected_candidate": best_candidate,
                "selected_evidence": best_evidence,
                "search_candidates": candidates,
                "search_candidate_count": len(
                    candidates
                ),
            }

    # ------------------------------------------------------------------
    # AMBIGUOUS
    # ------------------------------------------------------------------
    #
    # If an exact candidate existed but failed validation, retain it in
    # metadata so that the reason for rejection remains auditable.
    #
    # Never mark that candidate as resolved.

    exact_candidate = None
    exact_evidence = None

    if exact_evaluations:
        (
            exact_candidate,
            exact_evidence,
        ) = exact_evaluations[0]

    return {
        "status": IDENTITY_AMBIGUOUS_STATUS,
        "resolved_symbol": None,
        "selected_candidate": exact_candidate,
        "selected_evidence": exact_evidence,
        "search_candidates": candidates,
        "search_candidate_count": len(
            candidates
        ),
    }


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
        raise ConnectorResponseError(
            "yfinance download failed."
        ) from error

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
                frame.columns"timestamp"
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
                    frame.columns"timestamp"
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
) -> listreturn [
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