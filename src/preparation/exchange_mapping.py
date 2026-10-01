"""
exchange_mapping.py

Central reference mapping between benchmark exchange labels and
provider-specific market identifiers.

Purpose
-------
Translate benchmark exchange labels into provider-specific market
identifiers used during Preparation.

Provider representations
------------------------
openfigi
    OpenFIGI exchange code used during Security Master resolution.

yahoo_suffix
    Yahoo Finance exchange suffix used to construct market-data
    symbols.

Design principles
-----------------
- the benchmark exchange label is the internal reference key;
- provider-specific identifiers remain explicitly separated;
- OpenFIGI acquisition remains RAW-first;
- this mapping is used during Preparation only;
- provider identifiers are never assumed to be interchangeable;
- unknown exchanges return None rather than being guessed.

Important
---------
This is an internal translation reference table, not a universal
exchange-identification standard.
"""


# ============================================================================
# EXCHANGE REFERENCE
# ============================================================================

EXCHANGE_MAPPING = {
    "NASDAQ": {
        "openfigi": "US",
        "yahoo_suffix": "",
    },
    "NYSE": {
        "openfigi": "US",
        "yahoo_suffix": "",
    },
    "SIX Swiss Exchange": {
        "openfigi": "SW",
        "yahoo_suffix": ".SW",
    },
    "London Stock Exchange": {
        "openfigi": "LN",
        "yahoo_suffix": ".L",
    },
    "Hong Kong Exchanges And Clearing Ltd": {
        "openfigi": "HK",
        "yahoo_suffix": ".HK",
    },
    "Tokyo Stock Exchange": {
        "openfigi": "JP",
        "yahoo_suffix": ".T",
    },
    "Taiwan Stock Exchange": {
        "openfigi": "TT",
        "yahoo_suffix": ".TW",
    },
    "Toronto Stock Exchange": {
        "openfigi": "CN",
        "yahoo_suffix": ".TO",
    },
    "Shanghai Stock Exchange": {
        "openfigi": "CH",
        "yahoo_suffix": ".SS",
    },
    "Shenzhen Stock Exchange": {
        "openfigi": "SZ",
        "yahoo_suffix": ".SZ",
    },
    "Singapore Exchange": {
        "openfigi": "SP",
        "yahoo_suffix": ".SI",
    },
    "National Stock Exchange Of India": {
        "openfigi": "IN",
        "yahoo_suffix": ".NS",
    },
    "Korea Exchange (Stock Market)": {
        "openfigi": "KS",
        "yahoo_suffix": ".KS",
    },
    "Korea Exchange (Kosdaq)": {
        "openfigi": "KQ",
        "yahoo_suffix": ".KQ",
    },
    "Xetra": {
        "openfigi": "GY",
        "yahoo_suffix": ".DE",
    },
    "Bolsa De Madrid": {
        "openfigi": "SM",
        "yahoo_suffix": ".MC",
    },
    "Bolsa Mexicana De Valores": {
        "openfigi": "MM",
        "yahoo_suffix": ".MX",
    },
    "Oslo Bors Asa": {
        "openfigi": "NO",
        "yahoo_suffix": ".OL",
    },
    "Nasdaq Omx Nordic": {
        "openfigi": "SS",
        "yahoo_suffix": ".ST",
    },
    "Omx Nordic Exchange Copenhagen A/S": {
        "openfigi": "DC",
        "yahoo_suffix": ".CO",
    },
    "Nyse Euronext - Euronext Paris": {
        "openfigi": "FP",
        "yahoo_suffix": ".PA",
    },
    "Nyse Euronext - Euronext Brussels": {
        "openfigi": "BB",
        "yahoo_suffix": ".BR",
    },
    "Nyse Euronext - Euronext Lisbon": {
        "openfigi": "PL",
        "yahoo_suffix": ".LS",
    },
    "Wiener Boerse Ag": {
        "openfigi": "AV",
        "yahoo_suffix": ".VI",
    },
    "Johannesburg Stock Exchange": {
        "openfigi": "SJ",
        "yahoo_suffix": ".JO",
    },
    "Tel Aviv Stock Exchange": {
        "openfigi": "IT",
        "yahoo_suffix": ".TA",
    },
    "Saudi Stock Exchange": {
        "openfigi": "AB",
        "yahoo_suffix": ".SR",
    },
    "Borsa Italiana": {
        "openfigi": "IM",
        "yahoo_suffix": ".MI",
    },
    "Asx - All Markets": {
        "openfigi": "AU",
        "yahoo_suffix": ".AX",
    },
}


# ============================================================================
# NORMALIZATION
# ============================================================================

def normalize_exchange(
    exchange: str,
) -> str:
    """
    Normalize a required benchmark exchange label.
    """

    if exchange is None:
        raise ValueError(
            "exchange is required."
        )

    normalized = str(
        exchange
    ).strip()

    if not normalized:
        raise ValueError(
            "exchange cannot be empty."
        )

    return normalized


def normalize_ticker(
    ticker: str,
) -> str:
    """
    Normalize a required benchmark ticker.
    """

    if ticker is None:
        raise ValueError(
            "ticker is required."
        )

    normalized = str(
        ticker
    ).strip()

    if not normalized:
        raise ValueError(
            "ticker cannot be empty."
        )

    return normalized


# ============================================================================
# INTERNAL LOOKUP
# ============================================================================

def get_exchange_configuration(
    exchange: str,
) -> dict[str, str] | None:
    """
    Return provider mappings for one benchmark exchange.

    Matching is case-insensitive.

    Returns
    -------
    dict | None
        Defensive copy of the provider mapping, or None when the
        benchmark exchange is unknown.
    """

    normalized_exchange = normalize_exchange(
        exchange
    )

    requested_key = (
        normalized_exchange
        .casefold()
    )

    for (
        benchmark_exchange,
        configuration,
    ) in EXCHANGE_MAPPING.items():

        if (
            benchmark_exchange
            .casefold()
            == requested_key
        ):
            return configuration.copy()

    return None


# ============================================================================
# OPENFIGI
# ============================================================================

def get_openfigi_exchange_code(
    exchange: str,
) -> str | None:
    """
    Return the OpenFIGI exchange code associated with a
    benchmark exchange.

    Returns None when no mapping exists.
    """

    configuration = (
        get_exchange_configuration(
            exchange
        )
    )

    if configuration is None:
        return None

    value = configuration.get(
        "openfigi"
    )

    if value is None:
        return None

    normalized = str(
        value
    ).strip()

    return normalized or None


# ============================================================================
# YAHOO FINANCE
# ============================================================================

def get_yahoo_suffix(
    exchange: str,
) -> str | None:
    """
    Return the Yahoo Finance exchange suffix.

    An empty string is a valid suffix for exchanges where Yahoo
    uses the unsuffixed ticker, including NASDAQ and NYSE.

    Returns None when no exchange mapping exists.
    """

    configuration = (
        get_exchange_configuration(
            exchange
        )
    )

    if configuration is None:
        return None

    if (
        "yahoo_suffix"
        not in configuration
    ):
        return None

    value = configuration[
        "yahoo_suffix"
    ]

    if value is None:
        return None

    return str(
        value
    ).strip()


def build_yahoo_symbol(
    ticker: str,
    exchange: str,
) -> str | None:
    """
    Build a Yahoo Finance symbol from benchmark ticker and exchange.

    Provider-specific normalization is applied only where the
    transformation is deterministic.

    Returns None when the benchmark exchange has no configured
    Yahoo mapping.
    """

    normalized_ticker = normalize_ticker(
        ticker
    )

    normalized_exchange = normalize_exchange(
        exchange
    )

    suffix = get_yahoo_suffix(
        normalized_exchange
    )

    if suffix is None:
        return None

    # Remove benchmark punctuation immediately before
    # appending a Yahoo market suffix.
    if suffix:
        normalized_ticker = (
            normalized_ticker.rstrip(".")
        )

    # Yahoo uses four-digit security codes for Hong Kong.
    if (
        normalized_exchange.casefold()
        == (
            "Hong Kong Exchanges "
            "And Clearing Ltd"
        ).casefold()
        and normalized_ticker.isdigit()
    ):
        normalized_ticker = (
            normalized_ticker.zfill(4)
        )

    # Yahoo represents Stockholm share classes with a hyphen.
    if (
        normalized_exchange.casefold()
        == "Nasdaq Omx Nordic".casefold()
    ):
        normalized_ticker = (
            "-".join(
                normalized_ticker.split()
            )
        )

    return (
        normalized_ticker
        + suffix
    )

# ============================================================================
# REFERENCE VALIDATION
# ============================================================================

def validate_exchange_mapping() -> None:
    """
    Validate internal exchange-mapping invariants.

    Controls
    --------
    - mapping cannot be empty;
    - benchmark exchange labels must be unique case-insensitively;
    - every entry must contain openfigi and yahoo_suffix;
    - OpenFIGI values cannot be blank;
    - Yahoo suffix cannot be None;
    - populated Yahoo suffixes must start with a period.
    """

    if not EXCHANGE_MAPPING:
        raise ValueError(
            "EXCHANGE_MAPPING cannot be empty."
        )

    seen_exchanges = set()

    required_provider_keys = {
        "openfigi",
        "yahoo_suffix",
    }

    for (
        exchange,
        configuration,
    ) in EXCHANGE_MAPPING.items():

        normalized_exchange = (
            normalize_exchange(
                exchange
            )
        )

        comparison_key = (
            normalized_exchange
            .casefold()
        )

        if comparison_key in seen_exchanges:
            raise ValueError(
                "Duplicate benchmark exchange "
                f"mapping: {exchange}"
            )

        seen_exchanges.add(
            comparison_key
        )

        if not isinstance(
            configuration,
            dict,
        ):
            raise TypeError(
                "Exchange mapping configuration "
                "must be a dictionary: "
                f"{exchange}"
            )

        missing_keys = (
            required_provider_keys
            - set(
                configuration
            )
        )

        if missing_keys:
            raise ValueError(
                "Exchange mapping is missing "
                f"provider keys for {exchange}: "
                f"{sorted(missing_keys)}"
            )

        openfigi_code = configuration[
            "openfigi"
        ]

        yahoo_suffix = configuration[
            "yahoo_suffix"
        ]

        if openfigi_code is None:
            raise ValueError(
                "OpenFIGI exchange code cannot "
                f"be None: {exchange}"
            )

        normalized_openfigi_code = str(
            openfigi_code
        ).strip()

        if not normalized_openfigi_code:
            raise ValueError(
                "OpenFIGI exchange code cannot "
                f"be blank: {exchange}"
            )

        if yahoo_suffix is None:
            raise ValueError(
                "Yahoo suffix cannot be None: "
                f"{exchange}"
            )

        normalized_suffix = str(
            yahoo_suffix
        ).strip()

        if (
            normalized_suffix
            and not normalized_suffix.startswith(
                "."
            )
        ):
            raise ValueError(
                "Yahoo exchange suffix must "
                "start with '.': "
                f"{exchange} -> "
                f"{normalized_suffix}"
            )


# ============================================================================
# MODULE VALIDATION
# ============================================================================

validate_exchange_mapping()