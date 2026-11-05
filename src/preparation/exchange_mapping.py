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
yahoo_suffix
    Yahoo Finance exchange suffix used to construct market-data
    symbols.

listing_currency
    ISO currency of the listing venue. This is not the benchmark
    holdings currency and not the SNB quotation code.

listing_code
    Trading code published by the venue when it differs from the
    iShares ticker. Maintained by hand, one line at a time, with the
    page that publishes the code.

Design principles
-----------------
- the benchmark exchange label is the internal reference key;
- this mapping is used during Preparation only;
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
        "yahoo_suffix": "",
        "listing_currency": "USD",
    },
    "NYSE": {
        "yahoo_suffix": "",
        "listing_currency": "USD",
    },
    "SIX Swiss Exchange": {
        "yahoo_suffix": ".SW",
        "listing_currency": "CHF",
    },
    "London Stock Exchange": {
        "yahoo_suffix": ".L",
        "listing_currency": "GBP",
    },
    "Hong Kong Exchanges And Clearing Ltd": {
        "yahoo_suffix": ".HK",
        "listing_currency": "HKD",
    },
    "Tokyo Stock Exchange": {
        "yahoo_suffix": ".T",
        "listing_currency": "JPY",
    },
    "Taiwan Stock Exchange": {
        "yahoo_suffix": ".TW",
        "listing_currency": "TWD",
    },
    "Toronto Stock Exchange": {
        "yahoo_suffix": ".TO",
        "listing_currency": "CAD",
    },
    "Shanghai Stock Exchange": {
        "yahoo_suffix": ".SS",
        "listing_currency": "CNY",
    },
    "Shenzhen Stock Exchange": {
        "yahoo_suffix": ".SZ",
        "listing_currency": "CNY",
    },
    "Singapore Exchange": {
        "yahoo_suffix": ".SI",
        "listing_currency": "SGD",
    },
    "National Stock Exchange Of India": {
        "yahoo_suffix": ".NS",
        "listing_currency": "INR",
    },
    "Korea Exchange (Stock Market)": {
        "yahoo_suffix": ".KS",
        "listing_currency": "KRW",
    },
    "Korea Exchange (Kosdaq)": {
        "yahoo_suffix": ".KQ",
        "listing_currency": "KRW",
    },
    "Xetra": {
        "yahoo_suffix": ".DE",
        "listing_currency": "EUR",
    },
    "Bolsa De Madrid": {
        "yahoo_suffix": ".MC",
        "listing_currency": "EUR",
    },
    "Bolsa Mexicana De Valores": {
        "yahoo_suffix": ".MX",
        "listing_currency": "MXN",
    },
    "Oslo Bors Asa": {
        "yahoo_suffix": ".OL",
        "listing_currency": "NOK",
    },
    "Nasdaq Omx Nordic": {
        "yahoo_suffix": ".ST",
        "listing_currency": "SEK",
    },
    "Omx Nordic Exchange Copenhagen A/S": {
        "yahoo_suffix": ".CO",
        "listing_currency": "DKK",
    },
    "Nyse Euronext - Euronext Paris": {
        "yahoo_suffix": ".PA",
        "listing_currency": "EUR",
    },
    "Nyse Euronext - Euronext Brussels": {
        "yahoo_suffix": ".BR",
        "listing_currency": "EUR",
    },
    "Nyse Euronext - Euronext Lisbon": {
        "yahoo_suffix": ".LS",
        "listing_currency": "EUR",
    },
    "Wiener Boerse Ag": {
        "yahoo_suffix": ".VI",
        "listing_currency": "EUR",
    },
    "Johannesburg Stock Exchange": {
        "yahoo_suffix": ".JO",
        "listing_currency": "ZAR",
    },
    "Tel Aviv Stock Exchange": {
        "yahoo_suffix": ".TA",
        "listing_currency": "ILS",
    },
    "Saudi Stock Exchange": {
        "yahoo_suffix": ".SR",
        "listing_currency": "SAR",
    },
    "Borsa Italiana": {
        "yahoo_suffix": ".MI",
        "listing_currency": "EUR",
    },
    "Asx - All Markets": {
        "yahoo_suffix": ".AX",
        "listing_currency": "AUD",
    },
}


# ============================================================================
# LISTING CODE ALIASES
# ============================================================================
#
# The iShares ticker is not always the venue trading code.
# Each row is entered after reading the page in "source".
# The Yahoo suffix still comes from EXCHANGE_MAPPING.

LISTING_CODE_ALIASES = (
    {
        "ticker": "CICT",
        "exchange": "Singapore Exchange",
        "listing_code": "C38U",
        "source": "https://investor.cict.com.sg/stock_fundamentals.html",
    },
    {
        "ticker": "CLAR",
        "exchange": "Singapore Exchange",
        "listing_code": "A17U",
        "source": "https://investor.capitaland-ascendasreit.com/stock-fundamentals.html",
    },
)


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
# LISTING CURRENCY
# ============================================================================

def get_listing_currency(
    exchange: str,
) -> str | None:
    """
    Return the ISO listing currency of a benchmark exchange.

    The value is the venue currency, such as JPY or USD.
    It is not the benchmark holdings currency and not an SNB
    quotation code such as JPY100.

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
        "listing_currency"
    )

    if value is None:
        return None

    normalized = str(
        value
    ).strip()

    return normalized or None


def get_listing_code(
    ticker: str,
    exchange: str,
) -> str | None:
    """
    Return the venue trading code when it differs from the iShares ticker.

    Returns None when the iShares ticker is the code to send.
    """

    normalized_ticker = normalize_ticker(ticker).casefold()
    normalized_exchange = normalize_exchange(exchange).casefold()

    for alias in LISTING_CODE_ALIASES:
        if (
            alias["ticker"].casefold() == normalized_ticker
            and alias["exchange"].casefold() == normalized_exchange
        ):
            return alias["listing_code"]

    return None


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

    When a listing-code alias exists, that venue code replaces the
    iShares ticker. The Yahoo suffix still comes from the exchange.

    Returns None when the benchmark exchange has no configured
    Yahoo mapping.
    """

    normalized_ticker = normalize_ticker(
        ticker
    )

    normalized_exchange = normalize_exchange(
        exchange
    )

    listing_code = get_listing_code(
        normalized_ticker,
        normalized_exchange,
    )

    if listing_code is not None:
        normalized_ticker = listing_code

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
    - every entry must contain yahoo_suffix and listing_currency;
    - Yahoo suffix cannot be None;
    - populated Yahoo suffixes must start with a period;
    - listing_currency must be a three-letter ISO code.
    """

    if not EXCHANGE_MAPPING:
        raise ValueError(
            "EXCHANGE_MAPPING cannot be empty."
        )

    seen_exchanges = set()

    required_provider_keys = {
        "yahoo_suffix",
        "listing_currency",
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

        yahoo_suffix = configuration[
            "yahoo_suffix"
        ]

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

        listing_currency = configuration[
            "listing_currency"
        ]

        if not isinstance(
            listing_currency,
            str,
        ):
            raise ValueError(
                "Listing currency must be a "
                f"three-letter ISO code: {exchange}"
            )

        normalized_currency = (
            listing_currency.strip()
        )

        if (
            len(normalized_currency) != 3
            or not normalized_currency.isalpha()
            or not normalized_currency.isupper()
        ):
            raise ValueError(
                "Listing currency must be a "
                "three-letter ISO code: "
                f"{exchange} -> "
                f"{normalized_currency}"
            )

    seen_aliases = set()

    for alias in LISTING_CODE_ALIASES:
        ticker = str(alias.get("ticker", "")).strip()
        exchange = str(alias.get("exchange", "")).strip()
        listing_code = str(alias.get("listing_code", "")).strip()
        source = str(alias.get("source", "")).strip()
        key = (ticker.casefold(), exchange.casefold())

        if not ticker or not exchange or not listing_code or not source:
            raise ValueError(
                "A listing-code alias needs a ticker, an exchange, "
                "a listing code, and the page that publishes it."
            )

        if key in seen_aliases:
            raise ValueError(
                "Duplicate listing-code alias: "
                f"{ticker} / {exchange}"
            )

        seen_aliases.add(key)

        if get_exchange_configuration(exchange) is None:
            raise ValueError(
                "Listing-code alias exchange is not in the "
                f"exchange table: {exchange}"
            )

        if "." in listing_code or listing_code.casefold() == ticker.casefold():
            raise ValueError(
                "Listing code must be the venue code, without a "
                f"Yahoo suffix: {ticker} -> {listing_code}"
            )


# ============================================================================
# MODULE VALIDATION
# ============================================================================

validate_exchange_mapping()