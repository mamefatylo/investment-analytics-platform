"""
Règles qualité par table publiée.

Le registre décide quelles tables sont contrôlées. Ce module dit
comment contrôler une table dont le grain est connu. Une table
absente de ce référentiel reçoit les règles génériques.
"""

TABLE_RULES = {
    "securities_master": {
        "mandatory": (
            "ticker",
            "name",
            "exchange",
            "currency",
            "listing_currency",
            "asset_class",
            "resolution_status",
        ),
        "mandatory_when": {
            "resolution_status": {
                "RESOLVED": ("figi", "exch_code", "openfigi_ticker"),
            }
        },
        "duplicate_key": ("ticker", "exchange"),
        "outlier": None,
        "formats": ("currency", "listing_currency", "figi"),
        "currency": "listing",
        "chronology_columns": (),
    },
    "openfigi_mapping": {
        "mandatory": ("id_type", "id_value", "figi"),
        "duplicate_key": ("input_position", "result_position"),
        "outlier": None,
        "formats": ("figi",),
        "currency": None,
        "chronology_columns": (),
    },
    "market_prices": {
        "mandatory": ("symbol", "timestamp", "open", "high", "low", "close", "volume"),
        "duplicate_key": ("symbol", "timestamp"),
        "outlier": "ohlc",
        "formats": (),
        "currency": None,
        "chronology_columns": ("timestamp",),
    },
    "fx_rates": {
        "mandatory": ("series_key", "date", "value", "unit"),
        "duplicate_key": ("series_key", "date"),
        "outlier": "positive_value",
        "formats": (),
        "currency": "fx_quote",
        "chronology_columns": ("date",),
    },
    "saron": {
        "mandatory": ("series_key", "date", "value", "unit"),
        "duplicate_key": ("series_key", "date"),
        "outlier": "saron",
        "formats": (),
        "currency": "saron_quote",
        "chronology_columns": ("date",),
    },
    "macro_indicators": {
        "mandatory": ("series_id", "date", "value"),
        "duplicate_key": ("series_id", "date"),
        "outlier": "macro",
        "formats": (),
        "currency": None,
        "chronology_columns": ("date",),
    },
    "fundamentals_sec": {
        "mandatory": (
            "cik",
            "concept",
            "unit",
            "end",
            "value",
            "accession",
            "filed",
        ),
        "duplicate_key": ("cik", "concept", "unit", "start", "end", "accession"),
        "outlier": None,
        "formats": ("value",),
        "currency": None,
        "chronology_columns": ("end", "filed"),
        "chronology_order": ("start", "end"),
    },
}


# lower, upper, lower inclusive, upper inclusive. None means no bound.
# A series id absent from this map is an outlier: a new indicator is
# not treated as acceptable until a bound is declared here.
MACRO_VALUE_BOUNDS = {
    "UNRATE": (0, 100, True, True),
    "CPIAUCSL": (0, None, False, True),
    "FEDFUNDS": (0, 30, True, True),
}


def generic_rules() -> dict:
    """Règles utilisées pour une table publiée sans règle dédiée."""
    return {
        "mandatory": (),
        "duplicate_key": (),
        "outlier": None,
        "formats": (),
        "currency": None,
        "chronology_columns": (),
    }


def rules_for_table(table_name: str) -> dict:
    """Retourne la règle dédiée ou la règle générique."""
    return TABLE_RULES.get(table_name, generic_rules())
