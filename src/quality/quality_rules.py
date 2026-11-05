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
            "yahoo_symbol",
        ),
        "duplicate_key": ("ticker", "exchange"),
        "outlier": None,
        "formats": ("currency", "listing_currency"),
        "currency": "listing",
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
    "odd_assessment": {
        "mandatory": (
            "ticker",
            "exchange",
            "company",
            "activity",
            "status",
            "positive",
        ),
        "mandatory_when": {
            "status": {
                "Eligible": (
                    "sdg",
                    "positive_contribution",
                    "negative_contribution",
                    "evidence",
                    "source",
                    "controversy",
                ),
            },
        },
        "duplicate_key": ("ticker", "exchange"),
        "outlier": None,
        "formats": ("status",),
        "currency": None,
        "chronology_columns": (),
    },
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
