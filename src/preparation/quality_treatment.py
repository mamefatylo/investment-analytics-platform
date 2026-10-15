"""
Phase 4 treatment.

The quality report records defects. This preparation publishes the
series later phases share, without rewriting the raw files and
without inventing a price, a rate, a FIGI, or a cross rate.

Outputs
-------
data/02_processed/market/market_prices_chf.csv
data/02_processed/market/market_price_exclusions.csv
data/02_processed/macro/saron.csv
data/02_processed/macro/macro_indicators.csv
data/03_universe/eligible/chf_portfolio_universe.csv
"""

import argparse
import json
import os
import sqlite3
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd

from src.acquisition.acquisition_logger import log_preparation
from src.config import (
    DATABASE_FILE,
    ELIGIBLE_UNIVERSE,
    PROCESSED_MACRO,
    PROCESSED_MARKET,
    REPORTING_CURRENCY,
)
from src.quality.quality_checks import ohlc_bar_flags, snb_quote_table
from src.quality.quality_config import BLANK_TOKENS
from src.utils.logger import logger


SUCCESS_STATUS = "Success"
FAILED_STATUS = "Failed"

MARKET_PRICES_CHF = PROCESSED_MARKET / "market_prices_chf.csv"
MARKET_PRICE_EXCLUSIONS = PROCESSED_MARKET / "market_price_exclusions.csv"
SARON_TREATED = PROCESSED_MACRO / "saron.csv"
MACRO_TREATED = PROCESSED_MACRO / "macro_indicators.csv"
CHF_UNIVERSE = ELIGIBLE_UNIVERSE / "chf_portfolio_universe.csv"

QUOTE_CURRENCY_FILE = PROCESSED_MARKET / "quote_currency.csv"

# Quote units Yahoo publishes in subunits rather than the major currency.
# The price history is left in that unit, so the close is divided by the factor.
YAHOO_SUBUNIT_QUOTES = {
    "GBp": ("GBP", 100),
    "ZAc": ("ZAR", 100),
    "ILA": ("ILS", 100),
}

# Currency of a series when the source file has no currency column.
# Applied only to rows of that series. Any other series is left unlabelled.
SARON_SERIES = "SARON"
SARON_NOTE = (
    "SARON is the Swiss Average Rate Overnight published by the SNB "
    "in percent at monthly frequency. It is the CHF risk-free rate "
    "for later Sharpe calculations. It is an overnight rate."
)
PRICE_COLUMNS = (
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "listing_currency",
    "quote_currency",
    "quote_scale",
    "fx_period",
    "fx_quote",
    "fx_unit",
    "chf_per_unit",
    "close_chf",
)
EXCLUSION_COLUMNS = (
    "symbol",
    "timestamp",
    "listing_currency",
    "reason",
)
OBSERVATION_COLUMNS = (
    "series_id",
    "date",
    "value",
)
RISK_FREE_COLUMNS = OBSERVATION_COLUMNS + (
    "rate_currency",
    "rate_note",
)
UNIVERSE_COLUMNS = (
    "ticker",
    "name",
    "exchange",
    "listing_currency",
    "yahoo_symbol",
    "chf_status",
    "exclusion_reason",
    "clean_price_rows",
)


def read_table(connection, table_name):
    """Read one loaded table. The name is a fixed identifier, not user input."""
    return pd.read_sql_query(f"SELECT * FROM {table_name}", connection)


def write_csv_atomically(dataframe, output_path, columns):
    """Write a CSV beside the destination, then replace it."""
    if output_path.suffix.casefold() != ".csv":
        raise ValueError(f"Treated output must be CSV: {output_path.name}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    staged = dataframe.loc[:, list(columns)].copy()
    temporary = None
    try:
        with NamedTemporaryFile(
            mode="wb",
            prefix=f".{output_path.stem}_",
            suffix=output_path.suffix,
            dir=output_path.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
        staged.to_csv(temporary, index=False, encoding="utf-8")
        verified = pd.read_csv(
            temporary,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )
        if list(verified.columns) != list(columns) or len(verified) != len(staged):
            raise RuntimeError(f"Staged file failed verification: {output_path.name}")
        os.replace(temporary, output_path)
        temporary = None
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)


def load_quote_currencies():
    """One Yahoo quote currency per symbol. An absent file is not guessed."""
    if not QUOTE_CURRENCY_FILE.exists():
        raise RuntimeError(
            "Quote currency file is missing. "
            "Run python -m src.preparation.quote_currency first."
        )
    currencies = pd.read_csv(
        QUOTE_CURRENCY_FILE,
        dtype=str,
        keep_default_na=False,
    )
    required = {"symbol", "quote_currency"}
    if not required.issubset(currencies.columns):
        raise RuntimeError("Quote currency file must contain symbol and quote_currency.")
    currencies["symbol"] = currencies["symbol"].astype(str).str.strip()
    currencies["quote_currency"] = currencies["quote_currency"].astype(str).str.strip()
    if currencies["symbol"].duplicated().any():
        raise RuntimeError("Quote currency file has more than one row for a symbol.")
    return currencies.loc[:, ["symbol", "quote_currency"]]


def assign_quote_scale(bars):
    """
    Scale from the quote currency Yahoo published for the symbol.

    A code equal to the listing currency is one unit. GBp, ZAc and ILA
    are Yahoo's subunit quotes for GBP, ZAR and ILS. Any other code is
    left unset so the bar is excluded instead of rescaled by guesswork.
    """
    quote = bars["quote_currency"].fillna("").astype(str).str.strip()
    listing = bars["listing_currency"].fillna("").astype(str).str.strip().str.upper()
    scale = pd.Series(pd.NA, index=bars.index, dtype="Float64")
    scale = scale.mask(quote.eq(listing) & quote.ne(""), 1)
    for code, (major, factor) in YAHOO_SUBUNIT_QUOTES.items():
        scale = scale.mask(quote.eq(code) & listing.eq(major), factor)
    return scale


def price_defect(prices):
    """
    Same bar rules as the outlier control, plus a missing close.

    A missing open does not drop the bar when the close sits inside
    the high-low range. Nothing is filled from the previous close.
    """
    outside, negative = ohlc_bar_flags(prices)
    missing_close = prices["close"].isna()
    reason = pd.Series("", index=prices.index, dtype="string")
    reason = reason.mask(outside, "outside_high_low")
    reason = reason.mask(negative, "negative_price")
    reason = reason.mask(missing_close, "missing_close")
    return reason


def snb_quotes(fx_rates):
    """CHF per one unit, using the same SNB parse as the currency control."""
    quotes = snb_quote_table(fx_rates)
    if quotes.duplicated(["listing_currency", "fx_period"]).any():
        raise RuntimeError("SNB file has more than one rate for a currency and month.")
    return quotes


def treat_market_prices(prices, master, fx_rates):
    """Drop unusable bars and convert the remaining close to CHF."""
    bars = prices.loc[:, ["symbol", "timestamp", "open", "high", "low", "close", "volume"]].copy()
    bars["symbol"] = bars["symbol"].astype(str)
    bars["timestamp"] = bars["timestamp"].astype(str).str.slice(0, 10)
    for column in ("open", "high", "low", "close", "volume"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars["price_reason"] = price_defect(bars)

    identity = master.loc[:, ["yahoo_symbol", "listing_currency"]].copy()
    identity["yahoo_symbol"] = identity["yahoo_symbol"].astype(str).str.strip()
    identity["listing_currency"] = (
        identity["listing_currency"].astype(str).str.strip().str.upper()
    )
    bars = bars.merge(
        identity,
        left_on="symbol",
        right_on="yahoo_symbol",
        how="left",
    )
    bars = bars.merge(
        load_quote_currencies(),
        on="symbol",
        how="left",
    )

    quotes = snb_quotes(fx_rates)
    published = set(quotes["listing_currency"])
    bars["fx_period"] = bars["timestamp"].str.slice(0, 7)
    bars = bars.merge(
        quotes,
        on=["listing_currency", "fx_period"],
        how="left",
    )

    reason = bars["price_reason"].astype("string")
    reporting = bars["listing_currency"].eq(REPORTING_CURRENCY)
    known_fx = bars["listing_currency"].isin(published)
    reason = reason.mask(
        reason.eq("") & bars["listing_currency"].isna(),
        "symbol_not_in_master",
    )
    reason = reason.mask(
        reason.eq("") & bars["listing_currency"].notna() & ~reporting & ~known_fx,
        "no_snb_series",
    )
    reason = reason.mask(
        reason.eq("") & known_fx & bars["chf_per_unit"].isna(),
        "missing_monthly_fx",
    )
    bars["reason"] = reason

    reporting_rows = bars["reason"].eq("") & reporting
    bars.loc[reporting_rows, "chf_per_unit"] = 1.0
    bars.loc[reporting_rows, "fx_unit"] = 1.0

    bars["quote_currency"] = bars["quote_currency"].fillna("").astype(str).str.strip()
    bars["quote_scale"] = assign_quote_scale(bars)
    bars["reason"] = bars["reason"].mask(
        bars["reason"].eq("") & bars["quote_currency"].eq(""),
        "quote_currency_missing",
    )
    bars["reason"] = bars["reason"].mask(
        bars["reason"].eq("") & bars["quote_scale"].isna(),
        "quote_currency_unmatched",
    )

    kept = bars["reason"].eq("")
    if bars.loc[kept, "chf_per_unit"].isna().any() or bars.loc[kept, "close"].isna().any():
        raise RuntimeError("A kept price bar has no close or no CHF rate.")
    bars.loc[kept, "close_chf"] = (
        bars.loc[kept, "close"]
        / bars.loc[kept, "quote_scale"]
        * bars.loc[kept, "chf_per_unit"]
    )

    clean = bars.loc[kept, list(PRICE_COLUMNS)].sort_values(
        ["symbol", "timestamp"],
        kind="mergesort",
    )
    excluded = bars.loc[~kept, list(EXCLUSION_COLUMNS)].sort_values(
        ["reason", "symbol", "timestamp"],
        kind="mergesort",
    )
    excluded["listing_currency"] = excluded["listing_currency"].fillna("")
    return clean.reset_index(drop=True), excluded.reset_index(drop=True), published


def build_chf_universe(master, published_currencies, clean_prices):
    """Master names that can be valued in CHF, plus the excluded listings."""
    universe = master.loc[
        :,
        ["ticker", "name", "exchange", "listing_currency", "yahoo_symbol"],
    ].copy()
    universe["listing_currency"] = (
        universe["listing_currency"].astype(str).str.strip().str.upper()
    )
    universe["yahoo_symbol"] = universe["yahoo_symbol"].astype(str).str.strip()
    convertible = universe["listing_currency"].eq(REPORTING_CURRENCY) | universe[
        "listing_currency"
    ].isin(published_currencies)
    universe["chf_status"] = "excluded"
    universe.loc[convertible, "chf_status"] = "included"
    universe["exclusion_reason"] = ""
    universe.loc[~convertible, "exclusion_reason"] = "no_snb_series"
    counts = clean_prices.groupby("symbol").size()
    universe["clean_price_rows"] = (
        universe["yahoo_symbol"].map(counts).fillna(0).astype(int)
    )
    return universe.loc[:, list(UNIVERSE_COLUMNS)].sort_values(
        ["ticker", "exchange"],
        kind="mergesort",
    ).reset_index(drop=True)


def standardise_observations(observations):
    """Store the FRED missing sentinel as an empty value. Do not interpolate."""
    treated = observations.loc[:, ["series_id", "date", "value"]].copy()
    treated["series_id"] = treated["series_id"].astype(str)
    treated["date"] = treated["date"].astype(str).str.slice(0, 10)
    tokens = {token.casefold() for token in BLANK_TOKENS}
    raw_value = treated["value"].astype(str).str.strip()
    missing = treated["value"].isna() | raw_value.str.casefold().isin(tokens)
    treated["value"] = pd.to_numeric(treated["value"], errors="coerce")
    treated.loc[missing, "value"] = pd.NA
    return treated.reset_index(drop=True)


def treat_saron(observations):
    """Publish SARON as the CHF risk-free rate when the source says so."""
    header = observations["header_item_1"].astype(str)
    unit = observations["unit"].astype(str)
    series_key = observations["series_key"].astype(str)
    if (
        not header.str.contains(REPORTING_CURRENCY, regex=False).all()
        or not unit.str.contains("percent", case=False, regex=False).all()
        or not series_key.str.contains(SARON_SERIES, regex=False).all()
    ):
        raise RuntimeError(
            "SARON source is not the CHF overnight series published in percent."
        )
    labelled = pd.DataFrame(
        {
            "series_id": SARON_SERIES,
            "date": observations["date"],
            "value": observations["value"],
        }
    )
    treated = standardise_observations(labelled)
    treated["rate_currency"] = REPORTING_CURRENCY
    treated["rate_note"] = SARON_NOTE
    return treated.loc[:, list(RISK_FREE_COLUMNS)]


def publish_output(dataframe, output_path, columns, notes):
    """Write one treated file and record the preparation."""
    write_csv_atomically(dataframe, output_path, columns)
    run_id = log_preparation(
        output_file=output_path,
        storage_location=output_path.parent,
        status=SUCCESS_STATUS,
        records_produced=len(dataframe),
        notes=json.dumps(notes, ensure_ascii=False, sort_keys=True),
    )
    logger.info(
        "Quality treatment published %s rows to %s (%s)",
        len(dataframe),
        output_path,
        run_id,
    )
    return output_path


def build_quality_treatment():
    """Read the loaded tables and publish the shared CHF inputs."""
    logger.info("Quality treatment started")
    with sqlite3.connect(DATABASE_FILE) as connection:
        prices = read_table(connection, "market_prices")
        master = read_table(connection, "securities_master")
        fx_rates = read_table(connection, "fx_rates")
        saron = read_table(connection, "saron")
        macro = read_table(connection, "macro_indicators")

    clean_prices, exclusions, published = treat_market_prices(prices, master, fx_rates)
    universe = build_chf_universe(master, published, clean_prices)
    saron_treated = treat_saron(saron)
    macro_treated = standardise_observations(macro)

    outputs = {
        "market_prices_chf": publish_output(
            clean_prices,
            MARKET_PRICES_CHF,
            PRICE_COLUMNS,
            {
                "source_rows": len(prices),
                "kept_rows": len(clean_prices),
                "excluded_rows": len(exclusions),
                "reporting_currency": REPORTING_CURRENCY,
            },
        ),
        "market_price_exclusions": publish_output(
            exclusions,
            MARKET_PRICE_EXCLUSIONS,
            EXCLUSION_COLUMNS,
            exclusions["reason"].value_counts().to_dict(),
        ),
        "saron": publish_output(
            saron_treated,
            SARON_TREATED,
            RISK_FREE_COLUMNS,
            {
                "series_id": SARON_SERIES,
                "rate_currency": REPORTING_CURRENCY,
                "empty_values": int(saron_treated["value"].isna().sum()),
            },
        ),
        "macro_indicators": publish_output(
            macro_treated,
            MACRO_TREATED,
            OBSERVATION_COLUMNS,
            {"empty_values": int(macro_treated["value"].isna().sum())},
        ),
        "chf_portfolio_universe": publish_output(
            universe,
            CHF_UNIVERSE,
            UNIVERSE_COLUMNS,
            {
                "included": int(universe["chf_status"].eq("included").sum()),
                "excluded": int(universe["chf_status"].eq("excluded").sum()),
            },
        ),
    }
    logger.info(
        "Quality treatment completed: kept %s price bars, excluded %s, CHF names %s",
        len(clean_prices),
        len(exclusions),
        int(universe["chf_status"].eq("included").sum()),
    )
    return outputs


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Publish the Phase 4 treated datasets used by later phases.",
    )
    return parser.parse_args()


def main():
    parse_arguments()
    outputs = build_quality_treatment()
    for name, path in outputs.items():
        logger.info("Treated dataset %s: %s", name, path)


if __name__ == "__main__":
    main()
