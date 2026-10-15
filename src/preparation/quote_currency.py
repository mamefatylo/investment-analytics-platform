"""
Store the quote currency Yahoo publishes for each master symbol.

The price history does not carry this field. The value is kept as
Yahoo spells it, one row per symbol, so a later treatment can tell
pence (GBp) from pounds (GBP) without looking at the ticker suffix.
"""

import json
import os
import sqlite3
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd

from src.acquisition.acquisition_logger import log_preparation
from src.acquisition.connectors.yfinance import (
    load_yfinance,
    read_quote_currency,
)
from src.config import DATABASE_FILE, PROCESSED_MARKET
from src.utils.logger import logger


SUCCESS_STATUS = "Success"
QUOTE_CURRENCY_FILE = PROCESSED_MARKET / "quote_currency.csv"
QUOTE_CURRENCY_COLUMNS = (
    "symbol",
    "quote_currency",
)


def write_csv_atomically(dataframe, output_path, columns):
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


def load_symbols(connection):
    symbols = pd.read_sql_query(
        """
        SELECT DISTINCT TRIM(yahoo_symbol) AS symbol
        FROM securities_master
        WHERE TRIM(yahoo_symbol) <> ''
        ORDER BY symbol
        """,
        connection,
    )
    return symbols["symbol"].astype(str).tolist()


def collect_quote_currencies(symbols):
    module = load_yfinance()
    rows = []
    for symbol in symbols:
        currency = read_quote_currency(
            yfinance_module=module,
            symbol=symbol,
        )
        rows.append(
            {
                "symbol": symbol,
                "quote_currency": currency or "",
            }
        )
        logger.info(
            "Quote currency %s: %s",
            symbol,
            currency or "absent",
        )
    return pd.DataFrame(rows, columns=list(QUOTE_CURRENCY_COLUMNS))


def load_stored_quote_currencies():
    """Return the stored file, or an empty frame when it has not been created."""
    if not QUOTE_CURRENCY_FILE.exists():
        return pd.DataFrame(columns=list(QUOTE_CURRENCY_COLUMNS))
    stored = pd.read_csv(
        QUOTE_CURRENCY_FILE,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )
    required = set(QUOTE_CURRENCY_COLUMNS)
    if not required.issubset(stored.columns):
        raise RuntimeError("Quote currency file must contain symbol and quote_currency.")
    stored = stored.loc[:, list(QUOTE_CURRENCY_COLUMNS)].copy()
    stored["symbol"] = stored["symbol"].astype(str).str.strip()
    stored["quote_currency"] = stored["quote_currency"].astype(str).str.strip()
    return stored.drop_duplicates("symbol", keep="last")


def complete_quote_currencies():
    """Fetch quote currencies for master symbols that are not stored yet.

    A symbol already present, including one whose currency is blank, is
    left as stored. The full recollection remains build_quote_currency().
    """
    logger.info("Quote currency completion started")
    with sqlite3.connect(DATABASE_FILE) as connection:
        symbols = load_symbols(connection)
    stored = load_stored_quote_currencies()
    known = set(stored["symbol"].astype(str))
    pending = [symbol for symbol in symbols if symbol not in known]
    if not pending:
        logger.info(
            "Quote currencies already cover every master symbol (%s)",
            len(symbols),
        )
        return QUOTE_CURRENCY_FILE

    fetched = collect_quote_currencies(pending)
    kept = stored.loc[~stored["symbol"].isin(pending)]
    combined = pd.concat([kept, fetched], ignore_index=True)
    combined = (
        combined.drop_duplicates("symbol", keep="last")
        .set_index("symbol")
        .reindex(symbols)
        .reset_index()
    )
    combined["quote_currency"] = combined["quote_currency"].fillna("")
    write_csv_atomically(combined, QUOTE_CURRENCY_FILE, QUOTE_CURRENCY_COLUMNS)
    present = int(combined["quote_currency"].ne("").sum())
    run_id = log_preparation(
        output_file=QUOTE_CURRENCY_FILE,
        storage_location=QUOTE_CURRENCY_FILE.parent,
        status=SUCCESS_STATUS,
        records_produced=len(combined),
        notes=json.dumps(
            {
                "symbols": len(combined),
                "fetched": len(pending),
                "currencies_present": present,
                "currencies_absent": len(combined) - present,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
    )
    logger.info(
        "Quote currency completion fetched %s symbols, run %s",
        len(pending),
        run_id,
    )
    return QUOTE_CURRENCY_FILE


def build_quote_currency():
    logger.info("Quote currency collection started")
    with sqlite3.connect(DATABASE_FILE) as connection:
        symbols = load_symbols(connection)
    collected = collect_quote_currencies(symbols)
    write_csv_atomically(collected, QUOTE_CURRENCY_FILE, QUOTE_CURRENCY_COLUMNS)
    present = int(collected["quote_currency"].ne("").sum())
    run_id = log_preparation(
        output_file=QUOTE_CURRENCY_FILE,
        storage_location=QUOTE_CURRENCY_FILE.parent,
        status=SUCCESS_STATUS,
        records_produced=len(collected),
        notes=json.dumps(
            {
                "symbols": len(collected),
                "currencies_present": present,
                "currencies_absent": len(collected) - present,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
    )
    logger.info(
        "Quote currency collection completed: %s symbols, %s currencies, run %s",
        len(collected),
        present,
        run_id,
    )
    return QUOTE_CURRENCY_FILE


def main():
    build_quote_currency()


if __name__ == "__main__":
    main()
