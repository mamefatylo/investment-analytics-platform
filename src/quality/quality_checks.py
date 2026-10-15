"""
quality_checks.py

Phase 4 - Data Quality Framework

Contrôles
---------
QC-001 Valeurs manquantes
QC-002 Doublons
QC-003 Valeurs aberrantes
QC-004 Formats incorrects
QC-005 Cohérence des devises
QC-006 Chronologie
"""

from datetime import date, datetime
import re
import sqlite3

import pandas as pd

from src.config import DATABASE_FILE, REPORTING_CURRENCY
from src.preparation.exchange_mapping import get_listing_currency
from src.quality.quality_config import (
    BLANK_TOKENS,
    CONTROL_IDS,
    CONTROL_ORDER,
    FAIL_RATE,
    FIGI_PATTERN,
    ISO_CURRENCY_PATTERN,
    OHLC_ABSOLUTE_TOLERANCE,
    OHLC_RELATIVE_TOLERANCE,
    PASS_RATE,
)
from src.quality.quality_rules import MACRO_VALUE_BOUNDS, rules_for_table
from src.utils.logger import logger


def create_connection():
    return sqlite3.connect(DATABASE_FILE)


def load_quality_enabled_datasets(connection) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT
            dataset_id,
            source_id,
            table_name,
            chronology_enabled
        FROM dataset_registry
        WHERE active = 1
        AND quality_enabled = 1
        ORDER BY dataset_id
        """,
        connection,
    )


def table_exists(connection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
        AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def load_dataset(connection, table_name: str) -> pd.DataFrame:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Invalid SQLite identifier: {table_name}")
    return pd.read_sql(
        f'SELECT * FROM "{table_name}"',
        connection,
    )


def is_blank(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.lower()
    return series.isna() | text.isin(BLANK_TOKENS)


def control_status(issues: int, records_checked: int, applicable: bool) -> str:
    if not applicable:
        return "NOT_APPLICABLE"
    if records_checked <= 0:
        return "FAIL"
    if issues <= 0:
        return "PASS"
    rate = 100.0 * (1.0 - (issues / records_checked))
    if rate < FAIL_RATE:
        return "FAIL"
    if rate < PASS_RATE or issues > 0:
        return "WARNING"
    return "PASS"


def build_result(
    dataset,
    control_name: str,
    issues: int,
    records_checked: int,
    notes: str,
    execution_date: str,
    applicable: bool = True,
) -> dict:
    status = control_status(issues, records_checked, applicable)
    return {
        "dataset_id": dataset.dataset_id,
        "source_id": dataset.source_id,
        "table_name": dataset.table_name,
        "control_id": CONTROL_IDS[control_name],
        "control_name": control_name,
        "status": status,
        "issues": int(issues if applicable else 0),
        "records_checked": int(records_checked),
        "execution_date": execution_date,
        "notes": notes,
        "applicable": int(applicable),
    }


def missing_columns(dataframe: pd.DataFrame, columns) -> list[str]:
    return [column for column in columns if column not in dataframe.columns]


def check_missing_values(dataset, dataframe, rules) -> tuple[int, str]:
    mandatory = list(rules.get("mandatory", ()))
    absent = missing_columns(dataframe, mandatory)
    if absent:
        return len(dataframe), f"Colonnes obligatoires absentes : {', '.join(absent)}."
    if dataframe.empty:
        return 1, "La table ne contient aucune ligne."

    row_missing = pd.Series(False, index=dataframe.index)
    for column in mandatory:
        row_missing = row_missing | is_blank(dataframe[column])

    conditional = rules.get("mandatory_when", {})
    for column, expected in conditional.items():
        if column not in dataframe.columns:
            return len(dataframe), f"Colonne de condition absente : {column}."
        observed = dataframe[column].astype(str).str.strip()
        for value, required_columns in expected.items():
            selected = observed.eq(value)
            absent_required = missing_columns(dataframe, required_columns)
            if absent_required:
                return (
                    int(selected.sum()),
                    f"Colonnes obligatoires absentes pour {value} : {', '.join(absent_required)}.",
                )
            for required in required_columns:
                row_missing = row_missing | (
                    selected & is_blank(dataframe[required])
                )

    issues = int(row_missing.sum())
    if issues == 0:
        return 0, "Les champs obligatoires sont renseignés."
    counted = []
    for column in mandatory:
        count = int(is_blank(dataframe[column]).sum())
        if count:
            counted.append(f"{column} {count}")
    detail = f" ({', '.join(counted)})" if counted else ""
    return issues, f"{issues} ligne(s) ont au moins un champ obligatoire vide{detail}."


def check_duplicates(dataset, dataframe, rules) -> tuple[int, str]:
    key = list(rules.get("duplicate_key", ()))
    if not key:
        key = list(dataframe.columns)
    absent = missing_columns(dataframe, key)
    if absent:
        return len(dataframe), f"Clé de doublon absente : {', '.join(absent)}."
    if dataframe.empty:
        return 0, "Table vide : aucun doublon à mesurer."
    issues = int(dataframe.duplicated(key).sum())
    label = ", ".join(key)
    if issues == 0:
        return 0, f"Aucun doublon sur la clé {label}."
    return issues, f"{issues} ligne(s) en doublon sur la clé {label}."


def check_outliers(dataframe, rules) -> tuple[bool, int, str]:
    kind = rules.get("outlier")
    if not kind:
        return False, 0, "Pas de mesure numérique contrôlée sur cette table."
    if dataframe.empty:
        return True, 1, "La table ne contient aucune ligne."

    if kind == "ohlc":
        prices = dataframe.copy()
        for column in ("open", "high", "low", "close", "volume"):
            prices[column] = pd.to_numeric(prices[column], errors="coerce")
        outside, negative = ohlc_bar_flags(prices)
        issues = int((outside | negative).sum())
        if issues == 0:
            return True, 0, "Aucun cours négatif ni barre incohérente."
        return True, issues, f"{issues} barre(s) ont un cours négatif ou hors de la fourchette high-low."

    if kind == "positive_value":
        values = pd.to_numeric(dataframe["value"], errors="coerce")
        issues = int((values.notna() & (values <= 0)).sum())
        if issues == 0:
            return True, 0, "Tous les taux de change renseignés sont strictement positifs."
        return True, issues, f"{issues} taux de change sont négatifs ou nuls."

    if kind == "saron":
        values = pd.to_numeric(dataframe["value"], errors="coerce")
        issues = int((values.notna() & ((values < -5) | (values > 15))).sum())
        if issues == 0:
            return True, 0, "Le SARON renseigné reste dans l'intervalle -5 à 15."
        return True, issues, f"{issues} observation(s) du SARON sont hors de l'intervalle -5 à 15."

    if kind == "macro":
        values = pd.to_numeric(dataframe["value"], errors="coerce")
        series = dataframe["series_id"].astype(str)
        flagged = pd.Series(False, index=dataframe.index)
        unknown = []
        for series_id, index in series.groupby(series, sort=False).groups.items():
            present = values.loc[index].notna()
            bounds = MACRO_VALUE_BOUNDS.get(str(series_id))
            if bounds is None:
                if bool(present.any()):
                    unknown.append(str(series_id))
                    flagged.loc[index] = present
                continue
            lower, upper, lower_inclusive, upper_inclusive = bounds
            outside = pd.Series(False, index=index)
            if lower is not None:
                outside = outside | (present & (values.loc[index] < lower if lower_inclusive else values.loc[index] <= lower))
            if upper is not None:
                outside = outside | (present & (values.loc[index] > upper if upper_inclusive else values.loc[index] >= upper))
            flagged.loc[index] = outside
        issues = int(flagged.sum())
        if issues == 0:
            return True, 0, "Les indicateurs macro renseignés restent dans leur intervalle."
        if unknown:
            names = ", ".join(sorted(unknown))
            return True, issues, (
                f"{issues} observation(s) macro sont hors intervalle ou sans borne déclarée : {names}."
            )
        return True, issues, f"{issues} observation(s) macro sont hors de leur intervalle."

    return False, 0, "Règle d'aberration inconnue."


def check_formats(dataframe, rules) -> tuple[bool, int, str]:
    columns = list(rules.get("formats", ()))
    if not columns:
        return False, 0, "Aucun format d'identifiant à contrôler sur cette table."
    if dataframe.empty:
        return True, 1, "La table ne contient aucune ligne."

    issues = 0
    details = []
    for column in columns:
        if column not in dataframe.columns:
            issues += len(dataframe)
            details.append(f"{column} absente")
            continue
        present = ~is_blank(dataframe[column])
        text = dataframe[column].astype(str).str.strip()
        if column in {"currency", "listing_currency"}:
            invalid = present & ~text.str.match(ISO_CURRENCY_PATTERN)
        elif column == "figi":
            invalid = present & ~text.str.match(FIGI_PATTERN)
        elif column == "value":
            numeric = pd.to_numeric(dataframe[column], errors="coerce")
            invalid = present & numeric.isna()
        else:
            invalid = pd.Series(False, index=dataframe.index)
        count = int(invalid.sum())
        issues += count
        if count:
            details.append(f"{column}: {count}")

    if issues == 0:
        return True, 0, "Les identifiants contrôlés respectent leur format."
    return True, issues, "Formats incorrects. " + "; ".join(details) + "."


def ohlc_bar_flags(prices: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Outside-range and negative flags. Columns are already numeric."""
    span = (prices["high"] - prices["low"]).abs()
    tolerance = (span * OHLC_RELATIVE_TOLERANCE).clip(lower=OHLC_ABSOLUTE_TOLERANCE)
    comparable = prices[["open", "high", "low", "close"]].notna().all(axis=1)
    outside = comparable & (
        (prices["open"] > prices["high"] + tolerance)
        | (prices["open"] < prices["low"] - tolerance)
        | (prices["close"] > prices["high"] + tolerance)
        | (prices["close"] < prices["low"] - tolerance)
        | (prices["high"] < prices["low"])
    )
    negative = (prices[["open", "high", "low", "close", "volume"]] < 0).any(axis=1)
    return outside.fillna(False), negative.fillna(False)


def snb_quote_table(fx_rates: pd.DataFrame) -> pd.DataFrame:
    """One CHF-per-unit rate for each currency and month present in the file."""
    columns = ["listing_currency", "fx_unit", "fx_period", "fx_quote", "chf_per_unit"]
    if fx_rates is None or fx_rates.empty or "series_key" not in fx_rates.columns:
        return pd.DataFrame(columns=columns)
    extracted = fx_rates["series_key"].astype(str).str.extract(r",([A-Z]{3})(\d+)")
    quotes = pd.DataFrame(
        {
            "listing_currency": extracted[0],
            "fx_unit": pd.to_numeric(extracted[1], errors="coerce"),
            "fx_period": fx_rates["date"].astype(str).str.slice(0, 7),
            "fx_quote": pd.to_numeric(fx_rates["value"], errors="coerce"),
        }
    )
    quotes = quotes.dropna(subset=["listing_currency", "fx_unit", "fx_quote"])
    quotes = quotes[quotes["fx_unit"] > 0].copy()
    quotes["chf_per_unit"] = quotes["fx_quote"] / quotes["fx_unit"]
    return quotes.loc[:, columns].reset_index(drop=True)


def fx_currency_codes(frames: dict[str, pd.DataFrame]) -> set[str]:
    fx = frames.get("fx_rates")
    if fx is None:
        return set()
    return set(snb_quote_table(fx)["listing_currency"].dropna().unique())


def check_currency(dataframe, rules, frames) -> tuple[bool, int, str]:
    kind = rules.get("currency")
    if not kind:
        return False, 0, "Cette table ne porte pas de devise à rapprocher."
    if dataframe.empty:
        return True, 1, "La table ne contient aucune ligne."

    if kind == "listing":
        mapped = dataframe["exchange"].map(get_listing_currency)
        listing = dataframe["listing_currency"].astype(str).str.strip()
        mismatch = mapped.isna() | listing.ne(mapped.fillna(""))
        published = fx_currency_codes(frames)
        needs_rate = ~listing.eq(REPORTING_CURRENCY)
        missing_rate = needs_rate & ~listing.isin(published)
        flagged = mismatch | missing_rate
        issues = int(flagged.sum())
        missing_codes = sorted(listing[missing_rate].unique())
        if issues == 0:
            return True, 0, (
                "Chaque devise de cotation correspond à la place et à une série de change, "
                f"ou est déjà en {REPORTING_CURRENCY}."
            )
        parts = []
        if int(mismatch.sum()):
            parts.append(f"{int(mismatch.sum())} devise(s) de cotation diffèrent de la place")
        if missing_codes:
            parts.append(
                f"{int(missing_rate.sum())} titre(s) sans série de change : {', '.join(missing_codes)}"
            )
        return True, issues, ". ".join(parts) + "."

    if kind == "fx_quote":
        unit_ok = dataframe["unit"].astype(str).str.contains(REPORTING_CURRENCY, regex=False)
        values = pd.to_numeric(dataframe["value"], errors="coerce")
        codes = dataframe["series_key"].astype(str).str.extract(r",([A-Z]{3})\d+", expand=False)
        flagged = ~unit_ok | codes.isna() | (values.notna() & (values <= 0))
        issues = int(flagged.sum())
        if issues == 0:
            return True, 0, (
                f"Les séries sont cotées en {REPORTING_CURRENCY}, "
                "avec un code devise lisible et un taux positif."
            )
        return True, issues, f"{issues} observation(s) de change ont une unité, un code ou un taux incohérent."

    if kind == "saron_quote":
        header = dataframe["header_item_1"].astype(str)
        unit = dataframe["unit"].astype(str)
        series_key = dataframe["series_key"].astype(str)
        flagged = (
            ~header.str.contains(REPORTING_CURRENCY, regex=False)
            | ~unit.str.contains("percent", case=False, regex=False)
            | ~series_key.str.contains("SARON", regex=False)
        )
        issues = int(flagged.sum())
        if issues == 0:
            return True, 0, f"Le SARON est publié en {REPORTING_CURRENCY}, en pourcentage."
        return True, issues, (
            f"{issues} observation(s) SARON n'ont pas le libellé "
            f"{REPORTING_CURRENCY}, le pourcentage ou le code SARON."
        )

    return False, 0, "Règle de devise inconnue."


def parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def check_chronology(dataframe, rules, chronology_enabled: bool) -> tuple[bool, int, str]:
    if not chronology_enabled:
        return False, 0, "Chronologie désactivée pour ce jeu."
    columns = list(rules.get("chronology_columns", ()))
    if not columns:
        detected = [
            column
            for column in dataframe.columns
            if any(token in column.lower() for token in ("date", "timestamp"))
        ]
        columns = detected
    if not columns:
        return True, 1, "Chronologie activée, mais aucune colonne de date n'est définie."
    absent = missing_columns(dataframe, columns)
    if absent:
        return True, len(dataframe), f"Colonnes de date absentes : {', '.join(absent)}."
    if dataframe.empty:
        return True, 1, "La table ne contient aucune ligne."

    flagged = pd.Series(False, index=dataframe.index)
    today = pd.Timestamp(date.today())
    latest = []
    for column in columns:
        parsed = parse_dates(dataframe[column])
        present = ~is_blank(dataframe[column])
        flagged = flagged | (present & parsed.isna()) | (parsed.notna() & (parsed > today))
        if parsed.notna().any():
            latest.append(f"{column} {parsed.max().date().isoformat()}")

    order = rules.get("chronology_order")
    if order and all(column in dataframe.columns for column in order):
        start, end = order
        start_parsed = parse_dates(dataframe[start])
        end_parsed = parse_dates(dataframe[end])
        comparable = start_parsed.notna() & end_parsed.notna()
        flagged = flagged | (comparable & (start_parsed > end_parsed))

    issues = int(flagged.sum())
    latest_note = ", ".join(latest)
    if issues == 0:
        return True, 0, f"Les dates se lisent et ne sont pas postérieures à aujourd'hui. Dernière date : {latest_note}."
    return True, issues, f"{issues} date(s) illisibles ou postérieures à aujourd'hui. Dernière date : {latest_note}."


def unavailable_results(dataset, execution_date: str) -> list[dict]:
    note = "La table physique est absente. Le contrôle ne peut pas lire les lignes."
    return [
        build_result(
            dataset,
            control_name,
            issues=1,
            records_checked=0,
            notes=note,
            execution_date=execution_date,
            applicable=True,
        )
        for control_name in CONTROL_ORDER
    ]


def check_dataset(dataset, dataframe, frames, execution_date: str) -> list[dict]:
    rules = rules_for_table(dataset.table_name)
    records = len(dataframe)
    chronology_enabled = bool(dataset.chronology_enabled)

    missing_issues, missing_notes = check_missing_values(dataset, dataframe, rules)
    duplicate_issues, duplicate_notes = check_duplicates(dataset, dataframe, rules)
    outlier_applicable, outlier_issues, outlier_notes = check_outliers(dataframe, rules)
    format_applicable, format_issues, format_notes = check_formats(dataframe, rules)
    currency_applicable, currency_issues, currency_notes = check_currency(dataframe, rules, frames)
    chronology_applicable, chronology_issues, chronology_notes = check_chronology(
        dataframe,
        rules,
        chronology_enabled,
    )

    outcomes = (
        ("Valeurs manquantes", True, missing_issues, missing_notes),
        ("Doublons", True, duplicate_issues, duplicate_notes),
        ("Valeurs aberrantes", outlier_applicable, outlier_issues, outlier_notes),
        ("Formats incorrects", format_applicable, format_issues, format_notes),
        ("Cohérence des devises", currency_applicable, currency_issues, currency_notes),
        ("Chronologie", chronology_applicable, chronology_issues, chronology_notes),
    )
    return [
        build_result(
            dataset,
            control_name,
            issues,
            records,
            notes,
            execution_date,
            applicable,
        )
        for control_name, applicable, issues, notes in outcomes
    ]


def run_quality_checks(execution_date: str | None = None) -> pd.DataFrame:
    logger.info("Quality controls started")
    if execution_date is None:
        execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    results = []
    connection = create_connection()
    try:
        datasets = load_quality_enabled_datasets(connection)
        frames = {}
        loaded = []
        for dataset in datasets.itertuples(index=False):
            if not table_exists(connection, dataset.table_name):
                results.extend(unavailable_results(dataset, execution_date))
                continue
            frames[dataset.table_name] = load_dataset(connection, dataset.table_name)
            loaded.append(dataset)

        for dataset in loaded:
            results.extend(
                check_dataset(
                    dataset,
                    frames[dataset.table_name],
                    frames,
                    execution_date,
                )
            )
    finally:
        connection.close()

    controls = pd.DataFrame(results)
    logger.info("Controls executed: %s", len(controls))
    logger.info("Quality controls completed")
    return controls


def main():
    print(run_quality_checks().to_string(index=False))


if __name__ == "__main__":
    main()
