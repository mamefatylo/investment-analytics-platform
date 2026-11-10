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
    DAILY_FRESHNESS_DAYS,
    HOLDINGS_WEIGHT_TOLERANCE,
    HOLDINGS_WEIGHT_TOTAL,
    MONTHLY_FRESHNESS_DAYS,
    ODD_STATUS_PATTERN,
    OHLC_ABSOLUTE_TOLERANCE,
    OHLC_RELATIVE_TOLERANCE,
    PASS_RATE,
)
from src.quality.quality_rules import rules_for_table
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
            chronology_enabled,
            last_acquisition_date
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

    if kind == "holdings":
        columns = ("market_value", "weight", "notional_value", "price", "fx_rate", "quantity")
        absent = missing_columns(dataframe, columns)
        if absent:
            return True, len(dataframe), f"Colonnes de montant absentes : {', '.join(absent)}."
        numbers = dataframe.loc[:, list(columns)].apply(pd.to_numeric, errors="coerce")
        unreadable = numbers.isna().any(axis=1)
        negative = (numbers < 0).any(axis=1)
        weight_total = float(numbers["weight"].sum())
        weight_off = abs(weight_total - HOLDINGS_WEIGHT_TOTAL) > HOLDINGS_WEIGHT_TOLERANCE
        issues = int((unreadable | negative).sum()) + int(weight_off)
        if issues == 0:
            return True, 0, (
                "Les montants sont lisibles et ne sont pas négatifs. "
                f"La somme des poids est {weight_total:.5f}."
            )
        parts = []
        if int(unreadable.sum()):
            parts.append(f"{int(unreadable.sum())} ligne(s) ont un montant illisible")
        if int(negative.sum()):
            parts.append(f"{int(negative.sum())} ligne(s) ont un montant négatif")
        if weight_off:
            parts.append(f"la somme des poids est {weight_total:.5f}")
        return True, issues, ". ".join(parts) + "."

    if kind == "nav_return":
        column = "monthly_total_nav_return"
        if column not in dataframe.columns or "month_end_date" not in dataframe.columns:
            return True, len(dataframe), "Colonnes du rendement absentes."
        values = pd.to_numeric(dataframe[column], errors="coerce")
        dates = parse_dates(dataframe["month_end_date"])
        blank = is_blank(dataframe[column]) | values.isna()
        earliest = dates.min()
        blank_later = blank & dates.notna() & (dates != earliest)
        unreadable = ~is_blank(dataframe[column]) & values.isna()
        issues = int((blank_later | unreadable).sum())
        if issues == 0:
            return True, 0, "Le rendement de la valeur liquidative est numérique, hors le premier mois."
        return True, issues, f"{issues} mois ont un rendement vide ou illisible après le premier mois."

    if kind == "nav_history":
        if "nav_per_share" not in dataframe.columns or "shares_outstanding" not in dataframe.columns:
            return True, len(dataframe), "Colonnes de la valeur liquidative absentes."
        nav = pd.to_numeric(dataframe["nav_per_share"], errors="coerce")
        shares = pd.to_numeric(dataframe["shares_outstanding"], errors="coerce")
        flagged = nav.isna() | (nav <= 0) | shares.isna() | (shares <= 0)
        issues = int(flagged.sum())
        if issues == 0:
            return True, 0, "La valeur liquidative et les parts en circulation sont strictement positives."
        return True, issues, f"{issues} séance(s) ont une valeur liquidative ou un nombre de parts invalide."

    if kind == "distribution":
        if "total_distribution" not in dataframe.columns:
            return True, len(dataframe), "Colonne du montant de distribution absente."
        total = pd.to_numeric(dataframe["total_distribution"], errors="coerce")
        issues = int((total.isna() | (total <= 0)).sum())
        if issues == 0:
            return True, 0, "Chaque distribution a un montant strictement positif."
        return True, issues, f"{issues} distribution(s) ont un montant vide, nul ou négatif."

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
        elif column == "status":
            invalid = ~text.str.match(ODD_STATUS_PATTERN)
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

    if kind == "valuation":
        if "currency" not in dataframe.columns:
            return True, len(dataframe), "Colonne de devise absente : currency."
        text = dataframe["currency"].astype(str).str.strip()
        present = ~is_blank(dataframe["currency"])
        invalid = ~present | ~text.str.match(ISO_CURRENCY_PATTERN)
        codes = text[present & text.str.match(ISO_CURRENCY_PATTERN)]
        unique_codes = sorted(codes.unique())
        if len(unique_codes) == 1:
            flagged = invalid
            note = f"La devise de valorisation est {unique_codes[0]} sur chaque ligne lisible."
        else:
            flagged = pd.Series(True, index=dataframe.index)
            listed = ", ".join(unique_codes) if unique_codes else "aucune"
            note = f"La devise de valorisation n'est pas unique : {listed}."
        issues = int(flagged.sum())
        if issues == 0:
            return True, 0, note
        return True, issues, note

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


def freshness_allowance(columns) -> int:
    if any("timestamp" in column.lower() for column in columns):
        return DAILY_FRESHNESS_DAYS
    return MONTHLY_FRESHNESS_DAYS


def check_chronology(
    dataframe,
    rules,
    chronology_enabled: bool,
    acquired_at=None,
) -> tuple[bool, int, str]:
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

    date_issues = int(flagged.sum())
    latest_note = ", ".join(latest)
    freshness = "La fraîcheur n'est pas mesurée : last_acquisition_date est absente."
    freshness_issues = 0
    acquired = pd.to_datetime(acquired_at, utc=True, errors="coerce")
    if pd.notna(acquired) and acquired.tzinfo is not None:
        acquired = acquired.tz_convert(None)
    observed = []
    for column in columns:
        parsed = parse_dates(dataframe[column])
        if parsed.notna().any():
            observed.append(parsed.max())
    if pd.isna(acquired):
        freshness_issues = 1
    elif observed:
        latest_observed = max(observed)
        lag_days = int((acquired.normalize() - latest_observed.normalize()).days)
        allowance = int(rules.get("freshness_days") or freshness_allowance(columns))
        if lag_days > allowance:
            freshness_issues = 1
            freshness = (
                f"Série en retard de {lag_days} jours sur le chargement "
                f"du {acquired.date().isoformat()} (plafond {allowance})."
            )
        else:
            freshness = (
                f"Fraîcheur tenue : dernière observation {latest_observed.date().isoformat()}, "
                f"chargement {acquired.date().isoformat()}, écart {max(lag_days, 0)} jours "
                f"(plafond {allowance})."
            )
    if rules.get("chronology_freshness", True) is False:
        freshness = "La fraîcheur ne s'applique pas : cette table est une liste d'événements."
        freshness_issues = 0
    if date_issues == 0:
        dates_note = (
            "Les dates se lisent et ne sont pas postérieures à aujourd'hui. "
            f"Dernière date : {latest_note}."
        )
    else:
        dates_note = (
            f"{date_issues} date(s) illisibles ou postérieures à aujourd'hui. "
            f"Dernière date : {latest_note}."
        )
    note = f"{dates_note} {freshness}"
    issues = date_issues + freshness_issues
    if issues == 0:
        return True, 0, note
    return True, issues, note


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


SDG_NUMBER = re.compile(r"(?i)\b(?:SDG|ODS|ODD|Goals?)\s+(\d{1,2})\b")
SHARE_STATED = re.compile(
    r"(?i)\d+(?:[.,]\d+)?\s*(?:%|percent)\s+of\s+"
    r"(?:the\s+)?(?:reported\s+)?(?:annual\s+)?(?:\d{4}\s+)?"
    r"(?:activity|sales|revenue|net revenues|total revenue)"
)
CONTROVERSY_PENDING = re.compile(
    r"(?i)(pending|no judgment|no assurance|still blocked|pleading stage|"
    r"set of allegations|still open|no controversy finding is recorded|"
    r"not a completed|no completed finding|does not record a decision|"
    r"instruction phase was still blocked)"
)


def _filled(series: pd.Series) -> pd.Series:
    return ~is_blank(series)


def _identity(frame: pd.DataFrame) -> set[tuple[str, str]]:
    return set(
        zip(
            frame["ticker"].astype(str).str.strip(),
            frame["exchange"].astype(str).str.strip(),
        )
    )


def odd_listing_coverage(dataframe: pd.DataFrame, frames: dict) -> tuple[int, str]:
    """One register row for each listing in the security master."""
    master = frames.get("securities_master")
    if master is None or "ticker" not in getattr(master, "columns", ()):
        return 0, "La comparaison au référentiel attend le chargement de DS-002."
    register_keys = _identity(dataframe)
    master_keys = _identity(master)
    missing = master_keys - register_keys
    extra = register_keys - master_keys
    issues = len(missing) + len(extra)
    if issues == 0:
        return 0, f"Chaque titre du référentiel a une ligne ({len(master_keys)})."
    return (
        issues,
        f"{len(missing)} titre(s) du référentiel n'ont pas de ligne. "
        f"{len(extra)} ligne(s) ne sont pas dans le référentiel.",
    )


def odd_register_reading(dataframe: pd.DataFrame) -> str:
    """Count register facts for the phase 4 result. Does not write the source file."""
    sdg = dataframe["sdg"].astype(str)
    printed = sdg.map(
        lambda text: any(1 <= int(value) <= 17 for value in SDG_NUMBER.findall(text))
    )
    controversy = dataframe["controversy"].astype(str)
    pending = controversy.map(lambda text: CONTROVERSY_PENDING.search(text) is not None)
    sourced = _filled(dataframe["controversy"]) & ~pending
    absent = ~_filled(dataframe["controversy"])
    share_text = (
        dataframe["activity"].astype(str)
        + " "
        + dataframe["positive_contribution"].astype(str)
        + " "
        + dataframe["negative_contribution"].astype(str)
    )
    stated = share_text.map(lambda text: SHARE_STATED.search(text) is not None)
    return (
        "Lecture du registre, consignée dans le résultat du contrôle et non dans le fichier source. "
        f"sdg_numeral printed {int(printed.sum())}, absent {int((~printed).sum())}. "
        f"controversy sourced {int(sourced.sum())}, pending {int(pending.sum())}, absent {int(absent.sum())}. "
        f"revenue_share stated {int(stated.sum())}, unprinted {int((~stated).sum())}."
    )


def check_dataset(dataset, dataframe, frames, execution_date: str) -> list[dict]:
    rules = rules_for_table(dataset.table_name)
    records = len(dataframe)
    chronology_enabled = bool(dataset.chronology_enabled) or bool(
        rules.get("chronology_columns")
    )

    missing_issues, missing_notes = check_missing_values(dataset, dataframe, rules)
    duplicate_issues, duplicate_notes = check_duplicates(dataset, dataframe, rules)
    if dataset.table_name == "odd_assessment":
        coverage_issues, coverage_notes = odd_listing_coverage(dataframe, frames)
        duplicate_issues += coverage_issues
        duplicate_notes = f"{duplicate_notes} {coverage_notes}"
    outlier_applicable, outlier_issues, outlier_notes = check_outliers(dataframe, rules)
    format_applicable, format_issues, format_notes = check_formats(dataframe, rules)
    if dataset.table_name == "odd_assessment" and format_applicable:
        reading = odd_register_reading(dataframe)
        format_notes = f"{format_notes} {reading}"
        logger.info("ODD register reading: %s", reading)
    currency_applicable, currency_issues, currency_notes = check_currency(dataframe, rules, frames)
    chronology_applicable, chronology_issues, chronology_notes = check_chronology(
        dataframe,
        rules,
        chronology_enabled,
        getattr(dataset, "last_acquisition_date", None),
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


def published_sheet_tables(dataset) -> list[str]:
    """Table names declared for the other sheets of this dataset."""
    from src.governance.source_registry import get_dataset_by_id

    configuration = get_dataset_by_id(dataset.dataset_id)
    parameters = configuration.get("Parameters")
    if not isinstance(parameters, dict):
        return []
    sheets = parameters.get("sheets") or {}
    if not isinstance(sheets, dict):
        return []
    names = []
    for table_name in sheets.values():
        name = str(table_name).strip()
        if name and name != dataset.table_name and name not in names:
            names.append(name)
    return names


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
            for table_name in published_sheet_tables(dataset):
                if table_name in frames:
                    continue
                sheet = dataset._replace(table_name=table_name)
                if not table_exists(connection, table_name):
                    results.extend(unavailable_results(sheet, execution_date))
                    continue
                frames[table_name] = load_dataset(connection, table_name)
                results.extend(
                    check_dataset(sheet, frames[table_name], frames, execution_date)
                )

        odd = pd.read_sql(
            """
            SELECT dataset_id, source_id, table_name, chronology_enabled,
                   last_acquisition_date
            FROM dataset_registry
            WHERE active = 1
            AND table_name = 'odd_assessment'
            """,
            connection,
        )
        if not odd.empty and table_exists(connection, "odd_assessment"):
            odd_dataset = odd.iloc[0]
            frames["odd_assessment"] = load_dataset(connection, "odd_assessment")
            results.extend(
                check_dataset(
                    odd_dataset,
                    frames["odd_assessment"],
                    frames,
                    execution_date,
                )
            )
            logger.info(
                "ODD file checks recorded. They stay outside the quality score."
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
