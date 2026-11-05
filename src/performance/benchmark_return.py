"""
Published iShares NAV return, converted to CHF with the SNB rate.

The benchmark workbook already contains the monthly total NAV return.
This module reads that figure. It does not rebuild it from today's
weights, and it does not fill a missing month.

The holdings sheet states the currency of the market values. Rows
whose FX rate is 1 are already in that currency, which the price
times quantity check confirms. The NAV return is in the same currency.

The return column has no unit label. A month with no distribution
must match the change in the published month-end NAV. That match
decides whether the figure is a percentage or a decimal. If it
matches neither, the module stops.

CHF conversion uses the SNB end-of-month rate already in the project:

    R_chf = (1 + R_nav) * (S_t / S_{t-1}) - 1

S is CHF per one unit of the fund currency. A missing rate, a missing
return, or a missing prior month stops the calculation.
"""

import re
from pathlib import Path

import pandas as pd

from src.governance.source_registry import (
    DATASET_KEY,
    OUTPUT_FILE_KEY,
    build_dataset_absolute_path,
    build_resolved_registry,
)
from src.preparation.quality_treatment import snb_quotes
from src.preparation.securities_candidates import (
    extract_worksheet_content,
    extract_worksheet_rows,
    read_text_file,
    resolve_preparation_paths,
    select_preparation_configuration,
)


ENGLISH_MONTHS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
DATE_PATTERN = re.compile(r"([A-Z][a-z]{2}) (\d{1,2}), (\d{4})")
MISSING_MARKS = {"", "--", "nan"}
# The sheet often prints two decimal percentage points. The total return
# can also differ from the pure NAV change by a few hundredths of a point.
# The tolerance only chooses the unit. The stored return stays the published figure.
UNIT_TOLERANCE = 5e-4
VALUE_TOLERANCE = 1e-4
MINIMUM_UNIT_CHECKS = 6
PERFORMANCE_SHEET = "Performance"
HISTORICAL_SHEET = "Historical"
HOLDINGS_SHEET = "Holdings"
DISTRIBUTIONS_SHEET = "Distributions"
CANDIDATES_OUTPUT = "security_candidates.csv"
FX_OUTPUT = "fx_rates.csv"


def _workbook_path():
    """The benchmark file already downloaded for the holdings."""
    registry = build_resolved_registry()
    output_name = registry[OUTPUT_FILE_KEY].astype(str).map(
        lambda value: Path(value).name.casefold()
    )
    matched = registry.loc[output_name.eq(CANDIDATES_OUTPUT)]
    if len(matched) != 1:
        raise RuntimeError(
            "The security candidates file is not produced by exactly one active dataset."
        )
    configuration = select_preparation_configuration(
        matched.iloc[0][DATASET_KEY],
        registry=registry,
    )
    input_path, _output_path = resolve_preparation_paths(configuration, registry=registry)
    return input_path


def _fx_path():
    """The SNB file already downloaded for the CHF conversion."""
    registry = build_resolved_registry()
    output_name = registry[OUTPUT_FILE_KEY].astype(str).map(
        lambda value: Path(value).name.casefold()
    )
    matched = registry.loc[output_name.eq(FX_OUTPUT)]
    if len(matched) != 1:
        raise RuntimeError("The FX file is not produced by exactly one active dataset.")
    return build_dataset_absolute_path(matched.iloc[0].to_dict())


def _sheet_rows(workbook, name):
    return extract_worksheet_rows(extract_worksheet_content(workbook, name))


def _parse_date(label):
    match = DATE_PATTERN.fullmatch(str(label).strip())
    if match is None:
        return None
    month = ENGLISH_MONTHS.get(match.group(1))
    if month is None:
        raise RuntimeError(f"The benchmark file has an unreadable date: {label}")
    return pd.Timestamp(year=int(match.group(3)), month=month, day=int(match.group(2)))


def _month_key(stamp):
    return f"{stamp.year:04d}-{stamp.month:02d}"


def _number(value):
    text = str(value).strip()
    if text.casefold() in MISSING_MARKS:
        return None
    try:
        return float(text)
    except ValueError as error:
        raise RuntimeError(f"The benchmark file has a value that is not a number: {value}") from error


def _fund_name(performance_rows, holdings_rows):
    performance_name = next(
        (str(row[0]).strip() for row in performance_rows if str(row[0]).strip()),
        "",
    )
    holdings_name = next(
        (
            str(row[0]).strip()
            for row in holdings_rows
            if str(row[0]).strip() and _parse_date(row[0]) is None and not str(row[0]).strip()[:1].isdigit()
        ),
        "",
    )
    if not performance_name or performance_name != holdings_name:
        raise RuntimeError("The performance sheet and the holdings sheet do not name the same fund.")
    return performance_name


def _fund_currency(holdings_rows):
    """Currency of the market values. An FX rate of 1 means no conversion."""
    header = next((row for row in holdings_rows if row and str(row[0]).strip() == "Ticker"), None)
    if header is None:
        raise RuntimeError("The holdings sheet has no Ticker header.")
    columns = {str(name).strip(): index for index, name in enumerate(header)}
    for name in ("Currency", "FX Rate", "Price", "Quantity", "Market Value", "Asset Class"):
        if name not in columns:
            raise RuntimeError(f"The holdings sheet has no {name} column.")
    currencies = set()
    checked = 0
    for row in holdings_rows:
        if len(row) <= columns["Market Value"] or str(row[0]).strip() in {"", "Ticker"}:
            continue
        if len(row) <= max(columns.values()):
            raise RuntimeError("A holdings row is shorter than the header.")
        currency = str(row[columns["Currency"]]).strip()
        if not currency:
            raise RuntimeError("A holding has no currency.")
        currencies.add(currency)
        if str(row[columns["Asset Class"]]).strip() != "Equity":
            continue
        rate = _number(row[columns["FX Rate"]])
        if rate is None or rate != 1.0:
            continue
        price = _number(row[columns["Price"]])
        quantity = _number(row[columns["Quantity"]])
        market_value = _number(row[columns["Market Value"]])
        if None in (price, quantity, market_value) or market_value == 0:
            raise RuntimeError("An equity line quoted at FX rate 1 has an empty price or value.")
        gap = abs(price * quantity - market_value) / abs(market_value)
        if gap > VALUE_TOLERANCE:
            raise RuntimeError(
                "Price times quantity does not match market value for an equity line at FX rate 1, "
                "so the fund currency is not confirmed."
            )
        checked += 1
    if len(currencies) != 1:
        raise RuntimeError(
            "The holdings sheet does not state one currency for the market values: "
            + ", ".join(sorted(currencies))
        )
    if checked == 0:
        raise RuntimeError("No holding is quoted with an FX rate of 1.")
    return next(iter(currencies))


def _published_returns(performance_rows):
    header_seen = False
    published = {}
    for row in performance_rows:
        if not row:
            continue
        label = str(row[0]).strip()
        if label == "Month End Date":
            if len(row) < 2 or str(row[1]).strip() != "Monthly Total (NAV) Return":
                raise RuntimeError("The performance sheet does not publish the monthly total NAV return.")
            header_seen = True
            continue
        stamp = _parse_date(label)
        if stamp is None:
            continue
        if not header_seen:
            raise RuntimeError("A performance date appears before the return header.")
        if len(row) < 2:
            raise RuntimeError(f"The NAV return for {label} has no value.")
        month = _month_key(stamp)
        if month in published:
            raise RuntimeError(f"The NAV return is published twice for {month}.")
        published[month] = _number(row[1])
    if not header_seen or not published:
        raise RuntimeError("The performance sheet has no monthly total NAV return.")
    return published


def _month_end_nav(historical_rows):
    if not historical_rows or str(historical_rows[0][0]).strip() != "As Of":
        raise RuntimeError("The historical sheet does not start with the NAV date.")
    if len(historical_rows[0]) < 2 or str(historical_rows[0][1]).strip() != "NAV per Share":
        raise RuntimeError("The historical sheet has no NAV per Share column.")
    latest = {}
    for row in historical_rows[1:]:
        stamp = _parse_date(row[0])
        if stamp is None:
            raise RuntimeError(f"The historical sheet has an unreadable date: {row[0]}")
        nav = _number(row[1])
        if nav is None:
            continue
        month = _month_key(stamp)
        current = latest.get(month)
        if current is None or stamp > current[0]:
            latest[month] = (stamp, nav)
    if not latest:
        raise RuntimeError("The historical sheet has no NAV.")
    return {month: nav for month, (_stamp, nav) in latest.items()}


def _dividend_months(distribution_rows):
    header = next(
        (row for row in distribution_rows if row and str(row[0]).strip() == "Record Date"),
        None,
    )
    if header is None or "Ex-Date" not in [str(cell).strip() for cell in header]:
        raise RuntimeError("The distributions sheet has no Ex-Date column.")
    ex_date = [str(cell).strip() for cell in header].index("Ex-Date")
    months = set()
    for row in distribution_rows:
        if len(row) <= ex_date or str(row[0]).strip() in {"", "Record Date"}:
            continue
        stamp = _parse_date(row[ex_date])
        if stamp is None:
            raise RuntimeError(f"A distribution has an unreadable ex-date: {row[ex_date]}")
        months.add(_month_key(stamp))
    return months


def _return_scale(published, navs, dividend_months):
    """100 when the sheet publishes percentage points, 1 when it publishes a decimal."""
    percent_gaps = []
    decimal_gaps = []
    for month, raw in published.items():
        if raw is None or month in dividend_months:
            continue
        prior = _month_key((pd.Period(month, freq="M") - 1).to_timestamp())
        if month not in navs or prior not in navs or navs[prior] == 0:
            continue
        change = navs[month] / navs[prior] - 1
        percent_gaps.append(abs(raw / 100 - change))
        decimal_gaps.append(abs(raw - change))
    if len(percent_gaps) < MINIMUM_UNIT_CHECKS:
        raise RuntimeError(
            "Not enough months without a distribution to read the unit of the NAV return."
        )
    percent_ok = max(percent_gaps) <= UNIT_TOLERANCE
    decimal_ok = max(decimal_gaps) <= UNIT_TOLERANCE
    if percent_ok and not decimal_ok:
        return 100.0
    if decimal_ok and not percent_ok:
        return 1.0
    raise RuntimeError(
        "The published NAV return matches neither the NAV change nor the NAV change in percent."
    )


def _chf_per_unit(currency):
    quotes = snb_quotes(pd.read_csv(_fx_path()))
    series = quotes.loc[quotes["listing_currency"].eq(currency)].set_index("fx_period")["chf_per_unit"]
    if series.empty:
        raise RuntimeError(f"The SNB file has no series for {currency}.")
    if series.index.duplicated().any():
        raise RuntimeError(f"The SNB file repeats a month for {currency}.")
    return series.sort_index()


def load_benchmark_returns(months):
    """CHF total return of the fund for each requested month."""
    workbook = read_text_file(_workbook_path())
    performance_rows = _sheet_rows(workbook, PERFORMANCE_SHEET)
    holdings_rows = _sheet_rows(workbook, HOLDINGS_SHEET)
    fund_name = _fund_name(performance_rows, holdings_rows)
    currency = _fund_currency(holdings_rows)
    published = _published_returns(performance_rows)
    scale = _return_scale(
        published,
        _month_end_nav(_sheet_rows(workbook, HISTORICAL_SHEET)),
        _dividend_months(_sheet_rows(workbook, DISTRIBUTIONS_SHEET)),
    )
    rates = _chf_per_unit(currency)
    rows = []
    for month in months:
        raw = published.get(month)
        if raw is None:
            raise RuntimeError(f"The fund publishes no NAV return for {month}.")
        prior = _month_key((pd.Period(month, freq="M") - 1).to_timestamp())
        if month not in rates.index or prior not in rates.index:
            raise RuntimeError(f"The SNB file has no {currency} rate for {prior} or {month}.")
        current = float(rates.loc[month])
        previous = float(rates.loc[prior])
        if previous == 0:
            raise RuntimeError(f"The SNB {currency} rate for {prior} is zero.")
        local_return = raw / scale
        rows.append(
            {
                "month": month,
                "fund_name": fund_name,
                "fund_currency": currency,
                "nav_total_return": local_return,
                "chf_per_unit": current,
                "prior_chf_per_unit": previous,
                "benchmark_return_chf": (1 + local_return) * (current / previous) - 1,
            }
        )
    frame = pd.DataFrame(rows)
    if frame["fund_name"].nunique() != 1 or frame["fund_currency"].nunique() != 1:
        raise RuntimeError("The benchmark series does not stay on one fund and one currency.")
    return frame
