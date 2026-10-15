"""
Phase 6 portfolio monitoring.

The three portfolios already share one set of weights. This module
describes those weights and the path those weights produced. It does
not estimate them again. Target weights are put back each month, so
the allocation itself does not drift. What moves is the result:
the value of one franc, the drawdown, and each sector's contribution.

Sector, country and asset class come from the benchmark holdings.
A country that is not in the zone map stays in the book as
"Non classé". A missing holdings row does the same. The weight is
kept in every total.

Outputs
-------
data/05_monitoring/allocation_by_holding.csv
data/05_monitoring/allocation_by_sector.csv
data/05_monitoring/allocation_by_zone.csv
data/05_monitoring/exposure.csv
data/05_monitoring/concentration.csv
data/05_monitoring/top10_positions.csv
data/05_monitoring/monthly_report.csv
data/05_monitoring/portfolio_path.csv
data/05_monitoring/sector_contribution.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd

from src.config import MONITORING_DATA
from src.governance.source_registry import (
    DATASET_KEY,
    OUTPUT_FILE_KEY,
    build_resolved_registry,
    get_parameters,
)
from src.portfolio_construction.build_portfolios import (
    METRICS_FILE,
    MONTHLY_RETURNS_FILE,
    STRATEGY_IDS,
    WEIGHTS_FILE,
    estimation_window,
    load_construction_inputs,
    month_end_levels,
    shared_membership,
    shared_monthly_returns,
    write_csv_atomically,
)
from src.preparation.securities_candidates import (
    WORKSHEET_PARAMETER,
    extract_worksheet_content,
    extract_worksheet_rows,
    find_header_row,
    read_text_file,
    reconstruct_holdings,
    resolve_preparation_paths,
    select_preparation_configuration,
)
from src.utils.logger import logger


TOP_POSITIONS = 10
# Optimizer dust sits near 1e-17. A published holding in this universe
# is at least several basis points. Lines at or below this floor are
# not positions.
POSITION_WEIGHT_FLOOR = 1e-8
UNCLASSIFIED = "Non classé"
CANDIDATES_OUTPUT = "security_candidates.csv"
WEIGHT_SUM_TOLERANCE = 1e-8
METRIC_TOLERANCE = 1e-6
MISSING_LABELS = {"", "--", "nan", "none"}
REQUESTED_EXPOSURES = ("Actions", "Obligations", "ETF")
EXPOSURE_BY_ASSET_CLASS = {
    "equity": "Actions",
    "fixed income": "Obligations",
    "bond": "Obligations",
    "bonds": "Obligations",
    "etf": "ETF",
}
# Location strings published by the holdings file. A new country is
# reported as unclassified instead of being forced into a zone.
COUNTRY_ZONES = {
    "Australia": "Asie-Pacifique",
    "China": "Asie-Pacifique",
    "Hong Kong": "Asie-Pacifique",
    "India": "Asie-Pacifique",
    "Japan": "Asie-Pacifique",
    "Korea (South)": "Asie-Pacifique",
    "Malaysia": "Asie-Pacifique",
    "Singapore": "Asie-Pacifique",
    "Taiwan": "Asie-Pacifique",
    "Thailand": "Asie-Pacifique",
    "Austria": "Europe",
    "Belgium": "Europe",
    "Denmark": "Europe",
    "European Union": "Europe",
    "France": "Europe",
    "Germany": "Europe",
    "Italy": "Europe",
    "Norway": "Europe",
    "Portugal": "Europe",
    "Spain": "Europe",
    "Sweden": "Europe",
    "Switzerland": "Europe",
    "United Kingdom": "Europe",
    "Brazil": "Amériques",
    "Canada": "Amériques",
    "Chile": "Amériques",
    "Mexico": "Amériques",
    "United States": "Amériques",
    "Israel": "Moyen-Orient et Afrique",
    "Saudi Arabia": "Moyen-Orient et Afrique",
    "South Africa": "Moyen-Orient et Afrique",
}

HOLDING_COLUMNS = (
    "strategy",
    "yahoo_symbol",
    "ticker",
    "name",
    "listing_currency",
    "weight",
    "sector",
    "country",
    "zone",
    "asset_class",
    "exposure",
)
SECTOR_COLUMNS = ("strategy", "sector", "weight", "names")
ZONE_COLUMNS = ("strategy", "zone", "weight", "names")
EXPOSURE_COLUMNS = ("strategy", "exposure", "weight", "names")
CONCENTRATION_COLUMNS = (
    "strategy",
    "window_start",
    "window_end",
    "names",
    "invested_names",
    "maximum_weight",
    "top_10_weight",
    "hhi",
    "effective_holdings",
    "concentration_multiple",
)
TOP10_COLUMNS = (
    "strategy",
    "rank",
    "yahoo_symbol",
    "ticker",
    "name",
    "weight",
    "sector",
    "zone",
    "exposure",
)
REPORT_COLUMNS = (
    "strategy",
    "report_month",
    "window_start",
    "window_end",
    "names",
    "invested_names",
    "maximum_weight",
    "top_10_weight",
    "hhi",
    "effective_holdings",
    "concentration_multiple",
    "largest_sector",
    "largest_sector_weight",
    "largest_zone",
    "largest_zone_weight",
    "actions_weight",
    "obligations_weight",
    "etf_weight",
)
PATH_COLUMNS = (
    "month",
    "strategy",
    "portfolio_return",
    "cumulative_wealth",
    "drawdown",
)
CONTRIBUTION_COLUMNS = (
    "month",
    "strategy",
    "sector",
    "month_contribution",
    "cumulative_contribution",
)

HOLDINGS_FILE = MONITORING_DATA / "allocation_by_holding.csv"
SECTOR_FILE = MONITORING_DATA / "allocation_by_sector.csv"
ZONE_FILE = MONITORING_DATA / "allocation_by_zone.csv"
EXPOSURE_FILE = MONITORING_DATA / "exposure.csv"
CONCENTRATION_FILE = MONITORING_DATA / "concentration.csv"
TOP10_FILE = MONITORING_DATA / "top10_positions.csv"
REPORT_FILE = MONITORING_DATA / "monthly_report.csv"
PATH_FILE = MONITORING_DATA / "portfolio_path.csv"
CONTRIBUTION_FILE = MONITORING_DATA / "sector_contribution.csv"
CONTRIBUTION_TOLERANCE = 1e-8


def published_label(value):
    """Keep a source label. Treat the holdings missing markers as empty."""
    text = str(value).strip()
    if text.casefold() in MISSING_LABELS:
        return ""
    return text


def zone_for(country):
    if country == "":
        return UNCLASSIFIED
    return COUNTRY_ZONES.get(country, UNCLASSIFIED)


def exposure_for(asset_class):
    """Map the three requested buckets. Any other source class keeps its name."""
    if asset_class == "":
        return UNCLASSIFIED
    mapped = EXPOSURE_BY_ASSET_CLASS.get(asset_class.casefold())
    if mapped is not None:
        return mapped
    return asset_class


def require_weight_sum(weights, strategy):
    total = float(weights.sum())
    if abs(total - 1) > WEIGHT_SUM_TOLERANCE:
        raise RuntimeError(
            f"{strategy} weights sum to {total}, so the allocation is not renormalized."
        )


def load_holding_classification():
    """Read sector, country and asset class from the holdings worksheet."""
    registry = build_resolved_registry()
    output_name = registry[OUTPUT_FILE_KEY].astype(str).map(
        lambda value: Path(value).name.casefold()
    )
    matched = registry.loc[output_name.eq(CANDIDATES_OUTPUT)]
    if len(matched) != 1:
        raise RuntimeError(
            "The security candidates file is not produced by exactly one active dataset."
        )
    dataset_id = matched.iloc[0][DATASET_KEY]
    configuration = select_preparation_configuration(dataset_id, registry=registry)
    parameters = get_parameters(configuration)
    if WORKSHEET_PARAMETER not in parameters:
        raise RuntimeError("The holdings worksheet name is not in the registry parameters.")
    input_path, _output_path = resolve_preparation_paths(
        configuration,
        registry=registry,
    )
    workbook = read_text_file(input_path)
    worksheet = extract_worksheet_content(workbook, parameters[WORKSHEET_PARAMETER])
    rows = extract_worksheet_rows(worksheet)
    holdings = reconstruct_holdings(rows, find_header_row(rows))
    required = ("Ticker", "Sector", "Asset Class", "Location")
    missing = [column for column in required if column not in holdings.columns]
    if missing:
        raise RuntimeError(
            "The holdings file has no classification column: " + ", ".join(missing)
        )
    classified = holdings.loc[:, list(required)].copy()
    for column in classified.columns:
        classified[column] = classified[column].map(published_label)
    if classified["Ticker"].eq("").any():
        raise RuntimeError("A holding row has no ticker, so it cannot be classified.")
    if classified["Ticker"].duplicated().any():
        raise RuntimeError("A holding ticker is repeated, so its sector is ambiguous.")
    return classified.rename(
        columns={
            "Ticker": "ticker",
            "Sector": "sector",
            "Asset Class": "asset_class",
            "Location": "country",
        }
    )


def load_published_portfolios():
    weights = pd.read_csv(
        WEIGHTS_FILE,
        dtype={"strategy": str, "yahoo_symbol": str, "ticker": str, "name": str},
    )
    metrics = pd.read_csv(METRICS_FILE, dtype={"strategy": str})
    weights["weight"] = pd.to_numeric(weights["weight"], errors="coerce")
    if weights["weight"].isna().any() or weights["weight"].lt(0).any():
        raise RuntimeError("A published weight is empty or negative.")
    if set(weights["strategy"]) != set(STRATEGY_IDS):
        raise RuntimeError("The weight file does not contain the three strategies.")
    if set(metrics["strategy"]) != set(STRATEGY_IDS):
        raise RuntimeError("The metrics file does not contain the three strategies.")
    if weights.duplicated(["strategy", "yahoo_symbol"]).any():
        raise RuntimeError("A strategy lists the same symbol twice.")
    if metrics["window_start"].nunique() != 1 or metrics["window_end"].nunique() != 1:
        raise RuntimeError("The strategies do not share one estimation window.")
    return weights, metrics


def classify_holdings(weights, classification):
    classified = weights.merge(classification, on="ticker", how="left", indicator=True)
    missing = classified.loc[classified["_merge"].ne("both"), "yahoo_symbol"]
    if not missing.empty:
        logger.warning(
            "Holdings classification missing for %s",
            ", ".join(sorted(missing.astype(str).unique())),
        )
    for column in ("sector", "country", "asset_class"):
        classified[column] = classified[column].map(published_label)
    unknown = sorted(
        {
            country
            for country in classified["country"]
            if country and country not in COUNTRY_ZONES
        }
    )
    if unknown:
        logger.warning(
            "Countries without a zone, kept as %s: %s",
            UNCLASSIFIED,
            ", ".join(unknown),
        )
    classified["sector"] = classified["sector"].map(lambda value: value or UNCLASSIFIED)
    classified["zone"] = classified["country"].map(zone_for)
    classified["country"] = classified["country"].map(lambda value: value or UNCLASSIFIED)
    classified["exposure"] = classified["asset_class"].map(exposure_for)
    classified["asset_class"] = classified["asset_class"].map(
        lambda value: value or UNCLASSIFIED
    )
    return classified.drop(columns="_merge")


def group_allocation(classified, key, key_column):
    frames = []
    for strategy in STRATEGY_IDS:
        group = classified.loc[classified["strategy"].eq(strategy)]
        require_weight_sum(group["weight"], strategy)
        invested = group.loc[group["weight"].gt(POSITION_WEIGHT_FLOOR)]
        grouped = (
            invested.groupby(key, as_index=False)
            .agg(weight=("weight", "sum"), names=("yahoo_symbol", "nunique"))
            .rename(columns={key: key_column})
        )
        grouped.insert(0, "strategy", strategy)
        frames.append(grouped.sort_values(["weight", key_column], ascending=[False, True]))
    return pd.concat(frames, ignore_index=True)


def build_exposure(classified):
    grouped = group_allocation(classified, "exposure", "exposure")
    frames = []
    for strategy in STRATEGY_IDS:
        group = grouped.loc[grouped["strategy"].eq(strategy)].set_index("exposure")
        rows = []
        extras = [
            label
            for label in group.index
            if label not in REQUESTED_EXPOSURES
        ]
        for label in list(REQUESTED_EXPOSURES) + extras:
            if label in group.index:
                weight = float(group.loc[label, "weight"])
                names = int(group.loc[label, "names"])
            else:
                weight = 0.0
                names = 0
            rows.append(
                {
                    "strategy": strategy,
                    "exposure": label,
                    "weight": weight,
                    "names": names,
                }
            )
        frames.append(pd.DataFrame(rows))
    return pd.concat(frames, ignore_index=True)


def build_top10(classified):
    frames = []
    for strategy in STRATEGY_IDS:
        group = classified.loc[classified["strategy"].eq(strategy)].sort_values(
            ["weight", "yahoo_symbol"],
            ascending=[False, True],
        )
        chosen = group.head(TOP_POSITIONS).copy()
        chosen["rank"] = np.arange(1, len(chosen) + 1)
        frames.append(chosen)
    return pd.concat(frames, ignore_index=True)


def build_concentration(classified, metrics):
    rows = []
    indexed = metrics.set_index("strategy")
    for strategy in STRATEGY_IDS:
        group = classified.loc[classified["strategy"].eq(strategy)]
        values = group["weight"].to_numpy(dtype=float)
        hhi = float(np.square(values).sum())
        names = int(values.size)
        maximum = float(values.max())
        top_weight = float(np.sort(values)[::-1][:TOP_POSITIONS].sum())
        effective = 1.0 / hhi
        published = indexed.loc[strategy]
        if int(published["names"]) != names:
            raise RuntimeError(f"{strategy} name count does not match the weight file.")
        if abs(float(published["maximum_weight"]) - maximum) > METRIC_TOLERANCE:
            raise RuntimeError(f"{strategy} maximum weight does not match phase 5.")
        if abs(float(published["effective_holdings"]) - effective) > METRIC_TOLERANCE:
            raise RuntimeError(f"{strategy} effective holdings do not match phase 5.")
        rows.append(
            {
                "strategy": strategy,
                "window_start": published["window_start"],
                "window_end": published["window_end"],
                "names": names,
                "invested_names": int(np.count_nonzero(values > POSITION_WEIGHT_FLOOR)),
                "maximum_weight": maximum,
                "top_10_weight": top_weight,
                "hhi": hhi,
                "effective_holdings": effective,
                "concentration_multiple": hhi * names,
            }
        )
    return pd.DataFrame(rows)


def largest_group(table, strategy, key):
    group = table.loc[table["strategy"].eq(strategy)].sort_values(
        ["weight", key],
        ascending=[False, True],
    )
    row = group.iloc[0]
    return str(row[key]), float(row["weight"])


def exposure_weight(exposure, strategy, label):
    matched = exposure.loc[
        exposure["strategy"].eq(strategy) & exposure["exposure"].eq(label),
        "weight",
    ]
    if len(matched) != 1:
        raise RuntimeError(f"{strategy} has no {label} exposure row.")
    return float(matched.iloc[0])


def build_monthly_report(concentration, sectors, zones, exposure):
    rows = []
    for strategy in STRATEGY_IDS:
        facts = concentration.loc[concentration["strategy"].eq(strategy)].iloc[0]
        sector, sector_weight = largest_group(sectors, strategy, "sector")
        zone, zone_weight = largest_group(zones, strategy, "zone")
        rows.append(
            {
                "strategy": strategy,
                "report_month": facts["window_end"],
                "window_start": facts["window_start"],
                "window_end": facts["window_end"],
                "names": int(facts["names"]),
                "invested_names": int(facts["invested_names"]),
                "maximum_weight": float(facts["maximum_weight"]),
                "top_10_weight": float(facts["top_10_weight"]),
                "hhi": float(facts["hhi"]),
                "effective_holdings": float(facts["effective_holdings"]),
                "concentration_multiple": float(facts["concentration_multiple"]),
                "largest_sector": sector,
                "largest_sector_weight": sector_weight,
                "largest_zone": zone,
                "largest_zone_weight": zone_weight,
                "actions_weight": exposure_weight(exposure, strategy, "Actions"),
                "obligations_weight": exposure_weight(exposure, strategy, "Obligations"),
                "etf_weight": exposure_weight(exposure, strategy, "ETF"),
            }
        )
    return pd.DataFrame(rows)


def shared_asset_returns():
    """Rebuild the monthly CHF returns already used to publish the portfolios."""
    prices, universe, saron = load_construction_inputs()
    levels = month_end_levels(prices)
    return_months, price_months, _risk_free = estimation_window(saron)
    membership = shared_membership(universe, levels, price_months)
    return shared_monthly_returns(levels, membership, return_months, price_months)


def build_time_series(classified):
    """Path of the constant target weights, and each sector's additive contribution."""
    asset_returns = shared_asset_returns()
    published = pd.read_csv(MONTHLY_RETURNS_FILE, dtype={"strategy": str, "month": str})
    sector_of = (
        classified.drop_duplicates("yahoo_symbol").set_index("yahoo_symbol")["sector"].astype(str)
    )
    path_frames = []
    contribution_frames = []
    for strategy in STRATEGY_IDS:
        published_symbols = set(
            classified.loc[classified["strategy"].eq(strategy), "yahoo_symbol"].astype(str)
        )
        if published_symbols != set(asset_returns.columns):
            raise RuntimeError(f"{strategy} weights do not match the shared return panel.")
        weights = (
            classified.loc[classified["strategy"].eq(strategy)]
            .set_index("yahoo_symbol")["weight"]
            .reindex(asset_returns.columns)
        )
        contributed = asset_returns.mul(weights, axis=1)
        contributed.index = contributed.index.astype(str)
        total = contributed.sum(axis=1)
        official = (
            published.loc[published["strategy"].eq(strategy)]
            .set_index("month")["portfolio_return"]
            .reindex(total.index)
        )
        if official.isna().any() or not np.allclose(
            total.to_numpy(),
            official.to_numpy(),
            atol=CONTRIBUTION_TOLERANCE,
            rtol=0,
        ):
            raise RuntimeError(f"{strategy} contributions do not rebuild the monthly return.")
        wealth = (1 + total).cumprod()
        path_frames.append(
            pd.DataFrame(
                {
                    "month": total.index,
                    "strategy": strategy,
                    "portfolio_return": total.to_numpy(),
                    "cumulative_wealth": wealth.to_numpy(),
                    "drawdown": (wealth / wealth.cummax() - 1).to_numpy(),
                }
            )
        )
        contributed.index.name = "month"
        contributed.columns.name = "yahoo_symbol"
        melted = contributed.stack().rename("month_contribution").reset_index()
        melted["sector"] = melted["yahoo_symbol"].map(sector_of)
        if melted["sector"].isna().any():
            raise RuntimeError("A name in the return panel has no sector.")
        grouped = melted.groupby(["month", "sector"], as_index=False)["month_contribution"].sum()
        kept = grouped.groupby("sector")["month_contribution"].transform(
            lambda values: values.abs().max() > POSITION_WEIGHT_FLOOR
        )
        grouped = grouped.loc[kept].sort_values(["sector", "month"])
        grouped["cumulative_contribution"] = grouped.groupby("sector")["month_contribution"].cumsum()
        grouped.insert(1, "strategy", strategy)
        contribution_frames.append(grouped)
    return (
        pd.concat(path_frames, ignore_index=True),
        pd.concat(contribution_frames, ignore_index=True),
    )


def build_monitoring():
    """Publish the allocation, exposure and concentration of the current weights."""
    logger.info("Portfolio monitoring started")
    weights, metrics = load_published_portfolios()
    classification = load_holding_classification()
    classified = classify_holdings(weights, classification)
    sectors = group_allocation(classified, "sector", "sector")
    zones = group_allocation(classified, "zone", "zone")
    exposure = build_exposure(classified)
    concentration = build_concentration(classified, metrics)
    top10 = build_top10(classified)
    report = build_monthly_report(concentration, sectors, zones, exposure)
    path, contributions = build_time_series(classified)
    outputs = {
        "holdings": (classified, HOLDINGS_FILE, HOLDING_COLUMNS),
        "sectors": (sectors, SECTOR_FILE, SECTOR_COLUMNS),
        "zones": (zones, ZONE_FILE, ZONE_COLUMNS),
        "exposure": (exposure, EXPOSURE_FILE, EXPOSURE_COLUMNS),
        "concentration": (concentration, CONCENTRATION_FILE, CONCENTRATION_COLUMNS),
        "top10": (top10, TOP10_FILE, TOP10_COLUMNS),
        "report": (report, REPORT_FILE, REPORT_COLUMNS),
        "path": (path, PATH_FILE, PATH_COLUMNS),
        "contributions": (contributions, CONTRIBUTION_FILE, CONTRIBUTION_COLUMNS),
    }
    written = {}
    for name, (dataframe, path, columns) in outputs.items():
        write_csv_atomically(dataframe, path, columns)
        written[name] = path
        logger.info("Monitoring output %s written to %s (%s rows)", name, path, len(dataframe))
    from src.portfolio_monitoring.dashboard import write_dashboard

    written["dashboard"] = write_dashboard()
    logger.info("Monitoring dashboard written to %s", written["dashboard"])
    logger.info(
        "Portfolio monitoring finished: report month %s",
        report["report_month"].iloc[0],
    )
    return written


def main():
    written = build_monitoring()
    print(written["report"])


if __name__ == "__main__":
    main()
