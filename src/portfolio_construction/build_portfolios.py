"""
Phase 5 portfolio construction.

Equal Weight, Minimum Volatility, and Maximum Sharpe share one
list and one monthly CHF return series. A name is on that list
only when its ODD status is Eligible and its CHF month-end history
is complete. SARON is the risk-free rate. No price, rate, or
expected return is invented.

The weight of a quarter is fixed from the previous ESTIMATION_MONTHS
returns only, then held for the three months of that quarter.
Issuer, sector, zone and listing-currency caps come from the IPS.
The covariance is the Ledoit-Wolf (2004) shrinkage estimator.

Outputs
-------
data/04_portfolios/portfolio_weights.csv
data/04_portfolios/portfolio_metrics.csv
data/04_portfolios/portfolio_comparison.csv
data/04_portfolios/portfolio_monthly_returns.csv
data/04_portfolios/portfolio_membership.csv
"""

import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from src.config import (
    CURRENCY_WEIGHT_CAP,
    ESTIMATION_MONTHS,
    ISSUER_WEIGHT_CAP,
    MARKET_HISTORY_YEARS,
    MAX_POSITIONS,
    MIN_POSITIONS,
    MONTHS_PER_YEAR,
    PORTFOLIO_DATA,
    QUARTERLY_TURNOVER_CAP,
    REPORTING_CURRENCY,
    SECTOR_WEIGHT_CAP,
    ZONE_WEIGHT_CAP,
)
from src.preparation.quality_treatment import (
    CHF_UNIVERSE,
    MARKET_PRICES_CHF,
    SARON_TREATED,
)
from src.utils.logger import logger


STRATEGY_IDS = (
    "equal_weight",
    "minimum_volatility",
    "maximum_sharpe",
)
SOLVER_SEED = 15
SHARPE_STARTS = 8

WEIGHTS_FILE = PORTFOLIO_DATA / "portfolio_weights.csv"
METRICS_FILE = PORTFOLIO_DATA / "portfolio_metrics.csv"
COMPARISON_FILE = PORTFOLIO_DATA / "portfolio_comparison.csv"
MONTHLY_RETURNS_FILE = PORTFOLIO_DATA / "portfolio_monthly_returns.csv"
MEMBERSHIP_FILE = PORTFOLIO_DATA / "portfolio_membership.csv"

WEIGHT_COLUMNS = (
    "strategy",
    "month",
    "yahoo_symbol",
    "ticker",
    "name",
    "listing_currency",
    "weight",
)
METRIC_COLUMNS = (
    "strategy",
    "names",
    "months",
    "window_start",
    "window_end",
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "maximum_drawdown",
    "maximum_weight",
    "effective_holdings",
    "risk_free_series",
    "risk_free_currency",
)
COMPARISON_COLUMNS = (
    "metric",
    "equal_weight",
    "minimum_volatility",
    "maximum_sharpe",
    "minimum_volatility_minus_equal_weight",
    "maximum_sharpe_minus_equal_weight",
)
MONTHLY_COLUMNS = (
    "month",
    "strategy",
    "portfolio_return",
    "risk_free_return",
    "excess_return",
)
MEMBERSHIP_COLUMNS = (
    "yahoo_symbol",
    "ticker",
    "name",
    "exchange",
    "listing_currency",
    "history_decision",
    "history_reason",
    "odd_status",
    "decision",
    "reason",
    "available_price_months",
    "required_price_months",
)
ODD_STATUSES = ("Eligible", "Not_Eligible", "Indeterminate", "Excluded")
COMPARISON_METRICS = (
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "maximum_drawdown",
    "maximum_weight",
    "effective_holdings",
)


def write_csv_atomically(dataframe, output_path, columns):
    """Write a CSV beside the destination, then replace it."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    dataframe.loc[:, list(columns)].to_csv(temporary, index=False)
    temporary.replace(output_path)


def month_end_levels(prices):
    """Keep the last positive CHF close of each symbol and month."""
    levels = prices.loc[:, ["symbol", "timestamp", "close_chf"]].copy()
    levels["timestamp"] = pd.to_datetime(levels["timestamp"])
    levels["month"] = levels["timestamp"].dt.to_period("M")
    levels = levels.sort_values(["symbol", "timestamp"])
    return levels.groupby(["symbol", "month"], as_index=False).tail(1)


def estimation_window(saron):
    """Scored months, the price history they require, and monthly SARON.

    The scored window is the last MARKET_HISTORY_YEARS of SARON.
    Each scored month also needs ESTIMATION_MONTHS of earlier returns,
    so the price history starts one month before that estimation sample.
    """
    labelled = saron.loc[:, ["date", "value", "rate_currency", "series_id"]].copy()
    if labelled["series_id"].astype(str).ne("SARON").any():
        raise RuntimeError("The risk-free file is not the SARON series.")
    if set(labelled["rate_currency"].astype(str)) != {REPORTING_CURRENCY}:
        raise RuntimeError("SARON is not labelled in the reporting currency.")
    labelled["month"] = pd.PeriodIndex(labelled["date"].astype(str), freq="M")
    if labelled["month"].duplicated().any():
        raise RuntimeError("SARON has more than one value in a month.")
    labelled["value"] = pd.to_numeric(labelled["value"], errors="coerce")
    if labelled["value"].isna().any():
        raise RuntimeError("SARON has an empty value.")
    end = labelled["month"].max()
    scored_start = end - (MARKET_HISTORY_YEARS * MONTHS_PER_YEAR - 1)
    history_start = scored_start - ESTIMATION_MONTHS
    scored_months = pd.period_range(scored_start, end, freq="M")
    return_months = pd.period_range(history_start, end, freq="M")
    price_months = pd.period_range(history_start - 1, end, freq="M")
    published = set(labelled["month"])
    if not set(return_months).issubset(published):
        raise RuntimeError("SARON does not cover the months before the scored window.")
    annualized = labelled.set_index("month")["value"].reindex(return_months)
    monthly = (1 + annualized / 100) ** (1 / MONTHS_PER_YEAR) - 1
    monthly.index = monthly.index.astype(str)
    return scored_months, price_months, monthly


def shared_membership(universe, levels, price_months):
    """Decide which CHF-eligible names have a complete month-end history."""
    required = len(price_months)
    observed = levels.loc[levels["month"].isin(price_months)].copy()
    symbols_with_prices = set(levels["symbol"].astype(str))
    rows = []
    for record in universe.itertuples(index=False):
        symbol = str(record.yahoo_symbol)
        history = observed.loc[observed["symbol"].astype(str).eq(symbol)]
        positive_months = history.loc[history["close_chf"] > 0, "month"].nunique()
        if record.chf_status != "included":
            decision = "excluded"
            reason = record.exclusion_reason or "excluded"
        elif symbol not in symbols_with_prices:
            decision = "excluded"
            reason = "no_price_history"
        elif (history["close_chf"] <= 0).any():
            decision = "excluded"
            reason = "non_positive_close"
        elif positive_months < required:
            decision = "excluded"
            reason = "incomplete_monthly_history"
        else:
            decision = "included"
            reason = ""
        rows.append(
            {
                "yahoo_symbol": symbol,
                "ticker": record.ticker,
                "name": record.name,
                "exchange": record.exchange,
                "listing_currency": record.listing_currency,
                "decision": decision,
                "reason": reason,
                "available_price_months": int(positive_months),
                "required_price_months": required,
            }
        )
    membership = pd.DataFrame(rows, columns=list(MEMBERSHIP_COLUMNS))
    return membership.sort_values(
        ["decision", "yahoo_symbol"],
        ascending=[True, True],
    ).reset_index(drop=True)


def shared_monthly_returns(levels, membership, return_months, price_months):
    """Monthly simple CHF returns for the names that share the full window."""
    included = membership.loc[membership["decision"].eq("included"), "yahoo_symbol"]
    if included.nunique() != len(included):
        raise RuntimeError("A symbol is repeated in the shared universe.")
    wide = levels.pivot(index="month", columns="symbol", values="close_chf")
    wide = wide.reindex(index=price_months, columns=list(included))
    if wide.isna().any().any() or (wide <= 0).any().any():
        raise RuntimeError("The shared window contains a missing or non-positive close.")
    returns = wide.pct_change().iloc[1:].copy()
    returns.index = returns.index.astype(str)
    expected = pd.Index(return_months.astype(str))
    if not returns.index.equals(expected):
        raise RuntimeError("Monthly returns do not cover the estimation window.")
    if not np.isfinite(returns.to_numpy()).all():
        raise RuntimeError("A monthly return is not finite.")
    varying = returns.columns[returns.std(ddof=1) > 0]
    if len(varying) < len(returns.columns):
        flat = set(returns.columns) - set(varying)
        membership.loc[membership["yahoo_symbol"].isin(flat), "decision"] = "excluded"
        membership.loc[membership["yahoo_symbol"].isin(flat), "reason"] = "constant_monthly_return"
        returns = returns.loc[:, list(varying)]
    if returns.shape[1] < 2:
        raise RuntimeError("Fewer than two names have a complete CHF history.")
    return returns


def _buckets(labels):
    groups = {}
    for index, label in enumerate(labels):
        groups.setdefault(str(label), []).append(index)
    return {key: np.asarray(value, dtype=int) for key, value in groups.items()}


def _mandate_groups(sectors, zones, currencies):
    grouped = []
    for labels, cap in (
        (sectors, SECTOR_WEIGHT_CAP),
        (zones, ZONE_WEIGHT_CAP),
        (currencies, CURRENCY_WEIGHT_CAP),
    ):
        for indices in _buckets(labels).values():
            grouped.append((indices, cap))
    return grouped


def project_mandate(values, sectors, zones, currencies, locked_out=None):
    """Project onto the long-only IPS caps. Locked names stay at zero."""
    weights = np.clip(np.asarray(values, dtype=float), 0, None)
    locked = np.zeros(len(weights), dtype=bool) if locked_out is None else np.asarray(locked_out, dtype=bool)
    weights[locked] = 0.0
    if float(weights.sum()) <= 0:
        free = np.where(~locked)[0]
        if len(free) == 0:
            raise RuntimeError("Every name is locked out of the mandate.")
        weights[free] = 1.0 / len(free)
    else:
        weights = weights / float(weights.sum())
        weights[locked] = 0.0
        if float(weights.sum()) <= 0:
            raise RuntimeError("Portfolio weights sum to zero.")
        weights = weights / float(weights.sum())
    groups = _mandate_groups(sectors, zones, currencies)
    for _ in range(len(weights) * 20):
        weights = np.minimum(weights, ISSUER_WEIGHT_CAP)
        weights[locked] = 0.0
        for indices, cap in groups:
            total = float(weights[indices].sum())
            if total > cap:
                weights[indices] *= cap / total
        weights[locked] = 0.0
        deficit = 1.0 - float(weights.sum())
        if abs(deficit) <= 1e-10:
            break
        if deficit < 0:
            weights /= float(weights.sum())
            continue
        eligible = ~locked & (weights < ISSUER_WEIGHT_CAP - 1e-14)
        for indices, cap in groups:
            if float(weights[indices].sum()) >= cap - 1e-10:
                eligible[indices] = False
        if not np.any(eligible):
            raise RuntimeError("The mandate caps cannot be met.")
        room = ISSUER_WEIGHT_CAP - weights[eligible]
        if float(room.sum()) <= 0:
            raise RuntimeError("The mandate caps cannot be met.")
        weights[eligible] += deficit * room / float(room.sum())
    if abs(float(weights.sum()) - 1) > 1e-6:
        raise RuntimeError("Projected weights do not sum to one.")
    if float(weights.max()) > ISSUER_WEIGHT_CAP + 1e-6 or float(weights.min()) < -1e-10:
        raise RuntimeError("Projected weights leave the issuer cap.")
    for indices, cap in groups:
        if float(weights[indices].sum()) > cap + 1e-6:
            raise RuntimeError("Projected weights leave a group cap.")
    if float(weights[locked].sum()) > 1e-8:
        raise RuntimeError("A locked name received weight.")
    return weights


def _group_constraints(count, sectors, zones, currencies):
    constraints = [_fully_invested(count)]
    for indices, cap in _mandate_groups(sectors, zones, currencies):
        row = np.zeros(count)
        row[indices] = 1.0
        constraints.append(
            {
                "type": "ineq",
                "fun": lambda weights, row=row, cap=cap: cap - float(row @ weights),
                "jac": lambda weights, row=row: -row,
            }
        )
    return constraints


def _shape_positions(weights, preference):
    """Bring an optimized book back to the 25–40 name band.

    Names outside the kept set are locked at zero. Numerical dust is not a
    position, and the later projection must not put weight back on it.
    """
    weights = np.asarray(weights, dtype=float).copy()
    active = np.where(weights > 1e-8)[0]
    if len(active) > MAX_POSITIONS:
        keep = active[np.argsort(-weights[active])][:MAX_POSITIONS]
    elif len(active) < MIN_POSITIONS:
        chosen = [int(index) for index in active]
        for index in np.argsort(-np.asarray(preference, dtype=float)):
            if len(chosen) >= MIN_POSITIONS:
                break
            if int(index) not in chosen:
                chosen.append(int(index))
        keep = np.asarray(chosen, dtype=int)
        for index in keep:
            if weights[index] <= 0:
                weights[index] = 1e-6
    else:
        keep = active
    mask = np.zeros(len(weights), dtype=bool)
    mask[np.asarray(keep, dtype=int)] = True
    weights[~mask] = 0.0
    return weights, ~mask


def _respect_turnover(weights, previous, locked_out, sectors, zones, currencies):
    """Stay within the quarterly turnover budget by moving toward the previous book."""
    target = project_mandate(weights, sectors, zones, currencies, locked_out)
    if previous is None:
        return target
    if float(np.abs(target - previous).sum()) <= QUARTERLY_TURNOVER_CAP + 1e-8:
        return target
    best = np.asarray(previous, dtype=float).copy()
    low, high = 0.0, 1.0
    for _ in range(24):
        mid = (low + high) / 2
        mixed = (1 - mid) * previous + mid * target
        mixed[locked_out] = 0.0
        mixed = project_mandate(mixed, sectors, zones, currencies, locked_out)
        if float(np.abs(mixed - previous).sum()) <= QUARTERLY_TURNOVER_CAP + 1e-8:
            best = mixed
            low = mid
        else:
            high = mid
    return best


def ledoit_wolf_covariance(returns):
    """Shrink the sample covariance toward the average variance.

    Ledoit and Wolf, Journal of Multivariate Analysis, 2004.
    The sample second-moment matrix uses that paper's divisor T.
    The intensity is clipped to [0, 1].
    """
    values = np.asarray(returns, dtype=float)
    centred = values - values.mean(axis=0)
    observations, count = centred.shape
    squares = centred ** 2
    diagonal = squares.sum(axis=0) / observations
    average_variance = float(diagonal.sum()) / count
    beta_total = float((squares.T @ squares).sum())
    delta_total = float(((centred.T @ centred) ** 2).sum()) / observations ** 2
    beta = (1.0 / (count * observations)) * (beta_total / observations - delta_total)
    delta = (
        delta_total
        - 2.0 * average_variance * float(diagonal.sum())
        + count * average_variance ** 2
    ) / count
    if delta <= 0:
        intensity = 1.0
    else:
        intensity = min(1.0, max(0.0, beta / delta))
    sample = (centred.T @ centred) / observations
    shrunk = (1.0 - intensity) * sample
    shrunk.flat[:: count + 1] += intensity * average_variance
    return shrunk


def _solve(objective, gradient, start, bounds, constraints):
    result = minimize(
        objective,
        start,
        jac=gradient,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 1000, "ftol": 1e-14},
    )
    if not result.success:
        logger.warning("Portfolio solver returned: %s", result.message)
    return result.x


def _mandate_bounds(count):
    return [(0.0, ISSUER_WEIGHT_CAP)] * count


def _fully_invested(count):
    return {
        "type": "eq",
        "fun": lambda weights: np.sum(weights) - 1.0,
        "jac": lambda weights: np.ones(count),
    }


def minimum_volatility(asset_returns, sectors, zones, currencies, previous=None):
    """Smallest variance on this sample, inside the IPS caps."""
    covariance = ledoit_wolf_covariance(asset_returns)
    count = covariance.shape[0]
    equal = project_mandate(np.ones(count), sectors, zones, currencies)
    constraints = _group_constraints(count, sectors, zones, currencies)

    def objective(weights):
        return float(weights @ covariance @ weights)

    def gradient(weights):
        return 2 * covariance @ weights

    solved = _solve(objective, gradient, equal, _mandate_bounds(count), constraints)
    weights = project_mandate(solved, sectors, zones, currencies)
    if objective(weights) > objective(equal) + 1e-8:
        raise RuntimeError("Minimum volatility is above the equal-weight variance on its sample.")
    preference = -np.diag(covariance)
    shaped, locked_out = _shape_positions(weights, preference)
    held = _respect_turnover(shaped, previous, locked_out, sectors, zones, currencies)
    return pd.Series(held, index=asset_returns.columns, dtype=float)


def maximum_sharpe(asset_returns, risk_free, sectors, zones, currencies, previous=None):
    """Highest SARON excess return per unit of risk on this sample, inside the IPS caps."""
    excess = asset_returns.sub(risk_free, axis=0)
    mean_excess = excess.mean().to_numpy()
    covariance = ledoit_wolf_covariance(excess)
    count = covariance.shape[0]
    columns = asset_returns.columns
    constraints = _group_constraints(count, sectors, zones, currencies)

    def statistics(weights):
        expected = float(weights @ mean_excess)
        sigma_w = covariance @ weights
        variance = float(weights @ sigma_w)
        return expected, sigma_w, variance

    def sharpe_of(weights):
        expected, _sigma_w, variance = statistics(weights)
        if variance <= 0:
            return -math.inf
        return expected / math.sqrt(variance)

    def objective(weights):
        return -sharpe_of(weights)

    def gradient(weights):
        expected, sigma_w, variance = statistics(weights)
        volatility = math.sqrt(max(variance, 1e-18))
        return -(mean_excess / volatility - expected * sigma_w / volatility ** 3)

    equal = project_mandate(np.ones(count), sectors, zones, currencies)
    starts = [equal]
    best = int(np.argmax(mean_excess))
    concentrated = np.full(count, (1 - ISSUER_WEIGHT_CAP) / (count - 1))
    concentrated[best] = ISSUER_WEIGHT_CAP
    starts.append(project_mandate(concentrated, sectors, zones, currencies))
    generator = np.random.default_rng(SOLVER_SEED)
    for _draw in range(SHARPE_STARTS):
        starts.append(project_mandate(generator.dirichlet(np.ones(count)), sectors, zones, currencies))

    best_weights = None
    best_sharpe = -math.inf
    for start in starts:
        solved = _solve(objective, gradient, start, _mandate_bounds(count), constraints)
        candidate = project_mandate(solved, sectors, zones, currencies)
        value = sharpe_of(candidate)
        if value > best_sharpe:
            best_sharpe = value
            best_weights = candidate
    if best_weights is None or best_sharpe + 1e-8 < sharpe_of(equal):
        raise RuntimeError("Maximum Sharpe is below the equal-weight Sharpe on its sample.")
    shaped, locked_out = _shape_positions(best_weights, mean_excess)
    held = _respect_turnover(shaped, previous, locked_out, sectors, zones, currencies)
    return pd.Series(held, index=columns, dtype=float)


def portfolio_return(asset_returns, weights):
    """Monthly return from a single weight vector or from one vector per month."""
    if isinstance(weights, pd.DataFrame):
        ordered = weights.reindex(index=asset_returns.index, columns=asset_returns.columns)
        if ordered.isna().any().any():
            raise RuntimeError("A weight is missing for a scored month.")
        return asset_returns.mul(ordered).sum(axis=1)
    ordered = weights.reindex(asset_returns.columns)
    if ordered.isna().any():
        raise RuntimeError("A weight is missing for a shared name.")
    return asset_returns.mul(ordered, axis=1).sum(axis=1)


def measure_strategy(strategy, returns, risk_free, last_weights):
    """Annualize the scored path. The book statistics are those of the last month."""
    excess = returns - risk_free.reindex(returns.index)
    if excess.isna().any():
        raise RuntimeError("A portfolio month has no SARON return.")
    growth = float(np.prod(1 + returns.to_numpy()))
    if growth <= 0:
        raise RuntimeError("Portfolio wealth is not positive.")
    wealth = (1 + returns).cumprod()
    drawdown = wealth / wealth.cummax() - 1
    volatility = float(excess.std(ddof=1))
    return {
        "strategy": strategy,
        "names": int(last_weights.shape[0]),
        "months": int(returns.shape[0]),
        "window_start": str(returns.index.min()),
        "window_end": str(returns.index.max()),
        "annualized_return": growth ** (MONTHS_PER_YEAR / len(returns)) - 1,
        "annualized_volatility": float(returns.std(ddof=1) * math.sqrt(MONTHS_PER_YEAR)),
        "sharpe_ratio": float(excess.mean() / volatility * math.sqrt(MONTHS_PER_YEAR)),
        "maximum_drawdown": float(drawdown.min()),
        "maximum_weight": float(last_weights.max()),
        "effective_holdings": float(1 / np.square(last_weights.to_numpy()).sum()),
        "risk_free_series": "SARON",
        "risk_free_currency": REPORTING_CURRENCY,
    }


def build_comparison(metrics):
    """Place the three strategies on the same metrics."""
    table = metrics.set_index("strategy")
    rows = []
    for metric in COMPARISON_METRICS:
        equal = float(table.loc["equal_weight", metric])
        minimum = float(table.loc["minimum_volatility", metric])
        sharpe = float(table.loc["maximum_sharpe", metric])
        rows.append(
            {
                "metric": metric,
                "equal_weight": equal,
                "minimum_volatility": minimum,
                "maximum_sharpe": sharpe,
                "minimum_volatility_minus_equal_weight": minimum - equal,
                "maximum_sharpe_minus_equal_weight": sharpe - equal,
            }
        )
    return pd.DataFrame(rows, columns=list(COMPARISON_COLUMNS))


def build_monthly_returns(asset_returns, risk_free, strategy_weights):
    """Store the monthly portfolio return, the SARON return, and the excess return."""
    frames = []
    for strategy in STRATEGY_IDS:
        returns = portfolio_return(asset_returns, strategy_weights[strategy])
        frame = pd.DataFrame(
            {
                "month": returns.index.astype(str),
                "strategy": strategy,
                "portfolio_return": returns.to_numpy(),
                "risk_free_return": risk_free.reindex(returns.index).to_numpy(),
                "excess_return": (returns - risk_free.reindex(returns.index)).to_numpy(),
            }
        )
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def build_weight_table(strategy_weights, membership):
    """Attach the security identity to each month's weight."""
    identity = membership.loc[
        membership["decision"].eq("included"),
        ["yahoo_symbol", "ticker", "name", "listing_currency"],
    ]
    frames = []
    for strategy in STRATEGY_IDS:
        panel = strategy_weights[strategy].copy()
        panel.index.name = "month"
        panel.columns.name = "yahoo_symbol"
        frame = panel.stack().rename("weight").reset_index()
        frame.insert(0, "strategy", strategy)
        frames.append(frame.merge(identity, on="yahoo_symbol", how="left", validate="many_to_one"))
    table = pd.concat(frames, ignore_index=True)
    table["strategy"] = pd.Categorical(
        table["strategy"],
        categories=STRATEGY_IDS,
        ordered=True,
    )
    table = table.sort_values(
        ["strategy", "month", "weight", "yahoo_symbol"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)
    table["strategy"] = table["strategy"].astype(str)
    table["month"] = table["month"].astype(str)
    return table.loc[:, list(WEIGHT_COLUMNS)]


def _quarter_start(month):
    return int(str(month)[5:7]) in (1, 4, 7, 10)


def group_labels(columns, membership):
    """Sector, zone and listing currency aligned with the return columns."""
    from src.portfolio_monitoring.build_monitoring import (
        UNCLASSIFIED,
        load_holding_classification,
        zone_for,
    )

    classification = load_holding_classification().drop_duplicates("ticker")
    identity = membership.loc[
        membership["decision"].eq("included"),
        ["yahoo_symbol", "ticker", "listing_currency"],
    ]
    merged = identity.merge(classification, on="ticker", how="left")
    merged = merged.drop_duplicates("yahoo_symbol").set_index("yahoo_symbol")
    sectors, zones, currencies = [], [], []
    for symbol in columns:
        row = merged.loc[symbol]
        sector = "" if pd.isna(row["sector"]) else str(row["sector"]).strip()
        country = "" if pd.isna(row["country"]) else str(row["country"]).strip()
        sectors.append(sector or UNCLASSIFIED)
        zones.append(zone_for(country) if country else UNCLASSIFIED)
        currencies.append(str(row["listing_currency"]))
    return sectors, zones, currencies


def mandate_weights(asset_returns, risk_free, scored_months, membership):
    """Quarterly weights from the preceding sample, held for the three months."""
    history = list(asset_returns.index.astype(str))
    scored = [str(month) for month in scored_months]
    columns = asset_returns.columns
    sectors, zones, currencies = group_labels(columns, membership)
    count = len(columns)
    if count < MIN_POSITIONS:
        raise RuntimeError("The shared list is shorter than the mandate minimum.")
    panels = {strategy: [] for strategy in STRATEGY_IDS}
    base = pd.Series(
        project_mandate(np.ones(count), sectors, zones, currencies),
        index=columns,
        dtype=float,
    )
    held = None
    previous = {"minimum_volatility": None, "maximum_sharpe": None}
    for month in scored:
        if held is None or _quarter_start(month):
            end = history.index(month)
            sample_index = history[end - ESTIMATION_MONTHS : end]
            if len(sample_index) != ESTIMATION_MONTHS or sample_index[-1] >= month:
                raise RuntimeError(f"{month} does not have {ESTIMATION_MONTHS} preceding returns.")
            sample = asset_returns.loc[sample_index]
            sample_rate = risk_free.reindex(sample_index)
            if sample_rate.isna().any():
                raise RuntimeError(f"{month} is missing a SARON observation in its estimation sample.")
            decided = {
                "equal_weight": base,
                "minimum_volatility": minimum_volatility(
                    sample, sectors, zones, currencies, previous["minimum_volatility"]
                ),
                "maximum_sharpe": maximum_sharpe(
                    sample, sample_rate, sectors, zones, currencies, previous["maximum_sharpe"]
                ),
            }
            previous["minimum_volatility"] = decided["minimum_volatility"].to_numpy()
            previous["maximum_sharpe"] = decided["maximum_sharpe"].to_numpy()
            held = decided
            logger.info("Mandate weights fixed for %s", month)
        for strategy, weights in held.items():
            row = weights.reindex(columns)
            if row.isna().any():
                raise RuntimeError(f"{strategy} is missing a weight for {month}.")
            row = row.copy()
            row.name = month
            panels[strategy].append(row)
    return {strategy: pd.DataFrame(rows) for strategy, rows in panels.items()}


def apply_portfolio_entry(membership):
    """Keep a weight only for an Eligible name with a complete CHF history.

    ``decision`` is still the history screen when this function is called.
    Both screens stay visible: ``history_decision`` and ``odd_status``.
    """
    frame = membership.copy()
    frame["history_decision"] = frame["decision"]
    frame["history_reason"] = frame["reason"]
    eligible = frame["odd_status"].eq("Eligible")
    history_ok = frame["history_decision"].eq("included")
    frame["decision"] = "excluded"
    frame["reason"] = frame["odd_status"]
    frame.loc[eligible & ~history_ok, "reason"] = frame.loc[eligible & ~history_ok, "history_reason"]
    frame.loc[eligible & history_ok, "decision"] = "included"
    frame.loc[eligible & history_ok, "reason"] = ""
    return frame.sort_values(
        ["decision", "yahoo_symbol"],
        ascending=[True, True],
    ).reset_index(drop=True)


def attach_odd_status(membership):
    """Copy the ODD status of every listing onto the membership."""
    from src.governance.source_registry import (
        build_dataset_absolute_path,
        get_dataset_by_id,
    )

    configuration = get_dataset_by_id("DS-006").to_dict()
    register = pd.read_csv(
        build_dataset_absolute_path(configuration),
        dtype=str,
        keep_default_na=False,
    )
    required = ("Ticker", "Exchange", "Status")
    absent = [column for column in required if column not in register.columns]
    if absent:
        raise RuntimeError("The ODD register has no column: " + ", ".join(absent))
    status = register.loc[:, list(required)]
    if status.duplicated(["Ticker", "Exchange"]).any():
        raise RuntimeError("The ODD register repeats a listing.")
    merged = membership.merge(
        status,
        left_on=["ticker", "exchange"],
        right_on=["Ticker", "Exchange"],
        how="left",
        validate="one_to_one",
    )
    observed = merged["Status"].fillna("").str.strip()
    invalid = ~observed.isin(ODD_STATUSES)
    if invalid.any():
        names = ", ".join(merged.loc[invalid, "yahoo_symbol"].astype(str))
        raise RuntimeError("A listing has no official ODD status: " + names)
    merged["odd_status"] = observed
    return merged.drop(columns=["Ticker", "Exchange", "Status"])


def load_construction_inputs():
    """Read the treated CHF prices, the eligible universe, and SARON."""
    prices = pd.read_csv(MARKET_PRICES_CHF, usecols=["symbol", "timestamp", "close_chf"])
    universe = pd.read_csv(CHF_UNIVERSE, dtype=str, keep_default_na=False)
    saron = pd.read_csv(SARON_TREATED, dtype=str, keep_default_na=False)
    if universe["yahoo_symbol"].duplicated().any():
        raise RuntimeError("The CHF universe repeats a Yahoo symbol.")
    return prices, universe, saron


def construct_portfolios(prices, universe, saron):
    """Build the three strategies. Each month uses only information already known."""
    levels = month_end_levels(prices)
    scored_months, price_months, risk_free = estimation_window(saron)
    membership = shared_membership(universe, levels, price_months)
    history = shared_monthly_returns(
        levels,
        membership,
        pd.period_range(price_months[0] + 1, price_months[-1], freq="M"),
        price_months,
    )
    included = set(history.columns)
    membership.loc[
        membership["yahoo_symbol"].isin(included),
        ["decision", "reason"],
    ] = ["included", ""]
    membership = attach_odd_status(membership)
    membership = apply_portfolio_entry(membership)
    kept = membership.loc[membership["decision"].eq("included"), "yahoo_symbol"]
    history = history.loc[:, list(kept)]
    weights = mandate_weights(history, risk_free, scored_months, membership)
    scored_index = pd.Index([str(month) for month in scored_months])
    asset_returns = history.loc[scored_index]
    scored_rate = risk_free.reindex(scored_index)
    metrics = pd.DataFrame(
        [
            measure_strategy(
                strategy,
                portfolio_return(asset_returns, weights[strategy]),
                scored_rate,
                weights[strategy].iloc[-1],
            )
            for strategy in STRATEGY_IDS
        ],
        columns=list(METRIC_COLUMNS),
    )
    return {
        "membership": membership.loc[:, list(MEMBERSHIP_COLUMNS)],
        "weights": build_weight_table(weights, membership),
        "metrics": metrics,
        "comparison": build_comparison(metrics),
        "monthly_returns": build_monthly_returns(asset_returns, scored_rate, weights),
        "asset_returns": asset_returns,
        "risk_free": scored_rate,
    }


def build_portfolios():
    """Publish the three portfolios and the series used to calculate them."""
    logger.info("Portfolio construction started")
    prices, universe, saron = load_construction_inputs()
    constructed = construct_portfolios(prices, universe, saron)
    outputs = {
        "weights": (constructed["weights"], WEIGHTS_FILE, WEIGHT_COLUMNS),
        "metrics": (constructed["metrics"], METRICS_FILE, METRIC_COLUMNS),
        "comparison": (constructed["comparison"], COMPARISON_FILE, COMPARISON_COLUMNS),
        "monthly_returns": (
            constructed["monthly_returns"],
            MONTHLY_RETURNS_FILE,
            MONTHLY_COLUMNS,
        ),
        "membership": (constructed["membership"], MEMBERSHIP_FILE, MEMBERSHIP_COLUMNS),
    }
    written = {}
    for name, (dataframe, path, columns) in outputs.items():
        write_csv_atomically(dataframe, path, columns)
        written[name] = path
        logger.info("Portfolio output %s written to %s (%s rows)", name, path, len(dataframe))
    logger.info(
        "Portfolio construction finished: %s names, %s months",
        int(constructed["metrics"]["names"].iloc[0]),
        int(constructed["metrics"]["months"].iloc[0]),
    )
    return written


def main():
    build_portfolios()


if __name__ == "__main__":
    main()
