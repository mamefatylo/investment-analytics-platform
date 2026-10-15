"""
Phase 5 portfolio construction.

Equal Weight, Minimum Volatility, and Maximum Sharpe share one
CHF-eligible universe and one monthly CHF return series. SARON is
the risk-free rate. No price, rate, or weight is invented.

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
    MARKET_HISTORY_YEARS,
    MONTHS_PER_YEAR,
    PORTFOLIO_DATA,
    REPORTING_CURRENCY,
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
    "decision",
    "reason",
    "available_price_months",
    "required_price_months",
)
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
    """Last ten SARON years, as monthly return dates and the price months they need."""
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
    start = end - (MARKET_HISTORY_YEARS * MONTHS_PER_YEAR - 1)
    return_months = pd.period_range(start, end, freq="M")
    price_months = pd.period_range(start - 1, end, freq="M")
    published = set(labelled["month"])
    if not set(return_months).issubset(published):
        raise RuntimeError("SARON does not cover the estimation window.")
    annualized = labelled.set_index("month")["value"].reindex(return_months)
    monthly = (1 + annualized / 100) ** (1 / MONTHS_PER_YEAR) - 1
    monthly.index = monthly.index.astype(str)
    return return_months, price_months, monthly


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


def equal_weight(asset_returns):
    """Give every included name the same weight."""
    count = asset_returns.shape[1]
    return pd.Series(1 / count, index=asset_returns.columns, dtype=float)


def _long_only_weights(values, columns):
    """Drop numerical dust below zero and bring the weights back to a full investment."""
    weights = pd.Series(values, index=columns, dtype=float).clip(lower=0)
    total = float(weights.sum())
    if total <= 0:
        raise RuntimeError("Portfolio weights sum to zero.")
    return weights / total


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


def minimum_volatility(asset_returns):
    """Long-only fully invested portfolio with the smallest sample variance."""
    covariance = asset_returns.cov().to_numpy()
    count = covariance.shape[0]

    def objective(weights):
        return float(weights @ covariance @ weights)

    def gradient(weights):
        return 2 * covariance @ weights

    start = np.full(count, 1 / count)
    bounds = [(0.0, 1.0)] * count
    constraints = {
        "type": "eq",
        "fun": lambda weights: np.sum(weights) - 1.0,
        "jac": lambda weights: np.ones(count),
    }
    solved = _solve(objective, gradient, start, bounds, constraints)
    weights = _long_only_weights(solved, asset_returns.columns)
    equal = objective(start)
    if objective(weights.to_numpy()) > equal + 1e-10:
        raise RuntimeError("Minimum volatility is above the equal-weight variance.")
    return weights


def maximum_sharpe(asset_returns, risk_free):
    """Long-only portfolio with the highest SARON excess return per unit of risk."""
    excess = asset_returns.sub(risk_free, axis=0)
    mean_excess = excess.mean().to_numpy()
    covariance = excess.cov().to_numpy()
    count = covariance.shape[0]
    columns = asset_returns.columns

    def sharpe_of(weights):
        aligned = _long_only_weights(weights, columns).to_numpy()
        expected = float(aligned @ mean_excess)
        variance = float(aligned @ covariance @ aligned)
        if variance <= 0:
            return -math.inf
        return expected / math.sqrt(variance)

    if np.all(mean_excess <= 0):
        return _maximum_sharpe_invested(excess, mean_excess, covariance)

    def objective(weights):
        return -float(mean_excess @ weights)

    def gradient(weights):
        return -mean_excess

    bounds = [(0.0, None)] * count
    constraints = {
        "type": "ineq",
        "fun": lambda weights: 1.0 - float(weights @ covariance @ weights),
        "jac": lambda weights: -2 * covariance @ weights,
    }
    best = int(np.argmax(mean_excess))
    scale = 1 / math.sqrt(float(covariance[best, best]))
    starts = [np.zeros(count)]
    starts[0][best] = scale
    generator = np.random.default_rng(SOLVER_SEED)
    direction = np.clip(mean_excess, 0, None)
    if float(direction.sum()) == 0:
        direction = np.ones(count)
    for draw in range(SHARPE_STARTS):
        raw = generator.dirichlet(np.ones(count)) * (1 + draw)
        raw = raw * np.maximum(direction, 1e-8)
        variance = float(raw @ covariance @ raw)
        starts.append(raw / math.sqrt(variance))

    best_weights = None
    best_sharpe = -math.inf
    for start in starts:
        solved = _solve(objective, gradient, start, bounds, constraints)
        candidate = _long_only_weights(solved, columns)
        value = sharpe_of(candidate.to_numpy())
        if value > best_sharpe:
            best_sharpe = value
            best_weights = candidate
    equal_sharpe = sharpe_of(np.full(count, 1 / count))
    if best_weights is None or best_sharpe + 1e-8 < equal_sharpe:
        raise RuntimeError("Maximum Sharpe is below the equal-weight Sharpe.")
    return best_weights


def _maximum_sharpe_invested(excess, mean_excess, covariance):
    """Fully invested search used when every name has a negative mean excess return."""
    count = covariance.shape[0]
    columns = excess.columns

    def objective(weights):
        expected = float(weights @ mean_excess)
        variance = float(weights @ covariance @ weights)
        if variance <= 0:
            return 0.0
        return -expected / math.sqrt(variance)

    start = np.full(count, 1 / count)
    bounds = [(0.0, 1.0)] * count
    constraints = {
        "type": "eq",
        "fun": lambda weights: np.sum(weights) - 1.0,
        "jac": lambda weights: np.ones(count),
    }
    solved = _solve(objective, None, start, bounds, constraints)
    return _long_only_weights(solved, columns)


def portfolio_return(asset_returns, weights):
    """Monthly return of a portfolio brought back to the same weights each month."""
    ordered = weights.reindex(asset_returns.columns)
    if ordered.isna().any():
        raise RuntimeError("A weight is missing for a shared name.")
    return asset_returns.mul(ordered, axis=1).sum(axis=1)


def measure_strategy(strategy, asset_returns, risk_free, weights):
    """Annualize the monthly series. Sharpe subtracts the monthly SARON return."""
    returns = portfolio_return(asset_returns, weights)
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
        "names": int(weights.shape[0]),
        "months": int(returns.shape[0]),
        "window_start": str(returns.index.min()),
        "window_end": str(returns.index.max()),
        "annualized_return": growth ** (MONTHS_PER_YEAR / len(returns)) - 1,
        "annualized_volatility": float(returns.std(ddof=1) * math.sqrt(MONTHS_PER_YEAR)),
        "sharpe_ratio": float(excess.mean() / volatility * math.sqrt(MONTHS_PER_YEAR)),
        "maximum_drawdown": float(drawdown.min()),
        "maximum_weight": float(weights.max()),
        "effective_holdings": float(1 / np.square(weights.to_numpy()).sum()),
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
    """Attach the security identity to each weight."""
    identity = membership.loc[
        membership["decision"].eq("included"),
        ["yahoo_symbol", "ticker", "name", "listing_currency"],
    ]
    frames = []
    for strategy in STRATEGY_IDS:
        weights = strategy_weights[strategy].rename("weight")
        weights.index.name = "yahoo_symbol"
        frame = weights.reset_index()
        frame.insert(0, "strategy", strategy)
        frames.append(
            frame.merge(identity, on="yahoo_symbol", how="left", validate="one_to_one")
        )
    table = pd.concat(frames, ignore_index=True)
    table["strategy"] = pd.Categorical(
        table["strategy"],
        categories=STRATEGY_IDS,
        ordered=True,
    )
    table = table.sort_values(
        ["strategy", "weight", "yahoo_symbol"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    table["strategy"] = table["strategy"].astype(str)
    return table.loc[:, list(WEIGHT_COLUMNS)]


def load_construction_inputs():
    """Read the treated CHF prices, the eligible universe, and SARON."""
    prices = pd.read_csv(MARKET_PRICES_CHF, usecols=["symbol", "timestamp", "close_chf"])
    universe = pd.read_csv(CHF_UNIVERSE, dtype=str, keep_default_na=False)
    saron = pd.read_csv(SARON_TREATED, dtype=str, keep_default_na=False)
    if universe["yahoo_symbol"].duplicated().any():
        raise RuntimeError("The CHF universe repeats a Yahoo symbol.")
    return prices, universe, saron


def construct_portfolios(prices, universe, saron):
    """Build the three strategies on the shared monthly panel."""
    levels = month_end_levels(prices)
    return_months, price_months, risk_free = estimation_window(saron)
    membership = shared_membership(universe, levels, price_months)
    asset_returns = shared_monthly_returns(
        levels,
        membership,
        return_months,
        price_months,
    )
    included = set(asset_returns.columns)
    membership.loc[
        membership["yahoo_symbol"].isin(included),
        ["decision", "reason"],
    ] = ["included", ""]
    weights = {
        "equal_weight": equal_weight(asset_returns),
        "minimum_volatility": minimum_volatility(asset_returns),
        "maximum_sharpe": maximum_sharpe(asset_returns, risk_free),
    }
    metrics = pd.DataFrame(
        [
            measure_strategy(strategy, asset_returns, risk_free, weights[strategy])
            for strategy in STRATEGY_IDS
        ],
        columns=list(METRIC_COLUMNS),
    )
    return {
        "membership": membership.loc[:, list(MEMBERSHIP_COLUMNS)],
        "weights": build_weight_table(weights, membership),
        "metrics": metrics,
        "comparison": build_comparison(metrics),
        "monthly_returns": build_monthly_returns(asset_returns, risk_free, weights),
        "asset_returns": asset_returns,
        "risk_free": risk_free,
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
