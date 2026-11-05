"""
Phase 7 performance.

The monthly returns are already published. This module reads them.
It does not open prices, and it does not estimate weights again.

Four measures are repeated from phase 5: annualized return, annualized
volatility, Sharpe and maximum drawdown. They must match
portfolio_metrics.csv. The wealth path must match portfolio_path.csv.
A difference stops the run. Two Sharpes are not published.

Sortino uses only the months that fall short of SARON. The divisor
stays T - 1, the same divisor as the Sharpe. If no month is below
SARON, Sortino is left empty.

Tracking error and the information ratio use the fund's published
monthly total NAV return, converted to CHF with the SNB rate of the
fund currency. These ratios describe the sample on which the weights
were estimated. They are not the record of a mandate frozen in the
first month.

Output
------
data/06_performance/benchmark_monthly_return.csv
data/06_performance/performance_metrics.csv
reports/performance/dashboard.html
"""

import math

import numpy as np
import pandas as pd

from src.config import MONTHS_PER_YEAR, PERFORMANCE_DATA
from src.performance.benchmark_return import load_benchmark_returns
from src.portfolio_construction.build_portfolios import (
    METRICS_FILE,
    MONTHLY_RETURNS_FILE,
    STRATEGY_IDS,
    write_csv_atomically,
)
from src.portfolio_monitoring.build_monitoring import PATH_FILE
from src.utils.logger import logger


METRICS_OUTPUT = PERFORMANCE_DATA / "performance_metrics.csv"
BENCHMARK_RETURNS_FILE = PERFORMANCE_DATA / "benchmark_monthly_return.csv"
BENCHMARK_COLUMNS = (
    "month",
    "fund_name",
    "fund_currency",
    "nav_total_return",
    "chf_per_unit",
    "prior_chf_per_unit",
    "benchmark_return_chf",
)
AGREEMENT = 1e-8
OUTPUT_COLUMNS = (
    "strategy",
    "months",
    "window_start",
    "window_end",
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "sortino_ratio",
    "maximum_drawdown",
    "tracking_error",
    "information_ratio",
    "months_below_risk_free",
    "reference",
)
RETURN_COLUMNS = (
    "month",
    "strategy",
    "portfolio_return",
    "risk_free_return",
    "excess_return",
)
PUBLISHED_MEASURES = (
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "maximum_drawdown",
)


def _require_columns(frame, columns, label):
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise RuntimeError(f"{label} is missing columns: {', '.join(missing)}")


def _same(left, right, label):
    if not math.isfinite(left) or not math.isfinite(right):
        raise RuntimeError(f"{label} is not a finite number.")
    if abs(left - right) > AGREEMENT:
        raise RuntimeError(
            f"{label} does not match the published figure "
            f"({left} against {right})."
        )


def load_monthly_returns():
    """Read the phase 5 monthly file and keep one shared window."""
    frame = pd.read_csv(MONTHLY_RETURNS_FILE)
    _require_columns(frame, RETURN_COLUMNS, "Monthly returns")
    frame = frame.loc[:, list(RETURN_COLUMNS)].copy()
    frame["month"] = frame["month"].astype(str)
    frame["strategy"] = frame["strategy"].astype(str)
    for column in ("portfolio_return", "risk_free_return", "excess_return"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if frame[list(RETURN_COLUMNS[2:])].isna().any().any():
        raise RuntimeError("A monthly return is empty.")
    if set(frame["strategy"]) != set(STRATEGY_IDS):
        raise RuntimeError("Monthly returns do not cover the three strategies.")
    if frame.duplicated(["strategy", "month"]).any():
        raise RuntimeError("A strategy has the same month twice.")
    gap = frame["portfolio_return"] - frame["risk_free_return"] - frame["excess_return"]
    if float(gap.abs().max()) > AGREEMENT:
        raise RuntimeError("Excess return is not the portfolio return minus SARON.")
    months = None
    ordered = []
    for strategy in STRATEGY_IDS:
        block = frame.loc[frame["strategy"].eq(strategy)].sort_values(
            "month",
            kind="mergesort",
        )
        if len(block) < 2:
            raise RuntimeError(f"{strategy} does not have two months.")
        block_months = block["month"].tolist()
        if months is None:
            months = block_months
        elif block_months != months:
            raise RuntimeError("The three strategies do not share one month list.")
        ordered.append(block)
    combined = pd.concat(ordered, ignore_index=True)
    risk_free = combined.pivot(index="month", columns="strategy", values="risk_free_return")
    risk_free = risk_free.reindex(months)
    spread = risk_free.max(axis=1) - risk_free.min(axis=1)
    if float(spread.max()) > AGREEMENT:
        raise RuntimeError("SARON is not the same for every strategy in a month.")
    return combined


def load_published_metrics():
    """Read the four phase 5 measures that this phase must recover."""
    frame = pd.read_csv(METRICS_FILE)
    needed = ("strategy", "months", "window_start", "window_end", *PUBLISHED_MEASURES)
    _require_columns(frame, needed, "Portfolio metrics")
    frame["strategy"] = frame["strategy"].astype(str)
    if set(frame["strategy"]) != set(STRATEGY_IDS):
        raise RuntimeError("Portfolio metrics do not cover the three strategies.")
    return frame.set_index("strategy")


def load_path():
    """Read the phase 6 wealth path used to check the drawdown."""
    frame = pd.read_csv(PATH_FILE)
    _require_columns(
        frame,
        ("month", "strategy", "portfolio_return", "cumulative_wealth", "drawdown"),
        "Portfolio path",
    )
    frame["month"] = frame["month"].astype(str)
    frame["strategy"] = frame["strategy"].astype(str)
    for column in ("portfolio_return", "cumulative_wealth", "drawdown"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if frame[["portfolio_return", "cumulative_wealth", "drawdown"]].isna().any().any():
        raise RuntimeError("The wealth path has an empty value.")
    return frame


def _annualized_return(returns):
    growth = float(np.prod(1 + returns.to_numpy()))
    if growth <= 0:
        raise RuntimeError("Portfolio wealth is not positive.")
    return growth ** (MONTHS_PER_YEAR / len(returns)) - 1


def _drawdown(returns):
    wealth = (1 + returns).cumprod()
    drawdown = wealth / wealth.cummax() - 1
    return wealth, drawdown


def _sortino(excess):
    """Downside deviation over every month. Empty when nothing is below SARON."""
    shortfall = np.minimum(excess.to_numpy(dtype=float), 0.0)
    below = int(np.count_nonzero(shortfall < 0))
    deviation = math.sqrt(float(np.square(shortfall).sum()) / (len(excess) - 1))
    if deviation == 0.0:
        return math.nan, below
    ratio = float(excess.mean() / deviation * math.sqrt(MONTHS_PER_YEAR))
    return ratio, below


def _active(returns, reference):
    """Tracking error and information ratio against the fund NAV return in CHF."""
    gap = returns.to_numpy(dtype=float) - reference.to_numpy(dtype=float)
    deviation = float(pd.Series(gap).std(ddof=1))
    tracking = deviation * math.sqrt(MONTHS_PER_YEAR)
    if deviation == 0.0:
        return tracking, math.nan
    information = float(gap.mean() / deviation * math.sqrt(MONTHS_PER_YEAR))
    return tracking, information


def _check_path(strategy, returns, wealth, drawdown, path):
    block = path.loc[path["strategy"].eq(strategy)].sort_values("month", kind="mergesort")
    if block["month"].tolist() != returns.index.tolist():
        raise RuntimeError(f"{strategy} path months do not match the monthly returns.")
    return_gap = block["portfolio_return"].to_numpy() - returns.to_numpy()
    if float(np.max(np.abs(return_gap))) > AGREEMENT:
        raise RuntimeError(f"{strategy} path return does not match the monthly file.")
    wealth_gap = block["cumulative_wealth"].to_numpy() - wealth.to_numpy()
    if float(np.max(np.abs(wealth_gap))) > AGREEMENT:
        raise RuntimeError(f"{strategy} wealth does not match the path.")
    drawdown_gap = block["drawdown"].to_numpy() - drawdown.to_numpy()
    if float(np.max(np.abs(drawdown_gap))) > AGREEMENT:
        raise RuntimeError(f"{strategy} drawdown does not match the path.")


def measure_strategies(returns, published, path, benchmark):
    """Build one performance row per strategy and stop on a disagreement."""
    reference = benchmark.set_index("month").sort_index(kind="mergesort")["benchmark_return_chf"]
    fund_name = str(benchmark["fund_name"].iloc[0])
    rows = []
    for strategy in STRATEGY_IDS:
        block = returns.loc[returns["strategy"].eq(strategy)].set_index("month")
        block = block.sort_index(kind="mergesort")
        series = block["portfolio_return"]
        excess = series - block["risk_free_return"]
        excess_volatility = float(excess.std(ddof=1))
        if excess_volatility == 0.0:
            raise RuntimeError(f"{strategy} excess return does not vary.")
        wealth, drawdown = _drawdown(series)
        measured = {
            "annualized_return": _annualized_return(series),
            "annualized_volatility": float(series.std(ddof=1) * math.sqrt(MONTHS_PER_YEAR)),
            "sharpe_ratio": float(
                excess.mean() / excess_volatility * math.sqrt(MONTHS_PER_YEAR)
            ),
            "maximum_drawdown": float(drawdown.min()),
        }
        official = published.loc[strategy]
        if int(official["months"]) != len(series):
            raise RuntimeError(f"{strategy} month count does not match phase 5.")
        if str(official["window_start"]) != str(series.index.min()):
            raise RuntimeError(f"{strategy} window start does not match phase 5.")
        if str(official["window_end"]) != str(series.index.max()):
            raise RuntimeError(f"{strategy} window end does not match phase 5.")
        for name in PUBLISHED_MEASURES:
            _same(measured[name], float(official[name]), f"{strategy} {name}")
            measured[name] = float(official[name])
        _check_path(strategy, series, wealth, drawdown, path)
        sortino, below = _sortino(excess)
        if not series.index.equals(reference.index):
            raise RuntimeError(f"{strategy} months do not match the fund return.")
        tracking, information = _active(series, reference)
        rows.append(
            {
                "strategy": strategy,
                "months": int(len(series)),
                "window_start": str(series.index.min()),
                "window_end": str(series.index.max()),
                **measured,
                "sortino_ratio": sortino,
                "tracking_error": tracking,
                "information_ratio": information,
                "months_below_risk_free": below,
                "reference": fund_name,
            }
        )
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def build_performance():
    """Write the performance file. Raise if phase 5 or phase 6 disagrees."""
    returns = load_monthly_returns()
    published = load_published_metrics()
    path = load_path()
    months = (
        returns.loc[returns["strategy"].eq(STRATEGY_IDS[0]), "month"]
        .drop_duplicates()
        .tolist()
    )
    benchmark = load_benchmark_returns(months)
    write_csv_atomically(benchmark, BENCHMARK_RETURNS_FILE, BENCHMARK_COLUMNS)
    logger.info("Benchmark returns written to %s", BENCHMARK_RETURNS_FILE)
    metrics = measure_strategies(returns, published, path, benchmark)
    write_csv_atomically(metrics, METRICS_OUTPUT, OUTPUT_COLUMNS)
    logger.info("Performance metrics written to %s", METRICS_OUTPUT)
    from src.performance.dashboard import write_dashboard

    dashboard = write_dashboard()
    logger.info("Performance dashboard written to %s", dashboard)
    return {
        "metrics": METRICS_OUTPUT,
        "benchmark": BENCHMARK_RETURNS_FILE,
        "dashboard": dashboard,
    }


def main():
    written = build_performance()
    print(written["metrics"])


if __name__ == "__main__":
    main()
