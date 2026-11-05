"""
Charts for the phase 7 performance reading.

The figures read the published monthly returns, the wealth path and
the performance file. They do not estimate weights again.

write_dashboard publishes the same figures as
reports/performance/dashboard.html. The page is interactive:
hover, zoom, and a line that moves forward through the months.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.config import PERFORMANCE_REPORTS
from src.performance.build_performance import BENCHMARK_RETURNS_FILE, METRICS_OUTPUT
from src.portfolio_construction.build_portfolios import (
    MONTHLY_RETURNS_FILE,
    STRATEGY_IDS,
)
from src.portfolio_monitoring.build_monitoring import PATH_FILE
from src.portfolio_monitoring.dashboard import STRATEGY_COLORS, STRATEGY_LABELS


DASHBOARD_FILE = PERFORMANCE_REPORTS / "dashboard.html"
ROLLING_MONTHS = 12
SECTION_TITLES = (
    "Performance relative au SARON",
    "Rendement glissant sur 12 mois",
    "Performance relative au fonds",
    "Indicateurs annualisés",
)
PERCENT_MEASURES = (
    ("annualized_return", "Rendement annualisé"),
    ("annualized_volatility", "Volatilité annualisée"),
    ("maximum_drawdown", "Drawdown maximal"),
    ("tracking_error", "Écart de suivi annualisé"),
)
RATIO_MEASURES = (
    ("sharpe_ratio", "Ratio de Sharpe"),
    ("sortino_ratio", "Ratio de Sortino"),
    ("information_ratio", "Ratio d'information"),
)


def load_frames():
    """Read the published performance inputs in strategy and month order."""
    metrics = pd.read_csv(METRICS_OUTPUT)
    monthly = pd.read_csv(MONTHLY_RETURNS_FILE)
    path = pd.read_csv(PATH_FILE)
    benchmark = pd.read_csv(BENCHMARK_RETURNS_FILE)
    frames = {"metrics": metrics, "monthly": monthly, "path": path, "benchmark": benchmark}
    for name, frame in frames.items():
        if name != "benchmark" and set(frame["strategy"].astype(str)) != set(STRATEGY_IDS):
            raise RuntimeError(f"{name} does not cover the three strategies.")
        if name == "benchmark":
            frame["month"] = frame["month"].astype(str)
            frames[name] = frame.sort_values("month", kind="mergesort")
            continue
        frame["strategy"] = pd.Categorical(
            frame["strategy"].astype(str).map(STRATEGY_LABELS),
            categories=list(STRATEGY_LABELS.values()),
            ordered=True,
        )
        sort_columns = ["strategy"]
        if "month" in frame.columns:
            frame["month"] = frame["month"].astype(str)
            sort_columns.append("month")
        frames[name] = frame.sort_values(sort_columns, kind="mergesort")
    return frames


def as_dates(frame):
    """Dates in calendar order. A line follows this order."""
    dated = frame.copy()
    dated["month"] = pd.to_datetime(dated["month"])
    dated = dated.sort_values(["strategy", "month"], kind="mergesort")
    forward = dated.groupby("strategy", observed=True)["month"].apply(
        lambda values: bool(values.is_monotonic_increasing)
    )
    if not bool(forward.all()):
        raise RuntimeError("A time series goes backwards.")
    return dated


def apply_layout(figure, title, height):
    figure.update_layout(
        title=title,
        template="plotly_white",
        height=height,
        legend_title_text="",
        margin={"t": 70, "b": 60, "l": 80, "r": 30},
        font={"family": "Segoe UI, sans-serif", "size": 13},
        hovermode="x unified",
    )
    return figure


def _strategy_lines(dated, column, hover):
    figure = go.Figure()
    for label in STRATEGY_LABELS.values():
        rows = dated.loc[dated["strategy"].eq(label)]
        figure.add_trace(
            go.Scatter(
                x=rows["month"],
                y=rows[column],
                mode="lines",
                name=label,
                line={"color": STRATEGY_COLORS[label], "width": 2},
                hovertemplate=hover,
            )
        )
    figure.update_xaxes(type="date", title="")
    return figure


def against_saron_figure(path, monthly):
    """Portfolio capital divided by the capital of one franc left in SARON."""
    dated = as_dates(path)
    reference = monthly.loc[monthly["strategy"].eq(STRATEGY_LABELS["equal_weight"])]
    reference = reference.sort_values("month", kind="mergesort")
    cash = pd.Series(
        (1 + reference["risk_free_return"].astype(float)).cumprod().to_numpy(),
        index=pd.to_datetime(reference["month"]),
    )
    rows = []
    for label in STRATEGY_LABELS.values():
        block = dated.loc[dated["strategy"].eq(label)].copy()
        aligned = cash.reindex(block["month"])
        if aligned.isna().any():
            raise RuntimeError("A portfolio month has no SARON capital.")
        block["saron_multiple"] = block["cumulative_wealth"].to_numpy() / aligned.to_numpy()
        rows.append(block)
    compared = pd.concat(rows, ignore_index=True)
    figure = _strategy_lines(
        compared,
        "saron_multiple",
        "%{x|%Y-%m}<br>multiple %{y:.2f}<extra>%{fullData.name}</extra>",
    )
    figure.update_yaxes(title="Multiple", rangemode="tozero")
    return apply_layout(figure, SECTION_TITLES[0], 480)


def rolling_figure(monthly):
    """Product of the twelve monthly returns ending at each date."""
    dated = as_dates(monthly)
    rows = []
    for label in STRATEGY_LABELS.values():
        block = dated.loc[dated["strategy"].eq(label)].copy()
        growth = np.exp(np.log1p(block["portfolio_return"].astype(float)).rolling(ROLLING_MONTHS).sum())
        block["rolling_return"] = growth - 1
        rows.append(block.dropna(subset=["rolling_return"]))
    rolled = pd.concat(rows, ignore_index=True)
    figure = _strategy_lines(
        rolled,
        "rolling_return",
        "%{x|%Y-%m}<br>%{y:.1%}<extra>%{fullData.name}</extra>",
    )
    figure.update_yaxes(title="Rendement", tickformat=".0%")
    return apply_layout(figure, SECTION_TITLES[1], 460)


def relative_figure(path, benchmark):
    """Portfolio capital divided by the fund capital in CHF."""
    dated = as_dates(path)
    fund = benchmark.sort_values("month", kind="mergesort")
    fund_wealth = (1 + fund["benchmark_return_chf"].astype(float)).cumprod()
    fund_wealth.index = pd.to_datetime(fund["month"])
    rows = []
    for label in STRATEGY_LABELS.values():
        block = dated.loc[dated["strategy"].eq(label)].copy()
        aligned = fund_wealth.reindex(block["month"])
        if aligned.isna().any():
            raise RuntimeError("A portfolio month has no fund capital.")
        block["relative_wealth"] = block["cumulative_wealth"].to_numpy() / aligned.to_numpy()
        rows.append(block)
    compared = pd.concat(rows, ignore_index=True)
    figure = _strategy_lines(
        compared,
        "relative_wealth",
        "%{x|%Y-%m}<br>multiple %{y:.2f}<extra>%{fullData.name}</extra>",
    )
    figure.update_yaxes(title="Multiple")
    return apply_layout(figure, SECTION_TITLES[2], 460)


def _bars(figure, metrics, measures, row, percent):
    for label in STRATEGY_LABELS.values():
        row_values = metrics.loc[metrics["strategy"].eq(label)].iloc[0]
        y_values = []
        text = []
        for column, _title in measures:
            value = row_values[column]
            if pd.isna(value):
                y_values.append(None)
                text.append("")
                continue
            number = float(value)
            y_values.append(number)
            text.append(f"{number * 100:.1f} %".replace(".", ",") if percent else f"{number:.2f}".replace(".", ","))
        figure.add_trace(
            go.Bar(
                x=[title for _column, title in measures],
                y=y_values,
                name=label,
                marker_color=STRATEGY_COLORS[label],
                text=text,
                textposition="outside",
                legendgroup=label,
                showlegend=row == 1,
                hovertemplate="%{x}<br>%{text}<extra>" + label + "</extra>",
            ),
            row=row,
            col=1,
        )


def measures_figure(metrics):
    """The published window measures. Two scales: percents, then ratios."""
    figure = make_subplots(
        rows=2,
        cols=1,
        vertical_spacing=0.16,
        subplot_titles=(
            "Rendement annualisé, volatilité annualisée, drawdown maximal et écart de suivi annualisé",
            "Ratio de Sharpe, ratio de Sortino et ratio d'information",
        ),
    )
    _bars(figure, metrics, PERCENT_MEASURES, 1, True)
    _bars(figure, metrics, RATIO_MEASURES, 2, False)
    figure.update_layout(barmode="group")
    figure.update_xaxes(tickangle=-25)
    figure.update_yaxes(tickformat=".0%", row=1, col=1)
    figure = apply_layout(figure, SECTION_TITLES[3], 820)
    figure.update_layout(margin={"t": 90, "b": 110, "l": 70, "r": 30})
    return figure


def build_figures():
    """Performance charts only. The capital path and the drawdown stay in phase 6."""
    frames = load_frames()
    return [
        against_saron_figure(frames["path"], frames["monthly"]),
        rolling_figure(frames["monthly"]),
        relative_figure(frames["path"], frames["benchmark"]),
        measures_figure(frames["metrics"]),
    ]


def render_dashboard_html():
    frames = load_frames()
    metrics = frames["metrics"]
    if metrics["window_start"].nunique() != 1 or metrics["window_end"].nunique() != 1:
        raise RuntimeError("The performance file has more than one window.")
    start = str(metrics["window_start"].iloc[0])
    end = str(metrics["window_end"].iloc[0])
    fund_name = str(frames["benchmark"]["fund_name"].iloc[0])
    fund_currency = str(frames["benchmark"]["fund_currency"].iloc[0])
    sections = []
    for index, figure in enumerate(build_figures()):
        sections.append(
            figure.to_html(
                full_html=False,
                include_plotlyjs=index == 0,
                config={"displayModeBar": False},
            )
        )
    body = "\n".join(sections)
    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Performance des portefeuilles — {end}</title>
<style>
html, body {{
  background: #ffffff;
  color: #1a1a1a;
}}
body {{
  font-family: "Segoe UI", sans-serif;
  margin: 2rem auto;
  max-width: 1100px;
  padding: 0 1rem 3rem;
}}
h1 {{ font-size: 1.7rem; margin-bottom: 0.3rem; }}
p {{ color: #333; line-height: 1.45; }}
</style>
</head>
<body>
<h1>Performance des portefeuilles</h1>
<p>Période {start} – {end}. Le détail s'affiche au survol.
Le poids de chaque trimestre a été fixé avec les soixante mois déjà observés,
sous une limite de 5 % par émetteur, 20 % par secteur, 50 % par zone
et 40 % par devise de cotation.
Le premier graphique rapporte la performance du portefeuille à celle d'un placement
resté au SARON. Le deuxième est le rendement glissant, produit des douze rendements
mensuels précédant chaque date. Le troisième rapporte la performance du portefeuille
à celle de {fund_name}. Ce rendement est le rendement mensuel total de la valeur
liquidative publié par iShares, converti en francs au cours BNS de fin de mois
({fund_currency}). Les mois sans donnée publiée ne sont pas estimés.
Les indicateurs sont ceux de la période. L'écart de suivi et le ratio d'information
comparent chaque portefeuille à ce fonds. La performance cumulée et le drawdown
figurent dans le suivi des portefeuilles.
La page est régénérée à chaque reconstruction.</p>
{body}
</body>
</html>
"""


def write_dashboard():
    """Write the interactive page next to the performance report."""
    DASHBOARD_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = DASHBOARD_FILE.with_suffix(".html.tmp")
    temporary.write_text(render_dashboard_html(), encoding="utf-8")
    temporary.replace(DASHBOARD_FILE)
    return DASHBOARD_FILE
