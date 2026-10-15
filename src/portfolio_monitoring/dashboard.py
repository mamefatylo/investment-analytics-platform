"""
Charts for the phase 6 monitoring snapshot.

The figures read data/05_monitoring. They do not recompute weights.
write_dashboard publishes the same figures as reports/monitoring/dashboard.html.
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.config import MONITORING_DATA, MONITORING_REPORTS
from src.portfolio_construction.build_portfolios import STRATEGY_IDS


STRATEGY_LABELS = {
    "equal_weight": "Équipondéré",
    "minimum_volatility": "Volatilité minimale",
    "maximum_sharpe": "Sharpe maximal",
}
STRATEGY_COLORS = {
    "Équipondéré": "#1f4e79",
    "Volatilité minimale": "#2e7d4f",
    "Sharpe maximal": "#c47b17",
}
EXPOSURE_ORDER = ("Actions", "Obligations", "ETF")
DASHBOARD_FILE = MONITORING_REPORTS / "dashboard.html"
SECTION_TITLES = (
    "Chemin du portefeuille",
    "Contribution cumulée des secteurs",
    "Allocation par secteur",
    "Allocation par zone",
    "Exposition",
    "Concentration",
    "Dix premières lignes",
)


def load_dashboard_frames():
    """Read the published monitoring files in strategy order."""
    report = pd.read_csv(MONITORING_DATA / "monthly_report.csv")
    sectors = pd.read_csv(MONITORING_DATA / "allocation_by_sector.csv")
    zones = pd.read_csv(MONITORING_DATA / "allocation_by_zone.csv")
    exposure = pd.read_csv(MONITORING_DATA / "exposure.csv")
    top10 = pd.read_csv(MONITORING_DATA / "top10_positions.csv")
    path = pd.read_csv(MONITORING_DATA / "portfolio_path.csv")
    contributions = pd.read_csv(MONITORING_DATA / "sector_contribution.csv")
    frames = {
        "report": report,
        "sectors": sectors,
        "zones": zones,
        "exposure": exposure,
        "top10": top10,
        "path": path,
        "contributions": contributions,
    }
    for name, frame in frames.items():
        missing = set(STRATEGY_IDS) - set(frame["strategy"])
        if missing:
            raise RuntimeError(f"{name} is missing strategies: {', '.join(sorted(missing))}")
        frame["strategy"] = pd.Categorical(
            frame["strategy"].map(STRATEGY_LABELS),
            categories=list(STRATEGY_LABELS.values()),
            ordered=True,
        )
        sort_columns = ["strategy"]
        if "month" in frame.columns:
            sort_columns.append("month")
        frames[name] = frame.sort_values(sort_columns, kind="mergesort")
    return frames


def apply_layout(figure, title, height):
    figure.update_layout(
        title=title,
        template="plotly_white",
        height=height,
        legend_title_text="",
        margin={"t": 70, "b": 70, "l": 60, "r": 30},
        font={"family": "Segoe UI, sans-serif", "size": 13},
    )
    return figure


def grouped_weights(frame, category, title, category_order, height=480):
    """Grouped bars. One color is one strategy. Weights stay on a 0–100% scale."""
    figure = px.bar(
        frame,
        x=category,
        y="weight",
        color="strategy",
        barmode="group",
        category_orders={
            category: category_order,
            "strategy": list(STRATEGY_LABELS.values()),
        },
        color_discrete_map=STRATEGY_COLORS,
        hover_data={"weight": ":.1%"},
    )
    figure.update_yaxes(tickformat=".0%", title="Poids", rangemode="tozero")
    figure.update_xaxes(title="")
    figure.update_traces(
        hovertemplate="%{x}<br>%{fullData.name}<br>%{y:.1%}<extra></extra>"
    )
    return apply_layout(figure, title, height)


def category_order(frame, category):
    reference = frame.loc[frame["strategy"].eq(STRATEGY_LABELS["equal_weight"])]
    return reference.sort_values("weight", ascending=False)[category].tolist()


def sector_figure(sectors):
    return grouped_weights(
        sectors,
        "sector",
        SECTION_TITLES[2],
        category_order(sectors, "sector"),
    )


def zone_figure(zones):
    return grouped_weights(
        zones,
        "zone",
        SECTION_TITLES[3],
        category_order(zones, "zone"),
        height=440,
    )


def exposure_figure(exposure):
    present = list(exposure["exposure"].unique())
    extras = [label for label in present if label not in EXPOSURE_ORDER]
    return grouped_weights(
        exposure,
        "exposure",
        SECTION_TITLES[4],
        list(EXPOSURE_ORDER) + extras,
        height=440,
    )


def concentration_figure(report):
    labels = [str(label) for label in report["strategy"]]
    colors = [STRATEGY_COLORS[label] for label in labels]
    figure = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Nombre effectif de titres", "Multiple de concentration"),
    )
    figure.add_trace(
        go.Bar(
            x=labels,
            y=report["effective_holdings"],
            marker_color=colors,
            text=[f"{value:.1f}" for value in report["effective_holdings"]],
            textposition="outside",
            hovertemplate="%{x}<br>%{y:.2f} titres effectifs<extra></extra>",
            showlegend=False,
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Bar(
            x=labels,
            y=report["concentration_multiple"],
            marker_color=colors,
            text=[f"{value:.2f}" for value in report["concentration_multiple"]],
            textposition="outside",
            hovertemplate="%{x}<br>multiple %{y:.2f}<extra></extra>",
            showlegend=False,
        ),
        row=1,
        col=2,
    )
    figure.update_yaxes(rangemode="tozero")
    figure.update_xaxes(title="")
    return apply_layout(figure, SECTION_TITLES[5], 440)


def top10_figure(top10):
    labels = list(STRATEGY_LABELS.values())
    figure = make_subplots(
        rows=len(labels),
        cols=1,
        subplot_titles=labels,
        vertical_spacing=0.08,
    )
    shared_max = float(top10["weight"].max()) * 1.12
    for index, label in enumerate(labels, start=1):
        rows = top10.loc[top10["strategy"].eq(label)].sort_values("weight", ascending=True)
        figure.add_trace(
            go.Bar(
                x=rows["weight"],
                y=rows["name"],
                orientation="h",
                marker_color=STRATEGY_COLORS[label],
                text=rows["name"],
                textposition="outside",
                cliponaxis=False,
                customdata=list(zip(rows["sector"], rows["zone"])),
                hovertemplate="%{y}<br>%{x:.1%}<br>%{customdata[0]} · %{customdata[1]}<extra></extra>",
                showlegend=False,
            ),
            row=index,
            col=1,
        )
        figure.update_xaxes(
            tickformat=".0%",
            range=[0, shared_max * 2.6],
            row=index,
            col=1,
        )
    figure.update_yaxes(showticklabels=False)
    return apply_layout(figure, SECTION_TITLES[6], 980)


def as_dates(frame):
    """Dates in calendar order. A line follows this order, so it must not be shuffled."""
    dated = frame.copy()
    dated["month"] = pd.to_datetime(dated["month"])
    columns = ["strategy", "month"]
    if "sector" in dated.columns:
        columns = ["strategy", "sector", "month"]
    dated = dated.sort_values(columns, kind="mergesort")
    if not dated.groupby(["strategy"] + (["sector"] if "sector" in dated.columns else []), observed=True)["month"].apply(lambda values: values.is_monotonic_increasing).all():
        raise RuntimeError("A time series goes backwards.")
    return dated


def path_figure(path):
    """Value of one franc, then the loss from the running peak. Weights stay fixed."""
    dated = as_dates(path)
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        subplot_titles=("Valeur d'un franc", "Perte depuis le plus haut"),
    )
    for label in STRATEGY_LABELS.values():
        rows = dated.loc[dated["strategy"].eq(label)]
        figure.add_trace(
            go.Scatter(
                x=rows["month"],
                y=rows["cumulative_wealth"],
                mode="lines",
                name=label,
                line={"color": STRATEGY_COLORS[label]},
                legendgroup=label,
                hovertemplate="%{x|%Y-%m}<br>%{y:.2f} franc<extra>" + label + "</extra>",
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=rows["month"],
                y=rows["drawdown"],
                mode="lines",
                name=label,
                line={"color": STRATEGY_COLORS[label]},
                legendgroup=label,
                showlegend=False,
                hovertemplate="%{x|%Y-%m}<br>%{y:.1%}<extra>" + label + "</extra>",
            ),
            row=2,
            col=1,
        )
    figure.update_yaxes(title="Francs", rangemode="tozero", row=1, col=1)
    figure.update_yaxes(tickformat=".0%", title="", row=2, col=1)
    figure.update_xaxes(type="date", title="")
    return apply_layout(figure, SECTION_TITLES[0], 680)


def contribution_figure(contributions):
    """Additive sector contribution under the same weights, cumulated through the window."""
    dated = as_dates(contributions)
    figure = px.line(
        dated,
        x="month",
        y="cumulative_contribution",
        color="sector",
        facet_row="strategy",
        category_orders={"strategy": list(STRATEGY_LABELS.values())},
        hover_data={"cumulative_contribution": ":.1%"},
    )
    figure.update_yaxes(tickformat=".0%", title="")
    figure.update_xaxes(type="date", title="")
    figure.for_each_annotation(
        lambda annotation: annotation.update(text=annotation.text.split("=")[-1])
    )
    return apply_layout(figure, SECTION_TITLES[1], 860)


def build_figures():
    """The monitoring charts: the path through the window, then the latest snapshot."""
    frames = load_dashboard_frames()
    return [
        path_figure(frames["path"]),
        contribution_figure(frames["contributions"]),
        sector_figure(frames["sectors"]),
        zone_figure(frames["zones"]),
        exposure_figure(frames["exposure"]),
        concentration_figure(frames["report"]),
        top10_figure(frames["top10"]),
    ]


def render_dashboard_html():
    frames = load_dashboard_frames()
    report = frames["report"]
    month = str(report["report_month"].iloc[0])
    window_start = str(report["window_start"].iloc[0])
    window_end = str(report["window_end"].iloc[0])
    if report["report_month"].nunique() != 1:
        raise RuntimeError("The monitoring report has more than one month.")
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
<title>Suivi des portefeuilles — {month}</title>
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
<h1>Suivi des portefeuilles</h1>
<p>Mois du rapport {month}. Fenêtre {window_start} – {window_end}.
Les poids ont été estimés sur toute la fenêtre, puis remis chaque mois.
Le chemin montre ce qu'ils ont produit : la valeur d'un franc, la perte
depuis le plus haut, et la contribution cumulée de chaque secteur.
Ce n'est pas le parcours d'un mandat figé au premier mois.
La répartition ne dérive pas ; la photo en dessous la décrit.
La page est remplacée à chaque reconstruction.</p>
{body}
</body>
</html>
"""


def write_dashboard():
    """Write the browser page next to the monthly report notebook."""
    DASHBOARD_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = DASHBOARD_FILE.with_suffix(".html.tmp")
    temporary.write_text(render_dashboard_html(), encoding="utf-8")
    temporary.replace(DASHBOARD_FILE)
    return DASHBOARD_FILE
