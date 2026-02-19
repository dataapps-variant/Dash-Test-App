"""
Chart builders for All Metrics Merged Dashboard

Chart types:
1. build_plan_line_chart()   - X=date, one line per plan (Tab 1 charts 2-8)
2. build_metric_line_chart() - X=date, one line per metric (Tab 2/3/4 multi-metric charts)
3. build_stacked_area_chart() - Stacked area % (Tab 4 rebill contribution)

Standards:
- Line width = 1.6
- hovermode = "x unified"
- Tooltip = "{name}  {value}" (x-axis label shown by unified mode)
"""

import plotly.graph_objects as go
from app.theme import get_theme_colors
# Distinct palette for merged dashboard (visible on dark background)
_MERGED_PALETTE = [
    # 15 fully distinct colors
    "#E74C3C",  # Red
    "#3B82F6",  # Blue
    "#22C55E",  # Green
    "#F59E0B",  # Amber
    "#A855F7",  # Purple
    "#EC4899",  # Pink
    "#06B6D4",  # Cyan
    "#F97316",  # Orange
    "#FACC15",  # Yellow
    "#14B8A6",  # Teal
    "#E879F9",  # Magenta
    "#FB923C",  # Peach
    "#38BDF8",  # Sky blue
    "#4ADE80",  # Lime
    "#F9A8D4",  # Rose
    # 12 supporting shades
    "#B91C1C",  # Dark red
    "#1D4ED8",  # Dark blue
    "#15803D",  # Dark green
    "#A16207",  # Dark amber
    "#7C3AED",  # Dark purple
    "#BE185D",  # Dark pink
    "#0E7490",  # Dark cyan
    "#C2410C",  # Dark orange
    "#CA8A04",  # Dark yellow
    "#0F766E",  # Dark teal
    "#86198F",  # Dark magenta
    "#9A3412",  # Dark peach
]

def build_merged_color_map(plan_names):
    """Assign visually distinct colors to plans, deterministic by plan name only"""
    color_map = {}
    for plan in plan_names:
        idx = hash(plan) % len(_MERGED_PALETTE)
        color_map[plan] = _MERGED_PALETTE[idx]
    return color_map

LINE_WIDTH = 1.6
LINE_OPACITY = 0.7

# Fixed colors for the 4-metric charts
METRIC_COLORS = {
    "Gross ARPU": "#3B82F6",   # Blue
    "Net ARPU": "#DC6B6B",     # Muted red
    "Recent CAC": "#22C55E",   # Green
    "Net LTV": "#F97316",      # Orange
}


def hex_to_rgba(hex_color, opacity=1.0):
    hex_color = hex_color.lstrip('#')
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {opacity})"


def _empty_figure(colors, message="No data available for selected filters"):
    fig = go.Figure()
    fig.update_layout(
        height=350,
        paper_bgcolor=colors["card_bg"],
        plot_bgcolor=colors["card_bg"],
        font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
        annotations=[{
            "text": message, "xref": "paper", "yref": "paper",
            "x": 0.5, "y": 0.5, "showarrow": False,
            "font": {"size": 14, "color": colors["text_secondary"]}
        }]
    )
    return fig


def _base_layout(colors, format_type="dollar", date_range=None):
    """Shared layout settings for all chart types"""
    if format_type == "dollar":
        yprefix, yformat = "$", ",.2f"
    elif format_type == "percent":
        yprefix, yformat = "", ".2%"
    else:
        yprefix, yformat = "", ",d"

    xrange = [date_range[0], date_range[1]] if date_range else None

    return dict(
        height=350,
        margin=dict(l=60, r=20, t=20, b=50),
        hovermode="x unified",
        paper_bgcolor=colors["card_bg"],
        plot_bgcolor=colors["card_bg"],
        font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
        xaxis=dict(
            gridcolor=colors["border"], linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickformat="%b %d, '%y", range=xrange, fixedrange=False,
        ),
        yaxis=dict(
            gridcolor=colors["border"], linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickprefix=yprefix, tickformat=yformat, fixedrange=False,
        ),
        legend=dict(font=dict(color=colors["text_primary"]), bgcolor="rgba(0,0,0,0)"),
        dragmode="zoom",
    )


def _hover_text(name, format_type):
    """Build hover template string for unified tooltip: 'name  value'"""
    if format_type == "dollar":
        return f'{name}  $%{{y:,.2f}}<extra></extra>'
    elif format_type == "percent":
        return f'{name}  %{{y:.2%}}<extra></extra>'
    else:
        return f'{name}  %{{y:,.0f}}<extra></extra>'


# =============================================================================
# 1. PLAN LINE CHART — one line per plan
# =============================================================================

def build_plan_line_chart(data_df, display_name, format_type="dollar", date_range=None, theme="dark"):
    """
    Build line chart with one line per plan.

    Args:
        data_df: DataFrame with columns [Plan_Name, Report_date, value]
        display_name: Chart title (not rendered, just for reference)
        format_type: 'dollar', 'percent', or 'number'
        date_range: (start, end) tuple
        theme: 'dark' or 'light'

    Returns:
        (fig, unique_plans)
    """
    colors = get_theme_colors(theme)

    if data_df is None or data_df.empty:
        return _empty_figure(colors), []

    # Filter out plans with no meaningful data (all zeros/NaN) in date range
    plan_sums = data_df.groupby("Plan_Name")["value"].sum()
    active_plans = plan_sums[plan_sums.abs() > 0].index.tolist()
    if not active_plans:
        return _empty_figure(colors), []
    data_df = data_df[data_df["Plan_Name"].isin(active_plans)]

    unique_plans = sorted(data_df["Plan_Name"].unique())
    color_map = build_merged_color_map(unique_plans)

    fig = go.Figure()
    for plan in unique_plans:
        pdf = data_df[data_df["Plan_Name"] == plan].sort_values("Report_date")
        fig.add_trace(go.Scatter(
            x=pdf["Report_date"], y=pdf["value"],
            mode="lines", name=plan,
            line=dict(color=hex_to_rgba(color_map.get(plan, "#6B7280"), LINE_OPACITY), width=LINE_WIDTH, shape="linear"),
            hovertemplate=_hover_text(plan, format_type),
            showlegend=False, connectgaps=False,
        ))

    fig.update_layout(**_base_layout(colors, format_type, date_range))
    return fig, unique_plans


# =============================================================================
# 2. METRIC LINE CHART — one line per metric (4-metric chart)
# =============================================================================

def build_metric_line_chart(metrics_dict, display_name, date_range=None, theme="dark"):
    """
    Build line chart with one line per metric. Net LTV on secondary Y axis.

    Args:
        metrics_dict: {metric_display_name: DataFrame(Report_date, value)}
        display_name: Chart title
        date_range: (start, end) tuple
        theme: 'dark' or 'light'

    Returns:
        (fig, metric_names)
    """
    colors = get_theme_colors(theme)

    if not metrics_dict:
        return _empty_figure(colors), []

    fig = go.Figure()
    metric_names = list(metrics_dict.keys())

    for metric_name, df in metrics_dict.items():
        if df is None or df.empty:
            continue
        df = df.sort_values("Report_date")
        color = METRIC_COLORS.get(metric_name, "#6B7280")

        # Net LTV goes on secondary Y axis
        use_secondary = (metric_name == "Net LTV")

        fig.add_trace(go.Scatter(
            x=df["Report_date"], y=df["value"],
            mode="lines", name=metric_name,
            line=dict(color=hex_to_rgba(color, LINE_OPACITY), width=LINE_WIDTH, shape="linear"),
            hovertemplate=f'{metric_name}  $%{{y:,.2f}}<extra></extra>',
            showlegend=False, connectgaps=False,
            yaxis="y2" if use_secondary else "y",
        ))

    layout = _base_layout(colors, "dollar", date_range)
    # Add secondary Y axis for Net LTV
    layout["yaxis2"] = dict(
        gridcolor="rgba(0,0,0,0)",  # hide secondary grid
        linecolor=colors["border"],
        tickfont=dict(color=METRIC_COLORS.get("Net LTV", "#F97316")),
        tickprefix="$", tickformat=",.0f",
        fixedrange=False,
        overlaying="y", side="right",
        title=dict(text="Net LTV", font=dict(color=METRIC_COLORS.get("Net LTV", "#F97316"), size=11)),
    )
    layout["margin"]["r"] = 70  # make room for right axis
    fig.update_layout(**layout)
    return fig, metric_names


# =============================================================================
# 3. STACKED AREA CHART — % contribution per plan
# =============================================================================

def build_stacked_area_chart(data_df, display_name, date_range=None, theme="dark"):
    """
    Build stacked area chart showing percentage contribution per plan.

    Args:
        data_df: DataFrame with columns [Plan_Name, Report_date, value (0-1 fraction)]
        display_name: Chart title
        date_range: (start, end) tuple
        theme: 'dark' or 'light'

    Returns:
        (fig, unique_plans)
    """
    colors = get_theme_colors(theme)

    if data_df is None or data_df.empty:
        return _empty_figure(colors), []

    # Filter out plans with no meaningful data (all zeros/NaN)
    plan_sums = data_df.groupby("Plan_Name")["value"].sum()
    active_plans = plan_sums[plan_sums.abs() > 0].index.tolist()
    if not active_plans:
        return _empty_figure(colors), []
    data_df = data_df[data_df["Plan_Name"].isin(active_plans)]

    unique_plans = sorted(data_df["Plan_Name"].unique())
    color_map = build_merged_color_map(unique_plans)

    fig = go.Figure()
    for plan in unique_plans:
        pdf = data_df[data_df["Plan_Name"] == plan].sort_values("Report_date")
        color = color_map.get(plan, "#6B7280")

        fig.add_trace(go.Scatter(
            x=pdf["Report_date"], y=pdf["value"],
            mode="lines", name=plan,
            line=dict(color=color, width=0.5),
            fillcolor=hex_to_rgba(color, 0.6),
            stackgroup="one",  # enables stacking
            groupnorm="percent",  # normalise to 100%
            hovertemplate=f'{plan}  %{{y:.1f}}%<extra></extra>',
            showlegend=False, connectgaps=False,
        ))

    layout = _base_layout(colors, "number", date_range)
    # Override y-axis for percentage (groupnorm makes y 0-100)
    layout["yaxis"]["tickformat"] = ".0f"
    layout["yaxis"]["ticksuffix"] = "%"
    layout["yaxis"]["tickprefix"] = ""
    layout["yaxis"]["range"] = [0, 100]
    fig.update_layout(**layout)
    return fig, unique_plans
