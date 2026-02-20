"""
Chart builders for Daedalus Dashboard

Chart types:
1. build_kpi_card          - KPI number card (Tab 1)
2. build_pivot_grid        - AG Grid pivot table
3. build_actual_target_lines - Two lines (solid actual + dotted target)
4. build_multi_app_lines   - Two lines per app (solid actual + dotted target)
5. build_grouped_bar       - Grouped bar chart (3 bars per app)
6. build_pie_chart         - Pie chart with outside labels
7. build_entity_lines      - One line per entity (Tab 3, 4, 5)
8. build_annotated_line    - Line with start/end value + % change (Tab 4)

Standards:
- Line width = 1.6
- Target lines = dotted
- hovermode = "x unified"
- Bar values in 1000s format
"""

import plotly.graph_objects as go
from app.theme import get_theme_colors
from app.config import APP_COLORS

LINE_WIDTH = 1.6
LINE_OPACITY = 0.85

# Distinct palette for entity/app lines
_ENTITY_PALETTE = [
    "#E74C3C", "#3B82F6", "#22C55E", "#F59E0B", "#A855F7",
    "#EC4899", "#06B6D4", "#F97316", "#FACC15", "#14B8A6",
    "#E879F9", "#FB923C", "#38BDF8", "#4ADE80", "#F9A8D4",
    "#B91C1C", "#1D4ED8", "#15803D", "#CA8A04", "#7C3AED",
    "#BE185D", "#0E7490", "#C2410C", "#0F766E", "#86198F",
    "#9A3412", "#DC6B6B",
]

# Colors for actual/target/delta bars
ACTUAL_COLOR = "#06B6D4"    # Cyan
TARGET_COLOR = "#1E3A5F"    # Dark navy
DELTA_COLOR = "#22C55E"     # Green


def _entity_color_map(names):
    """Assign colors to entity names using shared APP_COLORS from config"""
    # Handle variants with spaces (e.g. "CT - JP" → "CT-JP")
    def _normalize(n):
        return n.replace(" - ", "-").replace(" -", "-").replace("- ", "-")

    cmap = {}
    for name in sorted(names):
        normalized = _normalize(name)
        if normalized in APP_COLORS:
            cmap[name] = APP_COLORS[normalized]
        else:
            # Fallback: try 2-letter prefix, then hash
            prefix = name[:2].upper()
            if prefix in APP_COLORS:
                cmap[name] = APP_COLORS[prefix]
            else:
                idx = hash(name) % len(_ENTITY_PALETTE)
                cmap[name] = _ENTITY_PALETTE[idx]
    return cmap

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
    if format_type == "dollar":
        yprefix, yformat = "$", ",.0f"
    elif format_type == "percent":
        yprefix, yformat = "", ".1%"
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
            tickformat="%b %Y", hoverformat="%b %d, '%y", range=xrange, fixedrange=False,
        ),
        yaxis=dict(
            gridcolor=colors["border"], linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickprefix=yprefix, tickformat=yformat, fixedrange=False,
        ),
        legend=dict(
            font=dict(color=colors["text_primary"], size=10),
            bgcolor="rgba(0,0,0,0)",
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
        ),
        dragmode="zoom",
    )


def _format_value_k(val):
    """Format value in 1000s: 10340 → '10.3k'"""
    if abs(val) >= 1000:
        return f"{val/1000:.1f}k"
    return f"{val:,.0f}"


# =============================================================================
# 1. KPI CARD (not a plotly chart, returns styled component data)
# =============================================================================

def format_kpi_value(value, fmt="dollar"):
    """Format a KPI value for display"""
    if fmt == "dollar":
        if abs(value) >= 1_000_000:
            return f"$ {value:,.0f}"
        return f"$ {value:,.0f}"
    elif fmt == "percent":
        return f"{value:.2f}%"
    return f"{value:,.0f}"


# =============================================================================
# 2. ACTUAL vs TARGET LINE CHART (2 lines — solid + dotted)
# =============================================================================

def build_actual_target_lines(df, actual_label, target_label, format_type="dollar", date_range=None, theme="dark"):
    """Build chart with solid actual line + dotted target line.
    df must have columns: Date, actual, target
    """
    colors = get_theme_colors(theme)
    if df is None or df.empty:
        return _empty_figure(colors)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["Date"], y=df["actual"],
        mode="lines", name=actual_label,
        line=dict(color=ACTUAL_COLOR, width=LINE_WIDTH),
        hovertemplate=f'{actual_label}  $%{{y:,.0f}}<extra></extra>' if format_type == "dollar"
            else f'{actual_label}  %{{y:,.0f}}<extra></extra>',
        showlegend=True,
    ))
    fig.add_trace(go.Scatter(
        x=df["Date"], y=df["target"],
        mode="lines", name=target_label,
        line=dict(color=TARGET_COLOR, width=LINE_WIDTH, dash="dot"),
        hovertemplate=f'{target_label}  $%{{y:,.0f}}<extra></extra>' if format_type == "dollar"
            else f'{target_label}  %{{y:,.0f}}<extra></extra>',
        showlegend=True,
    ))

    layout = _base_layout(colors, format_type, date_range)
    layout["showlegend"] = True
    layout["margin"] = dict(l=60, r=20, t=40, b=50)
    fig.update_layout(**layout)
    return fig


# =============================================================================
# 3. MULTI-APP DUAL LINES (2 lines per app — solid actual + dotted target)
# =============================================================================

def build_multi_app_lines(df, actual_label, target_label, format_type="dollar", date_range=None, theme="dark"):
    """Build chart with 2 lines per app.
    df must have columns: App_Name, Date, actual, target
    """
    colors = get_theme_colors(theme)
    if df is None or df.empty:
        return _empty_figure(colors), []

    apps = sorted(df["App_Name"].unique())
    cmap = _entity_color_map(apps)

    fig = go.Figure()
    for app in apps:
        adf = df[df["App_Name"] == app].sort_values("Date")
        color = cmap.get(app, "#6B7280")

        # Actual (solid)
        fig.add_trace(go.Scatter(
            x=adf["Date"], y=adf["actual"],
            mode="lines", name=f"{actual_label}, {app}",
            line=dict(color=color, width=LINE_WIDTH),
            hovertemplate=f'{actual_label}, {app}  $%{{y:,.0f}}<extra></extra>' if format_type == "dollar"
                else f'{actual_label}, {app}  %{{y:,.0f}}<extra></extra>',
            showlegend=True,
        ))
        # Target (dotted)
        fig.add_trace(go.Scatter(
            x=adf["Date"], y=adf["target"],
            mode="lines", name=f"{target_label}, {app}",
            line=dict(color=color, width=LINE_WIDTH, dash="dot"),
            hovertemplate=f'{target_label}, {app}  $%{{y:,.0f}}<extra></extra>' if format_type == "dollar"
                else f'{target_label}, {app}  %{{y:,.0f}}<extra></extra>',
            showlegend=True,
        ))

    layout = _base_layout(colors, format_type, date_range)
    layout["showlegend"] = True
    layout["margin"] = dict(l=60, r=20, t=40, b=50)
    fig.update_layout(**layout)
    return fig, apps


# =============================================================================
# 4. GROUPED BAR CHART (3 bars per app)
# =============================================================================

def build_grouped_bar(df, labels=("Actual", "Target", "Delta"), format_type="dollar", theme="dark"):
    """Build grouped bar chart. df must have: App_Name, actual, target, delta"""
    colors = get_theme_colors(theme)
    if df is None or df.empty:
        return _empty_figure(colors)

    fig = go.Figure()

    # Format text values in 1000s
    actual_text = [_format_value_k(v) for v in df["actual"]]
    target_text = [_format_value_k(v) for v in df["target"]]
    delta_text = [_format_value_k(v) for v in df["delta"]]

    fig.add_trace(go.Bar(
        x=df["App_Name"], y=df["actual"],
        name=labels[0], marker_color=ACTUAL_COLOR,
        text=actual_text, textposition="outside", textfont=dict(size=10),
    ))
    fig.add_trace(go.Bar(
        x=df["App_Name"], y=df["target"],
        name=labels[1], marker_color=TARGET_COLOR,
        text=target_text, textposition="outside", textfont=dict(size=10),
    ))
    fig.add_trace(go.Bar(
        x=df["App_Name"], y=df["delta"],
        name=labels[2], marker_color=DELTA_COLOR,
        text=delta_text, textposition="outside", textfont=dict(size=10),
    ))

    layout = _base_layout(colors, format_type)
    layout["barmode"] = "group"
    layout["showlegend"] = True
    layout["xaxis"]["tickformat"] = None
    layout["xaxis"]["tickangle"] = -45
    layout["legend"] = dict(
        font=dict(color=colors["text_primary"]),
        bgcolor="rgba(0,0,0,0)",
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
    )
    fig.update_layout(**layout)
    return fig


# =============================================================================
# 5. PIE CHART (with outside labels and connector lines)
# =============================================================================

def build_pie_chart(labels, values, theme="dark"):
    """Build pie chart with outside labels — hide labels below 10%"""
    colors = get_theme_colors(theme)
    if not labels or not values or sum(values) == 0:
        return _empty_figure(colors)

    total = sum(values)

    # Build custom text: show label only if slice >= 10%
    custom_text = []
    for l, v in zip(labels, values):
        pct = v / total if total > 0 else 0
        if pct >= 0.10:
            custom_text.append(f"{l}: {v:,.0f} ({pct:.1%})")
        else:
            custom_text.append("")

    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        text=custom_text,
        textinfo="text",
        textposition="outside",
        pull=[0.02] * len(labels),
        hole=0,
        marker=dict(
            colors=[_entity_color_map(labels).get(l, "#6B7280") for l in labels],
            line=dict(color=colors["card_bg"], width=1),
        ),
    )])

    fig.update_layout(
        height=400,
        paper_bgcolor=colors["card_bg"],
        plot_bgcolor=colors["card_bg"],
        font=dict(family="Inter, sans-serif", size=11, color=colors["text_primary"]),
        showlegend=False,
        margin=dict(l=40, r=40, t=40, b=40),
        annotations=[dict(
            text=f"Total: {total:,.0f}",
            x=0.5, y=1.08, xref="paper", yref="paper",
            showarrow=False, font=dict(size=14, color=colors["text_primary"])
        )],
    )
    return fig


# =============================================================================
# 6. ENTITY LINE CHART (one line per entity)
# =============================================================================

def build_entity_lines(data_df, format_type="dollar", date_range=None, theme="dark", value_col="value"):
    """Build line chart with one line per App_Name.
    data_df must have: App_Name, Date, <value_col>
    Returns (fig, app_names)
    """
    colors = get_theme_colors(theme)
    if data_df is None or data_df.empty:
        return _empty_figure(colors), []

    apps = sorted(data_df["App_Name"].unique())
    cmap = _entity_color_map(apps)

    fig = go.Figure()
    for app in apps:
        adf = data_df[data_df["App_Name"] == app].sort_values("Date")
        color = cmap.get(app, "#6B7280")

        if format_type == "dollar":
            ht = f'{app}  $%{{y:,.2f}}<extra></extra>'
        elif format_type == "percent":
            ht = f'{app}  %{{y:.2%}}<extra></extra>'
        else:
            ht = f'{app}  %{{y:,.0f}}<extra></extra>'

        fig.add_trace(go.Scatter(
            x=adf["Date"], y=adf[value_col],
            mode="lines", name=app,
            line=dict(color=color, width=LINE_WIDTH),
            hovertemplate=ht,
            showlegend=True,
        ))

    layout = _base_layout(colors, format_type, date_range)
    layout["showlegend"] = True
    layout["margin"] = dict(l=60, r=20, t=40, b=50)
    fig.update_layout(**layout)
    return fig, apps


# =============================================================================
# 7. ANNOTATED LINE CHART (Tab 4 — with start/end values + % change)
# =============================================================================

def build_annotated_line(df, format_type="number", date_range=None, theme="dark",
                         date_col="Date", value_col="Current_Active_Subscription", name="Value"):
    """Build single line chart with start/end value annotations + % change.
    Top-left: {start_value} to {end_value}
    Top-right: {%_change} in green/red
    """
    colors = get_theme_colors(theme)
    if df is None or df.empty:
        return _empty_figure(colors)

    df = df.sort_values(date_col)
    start_val = df[value_col].iloc[0]
    end_val = df[value_col].iloc[-1]
    pct_change = ((end_val - start_val) / start_val * 100) if start_val != 0 else 0

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[date_col], y=df[value_col],
        mode="lines", name=name,
        line=dict(color=ACTUAL_COLOR, width=LINE_WIDTH),
        hovertemplate=f'{name}  %{{y:,.0f}}<extra></extra>',
        showlegend=False,
    ))

    layout = _base_layout(colors, format_type, date_range)

    # Start/end annotation (top-left)
    start_str = f"{start_val:,.0f}"
    end_str = f"{end_val:,.0f}"
    change_color = "#22C55E" if pct_change >= 0 else "#E74C3C"
    change_arrow = "↑" if pct_change >= 0 else "↓"

    fig.update_layout(**layout)
    return fig, start_val, end_val, pct_change

def build_annotated_entity_lines(data_df, format_type="percent", date_range=None, theme="dark",
                                  value_col="value"):
    """Entity line chart WITH start/end annotations (Tab 4 entity charts).
    Annotations show portfolio-level start/end from the sum of all entities.
    """
    colors = get_theme_colors(theme)
    if data_df is None or data_df.empty:
        return _empty_figure(colors), []

    apps = sorted(data_df["App_Name"].unique())
    cmap = _entity_color_map(apps)

    # Compute portfolio totals for annotation
    totals = data_df.groupby("Date", as_index=False)[value_col].mean()
    totals = totals.sort_values("Date")
    if not totals.empty:
        start_val = totals[value_col].iloc[0]
        end_val = totals[value_col].iloc[-1]
        pct_change = ((end_val - start_val) / start_val * 100) if start_val != 0 else 0
    else:
        start_val = end_val = pct_change = 0

    fig = go.Figure()
    for app in apps:
        adf = data_df[data_df["App_Name"] == app].sort_values("Date")
        color = cmap.get(app, "#6B7280")

        if format_type == "percent":
            ht = f'{app}  %{{y:.2%}}<extra></extra>'
        else:
            ht = f'{app}  %{{y:,.0f}}<extra></extra>'

        fig.add_trace(go.Scatter(
            x=adf["Date"], y=adf[value_col],
            mode="lines", name=app,
            line=dict(color=color, width=LINE_WIDTH),
            hovertemplate=ht,
            showlegend=True,
        ))

    layout = _base_layout(colors, format_type, date_range)
    layout["showlegend"] = True
    layout["margin"] = dict(l=60, r=20, t=40, b=50)

    # Annotations
    if format_type == "percent":
        start_str = f"{start_val:.2%}"
        end_str = f"{end_val:.2%}"
    else:
        start_str = f"{start_val:,.0f}"
        end_str = f"{end_val:,.0f}"

    fig.update_layout(**layout)
    return fig, apps, start_val, end_val, pct_change

def build_annotated_portfolio_line(df, format_type="percent", date_range=None, theme="dark",
                                    date_col="Date", value_col="value", name="Portfolio"):
    """Single portfolio line with start/end annotations (Tab 4 portfolio charts)"""
    colors = get_theme_colors(theme)
    if df is None or df.empty:
        return _empty_figure(colors)

    df = df.sort_values(date_col)
    start_val = df[value_col].iloc[0]
    end_val = df[value_col].iloc[-1]
    pct_change = ((end_val - start_val) / start_val * 100) if start_val != 0 else 0

    fig = go.Figure()
    if format_type == "percent":
        ht = f'{name}  %{{y:.2%}}<extra></extra>'
    else:
        ht = f'{name}  %{{y:,.0f}}<extra></extra>'

    fig.add_trace(go.Scatter(
        x=df[date_col], y=df[value_col],
        mode="lines", name=name,
        line=dict(color=ACTUAL_COLOR, width=LINE_WIDTH),
        hovertemplate=ht,
        showlegend=False,
    ))

    layout = _base_layout(colors, format_type, date_range)

    if format_type == "percent":
        start_str = f"{start_val:.2%}"
        end_str = f"{end_val:.2%}"
    else:
        start_str = f"{start_val:,.0f}"
        end_str = f"{end_val:,.0f}"

    fig.update_layout(**layout)
    return fig, start_val, end_val, pct_change
