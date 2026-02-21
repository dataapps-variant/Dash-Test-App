"""
Chart builders for Daedalus Dashboard

Chart types (Tabs 1-5):
1. build_kpi_card              - KPI number card (Tab 1)
2. build_pivot_grid            - AG Grid pivot table
3. build_actual_target_lines   - Two lines (solid actual + dotted target)
4. build_multi_app_lines       - Two lines per app (solid actual + dotted target)
5. build_grouped_bar           - Grouped bar chart (3 bars per app)
6. build_pie_chart             - Pie chart with outside labels
7. build_entity_lines          - One line per entity (Tab 3, 4, 5)
8. build_annotated_line        - Line with start/end value + % change (Tab 4)

Chart types (Tabs 6-16):
9.  build_tc_multi_lines       - One line per Traffic Channel (Tab 6)
10. build_tc_pie               - Pie chart per Traffic Channel (Tabs 7, 8)
11. build_stacked_area         - Stacked area chart (Tabs 7, 8, 10)
12. build_cac_tc_lines         - Dotted Daily + Solid T7D per channel (Tab 9)
13. build_dual_axis_approval   - CIT left axis / MIT right axis (Tab 13)
14. build_stacked_bar_100      - 100% stacked bar chart (Tabs 14-16)

Standards:
- Line width = 1.6
- Target lines = dotted
- hovermode = "x unified"
- Bar values in 1000s format
- Legend: horizontal, top-left (orientation="h", y=1.02, x=0)
- Tooltip date: hoverformat="%b %d, '%y"
- Pie labels hidden below 10%
"""

import plotly.graph_objects as go
from app.theme import get_theme_colors
from app.config import APP_COLORS
from app.traffic_channel_map import get_channel_label

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


# =============================================================================
# TRAFFIC CHANNEL COLOR MAP (Tabs 6-9)
# =============================================================================

def _tc_color_map(channel_ids):
    """Assign colors to Traffic_Channel integer IDs using hash-based palette"""
    cmap = {}
    for cid in sorted(channel_ids):
        idx = hash(int(cid)) % len(_ENTITY_PALETTE)
        cmap[cid] = _ENTITY_PALETTE[idx]
    return cmap


# =============================================================================
# 9. TRAFFIC CHANNEL MULTI-LINE CHART (Tab 6)
# =============================================================================

def build_tc_multi_lines(data_df, format_type="dollar", date_range=None, theme="dark"):
    """Build line chart with one line per Traffic_Channel.
    data_df must have: Date, Traffic_Channel, value
    Returns (fig, channel_ids)
    """
    colors = get_theme_colors(theme)
    if data_df is None or data_df.empty:
        return _empty_figure(colors), []

    channels = sorted(data_df["Traffic_Channel"].unique())
    cmap = _tc_color_map(channels)

    fig = go.Figure()
    for ch in channels:
        chdf = data_df[data_df["Traffic_Channel"] == ch].sort_values("Date")
        color = cmap.get(ch, "#6B7280")
        label = get_channel_label(ch)

        if format_type == "dollar":
            ht = f'{label}  $%{{y:,.2f}}<extra></extra>'
        elif format_type == "percent":
            ht = f'{label}  %{{y:.2%}}<extra></extra>'
        else:
            ht = f'{label}  %{{y:,.0f}}<extra></extra>'

        fig.add_trace(go.Scatter(
            x=chdf["Date"], y=chdf["value"],
            mode="lines", name=label,
            line=dict(color=color, width=LINE_WIDTH),
            hovertemplate=ht,
            showlegend=True,
        ))

    layout = _base_layout(colors, format_type, date_range)
    layout["showlegend"] = True
    layout["margin"] = dict(l=60, r=20, t=40, b=50)
    fig.update_layout(**layout)
    return fig, channels


# =============================================================================
# 10. TRAFFIC CHANNEL PIE CHART (Tabs 7, 8)
# =============================================================================

def build_tc_pie(data_df, theme="dark"):
    """Build pie chart for traffic channel data — hide labels below 10%.
    data_df must have: Traffic_Channel, total
    """
    colors = get_theme_colors(theme)
    if data_df is None or data_df.empty:
        return _empty_figure(colors)

    labels = [get_channel_label(ch) for ch in data_df["Traffic_Channel"]]
    values = data_df["total"].tolist()
    total = sum(values)
    if total == 0:
        return _empty_figure(colors)

    cmap = _tc_color_map(data_df["Traffic_Channel"].tolist())
    pie_colors = [cmap.get(ch, "#6B7280") for ch in data_df["Traffic_Channel"]]

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
            colors=pie_colors,
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
# 11. STACKED AREA CHART (Tabs 7, 8, 10)
# =============================================================================

def build_stacked_area(data_df, format_type="number", date_range=None, theme="dark",
                       group_col="Traffic_Channel", use_channel_labels=True):
    """Build stacked area chart — raw values, not percentage-based.
    data_df must have: Date, <group_col>, value
    """
    colors = get_theme_colors(theme)
    if data_df is None or data_df.empty:
        return _empty_figure(colors)

    groups = sorted(data_df[group_col].unique())
    if use_channel_labels:
        cmap = _tc_color_map(groups)
    else:
        # For AFID or other string groups, use hash-based palette
        cmap = {}
        for g in groups:
            idx = hash(str(g)) % len(_ENTITY_PALETTE)
            cmap[g] = _ENTITY_PALETTE[idx]

    fig = go.Figure()
    for g in groups:
        gdf = data_df[data_df[group_col] == g].sort_values("Date")
        color = cmap.get(g, "#6B7280")
        label = get_channel_label(g) if use_channel_labels else str(g)

        if format_type == "dollar":
            ht = f'{label}  $%{{y:,.2f}}<extra></extra>'
        else:
            ht = f'{label}  %{{y:,.0f}}<extra></extra>'

        fig.add_trace(go.Scatter(
            x=gdf["Date"], y=gdf["value"],
            mode="lines", name=label,
            line=dict(color=color, width=0.5),
            stackgroup="one",
            fillcolor=color,
            hovertemplate=ht,
            showlegend=True,
        ))

    layout = _base_layout(colors, format_type, date_range)
    layout["showlegend"] = True
    layout["margin"] = dict(l=60, r=20, t=40, b=50)
    fig.update_layout(**layout)
    return fig


# =============================================================================
# 12. CAC TRAFFIC CHANNEL LINES — dotted Daily + solid T7D (Tab 9)
# =============================================================================

def build_cac_tc_lines(data_df, metrics, date_range=None, theme="dark"):
    """Build line chart with 2 lines per Traffic_Channel (dotted Daily_CAC + solid T7D_CAC).
    data_df must have: Date, Traffic_Channel, Daily_CAC, T7D_CAC
    metrics = list of metric column names selected
    Returns (fig, channel_ids)
    """
    colors = get_theme_colors(theme)
    if data_df is None or data_df.empty:
        return _empty_figure(colors), []

    channels = sorted(data_df["Traffic_Channel"].unique())
    cmap = _tc_color_map(channels)

    fig = go.Figure()
    for ch in channels:
        chdf = data_df[data_df["Traffic_Channel"] == ch].sort_values("Date")
        color = cmap.get(ch, "#6B7280")
        label = get_channel_label(ch)

        if "Daily_CAC" in metrics and "Daily_CAC" in chdf.columns:
            fig.add_trace(go.Scatter(
                x=chdf["Date"], y=chdf["Daily_CAC"],
                mode="lines", name=f"{label} - Daily CAC",
                line=dict(color=color, width=LINE_WIDTH, dash="dot"),
                hovertemplate=f'{label} - Daily CAC  $%{{y:,.2f}}<extra></extra>',
                showlegend=True,
            ))
        if "T7D_CAC" in metrics and "T7D_CAC" in chdf.columns:
            fig.add_trace(go.Scatter(
                x=chdf["Date"], y=chdf["T7D_CAC"],
                mode="lines", name=f"{label} - T7D CAC",
                line=dict(color=color, width=LINE_WIDTH),
                hovertemplate=f'{label} - T7D CAC  $%{{y:,.2f}}<extra></extra>',
                showlegend=True,
            ))

    layout = _base_layout(colors, "dollar", date_range)
    layout["showlegend"] = True
    layout["margin"] = dict(l=60, r=20, t=40, b=50)
    fig.update_layout(**layout)
    return fig, channels


# =============================================================================
# 13. DUAL Y-AXIS APPROVAL RATES (Tab 13)
# =============================================================================

def build_dual_axis_approval(per_entity_df, total_df, entity_col, date_range=None, theme="dark"):
    """Build dual y-axis chart: CIT on left, MIT on right.
    per_entity_df: Report_Date, <entity_col>, CIT_Percent, MIT_Percent
    total_df: Report_Date, CIT_Percent, MIT_Percent
    entity_col: 'App_Name', 'Channel_Name', or 'AFID'
    """
    colors = get_theme_colors(theme)
    if (per_entity_df is None or per_entity_df.empty) and (total_df is None or total_df.empty):
        return _empty_figure(colors)

    fig = go.Figure()

    # Per entity lines
    if per_entity_df is not None and not per_entity_df.empty:
        entities = sorted(per_entity_df[entity_col].unique())
        cmap = _entity_color_map(entities) if entity_col == "App_Name" else {}
        if not cmap:
            for e in entities:
                idx = hash(str(e)) % len(_ENTITY_PALETTE)
                cmap[e] = _ENTITY_PALETTE[idx]

        for ent in entities:
            edf = per_entity_df[per_entity_df[entity_col] == ent].sort_values("Report_Date")
            color = cmap.get(ent, "#6B7280")

            # CIT — left y-axis (yaxis)
            fig.add_trace(go.Scatter(
                x=edf["Report_Date"], y=edf["CIT_Percent"],
                mode="lines", name=f"{ent} - CIT",
                line=dict(color=color, width=LINE_WIDTH),
                hovertemplate=f'{ent} CIT  %{{y:.2%}}<extra></extra>',
                showlegend=True, yaxis="y",
            ))
            # MIT — right y-axis (yaxis2)
            fig.add_trace(go.Scatter(
                x=edf["Report_Date"], y=edf["MIT_Percent"],
                mode="lines", name=f"{ent} - MIT",
                line=dict(color=color, width=LINE_WIDTH, dash="dash"),
                hovertemplate=f'{ent} MIT  %{{y:.2%}}<extra></extra>',
                showlegend=True, yaxis="y2",
            ))

    # Total lines
    if total_df is not None and not total_df.empty:
        total_df = total_df.sort_values("Report_Date")
        fig.add_trace(go.Scatter(
            x=total_df["Report_Date"], y=total_df["CIT_Percent"],
            mode="lines", name="Total - CIT",
            line=dict(color="#FFFFFF", width=LINE_WIDTH + 0.5),
            hovertemplate='Total CIT  %{y:.2%}<extra></extra>',
            showlegend=True, yaxis="y",
        ))
        fig.add_trace(go.Scatter(
            x=total_df["Report_Date"], y=total_df["MIT_Percent"],
            mode="lines", name="Total - MIT",
            line=dict(color="#FFFFFF", width=LINE_WIDTH + 0.5, dash="dash"),
            hovertemplate='Total MIT  %{y:.2%}<extra></extra>',
            showlegend=True, yaxis="y2",
        ))

    xrange = [date_range[0], date_range[1]] if date_range else None
    fig.update_layout(
        height=450,
        margin=dict(l=60, r=60, t=40, b=50),
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
            title="CIT %",
            gridcolor=colors["border"], linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickformat=".0%", fixedrange=False, side="left",
        ),
        yaxis2=dict(
            title="MIT %",
            gridcolor="rgba(0,0,0,0)", linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickformat=".0%", fixedrange=False, side="right",
            overlaying="y",
        ),
        legend=dict(
            font=dict(color=colors["text_primary"], size=10),
            bgcolor="rgba(0,0,0,0)",
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
        ),
        dragmode="zoom",
    )
    return fig


# =============================================================================
# 14. 100% STACKED BAR CHART (Tabs 14-16)
# =============================================================================

def build_stacked_bar_100(data_df, date_range=None, theme="dark"):
    """Build 100% stacked bar chart — one bar per date, segments per Final_Category.
    data_df must have: Report_Date, Final_Category, pct
    """
    colors = get_theme_colors(theme)
    if data_df is None or data_df.empty:
        return _empty_figure(colors)

    categories = sorted(data_df["Final_Category"].unique())
    cmap = {}
    for cat in categories:
        idx = hash(cat) % len(_ENTITY_PALETTE)
        cmap[cat] = _ENTITY_PALETTE[idx]

    dates = sorted(data_df["Report_Date"].unique())

    fig = go.Figure()
    for cat in categories:
        cdf = data_df[data_df["Final_Category"] == cat].set_index("Report_Date")
        y_vals = [cdf.loc[d, "pct"] if d in cdf.index else 0 for d in dates]
        fig.add_trace(go.Bar(
            x=dates, y=y_vals,
            name=cat,
            marker_color=cmap.get(cat, "#6B7280"),
            hovertemplate=f'{cat}  %{{y:.1f}}%<extra></extra>',
        ))

    xrange = [date_range[0], date_range[1]] if date_range else None
    fig.update_layout(
        barmode="stack",
        height=400,
        margin=dict(l=60, r=20, t=40, b=50),
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
            tickformat=".0f", ticksuffix="%",
            range=[0, 100], fixedrange=False,
        ),
        legend=dict(
            font=dict(color=colors["text_primary"], size=10),
            bgcolor="rgba(0,0,0,0)",
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
        ),
        dragmode="zoom",
    )
    return fig
