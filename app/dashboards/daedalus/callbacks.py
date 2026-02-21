"""
Callbacks for Daedalus Dashboard — Tabs 1-16

Tabs 1-5:
Tab 1: Daedalus (KPI cards, pivot tables, line charts, bar charts)
Tab 2: Pacing by Entity (dynamic spend+users line pairs per app)
Tab 3: CAC by Entity (one chart per app with Daily_CAC/T7D_CAC)
Tab 4: Current Subscriptions (line, pivot, pie, annotated entity charts)
Tab 5: Daedalus Historical (6 line charts + 1 pie)

Tabs 6-16:
Tab 6:  Traffic Channel (T30D line charts per app)
Tab 7:  New Users - Traffic Channel (pie + stacked area per app)
Tab 8:  Spend - Traffic Channel (pie + stacked area per app)
Tab 9:  CAC - Traffic Channel (dotted Daily + solid T7D per channel)
Tab 10: AFID Unknown (pie + stacked area, aggregated)
Tab 11: Daily Report (2 AG Grid tables)
Tab 12: MTD Report (2 AG Grid tables)
Tab 13: Approval Rates (3 dual y-axis charts)
Tab 14: Decline Reason % - App (2 stacked bar charts)
Tab 15: Decline Reason % - Channel (2 stacked bar charts)
Tab 16: Decline Reason % - AFID (2 stacked bar charts)
"""

from datetime import date, datetime, timedelta
from dash import html, dcc, Input, Output, State, callback_context, ALL, MATCH
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import plotly.graph_objects as go

from app.theme import get_theme_colors

from app.dashboards.daedalus.data import (
    get_tab1_kpi_cards,
    get_spend_pivot, get_users_pivot, get_cac_pivot,
    get_lines_by_app, get_lines_total, get_bars_by_app,
    get_pacing_by_entity,
    get_cac_by_entity,
    get_portfolio_active_subs, get_current_subs_pivot,
    get_pie_by_app, get_pie_by_app_channel,
    get_entity_active_subs, get_entity_churn, get_portfolio_churn,
    get_entity_ss, get_portfolio_ss, get_entity_pending, get_portfolio_pending,
    get_historical_metric_by_app, get_historical_spend_split,
    refresh_daedalus_bq_to_staging, refresh_daedalus_gcs_from_staging,
    # Tabs 6-8
    get_tc_lines_by_app, get_tc_pie_by_app, get_tc_stacked_by_app,
    # Tab 9
    get_cac_tc_by_app,
    # Tab 10
    get_afid_unknown_pie, get_afid_unknown_stacked,
    # Tab 11
    get_cpa_by_entity_daily, get_cpa_by_application_daily,
    # Tab 12
    get_cpa_by_entity_mtd, get_cpa_by_application_mtd,
    # Tab 13
    get_app_approval_rates, get_channel_approval_rates, get_afid_approval_rates,
    # Tabs 14-16
    get_decline_app_data, get_decline_channel_data, get_decline_afid_data,
)

from app.dashboards.daedalus.charts import (
    format_kpi_value,
    build_actual_target_lines, build_multi_app_lines,
    build_grouped_bar, build_pie_chart, build_entity_lines,
    build_annotated_line, build_annotated_entity_lines,
    build_annotated_portfolio_line,
    _empty_figure,
    # Tabs 6-16
    build_tc_multi_lines, build_tc_pie, build_stacked_area,
    build_cac_tc_lines, build_dual_axis_approval, build_stacked_bar_100,
)

from app.traffic_channel_map import get_channel_label

THEME = "dark"
CHART_CONFIG = {
    "displayModeBar": True, "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "scrollZoom": False,
}


def _colors():
    return get_theme_colors(THEME)


def _card_style(colors):
    return {
        "backgroundColor": colors["card_bg"],
        "borderRadius": "8px",
        "border": f"1px solid {colors['border']}",
        "padding": "16px",
        "marginBottom": "16px",
    }


def _section_title(text, colors):
    return html.H6(text, style={"color": colors["text_primary"], "marginBottom": "12px", "fontWeight": "600"})


def _annotation_box(start_val, end_val, pct_change, format_type, colors):
    """Build an HTML summary box showing Start | End | % Change"""
    if format_type == "percent":
        start_str = f"{start_val:.2%}"
        end_str = f"{end_val:.2%}"
    elif format_type == "dollar":
        start_str = f"${start_val:,.0f}"
        end_str = f"${end_val:,.0f}"
    else:
        start_str = f"{start_val:,.0f}"
        end_str = f"{end_val:,.0f}"

    change_color = "#22C55E" if pct_change >= 0 else "#E74C3C"
    change_arrow = "↑" if pct_change >= 0 else "↓"
    change_str = f"{pct_change:+.2f}% {change_arrow}"

    item_style = {
        "display": "inline-flex", "alignItems": "center", "gap": "6px",
        "padding": "0 16px",
    }
    separator_style = {
        "width": "1px", "height": "24px",
        "backgroundColor": colors["border"], "display": "inline-block",
    }

    return html.Div([
        html.Span([
            html.Span("Start: ", style={"color": colors["text_secondary"], "fontSize": "12px"}),
            html.Span(start_str, style={"color": colors["text_primary"], "fontSize": "13px", "fontWeight": "600"}),
        ], style=item_style),
        html.Span(style=separator_style),
        html.Span([
            html.Span("End: ", style={"color": colors["text_secondary"], "fontSize": "12px"}),
            html.Span(end_str, style={"color": colors["text_primary"], "fontSize": "13px", "fontWeight": "600"}),
        ], style=item_style),
        html.Span(style=separator_style),
        html.Span([
            html.Span("Change: ", style={"color": colors["text_secondary"], "fontSize": "12px"}),
            html.Span(change_str, style={"color": change_color, "fontSize": "13px", "fontWeight": "700"}),
        ], style=item_style),
    ], style={
        "display": "inline-flex", "alignItems": "center",
        "backgroundColor": colors["card_bg"],
        "border": f"1px solid {colors['border']}",
        "borderRadius": "6px",
        "padding": "6px 4px",
        "marginBottom": "10px",
    })


# =============================================================================
# KPI CARD COMPONENT
# =============================================================================

def _kpi_card(title, value, fmt="dollar", colors=None):
    """Build a single KPI card component"""
    display = format_kpi_value(value, fmt)
    text_color = colors["text_primary"]
    if fmt == "percent":
        text_color = "#22C55E" if value >= 0 else "#E74C3C"
    elif fmt == "dollar" and "Delta" in title:
        text_color = "#22C55E" if value >= 0 else "#E74C3C"

    return dbc.Col(
        html.Div([
            html.Div(title, style={"color": colors["text_secondary"], "fontSize": "13px", "marginBottom": "4px"}),
            html.Div(display, style={"color": text_color, "fontSize": "28px", "fontWeight": "700"}),
        ], style={
            "backgroundColor": colors["card_bg"],
            "border": f"1px solid {colors['border']}",
            "borderRadius": "8px",
            "padding": "16px",
        }),
        width=3,
    )


# =============================================================================
# PIVOT TABLE COMPONENT (AG Grid)
# =============================================================================

def _pivot_grid(pivot_df, colors, grid_id):
    """Build AG Grid for pivot table"""
    if pivot_df is None or pivot_df.empty:
        return html.Div("No data", style={"color": colors["text_secondary"]})

    columns = pivot_df.columns.tolist()
    col_defs = []
    for col in columns:
        cd = {"headerName": col, "field": col, "sortable": True, "filter": True}
        if col == "Metric":
            cd["pinned"] = "left"
            cd["width"] = 160
        else:
            cd["width"] = 160
            cd["type"] = "rightAligned"
            cd["valueFormatter"] = {"function": "params.value != null ? (typeof params.value === 'number' ? '$ ' + params.value.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : params.value) : ''"}
            cd["cellStyle"] = {"function": "params.data && params.data.Metric && params.data.Metric.indexOf('Delta') !== -1 && params.value != null && typeof params.value === 'number' ? (params.data.Metric.indexOf('CAC') !== -1 ? (params.value < 0 ? {'color': '#22C55E'} : params.value > 0 ? {'color': '#E74C3C'} : null) : (params.value > 0 ? {'color': '#22C55E'} : params.value < 0 ? {'color': '#E74C3C'} : null)) : null"}
        col_defs.append(cd)
        
    return dag.AgGrid(
        id=grid_id,
        columnDefs=col_defs,
        rowData=pivot_df.to_dict("records"),
        defaultColDef={"resizable": True},
        dashGridOptions={"domLayout": "autoHeight"},
        style={"width": "100%"},
        className="ag-theme-alpine-dark",
    )


# =============================================================================
# FILTER UI BUILDERS
# =============================================================================

def _build_app_checklist(apps, id_prefix, colors, default_all=True):
    """Build app name checklist with Select All toggle"""
    return html.Div([
        html.Div("App Name", style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
        dbc.Checklist(
            options=[{"label": "Select All", "value": "__all__"}],
            value=["__all__"] if default_all else [],
            id=f"{id_prefix}-select-all-apps",
            inline=True,
            className="daedalus-checkbox",
            style={"fontSize": "12px", "fontWeight": "600", "marginBottom": "4px"},
        ),
        dbc.Checklist(
            options=[{"label": a, "value": a} for a in apps],
            value=apps if default_all else [],
            id=f"{id_prefix}-app-checklist",
            inline=True,
            className="daedalus-checkbox",
            style={"fontSize": "12px"},
        ),
    ])


def _build_month_selector(month_options, default_value, id_prefix, colors):
    return html.Div([
        html.Div("Month & Year", style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
        dcc.Dropdown(
            id=f"{id_prefix}-month-select",
            options=month_options,
            value=default_value,
            clearable=False,
            style={"width": "140px", "backgroundColor": colors["card_bg"], "color": colors["text_primary"]},
        ),
    ])


def _build_date_picker(id_str, min_date, max_date, default_date, label, colors):
    return html.Div([
        html.Div(label, style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
        dcc.DatePickerSingle(
            id=id_str,
            min_date_allowed=min_date,
            max_date_allowed=max_date,
            date=default_date,
            display_format="YYYY-MM-DD",
        ),
    ])


def _build_metric_checklist(metrics, id_str, colors, default_all=True):
    return html.Div([
        html.Div("Metrics", style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
        dbc.Checklist(
            options=[{"label": "Select All", "value": "__all__"}],
            value=["__all__"] if default_all else [],
            id=f"{id_str}-select-all",
            inline=True,
            className="daedalus-checkbox",
            style={"fontSize": "12px", "fontWeight": "600", "marginBottom": "4px"},
        ),
        dbc.Checklist(
            options=[{"label": m, "value": m} for m in metrics],
            value=metrics if default_all else [],
            id=id_str,
            inline=True,
            className="daedalus-checkbox",
            style={"fontSize": "12px"},
        ),
    ])


# =============================================================================
# REGISTER CALLBACKS
# =============================================================================

def register_callbacks(app):
    """Register all Daedalus callbacks"""

    # -----------------------------------------------------------------
    # TAB SWITCHING — render content for active tab
    # -----------------------------------------------------------------
    @app.callback(
        [Output(f"daedalus-tab-{t['id']}-content", "children") for t in [
            {"id": "daedalus"}, {"id": "pacing-entity"}, {"id": "cac-entity"},
            {"id": "current-subs"}, {"id": "daedalus-historical"},
        ]],
        Input("daedalus-dashboard-tabs", "active_tab"),
        State("daedalus-filter-options", "data"),
    )
    def render_active_tab(active_tab, filter_opts):
        colors = _colors()
        empty = html.Div()
        outputs = [empty] * 5

        tab_map = {
            "daedalus": 0, "pacing-entity": 1, "cac-entity": 2,
            "current-subs": 3, "daedalus-historical": 4,
        }
        idx = tab_map.get(active_tab)
        if idx is None:
            # Placeholder for tabs 6-16
            return outputs

        if active_tab == "daedalus":
            outputs[0] = _build_tab1(colors, filter_opts)
        elif active_tab == "pacing-entity":
            outputs[1] = _build_tab2(colors, filter_opts)
        elif active_tab == "cac-entity":
            outputs[2] = _build_tab3(colors, filter_opts)
        elif active_tab == "current-subs":
            outputs[3] = _build_tab4(colors, filter_opts)
        elif active_tab == "daedalus-historical":
            outputs[4] = _build_tab5(colors, filter_opts)

        return outputs

    # -----------------------------------------------------------------
    # TAB 1: DAEDALUS — update charts on filter change
    # -----------------------------------------------------------------
    @app.callback(
        Output("daedalus-tab1-charts", "children"),
        Input("tab1-load-btn", "n_clicks"),
        [State("tab1-app-checklist", "value"),
         State("tab1-date-picker", "date"),
         State("tab1-month-select", "value")],
        prevent_initial_call=True,
    )
    def update_tab1_charts(n_clicks, app_names, selected_date, month_str):
        colors = _colors()
        if not app_names or not selected_date or not month_str:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})

        year, month = int(month_str.split("-")[0]), int(month_str.split("-")[1])

        # --- KPI Cards (always all apps, latest date) ---
        kpi = get_tab1_kpi_cards()
        kpi_row = dbc.Row([
            _kpi_card("Actual Spend ($)", kpi.get("actual_spend", 0), "dollar", colors),
            _kpi_card("Allocated Spend ($)", kpi.get("allocated_spend", 0), "dollar", colors),
            _kpi_card("Spend Delta ($)", kpi.get("spend_delta", 0), "dollar", colors),
            _kpi_card("Spend Delta (%)", kpi.get("spend_delta_pct", 0), "percent", colors),
        ], className="mb-3")

        # --- SPEND SECTION ---
        spend_pivot = get_spend_pivot(app_names, selected_date)
        spend_lines = get_lines_by_app(app_names, year, month, "Actual_Spend_MTD", "Target_Spend_MTD")
        spend_total = get_lines_total(app_names, year, month, "Actual_Spend_MTD", "Target_Spend_MTD")
        spend_bars = get_bars_by_app(app_names, selected_date, "Actual_Spend_MTD", "Target_Spend_MTD", "Delta_Spend")

        spend_pivot_grid = _pivot_grid(spend_pivot, colors, "tab1-spend-pivot")
        spend_lines_fig, _ = build_multi_app_lines(spend_lines, "Actual Spend", "Target Spend", "dollar", theme=THEME)
        spend_total_fig = build_actual_target_lines(spend_total, "Actual Spend", "Target Spend", "dollar", theme=THEME)
        spend_bar_fig = build_grouped_bar(spend_bars, ("Actual Spend", "Target Spend", "Delta Spend"), "dollar", THEME)

        spend_section = html.Div([
            _section_title("Spend Pacing: Actual vs Target (MTD)", colors),
            spend_pivot_grid,
            _section_title("Monthly Spend Pacing", colors),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=spend_lines_fig, config=CHART_CONFIG), width=12),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    _section_title("Monthly Portfolio Pacing: Spend", colors),
                    dcc.Graph(figure=spend_total_fig, config=CHART_CONFIG),
                ], width=6),
                dbc.Col([
                    _section_title("Marketing Spend: Actual vs Target (MTD)", colors),
                    dcc.Graph(figure=spend_bar_fig, config=CHART_CONFIG),
                ], width=6),
            ]),
        ], style=_card_style(colors))

        # --- USERS SECTION ---
        users_pivot = get_users_pivot(app_names, selected_date)
        users_lines = get_lines_by_app(app_names, year, month, "Actual_New_Users_MTD", "Target_New_Users_MTD")
        users_total = get_lines_total(app_names, year, month, "Actual_New_Users_MTD", "Target_New_Users_MTD")
        users_bars = get_bars_by_app(app_names, selected_date, "Actual_New_Users_MTD", "Target_New_Users_MTD", "Delta_Users")

        users_pivot_grid = _pivot_grid(users_pivot, colors, "tab1-users-pivot")
        users_lines_fig, _ = build_multi_app_lines(users_lines, "Actual Users", "Target Users", "number", theme=THEME)
        users_total_fig = build_actual_target_lines(users_total, "Actual Users", "Target Users", "number", theme=THEME)
        users_bar_fig = build_grouped_bar(users_bars, ("Actual Users", "Target Users", "Delta Users"), "number", THEME)

        users_section = html.Div([
            _section_title("New Users: Actual vs Target (MTD)", colors),
            users_pivot_grid,
            _section_title("Monthly New Users Pacing", colors),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=users_lines_fig, config=CHART_CONFIG), width=12),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    _section_title("Monthly New User Pacing: Actual vs Target", colors),
                    dcc.Graph(figure=users_total_fig, config=CHART_CONFIG),
                ], width=6),
                dbc.Col([
                    _section_title("New Users: Actual vs Target (MTD)", colors),
                    dcc.Graph(figure=users_bar_fig, config=CHART_CONFIG),
                ], width=6),
            ]),
        ], style=_card_style(colors))
        # --- CAC SECTION ---
        cac_pivot = get_cac_pivot(app_names, selected_date)
        cac_bars = get_bars_by_app(app_names, selected_date, "Actual_CAC", "Target_CAC", "Delta_CAC")

        cac_pivot_grid = _pivot_grid(cac_pivot, colors, "tab1-cac-pivot")
        cac_bar_fig = build_grouped_bar(cac_bars, ("Actual CAC", "Target CAC", "Delta CAC"), "dollar", THEME)

        cac_section = html.Div([
            _section_title("CAC: Actual vs Target (MTD)", colors),
            cac_pivot_grid,
            _section_title("MTD CAC Targets", colors),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=cac_bar_fig, config=CHART_CONFIG), width=12),
            ]),
        ], style=_card_style(colors))

        return html.Div([kpi_row, spend_section, users_section, cac_section])

    # -----------------------------------------------------------------
    # TAB 2: PACING BY ENTITY — update on month change
    # -----------------------------------------------------------------
    @app.callback(
        Output("daedalus-tab2-charts", "children"),
        Input("tab2-load-btn", "n_clicks"),
        State("tab2-month-select", "value"),
        prevent_initial_call=True,
    )
    def update_tab2_charts(n_clicks, month_str):
        colors = _colors()
        if not month_str:
            return html.Div("Select a month", style={"color": colors["text_secondary"]})

        year, month = int(month_str.split("-")[0]), int(month_str.split("-")[1])
        pacing = get_pacing_by_entity(year, month)

        if not pacing:
            return html.Div("No data for selected month", style={"color": colors["text_secondary"]})

        rows = []
        # VG (portfolio) first
        if "VG" in pacing:
            vg_df = pacing["VG"]
            spend_fig = build_actual_target_lines(vg_df.rename(columns={"actual_spend": "actual", "target_spend": "target"}),
                                                   "Actual Spend", "Target Spend", "dollar", theme=THEME)
            users_fig = build_actual_target_lines(vg_df.rename(columns={"actual_users": "actual", "target_users": "target"}),
                                                   "Actual Users", "Target Users", "number", theme=THEME)
            rows.append(html.Div([
                dbc.Row([
                    dbc.Col(_section_title("Monthly Spend Pacing VG (Portfolio)", colors), width=6),
                    dbc.Col(_section_title("Monthly Users Pacing VG", colors), width=6),
                ]),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=spend_fig, config=CHART_CONFIG), width=6),
                    dbc.Col(dcc.Graph(figure=users_fig, config=CHART_CONFIG), width=6),
                ]),
            ], style=_card_style(colors)))

        # Per app
        for app_name, app_df in pacing.items():
            if app_name == "VG":
                continue
            spend_df = app_df.rename(columns={"actual_spend": "actual", "target_spend": "target"})
            users_df = app_df.rename(columns={"actual_users": "actual", "target_users": "target"})

            spend_fig = build_actual_target_lines(spend_df, "Actual Spend", "Target Spend", "dollar", theme=THEME)
            users_fig = build_actual_target_lines(users_df, "Actual Users", "Target Users", "number", theme=THEME)

            rows.append(html.Div([
                dbc.Row([
                    dbc.Col(_section_title(f"Monthly Spend Pacing {app_name}", colors), width=6),
                    dbc.Col(_section_title(f"Monthly Users Pacing {app_name}", colors), width=6),
                ]),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=spend_fig, config=CHART_CONFIG), width=6),
                    dbc.Col(dcc.Graph(figure=users_fig, config=CHART_CONFIG), width=6),
                ]),
            ], style=_card_style(colors)))
            
        return html.Div(rows)

    # -----------------------------------------------------------------
    # TAB 3: CAC BY ENTITY — update on filter change
    # -----------------------------------------------------------------
    @app.callback(
        Output("daedalus-tab3-charts", "children"),
        Input("tab3-load-btn", "n_clicks"),
        [State("tab3-start-date", "date"),
         State("tab3-end-date", "date"),
         State("tab3-metric-checklist", "value")],
        prevent_initial_call=True,
    )
    def update_tab3_charts(n_clicks, start_date, end_date, metrics):
        colors = _colors()
        if not start_date or not end_date or not metrics:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})

        metric_map = {"Daily CAC": "Daily_CAC", "T7D CAC": "T7D_CAC"}
        metric_cols = [metric_map[m] for m in metrics if m in metric_map]

        if not metric_cols:
            return html.Div("Select at least one metric", style={"color": colors["text_secondary"]})

        # Get all unique app names from data
        from app.dashboards.daedalus.data import get_cac_entity_app_names
        all_apps = get_cac_entity_app_names()
        entity_data = get_cac_by_entity(all_apps, start_date, end_date, metric_cols)

        if not entity_data:
            return html.Div("No data", style={"color": colors["text_secondary"]})

        rows = []
        for app_name, app_df in entity_data.items():
            fig = go.Figure()
            for col in metric_cols:
                if col in app_df.columns:
                    label = col.replace("_", " ")
                    dash_style = "solid" if "T7D" in col else "dot"
                    color = "#06B6D4" if "Daily" in col else "#F97316"
                    fig.add_trace(go.Scatter(
                        x=app_df["Date"], y=app_df[col],
                        mode="lines", name=label,
                        line=dict(color=color, width=1.6, dash=dash_style),
                        hovertemplate=f'{label}  $%{{y:,.2f}}<extra></extra>',
                    ))

            fig.update_layout(
                height=300,
                margin=dict(l=60, r=20, t=40, b=40),
                hovermode="x unified",
                paper_bgcolor=colors["card_bg"],
                plot_bgcolor=colors["card_bg"],
                font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
                xaxis=dict(gridcolor=colors["border"], tickformat="%b %Y", hoverformat="%b %d, '%y"),
                yaxis=dict(gridcolor=colors["border"], tickprefix="$"),
                legend=dict(
                    font=dict(color=colors["text_primary"], size=10),
                    bgcolor="rgba(0,0,0,0)",
                    orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
                ),
                showlegend=True,
            )

            rows.append(html.Div([
                _section_title(f"{app_name}", colors),
                dcc.Graph(figure=fig, config=CHART_CONFIG),
            ], style=_card_style(colors)))

        return html.Div(rows)

    # -----------------------------------------------------------------
    # TAB 4: CURRENT SUBSCRIPTIONS — update on filter change
    # -----------------------------------------------------------------
    @app.callback(
        Output("daedalus-tab4-charts", "children"),
        Input("tab4-load-btn", "n_clicks"),
        [State("tab4-app-checklist", "value"),
         State("tab4-channel-checklist", "value"),
         State("tab4-start-date", "date"),
         State("tab4-end-date", "date")],
        prevent_initial_call=True,
    )
    def update_tab4_charts(n_clicks, app_names, channels, start_date, end_date):
        colors = _colors()
        if not app_names or not channels or not start_date or not end_date:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})

        # Convert channels to int if needed
        channels_int = [int(c) if isinstance(c, str) and c.isdigit() else c for c in channels]

        # Chart 1: Portfolio active subs
        portfolio_df = get_portfolio_active_subs(app_names, channels_int, start_date, end_date)
        chart1, c1_s, c1_e, c1_p = build_annotated_line(portfolio_df, "number", theme=THEME,
                                       value_col="Current_Active_Subscription",
                                       name="Current Active Subscriptions")

        # Chart 2: Pivot table
        pivot_df = get_current_subs_pivot(app_names, channels_int, start_date, end_date)
        chart2 = _pivot_grid(pivot_df, colors, "tab4-subs-pivot") if not pivot_df.empty else html.Div("No data")

        # Chart 3: Pie by App (uses end_date as single date)
        pie_app_df = get_pie_by_app(app_names, channels_int, end_date)
        if not pie_app_df.empty:
            chart3 = build_pie_chart(
                pie_app_df["App_Name"].tolist(),
                pie_app_df["Current_Active_Subscription"].tolist(),
                theme=THEME,
            )
        else:
            chart3 = _empty_figure(colors)

        # Chart 4: Pie by App+Channel (uses end_date)
        pie_ac_df = get_pie_by_app_channel(app_names, channels_int, end_date)
        if not pie_ac_df.empty:
            chart4 = build_pie_chart(
                pie_ac_df["Label"].tolist(),
                pie_ac_df["Current_Active_Subscription"].tolist(),
                theme=THEME,
            )
        else:
            chart4 = _empty_figure(colors)

        # Chart 5: Entity active subs (line per app)
        entity_subs_df = get_entity_active_subs(app_names, channels_int, start_date, end_date)
        chart5, _, c5_s, c5_e, c5_p = build_annotated_entity_lines(entity_subs_df, "number", theme=THEME,
                                                  value_col="Current_Active_Subscription")

        # Charts 6-11: Ratio charts (entity + portfolio pairs)
        churn_entity = get_entity_churn(app_names, channels_int, start_date, end_date)
        churn_port = get_portfolio_churn(app_names, channels_int, start_date, end_date)
        chart6, _, c6_s, c6_e, c6_p = build_annotated_entity_lines(churn_entity, "percent", theme=THEME)
        chart7, c7_s, c7_e, c7_p = build_annotated_portfolio_line(churn_port, "percent", theme=THEME, name="Portfolio Churn Rate")

        ss_entity = get_entity_ss(app_names, channels_int, start_date, end_date)
        ss_port = get_portfolio_ss(app_names, channels_int, start_date, end_date)
        chart8, _, c8_s, c8_e, c8_p = build_annotated_entity_lines(ss_entity, "percent", theme=THEME)
        chart9, c9_s, c9_e, c9_p = build_annotated_portfolio_line(ss_port, "percent", theme=THEME, name="Portfolio SS Distribution")

        pend_entity = get_entity_pending(app_names, channels_int, start_date, end_date)
        pend_port = get_portfolio_pending(app_names, channels_int, start_date, end_date)
        chart10, _, c10_s, c10_e, c10_p = build_annotated_entity_lines(pend_entity, "percent", theme=THEME)
        chart11, c11_s, c11_e, c11_p = build_annotated_portfolio_line(pend_port, "percent", theme=THEME, name="Portfolio Pending Subs")

        return html.Div([
            # Chart 1
            html.Div([
                _section_title("Historical Portfolio Current Active Subscriptions", colors),
                _annotation_box(c1_s, c1_e, c1_p, "number", colors),
                dcc.Graph(figure=chart1, config=CHART_CONFIG),
            ], style=_card_style(colors)),

            # Chart 2 — Pivot
            html.Div([
                _section_title("Current Subscriptions", colors),
                chart2,
            ], style=_card_style(colors)),

            # Charts 3-4 — Pie charts
            dbc.Row([
                dbc.Col(html.Div([
                    _section_title("Current Active Subscription by App Name", colors),
                    dcc.Graph(figure=chart3, config=CHART_CONFIG),
                ], style=_card_style(colors)), width=6),
                dbc.Col(html.Div([
                    _section_title("Current Active Subscription by App Name & Channel", colors),
                    dcc.Graph(figure=chart4, config=CHART_CONFIG),
                ], style=_card_style(colors)), width=6),
            ]),

            # Chart 5
            html.Div([
                _section_title("Historical Entity-by-Entity Current Active Subscriptions", colors),
                _annotation_box(c5_s, c5_e, c5_p, "number", colors),
                dcc.Graph(figure=chart5, config=CHART_CONFIG),
            ], style=_card_style(colors)),

            # Charts 6-7
            html.Div([
                _section_title("Historical Daily T30D Entity-by-Entity Churn Rate", colors),
                _annotation_box(c6_s, c6_e, c6_p, "percent", colors),
                dcc.Graph(figure=chart6, config=CHART_CONFIG),
            ], style=_card_style(colors)),
            html.Div([
                _section_title("Historical Daily T30D Portfolio Churn Rate", colors),
                _annotation_box(c7_s, c7_e, c7_p, "percent", colors),
                dcc.Graph(figure=chart7, config=CHART_CONFIG),
            ], style=_card_style(colors)),

            # Charts 8-9
            html.Div([
                _section_title("Historical Daily T30D Entity-by-Entity SS Distribution", colors),
                _annotation_box(c8_s, c8_e, c8_p, "percent", colors),
                dcc.Graph(figure=chart8, config=CHART_CONFIG),
            ], style=_card_style(colors)),
            html.Div([
                _section_title("Historical Daily T30D Portfolio SS Distribution", colors),
                _annotation_box(c9_s, c9_e, c9_p, "percent", colors),
                dcc.Graph(figure=chart9, config=CHART_CONFIG),
            ], style=_card_style(colors)),

            # Charts 10-11
            html.Div([
                _section_title("Historical Daily T30D Entity-by-Entity Pending Subscriptions", colors),
                _annotation_box(c10_s, c10_e, c10_p, "percent", colors),
                dcc.Graph(figure=chart10, config=CHART_CONFIG),
            ], style=_card_style(colors)),
            html.Div([
                _section_title("Historical Daily T30D Portfolio Pending Subscriptions", colors),
                _annotation_box(c11_s, c11_e, c11_p, "percent", colors),
                dcc.Graph(figure=chart11, config=CHART_CONFIG),
            ], style=_card_style(colors)),
        ])

    # -----------------------------------------------------------------
    # TAB 5: DAEDALUS HISTORICAL — update on filter change
    # -----------------------------------------------------------------
    @app.callback(
        Output("daedalus-tab5-charts", "children"),
        Input("tab5-load-btn", "n_clicks"),
        [State("tab5-app-checklist", "value"),
         State("tab5-start-date", "date"),
         State("tab5-end-date", "date")],
        prevent_initial_call=True,
    )
    def update_tab5_charts(n_clicks, app_names, start_date, end_date):
        colors = _colors()
        if not app_names or not start_date or not end_date:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})

        # 6 line charts
        metrics = [
            ("Daily_CAC", "Historical CAC", "dollar"),
            ("T7D_CAC", "Trailing 7 Day CAC", "dollar"),
            ("Daily_Spend", "Historical Spend", "dollar"),
            ("T7D_Spend", "Trailing 7 Day Spend", "dollar"),
            ("Daily_New_Regular_Users", "Historical New Users", "number"),
            ("T7D_Users", "Trailing 7 Day Users", "number"),
        ]

        charts = []
        for i in range(0, len(metrics), 2):
            row_cols = []
            for j in range(2):
                if i + j < len(metrics):
                    col_name, title, fmt = metrics[i + j]
                    df = get_historical_metric_by_app(app_names, start_date, end_date, col_name)
                    fig, _ = build_entity_lines(df, fmt, theme=THEME)
                    row_cols.append(
                        dbc.Col(html.Div([
                            _section_title(title, colors),
                            dcc.Graph(figure=fig, config=CHART_CONFIG),
                        ], style=_card_style(colors)), width=6)
                    )
            charts.append(dbc.Row(row_cols))

        # Pie chart
        pie_df = get_historical_spend_split(app_names, start_date, end_date)
        if not pie_df.empty:
            pie_fig = build_pie_chart(
                pie_df["App_Name"].tolist(),
                pie_df["Daily_Spend"].tolist(),
                theme=THEME,
            )
        else:
            pie_fig = _empty_figure(colors)

        charts.append(html.Div([
            _section_title("Historical Spend Split", colors),
            dcc.Graph(figure=pie_fig, config=CHART_CONFIG),
        ], style=_card_style(colors)))

        return html.Div(charts)

    # -----------------------------------------------------------------
    # REFRESH CALLBACKS
    # -----------------------------------------------------------------
    @app.callback(
        Output("daedalus-refresh-status", "children"),
        [Input("daedalus-refresh-bq-btn", "n_clicks"),
         Input("daedalus-refresh-gcs-btn", "n_clicks")],
        prevent_initial_call=True,
    )
    def handle_daedalus_refresh(bq_clicks, gcs_clicks):
        ctx = callback_context
        if not ctx.triggered:
            return ""
        btn_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if btn_id == "daedalus-refresh-bq-btn":
            ok, msg = refresh_daedalus_bq_to_staging()
            color = "#22C55E" if ok else "#E74C3C"
            return html.Span(msg, style={"color": color, "fontSize": "12px"})
        elif btn_id == "daedalus-refresh-gcs-btn":
            ok, msg = refresh_daedalus_gcs_from_staging()
            color = "#22C55E" if ok else "#E74C3C"
            return html.Span(msg, style={"color": color, "fontSize": "12px"})
        return ""

    # -----------------------------------------------------------------
    # DATEPICKER DARK THEME OVERRIDE (CSS injection approach)
    # -----------------------------------------------------------------
    app.clientside_callback(
        """
        function(tab) {
            var style = document.getElementById('datepicker-dark-override');
            if (!style) {
                style = document.createElement('style');
                style.id = 'datepicker-dark-override';
                style.textContent = `
                    .dash-datepicker-input,
                    .dash-datepicker-input-wrapper,
                    .dash-datepicker,
                    [class*="dash-datepicker"] {
                        background-color: #111111 !important;
                        color: #FFFFFF !important;
                        border-color: #333333 !important;
                    }
                    .CalendarMonth_caption select,
                    [class*="CalendarMonth_caption"] select {
                        background-color: #111111 !important;
                        color: #FFFFFF !important;
                        border: 1px solid #333333 !important;
                    }
                    [class*="dash-datepicker"] th {
                        color: #999999 !important;
                        background-color: #111111 !important;
                    }
                    [class*="dash-datepicker-calendar"] .row,
                    [class*="dash-datepicker-calendar"] div {
                        background-color: #111111 !important;
                    }
                    .dash-dropdown, button.dash-dropdown {
                        background-color: #111111 !important;
                        color: #FFFFFF !important;
                        border-color: #333333 !important;
                    }
                    .dash-dropdown-option, .dash-options-list-option {
                        color: #FFFFFF !important;
                    }
                    .dash-options-list-option:hover {
                        background-color: #333333 !important;
                    }
                    .DateInput, .DateInput input, [class*="DateInput"] input,
                    .SingleDatePickerInput, [class*="SingleDatePickerInput"] {
                        background-color: #111111 !important;
                        color: #FFFFFF !important;
                        border-color: #333333 !important;
                    }
                    .SingleDatePicker_picker, [class*="SingleDatePicker_picker"] {
                        background-color: #111111 !important;
                    }
                    .DayPicker, [class*="DayPicker_"], [class*="DayPicker__"],
                    .DayPicker_transitionContainer, .CalendarMonthGrid,
                    .CalendarMonth, [class*="CalendarMonth_"] {
                        background-color: #111111 !important;
                    }
                    .CalendarDay__default, [class*="CalendarDay__default"] {
                        background-color: #111111 !important;
                        color: #FFFFFF !important;
                        border: 1px solid #222222 !important;
                    }
                    .CalendarDay__default:hover {
                        background-color: #333333 !important;
                    }
                    .CalendarDay__selected, [class*="CalendarDay__selected"] {
                        background-color: #FFFFFF !important;
                        color: #000000 !important;
                        border: 1px solid #FFFFFF !important;
                    }
                    .CalendarDay__blocked_out_of_range, [class*="CalendarDay__blocked"] {
                        color: #333333 !important;
                        background-color: #111111 !important;
                    }
                    .DayPicker_weekHeader small { color: #999999 !important; }
                    .CalendarMonth_caption, .CalendarMonth_caption strong { color: #FFFFFF !important; }
                    [class*="DateInput_fang"], [class*="DayPickerKeyboardShortcuts"] { display: none !important; }
                    [class*="DayPickerNavigation_button"] { background-color: #1A1A1A !important; border: 1px solid #333333 !important; }
                    [class*="DayPickerNavigation_svg"] { fill: #FFFFFF !important; }

                    /* dcc.Dropdown dark theme */
                    .Select-control {
                        background-color: #111111 !important;
                        border-color: #333333 !important;
                        color: #FFFFFF !important;
                    }
                    .Select-value-label, .Select-placeholder {
                        color: #FFFFFF !important;
                    }
                    .Select-input > input {
                        background-color: #111111 !important;
                        color: #FFFFFF !important;
                    }
                    .Select-menu-outer {
                        background-color: #111111 !important;
                        border: 1px solid #333333 !important;
                    }
                    .Select-menu {
                        background-color: #111111 !important;
                    }
                    .Select-option, .VirtualizedSelectOption {
                        background-color: #111111 !important;
                        color: #FFFFFF !important;
                    }
                    .Select-option:hover, .VirtualizedSelectOption:hover,
                    .Select-option.is-focused, .VirtualizedSelectFocusedOption {
                        background-color: #333333 !important;
                        color: #FFFFFF !important;
                    }
                    .Select-option.is-selected, .VirtualizedSelectSelectedOption {
                        background-color: #222222 !important;
                        color: #FFFFFF !important;
                    }
                    .Select-arrow-zone .Select-arrow {
                        border-top-color: #FFFFFF !important;
                    }
                    .Select-clear-zone { color: #999999 !important; }
                    .Select.is-open > .Select-control { background-color: #111111 !important; }
                    .Select-noresults {
                        background-color: #111111 !important;
                        color: #999999 !important;
                    }
                `;
                document.head.appendChild(style);
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('daedalus-dashboard-tabs', 'className'),
        Input('daedalus-dashboard-tabs', 'active_tab')
    )
    # -----------------------------------------------------------------
    # SELECT ALL SYNC CALLBACKS
    # -----------------------------------------------------------------

    # --- Tab 1: App Names ---
    @app.callback(
        Output("tab1-app-checklist", "value"),
        Output("tab1-select-all-apps", "value"),
        Input("tab1-select-all-apps", "value"),
        Input("tab1-app-checklist", "value"),
        State("daedalus-filter-options", "data"),
        prevent_initial_call=True,
    )
    def sync_tab1_apps(select_all, selected, filter_opts):
        trigger = callback_context.triggered_id
        all_apps = filter_opts.get("daedalus_apps", [])
        if trigger == "tab1-select-all-apps":
            if "__all__" in select_all:
                return all_apps, ["__all__"]
            else:
                return [], []
        else:
            if len(selected) == len(all_apps):
                return selected, ["__all__"]
            else:
                return selected, []

    # --- Tab 3: Metrics ---
    @app.callback(
        Output("tab3-metric-checklist", "value"),
        Output("tab3-metric-checklist-select-all", "value"),
        Input("tab3-metric-checklist-select-all", "value"),
        Input("tab3-metric-checklist", "value"),
        prevent_initial_call=True,
    )
    def sync_tab3_metrics(select_all, selected):
        trigger = callback_context.triggered_id
        all_metrics = ["Daily CAC", "T7D CAC"]
        if trigger == "tab3-metric-checklist-select-all":
            if "__all__" in select_all:
                return all_metrics, ["__all__"]
            else:
                return [], []
        else:
            if len(selected) == len(all_metrics):
                return selected, ["__all__"]
            else:
                return selected, []

    # --- Tab 4: App Names ---
    @app.callback(
        Output("tab4-app-checklist", "value"),
        Output("tab4-select-all-apps", "value"),
        Input("tab4-select-all-apps", "value"),
        Input("tab4-app-checklist", "value"),
        State("daedalus-filter-options", "data"),
        prevent_initial_call=True,
    )
    def sync_tab4_apps(select_all, selected, filter_opts):
        trigger = callback_context.triggered_id
        all_apps = filter_opts.get("subs_apps", [])
        if trigger == "tab4-select-all-apps":
            if "__all__" in select_all:
                return all_apps, ["__all__"]
            else:
                return [], []
        else:
            if len(selected) == len(all_apps):
                return selected, ["__all__"]
            else:
                return selected, []

    # --- Tab 4: Channels ---
    @app.callback(
        Output("tab4-channel-checklist", "value"),
        Output("tab4-select-all-channels", "value"),
        Input("tab4-select-all-channels", "value"),
        Input("tab4-channel-checklist", "value"),
        State("daedalus-filter-options", "data"),
        prevent_initial_call=True,
    )
    def sync_tab4_channels(select_all, selected, filter_opts):
        trigger = callback_context.triggered_id
        all_channels = filter_opts.get("subs_channels", [])
        if trigger == "tab4-select-all-channels":
            if "__all__" in select_all:
                return all_channels, ["__all__"]
            else:
                return [], []
        else:
            if len(selected) == len(all_channels):
                return selected, ["__all__"]
            else:
                return selected, []

    # --- Tab 5: App Names ---
    @app.callback(
        Output("tab5-app-checklist", "value"),
        Output("tab5-select-all-apps", "value"),
        Input("tab5-select-all-apps", "value"),
        Input("tab5-app-checklist", "value"),
        State("daedalus-filter-options", "data"),
        prevent_initial_call=True,
    )
    def sync_tab5_apps(select_all, selected, filter_opts):
        trigger = callback_context.triggered_id
        all_apps = filter_opts.get("cac_apps", [])
        if trigger == "tab5-select-all-apps":
            if "__all__" in select_all:
                return all_apps, ["__all__"]
            else:
                return [], []
        else:
            if len(selected) == len(all_apps):
                return selected, ["__all__"]
            else:
                return selected, []

    # =================================================================
    # TABS 6-16: TAB SWITCHING
    # =================================================================
    @app.callback(
        [Output(f"daedalus-tab-{t['id']}-content", "children") for t in [
            {"id": "traffic-channel"}, {"id": "new-users-tc"}, {"id": "spend-tc"},
            {"id": "cac-tc"}, {"id": "afid-unknown"}, {"id": "daily-report"},
            {"id": "mtd-report"}, {"id": "approval-rates"}, {"id": "decline-app"},
            {"id": "decline-channel"}, {"id": "decline-afid"},
        ]],
        Input("daedalus-dashboard-tabs", "active_tab"),
        State("daedalus-filter-options", "data"),
    )
    def render_active_tab_6_16(active_tab, filter_opts):
        colors = _colors()
        empty = html.Div()
        outputs = [empty] * 11

        tab_map = {
            "traffic-channel": 0, "new-users-tc": 1, "spend-tc": 2,
            "cac-tc": 3, "afid-unknown": 4, "daily-report": 5,
            "mtd-report": 6, "approval-rates": 7, "decline-app": 8,
            "decline-channel": 9, "decline-afid": 10,
        }
        idx = tab_map.get(active_tab)
        if idx is None:
            return outputs

        builders = {
            "traffic-channel": _build_tab6,
            "new-users-tc": _build_tab7,
            "spend-tc": _build_tab8,
            "cac-tc": _build_tab9,
            "afid-unknown": _build_tab10,
            "daily-report": _build_tab11,
            "mtd-report": _build_tab12,
            "approval-rates": _build_tab13,
            "decline-app": _build_tab14,
            "decline-channel": _build_tab15,
            "decline-afid": _build_tab16,
        }
        builder = builders.get(active_tab)
        if builder:
            outputs[idx] = builder(colors, filter_opts)
        return outputs

    # =================================================================
    # TABS 6-16: DATA LOADING CALLBACKS
    # =================================================================

    # --- Tab 6: Traffic Channel ---
    @app.callback(
        Output("daedalus-tab6-charts", "children"),
        Input("tab6-load-btn", "n_clicks"),
        [State("tab6-start-date", "date"),
         State("tab6-end-date", "date"),
         State("tab6-tc-checklist", "value")],
        prevent_initial_call=True,
    )
    def update_tab6_charts(n_clicks, start_date, end_date, channels):
        colors = _colors()
        if not start_date or not end_date or not channels:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})
        channels_int = [int(c) for c in channels]

        spend_data = get_tc_lines_by_app(start_date, end_date, channels_int, "T30D_Spend")
        users_data = get_tc_lines_by_app(start_date, end_date, channels_int, "T30D_Users")

        if not spend_data and not users_data:
            return html.Div("No data", style={"color": colors["text_secondary"]})

        rows = []
        all_apps = list(dict.fromkeys(list(spend_data.keys()) + list(users_data.keys())))
        for app_name in all_apps:
            s_fig, _ = build_tc_multi_lines(spend_data.get(app_name), "dollar", theme=THEME)
            u_fig, _ = build_tc_multi_lines(users_data.get(app_name), "number", theme=THEME)
            rows.append(html.Div([
                dbc.Row([
                    dbc.Col(_section_title(f"T30D {app_name} Spent by Traffic Channel", colors), width=6),
                    dbc.Col(_section_title(f"T30D {app_name} New Users by Traffic Channel", colors), width=6),
                ]),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=s_fig, config=CHART_CONFIG), width=6),
                    dbc.Col(dcc.Graph(figure=u_fig, config=CHART_CONFIG), width=6),
                ]),
            ], style=_card_style(colors)))
        return html.Div(rows)

    # --- Tab 7: New Users - Traffic Channel ---
    @app.callback(
        Output("daedalus-tab7-charts", "children"),
        Input("tab7-load-btn", "n_clicks"),
        [State("tab7-start-date", "date"),
         State("tab7-end-date", "date"),
         State("tab7-tc-checklist", "value")],
        prevent_initial_call=True,
    )
    def update_tab7_charts(n_clicks, start_date, end_date, channels):
        colors = _colors()
        if not start_date or not end_date or not channels:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})
        channels_int = [int(c) for c in channels]

        pie_data = get_tc_pie_by_app(start_date, end_date, channels_int, "Daily_New_Users")
        stacked_data = get_tc_stacked_by_app(start_date, end_date, channels_int, "Daily_New_Users")

        if not pie_data and not stacked_data:
            return html.Div("No data", style={"color": colors["text_secondary"]})

        rows = []
        all_apps = list(dict.fromkeys(list(pie_data.keys()) + list(stacked_data.keys())))
        for app_name in all_apps:
            pie_fig = build_tc_pie(pie_data.get(app_name), theme=THEME)
            area_fig = build_stacked_area(stacked_data.get(app_name), "number", theme=THEME,
                                          group_col="Traffic_Channel", use_channel_labels=True)
            rows.append(html.Div([
                dbc.Row([
                    dbc.Col(_section_title(f"{app_name} New Users by Traffic Channel", colors), width=6),
                    dbc.Col(_section_title(f"{app_name} New Users Distribution", colors), width=6),
                ]),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=pie_fig, config=CHART_CONFIG), width=6),
                    dbc.Col(dcc.Graph(figure=area_fig, config=CHART_CONFIG), width=6),
                ]),
            ], style=_card_style(colors)))
        return html.Div(rows)

    # --- Tab 8: Spend - Traffic Channel ---
    @app.callback(
        Output("daedalus-tab8-charts", "children"),
        Input("tab8-load-btn", "n_clicks"),
        [State("tab8-start-date", "date"),
         State("tab8-end-date", "date"),
         State("tab8-tc-checklist", "value")],
        prevent_initial_call=True,
    )
    def update_tab8_charts(n_clicks, start_date, end_date, channels):
        colors = _colors()
        if not start_date or not end_date or not channels:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})
        channels_int = [int(c) for c in channels]

        pie_data = get_tc_pie_by_app(start_date, end_date, channels_int, "Daily_Spend")
        stacked_data = get_tc_stacked_by_app(start_date, end_date, channels_int, "Daily_Spend")

        if not pie_data and not stacked_data:
            return html.Div("No data", style={"color": colors["text_secondary"]})

        rows = []
        all_apps = list(dict.fromkeys(list(pie_data.keys()) + list(stacked_data.keys())))
        for app_name in all_apps:
            pie_fig = build_tc_pie(pie_data.get(app_name), theme=THEME)
            area_fig = build_stacked_area(stacked_data.get(app_name), "dollar", theme=THEME,
                                          group_col="Traffic_Channel", use_channel_labels=True)
            rows.append(html.Div([
                dbc.Row([
                    dbc.Col(_section_title(f"{app_name} Spend by Traffic Channel", colors), width=6),
                    dbc.Col(_section_title(f"{app_name} Spend Distribution", colors), width=6),
                ]),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=pie_fig, config=CHART_CONFIG), width=6),
                    dbc.Col(dcc.Graph(figure=area_fig, config=CHART_CONFIG), width=6),
                ]),
            ], style=_card_style(colors)))
        return html.Div(rows)

    # --- Tab 9: CAC - Traffic Channel ---
    @app.callback(
        Output("daedalus-tab9-charts", "children"),
        Input("tab9-load-btn", "n_clicks"),
        [State("tab9-start-date", "date"),
         State("tab9-end-date", "date"),
         State("tab9-tc-checklist", "value"),
         State("tab9-metric-checklist", "value")],
        prevent_initial_call=True,
    )
    def update_tab9_charts(n_clicks, start_date, end_date, channels, metrics):
        colors = _colors()
        if not start_date or not end_date or not channels or not metrics:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})
        channels_int = [int(c) for c in channels]

        data = get_cac_tc_by_app(start_date, end_date, channels_int, metrics)
        if not data:
            return html.Div("No data", style={"color": colors["text_secondary"]})

        rows = []
        for app_name, app_df in data.items():
            fig, _ = build_cac_tc_lines(app_df, metrics, theme=THEME)
            rows.append(html.Div([
                _section_title(f"{app_name} CAC by Traffic Channel", colors),
                dcc.Graph(figure=fig, config=CHART_CONFIG),
            ], style=_card_style(colors)))
        return html.Div(rows)

    # --- Tab 10: AFID Unknown ---
    @app.callback(
        Output("daedalus-tab10-charts", "children"),
        Input("tab10-load-btn", "n_clicks"),
        [State("tab10-start-date", "date"),
         State("tab10-end-date", "date"),
         State("tab10-app-checklist", "value"),
         State("tab10-afid-checklist", "value")],
        prevent_initial_call=True,
    )
    def update_tab10_charts(n_clicks, start_date, end_date, app_names, afids):
        colors = _colors()
        if not start_date or not end_date or not app_names or not afids:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})

        pie_df = get_afid_unknown_pie(app_names, afids, start_date, end_date)
        stacked_df = get_afid_unknown_stacked(app_names, afids, start_date, end_date)

        pie_fig = _empty_figure(colors)
        area_fig = _empty_figure(colors)

        if not pie_df.empty:
            pie_fig = build_tc_pie.__wrapped__(pie_df, theme=THEME) if hasattr(build_tc_pie, '__wrapped__') else _empty_figure(colors)
            # Use generic pie for AFID
            from app.dashboards.daedalus.charts import get_theme_colors as _gtc
            pie_fig = build_pie_chart(
                pie_df["AFID"].tolist(),
                pie_df["New_Users"].tolist(),
                theme=THEME,
            )
        if not stacked_df.empty:
            stacked_df = stacked_df.rename(columns={"New_Users": "value"})
            area_fig = build_stacked_area(stacked_df, "number", theme=THEME,
                                          group_col="AFID", use_channel_labels=False)

        return html.Div([
            html.Div([
                dbc.Row([
                    dbc.Col(_section_title("T30D New Users By Traffic Channel 99", colors), width=6),
                    dbc.Col(_section_title("T30D New Users Distribution - Traffic Channel 99", colors), width=6),
                ]),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=pie_fig, config=CHART_CONFIG), width=6),
                    dbc.Col(dcc.Graph(figure=area_fig, config=CHART_CONFIG), width=6),
                ]),
            ], style=_card_style(colors)),
        ])

    # --- Tab 11: Daily Report ---
    @app.callback(
        Output("daedalus-tab11-charts", "children"),
        Input("tab11-load-btn", "n_clicks"),
        [State("tab11-entity-checklist", "value"),
         State("tab11-app-checklist", "value"),
         State("tab11-date-picker", "date")],
        prevent_initial_call=True,
    )
    def update_tab11_charts(n_clicks, entity_names, app_names, selected_date):
        colors = _colors()
        if not selected_date:
            return html.Div("Select a date", style={"color": colors["text_secondary"]})

        entity_df = get_cpa_by_entity_daily(selected_date)
        app_df = get_cpa_by_application_daily(selected_date,
                                               entity_names or [],
                                               app_names or [])

        sections = []
        sections.append(html.Div([
            _section_title("CPA By Entity", colors),
            _build_report_grid(entity_df, colors, "tab11-entity-grid"),
        ], style=_card_style(colors)))

        sections.append(html.Div([
            _section_title("CPA By Application", colors),
            _build_report_grid(app_df, colors, "tab11-app-grid"),
        ], style=_card_style(colors)))

        return html.Div(sections)

    # --- Tab 12: MTD Report ---
    @app.callback(
        Output("daedalus-tab12-charts", "children"),
        Input("tab12-load-btn", "n_clicks"),
        [State("tab12-entity-checklist", "value"),
         State("tab12-app-checklist", "value"),
         State("tab12-date-picker", "date")],
        prevent_initial_call=True,
    )
    def update_tab12_charts(n_clicks, entity_names, app_names, selected_date):
        colors = _colors()
        if not selected_date:
            return html.Div("Select a date", style={"color": colors["text_secondary"]})

        entity_df = get_cpa_by_entity_mtd(selected_date)
        app_df = get_cpa_by_application_mtd(selected_date,
                                             entity_names or [],
                                             app_names or [])

        sections = []
        sections.append(html.Div([
            _section_title("CPA By Entity (MTD)", colors),
            _build_report_grid(entity_df, colors, "tab12-entity-grid"),
        ], style=_card_style(colors)))

        sections.append(html.Div([
            _section_title("CPA By Application (MTD)", colors),
            _build_report_grid(app_df, colors, "tab12-app-grid"),
        ], style=_card_style(colors)))

        return html.Div(sections)

    # --- Tab 13: Approval Rates ---
    @app.callback(
        Output("daedalus-tab13-charts", "children"),
        Input("tab13-load-btn", "n_clicks"),
        [State("tab13-start-date", "date"),
         State("tab13-end-date", "date"),
         State("tab13-app-checklist", "value"),
         State("tab13-channel-checklist", "value"),
         State("tab13-afid-checklist", "value")],
        prevent_initial_call=True,
    )
    def update_tab13_charts(n_clicks, start_date, end_date, app_names, channel_names, afids):
        colors = _colors()
        if not start_date or not end_date or not app_names:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})

        sections = []
        # Chart 1: App Approval Rates
        app_data = get_app_approval_rates(app_names, start_date, end_date)
        if app_data:
            fig1 = build_dual_axis_approval(
                app_data.get("per_app"), app_data.get("total"),
                "App_Name", theme=THEME)
            sections.append(html.Div([
                _section_title("App Approval Rates", colors),
                dcc.Graph(figure=fig1, config=CHART_CONFIG),
            ], style=_card_style(colors)))

        # Chart 2: Channel Approval Rates
        if channel_names:
            ch_data = get_channel_approval_rates(app_names, channel_names, start_date, end_date)
            if ch_data:
                fig2 = build_dual_axis_approval(
                    ch_data.get("per_channel"), ch_data.get("total"),
                    "Channel_Name", theme=THEME)
                sections.append(html.Div([
                    _section_title("Traffic Channel Approval Rates", colors),
                    dcc.Graph(figure=fig2, config=CHART_CONFIG),
                ], style=_card_style(colors)))

        # Chart 3: AFID Approval Rates
        if afids:
            afid_data = get_afid_approval_rates(app_names, afids, start_date, end_date)
            if afid_data:
                fig3 = build_dual_axis_approval(
                    afid_data.get("per_afid"), afid_data.get("total"),
                    "AFID", theme=THEME)
                sections.append(html.Div([
                    _section_title("AFID Approval Rates", colors),
                    dcc.Graph(figure=fig3, config=CHART_CONFIG),
                ], style=_card_style(colors)))

        if not sections:
            return html.Div("No data", style={"color": colors["text_secondary"]})
        return html.Div(sections)

    # --- Tab 14: Decline Reason % - App ---
    @app.callback(
        Output("daedalus-tab14-charts", "children"),
        Input("tab14-load-btn", "n_clicks"),
        [State("tab14-start-date", "date"),
         State("tab14-end-date", "date"),
         State("tab14-app-checklist", "value"),
         State("tab14-threshold", "value")],
        prevent_initial_call=True,
    )
    def update_tab14_charts(n_clicks, start_date, end_date, app_names, threshold):
        colors = _colors()
        if not start_date or not end_date or not app_names:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})
        threshold = float(threshold) if threshold else 0
        data = get_decline_app_data(app_names, start_date, end_date, threshold)
        return _build_decline_charts(data, colors)

    # --- Tab 15: Decline Reason % - Channel ---
    @app.callback(
        Output("daedalus-tab15-charts", "children"),
        Input("tab15-load-btn", "n_clicks"),
        [State("tab15-start-date", "date"),
         State("tab15-end-date", "date"),
         State("tab15-app-checklist", "value"),
         State("tab15-channel-checklist", "value"),
         State("tab15-threshold", "value")],
        prevent_initial_call=True,
    )
    def update_tab15_charts(n_clicks, start_date, end_date, app_names, channel_names, threshold):
        colors = _colors()
        if not start_date or not end_date or not app_names:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})
        threshold = float(threshold) if threshold else 0
        data = get_decline_channel_data(app_names, channel_names or [], start_date, end_date, threshold)
        return _build_decline_charts(data, colors)

    # --- Tab 16: Decline Reason % - AFID ---
    @app.callback(
        Output("daedalus-tab16-charts", "children"),
        Input("tab16-load-btn", "n_clicks"),
        [State("tab16-start-date", "date"),
         State("tab16-end-date", "date"),
         State("tab16-app-checklist", "value"),
         State("tab16-channel-checklist", "value"),
         State("tab16-afid-checklist", "value"),
         State("tab16-threshold", "value")],
        prevent_initial_call=True,
    )
    def update_tab16_charts(n_clicks, start_date, end_date, app_names, channel_names, afids, threshold):
        colors = _colors()
        if not start_date or not end_date or not app_names:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})
        threshold = float(threshold) if threshold else 0
        data = get_decline_afid_data(app_names, channel_names or [], afids or [],
                                      start_date, end_date, threshold)
        return _build_decline_charts(data, colors)

    # =================================================================
    # TABS 6-16: SELECT ALL SYNC CALLBACKS
    # =================================================================

    def _register_select_all(checklist_id, select_all_id, all_items_key):
        """Factory to register a Select All ↔ checklist sync callback"""
        @app.callback(
            Output(checklist_id, "value"),
            Output(select_all_id, "value"),
            Input(select_all_id, "value"),
            Input(checklist_id, "value"),
            State("daedalus-filter-options", "data"),
            prevent_initial_call=True,
        )
        def sync(select_all, selected, filter_opts):
            trigger = callback_context.triggered_id
            all_items = filter_opts.get(all_items_key, [])
            if trigger == select_all_id:
                if "__all__" in select_all:
                    return all_items, ["__all__"]
                else:
                    return [], []
            else:
                if len(selected) == len(all_items) and len(all_items) > 0:
                    return selected, ["__all__"]
                else:
                    return selected, []

    # Tabs 6/7/8: Traffic Channel
    _register_select_all("tab6-tc-checklist", "tab6-select-all-tc", "tc_channels")
    _register_select_all("tab7-tc-checklist", "tab7-select-all-tc", "tc_channels")
    _register_select_all("tab8-tc-checklist", "tab8-select-all-tc", "tc_channels")

    # Tab 9: Traffic Channel + Metrics
    _register_select_all("tab9-tc-checklist", "tab9-select-all-tc", "cac_tc_channels")

    @app.callback(
        Output("tab9-metric-checklist", "value"),
        Output("tab9-metric-select-all", "value"),
        Input("tab9-metric-select-all", "value"),
        Input("tab9-metric-checklist", "value"),
        prevent_initial_call=True,
    )
    def sync_tab9_metrics(select_all, selected):
        trigger = callback_context.triggered_id
        all_metrics = ["Daily_CAC", "T7D_CAC"]
        if trigger == "tab9-metric-select-all":
            if "__all__" in select_all:
                return all_metrics, ["__all__"]
            else:
                return [], []
        else:
            if len(selected) == len(all_metrics):
                return selected, ["__all__"]
            else:
                return selected, []

    # Tab 10: App + AFID
    _register_select_all("tab10-app-checklist", "tab10-select-all-apps", "au_apps")
    _register_select_all("tab10-afid-checklist", "tab10-select-all-afids", "au_afids")

    # Tab 11: Entity + App
    _register_select_all("tab11-entity-checklist", "tab11-select-all-entities", "cpa_entity_names")
    _register_select_all("tab11-app-checklist", "tab11-select-all-apps", "cpa_app_names")

    # Tab 12: Entity + App
    _register_select_all("tab12-entity-checklist", "tab12-select-all-entities", "cpa_mtd_entity_names")
    _register_select_all("tab12-app-checklist", "tab12-select-all-apps", "cpa_app_names")

    # Tab 13: App + Channel + AFID
    _register_select_all("tab13-app-checklist", "tab13-select-all-apps", "ap_apps")
    _register_select_all("tab13-channel-checklist", "tab13-select-all-channels", "ap_channels")
    _register_select_all("tab13-afid-checklist", "tab13-select-all-afids", "ap_afids")

    # Tab 14: App
    _register_select_all("tab14-app-checklist", "tab14-select-all-apps", "da_apps")

    # Tab 15: App + Channel
    _register_select_all("tab15-app-checklist", "tab15-select-all-apps", "da_apps")
    _register_select_all("tab15-channel-checklist", "tab15-select-all-channels", "dc_channels")

    # Tab 16: App + Channel + AFID
    _register_select_all("tab16-app-checklist", "tab16-select-all-apps", "da_apps")
    _register_select_all("tab16-channel-checklist", "tab16-select-all-channels", "dc_channels")
    _register_select_all("tab16-afid-checklist", "tab16-select-all-afids", "daf_afids")
# =============================================================================
# TAB CONTENT BUILDERS (called from render_active_tab)
# =============================================================================

def _build_tab1(colors, filter_opts):
    """Build Tab 1 initial layout with filters + chart container"""
    apps = filter_opts.get("daedalus_apps", [])
    months = filter_opts.get("month_options", [])
    d_max = filter_opts.get("d_max", str(date.today()))
    default_month = months[0]["value"] if months else f"{date.today().year}-{date.today().month:02d}"

    return html.Div([
        # Filters
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_app_checklist(apps, "tab1", colors), width=6),
                    dbc.Col(_build_date_picker("tab1-date-picker", filter_opts.get("d_min"),
                                                filter_opts.get("d_max"), d_max, "Date (Pivots & Bars)", colors), width=3),
                    dbc.Col(_build_month_selector(months, default_month, "tab1", colors), width=3),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),

        # Load Data button
        html.Div([
            dbc.Button("Load Data", id="tab1-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),

        # Charts container (updated by callback)
        dcc.Loading(html.Div(id="daedalus-tab1-charts"), type="dot", color="#FFFFFF"),
    ])


def _build_tab2(colors, filter_opts):
    """Build Tab 2 with month filter + chart container"""
    months = filter_opts.get("month_options", [])
    default_month = months[0]["value"] if months else f"{date.today().year}-{date.today().month:02d}"

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_month_selector(months, default_month, "tab2", colors), width=3),
                ]),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),

        html.Div([
            dbc.Button("Load Data", id="tab2-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),

        dcc.Loading(html.Div(id="daedalus-tab2-charts"), type="dot", color="#FFFFFF"),
    ])


def _build_tab3(colors, filter_opts):
    """Build Tab 3 with start/end date + metric checklist"""
    ce_min = filter_opts.get("ce_min", str(date.today() - timedelta(days=90)))
    ce_max = filter_opts.get("ce_max", str(date.today()))

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_date_picker("tab3-start-date", ce_min, ce_max, ce_min, "Start Date", colors), width=3),
                    dbc.Col(_build_date_picker("tab3-end-date", ce_min, ce_max, ce_max, "End Date", colors), width=3),
                    dbc.Col(_build_metric_checklist(
                        ["Daily CAC", "T7D CAC"], "tab3-metric-checklist", colors
                    ), width=6),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),

        html.Div([
            dbc.Button("Load Data", id="tab3-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),

        dcc.Loading(html.Div(id="daedalus-tab3-charts"), type="dot", color="#FFFFFF"),
    ])


def _build_tab4(colors, filter_opts):
    """Build Tab 4 with app, channel, start/end date filters"""
    subs_apps = filter_opts.get("subs_apps", [])
    subs_channels = filter_opts.get("subs_channels", [])
    as_min = filter_opts.get("as_min", str(date.today() - timedelta(days=90)))
    as_max = filter_opts.get("as_max", str(date.today()))

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_app_checklist(subs_apps, "tab4", colors), width=4),
                    dbc.Col(html.Div([
                        html.Div("Traffic Channel", style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
                        dbc.Checklist(
                            options=[{"label": "Select All", "value": "__all__"}],
                            value=["__all__"],
                            id="tab4-select-all-channels",
                            inline=True,
                            className="daedalus-checkbox",
                            style={"fontSize": "12px", "fontWeight": "600", "marginBottom": "4px"},
                        ),
                        dbc.Checklist(
                            options=[{"label": str(c), "value": str(c)} for c in subs_channels],
                            value=subs_channels,
                            id="tab4-channel-checklist",
                            inline=True,
                            className="daedalus-checkbox",
                            style={"fontSize": "12px"},
                        ),
                    ]), width=4),
                    dbc.Col([
                        dbc.Row([
                            dbc.Col(_build_date_picker("tab4-start-date", as_min, as_max, as_min, "Start Date", colors), width=6),
                            dbc.Col(_build_date_picker("tab4-end-date", as_min, as_max, as_max, "End Date", colors), width=6),
                        ]),
                    ], width=4),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),

        html.Div([
            dbc.Button("Load Data", id="tab4-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),

        dcc.Loading(html.Div(id="daedalus-tab4-charts"), type="dot", color="#FFFFFF"),
    ])


def _build_tab5(colors, filter_opts):
    """Build Tab 5 with app checklist + date range"""
    cac_apps = filter_opts.get("cac_apps", [])
    ce_min = filter_opts.get("ce_min", str(date.today() - timedelta(days=90)))
    ce_max = filter_opts.get("ce_max", str(date.today()))

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_app_checklist(cac_apps, "tab5", colors), width=6),
                    dbc.Col(_build_date_picker("tab5-start-date", ce_min, ce_max, ce_min, "Start Date", colors), width=3),
                    dbc.Col(_build_date_picker("tab5-end-date", ce_min, ce_max, ce_max, "End Date", colors), width=3),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),

        html.Div([
            dbc.Button("Load Data", id="tab5-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),

        dcc.Loading(html.Div(id="daedalus-tab5-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# HELPER: Traffic Channel filter bar (reused by tabs 6/7/8)
# =============================================================================

def _build_tc_filters(tab_prefix, filter_opts, colors):
    """Build start date + end date + traffic channel checklist for tabs 6/7/8"""
    tc_min = filter_opts.get("tc_min", str(date.today() - timedelta(days=90)))
    tc_max = filter_opts.get("tc_max", str(date.today()))
    tc_channels = filter_opts.get("tc_channels", [])

    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col(_build_date_picker(f"{tab_prefix}-start-date", tc_min, tc_max, tc_min,
                                            "Start Date", colors), width=2),
                dbc.Col(_build_date_picker(f"{tab_prefix}-end-date", tc_min, tc_max, tc_max,
                                            "End Date", colors), width=2),
                dbc.Col(_build_checklist_filter(
                    tc_channels, f"{tab_prefix}-tc-checklist", f"{tab_prefix}-select-all-tc",
                    "Traffic Channel", colors,
                    label_fn=lambda c: get_channel_label(int(c)),
                ), width=8),
            ], align="end"),
        ])
    ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
               "marginBottom": "16px"})


# =============================================================================
# HELPER: Generic checklist with Select All
# =============================================================================

def _build_checklist_filter(items, checklist_id, select_all_id, label, colors,
                             default_all=True, label_fn=None):
    """Build a labelled checklist with Select All toggle"""
    options = [{"label": label_fn(i) if label_fn else str(i), "value": str(i)} for i in items]
    values = [str(i) for i in items] if default_all else []

    return html.Div([
        html.Div(label, style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
        dbc.Checklist(
            options=[{"label": "Select All", "value": "__all__"}],
            value=["__all__"] if default_all else [],
            id=select_all_id,
            inline=True,
            className="daedalus-checkbox",
            style={"fontSize": "12px", "fontWeight": "600", "marginBottom": "4px"},
        ),
        dbc.Checklist(
            options=options,
            value=values,
            id=checklist_id,
            inline=True,
            className="daedalus-checkbox",
            style={"fontSize": "12px"},
        ),
    ])


# =============================================================================
# HELPER: AG Grid for CPA report tables (Tabs 11/12)
# =============================================================================

def _build_report_grid(df, colors, grid_id):
    """Build AG Grid for CPA report tables with $ formatting"""
    if df is None or df.empty:
        return html.Div("No data", style={"color": colors["text_secondary"]})

    columns = df.columns.tolist()
    col_defs = []
    dollar_cols = {"AD Spend", "CAC"}
    int_cols = {"Total", "Trials", "New Subscriptions", "Single Sale"}

    for col in columns:
        cd = {"headerName": col, "field": col, "sortable": True, "filter": True}
        if col in {"Entity", "App", "Source System"}:
            cd["pinned"] = "left" if col == "Entity" else None
            cd["width"] = 150
        elif col in dollar_cols:
            cd["width"] = 140
            cd["type"] = "rightAligned"
            cd["valueFormatter"] = {
                "function": "params.value != null && typeof params.value === 'number' ? '$ ' + params.value.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : ''"
            }
        elif col in int_cols:
            cd["width"] = 140
            cd["type"] = "rightAligned"
            cd["valueFormatter"] = {
                "function": "params.value != null && typeof params.value === 'number' ? params.value.toLocaleString('en-US') : ''"
            }
        else:
            cd["width"] = 140
        col_defs.append(cd)

    return dag.AgGrid(
        id=grid_id,
        columnDefs=col_defs,
        rowData=df.to_dict("records"),
        defaultColDef={"resizable": True},
        dashGridOptions={"domLayout": "autoHeight"},
        style={"width": "100%"},
        className="ag-theme-alpine-dark",
    )


# =============================================================================
# HELPER: Decline charts (reused by tabs 14/15/16)
# =============================================================================

def _build_decline_charts(data, colors):
    """Build CIT + MIT stacked bar charts from decline data"""
    if not data:
        return html.Div("No data", style={"color": colors["text_secondary"]})

    sections = []
    if "cit" in data and not data["cit"].empty:
        fig_cit = build_stacked_bar_100(data["cit"], theme=THEME)
        sections.append(html.Div([
            _section_title("CIT Decline Reason % (All)", colors),
            dcc.Graph(figure=fig_cit, config=CHART_CONFIG),
        ], style=_card_style(colors)))

    if "mit" in data and not data["mit"].empty:
        fig_mit = build_stacked_bar_100(data["mit"], theme=THEME)
        sections.append(html.Div([
            _section_title("MIT Decline Reason % (All)", colors),
            dcc.Graph(figure=fig_mit, config=CHART_CONFIG),
        ], style=_card_style(colors)))

    if not sections:
        return html.Div("No data", style={"color": colors["text_secondary"]})
    return html.Div(sections)


# =============================================================================
# TAB 6: TRAFFIC CHANNEL — Filter UI
# =============================================================================

def _build_tab6(colors, filter_opts):
    return html.Div([
        _build_tc_filters("tab6", filter_opts, colors),
        html.Div([
            dbc.Button("Load Data", id="tab6-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab6-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 7: NEW USERS - TRAFFIC CHANNEL — Filter UI
# =============================================================================

def _build_tab7(colors, filter_opts):
    return html.Div([
        _build_tc_filters("tab7", filter_opts, colors),
        html.Div([
            dbc.Button("Load Data", id="tab7-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab7-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 8: SPEND - TRAFFIC CHANNEL — Filter UI
# =============================================================================

def _build_tab8(colors, filter_opts):
    return html.Div([
        _build_tc_filters("tab8", filter_opts, colors),
        html.Div([
            dbc.Button("Load Data", id="tab8-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab8-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 9: CAC - TRAFFIC CHANNEL — Filter UI
# =============================================================================

def _build_tab9(colors, filter_opts):
    cac_tc_min = filter_opts.get("cac_tc_min", str(date.today() - timedelta(days=90)))
    cac_tc_max = filter_opts.get("cac_tc_max", str(date.today()))
    cac_tc_channels = filter_opts.get("cac_tc_channels", [])

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_date_picker("tab9-start-date", cac_tc_min, cac_tc_max,
                                                cac_tc_min, "Start Date", colors), width=2),
                    dbc.Col(_build_date_picker("tab9-end-date", cac_tc_min, cac_tc_max,
                                                cac_tc_max, "End Date", colors), width=2),
                    dbc.Col(_build_checklist_filter(
                        cac_tc_channels, "tab9-tc-checklist", "tab9-select-all-tc",
                        "Traffic Channel", colors,
                        label_fn=lambda c: get_channel_label(int(c)),
                    ), width=5),
                    dbc.Col(html.Div([
                        html.Div("Metric", style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
                        dbc.Checklist(
                            options=[{"label": "Select All", "value": "__all__"}],
                            value=["__all__"],
                            id="tab9-metric-select-all",
                            inline=True,
                            className="daedalus-checkbox",
                            style={"fontSize": "12px", "fontWeight": "600", "marginBottom": "4px"},
                        ),
                        dbc.Checklist(
                            options=[{"label": "Daily CAC", "value": "Daily_CAC"},
                                     {"label": "T7D CAC", "value": "T7D_CAC"}],
                            value=["Daily_CAC", "T7D_CAC"],
                            id="tab9-metric-checklist",
                            inline=True,
                            className="daedalus-checkbox",
                            style={"fontSize": "12px"},
                        ),
                    ]), width=3),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),
        html.Div([
            dbc.Button("Load Data", id="tab9-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab9-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 10: AFID UNKNOWN — Filter UI
# =============================================================================

def _build_tab10(colors, filter_opts):
    au_min = filter_opts.get("au_min", str(date.today() - timedelta(days=90)))
    au_max = filter_opts.get("au_max", str(date.today()))
    au_apps = filter_opts.get("au_apps", [])
    au_afids = filter_opts.get("au_afids", [])

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_date_picker("tab10-start-date", au_min, au_max, au_min,
                                                "Start Date", colors), width=2),
                    dbc.Col(_build_date_picker("tab10-end-date", au_min, au_max, au_max,
                                                "End Date", colors), width=2),
                    dbc.Col(_build_checklist_filter(
                        au_apps, "tab10-app-checklist", "tab10-select-all-apps",
                        "App Name", colors), width=3),
                    dbc.Col(_build_checklist_filter(
                        au_afids, "tab10-afid-checklist", "tab10-select-all-afids",
                        "AFID", colors), width=5),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),
        html.Div([
            dbc.Button("Load Data", id="tab10-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab10-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 11: DAILY REPORT — Filter UI
# =============================================================================

def _build_tab11(colors, filter_opts):
    entity_names = filter_opts.get("cpa_entity_names", [])
    app_names = filter_opts.get("cpa_app_names", [])
    cpa_dates = filter_opts.get("cpa_dates", [])
    default_date = cpa_dates[0] if cpa_dates else str(date.today())

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_checklist_filter(
                        entity_names, "tab11-entity-checklist", "tab11-select-all-entities",
                        "Entity Name", colors), width=4),
                    dbc.Col(_build_checklist_filter(
                        app_names, "tab11-app-checklist", "tab11-select-all-apps",
                        "App Name", colors), width=4),
                    dbc.Col(_build_date_picker("tab11-date-picker",
                                                cpa_dates[-1] if cpa_dates else str(date.today()),
                                                cpa_dates[0] if cpa_dates else str(date.today()),
                                                default_date, "Date", colors), width=4),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),
        html.Div([
            dbc.Button("Load Data", id="tab11-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab11-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 12: MTD REPORT — Filter UI
# =============================================================================

def _build_tab12(colors, filter_opts):
    entity_names = filter_opts.get("cpa_mtd_entity_names", [])
    app_names = filter_opts.get("cpa_app_names", [])
    mtd_dates = filter_opts.get("cpa_mtd_dates", [])
    default_date = mtd_dates[0] if mtd_dates else str(date.today())

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_checklist_filter(
                        entity_names, "tab12-entity-checklist", "tab12-select-all-entities",
                        "Entity Name", colors), width=4),
                    dbc.Col(_build_checklist_filter(
                        app_names, "tab12-app-checklist", "tab12-select-all-apps",
                        "App Name", colors), width=4),
                    dbc.Col(_build_date_picker("tab12-date-picker",
                                                mtd_dates[-1] if mtd_dates else str(date.today()),
                                                mtd_dates[0] if mtd_dates else str(date.today()),
                                                default_date, "Date", colors), width=4),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),
        html.Div([
            dbc.Button("Load Data", id="tab12-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab12-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 13: APPROVAL RATES — Filter UI
# =============================================================================

def _build_tab13(colors, filter_opts):
    ap_min = filter_opts.get("ap_min", str(date.today() - timedelta(days=90)))
    ap_max = filter_opts.get("ap_max", str(date.today()))
    ap_apps = filter_opts.get("ap_apps", [])
    ap_channels = filter_opts.get("ap_channels", [])
    ap_afids = filter_opts.get("ap_afids", [])

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(_build_date_picker("tab13-start-date", ap_min, ap_max, ap_min,
                                                "Start Date", colors), width=2),
                    dbc.Col(_build_date_picker("tab13-end-date", ap_min, ap_max, ap_max,
                                                "End Date", colors), width=2),
                    dbc.Col(_build_checklist_filter(
                        ap_apps, "tab13-app-checklist", "tab13-select-all-apps",
                        "App Name", colors), width=3),
                    dbc.Col(_build_checklist_filter(
                        ap_channels, "tab13-channel-checklist", "tab13-select-all-channels",
                        "Traffic Channel", colors), width=3),
                    dbc.Col(_build_checklist_filter(
                        ap_afids, "tab13-afid-checklist", "tab13-select-all-afids",
                        "AFID", colors), width=2),
                ], align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),
        html.Div([
            dbc.Button("Load Data", id="tab13-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id="daedalus-tab13-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# HELPER: Decline tab builder (reused by tabs 14/15/16)
# =============================================================================

def _build_decline_tab_layout(tab_prefix, filter_opts, colors, extra_filters=None):
    """Build filter UI for decline tabs"""
    da_min = filter_opts.get("da_min", str(date.today() - timedelta(days=90)))
    da_max = filter_opts.get("da_max", str(date.today()))
    da_apps = filter_opts.get("da_apps", [])

    filter_cols = [
        dbc.Col(_build_date_picker(f"{tab_prefix}-start-date", da_min, da_max, da_min,
                                    "Start Date", colors), width=2),
        dbc.Col(_build_date_picker(f"{tab_prefix}-end-date", da_min, da_max, da_max,
                                    "End Date", colors), width=2),
        dbc.Col(_build_checklist_filter(
            da_apps, f"{tab_prefix}-app-checklist", f"{tab_prefix}-select-all-apps",
            "App Name", colors), width=3),
    ]

    if extra_filters:
        filter_cols.extend(extra_filters)

    # Threshold input
    filter_cols.append(
        dbc.Col(html.Div([
            html.Div("Min Threshold %", style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
            dbc.Input(id=f"{tab_prefix}-threshold", type="number", value=0, min=0, max=100,
                      step=0.1, size="sm",
                      style={"width": "100px", "backgroundColor": colors["card_bg"],
                             "color": colors["text_primary"], "border": f"1px solid {colors['border']}"}),
        ]), width=2)
    )

    return html.Div([
        dbc.Card([
            dbc.CardBody([
                dbc.Row(filter_cols, align="end"),
            ])
        ], style={"backgroundColor": colors["card_bg"], "border": f"1px solid {colors['border']}",
                   "marginBottom": "16px"}),
        html.Div([
            dbc.Button("Load Data", id=f"{tab_prefix}-load-btn", color="primary", className="mt-2 mb-3")
        ], style={"textAlign": "center"}),
        dcc.Loading(html.Div(id=f"daedalus-{tab_prefix}-charts"), type="dot", color="#FFFFFF"),
    ])


# =============================================================================
# TAB 14: DECLINE REASON % - APP — Filter UI
# =============================================================================

def _build_tab14(colors, filter_opts):
    return _build_decline_tab_layout("tab14", filter_opts, colors)


# =============================================================================
# TAB 15: DECLINE REASON % - CHANNEL — Filter UI
# =============================================================================

def _build_tab15(colors, filter_opts):
    dc_channels = filter_opts.get("dc_channels", [])
    extra = [
        dbc.Col(_build_checklist_filter(
            dc_channels, "tab15-channel-checklist", "tab15-select-all-channels",
            "Channel Name", colors), width=3),
    ]
    return _build_decline_tab_layout("tab15", filter_opts, colors, extra_filters=extra)


# =============================================================================
# TAB 16: DECLINE REASON % - AFID — Filter UI
# =============================================================================

def _build_tab16(colors, filter_opts):
    dc_channels = filter_opts.get("dc_channels", [])
    daf_afids = filter_opts.get("daf_afids", [])
    extra = [
        dbc.Col(_build_checklist_filter(
            dc_channels, "tab16-channel-checklist", "tab16-select-all-channels",
            "Channel Name", colors), width=2),
        dbc.Col(_build_checklist_filter(
            daf_afids, "tab16-afid-checklist", "tab16-select-all-afids",
            "AFID", colors), width=2),
    ]
    return _build_decline_tab_layout("tab16", filter_opts, colors, extra_filters=extra)
