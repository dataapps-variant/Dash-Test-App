"""
Callbacks for Daedalus Dashboard — Tabs 1-5

Tab 1: Daedalus (KPI cards, pivot tables, line charts, bar charts)
Tab 2: Pacing by Entity (dynamic spend+users line pairs per app)
Tab 3: CAC by Entity (one chart per app with Daily_CAC/T7D_CAC)
Tab 4: Current Subscriptions (line, pivot, pie, annotated entity charts)
Tab 5: Daedalus Historical (6 line charts + 1 pie)
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
)

from app.dashboards.daedalus.charts import (
    format_kpi_value,
    build_actual_target_lines, build_multi_app_lines,
    build_grouped_bar, build_pie_chart, build_entity_lines,
    build_annotated_line, build_annotated_entity_lines,
    build_annotated_portfolio_line,
    _empty_figure,
)

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
            cd["width"] = 120
            cd["type"] = "rightAligned"
            cd["valueFormatter"] = {"function": """
                params.value != null ?
                    (typeof params.value === 'number' ? '$ ' + params.value.toLocaleString('en-US', {maximumFractionDigits: 0}) : params.value)
                    : ''
            """}
        col_defs.append(cd)

    return dag.AgGrid(
        id=grid_id,
        columnDefs=col_defs,
        rowData=pivot_df.to_dict("records"),
        defaultColDef={"resizable": True},
        style={"height": "180px"},
        className="ag-theme-alpine-dark",
    )


# =============================================================================
# FILTER UI BUILDERS
# =============================================================================

def _build_app_checklist(apps, id_prefix, colors, default_all=True):
    """Build app name checklist with all/none toggle"""
    return html.Div([
        html.Div("App Name", style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
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
        [Input("tab1-app-checklist", "value"),
         Input("tab1-date-picker", "date"),
         Input("tab1-month-select", "value")],
    )
    def update_tab1_charts(app_names, selected_date, month_str):
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
            dbc.Row([
                dbc.Col(dcc.Graph(figure=spend_lines_fig, config=CHART_CONFIG), width=12),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=spend_total_fig, config=CHART_CONFIG), width=6),
                dbc.Col(dcc.Graph(figure=spend_bar_fig, config=CHART_CONFIG), width=6),
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
            dbc.Row([
                dbc.Col(dcc.Graph(figure=users_lines_fig, config=CHART_CONFIG), width=12),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=users_total_fig, config=CHART_CONFIG), width=6),
                dbc.Col(dcc.Graph(figure=users_bar_fig, config=CHART_CONFIG), width=6),
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
        Input("tab2-month-select", "value"),
    )
    def update_tab2_charts(month_str):
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
                _section_title("Monthly Spend Pacing VG (Portfolio)", colors),
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
                _section_title(f"Monthly Pacing {app_name}", colors),
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
        [Input("tab3-start-date", "date"),
         Input("tab3-end-date", "date"),
         Input("tab3-metric-checklist", "value")],
    )
    def update_tab3_charts(start_date, end_date, metrics):
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
                    dash_style = "dot" if "T7D" in col else "solid"
                    color = "#06B6D4" if "Daily" in col else "#F97316"
                    fig.add_trace(go.Scatter(
                        x=app_df["Date"], y=app_df[col],
                        mode="lines", name=label,
                        line=dict(color=color, width=1.6, dash=dash_style),
                        hovertemplate=f'{label}  $%{{y:,.2f}}<extra></extra>',
                    ))

            fig.update_layout(
                height=300,
                margin=dict(l=60, r=20, t=20, b=40),
                hovermode="x unified",
                paper_bgcolor=colors["card_bg"],
                plot_bgcolor=colors["card_bg"],
                font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
                xaxis=dict(gridcolor=colors["border"], tickformat="%b %d"),
                yaxis=dict(gridcolor=colors["border"], tickprefix="$"),
                legend=dict(font=dict(color=colors["text_primary"]), bgcolor="rgba(0,0,0,0)"),
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
        [Input("tab4-app-checklist", "value"),
         Input("tab4-channel-checklist", "value"),
         Input("tab4-start-date", "date"),
         Input("tab4-end-date", "date")],
    )
    def update_tab4_charts(app_names, channels, start_date, end_date):
        colors = _colors()
        if not app_names or not channels or not start_date or not end_date:
            return html.Div("Select filters", style={"color": colors["text_secondary"]})

        # Convert channels to int if needed
        channels_int = [int(c) if isinstance(c, str) and c.isdigit() else c for c in channels]

        # Chart 1: Portfolio active subs
        portfolio_df = get_portfolio_active_subs(app_names, channels_int, start_date, end_date)
        chart1 = build_annotated_line(portfolio_df, "number", theme=THEME,
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
        chart5, _ = build_annotated_entity_lines(entity_subs_df, "number", theme=THEME,
                                                  value_col="Current_Active_Subscription")

        # Charts 6-11: Ratio charts (entity + portfolio pairs)
        churn_entity = get_entity_churn(app_names, channels_int, start_date, end_date)
        churn_port = get_portfolio_churn(app_names, channels_int, start_date, end_date)
        chart6, _ = build_annotated_entity_lines(churn_entity, "percent", theme=THEME)
        chart7 = build_annotated_portfolio_line(churn_port, "percent", theme=THEME, name="Portfolio Churn Rate")

        ss_entity = get_entity_ss(app_names, channels_int, start_date, end_date)
        ss_port = get_portfolio_ss(app_names, channels_int, start_date, end_date)
        chart8, _ = build_annotated_entity_lines(ss_entity, "percent", theme=THEME)
        chart9 = build_annotated_portfolio_line(ss_port, "percent", theme=THEME, name="Portfolio SS Distribution")

        pend_entity = get_entity_pending(app_names, channels_int, start_date, end_date)
        pend_port = get_portfolio_pending(app_names, channels_int, start_date, end_date)
        chart10, _ = build_annotated_entity_lines(pend_entity, "percent", theme=THEME)
        chart11 = build_annotated_portfolio_line(pend_port, "percent", theme=THEME, name="Portfolio Pending Subs")

        return html.Div([
            # Chart 1
            html.Div([
                _section_title("Historical Portfolio Current Active Subscriptions", colors),
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
                dcc.Graph(figure=chart5, config=CHART_CONFIG),
            ], style=_card_style(colors)),

            # Charts 6-7
            html.Div([
                _section_title("Historical Daily T30D Entity-by-Entity Churn Rate", colors),
                dcc.Graph(figure=chart6, config=CHART_CONFIG),
            ], style=_card_style(colors)),
            html.Div([
                _section_title("Historical Daily T30D Portfolio Churn Rate", colors),
                dcc.Graph(figure=chart7, config=CHART_CONFIG),
            ], style=_card_style(colors)),

            # Charts 8-9
            html.Div([
                _section_title("Historical Daily T30D Entity-by-Entity SS Distribution", colors),
                dcc.Graph(figure=chart8, config=CHART_CONFIG),
            ], style=_card_style(colors)),
            html.Div([
                _section_title("Historical Daily T30D Portfolio SS Distribution", colors),
                dcc.Graph(figure=chart9, config=CHART_CONFIG),
            ], style=_card_style(colors)),

            # Charts 10-11
            html.Div([
                _section_title("Historical Daily T30D Entity-by-Entity Pending Subscriptions", colors),
                dcc.Graph(figure=chart10, config=CHART_CONFIG),
            ], style=_card_style(colors)),
            html.Div([
                _section_title("Historical Daily T30D Portfolio Pending Subscriptions", colors),
                dcc.Graph(figure=chart11, config=CHART_CONFIG),
            ], style=_card_style(colors)),
        ])

    # -----------------------------------------------------------------
    # TAB 5: DAEDALUS HISTORICAL — update on filter change
    # -----------------------------------------------------------------
    @app.callback(
        Output("daedalus-tab5-charts", "children"),
        [Input("tab5-app-checklist", "value"),
         Input("tab5-start-date", "date"),
         Input("tab5-end-date", "date")],
    )
    def update_tab5_charts(app_names, start_date, end_date):
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

        dcc.Loading(html.Div(id="daedalus-tab5-charts"), type="dot", color="#FFFFFF"),
    ])
