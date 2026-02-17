"""
Callbacks for ICARUS Multi Dashboard

Handles:
- Active/Inactive tab loading with filters
- Data loading (pivot tables + charts)
- Plan group expand/collapse

All component IDs use 'multi-' prefix to avoid conflicts with Historical.
"""

from datetime import datetime
from dash import html, dcc, callback, Input, Output, State, ALL, MATCH, ctx, no_update
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd

from app.theme import get_theme_colors
from app.charts import get_chart_config, create_legend_component
from app.colors import build_plan_color_map
from app.auth import get_current_user, get_user_allowed_apps

from app.dashboards.icarus_multi.data import (
    load_multi_dates, load_multi_plan_groups,
    load_multi_pivot_data, load_all_multi_chart_data
)
from app.dashboards.icarus_multi.layout import (
    MULTI_METRICS_CONFIG, MULTI_CHART_METRICS,
    create_multi_filters_layout, filter_plan_groups_by_apps
)
from app.dashboards.icarus_multi.charts import build_bc_line_chart


# =============================================================================
# PIVOT TABLE PROCESSING (BC-based instead of date-based)
# =============================================================================

def format_metric_value(value, metric_name, is_crystal_ball=False):
    """Format value based on metric type"""
    if value is None or pd.isna(value):
        return None
    
    config = MULTI_METRICS_CONFIG.get(metric_name, {})
    format_type = config.get("format", "number")
    
    try:
        if metric_name == "Rebills" and is_crystal_ball:
            return round(float(value))
        
        if format_type == "percent":
            return round(float(value) * 100, 2)
        return round(float(value), 2)
    except:
        return None


def get_display_metric_name(metric_name):
    """Get display name with suffix"""
    config = MULTI_METRICS_CONFIG.get(metric_name, {})
    display = config.get("display", metric_name)
    suffix = config.get("suffix", "")
    return f"{display}{suffix}"


def process_multi_pivot_data(pivot_data, selected_metrics, is_crystal_ball=False):
    """
    Process Multi pivot data into DataFrame for AG Grid.
    
    Unlike Historical (dates as columns), Multi has BC 0-12 as columns.
    
    Returns DataFrame with columns: Plan_Name, Metric_Name, BC0, BC1, ..., BC12
    """
    if not pivot_data or "BC" not in pivot_data or len(pivot_data.get("Plan_Name", [])) == 0:
        return None
    
    # BC columns: BC0 through BC12
    bc_range = range(0, 13)
    bc_columns = [f"BC{i}" for i in bc_range]
    
    # Get unique plan combos
    plan_combos = []
    seen = set()
    for i in range(len(pivot_data["Plan_Name"])):
        plan = pivot_data["Plan_Name"][i]
        if plan not in seen:
            plan_combos.append(plan)
            seen.add(plan)
    
    plan_combos.sort()
    
    # Build lookup: (plan, bc) -> {metric: value}
    lookup = {}
    for i in range(len(pivot_data["Plan_Name"])):
        plan = pivot_data["Plan_Name"][i]
        bc = pivot_data["BC"][i]
        
        key = (plan, bc)
        if key not in lookup:
            lookup[key] = {}
        
        for metric in selected_metrics:
            if metric in pivot_data:
                lookup[key][metric] = pivot_data[metric][i]
    
    # Build rows: one row per Plan + Metric combination
    rows = []
    for plan_name in plan_combos:
        for metric in selected_metrics:
            row = {
                "Plan_Name": plan_name,
                "Metric_Name": get_display_metric_name(metric)
            }
            
            for bc in bc_range:
                col_name = f"BC{bc}"
                key = (plan_name, bc)
                raw_value = lookup.get(key, {}).get(metric, None)
                formatted_value = format_metric_value(raw_value, metric, is_crystal_ball)
                row[col_name] = formatted_value
            
            rows.append(row)
    
    df = pd.DataFrame(rows)
    column_order = ["Plan_Name", "Metric_Name"] + bc_columns
    df = df[[c for c in column_order if c in df.columns]]
    
    return df


# =============================================================================
# REGISTER ALL MULTI CALLBACKS
# =============================================================================

def register_callbacks(app):
    """Register all callbacks for the ICARUS Multi dashboard"""
    
    # =========================================================================
    # ACTIVE TAB CONTENT
    # =========================================================================
    @app.callback(
        Output('multi-active-tab-content', 'children'),
        Input('multi-dashboard-tabs', 'active_tab'),
        Input('session-store', 'data'),
        State('theme-store', 'data'),
        prevent_initial_call=False
    )
    def load_multi_active_tab(active_tab, session_data, theme):
        if not active_tab or active_tab != "active":
            return no_update
        
        theme = theme or "dark"
        
        try:
            session_id = session_data.get('session_id') if session_data else None
            user = get_current_user(session_id) if session_id else None
            allowed_apps = get_user_allowed_apps(user, "icarus_multi") if user else None
            
            available_dates = load_multi_dates()
            plan_groups = load_multi_plan_groups("Active")
            plan_groups = filter_plan_groups_by_apps(plan_groups, allowed_apps)
            
            if not plan_groups["Plan_Name"]:
                return dbc.Alert("No active plans found.", color="warning")
            
            return html.Div([
                create_multi_filters_layout(plan_groups, available_dates, "multi-active", theme),
                html.Div([
                    dbc.Button("Load Data", id="multi-active-load-btn", color="primary", className="mt-3 mb-3")
                ], style={"textAlign": "center"}),
                html.Hr(),
                dcc.Loading(html.Div(id="multi-active-pivot-container"), type="dot", color="#FFFFFF"),
                html.Div(id="multi-active-charts-container", style={"display": "none"})
            ])
        except Exception as e:
            return dbc.Alert(f"Error loading data: {str(e)}", color="danger")
    
    # =========================================================================
    # INACTIVE TAB CONTENT
    # =========================================================================
    @app.callback(
        Output('multi-inactive-tab-content', 'children'),
        Input('multi-dashboard-tabs', 'active_tab'),
        Input('session-store', 'data'),
        State('theme-store', 'data'),
        prevent_initial_call=True
    )
    def load_multi_inactive_tab(active_tab, session_data, theme):
        if active_tab != "inactive":
            return no_update
        
        theme = theme or "dark"
        
        try:
            session_id = session_data.get('session_id') if session_data else None
            user = get_current_user(session_id) if session_id else None
            allowed_apps = get_user_allowed_apps(user, "icarus_multi") if user else None
            
            available_dates = load_multi_dates()
            plan_groups = load_multi_plan_groups("Inactive")
            plan_groups = filter_plan_groups_by_apps(plan_groups, allowed_apps)
            
            if not plan_groups["Plan_Name"]:
                return dbc.Alert("No inactive plans found.", color="warning")
            
            return html.Div([
                create_multi_filters_layout(plan_groups, available_dates, "multi-inactive", theme),
                html.Div([
                    dbc.Button("Load Data", id="multi-inactive-load-btn", color="primary", className="mt-3 mb-3")
                ], style={"textAlign": "center"}),
                html.Hr(),
                dcc.Loading(html.Div(id="multi-inactive-pivot-container"), type="dot", color="#FFFFFF"),
                html.Div(id="multi-inactive-charts-container", style={"display": "none"})
            ])
        except Exception as e:
            return dbc.Alert(f"Error loading data: {str(e)}", color="danger")
    
    # =========================================================================
    # LOAD ACTIVE DATA
    # =========================================================================
    @app.callback(
        Output('multi-active-pivot-container', 'children'),
        Output('multi-active-charts-container', 'children'),
        Input('multi-active-load-btn', 'n_clicks'),
        State('multi-active-report-date', 'value'),
        State('multi-active-cohort', 'value'),
        State('multi-active-metrics', 'value'),
        State({'type': 'multi-active-plan-checklist', 'app': ALL}, 'value'),
        State({'type': 'multi-active-plan-checklist-more', 'app': ALL}, 'value'),
        State('theme-store', 'data'),
        prevent_initial_call=True
    )
    def load_multi_active_data(n_clicks, report_date, cohort, metrics,
                                plan_values, plan_more_values, theme):
        if not n_clicks:
            return no_update, no_update
        
        return _load_multi_data(report_date, cohort, metrics,
                                plan_values, plan_more_values, theme, "Active")
    
    # =========================================================================
    # LOAD INACTIVE DATA
    # =========================================================================
    @app.callback(
        Output('multi-inactive-pivot-container', 'children'),
        Output('multi-inactive-charts-container', 'children'),
        Input('multi-inactive-load-btn', 'n_clicks'),
        State('multi-inactive-report-date', 'value'),
        State('multi-inactive-cohort', 'value'),
        State('multi-inactive-metrics', 'value'),
        State({'type': 'multi-inactive-plan-checklist', 'app': ALL}, 'value'),
        State({'type': 'multi-inactive-plan-checklist-more', 'app': ALL}, 'value'),
        State('theme-store', 'data'),
        prevent_initial_call=True
    )
    def load_multi_inactive_data(n_clicks, report_date, cohort, metrics,
                                  plan_values, plan_more_values, theme):
        if not n_clicks:
            return no_update, no_update
        
        return _load_multi_data(report_date, cohort, metrics,
                                plan_values, plan_more_values, theme, "Inactive")
    
    # =========================================================================
    # PLAN GROUP EXPAND/COLLAPSE (clientside)
    # =========================================================================
    app.clientside_callback(
        """
        function(n_clicks, is_open, text) {
            if (!n_clicks) return [window.dash_clientside.no_update, window.dash_clientside.no_update];
            var new_open = !is_open;
            var new_text;
            if (new_open) {
                new_text = text.replace('+', '\u2212').replace('more', 'less');
            } else {
                new_text = text.replace('\u2212', '+').replace('less', 'more');
            }
            return [new_open, new_text];
        }
        """,
        Output({"type": "multi-active-plan-collapse", "app": MATCH}, "is_open"),
        Output({"type": "multi-active-plan-toggle", "app": MATCH}, "children"),
        Input({"type": "multi-active-plan-toggle", "app": MATCH}, "n_clicks"),
        State({"type": "multi-active-plan-collapse", "app": MATCH}, "is_open"),
        State({"type": "multi-active-plan-toggle", "app": MATCH}, "children"),
        prevent_initial_call=True
    )
    
    app.clientside_callback(
        """
        function(n_clicks, is_open, text) {
            if (!n_clicks) return [window.dash_clientside.no_update, window.dash_clientside.no_update];
            var new_open = !is_open;
            var new_text;
            if (new_open) {
                new_text = text.replace('+', '\u2212').replace('more', 'less');
            } else {
                new_text = text.replace('\u2212', '+').replace('less', 'more');
            }
            return [new_open, new_text];
        }
        """,
        Output({"type": "multi-inactive-plan-collapse", "app": MATCH}, "is_open"),
        Output({"type": "multi-inactive-plan-toggle", "app": MATCH}, "children"),
        Input({"type": "multi-inactive-plan-toggle", "app": MATCH}, "n_clicks"),
        State({"type": "multi-inactive-plan-collapse", "app": MATCH}, "is_open"),
        State({"type": "multi-inactive-plan-toggle", "app": MATCH}, "children"),
        prevent_initial_call=True
    )
    
    # Datepicker dark override for Multi tabs
    app.clientside_callback(
        """
        function(tab) {
            var style = document.getElementById('datepicker-dark-override');
            if (!style) {
                style = document.createElement('style');
                style.id = 'datepicker-dark-override';
                style.textContent = '';
                document.head.appendChild(style);
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('multi-dashboard-tabs', 'className'),
        Input('multi-dashboard-tabs', 'active_tab')
    )


# =============================================================================
# SHARED DATA LOADING LOGIC (used by both Active and Inactive)
# =============================================================================

def _load_multi_data(report_date, cohort, metrics, plan_values, plan_more_values, theme, active_inactive):
    """Shared logic for loading Multi dashboard data"""
    theme = theme or "dark"
    colors = get_theme_colors(theme)
    
    # Flatten selected plans
    selected_plans = []
    for plans in plan_values:
        if plans:
            selected_plans.extend(plans)
    for plans in plan_more_values:
        if plans:
            selected_plans.extend(plans)
    
    if not selected_plans:
        return dbc.Alert("Please select at least one Plan.", color="warning"), None
    
    if not metrics:
        return dbc.Alert("Please select at least one Metric.", color="warning"), None
    
    if not report_date:
        return dbc.Alert("Please select a Reporting Date.", color="warning"), None
    
    # Convert date string to date object
    if isinstance(report_date, str):
        report_date_obj = datetime.strptime(report_date, '%Y-%m-%d').date()
    else:
        report_date_obj = report_date
    
    try:
        # =============================================
        # PIVOT TABLES
        # =============================================
        
        # Load Regular data
        try:
            pivot_regular = load_multi_pivot_data(
                report_date_obj, cohort, selected_plans, metrics, "Regular", active_inactive
            )
            df_regular = process_multi_pivot_data(pivot_regular, metrics, False)
        except Exception as e:
            df_regular = None
            regular_error = str(e)
        else:
            regular_error = None
        
        # Load Crystal Ball data
        try:
            pivot_crystal = load_multi_pivot_data(
                report_date_obj, cohort, selected_plans, metrics, "Crystal Ball", active_inactive
            )
            df_crystal = process_multi_pivot_data(pivot_crystal, metrics, True)
        except Exception as e:
            df_crystal = None
            crystal_error = str(e)
        else:
            crystal_error = None
        
        pivot_content = []
        
        if regular_error:
            pivot_content.append(dbc.Alert(f"Data loading failed: {regular_error}", color="danger"))
        
        if df_regular is not None and not df_regular.empty:
            pivot_content.append(html.H5("Plans Overview (Regular)"))
            pivot_content.append(_build_multi_grid(df_regular, theme))
        
        if crystal_error:
            pivot_content.append(dbc.Alert(f"Data loading failed: {crystal_error}", color="danger"))
        
        if df_crystal is not None and not df_crystal.empty:
            pivot_content.append(html.Br())
            pivot_content.append(html.H5("Plans Overview (Crystal Ball)"))
            pivot_content.append(_build_multi_grid(df_crystal, theme))
        
        # =============================================
        # CHARTS
        # =============================================
        chart_metric_names = [cm["metric"] for cm in MULTI_CHART_METRICS]
        all_regular_data = load_all_multi_chart_data(
            report_date_obj, cohort, selected_plans, chart_metric_names, "Regular", active_inactive
        )
        all_crystal_data = load_all_multi_chart_data(
            report_date_obj, cohort, selected_plans, chart_metric_names, "Crystal Ball", active_inactive
        )
        
        charts_content = []
        for chart_config in MULTI_CHART_METRICS:
            display_name = chart_config["display"]
            metric = chart_config["metric"]
            format_type = chart_config["format"]
            
            if format_type == "dollar":
                display_title = f"{display_name} ($)"
            elif format_type == "percent":
                display_title = f"{display_name} (%)"
            else:
                display_title = display_name
            
            chart_data_regular = all_regular_data.get(metric, {"Plan_Name": [], "BC": [], "metric_value": []})
            chart_data_crystal = all_crystal_data.get(metric, {"Plan_Name": [], "BC": [], "metric_value": []})
            
            fig_regular, plans_regular = build_bc_line_chart(chart_data_regular, display_title, format_type, theme)
            fig_crystal, plans_crystal = build_bc_line_chart(chart_data_crystal, f"{display_title} (Crystal Ball)", format_type, theme)
            
            color_map_regular = build_plan_color_map(plans_regular) if plans_regular else {}
            color_map_crystal = build_plan_color_map(plans_crystal) if plans_crystal else {}
            
            charts_content.append(
                dbc.Row([
                    dbc.Col([
                        html.H6(display_title, style={"color": colors["text_primary"]}),
                        create_legend_component(plans_regular, color_map_regular, theme) if plans_regular else None,
                        dcc.Graph(figure=fig_regular, config=get_chart_config(), style={"height": "420px"})
                    ], width=6),
                    dbc.Col([
                        html.H6(f"{display_title} (Crystal Ball)", style={"color": colors["text_primary"]}),
                        create_legend_component(plans_crystal, color_map_crystal, theme) if plans_crystal else None,
                        dcc.Graph(figure=fig_crystal, config=get_chart_config(), style={"height": "420px"})
                    ], width=6)
                ], className="mb-4")
            )
        
        # Handle empty
        if not pivot_content and not charts_content:
            return dbc.Alert("No data found for the selected filters.", color="warning"), None
        
        return dbc.Tabs([
            dbc.Tab(html.Div(pivot_content, className="mt-3"), label="Pivot Table", tab_id="pivot"),
            dbc.Tab(html.Div(charts_content, className="mt-3"), label="Charts", tab_id="charts")
        ], active_tab="pivot", className="mt-3"), None
    
    except Exception as e:
        return dbc.Alert(f"Data loading failed: {str(e)}", color="danger"), None


def _build_multi_grid(df, theme):
    """Build AG Grid for Multi pivot table"""
    col_defs = []
    for c in df.columns:
        col_def = {"field": c}
        if c in ["Plan_Name", "Metric_Name"]:
            col_def["pinned"] = "left"
        col_defs.append(col_def)
    
    return dag.AgGrid(
        rowData=df.to_dict('records'),
        columnDefs=col_defs,
        defaultColDef={
            "resizable": True, "sortable": True, "filter": True,
            "wrapHeaderText": True, "autoHeaderHeight": True
        },
        columnSize="autoSize",
        columnSizeOptions={"skipHeader": False},
        className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
        style={"height": "400px"}
    )
