"""
Callbacks for ICARUS Historical Dashboard

Extracted from app.py - contains:
- Data processing functions (format_metric_value, process_pivot_data)
- Tab loading callbacks (Active/Inactive)
- Data loading callbacks (pivot + charts)
- Plan group expand/collapse clientside callbacks
- Datepicker dark theme override
"""

from datetime import datetime
from dash import html, dcc, callback, Input, Output, State, ALL, MATCH, ctx, no_update
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd

from app.config import METRICS_CONFIG, CHART_METRICS
from app.theme import get_theme_colors
from app.auth import get_current_user, get_user_allowed_apps
from app.bigquery_client import (
    load_date_bounds, load_plan_groups, load_pivot_data, load_all_chart_data
)
from app.charts import build_line_chart, get_chart_config, create_legend_component
from app.colors import build_plan_color_map

from app.dashboards.icarus_historical.layout import (
    create_filters_layout, filter_plan_groups_by_apps
)


# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def format_metric_value(value, metric_name, is_crystal_ball=False):
    """Format value based on metric type"""
    if value is None or pd.isna(value):
        return None
    
    config = METRICS_CONFIG.get(metric_name, {})
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
    config = METRICS_CONFIG.get(metric_name, {})
    display = config.get("display", metric_name)
    suffix = config.get("suffix", "")
    return f"{display}{suffix}"


def process_pivot_data(pivot_data, selected_metrics, is_crystal_ball=False):
    """Process pivot data into DataFrame for AG Grid"""
    if not pivot_data or "Reporting_Date" not in pivot_data or len(pivot_data["Reporting_Date"]) == 0:
        return None, []
    
    unique_dates = sorted(set(pivot_data["Reporting_Date"]), reverse=True)
    
    date_columns = []
    date_map = {}
    for d in unique_dates:
        if hasattr(d, 'strftime'):
            formatted = d.strftime("%m/%d/%Y")
        else:
            formatted = str(d)
        date_columns.append(formatted)
        date_map[d] = formatted
    
    plan_combos = []
    seen = set()
    for i in range(len(pivot_data["App_Name"])):
        combo = (pivot_data["App_Name"][i], pivot_data["Plan_Name"][i])
        if combo not in seen:
            plan_combos.append(combo)
            seen.add(combo)
    
    plan_combos.sort(key=lambda x: (x[0], x[1]))
    
    lookup = {}
    for i in range(len(pivot_data["Reporting_Date"])):
        app = pivot_data["App_Name"][i]
        plan = pivot_data["Plan_Name"][i]
        date = pivot_data["Reporting_Date"][i]
        
        key = (app, plan, date)
        if key not in lookup:
            lookup[key] = {}
        
        for metric in selected_metrics:
            if metric in pivot_data:
                lookup[key][metric] = pivot_data[metric][i]
    
    rows = []
    for app_name, plan_name in plan_combos:
        for metric in selected_metrics:
            row = {
                "App": app_name,
                "Plan": plan_name,
                "Metric": get_display_metric_name(metric)
            }
            
            for d in unique_dates:
                formatted_date = date_map[d]
                key = (app_name, plan_name, d)
                raw_value = lookup.get(key, {}).get(metric, None)
                formatted_value = format_metric_value(raw_value, metric, is_crystal_ball)
                row[formatted_date] = formatted_value
            
            rows.append(row)
    
    df = pd.DataFrame(rows)
    column_order = ["App", "Plan", "Metric"] + date_columns
    df = df[[c for c in column_order if c in df.columns]]
    
    return df, date_columns


# =============================================================================
# REGISTER ALL HISTORICAL CALLBACKS
# =============================================================================

def register_callbacks(app):
    """Register all callbacks for the ICARUS Historical dashboard"""

    # =========================================================================
    # ACTIVE TAB CONTENT
    # =========================================================================
    @app.callback(
        Output('active-tab-content', 'children'),
        Input('dashboard-tabs', 'active_tab'),
        Input('session-store', 'data'),
        State('theme-store', 'data'),
        prevent_initial_call=False
    )
    def load_active_tab(active_tab, session_data, theme):
        """Load content for Active tab"""
        if not active_tab or active_tab != "active":
            return no_update
        
        theme = theme or "dark"
        
        try:
            # Get current user for app filtering
            session_id = session_data.get('session_id') if session_data else None
            user = get_current_user(session_id) if session_id else None
            allowed_apps = get_user_allowed_apps(user, "icarus_historical") if user else None
            
            date_bounds = load_date_bounds()
            plan_groups = load_plan_groups("Active")
            
            # Filter plan groups by allowed apps
            plan_groups = filter_plan_groups_by_apps(plan_groups, allowed_apps)
            
            if not plan_groups["Plan_Name"]:
                return dbc.Alert("No active plans found.", color="warning")
            
            return html.Div([
                create_filters_layout(plan_groups, date_bounds["min_date"], date_bounds["max_date"], "active", theme),
                html.Div([
                    dbc.Button("Load Data", id="active-load-btn", color="primary", className="mt-3 mb-3")
                ], style={"textAlign": "center"}),
                html.Hr(),
                dcc.Loading(html.Div(id="active-pivot-container"), type="dot", color="#FFFFFF"),
                html.Div(id="active-charts-container", style={"display": "none"})
            ])
        except Exception as e:
            return dbc.Alert(f"Error loading data: {str(e)}", color="danger")

    # =========================================================================
    # INACTIVE TAB CONTENT
    # =========================================================================
    @app.callback(
        Output('inactive-tab-content', 'children'),
        Input('dashboard-tabs', 'active_tab'),
        Input('session-store', 'data'),
        State('theme-store', 'data'),
        prevent_initial_call=True
    )
    def load_inactive_tab(active_tab, session_data, theme):
        """Load content for Inactive tab"""
        if active_tab != "inactive":
            return no_update
        
        theme = theme or "dark"
        
        try:
            # Get current user for app filtering
            session_id = session_data.get('session_id') if session_data else None
            user = get_current_user(session_id) if session_id else None
            allowed_apps = get_user_allowed_apps(user, "icarus_historical") if user else None
            
            date_bounds = load_date_bounds()
            plan_groups = load_plan_groups("Inactive")
            
            # Filter plan groups by allowed apps
            plan_groups = filter_plan_groups_by_apps(plan_groups, allowed_apps)
            
            if not plan_groups["Plan_Name"]:
                return dbc.Alert("No inactive plans found.", color="warning")
            
            return html.Div([
                create_filters_layout(plan_groups, date_bounds["min_date"], date_bounds["max_date"], "inactive", theme),
                html.Div([
                    dbc.Button("Load Data", id="inactive-load-btn", color="primary", className="mt-3 mb-3")
                ], style={"textAlign": "center"}),
                html.Hr(),
                dcc.Loading(html.Div(id="inactive-pivot-container"), type="dot", color="#FFFFFF"),
                html.Div(id="inactive-charts-container", style={"display": "none"})
            ])
        except Exception as e:
            return dbc.Alert(f"Error loading data: {str(e)}", color="danger")

    # =========================================================================
    # LOAD ACTIVE DATA
    # =========================================================================
    @app.callback(
        Output('active-pivot-container', 'children'),
        Output('active-charts-container', 'children'),
        Input('active-load-btn', 'n_clicks'),
        State('active-from-date', 'date'),
        State('active-to-date', 'date'),
        State('active-bc', 'value'),
        State('active-cohort', 'value'),
        State('active-metrics', 'value'),
        State({'type': 'active-plan-checklist', 'app': ALL}, 'value'),
        State({'type': 'active-plan-checklist-more', 'app': ALL}, 'value'),
        State('theme-store', 'data'),
        prevent_initial_call=True
    )
    def load_active_data(n_clicks, from_date, to_date, bc, cohort, metrics, plan_values, plan_more_values, theme):
        """Load data for Active tab"""
        if not n_clicks:
            return no_update, no_update
        
        return _load_historical_data(from_date, to_date, bc, cohort, metrics,
                                     plan_values, plan_more_values, theme, "Active")

    # =========================================================================
    # LOAD INACTIVE DATA
    # =========================================================================
    @app.callback(
        Output('inactive-pivot-container', 'children'),
        Output('inactive-charts-container', 'children'),
        Input('inactive-load-btn', 'n_clicks'),
        State('inactive-from-date', 'date'),
        State('inactive-to-date', 'date'),
        State('inactive-bc', 'value'),
        State('inactive-cohort', 'value'),
        State('inactive-metrics', 'value'),
        State({'type': 'inactive-plan-checklist', 'app': ALL}, 'value'),
        State({'type': 'inactive-plan-checklist-more', 'app': ALL}, 'value'),
        State('theme-store', 'data'),
        prevent_initial_call=True
    )
    def load_inactive_data(n_clicks, from_date, to_date, bc, cohort, metrics, plan_values, plan_more_values, theme):
        """Load data for Inactive tab"""
        if not n_clicks:
            return no_update, no_update
        
        return _load_historical_data(from_date, to_date, bc, cohort, metrics,
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
        Output({"type": "active-plan-collapse", "app": MATCH}, "is_open"),
        Output({"type": "active-plan-toggle", "app": MATCH}, "children"),
        Input({"type": "active-plan-toggle", "app": MATCH}, "n_clicks"),
        State({"type": "active-plan-collapse", "app": MATCH}, "is_open"),
        State({"type": "active-plan-toggle", "app": MATCH}, "children"),
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
        Output({"type": "inactive-plan-collapse", "app": MATCH}, "is_open"),
        Output({"type": "inactive-plan-toggle", "app": MATCH}, "children"),
        Input({"type": "inactive-plan-toggle", "app": MATCH}, "n_clicks"),
        State({"type": "inactive-plan-collapse", "app": MATCH}, "is_open"),
        State({"type": "inactive-plan-toggle", "app": MATCH}, "children"),
        prevent_initial_call=True
    )

    # Force dark date picker - injects CSS after react-dates loads
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
                `;
                document.head.appendChild(style);
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('dashboard-tabs', 'className'),
        Input('dashboard-tabs', 'active_tab')
    )


# =============================================================================
# SHARED DATA LOADING LOGIC (used by both Active and Inactive)
# =============================================================================

def _load_historical_data(from_date, to_date, bc, cohort, metrics, plan_values, plan_more_values, theme, active_inactive):
    """Shared logic for loading Historical dashboard data"""
    theme = theme or "dark"
    colors = get_theme_colors(theme)
    
    # Flatten selected plans (visible + expanded)
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
    
    # Convert dates
    if isinstance(from_date, str):
        from_date = datetime.strptime(from_date.split('T')[0], '%Y-%m-%d').date()
    if isinstance(to_date, str):
        to_date = datetime.strptime(to_date.split('T')[0], '%Y-%m-%d').date()
    
    # Load pivot data
    try:
        # Load regular data
        try:
            pivot_regular = load_pivot_data(from_date, to_date, int(bc), cohort, selected_plans, metrics, "Regular", active_inactive)
            df_regular, date_cols_regular = process_pivot_data(pivot_regular, metrics, False)
        except Exception as e:
            pivot_regular = None
            df_regular = None
            regular_error = str(e)
        else:
            regular_error = None
        
        # Load crystal ball data independently
        try:
            pivot_crystal = load_pivot_data(from_date, to_date, int(bc), cohort, selected_plans, metrics, "Crystal Ball", active_inactive)
            df_crystal, date_cols_crystal = process_pivot_data(pivot_crystal, metrics, True)
        except Exception as e:
            pivot_crystal = None
            df_crystal = None
            crystal_error = str(e)
        else:
            crystal_error = None
        
        pivot_content = []
        
        if regular_error:
            pivot_content.append(dbc.Alert(f"Data loading failed: {regular_error}", color="danger"))
        
        if df_regular is not None and not df_regular.empty:
            pivot_content.append(html.H5("Plan Overview (Regular)"))
            pivot_content.append(
                dag.AgGrid(
                    rowData=df_regular.to_dict('records'),
                    columnDefs=[{"field": c, "pinned": "left" if c in ["App", "Plan", "Metric"] else None} for c in df_regular.columns],
                    defaultColDef={"resizable": True, "sortable": True, "filter": True, "wrapHeaderText": True, "autoHeaderHeight": True},
                    columnSize="autoSize",
                    columnSizeOptions={"skipHeader": False},
                    className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
                    style={"height": "400px"}
                )
            )
        
        if crystal_error:
            pivot_content.append(dbc.Alert(f"Data loading failed: {crystal_error}", color="danger"))
        
        if df_crystal is not None and not df_crystal.empty:
            pivot_content.append(html.Br())
            pivot_content.append(html.H5("Plan Overview (Crystal Ball)"))
            pivot_content.append(
                dag.AgGrid(
                    rowData=df_crystal.to_dict('records'),
                    columnDefs=[{"field": c, "pinned": "left" if c in ["App", "Plan", "Metric"] else None} for c in df_crystal.columns],
                    defaultColDef={"resizable": True, "sortable": True, "filter": True, "wrapHeaderText": True, "autoHeaderHeight": True},
                    columnSize="autoSize",
                    columnSizeOptions={"skipHeader": False},
                    className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
                    style={"height": "400px"}
                )
            )
        
        # Load chart data
        chart_metric_names = [cm["metric"] for cm in CHART_METRICS]
        all_regular_data = load_all_chart_data(from_date, to_date, int(bc), cohort, selected_plans, chart_metric_names, "Regular", active_inactive)
        all_crystal_data = load_all_chart_data(from_date, to_date, int(bc), cohort, selected_plans, chart_metric_names, "Crystal Ball", active_inactive)
        
        charts_content = []
        for chart_config in CHART_METRICS:
            display_name = chart_config["display"]
            metric = chart_config["metric"]
            format_type = chart_config["format"]
            
            if format_type == "dollar":
                display_title = f"{display_name} ($)"
            elif format_type == "percent":
                display_title = f"{display_name} (%)"
            else:
                display_title = display_name
            
            chart_data_regular = all_regular_data.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            chart_data_crystal = all_crystal_data.get(metric, {"Plan_Name": [], "Reporting_Date": [], "metric_value": []})
            
            fig_regular, plans_regular = build_line_chart(chart_data_regular, display_title, format_type, (from_date, to_date), theme)
            fig_crystal, plans_crystal = build_line_chart(chart_data_crystal, f"{display_title} (Crystal Ball)", format_type, (from_date, to_date), theme)
            
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
        
        # Handle case where both are empty
        if not pivot_content and not charts_content:
            return dbc.Alert("No data found for the selected filters.", color="warning"), None
        
        return dbc.Tabs([
            dbc.Tab(html.Div(pivot_content, className="mt-3"), label="Pivot Table", tab_id="pivot"),
            dbc.Tab(html.Div(charts_content, className="mt-3"), label="Charts", tab_id="charts")
        ], active_tab="pivot", className="mt-3"), None
        
    except Exception as e:
        return dbc.Alert(f"Data loading failed: {str(e)}", color="danger"), None
