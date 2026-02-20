"""
Variant Analytics Dashboard - Dash Version
Main Application Entry Point

To run:
    gunicorn app.app:server -b 0.0.0.0:8080

Environment Variables:
    GCS_CACHE_BUCKET - GCS bucket name for caching
    GOOGLE_APPLICATION_CREDENTIALS - Path to service account JSON
    SECRET_KEY - Secret key for session encryption
"""

import os
from datetime import datetime, date
from flask import Flask, request, make_response, redirect
import dash
from dash import Dash, html, dcc, callback, Input, Output, State, ALL, MATCH, ctx, no_update, clientside_callback
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd

from app.config import (
    APP_NAME, APP_TITLE, SECRET_KEY, DASHBOARDS,
    BC_OPTIONS, COHORT_OPTIONS, DEFAULT_BC, DEFAULT_COHORT, DEFAULT_PLAN,
    METRICS_CONFIG, CHART_METRICS, ROLE_OPTIONS, ROLE_DISPLAY,
    SESSION_TTL_DEFAULT, SESSION_TTL_REMEMBER
)
from app.theme import get_app_css, get_theme_colors, get_header_component, get_logo_component
from app.auth import (
    authenticate, logout, is_authenticated, get_current_user, is_admin,
    get_all_users, add_user, update_user, delete_user, get_role_display,
    get_readonly_users_for_dashboard, get_session_data,
    is_super_admin, can_manage_user, can_delete_user, get_assignable_roles,
    get_user_allowed_apps, get_user_app_access_from_db
)
from app.bigquery_client import (
    load_date_bounds, load_plan_groups, load_pivot_data, load_all_chart_data,
    refresh_bq_to_staging, refresh_gcs_from_staging, get_cache_info
)
from app.charts import build_line_chart, get_chart_config, create_legend_component
from app.colors import build_plan_color_map
# ICARUS Historical dashboard
from app.dashboards.icarus_historical.layout import create_icarus_historical_layout
from app.dashboards.icarus_historical import callbacks as historical_callbacks
# ICARUS Multi dashboard
from app.dashboards.icarus_multi.layout import create_icarus_multi_layout
from app.dashboards.icarus_multi import callbacks as multi_callbacks
# All Metrics Merged dashboard
from app.dashboards.all_metrics_merged.layout import create_merged_layout
from app.dashboards.all_metrics_merged import callbacks as merged_callbacks
from app.dashboards.all_metrics_merged.data import preload_merged_tables, get_merged_cache_info
# Daedalus dashboard
from app.dashboards.daedalus.layout import create_daedalus_layout
from app.dashboards.daedalus.data import preload_daedalus_tables
import app.dashboards.daedalus.callbacks as daedalus_callbacks
# Admin Panel
from app.dashboards.admin_panel.layout import create_admin_panel_layout
from app.dashboards.admin_panel import callbacks as admin_panel_callbacks
# =============================================================================
# APP INITIALIZATION
# =============================================================================

# Create Flask server
server = Flask(__name__)
server.secret_key = SECRET_KEY

# Simple health endpoint (doesn't load data)
@server.route('/health')
def health_check():
    """Simple health check that doesn't trigger data loading"""
    return {'status': 'healthy'}, 200

# Create Dash app
app = Dash(
    __name__,
    server=server,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
    ],
    suppress_callback_exceptions=True,
    title=APP_TITLE
)

# =============================================================================
# DATA PRELOADING - Load data at startup for faster response
# =============================================================================
def preload_data():
    """Preload data at app startup to avoid slow first requests"""
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Preloading data at startup...")
        start = datetime.now()
        
        # Load date bounds
        date_bounds = load_date_bounds()
        logger.info(f"  Date bounds loaded: {date_bounds['min_date']} to {date_bounds['max_date']}")
        
        # Load plan groups for both active and inactive
        active_plans = load_plan_groups("Active")
        logger.info(f"  Active plans loaded: {len(active_plans.get('Plan_Name', []))} plans")
        
        inactive_plans = load_plan_groups("Inactive")
        logger.info(f"  Inactive plans loaded: {len(inactive_plans.get('Plan_Name', []))} plans")
        
        # Preload All Metrics Merged tables
        logger.info("Preloading All Metrics Merged tables...")
        preload_merged_tables()
        logger.info("  Merged tables preloaded")
        
        # Get cache info
        cache_info = get_cache_info()
        logger.info(f"  Cache info loaded")
        
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"Preloading complete in {elapsed:.2f}s")
        
    except Exception as e:
        logger.error(f"Preloading failed: {e}")

# Preload data when the module is imported (happens once with --preload)
preload_data()
preload_daedalus_tables()

# Session cookie name
SESSION_COOKIE = "variant_session_id"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_session_id_from_cookie():
    """Get session ID from cookie"""
    return request.cookies.get(SESSION_COOKIE)


def get_available_apps_for_dashboard(dashboard_id):
    """Get all available App_Names for a dashboard by loading plan groups"""
    try:
        active_plans = load_plan_groups("Active")
        inactive_plans = load_plan_groups("Inactive")
        
        all_apps = set(active_plans.get("App_Name", []))
        all_apps.update(inactive_plans.get("App_Name", []))
        
        return sorted(all_apps)
    except Exception:
        return []


def get_dashboard_name(dashboard_id):
    """Get dashboard display name from ID"""
    for d in DASHBOARDS:
        if d["id"] == dashboard_id:
            return d["name"]
    return dashboard_id


# =============================================================================
# LAYOUT COMPONENTS
# =============================================================================

def create_login_layout(theme="dark"):
    """Create login page layout"""
    colors = get_theme_colors(theme)
    
    return html.Div([
        # Logo and header
        get_header_component(theme, "large", True, False, ""),
        
        # Subtitle
        html.P(
            "Sign in to access your dashboards",
            style={
                "textAlign": "center",
                "color": colors["text_secondary"],
                "fontSize": "14px",
                "margin": "0 0 40px 0"
            }
        ),
        
        # Login form
        dbc.Row([
            dbc.Col(width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.Input(
                            id="login-username",
                            placeholder="Enter your username",
                            type="text",
                            className="mb-3"
                        ),
                        dbc.InputGroup([
                            dbc.Input(
                                id="login-password",
                                placeholder="Enter your password",
                                type="password",
                            ),
                            dbc.Button(
                                "Show",
                                id="toggle-password-btn",
                                color="secondary",
                                outline=True,
                                n_clicks=0,
                                style={"borderColor": "#333333", "fontSize": "13px"}
                            )
                        ], className="mb-3"),
                        dbc.Checkbox(
                            id="login-remember",
                            label="Remember me",
                            className="mb-3"
                        ),
                        dbc.Button(
                            "Sign In",
                            id="login-button",
                            color="primary",
                            className="w-100 mb-3"
                        ),
                        html.Div(id="login-error"),
                        html.Hr(),
                        dbc.Alert([
                            html.Strong("Demo Credentials:"),
                            html.Br(),
                            "Admin: admin / admin123",
                            html.Br(),
                            "Viewer: viewer / viewer123"
                        ], color="info")
                    ])
                ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}"})
            ], width=6),
            dbc.Col(width=3)
        ])
    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px"
    })


def create_landing_layout(user, theme="dark"):
    """Create landing page layout"""
    colors = get_theme_colors(theme)
    cache_info = get_cache_info()
    
    # Check if user can access admin panel (admin or super_admin)
    user_role = user.get("role", "readonly") if user else "readonly"
    show_admin = user_role in ("admin", "super_admin")
    # Get merged cache info for All Metrics Merged timestamps
    merged_cache_info = get_merged_cache_info()

    # Build clickable dashboard table rows
    table_header = html.Thead(
        html.Tr([
            html.Th("Dashboard", style={"width": "35%"}),
            html.Th("Status", style={"width": "10%"}),
            html.Th("Last BQ Refresh", style={"width": "27%"}),
            html.Th("Last GCS Refresh", style={"width": "28%"})
        ])
    )
    
    table_rows = []
    for dashboard in DASHBOARDS:
        is_enabled = dashboard.get("enabled", False)
        status = "Active" if is_enabled else "Disabled"
        
        # Pick the correct timestamp based on which base tables the dashboard uses
        dash_id = dashboard["id"]
        if dash_id in ("icarus_historical", "icarus_multi"):
            bq_display = cache_info.get("last_bq_refresh", "--")
            gcs_display = cache_info.get("last_gcs_refresh", "--")
        elif dash_id == "all_metrics_merged":
            bq_display = merged_cache_info.get("last_bq_refresh", "--")
            gcs_display = merged_cache_info.get("last_gcs_refresh", "--")
        else:
            bq_display = "--"
            gcs_display = "--"
        
        if not is_enabled:
            bq_display = "--"
            gcs_display = "--"
        
        if is_enabled:
            name_cell = html.Td(
                html.A(
                    dashboard['name'],
                    id=f"nav-btn-{dashboard['id']}",
                    style={
                        "color": "#FFFFFF",
                        "cursor": "pointer",
                        "textDecoration": "none",
                        "fontWeight": "600",
                        "borderBottom": "1px solid rgba(255,255,255,0.3)",
                        "paddingBottom": "2px"
                    },
                    n_clicks=0
                )
            )
            row_style = {"cursor": "pointer"}
        else:
            name_cell = html.Td(
                f"  {dashboard['name']}",
                style={"color": "#555555"}
            )
            row_style = {"opacity": "0.5"}
        
        # BQ cell: button + timestamp
        bq_cell = html.Td([
            dbc.Button(
                "Refresh BQ",
                id={"type": "landing-refresh-bq", "index": dash_id},
                size="sm",
                className="refresh-btn-green me-2",
                disabled=not is_enabled,
                style={"fontSize": "11px", "padding": "2px 10px"}
            ),
            html.Span(
                bq_display,
                id={"type": "landing-bq-timestamp", "index": dash_id},
                style={"color": colors["text_secondary"], "fontSize": "13px"}
            )
        ])
        
        # GCS cell: button + timestamp
        gcs_cell = html.Td([
            dbc.Button(
                "Refresh GCS",
                id={"type": "landing-refresh-gcs", "index": dash_id},
                size="sm",
                className="refresh-btn-green me-2",
                disabled=not is_enabled,
                style={"fontSize": "11px", "padding": "2px 10px"}
            ),
            html.Span(
                gcs_display,
                id={"type": "landing-gcs-timestamp", "index": dash_id},
                style={"color": colors["text_secondary"], "fontSize": "13px"}
            )
        ])
        
        table_rows.append(
            html.Tr([
                name_cell,
                html.Td(status, style={"color": "#555555"} if not is_enabled else {}),
                bq_cell,
                gcs_cell
            ], style=row_style)
        )
    
    table_body = html.Tbody(table_rows)
    
    # Role display text
    if user_role == "super_admin":
        role_text = "Super Admin"
    elif user_role == "admin":
        role_text = "Admin"
    else:
        role_text = "Read Only"
    
    return html.Div([
       # Header with menu
        dbc.Row([
            dbc.Col(width=9),
            dbc.Col([
                html.Div([
                    dbc.Button("Admin Panel", id="nav-to-admin-btn", color="primary", size="sm", className="me-2") if show_admin else None,
                    dbc.Button("Logout", id="logout-btn", color="secondary", size="sm", className="me-2"),
                    dbc.DropdownMenu(
                        label=user['name'] if user else "Menu",
                        children=[
                            dbc.DropdownMenuItem(f"Role: {role_text}", disabled=True) if user else None,
                        ],
                        size="sm",
                        color="secondary"
                    )
                ], style={"display": "flex", "alignItems": "center", "justifyContent": "flex-end"})
            ], width=3, style={"textAlign": "right"})
        ], className="mb-3"),
        
        # Logo and welcome
        get_header_component(theme, "large", True, True, user["name"] if user else ""),
        
        # Unified clickable dashboard table
        html.H4("Available Dashboards", className="mb-3"),
dbc.Table(
            [table_header, table_body],
            striped=True, bordered=True, hover=True, className="mb-4"
        ),
        
        # Refresh status area
        html.Div(id="landing-refresh-status")
    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px"
    })


# =============================================================================
# MAIN LAYOUT
# =============================================================================

app.layout = html.Div([
    # URL location
    dcc.Location(id='url', refresh=False),

    # Session store (client-side)
    dcc.Store(id='session-store', storage_type='local'),

    # Theme store
    dcc.Store(id='theme-store', data='dark', storage_type='local'),

    # Current page store
    dcc.Store(id='page-store', data='login'),

    # Dynamic CSS container
    html.Div(id='dynamic-css-container'),

    # Main content
    html.Div(id='page-content'),

    # Admin modal container (kept for compatibility)
    html.Div(id='admin-modal-container')
])


# =============================================================================
# CALLBACKS
# =============================================================================

@callback(
    Output('dynamic-css-container', 'children'),
    Input('theme-store', 'data')
)
def update_css(theme):
    """Update body class based on theme"""
    return html.Div(id='theme-indicator', **{'data-theme': theme or 'dark'})


@callback(
    Output('page-content', 'children'),
    Output('admin-modal-container', 'children'),
    Input('session-store', 'data'),
    Input('page-store', 'data'),
    Input('theme-store', 'data')
)
def render_page(session_data, current_page, theme):
    """Render appropriate page based on authentication state"""
    theme = theme or "dark"

    # Check authentication
    session_id = session_data.get('session_id') if session_data else None

    if not session_id or not is_authenticated(session_id):
        return create_login_layout(theme), None

    user = get_current_user(session_id)

    if current_page == "landing" or current_page == "login":
        return create_landing_layout(user, theme), None
    elif current_page == "admin":
        return create_admin_panel_layout(user, theme), None
    elif current_page == "icarus_historical":
        return create_icarus_historical_layout(user, theme), None
    elif current_page == "icarus_multi":
        return create_icarus_multi_layout(user, theme), None
    elif current_page == "all_metrics_merged":
        return create_merged_layout(user, theme), None
    elif current_page == "daedalus":
        return create_daedalus_layout(user, theme), None
    else:
        return create_landing_layout(user, theme), None

@callback(
    Output('session-store', 'data'),
    Output('login-error', 'children'),
    Input('login-button', 'n_clicks'),
    Input('login-username', 'n_submit'),
    Input('login-password', 'n_submit'),
    State('login-username', 'value'),
    State('login-password', 'value'),
    State('login-remember', 'value'),
    prevent_initial_call=True
)
def handle_login(n_clicks, username_submit, password_submit, username, password, remember_me):
    """Handle login form submission - button click or Enter key"""
    if not n_clicks and not username_submit and not password_submit:
        return no_update, no_update
    
    if not username or not password:
        return no_update, dbc.Alert("Please enter both username and password", color="warning")
    
    success, session_id, expires_at = authenticate(username, password, remember_me or False)
    
    if success:
        return {'session_id': session_id}, dbc.Alert("Login successful!", color="success")
    else:
        return no_update, dbc.Alert("Invalid username or password", color="danger")

@callback(
    Output('login-password', 'type'),
    Output('toggle-password-btn', 'children'),
    Input('toggle-password-btn', 'n_clicks'),
    prevent_initial_call=True
)
def toggle_password_visibility(n_clicks):
    """Toggle password field between hidden and visible"""
    if n_clicks and n_clicks % 2 == 1:
        return "text", "Hide"
    return "password", "Show"

@callback(
    Output('session-store', 'data', allow_duplicate=True),
    Output('page-store', 'data', allow_duplicate=True),
    Input('logout-btn', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True
)
def handle_logout(n_clicks, session_data):
    """Handle logout"""
    if n_clicks:
        if session_data and session_data.get('session_id'):
            logout(session_data['session_id'])
        return {}, 'login'
    return no_update, no_update



@callback(
    Output('page-store', 'data'),
    Input('nav-btn-icarus_historical', 'n_clicks'),
    prevent_initial_call=True
)
def navigate_to_icarus(n_clicks):
    """Handle navigation to ICARUS dashboard"""
    if n_clicks:
        return "icarus_historical"
    return no_update
@callback(
    Output('page-store', 'data', allow_duplicate=True),
    Input('nav-btn-icarus_multi', 'n_clicks'),
    prevent_initial_call=True
)
def navigate_to_multi(n_clicks):
    """Handle navigation to ICARUS Multi dashboard"""
    if n_clicks:
        return "icarus_multi"
    return no_update

@callback(
    Output('page-store', 'data', allow_duplicate=True),
    Input('nav-btn-all_metrics_merged', 'n_clicks'),
    prevent_initial_call=True
)
def navigate_to_merged(n_clicks):
    """Handle navigation to All Metrics Merged dashboard"""
    if n_clicks:
        return "all_metrics_merged"
    return no_update
    
# Separate callback for back button (on dashboard page)
@callback(
    Output('page-store', 'data', allow_duplicate=True),
    Input('back-to-landing', 'n_clicks'),
    prevent_initial_call=True
)
def navigate_back(back_click):
    """Handle back button navigation"""
    if back_click:
        return "landing"
    return no_update


# =============================================================================
# SHARED CALLBACKS (used by both dashboards)
# =============================================================================
# =============================================================================
# LANDING PAGE REFRESH CALLBACKS
# =============================================================================

@callback(
    Output("landing-refresh-status", "children"),
    Output({"type": "landing-bq-timestamp", "index": ALL}, "children"),
    Input({"type": "landing-refresh-bq", "index": ALL}, "n_clicks"),
    prevent_initial_call=True
)
def handle_landing_bq_refresh(all_clicks):
    """Handle per-dashboard BQ refresh from landing page"""
    if not any(c for c in all_clicks if c):
        return no_update, no_update
    
    triggered = ctx.triggered_id
    if not isinstance(triggered, dict):
        return no_update, no_update
    
    dash_id = triggered.get("index", "")
    
    # Determine which refresh function to call
    if dash_id in ("icarus_historical", "icarus_multi"):
        success, msg = refresh_bq_to_staging()
    elif dash_id == "all_metrics_merged":
        from app.dashboards.all_metrics_merged.data import refresh_merged_bq_to_staging
        success, msg = refresh_merged_bq_to_staging()
    else:
        return dbc.Alert("No refresh available for this dashboard.", color="warning", dismissable=True), no_update
    
    # Build updated timestamps for ALL dashboard rows
    if success:
        # Re-fetch timestamps
        new_cache_info = get_cache_info()
        new_merged_info = get_merged_cache_info()
        
        # Build timestamp list in same order as DASHBOARDS
        new_timestamps = []
        for d in DASHBOARDS:
            did = d["id"]
            if did in ("icarus_historical", "icarus_multi"):
                new_timestamps.append(new_cache_info.get("last_bq_refresh", "--"))
            elif did == "all_metrics_merged":
                new_timestamps.append(new_merged_info.get("last_bq_refresh", "--"))
            else:
                new_timestamps.append("--")
        
        return dbc.Alert(msg, color="success", dismissable=True), new_timestamps
    else:
        return dbc.Alert(msg, color="danger", dismissable=True), no_update


@callback(
    Output("landing-refresh-status", "children", allow_duplicate=True),
    Output({"type": "landing-gcs-timestamp", "index": ALL}, "children"),
    Input({"type": "landing-refresh-gcs", "index": ALL}, "n_clicks"),
    prevent_initial_call=True
)
def handle_landing_gcs_refresh(all_clicks):
    """Handle per-dashboard GCS refresh from landing page"""
    if not any(c for c in all_clicks if c):
        return no_update, no_update
    
    triggered = ctx.triggered_id
    if not isinstance(triggered, dict):
        return no_update, no_update
    
    dash_id = triggered.get("index", "")
    
    # Determine which refresh function to call
    if dash_id in ("icarus_historical", "icarus_multi"):
        success, msg = refresh_gcs_from_staging()
    elif dash_id == "all_metrics_merged":
        from app.dashboards.all_metrics_merged.data import refresh_merged_gcs_from_staging
        success, msg = refresh_merged_gcs_from_staging()
    else:
        return dbc.Alert("No refresh available for this dashboard.", color="warning", dismissable=True), no_update
    
    # Build updated timestamps for ALL dashboard rows
    if success:
        # Re-fetch timestamps
        new_cache_info = get_cache_info()
        new_merged_info = get_merged_cache_info()
        
        # Build timestamp list in same order as DASHBOARDS
        new_timestamps = []
        for d in DASHBOARDS:
            did = d["id"]
            if did in ("icarus_historical", "icarus_multi"):
                new_timestamps.append(new_cache_info.get("last_gcs_refresh", "--"))
            elif did == "all_metrics_merged":
                new_timestamps.append(new_merged_info.get("last_gcs_refresh", "--"))
            else:
                new_timestamps.append("--")
        
        return dbc.Alert(msg, color="success", dismissable=True), new_timestamps
    else:
        return dbc.Alert(msg, color="danger", dismissable=True), no_update
@callback(
    Output('refresh-status', 'children'),
    Input('refresh-bq-btn', 'n_clicks'),
    Input('refresh-gcs-btn', 'n_clicks'),
    prevent_initial_call=True
)
def handle_refresh(bq_clicks, gcs_clicks):
    """Handle data refresh for ALL dashboards"""
    if not ctx.triggered_id:
        return no_update
    
    if ctx.triggered_id == "refresh-bq-btn":
        # Refresh ICARUS
        success1, msg1 = refresh_bq_to_staging()
        # Refresh Merged tables
        from app.dashboards.all_metrics_merged.data import refresh_merged_bq_to_staging
        success2, msg2 = refresh_merged_bq_to_staging()
        
        if success1 and success2:
            return dbc.Alert(f"{msg1} | {msg2}", color="success", dismissable=True)
        else:
            errors = []
            if not success1: errors.append(msg1)
            if not success2: errors.append(msg2)
            return dbc.Alert(f"Partial failure: {' | '.join(errors)}", color="warning", dismissable=True)
    
    elif ctx.triggered_id == "refresh-gcs-btn":
        # Refresh ICARUS
        success1, msg1 = refresh_gcs_from_staging()
        # Refresh Merged tables
        from app.dashboards.all_metrics_merged.data import refresh_merged_gcs_from_staging
        success2, msg2 = refresh_merged_gcs_from_staging()
        
        if success1 and success2:
            return dbc.Alert(f"{msg1} | {msg2}", color="success", dismissable=True)
        else:
            errors = []
            if not success1: errors.append(msg1)
            if not success2: errors.append(msg2)
            return dbc.Alert(f"Partial failure: {' | '.join(errors)}", color="warning", dismissable=True)
    
    return no_update
@callback(
    Output("page-store", "data", allow_duplicate=True),
    Input("nav-btn-daedalus", "n_clicks"),
    prevent_initial_call=True,
)
def navigate_to_daedalus(n_clicks):
    if n_clicks:
        return "daedalus"
    return no_update

@callback(
    Output("page-store", "data", allow_duplicate=True),
    Input("nav-to-admin-btn", "n_clicks"),
    prevent_initial_call=True,
)
def navigate_to_admin(n_clicks):
    if n_clicks:
        return "admin"
    return no_update

# =============================================================================
# REGISTER DASHBOARD CALLBACKS
# =============================================================================
historical_callbacks.register_callbacks(app)
multi_callbacks.register_callbacks(app)
merged_callbacks.register_callbacks(app)
daedalus_callbacks.register_callbacks(app)
admin_panel_callbacks.register_callbacks(app)

# =============================================================================
# RUN APPLICATION
# =============================================================================

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
