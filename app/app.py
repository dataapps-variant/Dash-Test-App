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
from app.dashboards.all_metrics_merged.data import preload_merged_tables
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
    
    # Build clickable dashboard table rows
    table_header = html.Thead(
        html.Tr([
            html.Th("Dashboard", style={"width": "40%"}),
            html.Th("Status", style={"width": "15%"}),
            html.Th("Last BQ Refresh", style={"width": "22%"}),
            html.Th("Last GCS Refresh", style={"width": "23%"})
        ])
    )
    
    table_rows = []
    for dashboard in DASHBOARDS:
        is_enabled = dashboard.get("enabled", False)
        status = "Active" if is_enabled else "Disabled"
        bq_display = cache_info.get("last_bq_refresh", "--") if is_enabled else "--"
        gcs_display = cache_info.get("last_gcs_refresh", "--") if is_enabled else "--"
        
        if is_enabled:
            # Clickable row — dashboard name is a styled button
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
            # Disabled row — grayed out
            name_cell = html.Td(
                f"  {dashboard['name']}",
                style={"color": "#555555"}
            )
            row_style = {"opacity": "0.5"}
        
        table_rows.append(
            html.Tr([
                name_cell,
                html.Td(status, style={"color": "#555555"} if not is_enabled else {}),
                html.Td(bq_display, style={"color": "#555555"} if not is_enabled else {}),
                html.Td(gcs_display, style={"color": "#555555"} if not is_enabled else {})
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
                    dbc.Button("Logout", id="logout-btn", color="secondary", size="sm", className="me-2"),
                    dbc.DropdownMenu(
                        label=":",
                        children=[
                            dbc.DropdownMenuItem("Admin Panel", id="admin-panel-btn") if show_admin else None,
                            dbc.DropdownMenuItem(divider=True) if show_admin else None,
                            dbc.DropdownMenuItem(f"User: {user['name']}", disabled=True) if user else None,
                            dbc.DropdownMenuItem(f"Role: {role_text}", disabled=True) if user else None,
                        ],
                        
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
        )
    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px"
    })


def create_admin_layout(theme="dark"):
    """Create admin panel with users table and edit user modal"""
    colors = get_theme_colors(theme)
    
    # Dashboard options for dropdowns
    dashboard_options = [{"label": d["name"], "value": d["id"]} for d in DASHBOARDS]
    
    return html.Div([
        # =====================================================================
        # ADMIN MODAL - Main Panel
        # =====================================================================
        dbc.Modal([
            dbc.ModalHeader([
                dbc.ModalTitle("Admin Panel"),
                dbc.Button("✕", id="close-admin-modal", color="light", size="sm", className="ms-auto")
            ]),
            dbc.ModalBody([
                # Users section header
                dbc.Row([
                    dbc.Col([
                        html.H5("Users", className="mb-0")
                    ], width=8),
                    dbc.Col([
                        dbc.Button("+ Add New User", id="admin-add-user-btn", color="primary", size="sm")
                    ], width=4, style={"textAlign": "right"})
                ], className="mb-3", align="center"),
                
                html.Small(
                    "Super Admin has full access and cannot be edited. Admin users have access to all dashboards and apps.",
                    style={"color": "#666666"}
                ),
                html.Hr(),
                
                # Dynamic users table (populated by callback)
                html.Div(id="admin-users-table"),
                
                # Status message
                html.Div(id="admin-status")
            ])
        ], id="admin-modal", size="xl", is_open=False, scrollable=True),
        
        # =====================================================================
        # EDIT USER MODAL
        # =====================================================================
        dbc.Modal([
            dbc.ModalHeader([
                dbc.ModalTitle(id="edit-modal-title"),
                dbc.Button("✕", id="edit-cancel-btn", color="light", size="sm", className="ms-auto")
            ]),
            dbc.ModalBody([
                # Row 1: User ID + Password
                dbc.Row([
                    dbc.Col([
                        html.Div("User ID", className="filter-title"),
                        dbc.Input(id="edit-user-id", placeholder="Login ID", className="mb-2")
                    ], width=6),
                    dbc.Col([
                        html.Div("Password", className="filter-title"),
                        dbc.Input(id="edit-user-password", placeholder="Password", type="text", className="mb-2")
                    ], width=6)
                ]),
                
                # Row 2: Name + Role
                dbc.Row([
                    dbc.Col([
                        html.Div("Name", className="filter-title"),
                        dbc.Input(id="edit-user-name", placeholder="Display Name", className="mb-2")
                    ], width=6),
                    dbc.Col([
                        html.Div("Role", className="filter-title"),
                        dbc.Select(id="edit-user-role", className="mb-2")
                    ], width=6)
                ]),
                
                html.Hr(),
                
                # Dashboard & App Access section (only for readonly)
                html.Div(id="edit-access-section", children=[
                    html.H6("Dashboard & App Access", className="mb-3"),
                    
                    # Current access display
                    html.Div(id="edit-access-display", className="mb-3"),
                    
                    html.Hr(),
                    
                    # Add access row
                    html.Div("Add Access", className="filter-title"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Select(
                                id="edit-add-dashboard",
                                options=dashboard_options,
                                placeholder="Select Dashboard..."
                            )
                        ], width=5),
                        dbc.Col([
                            dbc.Checklist(
                                id="edit-add-apps",
                                options=[],
                                value=[],
                                inline=True,
                                style={"fontSize": "13px"}
                            )
                        ], width=5),
                        dbc.Col([
                            dbc.Button("Add", id="edit-add-access-btn", color="primary", size="sm", className="w-100")
                        ], width=2)
                    ], className="mb-3", align="center"),
                ]),
                
                html.Hr(),
                
                # Status message
                html.Div(id="edit-status"),
                
                # Action buttons
                dbc.Row([
                    dbc.Col([
                        dbc.Button("Save", id="edit-save-btn", color="primary", className="me-2"),
                        dbc.Button("Cancel", id="edit-close-btn", color="secondary")
                    ], width=8),
                    dbc.Col([
                        dbc.Button("Delete User", id="edit-delete-btn", color="danger", outline=True, size="sm")
                    ], width=4, style={"textAlign": "right"})
                ])
            ])
        ], id="edit-user-modal", size="lg", is_open=False, backdrop="static"),
        
        # =====================================================================
        # DELETE CONFIRMATION MODAL
        # =====================================================================
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Confirm Delete")),
            dbc.ModalBody("Are you sure you want to delete this user? This cannot be undone."),
            dbc.ModalFooter([
                dbc.Button("Cancel", id="delete-cancel-btn", color="secondary", className="me-2"),
                dbc.Button("Delete", id="delete-confirm-btn", color="danger")
            ])
        ], id="delete-confirm-modal", is_open=False, centered=True),
        
        # =====================================================================
        # STORES
        # =====================================================================
        dcc.Store(id="admin-refresh-store", data=0),
        dcc.Store(id="edit-user-access-store", data={}),
        dcc.Store(id="edit-user-mode-store", data={"mode": "new", "user_id": ""})
    ])


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
    
    # Dynamic CSS container (using Div with dangerously_allow_html workaround)
    html.Div(id='dynamic-css-container'),
    
    # Main content
    html.Div(id='page-content'),
    
    # Admin modal
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
        return create_landing_layout(user, theme), create_admin_layout(theme)
    elif current_page == "icarus_historical":
        return create_icarus_historical_layout(user, theme), create_admin_layout(theme)
    elif current_page == "icarus_multi":
        return create_icarus_multi_layout(user, theme), create_admin_layout(theme)
    elif current_page == "all_metrics_merged":
        return create_merged_layout(user, theme), create_admin_layout(theme)    
    else:
        return create_landing_layout(user, theme), create_admin_layout(theme)

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
# ADMIN PANEL CALLBACKS
# =============================================================================

@callback(
    Output('admin-modal', 'is_open'),
    Input('admin-panel-btn', 'n_clicks'),
    Input('close-admin-modal', 'n_clicks'),
    State('admin-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_admin_modal(open_click, close_click, is_open):
    """Toggle admin modal"""
    triggered = ctx.triggered_id
    if triggered == "admin-panel-btn" and open_click:
        return True
    elif triggered == "close-admin-modal" and close_click:
        return False
    return False


@callback(
    Output('admin-users-table', 'children'),
    Input('admin-modal', 'is_open'),
    Input('admin-refresh-store', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True
)
def populate_admin_users_table(is_open, refresh_trigger, session_data):
    """Populate the users table in admin panel"""
    if not is_open:
        return no_update
    
    session_id = session_data.get('session_id') if session_data else None
    current_user = get_current_user(session_id) if session_id else None
    current_role = current_user.get("role", "readonly") if current_user else "readonly"
    
    users = get_all_users()
    
    # Build table header
    table_header = html.Thead(
        html.Tr([
            html.Th("User ID", style={"width": "10%"}),
            html.Th("Name", style={"width": "13%"}),
            html.Th("Role", style={"width": "10%"}),
            html.Th("Password", style={"width": "12%"}),
            html.Th("Dashboards", style={"width": "25%"}),
            html.Th("Apps", style={"width": "20%"}),
            html.Th("Action", style={"width": "10%"})
        ])
    )
    
    # Build table rows
    table_rows = []
    for user_id, user_info in users.items():
        role = user_info.get("role", "readonly")
        role_display = get_role_display(role)
        password = user_info.get("password", "")
        
        # Dashboards column
        dashboards = user_info.get("dashboards", [])
        if dashboards == "all" or role in ("admin", "super_admin"):
            dash_text = "All"
        elif isinstance(dashboards, list):
            dash_names = [get_dashboard_name(d) for d in dashboards]
            dash_text = ", ".join(dash_names) if dash_names else "None"
        else:
            dash_text = "None"
        
        # Apps column
        app_access = user_info.get("app_access", {})
        if role in ("admin", "super_admin"):
            apps_text = "All"
        elif app_access:
            # Collect unique apps across all dashboards
            all_apps = set()
            for dash_id, apps in app_access.items():
                all_apps.update(apps)
            if all_apps:
                apps_text = ", ".join(sorted(all_apps))
            else:
                apps_text = "All (no restrictions)"
        else:
            apps_text = "All (no restrictions)"
        
        # Action column - Edit button
        can_edit = can_manage_user(current_role, role)
        # Super admin can also edit themselves (password/name only)
        if role == "super_admin" and current_role == "super_admin":
            can_edit = True
        
        if can_edit:
            action_cell = html.Td(
                dbc.Button(
                    "Edit",
                    id={"type": "admin-edit-btn", "index": user_id},
                    color="secondary",
                    size="sm",
                    outline=True
                )
            )
        else:
            action_cell = html.Td("—", style={"color": "#555555"})
        
        table_rows.append(
            html.Tr([
                html.Td(user_id),
                html.Td(user_info.get("name", "")),
                html.Td(role_display),
                html.Td(password),
                html.Td(dash_text, style={"fontSize": "13px"}),
                html.Td(apps_text, style={"fontSize": "13px"}),
                action_cell
            ])
        )
    
    table_body = html.Tbody(table_rows)
    
    return dbc.Table(
        [table_header, table_body],
        striped=True, bordered=True, hover=True, size="sm"
    )


@callback(
    Output('edit-user-modal', 'is_open'),
    Output('edit-modal-title', 'children'),
    Output('edit-user-id', 'value'),
    Output('edit-user-id', 'disabled'),
    Output('edit-user-name', 'value'),
    Output('edit-user-password', 'value'),
    Output('edit-user-role', 'options'),
    Output('edit-user-role', 'value'),
    Output('edit-user-role', 'disabled'),
    Output('edit-user-access-store', 'data'),
    Output('edit-user-mode-store', 'data'),
    Output('edit-access-section', 'style'),
    Output('edit-delete-btn', 'style'),
    Output('edit-status', 'children'),
    Input({"type": "admin-edit-btn", "index": ALL}, "n_clicks"),
    Input("admin-add-user-btn", "n_clicks"),
    Input("edit-cancel-btn", "n_clicks"),
    Input("edit-close-btn", "n_clicks"),
    State('session-store', 'data'),
    prevent_initial_call=True
)
def open_edit_user_modal(edit_clicks, add_click, cancel_click, close_click, session_data):
    """Open/close the edit user modal and populate fields"""
    triggered = ctx.triggered_id
    
    # Close modal
    if triggered in ("edit-cancel-btn", "edit-close-btn"):
        return (False, "", "", False, "", "", [], "", False, {}, {"mode": "new", "user_id": ""}, 
                {"display": "none"}, {"display": "none"}, "")
    
    # Get current user role
    session_id = session_data.get('session_id') if session_data else None
    current_user = get_current_user(session_id) if session_id else None
    current_role = current_user.get("role", "readonly") if current_user else "readonly"
    
    # Get assignable roles
    assignable = get_assignable_roles(current_role)
    role_options = [{"label": ROLE_DISPLAY.get(r, r), "value": r} for r in assignable]
    
    # ADD NEW USER
    if triggered == "admin-add-user-btn" and add_click:
        return (
            True,                               # is_open
            "Add New User",                     # title
            "",                                 # user_id
            False,                              # user_id disabled
            "",                                 # name
            "",                                 # password
            role_options,                       # role options
            "readonly",                         # role value
            False,                              # role disabled
            {},                                 # access store
            {"mode": "new", "user_id": ""},     # mode store
            {"display": "block"},               # access section visible
            {"display": "none"},                # delete button hidden
            ""                                  # status clear
        )
    
    # EDIT EXISTING USER
    if isinstance(triggered, dict) and triggered.get("type") == "admin-edit-btn":
        user_id = triggered.get("index", "")
        
        # Check if the click actually happened
        all_clicks = [c for c in edit_clicks if c]
        if not all_clicks:
            return (no_update,) * 14
        
        users = get_all_users()
        if user_id not in users:
            return (no_update,) * 14
        
        user_info = users[user_id]
        target_role = user_info.get("role", "readonly")
        
        # Load current access
        current_access = user_info.get("app_access", {})
        
        # Role dropdown logic
        if target_role == "super_admin":
            # Super admin editing themselves - role locked
            edit_role_options = [{"label": "Super Admin", "value": "super_admin"}]
            role_disabled = True
        else:
            edit_role_options = role_options
            role_disabled = False
        
        # Access section visibility
        show_access = {"display": "block"} if target_role == "readonly" else {"display": "none"}
        
        # Delete button visibility
        can_del = can_delete_user(current_role, user_id, target_role)
        delete_style = {"display": "inline-block"} if can_del else {"display": "none"}
        
        # Dashboards for access store
        dashboards = user_info.get("dashboards", [])
        access_data = {}
        if isinstance(dashboards, list):
            for dash_id in dashboards:
                access_data[dash_id] = current_access.get(dash_id, [])
        
        return (
            True,                                   # is_open
            f"Edit User: {user_id}",                # title
            user_id,                                # user_id
            True,                                   # user_id disabled (can't change ID)
            user_info.get("name", ""),              # name
            user_info.get("password", ""),           # password
            edit_role_options,                       # role options
            target_role,                            # role value
            role_disabled,                          # role disabled
            access_data,                            # access store
            {"mode": "edit", "user_id": user_id},   # mode store
            show_access,                            # access section
            delete_style,                           # delete button
            ""                                      # status clear
        )
    
    return (no_update,) * 14


@callback(
    Output('edit-access-display', 'children'),
    Input('edit-user-access-store', 'data'),
    prevent_initial_call=True
)
def render_access_display(access_data):
    """Render the current dashboard/app access assignments"""
    if not access_data:
        return html.Small("No dashboard access assigned.", style={"color": "#666666"})
    
    rows = []
    for dash_id, apps in access_data.items():
        dash_name = get_dashboard_name(dash_id)
        apps_text = ", ".join(sorted(apps)) if apps else "All apps"
        
        rows.append(
            dbc.Row([
                dbc.Col([
                    html.Span(f"{dash_name}", style={"fontWeight": "500"}),
                    html.Span(f"  →  {apps_text}", style={"color": "#999999", "fontSize": "13px"})
                ], width=10),
                dbc.Col([
                    dbc.Button(
                        "Remove",
                        id={"type": "remove-access-btn", "index": dash_id},
                        color="danger",
                        size="sm",
                        outline=True
                    )
                ], width=2, style={"textAlign": "right"})
            ], className="mb-2", align="center",
               style={"padding": "6px 10px", "background": "#0A0A0A", "borderRadius": "6px", "border": "1px solid #1C1C1C"})
        )
    
    return html.Div(rows)


@callback(
    Output('edit-add-apps', 'options'),
    Output('edit-add-apps', 'value'),
    Input('edit-add-dashboard', 'value'),
    prevent_initial_call=True
)
def load_apps_for_selected_dashboard(dashboard_id):
    """Load available apps when a dashboard is selected in the add access section"""
    if not dashboard_id:
        return [], []
    
    apps = get_available_apps_for_dashboard(dashboard_id)
    options = [{"label": app, "value": app} for app in apps]
    return options, []


@callback(
    Output('edit-user-access-store', 'data', allow_duplicate=True),
    Input('edit-add-access-btn', 'n_clicks'),
    State('edit-add-dashboard', 'value'),
    State('edit-add-apps', 'value'),
    State('edit-user-access-store', 'data'),
    prevent_initial_call=True
)
def add_access_row(n_clicks, dashboard_id, selected_apps, current_access):
    """Add a dashboard+apps access row to the store"""
    if not n_clicks or not dashboard_id:
        return no_update
    
    updated = dict(current_access) if current_access else {}
    updated[dashboard_id] = selected_apps or []
    return updated


@callback(
    Output('edit-user-access-store', 'data', allow_duplicate=True),
    Input({"type": "remove-access-btn", "index": ALL}, "n_clicks"),
    State('edit-user-access-store', 'data'),
    prevent_initial_call=True
)
def remove_access_row(remove_clicks, current_access):
    """Remove a dashboard access row from the store"""
    if not any(c for c in remove_clicks if c):
        return no_update
    
    triggered = ctx.triggered_id
    if not isinstance(triggered, dict):
        return no_update
    
    dash_id = triggered.get("index", "")
    updated = dict(current_access) if current_access else {}
    
    if dash_id in updated:
        del updated[dash_id]
    
    return updated


@callback(
    Output('edit-access-section', 'style', allow_duplicate=True),
    Input('edit-user-role', 'value'),
    prevent_initial_call=True
)
def toggle_access_section_visibility(role):
    """Show/hide access section based on selected role"""
    if role == "readonly":
        return {"display": "block"}
    return {"display": "none"}


@callback(
    Output('edit-status', 'children', allow_duplicate=True),
    Output('admin-refresh-store', 'data', allow_duplicate=True),
    Output('edit-user-modal', 'is_open', allow_duplicate=True),
    Input('edit-save-btn', 'n_clicks'),
    State('edit-user-id', 'value'),
    State('edit-user-name', 'value'),
    State('edit-user-password', 'value'),
    State('edit-user-role', 'value'),
    State('edit-user-access-store', 'data'),
    State('edit-user-mode-store', 'data'),
    State('admin-refresh-store', 'data'),
    prevent_initial_call=True
)
def save_user_from_modal(n_clicks, user_id, name, password, role, access_data, mode_data, refresh_count):
    """Save user (create or update) from the edit modal"""
    if not n_clicks:
        return no_update, no_update, no_update
    
    if not user_id or not name or not password:
        return dbc.Alert("Please fill all required fields (User ID, Name, Password).", color="warning"), no_update, no_update
    
    mode = mode_data.get("mode", "new") if mode_data else "new"
    
    # Build dashboards list and app_access from access_data
    if role in ("admin", "super_admin"):
        dashboards = "all"
        app_access = {}
    else:
        dashboards = list(access_data.keys()) if access_data else []
        app_access = access_data if access_data else {}
    
    if mode == "new":
        success, msg = add_user(user_id, password, role, name, dashboards, app_access)
    else:
        success, msg = update_user(user_id, password=password, role=role, name=name,
                                   dashboards=dashboards, app_access=app_access)
    
    if success:
        new_refresh = (refresh_count or 0) + 1
        return "", new_refresh, False  # Close modal on success
    else:
        return dbc.Alert(msg, color="danger"), no_update, no_update


@callback(
    Output('delete-confirm-modal', 'is_open'),
    Input('edit-delete-btn', 'n_clicks'),
    Input('delete-cancel-btn', 'n_clicks'),
    Input('delete-confirm-btn', 'n_clicks'),
    State('delete-confirm-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_delete_confirmation(delete_click, cancel_click, confirm_click, is_open):
    """Toggle delete confirmation modal"""
    triggered = ctx.triggered_id
    if triggered == "edit-delete-btn" and delete_click:
        return True
    if triggered in ("delete-cancel-btn", "delete-confirm-btn"):
        return False
    return False


@callback(
    Output('edit-status', 'children', allow_duplicate=True),
    Output('admin-refresh-store', 'data', allow_duplicate=True),
    Output('edit-user-modal', 'is_open', allow_duplicate=True),
    Input('delete-confirm-btn', 'n_clicks'),
    State('edit-user-mode-store', 'data'),
    State('admin-refresh-store', 'data'),
    prevent_initial_call=True
)
def execute_delete_user(n_clicks, mode_data, refresh_count):
    """Actually delete the user after confirmation"""
    if not n_clicks:
        return no_update, no_update, no_update
    
    user_id = mode_data.get("user_id", "") if mode_data else ""
    if not user_id:
        return dbc.Alert("No user selected for deletion.", color="danger"), no_update, no_update
    
    success, msg = delete_user(user_id)
    
    if success:
        new_refresh = (refresh_count or 0) + 1
        return "", new_refresh, False  # Close edit modal
    else:
        return dbc.Alert(msg, color="danger"), no_update, no_update


# =============================================================================
# SHARED CALLBACKS (used by both dashboards)
# =============================================================================

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
    Output('refresh-status', 'children', allow_duplicate=True),
    Input('refresh-merged-no-spend-btn', 'n_clicks'),
    prevent_initial_call=True
)
def handle_merged_no_spend_refresh(n_clicks):
    """Refresh only merged tables (excluding spend) then activate"""
    if not n_clicks:
        return no_update
    
    from app.dashboards.all_metrics_merged.data import refresh_merged_bq_to_staging, refresh_merged_gcs_from_staging
    
    success1, msg1 = refresh_merged_bq_to_staging(skip_keys=["spend"])
    if not success1:
        return dbc.Alert(f"BQ failed: {msg1}", color="danger", dismissable=True)
    
    success2, msg2 = refresh_merged_gcs_from_staging(skip_keys=["spend"])
    if not success2:
        return dbc.Alert(f"GCS failed: {msg2}", color="danger", dismissable=True)
    
    return dbc.Alert(f"{msg1} | {msg2}", color="success", dismissable=True)
# =============================================================================
# REGISTER DASHBOARD CALLBACKS
# =============================================================================
historical_callbacks.register_callbacks(app)
multi_callbacks.register_callbacks(app)
merged_callbacks.register_callbacks(app)

# =============================================================================
# RUN APPLICATION
# =============================================================================

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8080)
