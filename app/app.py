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
    get_readonly_users_for_dashboard, get_session_data
)
from app.bigquery_client import (
    load_date_bounds, load_plan_groups, load_pivot_data, load_all_chart_data,
    refresh_bq_to_staging, refresh_gcs_from_staging, get_cache_info
)
from app.charts import build_line_chart, get_chart_config, create_legend_component
from app.colors import build_plan_color_map

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
        logger.info("🚀 Preloading data at startup...")
        start = datetime.now()
        
        # Load date bounds
        date_bounds = load_date_bounds()
        logger.info(f"  ✓ Date bounds loaded: {date_bounds['min_date']} to {date_bounds['max_date']}")
        
        # Load plan groups for both active and inactive
        active_plans = load_plan_groups("Active")
        logger.info(f"  ✓ Active plans loaded: {len(active_plans.get('Plan_Name', []))} plans")
        
        inactive_plans = load_plan_groups("Inactive")
        logger.info(f"  ✓ Inactive plans loaded: {len(inactive_plans.get('Plan_Name', []))} plans")
        
        # Get cache info
        cache_info = get_cache_info()
        logger.info(f"  ✓ Cache info loaded")
        
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"✅ Preloading complete in {elapsed:.2f}s")
        
    except Exception as e:
        logger.error(f"❌ Preloading failed: {e}")

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


def get_plans_by_app(plan_groups):
    """Group plans by App_Name"""
    result = {}
    for app, plan in zip(plan_groups["App_Name"], plan_groups["Plan_Name"]):
        if app not in result:
            result[app] = []
        if plan not in result[app]:
            result[app].append(plan)
    return result


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
                        dbc.Input(
                            id="login-password",
                            placeholder="Enter your password",
                            type="password",
                            className="mb-3"
                        ),
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
        status = "✅ Active" if is_enabled else "⏸️ Disabled"
        bq_display = cache_info.get("last_bq_refresh", "--") if is_enabled else "--"
        gcs_display = cache_info.get("last_gcs_refresh", "--") if is_enabled else "--"
        
        if is_enabled:
            # Clickable row — dashboard name is a styled button
            name_cell = html.Td(
                html.A(
                    f"📊 {dashboard['name']}",
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
    
    return html.Div([
        # Header with menu
        dbc.Row([
            dbc.Col(width=9),
            dbc.Col([
                dbc.Button("🚪 Logout", id="logout-btn", color="secondary", size="sm", className="me-2"),
                dbc.DropdownMenu(
                    label="⋮",
                    children=[
                        dbc.DropdownMenuItem("🔧 Admin Panel", id="admin-panel-btn") if user and user.get("role") == "admin" else None,
                        dbc.DropdownMenuItem(divider=True) if user and user.get("role") == "admin" else None,
                        dbc.DropdownMenuItem(f"User: {user['name']}", disabled=True) if user else None,
                        dbc.DropdownMenuItem(f"Role: {'Admin' if user and user.get('role') == 'admin' else 'Read Only'}", disabled=True) if user else None,
                    ],
                    
                    color="secondary"
                )
            ], width=3, style={"textAlign": "right"})
        ], className="mb-3"),
        
        # Logo and welcome
        get_header_component(theme, "large", True, True, user["name"] if user else ""),
        
        # Unified clickable dashboard table
        html.H4("📊 Available Dashboards", className="mb-3"),
        dbc.Table(
            [table_header, table_body],
            striped=True, bordered=True, hover=True, className="mb-4"
        )
    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px"
    })


def create_icarus_historical_layout(user, theme="dark"):
    """Create ICARUS Historical dashboard layout"""
    colors = get_theme_colors(theme)
    cache_info = get_cache_info()
    
    return html.Div([
        # Header
        dbc.Row([
            dbc.Col([
                dbc.Button("← Back", id="back-to-landing", color="secondary", size="sm")
            ], width=2),
            dbc.Col([
                html.H4(
                    "ICARUS - Plan (Historical)",
                    style={"textAlign": "center", "color": colors["text_primary"], "margin": "0"}
                )
            ], width=8),
            dbc.Col([
                dbc.Button("🚪 Logout", id="logout-btn", color="secondary", size="sm", className="me-2"),
                dbc.DropdownMenu(
                    label="⋮",
                    children=[
                        dbc.DropdownMenuItem("📄 Export Full Dashboard as PDF", disabled=True),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem(f"User: {user['name']}" if user else "User: --", disabled=True),
                    ],
                    
                    color="secondary"
                )
            ], width=2, style={"textAlign": "right"})
        ], className="mb-4", align="center"),
        
        # Refresh section
        dbc.Row([
            dbc.Col(width=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Strong("🔄 Data Refresh"),
                        dbc.Row([
                            dbc.Col([
                                dbc.Button("Refresh BQ", id="refresh-bq-btn", color="primary", size="sm", className="w-100")
                            ], width=6),
                            dbc.Col([
                                html.Small(f"Last: {cache_info.get('last_bq_refresh', '--')}", style={"color": colors["text_secondary"]})
                            ], width=6)
                        ], className="mb-2"),
                        dbc.Row([
                            dbc.Col([
                                dbc.Button("Refresh GCS", id="refresh-gcs-btn", color="primary", size="sm", className="w-100")
                            ], width=6),
                            dbc.Col([
                                html.Small(f"Last: {cache_info.get('last_gcs_refresh', '--')}", style={"color": colors["text_secondary"]})
                            ], width=6)
                        ]),
                        html.Div(id="refresh-status")
                    ])
                ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}"})
            ], width=4)
        ], className="mb-4"),
        
        # Tabs for Active/Inactive
        dbc.Tabs([
            dbc.Tab(
                html.Div(id="active-tab-content"),
                label="📈 Active",
                tab_id="active"
            ),
            dbc.Tab(
                html.Div(id="inactive-tab-content"),
                label="📉 Inactive",
                tab_id="inactive"
            )
        ], id="dashboard-tabs", active_tab="active", className="mb-4"),
        
        # Hidden stores for filter state
        dcc.Store(id="active-filter-state", data={}),
        dcc.Store(id="inactive-filter-state", data={})
        
    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px"
    })


def create_filters_layout(plan_groups, min_date, max_date, prefix, theme="dark"):
    """Create filters section layout"""
    colors = get_theme_colors(theme)
    
    plans_by_app = get_plans_by_app(plan_groups)
    app_names = sorted(plans_by_app.keys())
    
# Plan group checkboxes - show 2 visible, rest collapsed
    plan_checkboxes = []
    for app_name in app_names:
        plans = sorted(plans_by_app.get(app_name, []))
        visible_plans = plans[:2]
        hidden_plans = plans[2:]
        extra_count = len(hidden_plans)
        
        visible_options = [{"label": plan, "value": plan} for plan in visible_plans]
        hidden_options = [{"label": plan, "value": plan} for plan in hidden_plans]
        
        default_visible = [DEFAULT_PLAN] if DEFAULT_PLAN in visible_plans else []
        default_hidden = [DEFAULT_PLAN] if DEFAULT_PLAN in hidden_plans else []
        
        plan_checkboxes.append(
            dbc.Col([
                html.Div(app_name, className="filter-title"),
                # First 2 plans - always visible
                dbc.Checklist(
                    id={"type": f"{prefix}-plan-checklist", "app": app_name},
                    options=visible_options,
                    value=default_visible,
                ),
                # Remaining plans in collapse
                dbc.Collapse(
                    dbc.Checklist(
                        id={"type": f"{prefix}-plan-checklist-more", "app": app_name},
                        options=hidden_options,
                        value=default_hidden,
                    ),
                    id={"type": f"{prefix}-plan-collapse", "app": app_name},
                    is_open=False
                ),
                # Toggle link (hidden if ≤2 plans)
                html.A(
                    f"+{extra_count} more",
                    id={"type": f"{prefix}-plan-toggle", "app": app_name},
                    n_clicks=0,
                    style={
                        "cursor": "pointer",
                        "color": "#999999",
                        "fontSize": "12px",
                        "display": "block" if extra_count > 0 else "none",
                        "marginTop": "4px"
                    }
                )
            ], width=2)
        )
    
    # Metrics checkboxes
    metrics_options = [{"label": METRICS_CONFIG[m]["display"], "value": m} for m in METRICS_CONFIG.keys()]
    
    return dbc.Accordion([
        dbc.AccordionItem([
            # Row 1: Date Range, BC, Cohort, Reset
            dbc.Row([
                dbc.Col([
                    html.Div("Date Range", className="filter-title"),
                    dbc.Row([
                        dbc.Col([
                            dcc.DatePickerSingle(
                                id=f"{prefix}-from-date",
                                date=min_date,
                                min_date_allowed=min_date,
                                max_date_allowed=max_date,
                                display_format="YYYY-MM-DD"
                            )
                        ], width=6),
                        dbc.Col([
                            dcc.DatePickerSingle(
                                id=f"{prefix}-to-date",
                                date=max_date,
                                min_date_allowed=min_date,
                                max_date_allowed=max_date,
                                display_format="YYYY-MM-DD"
                            )
                        ], width=6)
                    ])
                ], width=3),
                dbc.Col([
                    html.Div("Billing Cycle", className="filter-title"),
                    dbc.Select(
                        id=f"{prefix}-bc",
                        options=[{"label": str(bc), "value": bc} for bc in BC_OPTIONS],
                        value=DEFAULT_BC
                    )
                ], width=2),
                dbc.Col([
                    html.Div("Cohort", className="filter-title"),
                    dbc.Select(
                        id=f"{prefix}-cohort",
                        options=[{"label": c, "value": c} for c in COHORT_OPTIONS],
                        value=DEFAULT_COHORT
                    )
                ], width=2),
                dbc.Col([
                    html.Div(" ", className="filter-title"),
                    dbc.Button("🔄 Reset", id=f"{prefix}-reset-btn", color="secondary", className="w-100")
                ], width=2)
            ], className="mb-4"),
            
            html.Hr(),
            
            # Row 2: Plan Groups
            html.Div("Plan Groups", className="filter-title"),
            dbc.Row(plan_checkboxes[:6]),
            dbc.Row(plan_checkboxes[6:]) if len(plan_checkboxes) > 6 else None,
            
            html.Hr(),
            
            # Row 3: Metrics
            dbc.Row([
                dbc.Col([
                    html.Div("Metrics", className="filter-title"),
                    dbc.Checklist(
                        id=f"{prefix}-metrics",
                        options=metrics_options,
                        value=list(METRICS_CONFIG.keys()),
                        inline=True
                    )
                ])
            ])
        ], title="📊 Filters")
    ], start_collapsed=False)


def create_admin_layout(theme="dark"):
    """Create admin panel layout"""
    colors = get_theme_colors(theme)
    users = get_all_users()
    
    # Users table data
    user_data = []
    for user_id, user_info in users.items():
        user_data.append({
            "User ID": user_id,
            "Name": user_info["name"],
            "Role": get_role_display(user_info["role"]),
            "Password": "••••••••"
        })
    
    users_df = pd.DataFrame(user_data)
    
    # Dashboard access table
    access_data = []
    for dashboard in DASHBOARDS:
        readonly_users = get_readonly_users_for_dashboard(dashboard["id"])
        users_display = ", ".join(readonly_users) if readonly_users else "—"
        access_data.append({
            "Dashboard": dashboard["name"],
            "Read Only Users": users_display
        })
    
    access_df = pd.DataFrame(access_data)
    
    return dbc.Modal([
        dbc.ModalHeader([
            dbc.ModalTitle("Admin Panel"),
            dbc.Button("✕", id="close-admin-modal", color="light", size="sm", className="ms-auto")
        ]),
        dbc.ModalBody([
            # Users section
            html.H5("👥 Users"),
            dbc.Table.from_dataframe(users_df, striped=True, bordered=True, hover=True, className="mb-4"),
            
            html.Hr(),
            
            # Dashboard access section
            html.H5("📊 Dashboard Access"),
            html.Small("Note: Admin users have access to all dashboards.", className="text-muted"),
            dbc.Table.from_dataframe(access_df, striped=True, bordered=True, hover=True, className="mb-4"),
            
            html.Hr(),
            
            # Add new user section
            dbc.Accordion([
                dbc.AccordionItem([
                    dbc.Row([
                        dbc.Col([
                            dbc.Input(id="new-user-name", placeholder="Display Name", className="mb-2"),
                            dbc.Input(id="new-user-id", placeholder="Login ID", className="mb-2")
                        ], width=6),
                        dbc.Col([
                            dbc.Input(id="new-user-password", placeholder="Password", type="password", className="mb-2"),
                            dbc.Select(
                                id="new-user-role",
                                options=[{"label": ROLE_DISPLAY[r], "value": r} for r in ROLE_OPTIONS],
                                value="readonly",
                                className="mb-2"
                            )
                        ], width=6)
                    ]),
                    dbc.Button("Create User", id="create-user-btn", color="primary"),
                    html.Div(id="create-user-status")
                ], title="➕ Add New User")
            ])
        ])
    ], id="admin-modal", size="xl", is_open=False)


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
    # We can't inject CSS directly, but layouts will re-render with new theme
    # The inline styles in layouts will handle theming
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

# Clientside callbacks for plan group expand/collapse (no server round-trip)
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

@callback(
    Output('admin-modal', 'is_open'),
    Input('admin-panel-btn', 'n_clicks'),
    Input('close-admin-modal', 'n_clicks'),
    State('admin-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_admin_modal(open_click, close_click, is_open):
    """Toggle admin modal - only opens on explicit button click"""
    triggered = ctx.triggered_id
    if triggered == "admin-panel-btn" and open_click:
        return True
    elif triggered == "close-admin-modal" and close_click:
        return False
    return False  # Default to closed instead of preserving state


@callback(
    Output('active-tab-content', 'children'),
    Input('dashboard-tabs', 'active_tab'),
    Input('session-store', 'data'),
    State('theme-store', 'data'),
    prevent_initial_call=True
)
def load_active_tab(active_tab, session_data, theme):
    """Load content for Active tab"""
    if active_tab != "active":
        return no_update
    
    theme = theme or "dark"
    
    try:
        date_bounds = load_date_bounds()
        plan_groups = load_plan_groups("Active")
        
        if not plan_groups["Plan_Name"]:
            return dbc.Alert("No active plans found.", color="warning")
        
        return html.Div([
            create_filters_layout(plan_groups, date_bounds["min_date"], date_bounds["max_date"], "active", theme),
            html.Div([
                dbc.Button("Load Data", id="active-load-btn", color="primary", className="mt-3 mb-3")
            ], style={"textAlign": "center"}),
            html.Hr(),
            html.Div(id="active-pivot-container"),
            html.Div(id="active-charts-container")
        ])
    except Exception as e:
        return dbc.Alert(f"Error loading data: {str(e)}", color="danger")


@callback(
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
        date_bounds = load_date_bounds()
        plan_groups = load_plan_groups("Inactive")
        
        if not plan_groups["Plan_Name"]:
            return dbc.Alert("No inactive plans found.", color="warning")
        
        return html.Div([
            create_filters_layout(plan_groups, date_bounds["min_date"], date_bounds["max_date"], "inactive", theme),
            html.Div([
                dbc.Button("Load Data", id="inactive-load-btn", color="primary", className="mt-3 mb-3")
            ], style={"textAlign": "center"}),
            html.Hr(),
            html.Div(id="inactive-pivot-container"),
            html.Div(id="inactive-charts-container")
        ])
    except Exception as e:
        return dbc.Alert(f"Error loading data: {str(e)}", color="danger")


@callback(
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
        pivot_regular = load_pivot_data(from_date, to_date, int(bc), cohort, selected_plans, metrics, "Regular", "Active")
        pivot_crystal = load_pivot_data(from_date, to_date, int(bc), cohort, selected_plans, metrics, "Crystal Ball", "Active")
        
        df_regular, date_cols_regular = process_pivot_data(pivot_regular, metrics, False)
        df_crystal, date_cols_crystal = process_pivot_data(pivot_crystal, metrics, True)
        
        pivot_content = []
        
        if df_regular is not None and not df_regular.empty:
            pivot_content.append(html.H5("📊 Plan Overview (Regular)"))
            pivot_content.append(
                dag.AgGrid(
                    rowData=df_regular.to_dict('records'),
                    columnDefs=[{"field": c, "pinned": "left" if c in ["App", "Plan", "Metric"] else None} for c in df_regular.columns],
                    defaultColDef={"resizable": True, "sortable": True, "filter": True},
                    className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
                    style={"height": "400px"}
                )
            )
        
        if df_crystal is not None and not df_crystal.empty:
            pivot_content.append(html.Br())
            pivot_content.append(html.H5("🔮 Plan Overview (Crystal Ball)"))
            pivot_content.append(
                dag.AgGrid(
                    rowData=df_crystal.to_dict('records'),
                    columnDefs=[{"field": c, "pinned": "left" if c in ["App", "Plan", "Metric"] else None} for c in df_crystal.columns],
                    defaultColDef={"resizable": True, "sortable": True, "filter": True},
                    className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
                    style={"height": "400px"}
                )
            )
        
        # Load chart data
        chart_metric_names = [cm["metric"] for cm in CHART_METRICS]
        all_regular_data = load_all_chart_data(from_date, to_date, int(bc), cohort, selected_plans, chart_metric_names, "Regular", "Active")
        all_crystal_data = load_all_chart_data(from_date, to_date, int(bc), cohort, selected_plans, chart_metric_names, "Crystal Ball", "Active")
        
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
                        dcc.Graph(figure=fig_regular, config=get_chart_config(), style={"height": "350px"})
                    ], width=6),
                    dbc.Col([
                        html.H6(f"{display_title} (Crystal Ball)", style={"color": colors["text_primary"]}),
                        create_legend_component(plans_crystal, color_map_crystal, theme) if plans_crystal else None,
                        dcc.Graph(figure=fig_crystal, config=get_chart_config(), style={"height": "350px"})
                    ], width=6)
                ], className="mb-4")
            )
        
        return html.Div(pivot_content), html.Div(charts_content)
        
    except Exception as e:
        return dbc.Alert(f"Error loading data: {str(e)}", color="danger"), None


@callback(
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
        pivot_regular = load_pivot_data(from_date, to_date, int(bc), cohort, selected_plans, metrics, "Regular", "Inactive")
        pivot_crystal = load_pivot_data(from_date, to_date, int(bc), cohort, selected_plans, metrics, "Crystal Ball", "Inactive")
        
        df_regular, date_cols_regular = process_pivot_data(pivot_regular, metrics, False)
        df_crystal, date_cols_crystal = process_pivot_data(pivot_crystal, metrics, True)
        
        pivot_content = []
        
        if df_regular is not None and not df_regular.empty:
            pivot_content.append(html.H5("📊 Plan Overview (Regular)"))
            pivot_content.append(
                dag.AgGrid(
                    rowData=df_regular.to_dict('records'),
                    columnDefs=[{"field": c, "pinned": "left" if c in ["App", "Plan", "Metric"] else None} for c in df_regular.columns],
                    defaultColDef={"resizable": True, "sortable": True, "filter": True},
                    className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
                    style={"height": "400px"}
                )
            )
        
        if df_crystal is not None and not df_crystal.empty:
            pivot_content.append(html.Br())
            pivot_content.append(html.H5("🔮 Plan Overview (Crystal Ball)"))
            pivot_content.append(
                dag.AgGrid(
                    rowData=df_crystal.to_dict('records'),
                    columnDefs=[{"field": c, "pinned": "left" if c in ["App", "Plan", "Metric"] else None} for c in df_crystal.columns],
                    defaultColDef={"resizable": True, "sortable": True, "filter": True},
                    className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
                    style={"height": "400px"}
                )
            )
        
        # Load chart data
        chart_metric_names = [cm["metric"] for cm in CHART_METRICS]
        all_regular_data = load_all_chart_data(from_date, to_date, int(bc), cohort, selected_plans, chart_metric_names, "Regular", "Inactive")
        all_crystal_data = load_all_chart_data(from_date, to_date, int(bc), cohort, selected_plans, chart_metric_names, "Crystal Ball", "Inactive")
        
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
                        dcc.Graph(figure=fig_regular, config=get_chart_config(), style={"height": "350px"})
                    ], width=6),
                    dbc.Col([
                        html.H6(f"{display_title} (Crystal Ball)", style={"color": colors["text_primary"]}),
                        create_legend_component(plans_crystal, color_map_crystal, theme) if plans_crystal else None,
                        dcc.Graph(figure=fig_crystal, config=get_chart_config(), style={"height": "350px"})
                    ], width=6)
                ], className="mb-4")
            )
        
        return html.Div(pivot_content), html.Div(charts_content)
        
    except Exception as e:
        return dbc.Alert(f"Error loading data: {str(e)}", color="danger"), None


@callback(
    Output('refresh-status', 'children'),
    Input('refresh-bq-btn', 'n_clicks'),
    Input('refresh-gcs-btn', 'n_clicks'),
    prevent_initial_call=True
)
def handle_refresh(bq_clicks, gcs_clicks):
    """Handle data refresh"""
    if not ctx.triggered_id:
        return no_update
    
    if ctx.triggered_id == "refresh-bq-btn":
        success, msg = refresh_bq_to_staging()
        color = "success" if success else "danger"
        return dbc.Alert(msg, color=color, dismissable=True)
    
    elif ctx.triggered_id == "refresh-gcs-btn":
        success, msg = refresh_gcs_from_staging()
        color = "success" if success else "danger"
        return dbc.Alert(msg, color=color, dismissable=True)
    
    return no_update


@callback(
    Output('create-user-status', 'children'),
    Input('create-user-btn', 'n_clicks'),
    State('new-user-name', 'value'),
    State('new-user-id', 'value'),
    State('new-user-password', 'value'),
    State('new-user-role', 'value'),
    prevent_initial_call=True
)
def create_new_user(n_clicks, name, user_id, password, role):
    """Create a new user"""
    if not n_clicks:
        return no_update
    
    if not all([name, user_id, password]):
        return dbc.Alert("Please fill all required fields", color="warning")
    
    dashboards = [] if role == "readonly" else "all"
    success, msg = add_user(user_id, password, role, name, dashboards)
    
    color = "success" if success else "danger"
    return dbc.Alert(msg, color=color, dismissable=True)


# =============================================================================
# RUN APPLICATION
# =============================================================================

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8080)
