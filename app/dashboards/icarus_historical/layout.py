"""
Layout for ICARUS Historical Dashboard

Extracted from app.py - contains:
- create_icarus_historical_layout() - main dashboard layout
- create_filters_layout() - filter accordion with date range, BC, cohort, plans, metrics
- Helper functions for plan grouping and filtering
"""

from dash import html, dcc
import dash_bootstrap_components as dbc

from app.config import (
    BC_OPTIONS, COHORT_OPTIONS, DEFAULT_BC, DEFAULT_COHORT, DEFAULT_PLAN,
    METRICS_CONFIG
)
from app.theme import get_theme_colors
from app.bigquery_client import get_cache_info


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_plans_by_app(plan_groups):
    """Group plans by App_Name"""
    result = {}
    for app, plan in zip(plan_groups["App_Name"], plan_groups["Plan_Name"]):
        if app not in result:
            result[app] = []
        if plan not in result[app]:
            result[app].append(plan)
    return result


def filter_plan_groups_by_apps(plan_groups, allowed_apps):
    """
    Filter plan_groups dict to only include plans from allowed apps.
    If allowed_apps is None, return all (no filtering).
    """
    if allowed_apps is None:
        return plan_groups
    
    filtered_indices = [
        i for i in range(len(plan_groups["App_Name"]))
        if plan_groups["App_Name"][i] in allowed_apps
    ]
    
    return {
        "App_Name": [plan_groups["App_Name"][i] for i in filtered_indices],
        "Plan_Name": [plan_groups["Plan_Name"][i] for i in filtered_indices]
    }


# =============================================================================
# LAYOUT FUNCTIONS
# =============================================================================

def create_icarus_historical_layout(user, theme="dark"):
    """Create ICARUS Historical dashboard layout"""
    colors = get_theme_colors(theme)
    cache_info = get_cache_info()
    
    return html.Div([
        # Header - Back left, Title center, Logout right
        dbc.Row([
            dbc.Col([
                dbc.Button("\u2190 Back", id="back-to-landing", color="secondary", size="sm")
            ], width=2),
            dbc.Col([
                html.H5(
                    "ICARUS - Plan (Historical)",
                    style={"textAlign": "center", "color": colors["text_primary"], "fontWeight": "600", "margin": "0"}
                )
            ], width=6),
            dbc.Col([
                html.Div([
                    dbc.Button("Logout", id="logout-btn", color="secondary", size="sm", className="me-2"),
                    dbc.DropdownMenu(
                    label=":",
                    children=[
                        dbc.DropdownMenuItem("Export Full Dashboard as PDF", disabled=True),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem(f"User: {user['name']}" if user else "User: --", disabled=True),
                    ],
                    
                    color="secondary"
                )
                ], style={"display": "flex", "alignItems": "center", "justifyContent": "flex-end", "gap": "4px"})
            ], width=4, style={"textAlign": "right"})
        ], className="mb-2", align="center"),
        
        # Refresh section - compact inline strip
        html.Div([
            dbc.Button("Refresh BQ", id="refresh-bq-btn", size="sm", className="refresh-btn-green"),
            html.Small(f"  Last: {cache_info.get('last_bq_refresh', '--')}  ", style={"color": colors["text_secondary"], "margin": "0 16px 0 8px"}),
            dbc.Button("Refresh GCS", id="refresh-gcs-btn", size="sm", className="refresh-btn-green"),
            html.Small(f"  Last: {cache_info.get('last_gcs_refresh', '--')}", style={"color": colors["text_secondary"], "marginLeft": "8px"}),
            html.Div(id="refresh-status", style={"display": "inline-block", "marginLeft": "16px"})
        ], style={"textAlign": "right", "padding": "6px 0", "marginBottom": "8px"}),
        
        # Tabs for Active/Inactive
       dbc.Tabs([
            dbc.Tab(
                dcc.Loading(
                    html.Div(id="active-tab-content"),
                    type="dot", color="#FFFFFF"
                ),
                label="Active",
                tab_id="active"
            ),
            dbc.Tab(
                dcc.Loading(
                    html.Div(id="inactive-tab-content"),
                    type="dot", color="#FFFFFF"
                ),
                label="Inactive",
                tab_id="inactive"
            )
        ], id="dashboard-tabs", active_tab="active", className="mb-2"),
        
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
                        "display": "block",
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
                dbc.Col(width=3),
                dbc.Col([
                    dbc.Button("Reset", id=f"{prefix}-reset-btn", color="secondary", className="w-100", style={"marginTop": "22px"})
                ], width=2)
           ], className="mb-2"),
            
            html.Hr(style={"margin": "10px 0"}),
            
            # Row 2: Plan Groups
            html.Div("Plan Groups", className="filter-title"),
            dbc.Row(plan_checkboxes[:6], className="mb-3"),
            dbc.Row(plan_checkboxes[6:], className="mb-3") if len(plan_checkboxes) > 6 else None,
            
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
        ], title="Filters")
    ], start_collapsed=False)
