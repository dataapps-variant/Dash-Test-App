"""
Layout for ICARUS Multi Dashboard

Similar to Historical but with:
- Single date picker (not range)
- No Billing Cycle filter (BC is the pivot dimension 0-12)
- Cohort filter
- Plan group selection (same pattern as Historical)
"""

from dash import html, dcc
import dash_bootstrap_components as dbc

from app.theme import get_theme_colors
from app.config import COHORT_OPTIONS, DEFAULT_COHORT, DEFAULT_PLAN
from app.bigquery_client import get_cache_info


# =============================================================================
# MULTI-SPECIFIC METRICS CONFIG (includes Single_Sale & T30D_New_Users)
# =============================================================================

MULTI_METRICS_CONFIG = {
    "Single_Sale": {"display": "Single Sale", "format": "number", "suffix": ""},
    "Subscriptions": {"display": "Subscriptions", "format": "number", "suffix": ""},
    "Rebills": {"display": "Rebills", "format": "number", "suffix": ""},
    "Churn_Rate": {"display": "Churn Rate", "format": "percent", "suffix": " (%)"},
    "Refund_Rate": {"display": "Refund Rate", "format": "percent", "suffix": " (%)"},
    "Gross_ARPU_Retention_Rate": {"display": "Gross ARPU Retention", "format": "percent", "suffix": " (%)"},
    "Net_ARPU_Retention_Rate": {"display": "Net ARPU Retention", "format": "percent", "suffix": " (%)"},
    "Cohort_CAC": {"display": "Cohort CAC", "format": "dollar", "suffix": " ($)"},
    "Recent_CAC": {"display": "Recent CAC", "format": "dollar", "suffix": " ($)"},
    "T30D_New_Users": {"display": "T30D New Users", "format": "number", "suffix": ""},
    "Gross_ARPU_Discounted": {"display": "Gross ARPU", "format": "dollar", "suffix": " ($)"},
    "Net_ARPU_Discounted": {"display": "Net ARPU", "format": "dollar", "suffix": " ($)"},
    "Net_LTV_Discounted": {"display": "Net LTV", "format": "dollar", "suffix": " ($)"},
    "BC4_CAC_Ceiling": {"display": "BC4 CAC Ceiling", "format": "dollar", "suffix": " ($)"},
}

# Charts to display (all metrics, Regular + Crystal Ball)
MULTI_CHART_METRICS = [
    {"display": "Single Sale", "metric": "Single_Sale", "format": "number"},
    {"display": "Subscriptions", "metric": "Subscriptions", "format": "number"},
    {"display": "Rebills", "metric": "Rebills", "format": "number"},
    {"display": "Recent LTV", "metric": "Net_LTV_Discounted", "format": "dollar"},
    {"display": "Gross ARPU", "metric": "Gross_ARPU_Discounted", "format": "dollar"},
    {"display": "Net ARPU", "metric": "Net_ARPU_Discounted", "format": "dollar"},
    {"display": "Churn", "metric": "Churn_Rate", "format": "percent"},
    {"display": "Refund", "metric": "Refund_Rate", "format": "percent"},
    {"display": "Gross Retention", "metric": "Gross_ARPU_Retention_Rate", "format": "percent"},
    {"display": "Net ARPU Retention", "metric": "Net_ARPU_Retention_Rate", "format": "percent"},
    {"display": "Cohort CAC", "metric": "Cohort_CAC", "format": "dollar"},
    {"display": "Recent CAC", "metric": "Recent_CAC", "format": "dollar"},
    {"display": "T30D New Users", "metric": "T30D_New_Users", "format": "number"},
    {"display": "BC4 CAC Ceiling", "metric": "BC4_CAC_Ceiling", "format": "dollar"},
]


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
    """Filter plan_groups to only include plans from allowed apps"""
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

def create_icarus_multi_layout(user, theme="dark"):
    """Create main layout for ICARUS Multi dashboard"""
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
                    "ICARUS - Multi",
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
        
        # Refresh section
        html.Div([
            dbc.Button("Refresh BQ", id="refresh-bq-btn", size="sm", className="refresh-btn-green"),
            html.Small(f"  Last: {cache_info.get('last_bq_refresh', '--')}  ",
                       style={"color": colors["text_secondary"], "margin": "0 16px 0 8px"}),
            dbc.Button("Refresh GCS", id="refresh-gcs-btn", size="sm", className="refresh-btn-green"),
            html.Small(f"  Last: {cache_info.get('last_gcs_refresh', '--')}",
                       style={"color": colors["text_secondary"], "marginLeft": "8px"}),
            html.Div(id="refresh-status", style={"display": "inline-block", "marginLeft": "16px"})
        ], style={"textAlign": "right", "padding": "6px 0", "marginBottom": "8px"}),
        
        # Tabs for Active/Inactive
        dbc.Tabs([
            dbc.Tab(
                dcc.Loading(html.Div(id="multi-active-tab-content"), type="dot", color="#FFFFFF"),
                label="Active", tab_id="active"
            ),
            dbc.Tab(
                dcc.Loading(html.Div(id="multi-inactive-tab-content"), type="dot", color="#FFFFFF"),
                label="Inactive", tab_id="inactive"
            )
        ], id="multi-dashboard-tabs", active_tab="active", className="mb-2"),
        
        # Hidden stores
        dcc.Store(id="multi-active-filter-state", data={}),
        dcc.Store(id="multi-inactive-filter-state", data={})
        
    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px"
    })


def create_multi_filters_layout(plan_groups, available_dates, prefix, theme="dark"):
    """
    Create filters for Multi dashboard.
    
    Differences from Historical:
    - Single date picker (dropdown) instead of date range
    - No Billing Cycle filter (BC is the pivot dimension)
    - Cohort filter stays
    - Plan groups stay
    - Metrics selection stays
    """
    colors = get_theme_colors(theme)
    
    plans_by_app = get_plans_by_app(plan_groups)
    app_names = sorted(plans_by_app.keys())
    
    # Date dropdown options (newest first)
    date_options = []
    for d in available_dates:
        if hasattr(d, 'strftime'):
            label = d.strftime("%Y-%m-%d")
        else:
            label = str(d)
        date_options.append({"label": label, "value": label})
    
    default_date = date_options[0]["value"] if date_options else None
    
    # Plan group checkboxes - same pattern as Historical (2 visible, rest collapsed)
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
                dbc.Checklist(
                    id={"type": f"{prefix}-plan-checklist", "app": app_name},
                    options=visible_options,
                    value=default_visible,
                ),
                dbc.Collapse(
                    dbc.Checklist(
                        id={"type": f"{prefix}-plan-checklist-more", "app": app_name},
                        options=hidden_options,
                        value=default_hidden,
                    ),
                    id={"type": f"{prefix}-plan-collapse", "app": app_name},
                    is_open=False
                ),
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
    metrics_options = [{"label": v["display"], "value": k} for k, v in MULTI_METRICS_CONFIG.items()]
    
    return dbc.Accordion([
        dbc.AccordionItem([
            # Row 1: Date, Cohort, Reset
            dbc.Row([
                dbc.Col([
                    html.Div("Reporting Date", className="filter-title"),
                    dbc.Select(
                        id=f"{prefix}-report-date",
                        options=date_options,
                        value=default_date
                    )
                ], width=3),
                dbc.Col([
                    html.Div("Cohort", className="filter-title"),
                    dbc.Select(
                        id=f"{prefix}-cohort",
                        options=[{"label": c, "value": c} for c in COHORT_OPTIONS],
                        value=DEFAULT_COHORT
                    )
                ], width=2),
                dbc.Col(width=5),
                dbc.Col([
                    dbc.Button("Reset", id=f"{prefix}-reset-btn", color="secondary",
                               className="w-100", style={"marginTop": "22px"})
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
                        value=list(MULTI_METRICS_CONFIG.keys()),
                        inline=True
                    )
                ])
            ])
        ], title="Filters")
    ], start_collapsed=False)
