"""
Layout for All Metrics Merged Dashboard

4 tabs: All Plans, Individual Plans, Merged Plans - Breakup, Entity
Shared filters: Start Date, End Date, App Name (single-select)
Tab-specific filters vary per tab
"""

from datetime import date, timedelta
from dash import html, dcc
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

from app.theme import get_theme_colors
from app.dashboards.all_metrics_merged.data import get_app_names, get_date_range, get_merged_cache_info


def create_merged_layout(user, theme="dark"):
    """Main layout for All Metrics Merged dashboard"""
    colors = get_theme_colors(theme)

    # Get date range and app names for filters
    min_date, max_date = get_date_range()
    if min_date is None:
        min_date = date(2025, 1, 1)
    if max_date is None:
        max_date = date.today()

    app_names = get_app_names()
    default_app = app_names[0] if app_names else "CT-JP"

    merged_cache_info = get_merged_cache_info()

    return html.Div([
        # Header - Back left, Title center, Logout right
        dbc.Row([
            dbc.Col([
                dbc.Button("\u2190 Back", id="back-to-landing", color="secondary", size="sm")
            ], width=2),
            dbc.Col([
                html.H5(
                    "All Metrics Merged",
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
            html.Small(f"  Last: {merged_cache_info.get('last_bq_refresh', '--')}  ", style={"color": colors["text_secondary"], "margin": "0 16px 0 8px"}),
            dbc.Button("Refresh GCS", id="refresh-gcs-btn", size="sm", className="refresh-btn-green"),
            html.Small(f"  Last: {merged_cache_info.get('last_gcs_refresh', '--')}", style={"color": colors["text_secondary"], "marginLeft": "8px"}),
            html.Div(id="refresh-status", style={"display": "inline-block", "marginLeft": "16px"})
        ], style={"textAlign": "right", "padding": "6px 0", "marginBottom": "8px"}),

        # =====================================================================
        # SHARED FILTERS — Start Date, End Date, App Name
        # =====================================================================
        dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    # Start Date
                    dbc.Col([
                        html.Div("Start Date", className="filter-title",
                                 style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
                        dcc.DatePickerSingle(
                            id="merged-start-date",
                            min_date_allowed=min_date,
                            max_date_allowed=max_date,
                            date=max_date - timedelta(days=90),
                            display_format="YYYY-MM-DD",
                            style={"width": "100%"}
                        ),
                    ], width=3),
                    # End Date
                    dbc.Col([
                        html.Div("End Date", className="filter-title",
                                 style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
                        dcc.DatePickerSingle(
                            id="merged-end-date",
                            min_date_allowed=min_date,
                            max_date_allowed=max_date,
                            date=max_date,
                            display_format="YYYY-MM-DD",
                            style={"width": "100%"}
                        ),
                    ], width=3),
                    # App Name
                    dbc.Col([
                        html.Div("App Name", className="filter-title",
                                 style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
                        dbc.Select(
                            id="merged-app-name",
                            options=[{"label": a, "value": a} for a in app_names],
                            value=default_app,
                        ),
                    ], width=3),
                    # BC Dropdown (Tab 1 only — hidden on other tabs via callback)
                    dbc.Col([
                        html.Div(id="merged-bc-filter-container", children=[
                            html.Div("BC (Retention/Refund)", className="filter-title",
                                     style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px"}),
                            dbc.Select(
                                id="merged-bc-dropdown",
                                options=[{"label": str(i), "value": i} for i in [0, 1, 2, 3, 4]],
                                value=4,
                            ),
                        ]),
                    ], width=2),
                ], align="end"),

                # Plan Name filter (Tab 2 & 3 only — hidden on other tabs via callback)
                html.Div(id="merged-plan-filter-container", children=[
                    dbc.Row([
                        dbc.Col([
                            html.Div("Plan Name", className="filter-title",
                                     style={"color": colors["text_secondary"], "fontSize": "12px", "marginBottom": "4px", "marginTop": "12px"}),
                            dbc.Select(
                                id="merged-plan-name",
                                options=[],
                                value=None,
                                placeholder="Select a plan..."
                            ),
                        ], width=4),
                    ])
                ], style={"display": "none"}),
            ])
        ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}", "marginBottom": "16px"}),

        # =====================================================================
        # TABS
        # =====================================================================
        dbc.Tabs(
            id="merged-dashboard-tabs",
            active_tab="all-plans",
            children=[
                dbc.Tab(label="All Plans", tab_id="all-plans"),
                dbc.Tab(label="Individual Plans", tab_id="individual-plans"),
                dbc.Tab(label="Merged Plans - Breakup", tab_id="merged-breakup"),
                dbc.Tab(label="Entity", tab_id="entity"),
            ],
            className="mb-3"
        ),

        # Tab content container
        html.Div(id="merged-tab-content", children=[
            dbc.Spinner(color="primary", size="lg")
        ]),


    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px",
        "color": colors["text_primary"]
    })


# =============================================================================
# CHART CARD WRAPPER — reusable for all chart containers
# =============================================================================

def chart_card(title, chart_id, legend_id, theme="dark"):
    """Wrap a chart + legend in a styled card"""
    colors = get_theme_colors(theme)
    return dbc.Card([
        dbc.CardBody([
            html.H6(title, style={"color": colors["text_primary"], "marginBottom": "8px", "fontWeight": "600"}),
            html.Div(id=legend_id),
            dcc.Graph(id=chart_id, config={
                'displayModeBar': True,
                'displaylogo': False,
                'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                'scrollZoom': False
            }),
        ])
    ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}", "marginBottom": "16px"})


def table_card(title, table_id, theme="dark"):
    """Wrap an AG Grid table in a styled card"""
    colors = get_theme_colors(theme)
    return dbc.Card([
        dbc.CardBody([
            html.H6(title, style={"color": colors["text_primary"], "marginBottom": "8px", "fontWeight": "600"}),
            html.Div(id=table_id),
        ])
    ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}", "marginBottom": "16px"})
