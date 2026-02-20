"""
Layout for Daedalus Dashboard

16 tabs total — first 5 implemented:
Tab 1: Daedalus
Tab 2: Pacing by Entity
Tab 3: CAC by Entity
Tab 4: Current Subscriptions
Tab 5: Daedalus (Historical)
Tabs 6-16: Placeholder

No company logo. Title centered. Refresh + export at top-right.
"""

from datetime import date, timedelta
from dash import html, dcc
import dash_bootstrap_components as dbc

from app.theme import get_theme_colors
from app.dashboards.daedalus.data import (
    get_daedalus_app_names, get_daedalus_date_range, get_available_months,
    get_cac_entity_app_names, get_cac_entity_date_range,
    get_active_subs_app_names, get_active_subs_channels, get_active_subs_date_range,
    get_daedalus_cache_info,
)

# Tab definitions (all 16)
TAB_DEFS = [
    {"id": "daedalus", "label": "Daedalus"},
    {"id": "pacing-entity", "label": "Pacing by Entity"},
    {"id": "cac-entity", "label": "CAC by Entity"},
    {"id": "current-subs", "label": "Current Subscriptions"},
    {"id": "daedalus-historical", "label": "Daedalus (Historical)"},
    {"id": "traffic-channel", "label": "Traffic Channel"},
    {"id": "new-users-tc", "label": "New Users - Traffic Channel"},
    {"id": "spend-tc", "label": "Spend - Traffic Channel"},
    {"id": "cac-tc", "label": "CAC - Traffic Channel"},
    {"id": "afid-unknown", "label": "AFID Unknown"},
    {"id": "daily-report", "label": "Daily Report"},
    {"id": "mtd-report", "label": "MTD Report"},
    {"id": "approval-rates", "label": "Approval Rates"},
    {"id": "decline-app", "label": "Decline Reason % - App"},
    {"id": "decline-channel", "label": "Decline Reason % - Channel"},
    {"id": "decline-afid", "label": "Decline Reason % - AFID"},
]


def _checkbox_group(id_prefix, options, default_all=True, colors=None):
    """Create a multi-select checkbox group with white/grey tick style"""
    items = []
    for opt in options:
        items.append(
            dbc.Checklist(
                options=[{"label": opt, "value": opt}],
                value=[opt] if default_all else [],
                id={"type": f"{id_prefix}-check", "index": opt},
                inline=True,
                className="daedalus-checkbox",
                style={"display": "inline-block", "marginRight": "10px"},
            )
        )
    return html.Div(items, style={"display": "flex", "flexWrap": "wrap", "gap": "4px"})


def create_daedalus_layout(user, theme="dark"):
    """Main layout for Daedalus dashboard"""
    colors = get_theme_colors(theme)
    cache_info = get_daedalus_cache_info()

    # Date ranges
    d_min, d_max = get_daedalus_date_range()
    if d_min is None:
        d_min = date(2025, 1, 1)
    if d_max is None:
        d_max = date.today()

    ce_min, ce_max = get_cac_entity_date_range()
    if ce_min is None:
        ce_min = date(2025, 1, 1)
    if ce_max is None:
        ce_max = date.today()

    as_min, as_max = get_active_subs_date_range()
    if as_min is None:
        as_min = date(2025, 1, 1)
    if as_max is None:
        as_max = date.today()

    # App names
    daedalus_apps = get_daedalus_app_names()
    cac_apps = get_cac_entity_app_names()
    subs_apps = get_active_subs_app_names()
    subs_channels = get_active_subs_channels()

    # Available months for Tab 1 & 2
    months = get_available_months()
    if months:
        default_ym = months[0]  # Latest month
    else:
        default_ym = (date.today().year, date.today().month)

    month_options = [
        {"label": f"{m[1]:02d}/{m[0]}", "value": f"{m[0]}-{m[1]:02d}"}
        for m in months
    ]

    user_role = user.get("role", "readonly") if user else "readonly"
    show_admin = user_role in ("admin", "super_admin")

    return html.Div([
        # =================================================================
        # HEADER — Back | Title | Logout + Three-dot
        # =================================================================
        dbc.Row([
            dbc.Col([
                dbc.Button("← Back", id="back-to-landing", color="secondary", size="sm")
            ], width=2),
            dbc.Col([
                html.H5(
                    "Daedalus",
                    style={"textAlign": "center", "color": colors["text_primary"],
                           "fontWeight": "600", "margin": "0"}
                )
            ], width=6),
            dbc.Col([
                html.Div([
                    dbc.Button("Logout", id="logout-btn", color="secondary", size="sm", className="me-2"),
                    dbc.DropdownMenu(
                        label="⋮",
                        children=[
                            dbc.DropdownMenuItem("Export Full Dashboard as PDF", disabled=True),
                            dbc.DropdownMenuItem(divider=True),
                            dbc.DropdownMenuItem(
                                f"User: {user['name']}" if user else "User: --", disabled=True
                            ),
                        ],
                        color="secondary"
                    )
                ], style={"display": "flex", "alignItems": "center",
                           "justifyContent": "flex-end", "gap": "4px"})
            ], width=4, style={"textAlign": "right"})
        ], className="mb-2", align="center"),

        # =================================================================
        # REFRESH SECTION — right-aligned
        # =================================================================
        html.Div([
            dbc.Button("Refresh BQ", id="daedalus-refresh-bq-btn", size="sm",
                       className="refresh-btn-green"),
            html.Small(f"  Last: {cache_info.get('last_bq_refresh', '--')}  ",
                       id="daedalus-bq-timestamp",
                       style={"color": colors["text_secondary"], "margin": "0 16px 0 8px"}),
            dbc.Button("Refresh GCS", id="daedalus-refresh-gcs-btn", size="sm",
                       className="refresh-btn-green"),
            html.Small(f"  Last: {cache_info.get('last_gcs_refresh', '--')}",
                       id="daedalus-gcs-timestamp",
                       style={"color": colors["text_secondary"], "marginLeft": "8px"}),
            html.Div(id="daedalus-refresh-status",
                     style={"display": "inline-block", "marginLeft": "16px"})
        ], style={"textAlign": "right", "padding": "6px 0", "marginBottom": "8px"}),

        # =================================================================
        # 16 TABS
        # =================================================================
        dbc.Tabs(
            [
                dbc.Tab(
                    dcc.Loading(html.Div(id=f"daedalus-tab-{t['id']}-content"), type="dot", color="#FFFFFF"),
                    label=t["label"],
                    tab_id=t["id"],
                )
                for t in TAB_DEFS
            ],
            id="daedalus-dashboard-tabs",
            active_tab="daedalus",
            className="mb-2",
        ),

        # =================================================================
        # HIDDEN STORES for filter state
        # =================================================================
        # Tab 1 filters
        dcc.Store(id="daedalus-tab1-app-names", data=daedalus_apps),
        dcc.Store(id="daedalus-tab1-month",
                  data=f"{default_ym[0]}-{default_ym[1]:02d}"),
        dcc.Store(id="daedalus-tab1-date", data=str(d_max)),

        # Tab 3 filters
        dcc.Store(id="daedalus-tab3-apps", data=cac_apps),

        # Tab 4 filters
        dcc.Store(id="daedalus-tab4-apps", data=subs_apps),
        dcc.Store(id="daedalus-tab4-channels", data=[str(c) for c in subs_channels]),

        # Tab 5 filters
        dcc.Store(id="daedalus-tab5-apps", data=cac_apps),

        # Available filter options (for building filter UIs in callbacks)
        dcc.Store(id="daedalus-filter-options", data={
            "daedalus_apps": daedalus_apps,
            "cac_apps": cac_apps,
            "subs_apps": subs_apps,
            "subs_channels": [str(c) for c in subs_channels],
            "month_options": month_options,
            "d_min": str(d_min), "d_max": str(d_max),
            "ce_min": str(ce_min), "ce_max": str(ce_max),
            "as_min": str(as_min), "as_max": str(as_max),
        }),

    ], style={
        "minHeight": "100vh",
        "backgroundColor": colors["background"],
        "padding": "20px",
    })
