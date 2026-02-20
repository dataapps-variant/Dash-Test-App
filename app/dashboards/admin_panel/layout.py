"""
Admin Panel Layout - Professional SaaS Design
Sidebar-based layout with role tabs, activity log, and roles & permissions
"""

from dash import html, dcc
import dash_bootstrap_components as dbc
from app.theme import get_theme_colors
from app.config import DASHBOARDS, ROLE_DISPLAY


# Avatar gradient colors
AVATAR_COLORS = [
    "linear-gradient(135deg, #ef4444, #dc2626)",
    "linear-gradient(135deg, #3b82f6, #2563eb)",
    "linear-gradient(135deg, #8b5cf6, #7c3aed)",
    "linear-gradient(135deg, #f59e0b, #d97706)",
    "linear-gradient(135deg, #10b981, #059669)",
    "linear-gradient(135deg, #ec4899, #db2777)",
    "linear-gradient(135deg, #6366f1, #4f46e5)",
]

# Role configurations
ROLES_CONFIG = [
    {
        "name": "Super Admin",
        "color": "#ef4444",
        "bg": "rgba(239,68,68,0.12)",
        "perms": ["Full Access", "Manage Users", "System Settings", "Billing", "API Keys"]
    },
    {
        "name": "Admin",
        "color": "#f59e0b",
        "bg": "rgba(245,158,11,0.12)",
        "perms": ["Create Dashboards", "Edit Apps", "View Users", "Export Data"]
    },
    {
        "name": "Read Only",
        "color": "#8b5cf6",
        "bg": "rgba(139,92,246,0.12)",
        "perms": ["View Dashboards", "View Apps"]
    },
]


def create_admin_panel_layout(user, theme="dark"):
    """Create admin panel page layout with professional sidebar design"""
    colors = get_theme_colors(theme)

    user_role = user.get("role", "readonly") if user else "readonly"
    user_name = user.get("name", "") if user else ""

    role_display = ROLE_DISPLAY.get(user_role, user_role)

    # Dashboard options for dropdowns (only enabled ones)
    dashboard_options = [{"label": d["name"], "value": d["id"]} for d in DASHBOARDS if d.get("enabled")]

    # =========================================================================
    # STYLES
    # =========================================================================

    sidebar_style = {
        "backgroundColor": "#111318",
        "borderRight": "1px solid #1f2229",
        "width": "260px",
        "height": "100vh",
        "position": "fixed",
        "top": "0",
        "left": "0",
        "display": "flex",
        "flexDirection": "column",
        "zIndex": "100"
    }

    main_content_style = {
        "marginLeft": "260px",
        "minHeight": "100vh",
        "backgroundColor": "#0a0c10"
    }

    nav_item_style = {
        "display": "flex",
        "alignItems": "center",
        "gap": "10px",
        "padding": "10px 12px",
        "borderRadius": "6px",
        "cursor": "pointer",
        "color": "#9aa0ab",
        "fontSize": "13.5px",
        "fontWeight": "450",
        "marginBottom": "2px",
        "textDecoration": "none",
        "transition": "all 0.2s ease"
    }

    nav_item_active_style = {
        **nav_item_style,
        "backgroundColor": "rgba(108,141,250,0.1)",
        "color": "#6c8dfa",
        "fontWeight": "500"
    }

    panel_style = {
        "backgroundColor": "#111318",
        "border": "1px solid #1f2229",
        "borderRadius": "14px",
        "overflow": "hidden",
        "marginBottom": "24px"
    }

    panel_header_style = {
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "space-between",
        "padding": "20px 24px",
        "borderBottom": "1px solid #1f2229"
    }

    # =========================================================================
    # SIDEBAR
    # =========================================================================

    sidebar = html.Aside([
        # Brand
        html.Div([
            html.Img(src="/assets/variant_logo.png", style={
                "width": "38px",
                "height": "38px",
                "borderRadius": "10px",
                "objectFit": "contain"
            }),
            html.Div([
                html.H2("Variant", style={"fontSize": "15px", "fontWeight": "600", "margin": "0", "color": "#e8eaed"}),
                html.Span("Admin Console", style={"fontSize": "11px", "color": "#5f6672", "textTransform": "uppercase", "letterSpacing": "1px"})
            ])
        ], style={
            "padding": "24px 20px",
            "display": "flex",
            "alignItems": "center",
            "gap": "12px",
            "borderBottom": "1px solid #1f2229"
        }),

        # Navigation
        html.Nav([
            html.Div("Main", style={
                "fontSize": "10px",
                "fontWeight": "600",
                "textTransform": "uppercase",
                "letterSpacing": "1.5px",
                "color": "#5f6672",
                "padding": "12px 12px 8px"
            }),

            # Users - Active
            html.Div([
                html.Span("👥", style={"fontSize": "16px"}),
                "User Management",
                html.Span(id="admin-user-count-badge", children="0", style={
                    "marginLeft": "auto",
                    "background": "#ef4444",
                    "color": "white",
                    "fontSize": "10px",
                    "fontWeight": "600",
                    "padding": "2px 7px",
                    "borderRadius": "10px",
                    "minWidth": "20px",
                    "textAlign": "center"
                })
            ], id="nav-users", n_clicks=0, style=nav_item_active_style),

            # Roles & Permissions
            html.Div([
                html.Span("🛡️", style={"fontSize": "16px"}),
                "Roles & Permissions"
            ], id="nav-roles", n_clicks=0, style=nav_item_style),

            # Activity Log
            html.Div([
                html.Span("📊", style={"fontSize": "16px"}),
                "Activity Log",
                html.Span("3", style={
                    "marginLeft": "auto",
                    "background": "#ef4444",
                    "color": "white",
                    "fontSize": "10px",
                    "fontWeight": "600",
                    "padding": "2px 7px",
                    "borderRadius": "10px"
                })
            ], id="nav-activity", n_clicks=0, style=nav_item_style),

            # System section
            html.Div("System", style={
                "fontSize": "10px",
                "fontWeight": "600",
                "textTransform": "uppercase",
                "letterSpacing": "1.5px",
                "color": "#5f6672",
                "padding": "20px 12px 8px"
            }),

            html.Div([
                html.Span("⚙️", style={"fontSize": "16px"}),
                "Settings"
            ], id="nav-settings", style=nav_item_style),

        ], style={"padding": "16px 12px", "flex": "1"}),

        # Sidebar Footer - User Info
        html.Div([
            html.Div([
                html.Div(user_name[0].upper() if user_name else "A", style={
                    "width": "36px",
                    "height": "36px",
                    "borderRadius": "50%",
                    "background": "linear-gradient(135deg, #ef4444, #dc2626)",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "fontSize": "13px",
                    "fontWeight": "600",
                    "color": "white"
                }),
                html.Div([
                    html.P(user_name, style={"fontSize": "13px", "fontWeight": "500", "margin": "0", "color": "#e8eaed"}),
                    html.Span(role_display, style={"fontSize": "11px", "color": "#5f6672"})
                ], style={"flex": "1", "minWidth": "0"}),
                dbc.Button("↩", id="admin-logout-btn", color="link", style={"color": "#5f6672", "padding": "0", "fontSize": "18px"})
            ], style={
                "display": "flex",
                "alignItems": "center",
                "gap": "10px",
                "padding": "10px",
                "borderRadius": "10px",
                "cursor": "pointer"
            })
        ], style={
            "padding": "16px",
            "borderTop": "1px solid #1f2229"
        })
    ], style=sidebar_style)

    # =========================================================================
    # TOP BAR
    # =========================================================================

    top_bar = html.Header([
        html.Div([
            html.H1("User Management", style={"fontSize": "18px", "fontWeight": "600", "margin": "0", "color": "#e8eaed", "letterSpacing": "-0.3px"}),
            html.Div([
                "Admin ",
                html.Span("/ Users", style={"color": "#9aa0ab"})
            ], style={"fontSize": "12px", "color": "#5f6672", "marginTop": "2px"})
        ]),
        html.Div([
            dbc.Button("🔔", id="admin-notifications-btn", color="link", style={
                "width": "38px",
                "height": "38px",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "center",
                "borderRadius": "6px",
                "border": "1px solid #2a2d36",
                "color": "#9aa0ab",
                "fontSize": "16px",
                "padding": "0"
            }),
            dbc.Button("⬇", id="admin-export-btn", color="link", style={
                "width": "38px",
                "height": "38px",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "center",
                "borderRadius": "6px",
                "border": "1px solid #2a2d36",
                "color": "#9aa0ab",
                "fontSize": "16px",
                "padding": "0",
                "marginLeft": "8px"
            }),
            dbc.Button([
                html.Span("←", style={"marginRight": "6px"}),
                "Back"
            ], id="admin-back-btn", color="link", style={
                "display": "flex",
                "alignItems": "center",
                "borderRadius": "6px",
                "border": "1px solid #2a2d36",
                "color": "#9aa0ab",
                "fontSize": "13px",
                "padding": "8px 14px",
                "marginLeft": "8px",
                "textDecoration": "none"
            })
        ], style={"display": "flex", "alignItems": "center"})
    ], style={
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "space-between",
        "padding": "16px 32px",
        "borderBottom": "1px solid #1f2229",
        "backgroundColor": "#111318",
        "position": "sticky",
        "top": "0",
        "zIndex": "10"
    })

    # =========================================================================
    # USERS PANEL
    # =========================================================================

    users_panel = html.Div([
        # Anchor for scrolling
        html.Div(id="section-users", style={"position": "absolute", "top": "-80px"}),
        # Panel Header
        html.Div([
            html.Div([
                html.Div("Users", style={"fontSize": "15px", "fontWeight": "600", "color": "#e8eaed"}),
                html.Div(id="admin-users-count-subtitle", children="0 users found", style={"fontSize": "12px", "color": "#5f6672", "marginTop": "2px"})
            ]),
            html.Div([
                dbc.Button([
                    html.Span("+", style={"marginRight": "6px"}),
                    "Add User"
                ], id="admin-add-user-btn", color="primary", size="sm", style={
                    "fontSize": "13px",
                    "padding": "8px 16px",
                    "borderRadius": "6px",
                    "fontWeight": "500",
                    "background": "#6c8dfa",
                    "border": "none"
                })
            ])
        ], style=panel_header_style),

        # Role Tabs
        html.Div([
            dbc.Button("All Users", id="admin-tab-all", color="link", n_clicks=0, style={
                "padding": "12px 16px",
                "fontSize": "13px",
                "fontWeight": "500",
                "color": "#6c8dfa",
                "borderBottom": "2px solid #6c8dfa",
                "borderRadius": "0",
                "background": "none"
            }),
            dbc.Button("Admins", id="admin-tab-admins", color="link", n_clicks=0, style={
                "padding": "12px 16px",
                "fontSize": "13px",
                "fontWeight": "500",
                "color": "#5f6672",
                "borderBottom": "2px solid transparent",
                "borderRadius": "0",
                "background": "none"
            }),
            dbc.Button("Editors", id="admin-tab-editors", color="link", n_clicks=0, style={
                "padding": "12px 16px",
                "fontSize": "13px",
                "fontWeight": "500",
                "color": "#5f6672",
                "borderBottom": "2px solid transparent",
                "borderRadius": "0",
                "background": "none"
            }),
            dbc.Button("Viewers", id="admin-tab-viewers", color="link", n_clicks=0, style={
                "padding": "12px 16px",
                "fontSize": "13px",
                "fontWeight": "500",
                "color": "#5f6672",
                "borderBottom": "2px solid transparent",
                "borderRadius": "0",
                "background": "none"
            }),
        ], style={
            "display": "flex",
            "gap": "2px",
            "padding": "0 24px",
            "borderBottom": "1px solid #1f2229"
        }),

        # Toolbar (Search & Filters)
        html.Div([
            # Search
            html.Div([
                html.Span("🔍", style={"position": "absolute", "left": "12px", "top": "50%", "transform": "translateY(-50%)", "color": "#5f6672", "fontSize": "14px"}),
                dbc.Input(
                    id="admin-search-input",
                    placeholder="Search by name, ID, or email...",
                    style={
                        "width": "100%",
                        "padding": "8px 12px 8px 36px",
                        "backgroundColor": "#181b22",
                        "border": "1px solid #2a2d36",
                        "borderRadius": "6px",
                        "color": "#e8eaed",
                        "fontSize": "13px"
                    }
                )
            ], style={"position": "relative", "flex": "1", "minWidth": "200px", "maxWidth": "320px"}),

            # Role Filter
            dbc.Select(
                id="admin-filter-role",
                options=[
                    {"label": "All Roles", "value": "all"},
                    {"label": "Super Admin", "value": "super_admin"},
                    {"label": "Admin", "value": "admin"},
                    {"label": "Read Only", "value": "readonly"}
                ],
                value="all",
                style={
                    "padding": "8px 32px 8px 12px",
                    "backgroundColor": "#181b22",
                    "border": "1px solid #2a2d36",
                    "borderRadius": "6px",
                    "color": "#e8eaed",
                    "fontSize": "13px",
                    "minWidth": "140px"
                }
            ),

            # Status Filter
            dbc.Select(
                id="admin-filter-status",
                options=[
                    {"label": "All Status", "value": "all"},
                    {"label": "Active", "value": "active"},
                    {"label": "Suspended", "value": "suspended"},
                    {"label": "Inactive", "value": "inactive"}
                ],
                value="all",
                style={
                    "padding": "8px 32px 8px 12px",
                    "backgroundColor": "#181b22",
                    "border": "1px solid #2a2d36",
                    "borderRadius": "6px",
                    "color": "#e8eaed",
                    "fontSize": "13px",
                    "minWidth": "140px"
                }
            ),
        ], style={
            "display": "flex",
            "alignItems": "center",
            "gap": "10px",
            "padding": "16px 24px",
            "borderBottom": "1px solid #1f2229",
            "flexWrap": "wrap"
        }),

        # Users Table
        html.Div(id="admin-users-table-page", style={"overflowX": "auto"}),

        # Pagination
        html.Div([
            html.Div(id="admin-pagination-info", children="Showing 1-5 of 7", style={
                "fontSize": "12px",
                "color": "#5f6672"
            }),
            html.Div([
                dbc.Button("◀", id="admin-prev-page", color="link", disabled=True, style={
                    "width": "32px",
                    "height": "32px",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "borderRadius": "6px",
                    "border": "1px solid #2a2d36",
                    "color": "#9aa0ab",
                    "padding": "0"
                }),
                dbc.Button("1", id="admin-page-1", color="link", style={
                    "width": "32px",
                    "height": "32px",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "borderRadius": "6px",
                    "background": "#6c8dfa",
                    "border": "1px solid #6c8dfa",
                    "color": "white",
                    "padding": "0",
                    "fontSize": "13px",
                    "fontFamily": "'JetBrains Mono', monospace"
                }),
                dbc.Button("▶", id="admin-next-page", color="link", style={
                    "width": "32px",
                    "height": "32px",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "borderRadius": "6px",
                    "border": "1px solid #2a2d36",
                    "color": "#9aa0ab",
                    "padding": "0"
                }),
            ], style={"display": "flex", "alignItems": "center", "gap": "4px"})
        ], style={
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
            "padding": "14px 24px",
            "borderTop": "1px solid #1f2229"
        })
    ], style={**panel_style, "position": "relative"})

    # =========================================================================
    # ACTIVITY LOG PANEL
    # =========================================================================

    activity_panel = html.Div([
        # Anchor for scrolling
        html.Div(id="section-activity", style={"position": "absolute", "top": "-80px"}),
        # Header
        html.Div([
            html.Div([
                html.Div("Recent Activity", style={"fontSize": "15px", "fontWeight": "600", "color": "#e8eaed"}),
                html.Div("Last 24 hours", style={"fontSize": "12px", "color": "#5f6672", "marginTop": "2px"})
            ])
        ], style=panel_header_style),

        # Activity List
        html.Div(id="admin-activity-list", style={"padding": "8px 0"})
    ], style={**panel_style, "alignSelf": "start", "position": "relative"})

    # =========================================================================
    # ROLES & PERMISSIONS PANEL
    # =========================================================================

    roles_panel = html.Div([
        # Anchor for scrolling
        html.Div(id="section-roles", style={"position": "absolute", "top": "-80px"}),
        # Header
        html.Div([
            html.Div([
                html.Div("Roles & Permissions", style={"fontSize": "15px", "fontWeight": "600", "color": "#e8eaed"}),
                html.Div("Configure access levels", style={"fontSize": "12px", "color": "#5f6672", "marginTop": "2px"})
            ])
        ], style=panel_header_style),

        # Roles Grid
        html.Div([
            # Role columns
            html.Div([
                create_role_column(role, idx) for idx, role in enumerate(ROLES_CONFIG)
            ], style={
                "display": "grid",
                "gridTemplateColumns": "repeat(3, 1fr)",
                "gap": "0"
            })
        ], id="admin-roles-grid")
    ], style={**panel_style, "position": "relative"})

    # =========================================================================
    # MAIN LAYOUT
    # =========================================================================

    return html.Div([
        sidebar,

        html.Main([
            top_bar,

            html.Div([
                # Content Grid - Users + Activity
                html.Div([
                    users_panel,
                    activity_panel
                ], style={
                    "display": "grid",
                    "gridTemplateColumns": "1fr 380px",
                    "gap": "24px",
                    "marginBottom": "24px"
                }),

                # Roles Panel
                roles_panel,

                # Status message
                html.Div(id="admin-page-status", style={"marginBottom": "16px"}),

            ], style={"padding": "28px 32px"})
        ], style=main_content_style),

        # =========================================================================
        # MODALS
        # =========================================================================

        # Add/Edit User Modal
        dbc.Modal([
            dbc.ModalHeader([
                dbc.ModalTitle(id="admin-edit-modal-title", style={"fontSize": "16px", "fontWeight": "600", "color": "#e8eaed"}),
            ], close_button=True, style={"borderBottom": "1px solid #2a2d36", "padding": "20px 24px", "backgroundColor": "#111318"}),
            dbc.ModalBody([
                html.Small("* Required fields", style={"color": "#5f6672", "marginBottom": "20px", "display": "block", "fontSize": "11px"}),

                # Row 1: User ID + Name
                dbc.Row([
                    dbc.Col([
                        dbc.Label("User ID *", style={"fontSize": "12px", "color": "#9aa0ab", "marginBottom": "6px", "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "0.5px"}),
                        dbc.Input(id="admin-edit-user-id", placeholder="e.g. jane_d", size="sm",
                                  style={
                                      "backgroundColor": "#181b22",
                                      "border": "1px solid #2a2d36",
                                      "color": "#e8eaed",
                                      "fontSize": "14px",
                                      "borderRadius": "6px",
                                      "padding": "10px 14px"
                                  })
                    ], width=6),
                    dbc.Col([
                        dbc.Label("Full Name *", style={"fontSize": "12px", "color": "#9aa0ab", "marginBottom": "6px", "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "0.5px"}),
                        dbc.Input(id="admin-edit-name", placeholder="e.g. Jane Doe", size="sm",
                                  style={
                                      "backgroundColor": "#181b22",
                                      "border": "1px solid #2a2d36",
                                      "color": "#e8eaed",
                                      "fontSize": "14px",
                                      "borderRadius": "6px",
                                      "padding": "10px 14px"
                                  })
                    ], width=6)
                ], className="mb-3"),

                # Row 2: Role + Password
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Role *", style={"fontSize": "12px", "color": "#9aa0ab", "marginBottom": "6px", "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "0.5px"}),
                        dbc.Select(id="admin-edit-role", size="sm",
                                   style={
                                       "backgroundColor": "#181b22",
                                       "border": "1px solid #2a2d36",
                                       "color": "#e8eaed",
                                       "fontSize": "14px",
                                       "borderRadius": "6px",
                                       "padding": "10px 14px"
                                   })
                    ], width=6),
                    dbc.Col([
                        dbc.Label("Password *", style={"fontSize": "12px", "color": "#9aa0ab", "marginBottom": "6px", "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "0.5px"}),
                        dbc.Input(id="admin-edit-password", placeholder="••••••••", type="password", size="sm",
                                  style={
                                      "backgroundColor": "#181b22",
                                      "border": "1px solid #2a2d36",
                                      "color": "#e8eaed",
                                      "fontSize": "14px",
                                      "borderRadius": "6px",
                                      "padding": "10px 14px"
                                  })
                    ], width=6)
                ], className="mb-3"),

                # Dashboard & App Access Section
                html.Div(id="admin-edit-access-section", children=[
                    html.Hr(style={"borderColor": "#2a2d36", "margin": "24px 0"}),
                    dbc.Label("Dashboard Access", style={
                        "fontSize": "12px",
                        "color": "#9aa0ab",
                        "marginBottom": "12px",
                        "fontWeight": "600",
                        "display": "block",
                        "textTransform": "uppercase",
                        "letterSpacing": "0.5px"
                    }),

                    html.Div(id="admin-edit-access-display", className="mb-3",
                             style={
                                 "maxHeight": "120px",
                                 "overflowY": "auto",
                                 "padding": "8px",
                                 "backgroundColor": "#181b22",
                                 "borderRadius": "6px",
                                 "border": "1px solid #2a2d36"
                             }),

                    html.Div([
                        html.Small("Add Dashboard Access", style={"color": "#5f6672", "fontSize": "10px", "marginBottom": "8px", "display": "block"})
                    ]),
                    dbc.Row([
                        dbc.Col([
                            dbc.Select(
                                id="admin-edit-add-dashboard",
                                options=dashboard_options,
                                placeholder="Select dashboard...",
                                style={
                                    "backgroundColor": "#181b22",
                                    "border": "1px solid #2a2d36",
                                    "color": "#e8eaed",
                                    "fontSize": "12px",
                                    "borderRadius": "6px"
                                }
                            )
                        ], width=5),
                        dbc.Col([
                            dbc.Checklist(
                                id="admin-edit-add-apps",
                                options=[],
                                value=[],
                                inline=True,
                                style={"fontSize": "10px"}
                            )
                        ], width=5),
                        dbc.Col([
                            dbc.Button("Add", id="admin-edit-add-access-btn", color="success", size="sm",
                                       style={
                                           "fontSize": "11px",
                                           "padding": "6px 14px",
                                           "borderRadius": "6px",
                                           "width": "100%"
                                       })
                        ], width=2)
                    ], align="center"),
                ]),

                html.Div(id="admin-edit-status", className="mt-3"),
            ], style={"backgroundColor": "#0a0c10", "padding": "24px"}),
            dbc.ModalFooter([
                html.Div([
                    dbc.Button("Delete User", id="admin-edit-delete-btn", color="danger", size="sm", outline=True,
                               style={"fontSize": "11px", "padding": "6px 14px", "borderRadius": "6px"})
                ], id="admin-delete-btn-container"),
                html.Div([
                    dbc.Button("Cancel", id="admin-edit-cancel-btn", color="secondary", size="sm",
                               style={"fontSize": "11px", "padding": "6px 16px", "marginRight": "10px", "borderRadius": "6px"}),
                    dbc.Button("Create User", id="admin-edit-save-btn", color="primary", size="sm",
                               style={"fontSize": "11px", "padding": "6px 20px", "borderRadius": "6px", "fontWeight": "500", "background": "#6c8dfa", "border": "none"})
                ])
            ], style={
                "justifyContent": "space-between",
                "borderTop": "1px solid #2a2d36",
                "padding": "16px 24px",
                "backgroundColor": "#0a0c10"
            })
        ], id="admin-edit-modal", size="lg", is_open=False, backdrop="static", centered=True),

        # Delete Confirmation Modal
        dbc.Modal([
            dbc.ModalHeader(
                dbc.ModalTitle("Delete User", style={"fontSize": "16px", "fontWeight": "600", "color": "#e8eaed"}),
                close_button=True,
                style={"borderBottom": "1px solid #2a2d36", "padding": "16px 20px", "backgroundColor": "#111318"}
            ),
            dbc.ModalBody([
                html.Div([
                    html.Div([
                        html.Span("⚠️", style={"fontSize": "28px", "marginBottom": "12px", "display": "block"}),
                        html.P("Are you sure you want to delete this user?", style={
                            "marginBottom": "8px",
                            "color": "#e8eaed",
                            "fontSize": "14px",
                            "fontWeight": "500"
                        }),
                        html.P("This will deactivate the user account. They will no longer be able to log in.", style={
                            "color": "#9aa0ab",
                            "fontSize": "12px",
                            "margin": "0"
                        })
                    ], style={"textAlign": "center", "padding": "12px 0"})
                ])
            ], style={"backgroundColor": "#0a0c10", "padding": "20px"}),
            dbc.ModalFooter([
                dbc.Button("Cancel", id="admin-delete-cancel-btn", color="secondary", size="sm",
                           style={"fontSize": "11px", "padding": "6px 16px", "marginRight": "10px", "borderRadius": "6px"}),
                dbc.Button("Delete User", id="admin-delete-confirm-btn", color="danger", size="sm",
                           style={"fontSize": "11px", "padding": "6px 16px", "borderRadius": "6px", "fontWeight": "500"})
            ], style={
                "borderTop": "1px solid #2a2d36",
                "padding": "16px 20px",
                "backgroundColor": "#0a0c10",
                "justifyContent": "flex-end"
            })
        ], id="admin-delete-modal", is_open=False, centered=True, size="sm"),

        # Stores
        dcc.Store(id="admin-page-refresh-store", data=0),
        dcc.Store(id="admin-edit-access-store", data={}),
        dcc.Store(id="admin-edit-mode-store", data={"mode": "new", "user_id": ""}),
        dcc.Store(id="admin-active-tab-store", data="all"),
        dcc.Store(id="admin-current-page-store", data=1)

    ], style={
        "minHeight": "100vh",
        "backgroundColor": "#0a0c10",
        "color": "#e8eaed",
        "fontFamily": "'DM Sans', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif"
    }, className="admin-panel")


def create_role_column(role, idx):
    """Create a role column for the roles grid"""
    border_style = "1px solid #1f2229" if idx < 2 else "none"

    return html.Div([
        # Role header
        html.Div([
            html.Span(role["name"], style={
                "display": "inline-flex",
                "alignItems": "center",
                "padding": "5px 14px",
                "borderRadius": "20px",
                "fontSize": "13px",
                "fontWeight": "600",
                "background": role["bg"],
                "color": role["color"]
            }),
            html.Span(id=f"admin-role-count-{role['name'].lower().replace(' ', '-')}", children="0 users", style={
                "fontSize": "12px",
                "color": "#5f6672",
                "marginLeft": "10px"
            })
        ], style={"display": "flex", "alignItems": "center", "gap": "10px", "marginBottom": "14px"}),

        # Permissions list
        html.Div([
            html.Div([
                html.Span("✓", style={"color": role["color"], "marginRight": "8px"}),
                perm
            ], style={
                "display": "flex",
                "alignItems": "center",
                "fontSize": "13px",
                "color": "#9aa0ab",
                "marginBottom": "8px"
            }) for perm in role["perms"]
        ])
    ], style={
        "padding": "20px 24px",
        "borderRight": border_style
    })
