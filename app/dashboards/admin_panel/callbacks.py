"""
Admin Panel Callbacks
Production-ready with full permission enforcement, search, filter, and role tabs
"""

from dash import html, callback, Input, Output, State, ALL, ctx, no_update
import dash_bootstrap_components as dbc
from app.auth import (
    get_current_user, get_all_users, get_assignable_roles, logout
)
from app.config import ROLE_DISPLAY, DASHBOARDS
from app.dashboards.admin_panel.services import (
    get_users_with_metadata, create_user, edit_user, soft_delete_user,
    get_recent_audit_log, get_dashboard_name, can_edit_user, can_delete_user
)

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

# Activity icon styles
ACTIVITY_ICONS = {
    "CREATE": {"bg": "rgba(52,211,153,0.1)", "color": "#34d399", "icon": "+"},
    "UPDATE": {"bg": "rgba(96,165,250,0.1)", "color": "#60a5fa", "icon": "✎"},
    "DELETE": {"bg": "rgba(248,113,113,0.1)", "color": "#f87171", "icon": "×"},
    "DISABLE": {"bg": "rgba(251,191,36,0.1)", "color": "#fbbf24", "icon": "⊘"},
    "ENABLE": {"bg": "rgba(52,211,153,0.1)", "color": "#34d399", "icon": "✓"},
    "LOGIN": {"bg": "rgba(52,211,153,0.1)", "color": "#34d399", "icon": "→"},
}


def get_available_apps():
    """Get all available apps"""
    try:
        from app.bigquery_client import load_plan_groups
        active_plans = load_plan_groups("Active")
        inactive_plans = load_plan_groups("Inactive")
        all_apps = set(active_plans.get("App_Name", []))
        all_apps.update(inactive_plans.get("App_Name", []))
        return sorted(all_apps)
    except Exception:
        return ["AT", "CL", "CN", "CT-Non-JP", "CT-JP", "CV", "DT", "EN", "FS", "IQ", "JF", "PD", "RL", "RT"]


def register_callbacks(app):
    """Register admin panel callbacks"""

    # =========================================================================
    # SIDEBAR NAVIGATION - SMOOTH SCROLLING
    # =========================================================================

    # Clientside callback for smooth scrolling to sections
    app.clientside_callback(
        """
        function(users_clicks, roles_clicks, activity_clicks) {
            const triggered = dash_clientside.callback_context.triggered;
            if (!triggered || triggered.length === 0) return window.dash_clientside.no_update;

            const triggeredId = triggered[0].prop_id.split('.')[0];
            let targetId = null;

            if (triggeredId === 'nav-users') {
                targetId = 'section-users';
            } else if (triggeredId === 'nav-roles') {
                targetId = 'section-roles';
            } else if (triggeredId === 'nav-activity') {
                targetId = 'section-activity';
            }

            if (targetId) {
                const element = document.getElementById(targetId);
                if (element) {
                    element.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            }

            return window.dash_clientside.no_update;
        }
        """,
        Output('admin-page-refresh-store', 'data', allow_duplicate=True),
        Input('nav-users', 'n_clicks'),
        Input('nav-roles', 'n_clicks'),
        Input('nav-activity', 'n_clicks'),
        prevent_initial_call=True
    )

    # Update active nav item style
    @app.callback(
        Output('nav-users', 'style'),
        Output('nav-roles', 'style'),
        Output('nav-activity', 'style'),
        Input('nav-users', 'n_clicks'),
        Input('nav-roles', 'n_clicks'),
        Input('nav-activity', 'n_clicks'),
        prevent_initial_call=True
    )
    def update_nav_styles(users_clicks, roles_clicks, activity_clicks):
        triggered = ctx.triggered_id

        active_style = {
            "display": "flex",
            "alignItems": "center",
            "gap": "10px",
            "padding": "10px 12px",
            "borderRadius": "6px",
            "cursor": "pointer",
            "backgroundColor": "rgba(108,141,250,0.1)",
            "color": "#6c8dfa",
            "fontSize": "13.5px",
            "fontWeight": "500",
            "marginBottom": "2px",
            "textDecoration": "none",
            "position": "relative"
        }

        inactive_style = {
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
            "textDecoration": "none"
        }

        if triggered == 'nav-users':
            return active_style, inactive_style, inactive_style
        elif triggered == 'nav-roles':
            return inactive_style, active_style, inactive_style
        elif triggered == 'nav-activity':
            return inactive_style, inactive_style, active_style

        return no_update, no_update, no_update

    # =========================================================================
    # BACK NAVIGATION
    # =========================================================================

    @app.callback(
        Output('page-store', 'data', allow_duplicate=True),
        Input('admin-back-btn', 'n_clicks'),
        prevent_initial_call=True
    )
    def admin_go_back(n_clicks):
        if n_clicks:
            return "landing"
        return no_update

    @app.callback(
        Output('session-store', 'data', allow_duplicate=True),
        Output('page-store', 'data', allow_duplicate=True),
        Input('admin-logout-btn', 'n_clicks'),
        State('session-store', 'data'),
        prevent_initial_call=True
    )
    def admin_logout(n_clicks, session_data):
        if n_clicks:
            if session_data and session_data.get('session_id'):
                logout(session_data['session_id'])
            return {}, 'login'
        return no_update, no_update

    # =========================================================================
    # ROLE TABS
    # =========================================================================

    @app.callback(
        Output('admin-active-tab-store', 'data'),
        Output('admin-tab-all', 'style'),
        Output('admin-tab-admins', 'style'),
        Output('admin-tab-editors', 'style'),
        Output('admin-tab-viewers', 'style'),
        Input('admin-tab-all', 'n_clicks'),
        Input('admin-tab-admins', 'n_clicks'),
        Input('admin-tab-editors', 'n_clicks'),
        Input('admin-tab-viewers', 'n_clicks'),
        prevent_initial_call=True
    )
    def handle_tab_change(all_clicks, admins_clicks, editors_clicks, viewers_clicks):
        triggered = ctx.triggered_id

        active_style = {
            "padding": "12px 16px",
            "fontSize": "13px",
            "fontWeight": "500",
            "color": "#6c8dfa",
            "borderBottom": "2px solid #6c8dfa",
            "borderRadius": "0",
            "background": "none"
        }

        inactive_style = {
            "padding": "12px 16px",
            "fontSize": "13px",
            "fontWeight": "500",
            "color": "#5f6672",
            "borderBottom": "2px solid transparent",
            "borderRadius": "0",
            "background": "none"
        }

        if triggered == "admin-tab-all":
            return "all", active_style, inactive_style, inactive_style, inactive_style
        elif triggered == "admin-tab-admins":
            return "admins", inactive_style, active_style, inactive_style, inactive_style
        elif triggered == "admin-tab-editors":
            return "editors", inactive_style, inactive_style, active_style, inactive_style
        elif triggered == "admin-tab-viewers":
            return "viewers", inactive_style, inactive_style, inactive_style, active_style

        return no_update, no_update, no_update, no_update, no_update

    # =========================================================================
    # USERS TABLE WITH SEARCH, FILTER & TABS
    # =========================================================================

    @app.callback(
        Output('admin-users-table-page', 'children'),
        Output('admin-users-count-subtitle', 'children'),
        Output('admin-user-count-badge', 'children'),
        Output('admin-role-count-super-admin', 'children'),
        Output('admin-role-count-admin', 'children'),
        Output('admin-role-count-read-only', 'children'),
        Input('admin-page-refresh-store', 'data'),
        Input('page-store', 'data'),
        Input('admin-search-input', 'value'),
        Input('admin-filter-role', 'value'),
        Input('admin-filter-status', 'value'),
        Input('admin-active-tab-store', 'data'),
        State('session-store', 'data'),
        prevent_initial_call=False
    )
    def render_users_table(refresh_trigger, current_page, search_text, filter_role, filter_status, active_tab, session_data):
        if current_page != "admin":
            return no_update, no_update, no_update, no_update, no_update, no_update

        session_id = session_data.get('session_id') if session_data else None
        current_user = get_current_user(session_id) if session_id else None
        current_role = current_user.get("role", "readonly") if current_user else "readonly"
        current_username = current_user.get("username", "") if current_user else ""

        users = get_users_with_metadata()

        # Count by role
        role_counts = {"super_admin": 0, "admin": 0, "readonly": 0}
        for u in users:
            role = u.get("role", "readonly")
            if role in role_counts:
                role_counts[role] += 1

        # Apply tab filter
        tab_role_map = {
            "admins": "super_admin",
            "editors": "admin",
            "viewers": "readonly"
        }

        # Apply filters
        filtered_users = []
        for u in users:
            is_active = u.get("is_active", True)
            role = u.get("role", "readonly")

            # Tab filter
            if active_tab and active_tab != "all":
                if role != tab_role_map.get(active_tab, ""):
                    continue

            # Status filter
            if filter_status == "active" and not is_active:
                continue
            if filter_status == "inactive" and is_active:
                continue
            if filter_status == "suspended" and is_active:
                continue

            # Role filter
            if filter_role != "all" and role != filter_role:
                continue

            # Search filter
            if search_text:
                search_lower = search_text.lower()
                if search_lower not in u["user_id"].lower() and search_lower not in u.get("name", "").lower():
                    continue

            filtered_users.append(u)

        # Table header with improved styling
        header_style = {
            "backgroundColor": "#181b22",
            "fontSize": "11px",
            "color": "#5f6672",
            "fontWeight": "600",
            "textTransform": "uppercase",
            "letterSpacing": "0.8px",
            "padding": "10px 16px",
            "borderBottom": "1px solid #1f2229"
        }

        table_header = html.Thead(
            html.Tr([
                html.Th("", style={**header_style, "width": "40px"}),  # Checkbox
                html.Th("User", style={**header_style, "width": "25%"}),
                html.Th("Role", style={**header_style, "width": "12%", "textAlign": "center"}),
                html.Th("Status", style={**header_style, "width": "10%", "textAlign": "center"}),
                html.Th("Dashboards", style={**header_style, "width": "12%"}),
                html.Th("Last Login", style={**header_style, "width": "15%"}),
                html.Th("Actions", style={**header_style, "width": "100px", "textAlign": "right"})
            ])
        )

        # Cell style
        cell_style = {"padding": "14px 16px", "verticalAlign": "middle", "borderBottom": "1px solid #1f2229"}
        center_cell = {**cell_style, "textAlign": "center"}

        table_rows = []
        for idx, u in enumerate(filtered_users):
            user_id = u["user_id"]
            role = u["role"]
            is_active = u.get("is_active", True)
            name = u.get("name", user_id)

            # Avatar
            avatar_color = AVATAR_COLORS[idx % len(AVATAR_COLORS)]
            avatar_letter = name[0].upper() if name else "?"

            avatar = html.Div(avatar_letter, style={
                "width": "34px",
                "height": "34px",
                "borderRadius": "50%",
                "background": avatar_color,
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "center",
                "fontSize": "13px",
                "fontWeight": "600",
                "color": "white",
                "flexShrink": "0"
            })

            # User cell with avatar
            user_cell = html.Td(
                html.Div([
                    avatar,
                    html.Div([
                        html.P(name, style={"margin": "0", "color": "#e8eaed", "fontWeight": "500", "fontSize": "13.5px"}),
                        html.Span(user_id, style={"fontSize": "12px", "color": "#5f6672"})
                    ])
                ], style={"display": "flex", "alignItems": "center", "gap": "12px"}),
                style=cell_style
            )

            # Role badge with consistent styling
            role_styles = {
                "super_admin": {"bg": "rgba(239,68,68,0.12)", "color": "#ef4444", "text": "Super Admin"},
                "admin": {"bg": "rgba(245,158,11,0.12)", "color": "#f59e0b", "text": "Admin"},
                "readonly": {"bg": "rgba(139,92,246,0.12)", "color": "#8b5cf6", "text": "Read Only"}
            }
            role_info = role_styles.get(role, role_styles["readonly"])

            role_badge = html.Span(role_info["text"], style={
                "display": "inline-flex",
                "alignItems": "center",
                "padding": "3px 10px",
                "borderRadius": "20px",
                "fontSize": "11.5px",
                "fontWeight": "600",
                "background": role_info["bg"],
                "color": role_info["color"],
                "letterSpacing": "0.2px"
            })

            # Status badge
            if is_active:
                status_badge = html.Span([
                    html.Span(style={
                        "width": "7px",
                        "height": "7px",
                        "borderRadius": "50%",
                        "backgroundColor": "#34d399",
                        "boxShadow": "0 0 6px #34d399",
                        "marginRight": "6px",
                        "display": "inline-block"
                    }),
                    "Active"
                ], style={"display": "flex", "alignItems": "center", "fontSize": "12.5px", "fontWeight": "500", "color": "#34d399"})
            else:
                status_badge = html.Span([
                    html.Span(style={
                        "width": "7px",
                        "height": "7px",
                        "borderRadius": "50%",
                        "backgroundColor": "#5f6672",
                        "marginRight": "6px",
                        "display": "inline-block"
                    }),
                    "Inactive"
                ], style={"display": "flex", "alignItems": "center", "fontSize": "12.5px", "fontWeight": "500", "color": "#5f6672"})

            # Dashboards
            dashboards = u.get("dashboards", [])
            if dashboards == "all" or role in ("admin", "super_admin"):
                dash_text = "All"
            elif isinstance(dashboards, list) and dashboards:
                dash_text = f"{len(dashboards)} dashboard{'s' if len(dashboards) > 1 else ''}"
            else:
                dash_text = "-"

            # Last login
            last_login = u.get("last_login", "")[:10] if u.get("last_login") else "Never"

            # Action buttons
            can_edit = can_edit_user(current_role, current_username, role, user_id)

            action_btns = []
            if can_edit:
                action_btns = html.Div([
                    dbc.Button("👁", id={"type": "admin-view-btn", "index": user_id}, color="link",
                               style={"width": "30px", "height": "30px", "padding": "0", "color": "#5f6672", "fontSize": "12px"}),
                    dbc.Button("✎", id={"type": "admin-page-edit-btn", "index": user_id}, color="link",
                               style={"width": "30px", "height": "30px", "padding": "0", "color": "#5f6672", "fontSize": "12px"}),
                    dbc.Button("🗑", id={"type": "admin-quick-delete-btn", "index": user_id}, color="link",
                               style={"width": "30px", "height": "30px", "padding": "0", "color": "#5f6672", "fontSize": "12px"}),
                ], style={"display": "flex", "gap": "4px", "opacity": "0", "transition": "opacity 0.2s"}, className="action-btns")
            else:
                action_btns = html.Span("-", style={"color": "#2a2d36"})

            row_style = {"transition": "background 0.2s"}
            if not is_active:
                row_style["opacity"] = "0.4"

            # Checkbox
            checkbox = html.Div(
                html.Div(style={
                    "width": "16px",
                    "height": "16px",
                    "borderRadius": "4px",
                    "border": "1.5px solid #2a2d36",
                    "cursor": "pointer"
                }),
                style={"display": "flex", "alignItems": "center", "justifyContent": "center"}
            )

            table_rows.append(
                html.Tr([
                    html.Td(checkbox, style=cell_style),
                    user_cell,
                    html.Td(role_badge, style=center_cell),
                    html.Td(status_badge, style=center_cell),
                    html.Td(dash_text, style={**cell_style, "fontSize": "12px", "color": "#9aa0ab", "fontFamily": "'JetBrains Mono', monospace"}),
                    html.Td(last_login, style={**cell_style, "fontSize": "12px", "color": last_login == "Never" and "#5f6672" or "#9aa0ab", "fontFamily": "'JetBrains Mono', monospace"}),
                    html.Td(action_btns, style={**cell_style, "textAlign": "right"})
                ], style=row_style, className="admin-table-row")
            )

        if not table_rows:
            empty_state = html.Div([
                html.Div([
                    html.Span("👥", style={"fontSize": "32px", "opacity": "0.3", "marginBottom": "12px", "display": "block"}),
                    html.P("No users match your filters", style={"color": "#5f6672", "fontSize": "13px", "margin": "0"})
                ], style={
                    "textAlign": "center",
                    "padding": "48px 24px"
                })
            ])
            return empty_state, "0 users found", str(len(users)), f"{role_counts['super_admin']} users", f"{role_counts['admin']} users", f"{role_counts['readonly']} users"

        table = dbc.Table(
            [table_header, html.Tbody(table_rows)],
            bordered=False, hover=True, size="sm",
            style={
                "fontSize": "12px",
                "backgroundColor": "#0a0c10",
                "marginBottom": "0"
            },
            className="admin-users-table"
        )

        return table, f"{len(filtered_users)} user{'s' if len(filtered_users) != 1 else ''} found", str(len(users)), f"{role_counts['super_admin']} users", f"{role_counts['admin']} users", f"{role_counts['readonly']} users"

    # =========================================================================
    # ACTIVITY LOG
    # =========================================================================

    @app.callback(
        Output('admin-activity-list', 'children'),
        Input('admin-page-refresh-store', 'data'),
        Input('page-store', 'data'),
        prevent_initial_call=False
    )
    def render_activity_list(refresh_trigger, current_page):
        if current_page != "admin":
            return no_update

        audit_log = get_recent_audit_log(limit=8)

        if not audit_log:
            return html.Div([
                html.Div([
                    html.Span("📊", style={"fontSize": "24px", "opacity": "0.3", "marginBottom": "10px", "display": "block"}),
                    html.P("No activity yet", style={"color": "#5f6672", "fontSize": "13px", "margin": "0"})
                ], style={
                    "textAlign": "center",
                    "padding": "32px 20px"
                })
            ])

        activity_items = []
        for entry in audit_log:
            action = entry.get("action", "")
            actor = entry.get("actor_user_id", "Unknown")
            target = entry.get("target_user_id", "")
            timestamp = entry.get("timestamp", "")

            # Format timestamp
            time_display = timestamp[:16].replace("T", " ") if timestamp else ""

            # Get icon style
            icon_info = ACTIVITY_ICONS.get(action.split("_")[0] if "_" in action else action, ACTIVITY_ICONS["UPDATE"])

            # Build action text
            action_text = action.replace("_", " ").lower()

            activity_items.append(
                html.Div([
                    html.Div(icon_info["icon"], style={
                        "width": "32px",
                        "height": "32px",
                        "borderRadius": "50%",
                        "background": icon_info["bg"],
                        "color": icon_info["color"],
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "center",
                        "fontSize": "13px",
                        "flexShrink": "0"
                    }),
                    html.Div([
                        html.P([
                            html.Strong(actor, style={"fontWeight": "600"}),
                            f" {action_text}",
                            f" {target}" if target else ""
                        ], style={"fontSize": "13px", "color": "#e8eaed", "margin": "0", "lineHeight": "1.5"}),
                        html.Div(time_display, style={"fontSize": "11.5px", "color": "#5f6672", "marginTop": "2px"})
                    ], style={"flex": "1"})
                ], style={
                    "display": "flex",
                    "alignItems": "flex-start",
                    "gap": "14px",
                    "padding": "14px 24px",
                    "transition": "background 0.2s",
                    "cursor": "pointer"
                }, className="activity-item")
            )

        return html.Div(activity_items)

    # =========================================================================
    # OPEN ADD/EDIT MODAL
    # =========================================================================

    @app.callback(
        Output('admin-edit-modal', 'is_open'),
        Output('admin-edit-modal-title', 'children'),
        Output('admin-edit-user-id', 'value'),
        Output('admin-edit-user-id', 'disabled'),
        Output('admin-edit-name', 'value'),
        Output('admin-edit-password', 'value'),
        Output('admin-edit-role', 'options'),
        Output('admin-edit-role', 'value'),
        Output('admin-edit-role', 'disabled'),
        Output('admin-edit-access-store', 'data'),
        Output('admin-edit-mode-store', 'data'),
        Output('admin-edit-access-section', 'style'),
        Output('admin-delete-btn-container', 'style'),
        Input({"type": "admin-page-edit-btn", "index": ALL}, "n_clicks"),
        Input("admin-add-user-btn", "n_clicks"),
        Input("admin-edit-cancel-btn", "n_clicks"),
        State('session-store', 'data'),
        prevent_initial_call=True
    )
    def open_modal(edit_clicks, add_click, cancel_click, session_data):
        triggered = ctx.triggered_id

        # Close on cancel
        if triggered == "admin-edit-cancel-btn":
            return (False, "", "", False, "", "", [], "", False, {},
                    {"mode": "new", "user_id": ""}, {"display": "none"}, {"display": "none"})

        session_id = session_data.get('session_id') if session_data else None
        current_user = get_current_user(session_id) if session_id else None
        current_role = current_user.get("role", "readonly") if current_user else "readonly"
        current_username = current_user.get("username", "") if current_user else ""

        assignable = get_assignable_roles(current_role)
        role_options = [{"label": ROLE_DISPLAY.get(r, r), "value": r} for r in assignable]

        # ADD NEW USER
        if triggered == "admin-add-user-btn":
            if not add_click:
                return (no_update,) * 13
            default_role = "readonly" if "readonly" in assignable else (assignable[0] if assignable else "readonly")
            return (
                True, "Add New User", "", False, "", "",
                role_options, default_role, False, {},
                {"mode": "new", "user_id": ""},
                {"display": "block"}, {"display": "none"}
            )

        # EDIT USER
        if isinstance(triggered, dict) and triggered.get("type") == "admin-page-edit-btn":
            user_id = triggered.get("index", "")
            if not any(c for c in edit_clicks if c):
                return (no_update,) * 13

            users = get_all_users()
            if user_id not in users:
                return (no_update,) * 13

            user_info = users[user_id]
            target_role = user_info.get("role", "readonly")

            # Verify permission
            if not can_edit_user(current_role, current_username, target_role, user_id):
                return (no_update,) * 13

            # Role dropdown
            if target_role == "super_admin":
                edit_role_options = [{"label": "Super Admin", "value": "super_admin"}]
                role_disabled = True
            else:
                edit_role_options = role_options
                role_disabled = current_role == "admin"

            show_access = {"display": "block"} if target_role == "readonly" else {"display": "none"}
            show_delete = {"display": "block"} if can_delete_user(current_role, target_role) else {"display": "none"}

            # Build access data
            dashboards = user_info.get("dashboards", [])
            access_data = {}
            if isinstance(dashboards, list):
                for dash_id in dashboards:
                    access_data[dash_id] = user_info.get("app_access", {}).get(dash_id, [])

            return (
                True, f"Edit User: {user_id}", user_id, True,
                user_info.get("name", ""), user_info.get("password", ""),
                edit_role_options, target_role, role_disabled,
                access_data, {"mode": "edit", "user_id": user_id},
                show_access, show_delete
            )

        return (no_update,) * 13

    # =========================================================================
    # TOGGLE ACCESS SECTION
    # =========================================================================

    @app.callback(
        Output('admin-edit-access-section', 'style', allow_duplicate=True),
        Input('admin-edit-role', 'value'),
        prevent_initial_call=True
    )
    def toggle_access_section(role):
        return {"display": "block"} if role == "readonly" else {"display": "none"}

    # =========================================================================
    # RENDER ACCESS DISPLAY
    # =========================================================================

    @app.callback(
        Output('admin-edit-access-display', 'children'),
        Input('admin-edit-access-store', 'data'),
        prevent_initial_call=True
    )
    def render_access_display(access_data):
        if not access_data:
            return html.P("No dashboards assigned.", style={"color": "#5f6672", "fontSize": "11px", "margin": "10px 0"})

        rows = []
        for dash_id, apps in access_data.items():
            dash_name = get_dashboard_name(dash_id)
            apps_text = ", ".join(sorted(apps)) if apps else "All apps"
            rows.append(
                dbc.Row([
                    dbc.Col([
                        html.Span(dash_name, style={"fontWeight": "500", "color": "#e8eaed", "fontSize": "12px"}),
                        html.Span(f" ({apps_text})", style={"color": "#5f6672", "fontSize": "11px"})
                    ], width=10),
                    dbc.Col([
                        dbc.Button("×", id={"type": "admin-remove-access-btn", "index": dash_id},
                                   color="danger", size="sm", outline=True,
                                   style={"padding": "0 5px", "fontSize": "10px"})
                    ], width=2, style={"textAlign": "right"})
                ], className="mb-1", style={"padding": "6px 8px", "background": "#1f2229", "borderRadius": "4px"})
            )
        return html.Div(rows)

    # =========================================================================
    # LOAD APPS FOR DASHBOARD
    # =========================================================================

    @app.callback(
        Output('admin-edit-add-apps', 'options'),
        Output('admin-edit-add-apps', 'value'),
        Input('admin-edit-add-dashboard', 'value'),
        prevent_initial_call=True
    )
    def load_apps(dashboard_id):
        if not dashboard_id:
            return [], []
        apps = get_available_apps()
        return [{"label": a, "value": a} for a in apps], []

    # =========================================================================
    # ADD ACCESS
    # =========================================================================

    @app.callback(
        Output('admin-edit-access-store', 'data', allow_duplicate=True),
        Input('admin-edit-add-access-btn', 'n_clicks'),
        State('admin-edit-add-dashboard', 'value'),
        State('admin-edit-add-apps', 'value'),
        State('admin-edit-access-store', 'data'),
        prevent_initial_call=True
    )
    def add_access(n_clicks, dashboard_id, apps, current):
        if not n_clicks or not dashboard_id:
            return no_update
        updated = dict(current) if current else {}
        updated[dashboard_id] = apps or []
        return updated

    # =========================================================================
    # REMOVE ACCESS
    # =========================================================================

    @app.callback(
        Output('admin-edit-access-store', 'data', allow_duplicate=True),
        Input({"type": "admin-remove-access-btn", "index": ALL}, "n_clicks"),
        State('admin-edit-access-store', 'data'),
        prevent_initial_call=True
    )
    def remove_access(clicks, current):
        if not any(c for c in clicks if c):
            return no_update
        triggered = ctx.triggered_id
        if isinstance(triggered, dict):
            updated = dict(current) if current else {}
            updated.pop(triggered.get("index", ""), None)
            return updated
        return no_update

    # =========================================================================
    # SAVE USER
    # =========================================================================

    @app.callback(
        Output('admin-edit-status', 'children'),
        Output('admin-page-refresh-store', 'data'),
        Output('admin-edit-modal', 'is_open', allow_duplicate=True),
        Input('admin-edit-save-btn', 'n_clicks'),
        State('admin-edit-user-id', 'value'),
        State('admin-edit-name', 'value'),
        State('admin-edit-password', 'value'),
        State('admin-edit-role', 'value'),
        State('admin-edit-access-store', 'data'),
        State('admin-edit-mode-store', 'data'),
        State('admin-page-refresh-store', 'data'),
        State('session-store', 'data'),
        prevent_initial_call=True
    )
    def save_user(n_clicks, user_id, name, password, role, access_data, mode_data, refresh, session_data):
        if not n_clicks:
            return no_update, no_update, no_update

        # Validation
        if not user_id or not str(user_id).strip():
            return dbc.Alert("User ID is required", color="warning", duration=3000), no_update, no_update
        if not name or not str(name).strip():
            return dbc.Alert("Name is required", color="warning", duration=3000), no_update, no_update
        if not password or not str(password).strip():
            return dbc.Alert("Password is required", color="warning", duration=3000), no_update, no_update
        if not role:
            return dbc.Alert("Role is required", color="warning", duration=3000), no_update, no_update

        session_id = session_data.get('session_id') if session_data else None
        current_user = get_current_user(session_id) if session_id else None
        actor_id = current_user.get("username", "unknown") if current_user else "unknown"
        actor_role = current_user.get("role", "readonly") if current_user else "readonly"

        mode = mode_data.get("mode", "new") if mode_data else "new"

        if role in ("admin", "super_admin"):
            dashboards, app_access = "all", {}
        else:
            dashboards = list(access_data.keys()) if access_data else []
            app_access = access_data or {}

        if mode == "new":
            success, msg = create_user(actor_id, str(user_id).strip(), str(password).strip(), role, str(name).strip(), dashboards, app_access)
        else:
            success, msg = edit_user(actor_id, actor_role, str(user_id).strip(), str(password).strip(), role, str(name).strip(), dashboards, app_access)

        if success:
            return "", (refresh or 0) + 1, False
        return dbc.Alert(msg, color="danger", duration=4000), no_update, no_update

    # =========================================================================
    # DELETE MODAL
    # =========================================================================

    @app.callback(
        Output('admin-delete-modal', 'is_open'),
        Input('admin-edit-delete-btn', 'n_clicks'),
        Input('admin-delete-cancel-btn', 'n_clicks'),
        Input('admin-delete-confirm-btn', 'n_clicks'),
        prevent_initial_call=True
    )
    def toggle_delete_modal(del_click, cancel, confirm):
        triggered = ctx.triggered_id
        if triggered == "admin-edit-delete-btn" and del_click:
            return True
        return False

    # =========================================================================
    # CONFIRM DELETE
    # =========================================================================

    @app.callback(
        Output('admin-page-status', 'children'),
        Output('admin-page-refresh-store', 'data', allow_duplicate=True),
        Output('admin-edit-modal', 'is_open', allow_duplicate=True),
        Input('admin-delete-confirm-btn', 'n_clicks'),
        State('admin-edit-mode-store', 'data'),
        State('admin-page-refresh-store', 'data'),
        State('session-store', 'data'),
        prevent_initial_call=True
    )
    def confirm_delete(n_clicks, mode_data, refresh, session_data):
        if not n_clicks:
            return no_update, no_update, no_update

        user_id = mode_data.get("user_id", "") if mode_data else ""
        if not user_id:
            return dbc.Alert("No user selected", color="danger", duration=3000), no_update, no_update

        session_id = session_data.get('session_id') if session_data else None
        current_user = get_current_user(session_id) if session_id else None
        actor_id = current_user.get("username", "unknown") if current_user else "unknown"
        actor_role = current_user.get("role", "readonly") if current_user else "readonly"

        success, msg = soft_delete_user(actor_id, actor_role, user_id)

        if success:
            return dbc.Alert(f"User '{user_id}' deleted", color="success", duration=3000), (refresh or 0) + 1, False
        return dbc.Alert(msg, color="danger", duration=4000), no_update, no_update
