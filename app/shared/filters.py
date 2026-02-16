"""
Shared Filter Components
Reusable filter layouts for any dashboard
"""

from dash import html, dcc
import dash_bootstrap_components as dbc

from app.theme import get_theme_colors


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


def create_filters_layout(plan_groups, min_date, max_date, prefix, filter_config, theme="dark"):
    """
    Create filters section layout - reusable across dashboards.
    
    Args:
        plan_groups: Dict with App_Name and Plan_Name lists
        min_date: Minimum allowed date
        max_date: Maximum allowed date
        prefix: ID prefix for filter components (e.g. "active", "inactive")
        filter_config: Dict defining which filters to show and their options:
            {
                "show_date_range": True,
                "show_billing_cycle": True,
                "show_cohort": True,
                "show_plan_groups": True,
                "show_metrics": True,
                "bc_options": [0, 1, 2, ...],
                "default_bc": 4,
                "cohort_options": ["7K", "7K_30D"],
                "default_cohort": "7K",
                "default_plan": "JF2788ST",
                "metrics_config": {...},  # Dict of metric_name -> {display, format, suffix}
            }
        theme: "dark" or "light"
    """
    colors = get_theme_colors(theme)
    
    # Extract config with defaults
    show_date_range = filter_config.get("show_date_range", True)
    show_billing_cycle = filter_config.get("show_billing_cycle", True)
    show_cohort = filter_config.get("show_cohort", True)
    show_plan_groups = filter_config.get("show_plan_groups", True)
    show_metrics = filter_config.get("show_metrics", True)
    
    bc_options = filter_config.get("bc_options", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    default_bc = filter_config.get("default_bc", 4)
    cohort_options = filter_config.get("cohort_options", ["7K", "7K_30D"])
    default_cohort = filter_config.get("default_cohort", "7K")
    default_plan = filter_config.get("default_plan", "JF2788ST")
    metrics_config = filter_config.get("metrics_config", {})
    
    # Build filter rows
    filter_rows = []
    
    # ---- Row 1: Date Range, BC, Cohort, Reset ----
    row1_cols = []
    
    if show_date_range:
        row1_cols.append(
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
            ], width=3)
        )
    
    if show_billing_cycle:
        row1_cols.append(
            dbc.Col([
                html.Div("Billing Cycle", className="filter-title"),
                dbc.Select(
                    id=f"{prefix}-bc",
                    options=[{"label": str(bc), "value": bc} for bc in bc_options],
                    value=default_bc
                )
            ], width=2)
        )
    
    if show_cohort:
        row1_cols.append(
            dbc.Col([
                html.Div("Cohort", className="filter-title"),
                dbc.Select(
                    id=f"{prefix}-cohort",
                    options=[{"label": c, "value": c} for c in cohort_options],
                    value=default_cohort
                )
            ], width=2)
        )
    
    # Add any extra filters from config
    extra_filters = filter_config.get("extra_filters", [])
    for extra in extra_filters:
        row1_cols.append(
            dbc.Col([
                html.Div(extra["label"], className="filter-title"),
                dbc.Select(
                    id=f"{prefix}-{extra['id']}",
                    options=[{"label": o, "value": o} for o in extra["options"]],
                    value=extra.get("default", extra["options"][0] if extra["options"] else None)
                )
            ], width=extra.get("width", 2))
        )
    
    # Reset button
    row1_cols.append(
        dbc.Col([
            html.Div(" ", className="filter-title"),
            dbc.Button("🔄 Reset", id=f"{prefix}-reset-btn", color="secondary", className="w-100")
        ], width=2)
    )
    
    filter_rows.append(dbc.Row(row1_cols, className="mb-2"))
    filter_rows.append(html.Hr(style={"margin": "8px 0"}))
    
    # ---- Row 2: Plan Groups ----
    if show_plan_groups:
        plans_by_app = get_plans_by_app(plan_groups)
        app_names = sorted(plans_by_app.keys())
        
        plan_checkboxes = []
        for app_name in app_names:
            plans = sorted(plans_by_app.get(app_name, []))
            visible_plans = plans[:2]
            hidden_plans = plans[2:]
            extra_count = len(hidden_plans)
            
            visible_options = [{"label": plan, "value": plan} for plan in visible_plans]
            hidden_options = [{"label": plan, "value": plan} for plan in hidden_plans]
            
            default_visible = [default_plan] if default_plan in visible_plans else []
            default_hidden = [default_plan] if default_plan in hidden_plans else []
            
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
        
        filter_rows.append(html.Div("Plan Groups", className="filter-title"))
        filter_rows.append(dbc.Row(plan_checkboxes[:6]))
        if len(plan_checkboxes) > 6:
            filter_rows.append(dbc.Row(plan_checkboxes[6:]))
        filter_rows.append(html.Hr())
    
    # ---- Row 3: Metrics ----
    if show_metrics and metrics_config:
        metrics_options = [{"label": metrics_config[m]["display"], "value": m} for m in metrics_config.keys()]
        filter_rows.append(
            dbc.Row([
                dbc.Col([
                    html.Div("Metrics", className="filter-title"),
                    dbc.Checklist(
                        id=f"{prefix}-metrics",
                        options=metrics_options,
                        value=list(metrics_config.keys()),
                        inline=True
                    )
                ])
            ])
        )
    
    return dbc.Accordion([
        dbc.AccordionItem(filter_rows, title="📊 Filters")
    ], start_collapsed=False)
