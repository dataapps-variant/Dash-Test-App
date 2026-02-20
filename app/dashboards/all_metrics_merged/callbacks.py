"""
Callbacks for All Metrics Merged Dashboard

Handles:
- Tab switching and filter visibility
- Plan Name dropdown population
- Tab 1: All Plans (8 charts)
- Tab 2: Individual Plans (4 charts)
- Tab 3: Merged Plans - Breakup (2 charts)
- Tab 4: Entity (5 charts)
"""

from dash import html, dcc, callback, Input, Output, State, no_update, ctx, clientside_callback
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

from app.theme import get_theme_colors
from app.dashboards.all_metrics_merged.charts import build_merged_color_map
from app.charts import create_legend_component
from app.dashboards.all_metrics_merged.layout import chart_card, table_card
from app.dashboards.all_metrics_merged.charts import (
    build_plan_line_chart, build_metric_line_chart, build_stacked_area_chart
)
from app.dashboards.all_metrics_merged.data import (
    get_plan_names_for_app,
    get_vpu_plan_names_for_app,
    get_plan_details,
    get_spend_by_plan,
    get_users_by_plan,
    get_spend_by_plan_single,
    get_metric_summed_all_bcs,
    get_metric_by_bc,
    get_four_metrics_for_plan,
    get_entity_four_metrics,
    get_rebill_contribution,
)


def register_callbacks(app):
    """Register all callbacks for the All Metrics Merged dashboard"""

    # =================================================================
    # FILTER VISIBILITY — show/hide BC and Plan Name based on tab
    # =================================================================

    @callback(
        Output("merged-bc-filter-container", "style"),
        Output("merged-plan-filter-container", "style"),
        Input("merged-dashboard-tabs", "active_tab"),
        prevent_initial_call=True
    )
    def toggle_filter_visibility(active_tab):
        """Show BC dropdown only on All Plans tab, Plan Name only on Individual/Merged tabs"""
        bc_style = {"display": "none"}
        plan_style = {"display": "none"}

        if active_tab == "all-plans":
            bc_style = {"display": "block"}
        elif active_tab in ("individual-plans", "merged-breakup"):
            plan_style = {"display": "block"}
        # entity tab: neither

        return bc_style, plan_style

    # =================================================================
    # PLAN NAME DROPDOWN — populate based on App Name AND active tab
    # =================================================================

    @callback(
        Output("merged-plan-name", "options"),
        Output("merged-plan-name", "value"),
        Input("merged-app-name", "value"),
        Input("merged-dashboard-tabs", "active_tab"),
    )
    def update_plan_dropdown(app_name, active_tab):
        if not app_name:
            return [], None
        # Tab 3 uses VPU (non-merged) plan names
        if active_tab == "merged-breakup":
            plans = get_vpu_plan_names_for_app(app_name)
        else:
            plans = get_plan_names_for_app(app_name)
        options = [{"label": p, "value": p} for p in plans]
        default = plans[0] if plans else None
        return options, default

    # =================================================================
    # MAIN TAB CONTENT RENDERER
    # =================================================================

    @callback(
        Output("merged-tab-content", "children"),
        Input("merged-dashboard-tabs", "active_tab"),
        Input("merged-start-date", "date"),
        Input("merged-end-date", "date"),
        Input("merged-app-name", "value"),
        Input("merged-bc-dropdown", "value"),
        Input("merged-plan-name", "value"),
        State("theme-store", "data"),
    )
    def render_tab_content(active_tab, start_date, end_date, app_name, bc, plan_name, theme):
        theme = theme or "dark"

        if not app_name or not start_date or not end_date:
            return dbc.Alert("Please select App Name and date range.", color="warning")

        try:
            bc = int(bc) if bc is not None else 4
        except (ValueError, TypeError):
            bc = 4

        if active_tab == "all-plans":
            return _render_all_plans(app_name, start_date, end_date, bc, theme)
        elif active_tab == "individual-plans":
            if not plan_name:
                return dbc.Alert("Please select a Plan Name.", color="warning")
            return _render_individual_plans(app_name, start_date, end_date, plan_name, theme)
        elif active_tab == "merged-breakup":
            if not plan_name:
                return dbc.Alert("Please select a Plan Name.", color="warning")
            return _render_merged_breakup(app_name, start_date, end_date, plan_name, theme)
        elif active_tab == "entity":
            return _render_entity(app_name, start_date, end_date, theme)
        else:
            return html.Div()

    # =================================================================
    # TAB 1: ALL PLANS
    # =================================================================

    def _render_all_plans(app_name, start_date, end_date, bc, theme):
        colors = get_theme_colors(theme)
        date_range = (start_date, end_date)
        children = []

        # --- Chart 1: Plan Details Table ---
        plan_df = get_plan_details(app_name)
        if not plan_df.empty:
            col_defs = [{"field": c, "sortable": True, "filter": True, "resizable": True} for c in plan_df.columns]
            grid = dag.AgGrid(
                rowData=plan_df.to_dict("records"),
                columnDefs=col_defs,
                defaultColDef={"flex": 1, "minWidth": 100},
                dashGridOptions={
                    "animateRows": True,
                },
                style={"height": "230px", "width": "100%"},
                className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
            )
            children.append(
                dbc.Card([
                    dbc.CardBody([
                        html.H6("Plan Details", style={"color": colors["text_primary"], "marginBottom": "8px", "fontWeight": "600"}),
                        grid
                    ])
                ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}", "marginBottom": "16px"})
            )
        else:
            children.append(dbc.Alert("No plan details found.", color="secondary"))

        # --- Chart 2: Spend ($) by Individual Plan ---
        spend_df = get_spend_by_plan(app_name, start_date, end_date)
        fig_spend, plans_spend = build_plan_line_chart(spend_df, "Spend ($) by Individual Plan", "dollar", date_range, theme)
        children.append(_chart_with_legend("Spend ($) by Individual Plan", fig_spend, plans_spend, theme))

        # --- Chart 3: New Users by Individual Plan ---
        users_df = get_users_by_plan(app_name, start_date, end_date)
        fig_users, plans_users = build_plan_line_chart(users_df, "New Users by Individual Plan", "number", date_range, theme)
        children.append(_chart_with_legend("New Users by Individual Plan", fig_users, plans_users, theme))

        # --- Charts 4-6: Net ARPU, Net LTV, Recent CAC (SUM all BCs) ---
        for metric, title, fmt in [
            ("Net_ARPU_Discounted", "Net ARPU ($) by Individual Plan", "dollar"),
            ("Net_LTV_Discounted", "Net LTV ($) by Individual Plan", "dollar"),
            ("Recent_CAC", "Recent CAC ($) by Individual Plan", "dollar"),
        ]:
            df = get_metric_summed_all_bcs(app_name, start_date, end_date, metric, "main_30")
            fig, plans = build_plan_line_chart(df, title, fmt, date_range, theme)
            children.append(_chart_with_legend(title, fig, plans, theme))

        # --- Chart 7: Gross Retention by Individual Plan (filtered by BC) ---
        retention_df = get_metric_by_bc(app_name, start_date, end_date, "Retention_rate", bc, "main_30")
        fig_ret, plans_ret = build_plan_line_chart(retention_df, "Gross Retention by Individual Plan", "percent", date_range, theme)
        children.append(_chart_with_legend("Gross Retention by Individual Plan", fig_ret, plans_ret, theme))

        # --- Chart 8: Refund by Individual Plan (filtered by BC) ---
        refund_df = get_metric_by_bc(app_name, start_date, end_date, "Refund_ratio", bc, "main_30")
        fig_ref, plans_ref = build_plan_line_chart(refund_df, "Refund by Individual Plan", "percent", date_range, theme)
        children.append(_chart_with_legend("Refund by Individual Plan", fig_ref, plans_ref, theme))

        return html.Div(children)

    # =================================================================
    # TAB 2: INDIVIDUAL PLANS
    # =================================================================

    def _render_individual_plans(app_name, start_date, end_date, plan_name, theme):
        colors = get_theme_colors(theme)
        date_range = (start_date, end_date)
        children = []

        # --- Chart 1: Individual Plan - T30D (4 metrics, SUM all BCs) ---
        metrics_30 = get_four_metrics_for_plan(app_name, start_date, end_date, plan_name, "main_30")
        fig_30, names_30 = build_metric_line_chart(metrics_30, "Individual Plan - T30D", date_range, theme)
        children.append(_metric_chart_with_legend("Individual Plan - T30D", fig_30, names_30, theme))

        # --- Chart 2: Individual Plan - T300D (4 metrics, SUM all BCs) ---
        metrics_300 = get_four_metrics_for_plan(app_name, start_date, end_date, plan_name, "main_300")
        fig_300, names_300 = build_metric_line_chart(metrics_300, "Individual Plan - T300D", date_range, theme)
        children.append(_metric_chart_with_legend("Individual Plan - T300D", fig_300, names_300, theme))

        # --- Chart 3: New Users by Plan ---
        users_df = get_users_by_plan(app_name, start_date, end_date, plan_name)
        fig_users, plans_users = build_plan_line_chart(users_df, "New Users by Plan", "number", date_range, theme)
        children.append(_chart_with_legend("New Users by Plan", fig_users, plans_users, theme))

        # --- Chart 4: Spend by Individual Plan ---
        spend_df = get_spend_by_plan_single(app_name, start_date, end_date, plan_name)
        fig_spend, plans_spend = build_plan_line_chart(spend_df, "Spend by Individual Plan", "dollar", date_range, theme)
        children.append(_chart_with_legend("Spend by Individual Plan", fig_spend, plans_spend, theme))

        return html.Div(children)

    # =================================================================
    # TAB 3: MERGED PLANS - BREAKUP
    # =================================================================

    def _render_merged_breakup(app_name, start_date, end_date, plan_name, theme):
        colors = get_theme_colors(theme)
        date_range = (start_date, end_date)
        children = []

        # --- Chart 1: Individual Plan - T30D (from VPU.15K_Main_Table) ---
        metrics_30 = get_four_metrics_for_plan(app_name, start_date, end_date, plan_name, "vpu_main")
        fig_30, names_30 = build_metric_line_chart(metrics_30, "Individual Plan - T30D", date_range, theme)
        children.append(_metric_chart_with_legend("Individual Plan - T30D", fig_30, names_30, theme))

        # --- Chart 2: Individual Plan - T300D (from VPU.15K_Main_Table_300) ---
        metrics_300 = get_four_metrics_for_plan(app_name, start_date, end_date, plan_name, "vpu_main_300")
        fig_300, names_300 = build_metric_line_chart(metrics_300, "Individual Plan - T300D", date_range, theme)
        children.append(_metric_chart_with_legend("Individual Plan - T300D", fig_300, names_300, theme))

        return html.Div(children)

    # =================================================================
    # TAB 4: ENTITY
    # =================================================================

    def _render_entity(app_name, start_date, end_date, theme):
        colors = get_theme_colors(theme)
        date_range = (start_date, end_date)
        children = []

        # --- Chart 1: Entity Level (4 metrics, SUM all BCs) ---
        entity_metrics = get_entity_four_metrics(app_name, start_date, end_date)
        fig_entity, names_entity = build_metric_line_chart(entity_metrics, "Entity Level", date_range, theme)
        children.append(_metric_chart_with_legend("Entity Level", fig_entity, names_entity, theme))

        # --- Charts 2-5: Rebill Value Contribution (BC1-BC4) ---
        for bc_val in [1, 2, 3, 4]:
            rebill_df = get_rebill_contribution(app_name, start_date, end_date, bc_val)
            title = f"Rebill Value Contribution (BC{bc_val})"
            fig, plans = build_stacked_area_chart(rebill_df, title, date_range, theme)
            children.append(_chart_with_legend(title, fig, plans, theme))

        return html.Div(children)

    # =================================================================
    # HELPERS — chart + legend wrappers
    # =================================================================

    def _chart_with_legend(title, fig, plans, theme):
        """Wrap a plan-based chart with its color-coded legend"""
        colors = get_theme_colors(theme)
        legend_content = html.Div()
        if plans:
            color_map = build_merged_color_map(plans)
            legend_content = create_legend_component(plans, color_map, theme)

        return dbc.Card([
            dbc.CardBody([
                html.H6(title, style={"color": colors["text_primary"], "marginBottom": "8px", "fontWeight": "600"}),
                legend_content,
                dcc.Graph(figure=fig, config={
                    'displayModeBar': True,
                    'displaylogo': False,
                    'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                    'scrollZoom': False
                }),
            ])
        ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}", "marginBottom": "16px"})

    def _metric_chart_with_legend(title, fig, metric_names, theme):
        """Wrap a metric-based chart (4-metric) with its color-coded legend"""
        from app.dashboards.all_metrics_merged.charts import METRIC_COLORS
        colors = get_theme_colors(theme)

        legend_items = []
        for name in metric_names:
            color = METRIC_COLORS.get(name, "#6B7280")
            legend_items.append(
                html.Span([
                    html.Span(style={
                        "width": "10px", "height": "10px", "borderRadius": "50%",
                        "backgroundColor": color, "display": "inline-block", "marginRight": "6px"
                    }),
                    name
                ], style={
                    "display": "inline-flex", "alignItems": "center", "gap": "6px",
                    "fontSize": "12px", "color": colors["text_primary"], "marginRight": "12px"
                })
            )

        legend_div = html.Div(legend_items, style={
            "background": colors["surface"], "border": f"1px solid {colors['border']}",
            "borderRadius": "8px", "padding": "10px 16px", "marginBottom": "16px",
            "maxHeight": "60px", "overflowY": "auto",
            "display": "flex", "flexWrap": "wrap", "gap": "12px"
        }) if legend_items else html.Div()

        return dbc.Card([
            dbc.CardBody([
                html.H6(title, style={"color": colors["text_primary"], "marginBottom": "8px", "fontWeight": "600"}),
                legend_div,
                dcc.Graph(figure=fig, config={
                    'displayModeBar': True,
                    'displaylogo': False,
                    'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                    'scrollZoom': False
                }),
            ])
        ], style={"background": colors["card_bg"], "border": f"1px solid {colors['border']}", "marginBottom": "16px"})

    # =================================================================
    # DATEPICKER DARK THEME OVERRIDE (MutationObserver approach)
    # =================================================================
    clientside_callback(
        """
        function(active_tab) {
            if (window._dpDarkObserverMerged) return window.dash_clientside.no_update;

            function forceDark() {
                document.querySelectorAll('.DateInput, .DateInput_1').forEach(function(el) {
                    el.style.setProperty('background-color', '#111111', 'important');
                });
                document.querySelectorAll('.DateInput_input, .DateInput_input_1').forEach(function(el) {
                    el.style.setProperty('background-color', '#111111', 'important');
                    el.style.setProperty('color', '#FFFFFF', 'important');
                    el.style.setProperty('border-color', '#333333', 'important');
                });
                document.querySelectorAll('.SingleDatePickerInput, .SingleDatePickerInput_1').forEach(function(el) {
                    el.style.setProperty('background-color', '#111111', 'important');
                    el.style.setProperty('border', 'none', 'important');
                });
                document.querySelectorAll('.SingleDatePicker_picker').forEach(function(el) {
                    el.style.setProperty('background-color', '#111111', 'important');
                });
                document.querySelectorAll('.DayPicker, .DayPicker_transitionContainer, .CalendarMonthGrid, .CalendarMonth').forEach(function(el) {
                    el.style.setProperty('background', '#111111', 'important');
                });
                document.querySelectorAll('.CalendarDay__default').forEach(function(el) {
                    el.style.setProperty('background-color', '#111111', 'important');
                    el.style.setProperty('color', '#FFFFFF', 'important');
                    el.style.setProperty('border', '1px solid #222222', 'important');
                });
                document.querySelectorAll('.CalendarDay__selected').forEach(function(el) {
                    el.style.setProperty('background-color', '#FFFFFF', 'important');
                    el.style.setProperty('color', '#000000', 'important');
                });
                document.querySelectorAll('.CalendarDay__blocked_out_of_range').forEach(function(el) {
                    el.style.setProperty('color', '#333333', 'important');
                    el.style.setProperty('background-color', '#111111', 'important');
                });
                document.querySelectorAll('.CalendarMonth_caption').forEach(function(el) {
                    el.style.setProperty('color', '#FFFFFF', 'important');
                });
                document.querySelectorAll('.DayPicker_weekHeader small').forEach(function(el) {
                    el.style.setProperty('color', '#999999', 'important');
                });
                document.querySelectorAll('.DayPickerNavigation_button').forEach(function(el) {
                    el.style.setProperty('background-color', '#1A1A1A', 'important');
                    el.style.setProperty('border', '1px solid #333333', 'important');
                });
                document.querySelectorAll('.DayPickerNavigation_svg__horizontal').forEach(function(el) {
                    el.style.setProperty('fill', '#FFFFFF', 'important');
                });
                document.querySelectorAll('.DateInput_fang, .DayPickerKeyboardShortcuts_buttonReset').forEach(function(el) {
                    el.style.setProperty('display', 'none', 'important');
                });
            }

            forceDark();
            setTimeout(forceDark, 100);
            setTimeout(forceDark, 300);
            setTimeout(forceDark, 600);

            window._dpDarkObserverMerged = new MutationObserver(function(mutations) {
                forceDark();
                setTimeout(forceDark, 50);
                setTimeout(forceDark, 150);
            });
            window._dpDarkObserverMerged.observe(document.body, {childList: true, subtree: true});

            return window.dash_clientside.no_update;
        }
        """,
        Output("merged-dashboard-tabs", "className"),
        Input("merged-dashboard-tabs", "active_tab"),
    )
