"""
Shared Chart Builder
Builds chart sections from data - eliminates duplicate chart rendering code
"""

from dash import html, dcc
import dash_bootstrap_components as dbc

from app.theme import get_theme_colors
from app.charts import build_line_chart, get_chart_config, create_legend_component
from app.colors import build_plan_color_map
from app.shared.tables import build_pivot_grid


def build_charts_section(chart_metrics, all_regular_data, all_crystal_data, date_range, theme="dark"):
    """
    Build the complete charts section with Regular + Crystal Ball side-by-side.
    
    Args:
        chart_metrics: List of chart config dicts, each with:
            {"display": "...", "metric": "...", "agg": "...", "format": "..."}
        all_regular_data: Dict of metric -> chart data for Regular
        all_crystal_data: Dict of metric -> chart data for Crystal Ball
        date_range: Tuple of (from_date, to_date)
        theme: "dark" or "light"
    
    Returns:
        List of chart Row components
    """
    colors = get_theme_colors(theme)
    from_date, to_date = date_range
    
    charts_content = []
    for chart_config in chart_metrics:
        display_name = chart_config["display"]
        metric = chart_config["metric"]
        format_type = chart_config["format"]
        
        if format_type == "dollar":
            display_title = f"{display_name} ($)"
        elif format_type == "percent":
            display_title = f"{display_name} (%)"
        else:
            display_title = display_name
        
        empty_data = {"Plan_Name": [], "Reporting_Date": [], "metric_value": []}
        chart_data_regular = all_regular_data.get(metric, empty_data)
        chart_data_crystal = all_crystal_data.get(metric, empty_data)
        
        fig_regular, plans_regular = build_line_chart(
            chart_data_regular, display_title, format_type, (from_date, to_date), theme
        )
        fig_crystal, plans_crystal = build_line_chart(
            chart_data_crystal, f"{display_title} (Crystal Ball)", format_type, (from_date, to_date), theme
        )
        
        color_map_regular = build_plan_color_map(plans_regular) if plans_regular else {}
        color_map_crystal = build_plan_color_map(plans_crystal) if plans_crystal else {}
        
        charts_content.append(
            dbc.Row([
                dbc.Col([
                    html.H6(display_title, style={"color": colors["text_primary"]}),
                    create_legend_component(plans_regular, color_map_regular, theme) if plans_regular else None,
                    dcc.Graph(figure=fig_regular, config=get_chart_config(), style={"height": "420px"})
                ], width=6),
                dbc.Col([
                    html.H6(f"{display_title} (Crystal Ball)", style={"color": colors["text_primary"]}),
                    create_legend_component(plans_crystal, color_map_crystal, theme) if plans_crystal else None,
                    dcc.Graph(figure=fig_crystal, config=get_chart_config(), style={"height": "420px"})
                ], width=6)
            ], className="mb-4")
        )
    
    return charts_content


def build_pivot_section(load_pivot_fn, process_fn, from_date, to_date, bc, cohort, 
                         selected_plans, metrics, status, metrics_config, theme="dark"):
    """
    Build the complete pivot table section with Regular + Crystal Ball.
    
    Args:
        load_pivot_fn: Function to load pivot data (e.g. bigquery_client.load_pivot_data)
        process_fn: Function to process pivot data (e.g. shared.tables.process_pivot_data)
        from_date, to_date: Date range
        bc: Billing cycle
        cohort: Cohort value
        selected_plans: List of selected plan names
        metrics: List of selected metric names
        status: "Active" or "Inactive"
        metrics_config: Metrics configuration dict
        theme: "dark" or "light"
    
    Returns:
        List of pivot table components
    """
    pivot_content = []
    
    # Load regular data
    try:
        pivot_regular = load_pivot_fn(
            from_date, to_date, int(bc), cohort, selected_plans, metrics, "Regular", status
        )
        df_regular, _ = process_fn(pivot_regular, metrics, metrics_config, False)
    except Exception as e:
        df_regular = None
        pivot_content.append(dbc.Alert(f"⚠️ Data loading failed: {str(e)}", color="danger"))
    
    if df_regular is not None and not df_regular.empty:
        pivot_content.append(html.H5("📊 Plan Overview (Regular)"))
        pivot_content.append(build_pivot_grid(df_regular, theme))
    
    # Load crystal ball data
    try:
        pivot_crystal = load_pivot_fn(
            from_date, to_date, int(bc), cohort, selected_plans, metrics, "Crystal Ball", status
        )
        df_crystal, _ = process_fn(pivot_crystal, metrics, metrics_config, True)
    except Exception as e:
        df_crystal = None
        pivot_content.append(dbc.Alert(f"⚠️ Data loading failed: {str(e)}", color="danger"))
    
    if df_crystal is not None and not df_crystal.empty:
        pivot_content.append(html.Br())
        pivot_content.append(html.H5("🔮 Plan Overview (Crystal Ball)"))
        pivot_content.append(build_pivot_grid(df_crystal, theme))
    
    return pivot_content
