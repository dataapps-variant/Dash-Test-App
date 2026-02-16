"""
Shared Table Components
Reusable pivot table processing and AG Grid rendering
"""

from dash import html
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd


def format_metric_value(value, metric_name, metrics_config, is_crystal_ball=False):
    """Format value based on metric type"""
    if value is None or pd.isna(value):
        return None
    
    config = metrics_config.get(metric_name, {})
    format_type = config.get("format", "number")
    
    try:
        if metric_name == "Rebills" and is_crystal_ball:
            return round(float(value))
        
        if format_type == "percent":
            return round(float(value) * 100, 2)
        return round(float(value), 2)
    except:
        return None


def get_display_metric_name(metric_name, metrics_config):
    """Get display name with suffix"""
    config = metrics_config.get(metric_name, {})
    display = config.get("display", metric_name)
    suffix = config.get("suffix", "")
    return f"{display}{suffix}"


def process_pivot_data(pivot_data, selected_metrics, metrics_config, is_crystal_ball=False):
    """
    Process pivot data into DataFrame for AG Grid.
    
    Args:
        pivot_data: Dict with Reporting_Date, App_Name, Plan_Name, and metric columns
        selected_metrics: List of metric names to include
        metrics_config: Dict of metric_name -> {display, format, suffix}
        is_crystal_ball: Whether this is Crystal Ball data
    
    Returns:
        Tuple of (DataFrame, list of date column names)
    """
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
                "Metric": get_display_metric_name(metric, metrics_config)
            }
            
            for d in unique_dates:
                formatted_date = date_map[d]
                key = (app_name, plan_name, d)
                raw_value = lookup.get(key, {}).get(metric, None)
                formatted_value = format_metric_value(raw_value, metric, metrics_config, is_crystal_ball)
                row[formatted_date] = formatted_value
            
            rows.append(row)
    
    df = pd.DataFrame(rows)
    column_order = ["App", "Plan", "Metric"] + date_columns
    df = df[[c for c in column_order if c in df.columns]]
    
    return df, date_columns


def build_pivot_grid(df, theme="dark"):
    """
    Build an AG Grid component from a processed pivot DataFrame.
    
    Args:
        df: Processed DataFrame from process_pivot_data
        theme: "dark" or "light"
    
    Returns:
        dag.AgGrid component
    """
    return dag.AgGrid(
        rowData=df.to_dict('records'),
        columnDefs=[
            {"field": c, "pinned": "left" if c in ["App", "Plan", "Metric"] else None}
            for c in df.columns
        ],
        defaultColDef={
            "resizable": True,
            "sortable": True,
            "filter": True,
            "wrapHeaderText": True,
            "autoHeaderHeight": True
        },
        columnSize="autoSize",
        columnSizeOptions={"skipHeader": False},
        className="ag-theme-alpine-dark" if theme == "dark" else "ag-theme-alpine",
        style={"height": "400px"}
    )
