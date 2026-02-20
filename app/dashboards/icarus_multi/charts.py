"""
Chart Builder for ICARUS Multi Dashboard

Creates line charts with Billing Cycle (0-12) on x-axis
instead of dates. Reuses colors and theme from existing modules.
"""

import plotly.graph_objects as go
from app.colors import build_plan_color_map
from app.theme import get_theme_colors


def hex_to_rgba(hex_color, opacity=1.0):
    """Convert hex color to rgba string"""
    hex_color = hex_color.lstrip('#')
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {opacity})"


def build_bc_line_chart(data, display_name, format_type="dollar", theme="dark"):
    """
    Build a line chart with Billing Cycle (0-12) on x-axis.
    
    Args:
        data: Dict with Plan_Name, BC, metric_value lists
        display_name: Chart title
        format_type: 'dollar', 'percent', or 'number'
        theme: 'dark' or 'light'
    
    Returns:
        Plotly figure and list of unique plans
    """
    colors = get_theme_colors(theme)
    
    # Check for empty data
    if not data or "Plan_Name" not in data or len(data["Plan_Name"]) == 0:
        fig = go.Figure()
        fig.update_layout(
            height=350,
            paper_bgcolor=colors["card_bg"],
            plot_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
            annotations=[{
                "text": "No data available for selected filters",
                "xref": "paper", "yref": "paper",
                "x": 0.5, "y": 0.5,
                "showarrow": False,
                "font": {"size": 14, "color": colors["text_secondary"]}
            }]
        )
        return fig, []
    
    # Get unique plans and build color map
    unique_plans = sorted(set(data["Plan_Name"]))
    color_map = build_plan_color_map(unique_plans)
    
    # Organize data by plan
    plan_data = {}
    for i in range(len(data["Plan_Name"])):
        plan = data["Plan_Name"][i]
        bc = data["BC"][i]
        value = data["metric_value"][i]
        
        if plan not in plan_data:
            plan_data[plan] = {"bcs": [], "values": []}
        plan_data[plan]["bcs"].append(bc)
        plan_data[plan]["values"].append(value if value is not None else 0)
    
    # Create figure
    fig = go.Figure()
    
    LINE_OPACITY = 0.7
    LINE_WIDTH = 1.6
    
    for plan in unique_plans:
        if plan in plan_data:
            # Sort by BC
            sorted_pairs = sorted(zip(plan_data[plan]["bcs"], plan_data[plan]["values"]))
            bcs = [p[0] for p in sorted_pairs]
            values = [p[1] for p in sorted_pairs]
            
            base_color = color_map.get(plan, "#6B7280")
            line_color = hex_to_rgba(base_color, LINE_OPACITY)
            
            # Hover template matching Historical style
            if format_type == "dollar":
                hover_template = (
                    f'{plan}  $%{{y:,.2f}}'
                    f'<extra></extra>'
                )
            elif format_type == "percent":
                hover_template = (
                    f'{plan}  %{{y:.2%}}'
                    f'<extra></extra>'
                )
            else:
                hover_template = (
                    f'{plan}  %{{y:,.0f}}'
                    f'<extra></extra>'
                )
            
            fig.add_trace(
                go.Scatter(
                    x=bcs,
                    y=values,
                    mode='lines',
                    name=plan,
                    line=dict(color=line_color, width=LINE_WIDTH, shape='linear'),
                    hovertemplate=hover_template,
                    showlegend=False,
                    connectgaps=False
                )
            )
    
    # Y-axis formatting
    if format_type == "dollar":
        yaxis_tickprefix = "$"
        yaxis_tickformat = ",.2f"
    elif format_type == "percent":
        yaxis_tickprefix = ""
        yaxis_tickformat = ".1%"
    else:
        yaxis_tickprefix = ""
        yaxis_tickformat = ",d"
    
    # Update layout
    fig.update_layout(
        height=350,
        margin=dict(l=60, r=20, t=20, b=50),
        hovermode="x unified",
        paper_bgcolor=colors["card_bg"],
        plot_bgcolor=colors["card_bg"],
        font=dict(family="Inter, sans-serif", size=12, color=colors["text_primary"]),
        xaxis=dict(
            title=dict(text="Billing Cycle", font=dict(color=colors["text_secondary"], size=12)),
            gridcolor=colors["border"],
            linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickmode="linear",
            tick0=0,
            dtick=1,
            range=[-0.5, 12.5],
            fixedrange=False
        ),
        yaxis=dict(
            gridcolor=colors["border"],
            linecolor=colors["border"],
            tickfont=dict(color=colors["text_secondary"]),
            tickprefix=yaxis_tickprefix,
            tickformat=yaxis_tickformat,
            fixedrange=False
        ),
        legend=dict(font=dict(color=colors["text_primary"]), bgcolor="rgba(0,0,0,0)"),
        dragmode="zoom"
    )
    
    return fig, unique_plans
