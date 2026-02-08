"""
Theme System for Variant Analytics Dashboard - Dash Version
- Full screen layout
- Dark (default) and Light modes
- CSS generation for Dash components
"""

import base64
import os
from app.config import THEME_COLORS


def get_theme_colors(theme="dark"):
    """Get the color palette for specified theme"""
    return THEME_COLORS.get(theme, THEME_COLORS["dark"])


def get_logo_base64():
    """Get the logo as base64 encoded string"""
    logo_path = os.path.join(os.path.dirname(__file__), "assets", "variant_logo.png")
    if os.path.exists(logo_path):
        try:
            with open(logo_path, "rb") as f:
                return base64.b64encode(f.read()).decode()
        except Exception:
            return None
    return None


def get_app_css(theme="dark"):
    """Generate CSS for the entire app based on theme"""
    colors = get_theme_colors(theme)
    
    return f"""
    /* FULL SCREEN LAYOUT */
    html, body {{
        background-color: {colors['background']} !important;
        color: {colors['text_primary']} !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        margin: 0 !important;
        padding: 0 !important;
        min-height: 100vh;
    }}
    
    #root, ._dash-loading {{
        background-color: {colors['background']} !important;
    }}
    
    .dash-table-container {{
        background-color: {colors['card_bg']} !important;
    }}
    
    h1, h2, h3, h4, h5, h6 {{
        color: {colors['text_primary']} !important;
    }}
    
    p, span, label, div {{
        color: {colors['text_primary']};
    }}
    
    /* Card styling */
    .card {{
        background: {colors['card_bg']} !important;
        border: 1px solid {colors['border']} !important;
        border-radius: 12px !important;
        padding: 20px !important;
    }}
    
    /* Filter title */
    .filter-title {{
        font-size: 13px !important;
        font-weight: 600 !important;
        color: {colors['text_secondary']} !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
        margin-bottom: 10px !important;
        padding-bottom: 8px !important;
        border-bottom: 1px solid {colors['border']} !important;
    }}
    
    /* Bootstrap buttons override */
    .btn-primary {{
        background-color: {colors['accent']} !important;
        border-color: {colors['accent']} !important;
        color: #000000 !important;
        font-weight: 600 !important;
    }}
    
    .btn-primary:hover {{
        background-color: {colors['accent_hover']} !important;
        border-color: {colors['accent_hover']} !important;
        color: #000000 !important;
    }}
    
    .btn-secondary {{
        background-color: {colors['surface']} !important;
        border-color: #333333 !important;
        color: {colors['text_primary']} !important;
    }}
    
    .btn-secondary:hover {{
        background-color: {colors['hover']} !important;
        border-color: #444444 !important;
    }}
    
    /* Form inputs */
    .form-control, .form-select {{
        background-color: {colors['input_bg']} !important;
        color: {colors['text_primary']} !important;
        border: 1px solid #333333 !important;
        border-radius: 8px !important;
    }}
    
    .form-control:focus, .form-select:focus {{
        border-color: #FFFFFF !important;
        box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.15) !important;
    }}
    
    .form-control::placeholder {{
        color: #666666 !important;
    }}
    
    /* Dropdown menus */
    .dropdown-menu {{
        background-color: {colors['card_bg']} !important;
        border: 1px solid {colors['border']} !important;
        border-radius: 8px !important;
    }}
    
    .dropdown-item {{
        color: {colors['text_primary']} !important;
    }}
    
    .dropdown-item:hover {{
        background-color: {colors['hover']} !important;
    }}
    
    /* Tabs */
    .nav-tabs {{
        border-bottom: 1px solid {colors['border']} !important;
    }}
    
    .nav-tabs .nav-link {{
        color: {colors['text_secondary']} !important;
        border: none !important;
        border-radius: 8px 8px 0 0 !important;
    }}
    
    .nav-tabs .nav-link.active {{
        background-color: {colors['accent']} !important;
        color: white !important;
    }}
    
    .nav-tabs .nav-link:hover:not(.active) {{
        background-color: {colors['hover']} !important;
        color: {colors['text_primary']} !important;
    }}
    
    /* Scrollbar */
    ::-webkit-scrollbar {{
        width: 8px !important;
        height: 8px !important;
    }}
    
    ::-webkit-scrollbar-track {{
        background: {colors['background']} !important;
        border-radius: 4px !important;
    }}
    
    ::-webkit-scrollbar-thumb {{
        background: {colors['border']} !important;
        border-radius: 4px !important;
    }}
    
    ::-webkit-scrollbar-thumb:hover {{
        background: {colors['text_secondary']} !important;
    }}
    
    /* Alerts */
    .alert {{
        background: {colors['card_bg']} !important;
        border: 1px solid {colors['border']} !important;
        border-radius: 8px !important;
        color: {colors['text_primary']} !important;
    }}
    
    .alert-success {{
        border-left: 4px solid {colors['success']} !important;
    }}
    
    .alert-warning {{
        border-left: 4px solid {colors['warning']} !important;
    }}
    
    .alert-danger {{
        border-left: 4px solid {colors['danger']} !important;
    }}
    
    .alert-info {{
        border-left: 4px solid {colors['accent']} !important;
    }}
    
    /* Modal */
    .modal-content {{
        background-color: {colors['card_bg']} !important;
        border: 1px solid {colors['border']} !important;
        border-radius: 12px !important;
    }}
    
    .modal-header {{
        border-bottom: 1px solid {colors['border']} !important;
    }}
    
    .modal-footer {{
        border-top: 1px solid {colors['border']} !important;
    }}
    
    /* Tables */
    .table {{
        color: {colors['text_primary']} !important;
    }}
    
    .table > thead {{
        background-color: {colors['table_header_bg']} !important;
    }}
    
    .table > tbody > tr:nth-of-type(odd) {{
        background-color: {colors['table_row_odd']} !important;
    }}
    
    .table > tbody > tr:nth-of-type(even) {{
        background-color: {colors['table_row_even']} !important;
    }}
    
    .table > tbody > tr:hover {{
        background-color: {colors['hover']} !important;
    }}
    
    /* Expander/Accordion */
    .accordion-button {{
        background-color: {colors['card_bg']} !important;
        color: {colors['text_primary']} !important;
        border: 1px solid {colors['border']} !important;
    }}
    
    .accordion-button:not(.collapsed) {{
        background-color: {colors['surface']} !important;
    }}
    
    .accordion-body {{
        background-color: {colors['card_bg']} !important;
        border: 1px solid {colors['border']} !important;
        border-top: none !important;
    }}
    
    /* Checkbox styling */
    .form-check-input {{
        background-color: {colors['input_bg']} !important;
        border-color: {colors['border']} !important;
    }}
    
    .form-check-input:checked {{
        background-color: {colors['accent']} !important;
        border-color: {colors['accent']} !important;
    }}
    
    /* Popover */
    .popover {{
        background-color: {colors['card_bg']} !important;
        border: 1px solid {colors['border']} !important;
    }}
    
    .popover-body {{
        color: {colors['text_primary']} !important;
    }}
    
    /* Loading spinner */
    ._dash-loading-callback {{
        background-color: rgba(0, 0, 0, 0.85) !important;
    }}
    
    /* AG Grid overrides */
    .ag-theme-alpine-dark, .ag-theme-alpine {{
        --ag-background-color: {colors['card_bg']};
        --ag-header-background-color: {colors['table_header_bg']};
        --ag-odd-row-background-color: {colors['table_row_odd']};
        --ag-row-hover-color: {colors['hover']};
        --ag-border-color: #1C1C1C;
        --ag-foreground-color: {colors['text_primary']};
        --ag-secondary-foreground-color: {colors['text_secondary']};
        --ag-header-foreground-color: {colors['text_primary']};
        --ag-data-color: {colors['text_primary']};
        --ag-cell-horizontal-border: 1px solid #1C1C1C;
        --ag-row-border-color: #1C1C1C;
    }}
    
    /* Plotly chart background */
    .js-plotly-plot .plotly .bg {{
        fill: {colors['card_bg']} !important;
    }}
    
    /* Ensure ALL text is white on black */
    .form-check-label {{
        color: {colors['text_primary']} !important;
    }}
    
    .table td, .table th {{
        color: {colors['text_primary']} !important;
        border-color: #1C1C1C !important;
    }}
    
    .dropdown-toggle {{
        color: {colors['text_primary']} !important;
    }}
    
    /* Remove any colored badge/pill backgrounds */
    .badge {{
        background-color: #1C1C1C !important;
        color: {colors['text_primary']} !important;
    }}
    
    /* Date picker overrides for black theme */
    .DateInput_input {{
        background-color: {colors['input_bg']} !important;
        color: {colors['text_primary']} !important;
        border-color: #333333 !important;
    }}
    
    .SingleDatePickerInput {{
        background-color: {colors['input_bg']} !important;
        border: 1px solid #333333 !important;
        border-radius: 8px !important;
    }}
    
    .CalendarDay__default {{
        background: {colors['surface']} !important;
        color: {colors['text_primary']} !important;
        border: 1px solid #1C1C1C !important;
    }}
    
    .CalendarDay__selected {{
        background: #FFFFFF !important;
        color: #000000 !important;
    }}
    
    .CalendarDay__hovered_span, .CalendarDay__selected_span {{
        background: #333333 !important;
        color: #FFFFFF !important;
    }}
    
    .DayPickerNavigation_button {{
        background: {colors['surface']} !important;
        border: 1px solid #333333 !important;
    }}
    
    .CalendarMonth_caption {{
        color: {colors['text_primary']} !important;
    }}
    
    .DayPicker {{
        background: {colors['card_bg']} !important;
    }}
    
    .DateInput_fang {{
        display: none !important;
    }}
    """


def get_logo_component(theme="dark", size="large"):
    """Get logo HTML for the header"""
    from dash import html
    
    colors = get_theme_colors(theme)
    logo_size = "80px" if size == "large" else "50px"
    
    logo_base64 = get_logo_base64()
    
    if logo_base64:
        filter_style = "invert(1) brightness(2)"
        return html.Img(
            src=f"data:image/png;base64,{logo_base64}",
            style={
                "width": logo_size,
                "height": "auto",
                "filter": filter_style
            },
            alt="Variant Logo"
        )
    else:
        # Fallback: Styled V
        return html.Div(
            "V",
            style={
                "width": logo_size,
                "height": logo_size,
                "background": colors["accent"],
                "borderRadius": "12px",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "center",
                "fontSize": "36px" if size == "large" else "24px",
                "fontWeight": "bold",
                "color": "white"
            }
        )


def get_header_component(theme="dark", size="large", show_title=True, show_welcome=False, username=""):
    """Get the full header component with logo and title"""
    from dash import html
    
    colors = get_theme_colors(theme)
    title_size = "28px" if size == "large" else "20px"
    
    children = [get_logo_component(theme, size)]
    
    if show_title:
        children.append(
            html.H1(
                "VARIANT GROUP",
                style={
                    "fontSize": title_size,
                    "fontWeight": "700",
                    "color": colors["text_primary"],
                    "margin": "16px 0 0 0",
                    "letterSpacing": "3px"
                }
            )
        )
    
    if show_welcome and username:
        children.append(
            html.P(
                f"Welcome back, {username}",
                style={
                    "fontSize": "16px",
                    "color": colors["text_secondary"],
                    "margin": "8px 0 0 0",
                    "fontWeight": "400"
                }
            )
        )
    
    return html.Div(
        children,
        style={
            "display": "flex",
            "flexDirection": "column",
            "alignItems": "center",
            "justifyContent": "center",
            "padding": "30px 0",
            "textAlign": "center"
        }
    )


def get_plotly_layout(theme="dark"):
    """Get Plotly layout configuration based on theme"""
    colors = get_theme_colors(theme)
    
    return {
        "paper_bgcolor": colors["card_bg"],
        "plot_bgcolor": colors["card_bg"],
        "font": {
            "family": "Inter, sans-serif",
            "size": 12,
            "color": colors["text_primary"]
        },
        "xaxis": {
            "gridcolor": colors["border"],
            "linecolor": colors["border"],
            "tickfont": {"color": colors["text_secondary"]},
            "title": {"font": {"color": colors["text_secondary"]}}
        },
        "yaxis": {
            "gridcolor": colors["border"],
            "linecolor": colors["border"],
            "tickfont": {"color": colors["text_secondary"]},
            "title": {"font": {"color": colors["text_secondary"]}}
        },
        "legend": {
            "font": {"color": colors["text_primary"]},
            "bgcolor": "rgba(0,0,0,0)"
        }
    }
