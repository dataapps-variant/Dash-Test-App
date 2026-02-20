"""
Configuration and Constants for Variant Analytics Dashboard
Dash Version - Converted from Streamlit
"""

import os

# =============================================================================
# APPLICATION INFO
# =============================================================================
APP_NAME = "VARIANT GROUP"
APP_TITLE = "VARIANT GROUP"
VERSION = "2.0.0"

# =============================================================================
# BIGQUERY CONFIGURATION
# =============================================================================
BIGQUERY_PROJECT = "variant-finance-data-project"
BIGQUERY_DATASET = "ICARUS_Multi"
BIGQUERY_TABLE = "Final_Table"
BIGQUERY_FULL_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# Cache TTL (24 hours in seconds)
CACHE_TTL = 86400

# Auto refresh time (UTC) - 10:15 AM UTC daily
AUTO_REFRESH_HOUR = 10
AUTO_REFRESH_MINUTE = 15

# =============================================================================
# CACHE FILE NAMES (GCS)
# =============================================================================
GCS_ACTIVE_CACHE = "cache/master_data.parquet"
GCS_STAGING_CACHE = "cache/staging_data.parquet"
GCS_BQ_REFRESH_METADATA = "cache/bq_last_refresh.txt"
GCS_GCS_REFRESH_METADATA = "cache/gcs_last_refresh.txt"
GCS_USERS_FILE = "cache/users.json"
GCS_SESSIONS_PREFIX = "cache/sessions/"
GCS_AUDIT_LOG_FILE = "cache/audit_log.json"

# =============================================================================
# SESSION CONFIGURATION
# =============================================================================
SESSION_TTL_DEFAULT = 86400  # 1 day in seconds
SESSION_TTL_REMEMBER = 2592000  # 30 days in seconds
SECRET_KEY = os.environ.get("SECRET_KEY", "variant-dashboard-secret-key-change-in-production")

# =============================================================================
# DASHBOARD REGISTRY
# =============================================================================
DASHBOARDS = [
    {"id": "icarus_historical", "name": "ICARUS - Plan (Historical)", "icon": "📊", "enabled": True},
    {"id": "icarus_multi", "name": "ICARUS - Multi", "icon": "📈", "enabled": True},
    {"id": "all_metrics_merged", "name": "Metrics Merged", "icon": "📊", "enabled": True},
    {"id": "daedalus", "name": "Daedalus", "icon": "🏛️", "enabled": True},
    {"id": "vol_val_plan", "name": "Vol/Val Plan Level", "icon": "📉", "enabled": False},
    {"id": "icarus_cohort", "name": "ICARUS - Cohort", "icon": "👥", "enabled": False},
    {"id": "cwc", "name": "CWC", "icon": "🔄", "enabled": False},
    {"id": "vol_val_entity", "name": "Vol/Val Entity Level", "icon": "🏢", "enabled": False},
]

# =============================================================================
# FILTER OPTIONS
# =============================================================================
BC_OPTIONS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
COHORT_OPTIONS = ["7K", "7K_30D"]

# Default filter values
DEFAULT_BC = 4
DEFAULT_COHORT = "7K"
DEFAULT_PLAN = "JF2788ST"

# =============================================================================
# METRICS CONFIGURATION
# =============================================================================
METRICS_CONFIG = {
    "Subscriptions": {"display": "Subscriptions", "format": "number", "suffix": ""},
    "Rebills": {"display": "Rebills", "format": "number", "suffix": ""},
    "Churn_Rate": {"display": "Churn Rate", "format": "percent", "suffix": " (%)"},
    "Refund_Rate": {"display": "Refund Rate", "format": "percent", "suffix": " (%)"},
    "Gross_ARPU_Retention_Rate": {"display": "Gross ARPU Retention", "format": "percent", "suffix": " (%)"},
    "Net_ARPU_Retention_Rate": {"display": "Net ARPU Retention", "format": "percent", "suffix": " (%)"},
    "Cohort_CAC": {"display": "Cohort CAC", "format": "dollar", "suffix": " ($)"},
    "Recent_CAC": {"display": "Recent CAC", "format": "dollar", "suffix": " ($)"},
    "Gross_ARPU_Discounted": {"display": "Gross ARPU", "format": "dollar", "suffix": " ($)"},
    "Net_ARPU_Discounted": {"display": "Net ARPU", "format": "dollar", "suffix": " ($)"},
    "Net_LTV_Discounted": {"display": "Net LTV", "format": "dollar", "suffix": " ($)"},
    "BC4_CAC_Ceiling": {"display": "BC4 CAC Ceiling", "format": "dollar", "suffix": " ($)"},
}

# Metrics list for filters
METRICS_LIST = list(METRICS_CONFIG.keys())

# =============================================================================
# CHART CONFIGURATION (10 metrics x 2 versions = 20 charts)
# =============================================================================
CHART_METRICS = [
    {"display": "Recent LTV", "metric": "Net_LTV_Discounted", "agg": "SUM", "format": "dollar"},
    {"display": "Gross ARPU", "metric": "Gross_ARPU_Discounted", "agg": "SUM", "format": "dollar"},
    {"display": "Net ARPU", "metric": "Net_ARPU_Discounted", "agg": "SUM", "format": "dollar"},
    {"display": "Subscriptions", "metric": "Subscriptions", "agg": "SUM", "format": "number"},
    {"display": "Rebills", "metric": "Rebills", "agg": "SUM", "format": "number"},
    {"display": "Churn", "metric": "Churn_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Gross Retention", "metric": "Gross_ARPU_Retention_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Refund", "metric": "Refund_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Net ARPU Retention", "metric": "Net_ARPU_Retention_Rate", "agg": "SUM", "format": "percent"},
    {"display": "Recent CAC", "metric": "Recent_CAC", "agg": "SUM", "format": "dollar"},
]

# =============================================================================
# APP COLORS (14 apps - Universal for all charts)
# =============================================================================
APP_COLORS = {
    "AT": "#F97316",        # Orange
    "CL": "#3B82F6",        # Blue
    "CN": "#22C55E",        # Green
    "CT-Non-JP": "#14B8A6", # Teal
    "CT-JP": "#EC4899",     # Pink
    "CV": "#A855F7",        # Purple
    "DT": "#F59E0B",        # Amber
    "EN": "#84CC16",        # Lime
    "FS": "#EF4444",        # Red
    "IQ": "#6366F1",        # Indigo
    "JF": "#10B981",        # Emerald
    "PD": "#F43F5E",        # Rose
    "RL": "#0EA5E9",        # Sky
    "RT": "#8B5CF6",        # Violet
}

# =============================================================================
# THEME COLORS - Single Black & White Theme
# =============================================================================
THEME_COLORS = {
    "dark": {
        "background": "#000000",
        "surface": "#0A0A0A",
        "border": "#1C1C1C",
        "text_primary": "#FFFFFF",
        "text_secondary": "#999999",
        "accent": "#FFFFFF",
        "accent_hover": "#CCCCCC",
        "success": "#4ADE80",
        "warning": "#FACC15",
        "danger": "#F87171",
        "card_bg": "#0A0A0A",
        "input_bg": "#111111",
        "hover": "#1A1A1A",
        "logo_color": "#FFFFFF",
        "table_header_bg": "#141414",
        "table_row_odd": "#0A0A0A",
        "table_row_even": "#000000",
    },
    "light": {
        "background": "#000000",
        "surface": "#0A0A0A",
        "border": "#1C1C1C",
        "text_primary": "#FFFFFF",
        "text_secondary": "#999999",
        "accent": "#FFFFFF",
        "accent_hover": "#CCCCCC",
        "success": "#4ADE80",
        "warning": "#FACC15",
        "danger": "#F87171",
        "card_bg": "#0A0A0A",
        "input_bg": "#111111",
        "hover": "#1A1A1A",
        "logo_color": "#FFFFFF",
        "table_header_bg": "#141414",
        "table_row_odd": "#0A0A0A",
        "table_row_even": "#000000",
    }
}

# =============================================================================
# DEFAULT USERS
# =============================================================================
DEFAULT_USERS = {
    "admin": {
        "password": "admin123",
        "role": "super_admin",
        "name": "Administrator",
        "dashboards": "all",
        "app_access": {}
    },
    "viewer": {
        "password": "viewer123",
        "role": "readonly",
        "name": "Viewer User",
        "dashboards": ["icarus_historical"],
        "app_access": {}
    }
}

# =============================================================================
# ROLE OPTIONS
# =============================================================================
ROLE_OPTIONS = ["super_admin", "admin", "readonly"]
ROLE_DISPLAY = {
    "super_admin": "Super Admin",
    "admin": "Admin",
    "readonly": "Read Only"
}
