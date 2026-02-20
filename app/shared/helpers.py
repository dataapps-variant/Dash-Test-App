"""
Shared Helper Functions
Common utilities used across all dashboards
"""

from app.config import DASHBOARDS
from app.bigquery_client import load_plan_groups


def get_dashboard_name(dashboard_id):
    """Get dashboard display name from ID"""
    for d in DASHBOARDS:
        if d["id"] == dashboard_id:
            return d["name"]
    return dashboard_id


def get_available_apps_for_dashboard(dashboard_id):
    """Get all available App_Names for a dashboard by loading plan groups"""
    try:
        active_plans = load_plan_groups("Active")
        inactive_plans = load_plan_groups("Inactive")
        
        all_apps = set(active_plans.get("App_Name", []))
        all_apps.update(inactive_plans.get("App_Name", []))
        
        return sorted(all_apps)
    except Exception:
        return []
