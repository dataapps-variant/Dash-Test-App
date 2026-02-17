"""
Data Loading for ICARUS Multi Dashboard

Uses the shared master data cache from bigquery_client
but applies different filtering logic:
- Single date (not date range)
- BC is a dimension (0-12 as columns), not a filter
- Pivots by Billing Cycle instead of by Reporting Date
"""

import pyarrow.compute as pc
import pyarrow as pa
from datetime import datetime
import hashlib

from app.bigquery_client import get_master_data, _query_cache, QUERY_CACHE_TTL


# =============================================================================
# CACHE HELPERS
# =============================================================================

def _get_cache_key(*args):
    """Create hash for filter combination"""
    key = "_".join(str(a) for a in args)
    return hashlib.md5(key.encode()).hexdigest()[:16]


def _is_query_cache_valid(cache_key):
    """Check if query cache is valid"""
    if cache_key not in _query_cache:
        return False
    cache = _query_cache[cache_key]
    if cache.get("data") is None or cache.get("loaded_at") is None:
        return False
    age = (datetime.now() - cache["loaded_at"]).total_seconds()
    return age < QUERY_CACHE_TTL


# =============================================================================
# MULTI-SPECIFIC DATA FUNCTIONS
# =============================================================================

def load_multi_dates():
    """Get unique sorted dates from the master data"""
    data = get_master_data()
    dates = data.column("Reporting_Date")
    unique_dates = pc.unique(dates).to_pylist()
    
    # Convert to date objects and sort descending (newest first)
    clean_dates = []
    for d in unique_dates:
        if hasattr(d, 'date'):
            clean_dates.append(d.date())
        else:
            clean_dates.append(d)
    
    return sorted(clean_dates, reverse=True)


def load_multi_plan_groups(active_inactive="Active"):
    """Get unique plan groups - reuses master data"""
    cache_key = _get_cache_key("multi_plans", active_inactive)
    
    if _is_query_cache_valid(cache_key):
        return _query_cache[cache_key]["data"]
    
    data = get_master_data()
    
    mask = pc.equal(data.column("Active_Inactive"), active_inactive)
    filtered = data.filter(mask)
    
    app_names = filtered.column("App_Name").to_pylist()
    plan_names = filtered.column("Plan_Name").to_pylist()
    
    seen = set()
    unique_apps = []
    unique_plans = []
    
    for app, plan in zip(app_names, plan_names):
        if (app, plan) not in seen:
            seen.add((app, plan))
            unique_apps.append(app)
            unique_plans.append(plan)
    
    sorted_pairs = sorted(zip(unique_apps, unique_plans))
    
    result = {
        "App_Name": [p[0] for p in sorted_pairs],
        "Plan_Name": [p[1] for p in sorted_pairs]
    }
    
    _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
    return result


def load_multi_pivot_data(report_date, cohort, plans, metrics, table_type, active_inactive="Active"):
    """
    Load pivot data for Multi dashboard.
    
    Unlike Historical (which filters by BC and pivots by date),
    Multi filters by single date and pivots by BC (0-12 as columns).
    
    Returns dict with: App_Name, Plan_Name, BC, and metric columns
    """
    cache_key = _get_cache_key("multi_pivot", report_date, cohort,
                                tuple(sorted(plans)), tuple(sorted(metrics)),
                                table_type, active_inactive)
    
    if _is_query_cache_valid(cache_key):
        return _query_cache[cache_key]["data"]
    
    data = get_master_data()
    
    # Build filter mask: single date + cohort + active/inactive + table_type
    mask = pc.equal(data.column("Reporting_Date"), report_date)
    mask = pc.and_(mask, pc.equal(data.column("Cohort"), cohort))
    mask = pc.and_(mask, pc.equal(data.column("Active_Inactive"), active_inactive))
    mask = pc.and_(mask, pc.equal(data.column("Table"), table_type))
    
    # Filter by selected plans
    if plans:
        plan_mask = pc.is_in(data.column("Plan_Name"), value_set=pa.array(plans))
        mask = pc.and_(mask, plan_mask)
    
    filtered = data.filter(mask)
    
    result = {
        "App_Name": filtered.column("App_Name").to_pylist(),
        "Plan_Name": filtered.column("Plan_Name").to_pylist(),
        "BC": filtered.column("BC").to_pylist(),
    }
    
    for metric in metrics:
        if metric in filtered.column_names:
            result[metric] = filtered.column(metric).to_pylist()
    
    _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
    return result


def load_multi_chart_data(report_date, cohort, plans, metric, table_type, active_inactive="Active"):
    """
    Load chart data for Multi dashboard.
    
    Returns dict with: Plan_Name, BC, metric_value
    X-axis = BC (0-12), Y-axis = metric value, one line per plan
    """
    cache_key = _get_cache_key("multi_chart", report_date, cohort,
                                tuple(sorted(plans)), metric,
                                table_type, active_inactive)
    
    if _is_query_cache_valid(cache_key):
        return _query_cache[cache_key]["data"]
    
    data = get_master_data()
    
    # Build filter mask
    mask = pc.equal(data.column("Reporting_Date"), report_date)
    mask = pc.and_(mask, pc.equal(data.column("Cohort"), cohort))
    mask = pc.and_(mask, pc.equal(data.column("Active_Inactive"), active_inactive))
    mask = pc.and_(mask, pc.equal(data.column("Table"), table_type))
    
    if plans:
        plan_mask = pc.is_in(data.column("Plan_Name"), value_set=pa.array(plans))
        mask = pc.and_(mask, plan_mask)
    
    filtered = data.filter(mask)
    
    if filtered.num_rows == 0:
        result = {"Plan_Name": [], "BC": [], "metric_value": []}
        _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
        return result
    
    plan_names = filtered.column("Plan_Name").to_pylist()
    bcs = filtered.column("BC").to_pylist()
    values = filtered.column(metric).to_pylist()
    
    # Aggregate by (plan, bc)
    aggregated = {}
    for plan, bc, value in zip(plan_names, bcs, values):
        key = (plan, bc)
        if key not in aggregated:
            aggregated[key] = 0
        if value is not None:
            aggregated[key] += value
    
    result_plans, result_bcs, result_values = [], [], []
    for (plan, bc), total in sorted(aggregated.items()):
        result_plans.append(plan)
        result_bcs.append(bc)
        result_values.append(total)
    
    result = {
        "Plan_Name": result_plans,
        "BC": result_bcs,
        "metric_value": result_values
    }
    
    _query_cache[cache_key] = {"data": result, "loaded_at": datetime.now()}
    return result


def load_all_multi_chart_data(report_date, cohort, plans, metrics, table_type, active_inactive="Active"):
    """
    Load ALL chart metrics in ONE pass for Multi dashboard.
    Returns dict of {metric_name: {Plan_Name, BC, metric_value}}
    """
    cache_key = _get_cache_key("multi_all_charts", report_date, cohort,
                                tuple(sorted(plans)), tuple(sorted(metrics)),
                                table_type, active_inactive)
    
    if _is_query_cache_valid(cache_key):
        return _query_cache[cache_key]["data"]
    
    data = get_master_data()
    
    # Single filter pass
    mask = pc.equal(data.column("Reporting_Date"), report_date)
    mask = pc.and_(mask, pc.equal(data.column("Cohort"), cohort))
    mask = pc.and_(mask, pc.equal(data.column("Active_Inactive"), active_inactive))
    mask = pc.and_(mask, pc.equal(data.column("Table"), table_type))
    
    if plans:
        plan_mask = pc.is_in(data.column("Plan_Name"), value_set=pa.array(plans))
        mask = pc.and_(mask, plan_mask)
    
    filtered = data.filter(mask)
    
    plan_names = filtered.column("Plan_Name").to_pylist()
    bcs = filtered.column("BC").to_pylist()
    
    results = {}
    for metric in metrics:
        if metric not in filtered.column_names:
            results[metric] = {"Plan_Name": [], "BC": [], "metric_value": []}
            continue
        
        values = filtered.column(metric).to_pylist()
        
        # Aggregate by (plan, bc)
        aggregated = {}
        for plan, bc, value in zip(plan_names, bcs, values):
            key = (plan, bc)
            if key not in aggregated:
                aggregated[key] = 0
            if value is not None:
                aggregated[key] += value
        
        r_plans, r_bcs, r_values = [], [], []
        for (plan, bc), total in sorted(aggregated.items()):
            r_plans.append(plan)
            r_bcs.append(bc)
            r_values.append(total)
        
        results[metric] = {
            "Plan_Name": r_plans,
            "BC": r_bcs,
            "metric_value": r_values
        }
    
    _query_cache[cache_key] = {"data": results, "loaded_at": datetime.now()}
    return results
