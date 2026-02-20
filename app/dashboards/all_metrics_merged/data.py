"""
Data layer for All Metrics Merged Dashboard

Handles:
- Loading 8 BQ tables into GCS cache
- Preloading all tables at startup
- In-memory filtering and aggregation
- Refresh BQ/GCS functions

Tables:
1. Plan_List              - Plan metadata
2. User_Count_by_Day      - Daily user counts
3. 15K_Main_Table_30      - Main metrics (T30D) + Allocated_Spend_Total
4. 15K_Main_Table_300     - Main metrics (T300D)
5. Entity_Level_Main_MP   - Entity-level metrics
6. 15K_Main_Table_MP      - Main table MP
7. VPU.15K_Main_Table     - VPU main (non-merged)
8. VPU.15K_Main_Table_300 - VPU 300D (non-merged)
"""

import pandas as pd
from datetime import datetime
import logging

from app.bigquery_client import (
    get_gcs_bucket, load_parquet_from_gcs, save_parquet_to_gcs,
    get_metadata_timestamp, set_metadata_timestamp, log_debug
)

logger = logging.getLogger(__name__)

# =============================================================================
# TABLE CONFIGURATION
# =============================================================================

MERGED_TABLES = {
    "plan_list": {
        "bq": "variant-finance-data-project.VPU_Merged.Plan_List",
        "active": "merged_cache/plan_list_active.parquet",
        "staging": "merged_cache/plan_list_staging.parquet",
    },

    "user_count": {
        "bq": "variant-finance-data-project.VPU_Merged.User_Count_by_Day",
        "active": "merged_cache/user_count_active.parquet",
        "staging": "merged_cache/user_count_staging.parquet",
    },
    "main_30": {
        "bq": "variant-finance-data-project.VPU_Merged.15K_Main_Table_30",
        "active": "merged_cache/main_30_active.parquet",
        "staging": "merged_cache/main_30_staging.parquet",
    },
    "main_300": {
        "bq": "variant-finance-data-project.VPU_Merged.15K_Main_Table_300",
        "active": "merged_cache/main_300_active.parquet",
        "staging": "merged_cache/main_300_staging.parquet",
    },
    "entity": {
        "bq": "variant-finance-data-project.VPU_Merged.Entity_Level_Main_MP",
        "active": "merged_cache/entity_active.parquet",
        "staging": "merged_cache/entity_staging.parquet",
    },
    "main_mp": {
        "bq": "variant-finance-data-project.VPU.15K_Main_Table_MP",
        "active": "merged_cache/main_mp_active.parquet",
        "staging": "merged_cache/main_mp_staging.parquet",
    },
    "vpu_main": {
        "bq": "variant-finance-data-project.VPU.15K_Main_Table",
        "active": "merged_cache/vpu_main_active.parquet",
        "staging": "merged_cache/vpu_main_staging.parquet",
    },
    "vpu_main_300": {
        "bq": "variant-finance-data-project.VPU.15K_Main_Table_300",
        "active": "merged_cache/vpu_main_300_active.parquet",
        "staging": "merged_cache/vpu_main_300_staging.parquet",
    },
}

GCS_MERGED_BQ_REFRESH = "merged_cache/bq_last_refresh.txt"
GCS_MERGED_GCS_REFRESH = "merged_cache/gcs_last_refresh.txt"

# =============================================================================
# IN-MEMORY CACHE
# =============================================================================

_merged_cache = {}  # key -> pandas DataFrame


def _get_df(key):
    """Get cached DataFrame for a table key"""
    df = _merged_cache.get(key)
    if df is None:
        return pd.DataFrame()
    return df


# =============================================================================
# PRELOAD / REFRESH
# =============================================================================

def preload_merged_tables():
    """Load all 8 tables from GCS into memory at startup"""
    global _merged_cache
    bucket = get_gcs_bucket()

    for key, config in MERGED_TABLES.items():
        try:
            arrow_table = load_parquet_from_gcs(bucket, config["active"])
            if arrow_table is not None:
                _merged_cache[key] = arrow_table.to_pandas()
                logger.info(f"  Merged [{key}]: {len(_merged_cache[key])} rows")
            else:
                _merged_cache[key] = pd.DataFrame()
                logger.warning(f"  Merged [{key}]: no GCS cache found")
        except Exception as e:
            _merged_cache[key] = pd.DataFrame()
            logger.warning(f"  Merged [{key}] load error: {e}")


def refresh_merged_bq_to_staging(skip_keys=None):
    """Load tables from BQ and save to GCS staging. Optionally skip certain keys."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client()
        bucket = get_gcs_bucket()

        if not bucket:
            return False, "GCS bucket not configured"

        skip_keys = skip_keys or []
        loaded = []
        for key, config in MERGED_TABLES.items():
            if key in skip_keys:
                log_debug(f"Skipping merged [{key}]")
                continue
            log_debug(f"Refreshing merged [{key}] from BQ...")
            query = f"SELECT * FROM `{config['bq']}`"
            arrow_table = client.query(query).to_arrow()
            save_parquet_to_gcs(bucket, config["staging"], arrow_table)
            log_debug(f"  {key}: {arrow_table.num_rows} rows saved to staging")
            loaded.append(key)

        set_metadata_timestamp(bucket, GCS_MERGED_BQ_REFRESH)
        return True, f"Merged BQ refresh complete ({len(loaded)} tables). Data saved to staging."
    except Exception as e:
        return False, f"Merged BQ refresh failed: {str(e)}"


def refresh_merged_gcs_from_staging(skip_keys=None):
    """Copy tables from staging to active GCS cache, reload into memory"""
    global _merged_cache
    try:
        bucket = get_gcs_bucket()
        if not bucket:
            return False, "GCS bucket not configured"

        skip_keys = skip_keys or []
        activated = []
        for key, config in MERGED_TABLES.items():
            if key in skip_keys:
                continue
            arrow_table = load_parquet_from_gcs(bucket, config["staging"])
            if arrow_table is None:
                continue
            save_parquet_to_gcs(bucket, config["active"], arrow_table)
            _merged_cache[key] = arrow_table.to_pandas()
            log_debug(f"  Merged [{key}]: {arrow_table.num_rows} rows activated")
            activated.append(key)

        set_metadata_timestamp(bucket, GCS_MERGED_GCS_REFRESH)
        return True, f"Merged GCS refresh complete ({len(activated)} tables activated)."
    except Exception as e:
        return False, f"Merged GCS refresh failed: {str(e)}"


def get_merged_cache_info():
    """Get refresh timestamps for the merged dashboard"""
    bucket = get_gcs_bucket()
    bq_time = get_metadata_timestamp(bucket, GCS_MERGED_BQ_REFRESH)
    gcs_time = get_metadata_timestamp(bucket, GCS_MERGED_GCS_REFRESH)
    return {
        "last_bq_refresh": bq_time.strftime("%d %b, %H:%M") if bq_time else "--",
        "last_gcs_refresh": gcs_time.strftime("%d %b, %H:%M") if gcs_time else "--",
    }


# =============================================================================
# DROPDOWN HELPERS
# =============================================================================

def get_app_names():
    """Get unique App_Name values across main tables"""
    apps = set()
    for key in ["main_30", "plan_list", "user_count"]:
        df = _get_df(key)
        if not df.empty and "App_Name" in df.columns:
            apps.update(df["App_Name"].dropna().unique())
    return sorted(apps)


def get_plan_names_for_app(app_name, table_key="main_30"):
    """Get unique Product_Name_Final values for a given app"""
    df = _get_df(table_key)
    if df.empty:
        return []
    col = "Product_Name_Final"
    if col not in df.columns:
        return []
    filtered = df[df["App_Name"] == app_name]
    return sorted(filtered[col].dropna().unique().tolist())


def get_vpu_plan_names_for_app(app_name):
    """Get unique Product_Name_Final from VPU.15K_Main_Table for Tab 3 dropdown"""
    return get_plan_names_for_app(app_name, table_key="vpu_main")


def get_date_range():
    """Get min/max dates across all date-bearing tables"""
    all_dates = []
    date_col_map = {
        "main_30": "Report_date",
        "user_count": "Date_of_Sale",
    }
    for key, col in date_col_map.items():
        df = _get_df(key)
        if not df.empty and col in df.columns:
            dates = pd.to_datetime(df[col], errors="coerce").dropna()
            if not dates.empty:
                all_dates.append(dates.min())
                all_dates.append(dates.max())
    if not all_dates:
        return None, None
    return min(all_dates).date(), max(all_dates).date()


# =============================================================================
# TAB 1: ALL PLANS — FILTER FUNCTIONS
# =============================================================================

def get_plan_details(app_name):
    """Tab 1 Chart 1: Plan details table filtered by App_Name"""
    df = _get_df("plan_list")
    if df.empty:
        return pd.DataFrame()
    filtered = df[df["App_Name"] == app_name].copy()
    cols = ["Product_Name_Final", "Trial_Type", "Trial_Period", "Trial_Price", "Regular_Price"]
    return filtered[[c for c in cols if c in filtered.columns]].drop_duplicates().sort_values("Product_Name_Final")


def get_spend_by_plan(app_name, start_date, end_date):
    """Tab 1 Chart 2: SUM(Allocated_Spend_Total) per Product_Name_Final per Report_date"""
    df = _get_df("main_30")
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["Report_date"] = pd.to_datetime(df["Report_date"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Report_date"] >= pd.Timestamp(start_date)) &
        (df["Report_date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()
    grouped = filtered.groupby(["Product_Name_Final", "Report_date"], as_index=False)["Allocated_Spend_Total"].sum()
    grouped.rename(columns={"Product_Name_Final": "Plan_Name", "Allocated_Spend_Total": "value"}, inplace=True)
    return grouped.sort_values(["Plan_Name", "Report_date"])


def get_users_by_plan(app_name, start_date, end_date, plan_name=None):
    """Tab 1 Chart 3 / Tab 2 Chart 3: SUM(Daily_Users) per Product_Name_Final per Date"""
    df = _get_df("user_count")
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["Date_of_Sale"] = pd.to_datetime(df["Date_of_Sale"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Date_of_Sale"] >= pd.Timestamp(start_date)) &
        (df["Date_of_Sale"] <= pd.Timestamp(end_date))
    )
    if plan_name:
        mask = mask & (df["Product_Name_Final"] == plan_name)
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()
    grouped = filtered.groupby(["Product_Name_Final", "Date_of_Sale"], as_index=False)["Daily_Users"].sum()
    grouped.rename(columns={"Product_Name_Final": "Plan_Name", "Date_of_Sale": "Report_date", "Daily_Users": "value"}, inplace=True)
    return grouped.sort_values(["Plan_Name", "Report_date"])


def get_spend_by_plan_single(app_name, start_date, end_date, plan_name):
    """Tab 2 Chart 4: Spend for a single plan"""
    df = _get_df("main_30")
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["Report_date"] = pd.to_datetime(df["Report_date"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Report_date"] >= pd.Timestamp(start_date)) &
        (df["Report_date"] <= pd.Timestamp(end_date)) &
        (df["Product_Name_Final"] == plan_name)
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()
    grouped = filtered.groupby(["Product_Name_Final", "Report_date"], as_index=False)["Allocated_Spend_Total"].sum()
    grouped.rename(columns={"Product_Name_Final": "Plan_Name", "Allocated_Spend_Total": "value"}, inplace=True)
    return grouped.sort_values(["Plan_Name", "Report_date"])


def _get_main_table_summed(table_key, app_name, start_date, end_date, metric, plan_name=None):
    """Generic: SUM(metric) across all BCs, grouped by Plan+Date. For charts 4-6 in Tab 1, etc."""
    df = _get_df(table_key)
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["Report_date"] = pd.to_datetime(df["Report_date"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Report_date"] >= pd.Timestamp(start_date)) &
        (df["Report_date"] <= pd.Timestamp(end_date))
    )
    if plan_name:
        mask = mask & (df["Product_Name_Final"] == plan_name)
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()
    grouped = filtered.groupby(["Product_Name_Final", "Report_date"], as_index=False)[metric].sum()
    grouped.rename(columns={"Product_Name_Final": "Plan_Name", metric: "value"}, inplace=True)
    return grouped.sort_values(["Plan_Name", "Report_date"])


def get_metric_summed_all_bcs(app_name, start_date, end_date, metric, table_key="main_30"):
    """Tab 1 Charts 4-6: SUM(metric) across BCs per plan per date"""
    return _get_main_table_summed(table_key, app_name, start_date, end_date, metric)


def get_metric_by_bc(app_name, start_date, end_date, metric, bc, table_key="main_30"):
    """Tab 1 Charts 7-8: metric per plan per date, filtered to specific BC"""
    df = _get_df(table_key)
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["Report_date"] = pd.to_datetime(df["Report_date"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Report_date"] >= pd.Timestamp(start_date)) &
        (df["Report_date"] <= pd.Timestamp(end_date)) &
        (df["Billing_Cycle"] == bc)
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()
    result = filtered[["Product_Name_Final", "Report_date", metric]].copy()
    result.rename(columns={"Product_Name_Final": "Plan_Name", metric: "value"}, inplace=True)
    return result.sort_values(["Plan_Name", "Report_date"])


# =============================================================================
# TAB 2 & 3: INDIVIDUAL / MERGED PLANS — 4-METRIC CHARTS
# =============================================================================

FOUR_METRICS = ["ARPU_Discounted", "Net_ARPU_Discounted", "Recent_CAC", "Net_LTV_Discounted"]
FOUR_METRICS_DISPLAY = {
    "ARPU_Discounted": "Gross ARPU",
    "Net_ARPU_Discounted": "Net ARPU",
    "Recent_CAC": "Recent CAC",
    "Net_LTV_Discounted": "Net LTV",
}


def get_four_metrics_for_plan(app_name, start_date, end_date, plan_name, table_key="main_30"):
    """Tab 2/3 Charts 1-2: 4 metrics SUM across BCs for a single plan.
    Returns dict: {metric_display_name: DataFrame(Report_date, value)}
    """
    df = _get_df(table_key)
    if df.empty:
        return {}
    df = df.copy()
    df["Report_date"] = pd.to_datetime(df["Report_date"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Report_date"] >= pd.Timestamp(start_date)) &
        (df["Report_date"] <= pd.Timestamp(end_date)) &
        (df["Product_Name_Final"] == plan_name)
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    result = {}
    for metric in FOUR_METRICS:
        if metric not in filtered.columns:
            continue
        grouped = filtered.groupby("Report_date", as_index=False)[metric].sum()
        grouped.rename(columns={metric: "value"}, inplace=True)
        grouped = grouped.sort_values("Report_date")
        result[FOUR_METRICS_DISPLAY[metric]] = grouped
    return result


# =============================================================================
# TAB 4: ENTITY — 4-METRIC CHART + REBILL STACKED AREA
# =============================================================================

def get_entity_four_metrics(app_name, start_date, end_date):
    """Tab 4 Chart 1: 4 metrics SUM across BCs at entity level.
    Returns dict: {metric_display_name: DataFrame(Report_date, value)}
    """
    df = _get_df("entity")
    if df.empty:
        return {}
    df = df.copy()
    df["Report_date"] = pd.to_datetime(df["Report_date"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Report_date"] >= pd.Timestamp(start_date)) &
        (df["Report_date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    result = {}
    for metric in FOUR_METRICS:
        if metric not in filtered.columns:
            continue
        grouped = filtered.groupby("Report_date", as_index=False)[metric].sum()
        grouped.rename(columns={metric: "value"}, inplace=True)
        grouped = grouped.sort_values("Report_date")
        result[FOUR_METRICS_DISPLAY[metric]] = grouped
    return result


def get_rebill_contribution(app_name, start_date, end_date, bc):
    """Tab 4 Charts 2-5: Rebill_value per plan as % of total, for a specific BC.
    Returns DataFrame with columns: Plan_Name, Report_date, value (0-1 fraction), raw_value
    """
    df = _get_df("main_30")
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["Report_date"] = pd.to_datetime(df["Report_date"], errors="coerce")
    mask = (
        (df["App_Name"] == app_name) &
        (df["Report_date"] >= pd.Timestamp(start_date)) &
        (df["Report_date"] <= pd.Timestamp(end_date)) &
        (df["Billing_Cycle"] == bc)
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby(["Product_Name_Final", "Report_date"], as_index=False)["Rebill_value"].sum()
    # Compute total per date
    date_totals = grouped.groupby("Report_date")["Rebill_value"].transform("sum")
    grouped["pct"] = grouped["Rebill_value"] / date_totals.replace(0, float("nan"))
    grouped["pct"] = grouped["pct"].fillna(0)

    result = grouped.rename(columns={
        "Product_Name_Final": "Plan_Name",
        "Rebill_value": "raw_value",
        "pct": "value"
    })
    return result.sort_values(["Plan_Name", "Report_date"])
