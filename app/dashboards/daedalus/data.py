"""
Data layer for Daedalus Dashboard

Tables (Tabs 1-5):
1. Daedalus.Daedalus                     - Tab 1 (Daedalus) + Tab 2 (Pacing by Entity)
2. Daedalus.CAC_By_Entity               - Tab 3 (CAC by Entity) + Tab 5 (Historical)
3. Daedalus.Active_Subscriptions        - Tab 4 (Current Subscriptions)

Tables (Tabs 6-16):
4.  Daedalus.Daedalus_Traffic_Channel            - Tabs 6, 7, 8
5.  Daedalus.CAC_By_Traffic_Channel_7D           - Tab 9
6.  Daedalus.AFID_Unknown                        - Tab 10
7.  Daedalus.CPA_By_Entity                       - Tab 11
8.  Daedalus.CPA                                 - Tabs 11, 12
9.  Daedalus.CPA_By_Entity_MTD                   - Tab 12
10. Daedalus.App_Level_Metrics                   - Tab 13
11. Daedalus.App_Channel_Level_Metrics           - Tab 13
12. Daedalus.App_Channel_AFID_Level_Metrics      - Tab 13
13. Daedalus.App_Decline_Reason_Metrics          - Tab 14
14. Daedalus.App_Channel_Decline_Reason_Metrics  - Tab 15
15. Daedalus.App_Channel_AFID_Decline_Reason_Metrics - Tab 16
"""

import pandas as pd
import numpy as np
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

DAEDALUS_TABLES = {
    "daedalus": {
        "bq": "variant-finance-data-project.Daedalus.Daedalus",
        "active": "daedalus_cache/daedalus_active.parquet",
        "staging": "daedalus_cache/daedalus_staging.parquet",
    },
    "cac_entity": {
        "bq": "variant-finance-data-project.Daedalus.CAC_By_Entity",
        "active": "daedalus_cache/cac_entity_active.parquet",
        "staging": "daedalus_cache/cac_entity_staging.parquet",
    },
    "active_subs": {
        "bq": "variant-finance-data-project.Daedalus.Active_Subscriptions",
        "active": "daedalus_cache/active_subs_active.parquet",
        "staging": "daedalus_cache/active_subs_staging.parquet",
    },
    # --- Tabs 6-8: Traffic Channel ---
    "traffic_channel": {
        "bq": "variant-finance-data-project.Daedalus.Daedalus_Traffic_Channel",
        "active": "daedalus_cache/traffic_channel_active.parquet",
        "staging": "daedalus_cache/traffic_channel_staging.parquet",
    },
    # --- Tab 9: CAC Traffic Channel ---
    "cac_tc_7d": {
        "bq": "variant-finance-data-project.Daedalus.CAC_By_Traffic_Channel_7D",
        "active": "daedalus_cache/cac_tc_7d_active.parquet",
        "staging": "daedalus_cache/cac_tc_7d_staging.parquet",
    },
    # --- Tab 10: AFID Unknown ---
    "afid_unknown": {
        "bq": "variant-finance-data-project.Daedalus.AFID_Unknown",
        "active": "daedalus_cache/afid_unknown_active.parquet",
        "staging": "daedalus_cache/afid_unknown_staging.parquet",
    },
    # --- Tab 11: CPA By Entity ---
    "cpa_by_entity": {
        "bq": "variant-finance-data-project.Daedalus.CPA_By_Entity",
        "active": "daedalus_cache/cpa_by_entity_active.parquet",
        "staging": "daedalus_cache/cpa_by_entity_staging.parquet",
    },
    # --- Tabs 11-12: CPA ---
    "cpa": {
        "bq": "variant-finance-data-project.Daedalus.CPA",
        "active": "daedalus_cache/cpa_active.parquet",
        "staging": "daedalus_cache/cpa_staging.parquet",
    },
    # --- Tab 12: CPA By Entity MTD ---
    "cpa_by_entity_mtd": {
        "bq": "variant-finance-data-project.Daedalus.CPA_By_Entity_MTD",
        "active": "daedalus_cache/cpa_by_entity_mtd_active.parquet",
        "staging": "daedalus_cache/cpa_by_entity_mtd_staging.parquet",
    },
    # --- Tab 13: Approval Rates ---
    "app_level_metrics": {
        "bq": "variant-finance-data-project.Daedalus.App_Level_Metrics",
        "active": "daedalus_cache/app_level_metrics_active.parquet",
        "staging": "daedalus_cache/app_level_metrics_staging.parquet",
    },
    "app_channel_metrics": {
        "bq": "variant-finance-data-project.Daedalus.App_Channel_Level_Metrics",
        "active": "daedalus_cache/app_channel_metrics_active.parquet",
        "staging": "daedalus_cache/app_channel_metrics_staging.parquet",
    },
    "app_channel_afid_metrics": {
        "bq": "variant-finance-data-project.Daedalus.App_Channel_AFID_Level_Metrics",
        "active": "daedalus_cache/app_channel_afid_metrics_active.parquet",
        "staging": "daedalus_cache/app_channel_afid_metrics_staging.parquet",
    },
    # --- Tab 14: Decline Reason - App ---
    "decline_app": {
        "bq": "variant-finance-data-project.Daedalus.App_Decline_Reason_Metrics",
        "active": "daedalus_cache/decline_app_active.parquet",
        "staging": "daedalus_cache/decline_app_staging.parquet",
    },
    # --- Tab 15: Decline Reason - Channel ---
    "decline_channel": {
        "bq": "variant-finance-data-project.Daedalus.App_Channel_Decline_Reason_Metrics",
        "active": "daedalus_cache/decline_channel_active.parquet",
        "staging": "daedalus_cache/decline_channel_staging.parquet",
    },
    # --- Tab 16: Decline Reason - AFID ---
    "decline_afid": {
        "bq": "variant-finance-data-project.Daedalus.App_Channel_AFID_Decline_Reason_Metrics",
        "active": "daedalus_cache/decline_afid_active.parquet",
        "staging": "daedalus_cache/decline_afid_staging.parquet",
    },
}

GCS_DAEDALUS_BQ_REFRESH = "daedalus_cache/bq_last_refresh.txt"
GCS_DAEDALUS_GCS_REFRESH = "daedalus_cache/gcs_last_refresh.txt"

# =============================================================================
# IN-MEMORY CACHE
# =============================================================================

_daedalus_cache = {}


def _get_df(key):
    df = _daedalus_cache.get(key)
    if df is None:
        return pd.DataFrame()
    return df


def _ensure_date_col(df, col="Date"):
    """Convert date column to datetime if not already"""
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# =============================================================================
# PRELOAD / REFRESH
# =============================================================================

def preload_daedalus_tables():
    """Load all tables from GCS into memory at startup"""
    global _daedalus_cache
    bucket = get_gcs_bucket()

    for key, config in DAEDALUS_TABLES.items():
        try:
            arrow_table = load_parquet_from_gcs(bucket, config["active"])
            if arrow_table is not None:
                _daedalus_cache[key] = arrow_table.to_pandas()
                logger.info(f"  Daedalus [{key}]: {len(_daedalus_cache[key])} rows")
            else:
                _daedalus_cache[key] = pd.DataFrame()
                logger.warning(f"  Daedalus [{key}]: no GCS cache found")
        except Exception as e:
            _daedalus_cache[key] = pd.DataFrame()
            logger.warning(f"  Daedalus [{key}] load error: {e}")


def refresh_daedalus_bq_to_staging(skip_keys=None):
    """Load tables from BQ and save to GCS staging"""
    try:
        from google.cloud import bigquery
        client = bigquery.Client()
        bucket = get_gcs_bucket()
        if not bucket:
            return False, "GCS bucket not configured"

        skip_keys = skip_keys or []
        loaded = []
        for key, config in DAEDALUS_TABLES.items():
            if key in skip_keys:
                log_debug(f"Skipping Daedalus [{key}]")
                continue
            log_debug(f"Refreshing Daedalus [{key}] from BQ...")
            query = f"SELECT * FROM `{config['bq']}`"
            arrow_table = client.query(query).to_arrow()
            save_parquet_to_gcs(bucket, config["staging"], arrow_table)
            log_debug(f"  {key}: {arrow_table.num_rows} rows saved to staging")
            loaded.append(key)

        set_metadata_timestamp(bucket, GCS_DAEDALUS_BQ_REFRESH)
        return True, f"Daedalus BQ refresh complete ({len(loaded)} tables)."
    except Exception as e:
        return False, f"Daedalus BQ refresh failed: {str(e)}"


def refresh_daedalus_gcs_from_staging(skip_keys=None):
    """Copy tables from staging to active, reload into memory"""
    global _daedalus_cache
    try:
        bucket = get_gcs_bucket()
        if not bucket:
            return False, "GCS bucket not configured"

        skip_keys = skip_keys or []
        activated = []
        for key, config in DAEDALUS_TABLES.items():
            if key in skip_keys:
                continue
            arrow_table = load_parquet_from_gcs(bucket, config["staging"])
            if arrow_table is None:
                continue
            save_parquet_to_gcs(bucket, config["active"], arrow_table)
            _daedalus_cache[key] = arrow_table.to_pandas()
            log_debug(f"  Daedalus [{key}]: {arrow_table.num_rows} rows activated")
            activated.append(key)

        set_metadata_timestamp(bucket, GCS_DAEDALUS_GCS_REFRESH)
        return True, f"Daedalus GCS refresh complete ({len(activated)} tables)."
    except Exception as e:
        return False, f"Daedalus GCS refresh failed: {str(e)}"


def get_daedalus_cache_info():
    bucket = get_gcs_bucket()
    bq_time = get_metadata_timestamp(bucket, GCS_DAEDALUS_BQ_REFRESH)
    gcs_time = get_metadata_timestamp(bucket, GCS_DAEDALUS_GCS_REFRESH)
    return {
        "last_bq_refresh": bq_time.strftime("%d %b, %H:%M") if bq_time else "--",
        "last_gcs_refresh": gcs_time.strftime("%d %b, %H:%M") if gcs_time else "--",
    }


# =============================================================================
# DROPDOWN / FILTER HELPERS
# =============================================================================

def get_daedalus_app_names():
    """Get unique App_Name values from daedalus table"""
    df = _get_df("daedalus")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_daedalus_date_range():
    """Get min/max dates from daedalus table"""
    df = _get_df("daedalus")
    if df.empty or "Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_available_months():
    """Get list of (year, month) tuples from daedalus table"""
    df = _get_df("daedalus")
    if df.empty or "Date" not in df.columns:
        return []
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    ym = dates.dt.to_period("M").unique()
    return sorted([(p.year, p.month) for p in ym], reverse=True)


def get_cac_entity_app_names():
    """Get unique App_Name values from cac_entity table"""
    df = _get_df("cac_entity")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_cac_entity_date_range():
    df = _get_df("cac_entity")
    if df.empty or "Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_active_subs_app_names():
    df = _get_df("active_subs")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_active_subs_channels():
    df = _get_df("active_subs")
    if df.empty or "AFID_CHANNEL" not in df.columns:
        return []
    return sorted(df["AFID_CHANNEL"].dropna().unique().tolist())


def get_active_subs_date_range():
    df = _get_df("active_subs")
    if df.empty or "Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


# =============================================================================
# TAB 1: DAEDALUS — KPI CARDS
# =============================================================================

def get_tab1_kpi_cards():
    """Charts 1-4: KPI cards for latest date, SUM across all apps"""
    df = _get_df("daedalus")
    if df.empty:
        return {}
    df = _ensure_date_col(df.copy())
    latest = df["Date"].max()
    day = df[df["Date"] == latest]

    actual = day["Actual_Spend_MTD"].sum()
    target = day["Target_Spend_MTD"].sum()
    delta = day["Delta_Spend"].sum()
    delta_pct = (delta / target * 100) if target != 0 else 0

    return {
        "actual_spend": actual,
        "allocated_spend": target,
        "spend_delta": delta,
        "spend_delta_pct": delta_pct,
        "date": latest,
    }


# =============================================================================
# TAB 1: DAEDALUS — PIVOT TABLES
# =============================================================================

def get_spend_pivot(app_names, selected_date):
    """Chart 5: Spend pivot — rows=Actual/Target/Delta, cols=App_Name"""
    df = _get_df("daedalus")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (df["Date"] == pd.Timestamp(selected_date)) & (df["App_Name"].isin(app_names))
    day = df.loc[mask]
    if day.empty:
        return pd.DataFrame()

    pivot = day.pivot_table(
        index=None,
        columns="App_Name",
        values=["Actual_Spend_MTD", "Target_Spend_MTD", "Delta_Spend"],
        aggfunc="sum"
    )
    # Flatten to rows: Actual Spend, Target Spend, Delta Spend
    rows = []
    for metric, label in [("Actual_Spend_MTD", "Actual Spend"), ("Target_Spend_MTD", "Target Spend"), ("Delta_Spend", "Delta Spend")]:
        row = {"Metric": label}
        for app in sorted(app_names):
            val = day[day["App_Name"] == app][metric].sum()
            row[app] = val
        rows.append(row)
    return pd.DataFrame(rows)


def get_users_pivot(app_names, selected_date):
    """Chart 9: New Users pivot"""
    df = _get_df("daedalus")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (df["Date"] == pd.Timestamp(selected_date)) & (df["App_Name"].isin(app_names))
    day = df.loc[mask]
    if day.empty:
        return pd.DataFrame()

    rows = []
    for metric, label in [("Actual_New_Users_MTD", "Actual Users"), ("Target_New_Users_MTD", "Target Users"), ("Delta_Users", "Delta Users")]:
        row = {"Metric": label}
        for app in sorted(app_names):
            val = day[day["App_Name"] == app][metric].sum()
            row[app] = val
        rows.append(row)
    return pd.DataFrame(rows)


def get_cac_pivot(app_names, selected_date):
    """Chart 13: CAC pivot"""
    df = _get_df("daedalus")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (df["Date"] == pd.Timestamp(selected_date)) & (df["App_Name"].isin(app_names))
    day = df.loc[mask]
    if day.empty:
        return pd.DataFrame()

    rows = []
    for metric, label in [("Actual_CAC", "Actual CAC"), ("Target_CAC", "Target CAC"), ("Delta_CAC", "Delta CAC")]:
        row = {"Metric": label}
        for app in sorted(app_names):
            val = day[day["App_Name"] == app][metric].sum()
            row[app] = val
        rows.append(row)
    return pd.DataFrame(rows)


# =============================================================================
# TAB 1: DAEDALUS — LINE CHARTS
# =============================================================================

def get_lines_by_app(app_names, year, month, actual_col, target_col):
    """Charts 6, 10: Two lines (actual+target) per app for a given month"""
    df = _get_df("daedalus")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Date"].dt.year == year) &
        (df["Date"].dt.month == month)
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby(["App_Name", "Date"], as_index=False).agg(
        actual=(actual_col, "sum"),
        target=(target_col, "sum")
    )
    return grouped.sort_values(["App_Name", "Date"])


def get_lines_total(app_names, year, month, actual_col, target_col):
    """Charts 7, 11: Two lines (actual+target) summed across apps"""
    df = _get_df("daedalus")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Date"].dt.year == year) &
        (df["Date"].dt.month == month)
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby("Date", as_index=False).agg(
        actual=(actual_col, "sum"),
        target=(target_col, "sum")
    )
    return grouped.sort_values("Date")


# =============================================================================
# TAB 1: DAEDALUS — BAR CHARTS
# =============================================================================

def get_bars_by_app(app_names, selected_date, actual_col, target_col, delta_col):
    """Charts 8, 12, 14: Three bars per app"""
    df = _get_df("daedalus")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (df["Date"] == pd.Timestamp(selected_date)) & (df["App_Name"].isin(app_names))
    day = df.loc[mask]
    if day.empty:
        return pd.DataFrame()

    grouped = day.groupby("App_Name", as_index=False).agg(
        actual=(actual_col, "sum"),
        target=(target_col, "sum"),
        delta=(delta_col, "sum")
    )
    return grouped.sort_values("App_Name")


# =============================================================================
# TAB 2: PACING BY ENTITY
# =============================================================================

def get_pacing_by_entity(year, month):
    """Tab 2: Returns dict {app_name: DataFrame(Date, actual_spend, target_spend, actual_users, target_users)}
    Plus 'VG' key for portfolio total.
    """
    df = _get_df("daedalus")
    if df.empty:
        return {}
    df = _ensure_date_col(df.copy())
    mask = (df["Date"].dt.year == year) & (df["Date"].dt.month == month)
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    result = {}

    # Per App_Name
    for app in sorted(filtered["App_Name"].unique()):
        app_df = filtered[filtered["App_Name"] == app].groupby("Date", as_index=False).agg(
            actual_spend=("Actual_Spend_MTD", "sum"),
            target_spend=("Target_Spend_MTD", "sum"),
            actual_users=("Actual_New_Users_MTD", "sum"),
            target_users=("Target_New_Users_MTD", "sum"),
        ).sort_values("Date")
        result[app] = app_df

    # Portfolio total (VG)
    total = filtered.groupby("Date", as_index=False).agg(
        actual_spend=("Actual_Spend_MTD", "sum"),
        target_spend=("Target_Spend_MTD", "sum"),
        actual_users=("Actual_New_Users_MTD", "sum"),
        target_users=("Target_New_Users_MTD", "sum"),
    ).sort_values("Date")
    result["VG"] = total

    return result


# =============================================================================
# TAB 3: CAC BY ENTITY
# =============================================================================

def get_cac_by_entity(app_names, start_date, end_date, metrics):
    """Tab 3: Returns dict {app_name: DataFrame(Date, [Daily_CAC, T7D_CAC])}"""
    df = _get_df("cac_entity")
    if df.empty:
        return {}
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    cols = ["Date"] + [m for m in metrics if m in filtered.columns]
    result = {}
    for app in sorted(filtered["App_Name"].unique()):
        app_df = filtered[filtered["App_Name"] == app][["Date", "App_Name"] + metrics].copy()
        app_df = app_df.groupby("Date", as_index=False).agg(
            {m: "sum" for m in metrics}
        ).sort_values("Date")
        result[app] = app_df

    return result


# =============================================================================
# TAB 4: CURRENT SUBSCRIPTIONS
# =============================================================================

def get_portfolio_active_subs(app_names, channels, start_date, end_date):
    """Chart 1: SUM(Current_Active_Subscription) per date across all apps/channels"""
    df = _get_df("active_subs")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID_CHANNEL"].isin(channels)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby("Date", as_index=False)["Current_Active_Subscription"].sum()
    return grouped.sort_values("Date")


def get_current_subs_pivot(app_names, channels, start_date, end_date):
    """Chart 2: Pivot table — rows=metrics, cols=dates (reversed)"""
    df = _get_df("active_subs")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID_CHANNEL"].isin(channels)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    # Sum by date across all apps/channels
    daily = filtered.groupby("Date", as_index=False).agg(
        Active_Subscription_30_Days_Ago=("Active_Subscription_30_Days_Ago", "sum"),
        Cancelled_Subscription_Orders_Voluntary=("Cancelled_Subscription_Orders_Voluntary", "sum"),
        Ended_Subscriptions_Involuntary=("Ended_Subscriptions_Involuntary", "sum"),
        Total_Lost_Subscriptions=("Total_Lost_Subscriptions", "sum"),
        T30_Day_New_Subscriptions=("T30_Day_New_Subscriptions", "sum"),
        Current_Active_Subscription=("Current_Active_Subscription", "sum"),
        Current_Pending_Subscriptions=("Current_Pending_Subscriptions", "sum"),
        T30_Day_New_SS_Orders=("T30_Day_New_SS_Orders", "sum"),
    ).sort_values("Date", ascending=False)

    # Compute derived metrics
    daily["Churn_Rate_Pct"] = np.where(
        daily["Active_Subscription_30_Days_Ago"] > 0,
        (daily["Total_Lost_Subscriptions"] / daily["Active_Subscription_30_Days_Ago"] * 100).round(2),
        np.nan
    )
    daily["Pending_Subscriptions_Pct"] = np.where(
        daily["Current_Active_Subscription"] > 0,
        (daily["Current_Pending_Subscriptions"] / daily["Current_Active_Subscription"] * 100).round(2),
        np.nan
    )
    daily["SS_Orders_Pct"] = np.where(
        daily["T30_Day_New_Subscriptions"] > 0,
        (daily["T30_Day_New_SS_Orders"] / daily["T30_Day_New_Subscriptions"] * 100).round(2),
        np.nan
    )

    # Build rows (metric per row, date per column)
    dates = daily["Date"].tolist()
    metric_order = [
        ("30 Days Ago Active Subscriptions", "Active_Subscription_30_Days_Ago", "int"),
        ("Cancelled Subscription Orders (Voluntary)", "Cancelled_Subscription_Orders_Voluntary", "int"),
        ("Ended Subscriptions (Involuntary)", "Ended_Subscriptions_Involuntary", "int"),
        ("Total Lost Subscriptions", "Total_Lost_Subscriptions", "int"),
        ("Churn Rate %", "Churn_Rate_Pct", "pct"),
        ("T30 Day New Subscriptions", "T30_Day_New_Subscriptions", "int"),
        ("Current Active Subscription", "Current_Active_Subscription", "int"),
        ("Pending Subscriptions", "Current_Pending_Subscriptions", "int"),
        ("Pending Subscription %", "Pending_Subscriptions_Pct", "pct"),
        ("T30 Day New SS Orders", "T30_Day_New_SS_Orders", "int"),
        ("SS Order %", "SS_Orders_Pct", "pct"),
    ]

    rows = []
    for label, col, fmt in metric_order:
        row = {"Metric": label}
        for _, drow in daily.iterrows():
            date_str = drow["Date"].strftime("%Y-%m-%d")
            val = drow[col]
            if fmt == "pct":
                row[date_str] = f"{val:.2f}%"
            else:
                row[date_str] = f"{int(val):,}"
        rows.append(row)

    return pd.DataFrame(rows)


def get_pie_by_app(app_names, channels, selected_date):
    """Chart 3: Pie chart — Current_Active_Subscription per App_Name on single date"""
    df = _get_df("active_subs")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID_CHANNEL"].isin(channels)) &
        (df["Date"] == pd.Timestamp(selected_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby("App_Name", as_index=False)["Current_Active_Subscription"].sum()
    grouped = grouped[grouped["Current_Active_Subscription"] > 0]
    return grouped.sort_values("Current_Active_Subscription", ascending=False)


def get_pie_by_app_channel(app_names, channels, selected_date):
    """Chart 4: Pie chart — Current_Active_Subscription per App_Name + AFID_CHANNEL"""
    df = _get_df("active_subs")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID_CHANNEL"].isin(channels)) &
        (df["Date"] == pd.Timestamp(selected_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby(["App_Name", "AFID_CHANNEL"], as_index=False)["Current_Active_Subscription"].sum()
    grouped = grouped[grouped["Current_Active_Subscription"] > 0]
    grouped["Label"] = grouped["App_Name"] + ", " + grouped["AFID_CHANNEL"].astype(str)
    return grouped.sort_values("Current_Active_Subscription", ascending=False)


def get_entity_active_subs(app_names, channels, start_date, end_date):
    """Chart 5: Line per App_Name — Current_Active_Subscription over time"""
    df = _get_df("active_subs")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID_CHANNEL"].isin(channels)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby(["App_Name", "Date"], as_index=False)["Current_Active_Subscription"].sum()
    return grouped.sort_values(["App_Name", "Date"])


def _ratio_by_entity(app_names, channels, start_date, end_date, numerator, denominator):
    """Generic: Compute ratio per app per date"""
    df = _get_df("active_subs")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID_CHANNEL"].isin(channels)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby(["App_Name", "Date"], as_index=False).agg(
        num=(numerator, "sum"),
        den=(denominator, "sum"),
    )
    grouped["value"] = np.where(grouped["den"] > 0, grouped["num"] / grouped["den"], np.nan)
    return grouped[["App_Name", "Date", "value"]].sort_values(["App_Name", "Date"])


def _ratio_portfolio(app_names, channels, start_date, end_date, numerator, denominator):
    """Generic: Compute portfolio ratio per date"""
    df = _get_df("active_subs")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID_CHANNEL"].isin(channels)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby("Date", as_index=False).agg(
        num=(numerator, "sum"),
        den=(denominator, "sum"),
    )
    grouped["value"] = np.where(grouped["den"] > 0, grouped["num"] / grouped["den"], np.nan)
    return grouped[["Date", "value"]].sort_values("Date")


def get_entity_churn(app_names, channels, start_date, end_date):
    """Chart 6"""
    return _ratio_by_entity(app_names, channels, start_date, end_date,
                            "Total_Lost_Subscriptions", "Active_Subscription_30_Days_Ago")

def get_portfolio_churn(app_names, channels, start_date, end_date):
    """Chart 7"""
    return _ratio_portfolio(app_names, channels, start_date, end_date,
                            "Total_Lost_Subscriptions", "Active_Subscription_30_Days_Ago")

def get_entity_ss(app_names, channels, start_date, end_date):
    """Chart 8"""
    return _ratio_by_entity(app_names, channels, start_date, end_date,
                            "T30_Day_New_SS_Orders", "T30_Day_New_Subscriptions")

def get_portfolio_ss(app_names, channels, start_date, end_date):
    """Chart 9"""
    return _ratio_portfolio(app_names, channels, start_date, end_date,
                            "T30_Day_New_SS_Orders", "T30_Day_New_Subscriptions")

def get_entity_pending(app_names, channels, start_date, end_date):
    """Chart 10"""
    return _ratio_by_entity(app_names, channels, start_date, end_date,
                            "Current_Pending_Subscriptions", "Current_Active_Subscription")

def get_portfolio_pending(app_names, channels, start_date, end_date):
    """Chart 11"""
    return _ratio_portfolio(app_names, channels, start_date, end_date,
                            "Current_Pending_Subscriptions", "Current_Active_Subscription")


# =============================================================================
# TAB 5: DAEDALUS (HISTORICAL)
# =============================================================================

def get_historical_metric_by_app(app_names, start_date, end_date, metric):
    """Tabs 5 Charts 1-6: Line per app for a given metric from cac_entity"""
    df = _get_df("cac_entity")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby(["App_Name", "Date"], as_index=False)[metric].sum()
    grouped.rename(columns={metric: "value"}, inplace=True)
    return grouped.sort_values(["App_Name", "Date"])


def get_historical_spend_split(app_names, start_date, end_date):
    """Tab 5 Chart 7: Pie chart — SUM(Daily_Spend) per App_Name"""
    df = _get_df("cac_entity")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby("App_Name", as_index=False)["Daily_Spend"].sum()
    grouped = grouped[grouped["Daily_Spend"] > 0]
    return grouped.sort_values("Daily_Spend", ascending=False)


# =============================================================================
# TABS 6-8: TRAFFIC CHANNEL — Filter Helpers
# =============================================================================

def get_tc_app_names():
    """Get unique App_Name values from traffic_channel table"""
    df = _get_df("traffic_channel")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_tc_date_range():
    df = _get_df("traffic_channel")
    if df.empty or "Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_tc_channels():
    """Get unique Traffic_Channel integer IDs from traffic_channel table"""
    df = _get_df("traffic_channel")
    if df.empty or "Traffic_Channel" not in df.columns:
        return []
    return sorted(df["Traffic_Channel"].dropna().unique().tolist())


# =============================================================================
# TAB 6: TRAFFIC CHANNEL — Data Retrieval
# =============================================================================

def get_tc_lines_by_app(start_date, end_date, channels, metric_col):
    """Tab 6: Returns dict {app_name: DataFrame(Date, Traffic_Channel, value)}
    metric_col = 'T30D_Spend' or 'T30D_Users'
    """
    df = _get_df("traffic_channel")
    if df.empty:
        return {}
    df = _ensure_date_col(df.copy())
    mask = (
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date)) &
        (df["Traffic_Channel"].isin(channels))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    result = {}
    # VG first if present
    apps = sorted(filtered["App_Name"].unique())
    if "VG" in apps:
        apps = ["VG"] + [a for a in apps if a != "VG"]

    for app in apps:
        adf = filtered[filtered["App_Name"] == app].copy()
        grouped = adf.groupby(["Date", "Traffic_Channel"], as_index=False)[metric_col].sum()
        grouped.rename(columns={metric_col: "value"}, inplace=True)
        grouped = grouped.sort_values(["Traffic_Channel", "Date"])
        # Only include channels that have data
        grouped = grouped[grouped["value"].notna()]
        if not grouped.empty:
            result[app] = grouped

    return result


# =============================================================================
# TABS 7-8: NEW USERS / SPEND — Traffic Channel (Pie + Stacked Area)
# =============================================================================

def get_tc_pie_by_app(start_date, end_date, channels, metric_col):
    """Tabs 7/8: Returns dict {app_name: DataFrame(Traffic_Channel, total)}
    metric_col = 'Daily_New_Users' or 'Daily_Spend'
    """
    df = _get_df("traffic_channel")
    if df.empty:
        return {}
    df = _ensure_date_col(df.copy())
    mask = (
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date)) &
        (df["Traffic_Channel"].isin(channels))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    result = {}
    apps = sorted(filtered["App_Name"].unique())
    if "VG" in apps:
        apps = ["VG"] + [a for a in apps if a != "VG"]

    for app in apps:
        adf = filtered[filtered["App_Name"] == app]
        grouped = adf.groupby("Traffic_Channel", as_index=False)[metric_col].sum()
        grouped.rename(columns={metric_col: "total"}, inplace=True)
        grouped = grouped[grouped["total"] > 0]
        if not grouped.empty:
            result[app] = grouped.sort_values("total", ascending=False)

    return result


def get_tc_stacked_by_app(start_date, end_date, channels, metric_col):
    """Tabs 7/8: Returns dict {app_name: DataFrame(Date, Traffic_Channel, value)}
    For stacked area chart. metric_col = 'Daily_New_Users' or 'Daily_Spend'
    """
    df = _get_df("traffic_channel")
    if df.empty:
        return {}
    df = _ensure_date_col(df.copy())
    mask = (
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date)) &
        (df["Traffic_Channel"].isin(channels))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    result = {}
    apps = sorted(filtered["App_Name"].unique())
    if "VG" in apps:
        apps = ["VG"] + [a for a in apps if a != "VG"]

    for app in apps:
        adf = filtered[filtered["App_Name"] == app]
        grouped = adf.groupby(["Date", "Traffic_Channel"], as_index=False)[metric_col].sum()
        grouped.rename(columns={metric_col: "value"}, inplace=True)
        grouped = grouped.sort_values(["Traffic_Channel", "Date"])
        if not grouped.empty:
            result[app] = grouped

    return result


# =============================================================================
# TAB 9: CAC - TRAFFIC CHANNEL
# =============================================================================

def get_cac_tc_date_range():
    df = _get_df("cac_tc_7d")
    if df.empty or "Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_cac_tc_channels():
    df = _get_df("cac_tc_7d")
    if df.empty or "Traffic_Channel" not in df.columns:
        return []
    return sorted(df["Traffic_Channel"].dropna().unique().tolist())


def get_cac_tc_by_app(start_date, end_date, channels, metrics):
    """Tab 9: Returns dict {app_name: DataFrame(Date, Traffic_Channel, [Daily_CAC, T7D_CAC])}
    metrics = list of column names to include, e.g. ['Daily_CAC', 'T7D_CAC']
    """
    df = _get_df("cac_tc_7d")
    if df.empty:
        return {}
    df = _ensure_date_col(df.copy())
    mask = (
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date)) &
        (df["Traffic_Channel"].isin(channels))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    cols_needed = ["Date", "Traffic_Channel"] + [m for m in metrics if m in filtered.columns]
    result = {}
    apps = sorted(filtered["App_Name"].unique())
    if "VG" in apps:
        apps = ["VG"] + [a for a in apps if a != "VG"]

    for app in apps:
        adf = filtered[filtered["App_Name"] == app][cols_needed].copy()
        agg_dict = {m: "mean" for m in metrics if m in adf.columns}
        grouped = adf.groupby(["Date", "Traffic_Channel"], as_index=False).agg(agg_dict)
        grouped = grouped.sort_values(["Traffic_Channel", "Date"])
        if not grouped.empty:
            result[app] = grouped

    return result


# =============================================================================
# TAB 10: AFID UNKNOWN
# =============================================================================

def get_afid_unknown_date_range():
    df = _get_df("afid_unknown")
    if df.empty or "Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_afid_unknown_apps():
    df = _get_df("afid_unknown")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_afid_unknown_afids():
    df = _get_df("afid_unknown")
    if df.empty or "AFID" not in df.columns:
        return []
    return sorted(df["AFID"].dropna().unique().tolist())


def get_afid_unknown_pie(app_names, afids, start_date, end_date):
    """Tab 10 Chart 1: Pie — SUM(New_Users) per AFID over date range"""
    df = _get_df("afid_unknown")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID"].isin(afids)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby("AFID", as_index=False)["New_Users"].sum()
    grouped = grouped[grouped["New_Users"] > 0]
    return grouped.sort_values("New_Users", ascending=False)


def get_afid_unknown_stacked(app_names, afids, start_date, end_date):
    """Tab 10 Chart 2: Stacked Area — New_Users per date per AFID"""
    df = _get_df("afid_unknown")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID"].isin(afids)) &
        (df["Date"] >= pd.Timestamp(start_date)) &
        (df["Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return pd.DataFrame()

    grouped = filtered.groupby(["Date", "AFID"], as_index=False)["New_Users"].sum()
    return grouped.sort_values(["AFID", "Date"])


# =============================================================================
# TAB 11: DAILY REPORT
# =============================================================================

def get_cpa_entity_names():
    """Get unique Entity_Name values from CPA_By_Entity"""
    df = _get_df("cpa_by_entity")
    if df.empty or "Entity_Name" not in df.columns:
        return []
    return sorted(df["Entity_Name"].dropna().unique().tolist())


def get_cpa_app_names():
    """Get unique App_Name values from CPA table"""
    df = _get_df("cpa")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_cpa_dates():
    """Get sorted dates from CPA_By_Entity for date picker"""
    df = _get_df("cpa_by_entity")
    if df.empty or "Date" not in df.columns:
        return []
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return []
    return sorted(dates.dt.date.unique().tolist(), reverse=True)


def get_cpa_by_entity_daily(selected_date):
    """Tab 11 Table 1: CPA By Entity — filter by date only"""
    df = _get_df("cpa_by_entity")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    filtered = df[df["Date"] == pd.Timestamp(selected_date)].copy()
    if filtered.empty:
        return pd.DataFrame()

    result = filtered[["Entity_Name", "Daily_Total_Users", "Daily_New_Regular_Users",
                        "Daily_Subscriptions", "Daily_SS_Users", "Daily_Spend", "Daily_CAC"]].copy()
    result = result.rename(columns={
        "Entity_Name": "Entity",
        "Daily_Total_Users": "Total",
        "Daily_New_Regular_Users": "Trials",
        "Daily_Subscriptions": "New Subscriptions",
        "Daily_SS_Users": "Single Sale",
        "Daily_Spend": "AD Spend",
        "Daily_CAC": "CAC",
    })

    # Total row
    total_row = {
        "Entity": "Total",
        "Total": result["Total"].sum(),
        "Trials": result["Trials"].sum(),
        "New Subscriptions": result["New Subscriptions"].sum(),
        "Single Sale": result["Single Sale"].sum(),
        "AD Spend": result["AD Spend"].sum(),
        "CAC": None,
    }
    total_users = total_row["Total"]
    total_spend = total_row["AD Spend"]
    total_row["CAC"] = (total_spend / total_users) if total_users > 0 else 0
    result = pd.concat([result, pd.DataFrame([total_row])], ignore_index=True)
    return result


def get_cpa_by_application_daily(selected_date, entity_names, app_names):
    """Tab 11 Table 2: CPA By Application — filter by date + entity + app"""
    df = _get_df("cpa")
    if df.empty:
        return pd.DataFrame()
    df["Report_Date"] = pd.to_datetime(df["Report_Date"], errors="coerce")
    mask = (
        (df["Report_Date"] == pd.Timestamp(selected_date)) &
        (df["Entity_Name"].isin(entity_names)) &
        (df["App_Name"].isin(app_names))
    )
    filtered = df.loc[mask].copy()
    if filtered.empty:
        return pd.DataFrame()

    result = filtered[["Entity_Name", "App_Name", "Channel_Name",
                        "Total", "Trial_Users", "New_Subs_Users",
                        "Single_Sale_Users", "Ad_Spend", "CAC"]].copy()
    result = result.rename(columns={
        "Entity_Name": "Entity",
        "App_Name": "App",
        "Channel_Name": "Source System",
        "Trial_Users": "Trials",
        "New_Subs_Users": "New Subscriptions",
        "Single_Sale_Users": "Single Sale",
        "Ad_Spend": "AD Spend",
    })

    # Total row
    total_row = {
        "Entity": "Total", "App": "", "Source System": "",
        "Total": result["Total"].sum(),
        "Trials": result["Trials"].sum(),
        "New Subscriptions": result["New Subscriptions"].sum(),
        "Single Sale": result["Single Sale"].sum(),
        "AD Spend": result["AD Spend"].sum(),
        "CAC": None,
    }
    total_users = total_row["Total"]
    total_spend = total_row["AD Spend"]
    total_row["CAC"] = (total_spend / total_users) if total_users > 0 else 0
    result = pd.concat([result, pd.DataFrame([total_row])], ignore_index=True)
    return result


# =============================================================================
# TAB 12: MTD REPORT
# =============================================================================

def get_cpa_mtd_dates():
    """Get sorted dates from CPA_By_Entity_MTD"""
    df = _get_df("cpa_by_entity_mtd")
    if df.empty or "Date" not in df.columns:
        return []
    dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if dates.empty:
        return []
    return sorted(dates.dt.date.unique().tolist(), reverse=True)


def get_cpa_mtd_entity_names():
    df = _get_df("cpa_by_entity_mtd")
    if df.empty or "Entity_Name" not in df.columns:
        return []
    return sorted(df["Entity_Name"].dropna().unique().tolist())


def get_cpa_by_entity_mtd(selected_date):
    """Tab 12 Table 1: CPA By Entity (MTD)"""
    df = _get_df("cpa_by_entity_mtd")
    if df.empty:
        return pd.DataFrame()
    df = _ensure_date_col(df.copy())
    filtered = df[df["Date"] == pd.Timestamp(selected_date)].copy()
    if filtered.empty:
        return pd.DataFrame()

    result = filtered[["Entity_Name", "MTD_Total_Users", "MTD_New_Regular_Users",
                        "MTD_Subscriptions", "MTD_SS_Users", "MTD_Spend", "MTD_CAC"]].copy()
    result = result.rename(columns={
        "Entity_Name": "Entity",
        "MTD_Total_Users": "Total",
        "MTD_New_Regular_Users": "Trials",
        "MTD_Subscriptions": "New Subscriptions",
        "MTD_SS_Users": "Single Sale",
        "MTD_Spend": "AD Spend",
        "MTD_CAC": "CAC",
    })

    # Total row
    total_row = {
        "Entity": "Total",
        "Total": result["Total"].sum(),
        "Trials": result["Trials"].sum(),
        "New Subscriptions": result["New Subscriptions"].sum(),
        "Single Sale": result["Single Sale"].sum(),
        "AD Spend": result["AD Spend"].sum(),
        "CAC": None,
    }
    total_users = total_row["Total"]
    total_spend = total_row["AD Spend"]
    total_row["CAC"] = (total_spend / total_users) if total_users > 0 else 0
    result = pd.concat([result, pd.DataFrame([total_row])], ignore_index=True)
    return result


def get_cpa_by_application_mtd(selected_date, entity_names, app_names):
    """Tab 12 Table 2: CPA By Application (MTD)"""
    df = _get_df("cpa")
    if df.empty:
        return pd.DataFrame()
    df["Report_Date"] = pd.to_datetime(df["Report_Date"], errors="coerce")
    mask = (
        (df["Report_Date"] == pd.Timestamp(selected_date)) &
        (df["Entity_Name"].isin(entity_names)) &
        (df["App_Name"].isin(app_names))
    )
    filtered = df.loc[mask].copy()
    if filtered.empty:
        return pd.DataFrame()

    result = filtered[["Entity_Name", "App_Name", "Channel_Name",
                        "Total_MTD", "Trial_Users_MTD", "New_Subs_Users_MTD",
                        "Single_Sale_Users_MTD", "Ad_Spend_MTD", "CAC_MTD"]].copy()
    result = result.rename(columns={
        "Entity_Name": "Entity",
        "App_Name": "App",
        "Channel_Name": "Source System",
        "Total_MTD": "Total",
        "Trial_Users_MTD": "Trials",
        "New_Subs_Users_MTD": "New Subscriptions",
        "Single_Sale_Users_MTD": "Single Sale",
        "Ad_Spend_MTD": "AD Spend",
        "CAC_MTD": "CAC",
    })

    # Total row
    total_row = {
        "Entity": "Total", "App": "", "Source System": "",
        "Total": result["Total"].sum(),
        "Trials": result["Trials"].sum(),
        "New Subscriptions": result["New Subscriptions"].sum(),
        "Single Sale": result["Single Sale"].sum(),
        "AD Spend": result["AD Spend"].sum(),
        "CAC": None,
    }
    total_users = total_row["Total"]
    total_spend = total_row["AD Spend"]
    total_row["CAC"] = (total_spend / total_users) if total_users > 0 else 0
    result = pd.concat([result, pd.DataFrame([total_row])], ignore_index=True)
    return result


# =============================================================================
# TAB 13: APPROVAL RATES
# =============================================================================

def get_approval_date_range():
    df = _get_df("app_level_metrics")
    if df.empty or "Report_Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Report_Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_approval_app_names():
    df = _get_df("app_level_metrics")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_approval_channel_names():
    df = _get_df("app_channel_metrics")
    if df.empty or "Channel_Name" not in df.columns:
        return []
    return sorted(df["Channel_Name"].dropna().unique().tolist())


def get_approval_afids():
    df = _get_df("app_channel_afid_metrics")
    if df.empty or "AFID" not in df.columns:
        return []
    return sorted(df["AFID"].dropna().unique().tolist())


def get_app_approval_rates(app_names, start_date, end_date):
    """Tab 13 Chart 1: App-level approval rates.
    Returns dict with 'per_app' and 'total' DataFrames.
    per_app: Report_Date, App_Name, CIT_Percent, MIT_Percent
    total: Report_Date, CIT_Percent, MIT_Percent (aggregated)
    """
    df = _get_df("app_level_metrics")
    if df.empty:
        return {}
    df["Report_Date"] = pd.to_datetime(df["Report_Date"], errors="coerce")
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Report_Date"] >= pd.Timestamp(start_date)) &
        (df["Report_Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    per_app = filtered[["Report_Date", "App_Name", "CIT_Percent", "MIT_Percent"]].copy()
    per_app = per_app.sort_values(["App_Name", "Report_Date"])

    # Total: SUM(CIT_Approved)/SUM(CIT_Total) per date
    totals = filtered.groupby("Report_Date", as_index=False).agg(
        CIT_Approved=("CIT_Approved", "sum"),
        CIT_Total=("CIT_Total", "sum"),
        MIT_Approved=("MIT_Approved", "sum"),
        MIT_Total=("MIT_Total", "sum"),
    )
    totals["CIT_Percent"] = np.where(totals["CIT_Total"] > 0,
                                      totals["CIT_Approved"] / totals["CIT_Total"], np.nan)
    totals["MIT_Percent"] = np.where(totals["MIT_Total"] > 0,
                                      totals["MIT_Approved"] / totals["MIT_Total"], np.nan)
    totals = totals[["Report_Date", "CIT_Percent", "MIT_Percent"]].sort_values("Report_Date")

    return {"per_app": per_app, "total": totals}


def get_channel_approval_rates(app_names, channel_names, start_date, end_date):
    """Tab 13 Chart 2: Channel-level approval rates."""
    df = _get_df("app_channel_metrics")
    if df.empty:
        return {}
    df["Report_Date"] = pd.to_datetime(df["Report_Date"], errors="coerce")
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Channel_Name"].isin(channel_names)) &
        (df["Report_Date"] >= pd.Timestamp(start_date)) &
        (df["Report_Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    # Per channel: aggregate across selected apps
    per_channel = filtered.groupby(["Report_Date", "Channel_Name"], as_index=False).agg(
        CIT_Approved=("CIT_Approved", "sum"),
        CIT_Total=("CIT_Total", "sum"),
        MIT_Approved=("MIT_Approved", "sum"),
        MIT_Total=("MIT_Total", "sum"),
    )
    per_channel["CIT_Percent"] = np.where(per_channel["CIT_Total"] > 0,
                                           per_channel["CIT_Approved"] / per_channel["CIT_Total"], np.nan)
    per_channel["MIT_Percent"] = np.where(per_channel["MIT_Total"] > 0,
                                           per_channel["MIT_Approved"] / per_channel["MIT_Total"], np.nan)
    per_channel = per_channel.sort_values(["Channel_Name", "Report_Date"])

    # Total
    totals = filtered.groupby("Report_Date", as_index=False).agg(
        CIT_Approved=("CIT_Approved", "sum"),
        CIT_Total=("CIT_Total", "sum"),
        MIT_Approved=("MIT_Approved", "sum"),
        MIT_Total=("MIT_Total", "sum"),
    )
    totals["CIT_Percent"] = np.where(totals["CIT_Total"] > 0,
                                      totals["CIT_Approved"] / totals["CIT_Total"], np.nan)
    totals["MIT_Percent"] = np.where(totals["MIT_Total"] > 0,
                                      totals["MIT_Approved"] / totals["MIT_Total"], np.nan)
    totals = totals[["Report_Date", "CIT_Percent", "MIT_Percent"]].sort_values("Report_Date")

    return {"per_channel": per_channel, "total": totals}


def get_afid_approval_rates(app_names, afids, start_date, end_date):
    """Tab 13 Chart 3: AFID-level approval rates."""
    df = _get_df("app_channel_afid_metrics")
    if df.empty:
        return {}
    df["Report_Date"] = pd.to_datetime(df["Report_Date"], errors="coerce")
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["AFID"].isin(afids)) &
        (df["Report_Date"] >= pd.Timestamp(start_date)) &
        (df["Report_Date"] <= pd.Timestamp(end_date))
    )
    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    # Per AFID: aggregate across selected apps
    per_afid = filtered.groupby(["Report_Date", "AFID"], as_index=False).agg(
        CIT_Approved=("CIT_Approved", "sum"),
        CIT_Total=("CIT_Total", "sum"),
        MIT_Approved=("MIT_Approved", "sum"),
        MIT_Total=("MIT_Total", "sum"),
    )
    per_afid["CIT_Percent"] = np.where(per_afid["CIT_Total"] > 0,
                                        per_afid["CIT_Approved"] / per_afid["CIT_Total"], np.nan)
    per_afid["MIT_Percent"] = np.where(per_afid["MIT_Total"] > 0,
                                        per_afid["MIT_Approved"] / per_afid["MIT_Total"], np.nan)
    per_afid = per_afid.sort_values(["AFID", "Report_Date"])

    # Total
    totals = filtered.groupby("Report_Date", as_index=False).agg(
        CIT_Approved=("CIT_Approved", "sum"),
        CIT_Total=("CIT_Total", "sum"),
        MIT_Approved=("MIT_Approved", "sum"),
        MIT_Total=("MIT_Total", "sum"),
    )
    totals["CIT_Percent"] = np.where(totals["CIT_Total"] > 0,
                                      totals["CIT_Approved"] / totals["CIT_Total"], np.nan)
    totals["MIT_Percent"] = np.where(totals["MIT_Total"] > 0,
                                      totals["MIT_Approved"] / totals["MIT_Total"], np.nan)
    totals = totals[["Report_Date", "CIT_Percent", "MIT_Percent"]].sort_values("Report_Date")

    return {"per_afid": per_afid, "total": totals}


# =============================================================================
# TABS 14-16: DECLINE REASON %
# =============================================================================

def get_decline_app_date_range():
    df = _get_df("decline_app")
    if df.empty or "Report_Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Report_Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_decline_app_names():
    df = _get_df("decline_app")
    if df.empty or "App_Name" not in df.columns:
        return []
    return sorted(df["App_Name"].dropna().unique().tolist())


def get_decline_channel_names():
    df = _get_df("decline_channel")
    if df.empty or "Channel_Name" not in df.columns:
        return []
    return sorted(df["Channel_Name"].dropna().unique().tolist())


def get_decline_channel_date_range():
    df = _get_df("decline_channel")
    if df.empty or "Report_Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Report_Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def get_decline_afid_list():
    df = _get_df("decline_afid")
    if df.empty or "AFID" not in df.columns:
        return []
    return sorted(df["AFID"].dropna().unique().tolist())


def get_decline_afid_date_range():
    df = _get_df("decline_afid")
    if df.empty or "Report_Date" not in df.columns:
        return None, None
    dates = pd.to_datetime(df["Report_Date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def _get_decline_data(cache_key, app_names, start_date, end_date,
                      channel_names=None, afids=None, threshold=0):
    """Generic decline reason data for Tabs 14/15/16.
    Returns dict with 'cit' and 'mit' DataFrames:
    Each has: Report_Date, Final_Category, pct
    Threshold removes categories on per-date basis.
    """
    df = _get_df(cache_key)
    if df.empty:
        return {}
    df["Report_Date"] = pd.to_datetime(df["Report_Date"], errors="coerce")
    mask = (
        (df["App_Name"].isin(app_names)) &
        (df["Report_Date"] >= pd.Timestamp(start_date)) &
        (df["Report_Date"] <= pd.Timestamp(end_date))
    )
    if channel_names is not None and "Channel_Name" in df.columns:
        mask = mask & (df["Channel_Name"].isin(channel_names))
    if afids is not None and "AFID" in df.columns:
        mask = mask & (df["AFID"].isin(afids))

    filtered = df.loc[mask]
    if filtered.empty:
        return {}

    result = {}
    for prefix, count_col, total_col in [
        ("cit", "CIT_Decline_Count", "CIT_Total_Declines"),
        ("mit", "MIT_Decline_Count", "MIT_Total_Declines"),
    ]:
        agg = filtered.groupby(["Report_Date", "Final_Category"], as_index=False).agg(
            count=(count_col, "sum"),
            total=(total_col, "sum"),
        )
        # Compute per-date total for percentage
        date_totals = agg.groupby("Report_Date", as_index=False)["count"].sum()
        date_totals.rename(columns={"count": "date_total"}, inplace=True)
        agg = agg.merge(date_totals, on="Report_Date")
        agg["pct"] = np.where(agg["date_total"] > 0,
                               agg["count"] / agg["date_total"] * 100, 0)
        # Apply threshold: remove categories per-date where pct < threshold
        if threshold > 0:
            agg = agg[agg["pct"] >= threshold]
        agg = agg[["Report_Date", "Final_Category", "pct"]].sort_values(["Report_Date", "Final_Category"])
        result[prefix] = agg

    return result


def get_decline_app_data(app_names, start_date, end_date, threshold=0):
    """Tab 14: Decline Reason % - App"""
    return _get_decline_data("decline_app", app_names, start_date, end_date,
                             threshold=threshold)


def get_decline_channel_data(app_names, channel_names, start_date, end_date, threshold=0):
    """Tab 15: Decline Reason % - Channel"""
    return _get_decline_data("decline_channel", app_names, start_date, end_date,
                             channel_names=channel_names, threshold=threshold)


def get_decline_afid_data(app_names, channel_names, afids, start_date, end_date, threshold=0):
    """Tab 16: Decline Reason % - AFID"""
    return _get_decline_data("decline_afid", app_names, start_date, end_date,
                             channel_names=channel_names, afids=afids, threshold=threshold)
