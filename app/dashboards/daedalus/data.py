"""
Data layer for Daedalus Dashboard

Tables (first 5 tabs):
1. Daedalus.Daedalus                - Tab 1 (Daedalus) + Tab 2 (Pacing by Entity)
2. Daedalus.CAC_By_Entity           - Tab 3 (CAC by Entity) + Tab 5 (Historical)
3. Daedalus.Active_Subscriptions    - Tab 4 (Current Subscriptions)
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
        0
    )
    daily["Pending_Subscriptions_Pct"] = np.where(
        daily["Current_Active_Subscription"] > 0,
        (daily["Current_Pending_Subscriptions"] / daily["Current_Active_Subscription"] * 100).round(2),
        0
    )
    daily["SS_Orders_Pct"] = np.where(
        daily["T30_Day_New_Subscriptions"] > 0,
        (daily["T30_Day_New_SS_Orders"] / daily["T30_Day_New_Subscriptions"] * 100).round(2),
        0
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
    grouped["value"] = np.where(grouped["den"] > 0, grouped["num"] / grouped["den"], 0)
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
    grouped["value"] = np.where(grouped["den"] > 0, grouped["num"] / grouped["den"], 0)
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
