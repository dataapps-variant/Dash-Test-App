"""
Traffic Channel ID → Label mapping
Shared across all dashboards
"""

TRAFFIC_CHANNEL_MAP = {
    1: "01 - Google (Display)",
    2: "02 - Google (Search)",
    3: "03 - Quora",
    5: "05 - Google (YouTube)",
    6: "06 - Google (Performance Max)",
    7: "07 - Google (Demand Gen)",
    8: "08 - Taboola",
    9: "09 - Facebook",
    10: "10 - TikTok",
    11: "11 - LinkedIn (Mails)",
    12: "12 - Google (AI Max)",
    13: "13 - LinkedIn (Updates)",
    20: "20 - Microsoft (Search)",
    21: "21 - Microsoft (Display)",
    22: "22 - Microsoft (Performance Max)",
    90: "90 - SEO",
    91: "91 - Email",
    99: "99 - Organic",
}


def get_channel_label(channel_id):
    """Get display label for a traffic channel ID"""
    return TRAFFIC_CHANNEL_MAP.get(int(channel_id), f"{channel_id} - Unknown")


def get_all_channel_options():
    """Get sorted list of {label, value} for checkbox/dropdown"""
    return [
        {"label": label, "value": str(cid)}
        for cid, label in sorted(TRAFFIC_CHANNEL_MAP.items())
    ]
