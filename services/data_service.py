from __future__ import annotations

import logging
from typing import List, Dict

import config.af_config as cfg
from services.login_service import get_session
from model.user_app_data import UserAppDataDAO
from utils.retry import request_with_retry

logger = logging.getLogger(__name__)


def fetch_and_save_table_data(user: Dict, app_id: str, start_date: str, end_date: str):
    username = user["email"]
    password = user["password"]

    session, _ = get_session(username, password)

    headers = {
        "Referer": cfg.NEW_TABLE_API_REFERER,
        "Origin": "https://hq1.appsflyer.com",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }

    payload = cfg.NEW_TABLE_API_PARAM.copy()
    payload["dates"] = {"start": start_date, "end": end_date}
    payload["filters"]["app-id"] = [app_id]
    payload["groupings"] = ["adset", "filter_data"]

    resp = request_with_retry(session, "POST", cfg.NEW_TABLE_API, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows: List[Dict] = []
    for adset in data.get("data", []):
        adset_value = adset.get("adset")
        if not adset_value or adset_value == "None" or not adset_value.isdigit():
            continue
        # 计算天数
        from datetime import datetime as _dt
        fmt = "%Y-%m-%d"
        days_cnt = (
            (_dt.strptime(end_date, fmt) - _dt.strptime(start_date, fmt)).days + 1
        )

        rows.append({
            "username": username,
            "app_id": app_id,
            "offer_id": adset_value,
            "af_clicks": adset.get("filtersGranularityMetricIdClicksPeriod", 0),
            "af_installs": adset.get("attributionSourceAppsflyerFiltersGranularityMetricIdInstallsUaPeriod", 0),
            "start_date": start_date,
            "end_date": end_date,
            "days": days_cnt,
        })

    UserAppDataDAO.save_data_bulk(rows)
    return rows 