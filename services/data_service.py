from __future__ import annotations
from typing import List, Dict

import logging
import config.af_config as cfg
from services.login_service import get_session
from model.user_app_data import UserAppDataDAO
from model.user import UserDAO, UserProxyDAO
from utils.retry import request_with_retry

logger = logging.getLogger(__name__)

def fetch_user_app_data(username: str, password:str, app_id: str, start_date: str, end_date: str, proxies: dict | None = None, browser_context_args: dict = {}):
    """ 获取某个用户下的某个app的指定日期的数据 """

    session = get_session(username, password, proxies=proxies, browser_context_args=browser_context_args)   

    headers = {
        "Referer": cfg.NEW_TABLE_API_REFERER,
        "Origin": "https://hq1.appsflyer.com",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }

    # 更贴近前端请求形态的头部
    headers.setdefault("Accept-Language", "en-US,en;q=0.9")
    headers.setdefault("X-Requested-With", "XMLHttpRequest")

    # 注入 XSRF Token（来自 aws-waf-token），如果存在
    waf_token = session.cookies.get("aws-waf-token")
    if waf_token:
        headers["X-XSRF-TOKEN"] = waf_token

    payload = cfg.NEW_TABLE_API_PARAM.copy()
    payload["dates"] = {"start": start_date, "end": end_date}
    payload["filters"]["app-id"] = [app_id]
    # 保留 cfg 里的 groupings 结构（对象形式），仅在需要时可动态修改

    resp = request_with_retry(session, "POST", cfg.NEW_TABLE_API, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    try:
        data : dict = resp.json()
    except ValueError as e:
        logger.error(f"Failed to parse JSON response for {username}, app_id={app_id}: {e}. Response content: {resp.text[:500]}")
        raise

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
    return rows


def fetch_by_pid(pid: str, app_id: str, start_date: str, end_date: str):
    """ 获取某个用户下的某个pid的指定日期的数据 """

    user = UserDAO.get_user_by_pid(pid)
    if not user:
        logger.error(f"User with pid={pid} not found.")
        return []

    username = user["email"]
    password = user["password"]

    proxy = UserProxyDAO.get_by_pid(pid)

    proxies = {"http": proxy.get("proxy_url"), "https": proxy.get("proxy_url")} if proxy else None
    browser_context_args = {
        "user_agent": proxy.get("ua"),
        "timezone_id": proxy.get("timezone_id"),
    } if proxy else {}

    rows = fetch_user_app_data(
        username, password, app_id, start_date, end_date,
        proxies=proxies, browser_context_args=browser_context_args
    )
    return rows


def fetch_and_save_table_data(user: Dict, app_id: str, start_date: str, end_date: str):
    """ 获取某个用户下的某个app的指定日期的数据 """
    rows = fetch_user_app_data(
        user["email"], user["password"], app_id, start_date, end_date,
        proxies=user.get("proxies"), browser_context_args=user.get("browser_context_args", {})
    )
    UserAppDataDAO.save_data_bulk(rows)
    return rows