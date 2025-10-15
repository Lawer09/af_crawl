from __future__ import annotations
from typing import List, Dict

import logging
import config.af_config as cfg
from services.login_service import get_session
from model.user_app_data import UserAppDataDAO
from model.overall_report_count import OverallReportCountDAO
from model.user_app import UserAppDAO
from model.user import UserDAO, UserProxyDAO
from utils.retry import request_with_retry

logger = logging.getLogger(__name__)

def fetch_user_app_data(username: str, password:str, app_id: str, start_date: str, end_date: str, aff_id:str | None = None, proxies: dict | None = None, browser_context_args: dict = {}):
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

    # 注入 XSRF Token（来自 aws-waf-token），安全读取避免重复同名 Cookie 引发错误
    waf_token = None
    try:
        for _c in session.cookies:
            if getattr(_c, "name", None) == "aws-waf-token":
                waf_token = getattr(_c, "value", None)
                break
    except Exception as _e:
        logger.debug("read waf token failed: %s", _e)
    if waf_token:
        headers["X-XSRF-TOKEN"] = waf_token

    payload = cfg.NEW_TABLE_API_PARAM.copy()
    payload["dates"] = {"start": start_date, "end": end_date}
    payload["filters"]["app-id"] = [app_id] # 指定app包
    if aff_id:
        payload["filters"]["adgroup-id"] = [aff_id]  # 将 ad_id 作为渠道id

    # 保留 cfg 里的 groupings 结构（对象形式），仅在需要时可动态修改
    resp = request_with_retry(session, "POST", cfg.NEW_TABLE_API, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    try:
        data : dict = resp.json()
    except ValueError as e:
        # 非 JSON 或空响应时，记录细节并返回空数据，避免任务失败
        ct = resp.headers.get("Content-Type")
        logger.error(
            "Failed to parse JSON response for %s app_id=%s from %s: %s. status=%s, content-type=%s, body=%s",
            username,
            app_id,
            cfg.NEW_TABLE_API,
            e,
            resp.status_code,
            ct,
            (resp.text or "")[:500],
        )
        return []

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
            "aff_id": aff_id,
            "af_clicks": adset.get("filtersGranularityMetricIdClicksPeriod", 0),
            "af_installs": adset.get("attributionSourceAppsflyerFiltersGranularityMetricIdInstallsUaPeriod", 0),
            "start_date": start_date,
            "end_date": end_date,
            "days": days_cnt,
        })
    return rows


def fetch_by_pid(pid: str, app_id: str, start_date: str, end_date: str, aff_id: str | None = None):
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
        username, password, app_id, start_date, end_date, aff_id,
        proxies=proxies, browser_context_args=browser_context_args
    )
    return rows


def try_get_and_save_data(pid: str, app_id: str, start_date: str, end_date: str, aff_id: str | None = None):
    """优先返回最近1小时内的缓存数据；否则查询并落库后返回。"""

    # 1. 先查 DB 缓存（最近 60 分钟）
    cached = UserAppDataDAO.get_recent_rows(pid, app_id, start_date, end_date, within_minutes=60)
    if cached:
        return cached

    # 2. 无缓存则实时查询
    rows = fetch_by_pid(pid, app_id, start_date, end_date, aff_id)
    for row in rows:
        row['pid'] = pid

    # 3. 落库
    UserAppDataDAO.save_data_bulk(rows)
    return rows


def fetch_by_pid_and_offer_id(pid: str, app_id: str, offer_id: str | None = None, start_date: str = None, end_date: str = None, aff_id: str | None = None):
    """ 获取某个pid下的某个app的指定日期的数据 """
    rows = try_get_and_save_data(
        pid, app_id, start_date, end_date, aff_id
    )
    if offer_id:
        rows = list(filter(lambda x: x["offer_id"] == offer_id, rows))
    return rows


def fetch_with_overall_report_counts(pid: str, app_id: str, date: str, aff_id: str | None = None, offer_id: str | None = None,):
    """返回指定 date 的 AF 数据与 overall_report_count 的 clicks/installation 以及 gap(af_clicks/clicks) 百分比。"""
    # AF侧按单日查询
    rows = fetch_by_pid_and_offer_id(pid, app_id, offer_id, date, date, aff_id)

    enriched: List[Dict] = []
    for row in rows:
        # 解析 offer_id/aff_id
        offer_id_str = row.get("offer_id")
        offer_id = int(offer_id_str) if isinstance(offer_id_str, str) and offer_id_str.isdigit() else (
            int(offer_id_str) if isinstance(offer_id_str, (int,)) else None
        )
        aff_val = row.get("aff_id", aff_id)
        aff_int = None
        if isinstance(aff_val, str) and aff_val.isdigit():
            aff_int = int(aff_val)
        elif isinstance(aff_val, int):
            aff_int = aff_val

        clicks_install = {"clicks": 0, "installation": 0}
        if offer_id is not None:
            clicks_install = OverallReportCountDAO.get_counts(pid, offer_id, aff_int, date)

        clicks = int(clicks_install.get("clicks", 0) or 0)
        installation = int(clicks_install.get("installation", 0) or 0)
        af_clicks = int(row.get("af_clicks", 0) or 0)

        # 计算 gap 百分比（保留两位小数）
        gap = round((af_clicks / clicks * 100.0), 2) if clicks > 0 else 0.0

        enriched.append({
            **row,
            "clicks": clicks,
            "installation": installation,
            "gap": gap,
        })

    return enriched


def update_daily_data():
    """每天更新一次数据：遍历所有启用的 pid 用户，按其 app 列表更新昨天数据。
    仅处理 UserAppDAO 中 user_type_id 为 'pid' 的应用（兼容历史为空的情况）。
    """
    from datetime import datetime, timedelta
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    user_proxies = UserProxyDAO.get_enable()
    if not user_proxies:
        logger.error("No enable user proxy found for daily data update.")
        return

    pids = [p.get("pid") for p in user_proxies if p.get("pid")]
    pid_user_map = UserDAO.get_users_by_pids(pids)

    total_apps = 0
    total_success = 0
    for pid in pids:
        user = pid_user_map.get(pid)
        if not user:
            logger.error(f"User with pid={pid} not found for daily update.")
            continue
        username = user["email"]

        # 获取该用户的应用列表
        apps = UserAppDAO.get_user_apps(username)
        if not apps:
            continue

        # 仅选择 user_type_id 为 'pid' 的应用；兼容历史记录为空(None)时也视为 pid 账号下应用
        pid_apps = [a for a in apps if (a.get("user_type_id") == 'pid' or a.get("user_type_id") in (None, ''))]

        pid_total_apps = 0
        pid_success = 0
        for app in pid_apps:
            app_id = app["app_id"]
            total_apps += 1
            pid_total_apps += 1
            try:
                rows = try_get_and_save_data(pid, app_id, target_date, target_date)
                if rows:
                    total_success += 1
                    pid_success += 1
            except Exception:
                logger.exception(f"Daily update failed for pid={pid}, app_id={app_id}")

        # 输出当前pid的处理统计
        logger.info(
            "Daily data update stats: target_date=%s pid=%s username=%s apps=%d success=%d",
            target_date,
            pid,
            username,
            pid_total_apps,
            pid_success,
        )

    logger.info("Daily data update finished: target_date=%s, apps=%d, success=%d", target_date, total_apps, total_success)


