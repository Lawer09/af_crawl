from typing import List, Dict
import logging
from datetime import datetime, timedelta
import config.af_config as cfg
from services.login_service import get_session, get_session_by_pid
from model.user_app_data import UserAppDataDAO
from model.overall_report_count import OverallReportCountDAO
from model.offer import OfferDAO
from model.aff import AffDAO
from model.user import UserProxyDAO
from utils.retry import request_with_retry

logger = logging.getLogger(__name__)

def parse_app_data(data:List[Dict]) -> List[Dict]:
    rows: List[Dict] = []
    for adset in data:
        adset_value = adset.get("adset")
        if not adset_value or adset_value == "None" or not adset_value.isdigit():
            continue
        
        rows.append({
            "offer_id": adset_value,
            "af_clicks": adset.get("filtersGranularityMetricIdClicksPeriod", 0),
            "af_installs": adset.get("attributionSourceAppsflyerFiltersGranularityMetricIdInstallsUaPeriod", 0),
        })
    
    return rows


def fetch_pid_app_data(pid: str, app_id: str, start_date: str, end_date: str, aff_id: str | None = None):
    """基于 pid 获取某个 app 在指定日期范围的数据，使用按 pid 获取的会话简化流程。"""

    try:
        session = get_session_by_pid(pid)
    except Exception as e:
        logger.error(f"Failed to init session for pid={pid}: {e}")
        return []

    headers = {
        "Referer": cfg.NEW_TABLE_API_REFERER,
        "Origin": "https://hq1.appsflyer.com",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }

    payload = cfg.NEW_TABLE_API_PARAM.copy()
    payload["dates"] = {"start": start_date, "end": end_date}
    payload["filters"]["app-id"] = [app_id]
    if aff_id:
        payload["filters"]["adgroup-id"] = [aff_id]

    try:
        resp = request_with_retry(session, "POST", cfg.NEW_TABLE_API, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("fetch_pid_app_data request failed for pid=%s -> %s; skip", pid, e)
        return []

    try:
        data: dict = resp.json()
    except ValueError as e:
        ct = resp.headers.get("Content-Type")
        logger.error(
            "Failed to parse JSON response for pid=%s app_id=%s from %s: %s. status=%s, content-type=%s, body=%s",
            pid,
            app_id,
            cfg.NEW_TABLE_API,
            e,
            resp.status_code,
            ct,
            (resp.text or "")[:500],
        )
        return []

    rows = parse_app_data(data.get("data", [])) or []
    # 计算天数
    fmt = "%Y-%m-%d"
    days_cnt = (
        (datetime.strptime(end_date, fmt) - datetime.strptime(start_date, fmt)).days + 1
    )
    for row in rows:
        row["username"] = ""
        row["app_id"] = app_id
        row["aff_id"] = aff_id
        row["start_date"] = start_date
        row["end_date"] = end_date
        row["days"] = days_cnt

    return rows


def fetch_by_pid(pid: str, app_id: str, start_date: str | None = None, end_date: str | None = None, aff_id: str | None = None):
    """ 获取某个用户下的某个pid的指定日期的数据 """

    if start_date is None:
        start_date = datetime.now().strftime("%Y-%m-%d")
    
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    rows = fetch_pid_app_data(
        pid, app_id, start_date, end_date, aff_id,
    )
    return rows


def try_get_and_save_data(pid: str, app_id: str, start_date: str, end_date: str, aff_id: str | None = None):
    """按日期动态缓存策略：

    - 今天：最近 2 小时缓存命中则返回；否则实时查询并落库。
    - 昨天：最近 4 小时缓存命中则返回；否则实时查询并落库。
    - 前天及更早：优先返回该日期的最新缓存；若没有则实时查询并落库。
    - 非单日查询：保持原先 2 小时缓存策略。
    """
    fmt = "%Y-%m-%d"
    within_minutes = 120  # 默认 2 小时
    days_diff = None
    try:
        # 仅针对单日查询做差值判断
        if start_date == end_date:
            target_date = datetime.strptime(start_date, fmt).date()
            today = datetime.now().date()
            days_diff = (today - target_date).days
    except Exception:
        # 日期解析失败时，走默认缓存与实时查询逻辑
        pass

    if days_diff is not None and days_diff >= 2:
        cached_prev = UserAppDataDAO.get_rows_by_date(pid, app_id, start_date, end_date, aff_id)
        if cached_prev:
            return cached_prev
        rows = fetch_by_pid(pid, app_id, start_date, end_date, aff_id)
        for row in rows:
            row['pid'] = pid
        UserAppDataDAO.save_data_bulk(rows)
        return rows

    # 昨天：扩大缓存窗口到 4 小时
    if days_diff == 1:
        within_minutes = 240

    # 1. 先查 DB 缓存（包含 aff_id 过滤）
    cached = UserAppDataDAO.get_recent_rows(pid, app_id, start_date, end_date, aff_id, within_minutes)
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
        gap = round(( (clicks - af_clicks) / clicks), 4) if clicks > 0 else 0.0

        enriched.append({
            **row,
            "clicks": clicks,
            "installation": installation,
            "gap": gap,
        })

    return enriched


def update_daily_data():
    """
    每天更新一次数据：遍历所有启用的 pid 用户，按目前系统启动的offer中的app列表更新昨天数据。
    """
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    user_proxies = UserProxyDAO.get_enable()
    if not user_proxies:
        logger.error("No enable user proxy found for daily data update.")
        return

    pids = [p.get("pid") for p in user_proxies if p.get("pid")]

    pid_offer_map = OfferDAO.get_list_by_pids_group_pid(pids)
    offer_id_aff_map = AffDAO.get_list_by_offer_ids_group([o["id"] for o in pid_offer_map.values()])
    total_apps = 0
    total_success = 0
    for pid in pids:
        # 获取该用户的应用列表
        logger.info(f"Daily update for pid={pid}")
        offers = pid_offer_map.get(pid)
        if not offers:
            logger.info(f"Daily update for pid={pid} with no offers.")
            continue

        pid_total_apps = 0
        pid_success = 0
        for offer in offers:
            offer_id = offer["id"]
            app_id = offer["app_id"]
            logger.info(f"Daily update for pid={pid}, offer_id={offer_id}, app_id={app_id}")
            total_apps += 1
            pid_total_apps += 1
            affs = offer_id_aff_map.get(str(offer_id), [])
            for aff in affs:
                aff_id = aff.get("aff_id")
                need_proxy = aff.get("need_proxy")
                if not aff_id or not need_proxy:
                    logger.info(f"Daily update for pid={pid}, app_id={app_id}, offer_id={offer_id}, aff_id={aff_id} with no need_proxy.")
                    continue

                try:
                    logger.info(f"Start Daily update for pid={pid}, app_id={app_id}, offer_id={offer_id}, aff_id={aff_id}")
                    rows = try_get_and_save_data(pid, app_id, target_date, target_date, aff_id=aff_id)
                    if rows:
                        total_success += 1
                        pid_success += 1
                    logger.info(f"End Daily update success for pid={pid}, count={len(rows)}")
                except Exception:
                    logger.exception(f"Daily update failed for pid={pid}, app_id={app_id}")

        # 输出当前pid的处理统计
        logger.info(
            "Daily data update stats: target_date=%s pid=%s apps=%d success=%d",
            target_date,
            pid,
            pid_total_apps,
            pid_success,
        )

    logger.info("Daily data update finished: target_date=%s, apps=%d, success=%d", target_date, total_apps, total_success)


