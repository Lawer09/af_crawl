import time
from typing import List, Dict
import logging
import random
from datetime import datetime, timedelta
import config.af_config as cfg
from services.login_service import get_session, get_session_by_pid
from model.user_app_data import UserAppDataDAO
from model.af_data import AfAppDataDAO
from model.overall_report_count import OverallReportCountDAO
from model.offer import OfferDAO
from model.aff import AffDAO
from model.user import UserProxyDAO
from utils.retry import request_with_retry
from core.db import mysql_pool

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
        af_rows = []
        for row in rows:
            row['pid'] = pid
            if int(row['days']) == 1:
                af_row = row.copy()
                af_row['date'] = start_date
                af_rows.append(af_row)
        UserAppDataDAO.save_data_bulk(rows)
        # 只插入af数据中开始日期和结束日期相同的数据
        AfAppDataDAO.upsert_bulk_safe(af_rows)
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

    af_rows = []
    for row in rows:
        row['pid'] = pid
        if int(row['days']) == 1:
            af_row = row.copy()
            af_row['date'] = start_date
            af_rows.append(af_row)

    # 3. 落库
    UserAppDataDAO.save_data_bulk(rows)
    # 只插入af数据中开始日期和结束日期相同的数据
    
    AfAppDataDAO.upsert_bulk_safe(af_rows)
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

    # 根据 app_id 数量自增排序 pids
    try:
        pid_app_counts = {
            pid: len({str(o.get("app_id")) for o in (pid_offer_map.get(pid) or []) if o.get("app_id")})
            for pid in pids
        }
        pids = sorted(pids, key=lambda x: pid_app_counts.get(x, 0))
        logger.info("Daily update pid order (asc by app_id count): %s", [(pid, pid_app_counts.get(pid, 0)) for pid in pids])
    except Exception:
        logger.exception("Sort pids by app_id count failed; keep original order.")

    total_apps = 0
    total_success = 0
    for pid in pids:
        # 获取该用户的应用列表
        offers = pid_offer_map.get(pid)
        if not offers:
            logger.info(f"Daily update for pid={pid} with no offers.")
            continue
        
        pid_total_apps = 0
        pid_success = 0

        app_aff_map = get_app_aff_map_from_offers(offers)
        app_count = len(app_aff_map.keys())
        app_index = 0
        for app_id, aff_ids in app_aff_map.items():
            app_index += 1
            logger.info(f"{app_index}/{app_count} Daily update pid=%s app_id=%s", pid, app_id)
            for aff_id in aff_ids:
                time.sleep(random.uniform(3.5, 6.5))
                try:
                    logger.info(f"Start Daily update for pid={pid}, app_id={app_id}, aff_id={aff_id}")
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


def sync_all_user_app_data_latest_to_af_data() -> int:
    """同步 af_user_app_data 中所有 days=1 的最新记录到 af_data。
    - 分组键：(pid, app_id, offer_id, aff_id, end_date)
    - 每组选择 created_at 最大的一条作为“最新数据”
    - 只处理 days=1（单日），写入 af_data 的 `date`=end_date
    - 使用 AfAppDataDAO.upsert_bulk_safe 做覆盖更新（仅更新不删除）
    返回：成功更新/插入的总记录条数
    """
    # 选取每个 (pid, app_id, offer_id, aff_id, end_date) 的最新记录（days=1）
    sql = """
    SELECT t.pid, t.app_id, t.offer_id, t.aff_id, t.af_clicks, t.af_installs, t.end_date AS `date`, t.created_at
    FROM af_user_app_data AS t
    JOIN (
        SELECT pid, app_id, offer_id, aff_id, end_date, MAX(created_at) AS max_created
        FROM af_user_app_data
        WHERE days = 1
        GROUP BY pid, app_id, offer_id, aff_id, end_date
    ) AS m
    ON t.pid = m.pid AND t.app_id = m.app_id AND t.offer_id = m.offer_id AND (t.aff_id <=> m.aff_id) AND t.end_date = m.end_date AND t.created_at = m.max_created
    WHERE t.days = 1 AND t.aff_id IS NOT NULL AND t.aff_id <> ''
    """
    try:
        rows = mysql_pool.select(sql)
    except Exception as e:
        logger.exception("sync_all_user_app_data_latest_to_af_data: read latest cache failed: %s", e)
        return 0

    if not rows:
        return 0

    # 组装写入 af_data 的行
    to_write: List[Dict] = []
    for r in rows:
        try:
            to_write.append({
                "offer_id": r.get("offer_id"),
                "aff_id": r.get("aff_id"),
                "af_clicks": int(r.get("af_clicks", 0) or 0),
                "af_installs": int(r.get("af_installs", 0) or 0),
                "app_id": r.get("app_id"),
                "pid": r.get("pid"),
                "date": str(r.get("date")),
            })
        except Exception:
            # 单条异常不阻断整个批次
            logger.warning("sync_all_user_app_data_latest_to_af_data: skip invalid row=%s", r)
            continue

    if not to_write:
        return 0

    # 分批 upsert，避免 SQL 构造过长
    CHUNK = 500
    total = 0
    for i in range(0, len(to_write), CHUNK):
        chunk = to_write[i:i+CHUNK]
        try:
            total += AfAppDataDAO.upsert_bulk_safe(chunk, lock_timeout=5)
        except Exception:
            logger.exception("sync_all_user_app_data_latest_to_af_data: upsert chunk failed size=%d", len(chunk))
            continue

    logger.info("sync_all_user_app_data_latest_to_af_data: done, rows=%d updated=%d", len(to_write), total)
    return total


def sync_user_app_data_to_af_data(pid: str, app_id: str, start_date: str, end_date: str, aff_id: str | None = None) -> int:
    """将 UserAppDataDAO 中最新缓存的数据按指定日期范围同步到 AfAppDataDAO。
    - 逐日处理：对每个日期(day)读取缓存表最新一条记录（created_at DESC），按 (offer_id, aff_id) 取最新。
    - 仅写入单日数据：要求 day 的缓存记录 days=1（start=end=day）。
    - 当 aff_id 未指定时，将对该日的所有渠道分别写入（按每个 (offer_id, aff_id) 最新一条）。
    - 不依赖唯一索引：调用 AfAppDataDAO.upsert_bulk_safe 使用 GET_LOCK+DELETE+INSERT 保证并发安全。
    返回：成功写入/更新的记录条数。
    """
    try:
        fmt = "%Y-%m-%d"
        start = datetime.strptime(start_date, fmt).date()
        end = datetime.strptime(end_date, fmt).date()
        if start > end:
            start, end = end, start
    except Exception:
        logger.exception("sync_user_app_data_to_af_data: invalid date range start=%s end=%s", start_date, end_date)
        return 0

    to_write: List[Dict] = []
    cur = start
    while cur <= end:
        day_str = cur.strftime("%Y-%m-%d")
        try:
            rows = UserAppDataDAO.get_rows_by_date(pid, app_id, day_str, day_str, aff_id)
        except Exception as e:
            logger.exception("sync_user_app_data_to_af_data: read cache failed pid=%s app_id=%s date=%s err=%s", pid, app_id, day_str, e)
            rows = []
        if not rows:
            cur = cur + timedelta(days=1)
            continue
        # 选择每个 (offer_id, aff_id) 的最新一条记录（get_rows_by_date 已按 created_at DESC）
        latest_map: Dict[tuple, Dict] = {}
        for r in rows:
            off = r.get("offer_id")
            aff = r.get("aff_id")
            if not aff:
                # 缺少渠道无法在 af_data 形成键，跳过
                continue
            key = (off, aff)
            if key not in latest_map:
                latest_map[key] = r
        # 仅写入单日（days=1）的记录
        for (_, _), r in latest_map.items():
            if int(r.get("days", 0) or 0) != 1:
                continue
            to_write.append({
                "offer_id": r.get("offer_id"),
                "aff_id": r.get("aff_id"),
                "af_clicks": int(r.get("af_clicks", 0) or 0),
                "af_installs": int(r.get("af_installs", 0) or 0),
                "app_id": r.get("app_id"),
                "pid": pid,
                "date": day_str,
            })
        cur = cur + timedelta(days=1)

    if not to_write:
        return 0

    try:
        # 使用安全 upsert（不依赖唯一索引）
        affected = AfAppDataDAO.upsert_bulk_safe(to_write, lock_timeout=5)
        return affected
    except Exception as e:
        logger.exception("sync_user_app_data_to_af_data: upsert failed pid=%s app_id=%s range=%s..%s err=%s", pid, app_id, start_date, end_date, e)
        return 0



def get_app_aff_map_from_offers(offers: List[Dict]) -> Dict[str, List[str]]:
    """
    传入 offer 列表，返回 app_id 下对应的所有 aff_id（1对多）。
    - 输入 offers 每项示例：{"id": <offer_id>, "app_id": <app_id>, ...}
    - 输出字典：{app_id(str): [aff_id(str), ...]}，去重并按字符串排序。
    """
    if not offers:
        return {}

    # 先按 app_id 聚合该 app 下的 offer_id 集合
    app_offer_ids: Dict[str, set] = {}
    for off in offers:
        app_id = off.get("app_id")
        offer_id = off.get("id")
        if not app_id or offer_id is None:
            continue
        try:
            app_offer_ids.setdefault(str(app_id), set()).add(int(offer_id))
        except Exception:
            # 非法的 offer_id 跳过
            continue

    if not app_offer_ids:
        return {}

    # 一次性查询所有 offer_id 对应的 aff 列表
    all_offer_ids = [oid for oids in app_offer_ids.values() for oid in oids]
    try:
        offer_id_aff_map = AffDAO.get_list_by_offer_ids_group(all_offer_ids)
    except Exception as e:
        logger.exception("get_app_aff_map_from_offers: query aff by offers failed: %s", e)
        offer_id_aff_map = {}

    # 构建 {app_id: [aff_id,...]} 映射（字符串、去重、排序）
    result: Dict[str, List[str]] = {}
    for app_id, offer_ids in app_offer_ids.items():
        aff_ids_set: set = set()
        for oid in offer_ids:
            affs = offer_id_aff_map.get(int(oid), []) if isinstance(offer_id_aff_map, dict) else []
            for aff in affs:
                aff_id = aff.get("aff_id")
                if aff_id:
                    aff_ids_set.add(str(aff_id))
        result[app_id] = sorted(aff_ids_set) if aff_ids_set else []

    return result


