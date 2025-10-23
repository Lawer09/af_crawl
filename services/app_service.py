from __future__ import annotations

import logging
from typing import List, Dict

from services.login_service import get_session, get_session_by_pid
from model.user_app import UserAppDAO
from model.user import UserDAO, UserProxyDAO

import config.af_config as cfg
from utils.retry import request_with_retry

logger = logging.getLogger(__name__)

def fetch_apps(
    user: Dict[str, str],
    proxies: dict | None = None,
    browser_context_args: dict = {},
) -> List[Dict]:
    """获取用户 app 列表（支持代理与UA）"""
    username = user["email"]
    password = user["password"]
    account_type = user["account_type"]

    # 携带代理与UA，保持与 data_service 一致
    session = get_session(username, password, proxies=proxies, browser_context_args=browser_context_args)

    if account_type == "pid":
        url = cfg.HOME_APP_URL_PID
    else:
        url = cfg.HOME_APP_URL_PRT

    headers = {
        "Referer": "https://hq1.appsflyer.com/apps/myapps",
        "Origin": "https://hq1.appsflyer.com",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }

    try:
        resp = request_with_retry(session, "GET", url, headers=headers, timeout=30)
    except Exception as e:
        # 连续 202（排队/限流）或其他网络层异常：记录并跳过，避免中断批次
        logger.warning("fetch_apps request failed for %s -> %s; skip", username, e)
        return []
    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError as e:
        # 非 JSON 或空响应时，记录细节并返回空列表，避免调度失败
        ct = resp.headers.get("Content-Type")
        logger.error(
            "Failed to parse JSON response for %s (%s) from %s: %s. status=%s, content-type=%s, body=%s",
            username,
            account_type,
            url,
            e,
            resp.status_code,
            ct,
            (resp.text or "")[:500],
        )
        return []

    apps: List[Dict] = []

    if account_type == "pid":
        if "data" not in data:
            logger.error("unexpected response: %s", data)
            return []
        for app in data["data"]:
            apps.append({
                "username": username,
                "app_id": app["app_id"],
                "app_name": app.get("app_name"),
                "platform": app["platform"],
                "timezone": None,
                "user_type_id": None,
            })
    else:
        if "apps" not in data or "user" not in data:
            logger.error("unexpected response: %s", data)
            return []
        prt_id = data["user"].get("agencyId")
        for app in data["apps"]:
            if app.get("deleted"):
                continue
            apps.append({
                "username": username,
                "app_id": app["id"],
                "app_name": app["name"],
                "platform": app["platform"],
                "timezone": app["localization"].get("timezone"),
                "user_type_id": prt_id,
            })
    return apps

def save_apps(apps: List[Dict]):
    """批量保存app列表到数据库"""
    UserAppDAO.save_apps(apps)

def fetch_and_save_apps_by_pid(pid: str) -> List[Dict]:
    """获取某个pid下的app列表并写入数据库，返回列表（带代理与UA）"""
    user = UserDAO.get_user_by_pid(pid)
    if not user:
        logger.error(f"User with pid={pid} not found.")
        return []
    # 先查数据库中最近1天的数据，若存在直接返回
    recent_apps = UserAppDAO.get_recent_user_apps(user["email"], within_days=1)
    if recent_apps:
        return recent_apps

    # 无缓存则实时查询并更新（带静态代理配置）
    proxy_rec = UserProxyDAO.get_by_pid(pid)
    proxies = None
    browser_args = {}
    if proxy_rec:
        if proxy_rec.get("proxy_url"):
            proxies = {"http": proxy_rec["proxy_url"], "https": proxy_rec["proxy_url"]}
        if proxy_rec.get("ua"):
            browser_args["user_agent"] = proxy_rec["ua"]
        if proxy_rec.get("timezone_id"):
            browser_args["timezone_id"] = proxy_rec["timezone_id"]

    apps = fetch_apps(user, proxies=proxies, browser_context_args=browser_args)
    for app in apps:
        app["user_type_id"] = pid

    save_apps(apps)
    return apps


def fetch_pid_apps(pid: str) -> List[Dict]:
    """基于 pid 获取实时的 app 列表，使用按 pid 获取的会话，简化代理与UA流程。"""
    user = UserDAO.get_user_by_pid(pid)
    if not user:
        logger.error(f"User with pid={pid} not found.")
        return []

    try:
        session = get_session_by_pid(pid)
    except Exception as e:
        logger.error("Failed to init session for pid=%s: %s", pid, e)
        return []

    url = cfg.HOME_APP_URL_PID

    headers = {
        "Referer": "https://hq1.appsflyer.com/apps/myapps",
        "Origin": "https://hq1.appsflyer.com",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }

    try:
        resp = request_with_retry(session, "GET", url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("fetch_pid_apps request failed for pid=%s -> %s; skip", pid, e)
        return []

    apps: List[Dict] = []

    username = user["email"]
    if "data" not in data:
        logger.error("unexpected response: %s", data)
        return []

    for app in data["data"]:
        apps.append({
            "username": username,
            "app_id": app["app_id"],
            "app_name": app.get("app_name"),
            "platform": app["platform"],
            "timezone": "UTC",
            "user_type_id": pid,
        })
    return apps

def update_user_apps():
    """更新pid的app（批量，带代理与UA）"""
    user_proxies = UserProxyDAO.get_enable()
    if not user_proxies:
        logger.error(f"No enable user proxy found.")
        return []
    
    # 1) 批量获取所有 pid 对应的用户，避免循环内频繁 DB 查询
    pids = [p.get("pid") for p in user_proxies if p.get("pid")]
    pid_user_map = UserDAO.get_users_by_pids(pids)

    # 建立 pid 到代理记录的映射
    proxy_map = {p.get("pid"): p for p in user_proxies if p.get("pid")}

    # 2) 准备一次性查询最近一天已更新过的用户名集合，减少逐用户检查
    users = [u for u in pid_user_map.values() if u]
    usernames = [u["email"] for u in users]
    recent_usernames = UserAppDAO.get_recent_usernames(usernames, within_days=1)

    # 3) 仅为未在最近一天更新过的用户抓取 app 列表
    all_apps: List[Dict] = []
    # 迭代 (pid, user) 保证 user_type_id 正确设置，并根据 pid 查代理
    for pid, user in pid_user_map.items():
        if not user:
            continue
        if user["email"] in recent_usernames:
            continue
        
        logger.info("Start fetch apps: pid=%s username=%s", pid, user.get("email"))
        proxy_rec = proxy_map.get(pid)
        proxies = None
        browser_args = {}
        if proxy_rec:
            if proxy_rec.get("proxy_url"):
                proxies = {"http": proxy_rec["proxy_url"], "https": proxy_rec["proxy_url"]}
            if proxy_rec.get("ua"):
                browser_args["user_agent"] = proxy_rec["ua"]
            if proxy_rec.get("timezone_id"):
                browser_args["timezone_id"] = proxy_rec["timezone_id"]

        try:
            apps = fetch_apps(user, proxies=proxies, browser_context_args=browser_args)
            for app in apps:
                app["user_type_id"] = pid
            all_apps.extend(apps)
            # 输出当前用户（带pid）获取的app数量
            logger.info("Fetched apps count: pid=%s username=%s count=%d", pid, user.get("email"), len(apps))
            UserAppDAO.save_apps(all_apps)
        except Exception as e:
            # 单用户异常不影响整批次，记录并跳过
            logger.exception("update_daily_apps user failed: pid=%s username=%s -> %s", pid, user.get("email"), e)
            continue
