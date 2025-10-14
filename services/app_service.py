from __future__ import annotations

import logging
from typing import List, Dict

from services.login_service import get_session
from model.user_app import UserAppDAO
from model.user import UserDAO, UserProxyDAO

import config.af_config as cfg
from utils.retry import request_with_retry

logger = logging.getLogger(__name__)


def fetch_apps(user: Dict[str, str]) -> List[Dict]:
    """获取用户 app 列表"""
    username = user["email"]
    password = user["password"]
    account_type = user["account_type"]

    session = get_session(username, password)

    if account_type == "pid":
        url = cfg.HOME_APP_URL_PID
    else:
        url = cfg.HOME_APP_URL_PRT

    headers = {"Referer": "https://hq1.appsflyer.com/apps/myapps"}

    resp = request_with_retry(session, "GET", url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

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


def fetch_app_by_pid(pid: str) -> List[Dict]:
    """获取某个pid下的app列表并写入数据库，返回列表"""
    user = UserDAO.get_user_by_pid(pid)
    if not user:
        logger.error(f"User with pid={pid} not found.")
        return []
    # 先查数据库中最近1天的数据，若存在直接返回
    recent_apps = UserAppDAO.get_recent_user_apps(user["email"], within_days=1)
    if recent_apps:
        return recent_apps

    # 无缓存则实时查询并更新
    apps = fetch_apps(user)
    UserAppDAO.save_apps(apps)
    return apps


def update_daily_apps():
    """更新pid的app"""
    user_proxies = UserProxyDAO.get_enable()
    if not user_proxies:
        logger.error(f"No enable user proxy found.")
        return []
    
    # 1) 批量获取所有 pid 对应的用户，避免循环内频繁 DB 查询
    pids = [p.get("pid") for p in user_proxies if p.get("pid")]
    pid_user_map = UserDAO.get_users_by_pids(pids)

    # 2) 准备一次性查询最近一天已更新过的用户名集合，减少逐用户检查
    users = [u for u in pid_user_map.values() if u]
    usernames = [u["email"] for u in users]
    recent_usernames = UserAppDAO.get_recent_usernames(usernames, within_days=1)

    # 3) 仅为未在最近一天更新过的用户抓取 app 列表
    apps: List[Dict] = []
    for user in users:
        if user["email"] in recent_usernames:
            continue
        apps.extend(fetch_apps(user))

    # 4) 批量保存，减少 DB 操作
    UserAppDAO.save_apps(apps)

