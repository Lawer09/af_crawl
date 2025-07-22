from __future__ import annotations

import logging
from typing import List, Dict

from services.login_service import get_session
from model.user_app import UserAppDAO
import config.af_config as cfg
from utils.retry import request_with_retry

logger = logging.getLogger(__name__)


def fetch_and_save_apps(user: Dict[str, str]) -> List[Dict]:
    """获取用户 app 列表并写入数据库，返回列表"""
    username = user["email"]
    password = user["password"]
    account_type = user["account_type"]

    session, _ = get_session(username, password)

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

    # 保存
    UserAppDAO.save_apps(apps)
    return apps 