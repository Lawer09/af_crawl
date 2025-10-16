from __future__ import annotations

from ast import Dict
from typing import Tuple

import logging
from requests import Session

from core.session import session_manager
from core.proxy import proxy_pool, ProxyPool
from config.settings import USE_PROXY
from model.user import UserDAO, UserProxyDAO

logger = logging.getLogger(__name__)

def get_session(username: str, password: str, proxies: dict | None = None, browser_context_args: dict = {}) -> Session:
    """返回 session 可直接传给 requests"""
    # 1. 拿到代理（绑定用户名）
    if proxies is None and USE_PROXY:
        proxy_url = proxy_pool.get_proxy(username)
        proxies = ProxyPool.build_requests_proxy(proxy_url)

    # 2. 拿到 requests.Session（已包含 Cookie & UA）
    sess = session_manager.get_session(username, password, browser_context_args=browser_context_args, proxies=proxies)
    # 3. 通用头与 XSRF 注入
    try:
        # 与前端一致的常用头，若上层未指定则提供默认值
        sess.headers.setdefault("Accept-Language", "en-US,en;q=0.9")
        sess.headers.setdefault("X-Requested-With", "XMLHttpRequest")

        # 从会话 Cookie 注入 WAF/XSRF 令牌，避免各处重复解析
        waf_token = None
        try:
            waf_token = sess.cookies.get("aws-waf-token")
        except Exception:
            waf_token = None
        if waf_token:
            sess.headers["X-XSRF-TOKEN"] = waf_token
    except Exception:
        logger.debug("prepare session headers failed", exc_info=True)

    return sess


def get_session_by_pid(pid: str) -> Session:
    """通过 pid 获取用户与代理信息，生成带 Cookie/UA/代理 的 requests.Session。

    - 自动查 `UserDAO.get_user_by_pid(pid)` 获取用户名与密码
    - 自动查 `UserProxyDAO.get_by_pid(pid)` 生成 `proxies` 与 `browser_context_args`
    """
    user = UserDAO.get_user_by_pid(pid)
    if not user:
        raise ValueError(f"User with pid={pid} not found.")

    proxy_rec = UserProxyDAO.get_by_pid(pid)
    proxies = None
    browser_context_args = {}
    if proxy_rec:
        if proxy_rec.get("proxy_url"):
            proxies = {"http": proxy_rec["proxy_url"], "https": proxy_rec["proxy_url"]}
        if proxy_rec.get("ua"):
            browser_context_args["user_agent"] = proxy_rec["ua"]
        if proxy_rec.get("timezone_id"):
            browser_context_args["timezone_id"] = proxy_rec["timezone_id"]

    return get_session(user["email"], user["password"], proxies=proxies, browser_context_args=browser_context_args)


def get_cookie_by_pid(pid: str) -> Dict:
    """通过 pid 获取用户会话 Cookie（AWS-WAF-TOKEN & X-XSRF-TOKEN）"""
    sess = get_session_by_pid(pid)
    return sess.cookies.get_dict()