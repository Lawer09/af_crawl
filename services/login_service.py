from __future__ import annotations

from typing import Tuple

import logging
from requests import Session

from core.session import session_manager
from core.proxy import proxy_pool, ProxyPool
from config.settings import USE_PROXY

logger = logging.getLogger(__name__)

def get_session(username: str, password: str, proxies: dict | None = None, browser_context_args: dict = {}) -> Session:
    """返回 session 可直接传给 requests"""
    # 1. 拿到代理（绑定用户名）
    if proxies is None:
        proxy_url = proxy_pool.get_proxy(username) if USE_PROXY else None
        proxies = ProxyPool.build_requests_proxy(proxy_url)

    # 2. 拿到 requests.Session（已包含 Cookie & UA）
    sess = session_manager.get_session(username, password, browser_context_args=browser_context_args, proxies=proxies)

    return sess