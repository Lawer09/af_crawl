from __future__ import annotations

from typing import Tuple

import logging
from requests import Session

from core.session import session_manager
from core.proxy import proxy_pool, ProxyPool
from config.settings import USE_PROXY

logger = logging.getLogger(__name__)


def get_session(username: str, password: str) -> Tuple[Session, dict | None]:
    """返回 (session, proxies)；proxies 可直接传给 requests"""
    # 1. 拿到代理（绑定用户名）
    proxy_url = proxy_pool.get_proxy(username) if USE_PROXY else ""
    proxies = ProxyPool.build_requests_proxy(proxy_url)

    # 2. 拿到 requests.Session（已包含 Cookie & UA）
    sess = session_manager.get_session(username, password, proxies=proxies)

    # 3. 如果设置了代理，则同时设置到 session 里，方便后续复用
    if proxies:
        sess.proxies.update(proxies)

    return sess, proxies 