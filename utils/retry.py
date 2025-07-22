from __future__ import annotations

"""统一封装对 AppsFlyer 接口的请求，处理 202/429/403 限流，带随机人类化延时与指数退避。"""

import logging
import random
import time
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

# 需要重试的 HTTP 状态码
_RETRY_STATUS = {202, 429, 403}


def _backoff(base: int, attempt: int) -> int:
    """基础 5 分钟 + 每次递增 3~6 分钟随机抖动"""
    return base + attempt * random.randint(180, 360)


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    max_retry: int = 5,
    base_delay: int = 300,
    retry_status: set[int] | None = None,
    **kwargs: Any,
) -> requests.Response:
    """发送请求，捕获限流 / 排队状态自动重试。

    参数:
        session     已配置好 Cookie / 代理的 requests.Session
        method      'GET' / 'POST' 等
        url         请求地址
        max_retry   最大重试次数（不含首发）
        base_delay  初始延时（秒）
        retry_status 覆盖默认 status 集
        kwargs      其余 requests.request 参数
    """

    retry_set = retry_status or _RETRY_STATUS

    for attempt in range(max_retry + 1):
        # 人类化随机延时 1~3 秒
        time.sleep(random.uniform(1, 3))

        resp = session.request(method, url, **kwargs)
        if resp.status_code not in retry_set:
            return resp  # 成功（或其它错误交由上层处理）

        # 需要重试
        delay = _backoff(base_delay, attempt)
        logger.warning(
            "[%s] %s 返回 %s，第 %s 次重试，延时 %ss",
            method,
            url,
            resp.status_code,
            attempt + 1,
            delay,
        )
        time.sleep(delay)

    raise RuntimeError(f"请求 {url} 连续 {max_retry + 1} 次触发限流/排队") 