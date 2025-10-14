from __future__ import annotations

"""统一封装对 AppsFlyer 接口的请求，处理 202/429/403 限流，带随机人类化延时与指数退避。"""

import logging
import random
import time
from typing import Any, Dict, Optional
from config.settings import CRAWLER
import requests
from model.cookie import cookie_model

logger = logging.getLogger(__name__)

# 需要重试的 HTTP 状态码
_FAST_RETRY_STATUS = {202}
_NORMAL_RETRY_STATUS = {429, 403}


def _backoff(base: int, attempt: int) -> int:
    """基础分钟 + 每次递增 随机抖动"""
    return base + attempt * random.randint(1, 3)


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    max_retry: int = CRAWLER["max_retry"],
    base_delay: int = CRAWLER["retry_delay_seconds"],
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

    retry_set = retry_status or (_FAST_RETRY_STATUS | _NORMAL_RETRY_STATUS)
 
    for attempt in range(max_retry+1):
        # ------- 确保带上 X-Username -------
        base_headers = session.headers.copy()
        req_headers = kwargs.pop("headers", {}) or {}
        if "x-username" in base_headers:
            if "x-username" not in req_headers and "X-Username" not in req_headers:
                # propagate both forms
                req_headers["x-username"] = base_headers["x-username"]
                req_headers["X-Username"] = base_headers["x-username"]

        # ------- 注入浏览器态通用头部 -------
        # Accept-Language、X-Requested-With 保持更贴近前端请求形态
        req_headers.setdefault("Accept-Language", "en-US,en;q=0.9")
        req_headers.setdefault("X-Requested-With", "XMLHttpRequest")

        # ------- 注入 XSRF Token（来自 aws-waf-token），避免因多条同名 Cookie 报错 -------
        waf_token = None
        try:
            for _c in session.cookies:
                if getattr(_c, "name", None) == "aws-waf-token":
                    waf_token = getattr(_c, "value", None)
                    break
        except Exception as _e:
            logger.debug("read waf token failed: %s", _e)
        if waf_token and "X-XSRF-TOKEN" not in req_headers:
            req_headers["X-XSRF-TOKEN"] = waf_token
        kwargs["headers"] = req_headers

        # 首次尝试时打印当前出口 IP（使用同一 session 与代理）
        if attempt == 0:
            try:
                ip_resp = session.get("https://api.ipify.org?format=json", timeout=6)
                if ip_resp.ok:
                    ip = ip_resp.json().get("ip")
                    logger.info("outbound ip=%s ua=%s", ip, session.headers.get("User-Agent"))
            except Exception as _e:
                logger.debug("ip check failed: %s", _e)

        resp = session.request(method, url, **kwargs)
        if resp.status_code not in retry_set:
            return resp  # 成功（或其它错误交由上层处理）

        # 需要重试
        # 在进入下一次重试前，尝试用 DB 最新 Cookie 覆盖 session.cookies
        try:
            username = (
                req_headers.get("x-username")
                or req_headers.get("X-Username")
                or base_headers.get("x-username")
                or base_headers.get("X-Username")
            )
            if username:
                record = cookie_model.get_cookie_by_username(username)
                if record:
                    # 清空并写入最新 Cookie
                    session.cookies.clear()
                    for c in record.get("cookies", []):
                        session.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"), path=c.get("path"))
                    # 同步特殊 Cookie（若基础 cookies 中未包含），确保 XSRF/JWT 等最新，避免重复
                    names_present = {c.get("name") for c in record.get("cookies", [])}
                    if "aws-waf-token" not in names_present and record.get("aws_waf_token"):
                        session.cookies.set("aws-waf-token", record["aws_waf_token"], domain=".appsflyer.com", path="/")
                    if "af_jwt" not in names_present and record.get("af_jwt"):
                        session.cookies.set("af_jwt", record["af_jwt"], domain=".appsflyer.com", path="/")
                    if "auth_tkt" not in names_present and record.get("auth_tkt"):
                        session.cookies.set("auth_tkt", record["auth_tkt"], domain=".appsflyer.com", path="/")
                    # 更新下一次重试的 XSRF 头（安全读取）
                    waf_token = None
                    for _c in session.cookies:
                        if getattr(_c, "name", None) == "aws-waf-token":
                            waf_token = getattr(_c, "value", None)
                            break
                    if waf_token:
                        req_headers["X-XSRF-TOKEN"] = waf_token
                        kwargs["headers"] = req_headers
        except Exception as e:
            logger.debug("refresh cookies from DB failed: %s", e)

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

    raise RuntimeError(f"请求 {url} 连续 {max_retry+1} 次触发限流/排队")