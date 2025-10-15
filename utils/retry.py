from __future__ import annotations

"""统一封装对 AppsFlyer 接口的请求，处理 202/429/403 限流，带随机人类化延时与指数退避。"""

import logging
import random
import time
from typing import Any, Dict, Optional
from config.settings import CRAWLER
import config.af_config as cfg
import requests
from model.cookie import cookie_model
import re

logger = logging.getLogger(__name__)

# 需要重试的 HTTP 状态码
_FAST_RETRY_STATUS = {202}
_NORMAL_RETRY_STATUS = {429, 403}

# WAF 播种节流状态：记录每个用户名最近一次播种时间戳
_SEED_WAF_LAST_TS: dict[str, float] = {}


def _backoff(base: int, attempt: int) -> int:
    """基础分钟 + 每次递增 随机抖动"""
    return base + attempt * random.randint(1, 3)


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
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
        max_retry   最大重试次数
        base_delay  初始延时（秒）
        retry_status 覆盖默认 status 集
        kwargs      其余 requests.request 参数
    """

    retry_set = retry_status or (_FAST_RETRY_STATUS | _NORMAL_RETRY_STATUS)
 
    # -------------------- helpers for WAF token --------------------
    def _extract_waf_token(resp: requests.Response) -> Optional[str]:
        # 优先从响应 cookies 取
        try:
            if resp.cookies and resp.cookies.get("aws-waf-token"):
                return resp.cookies.get("aws-waf-token")
        except Exception:
            pass
        # 回退解析 Set-Cookie 中的 aws-waf-token
        try:
            sc = resp.headers.get("Set-Cookie") or ""
            m = re.search(r"aws-waf-token=([^;\r\n]+)", sc)
            if m:
                return m.group(1).strip() or None
        except Exception:
            pass
        return None

    def _update_session_waf(sess: requests.Session, token: str, headers: Dict[str, str]) -> None:
        try:
            # 统一设置到会话 Cookie，确保域与路径匹配
            sess.cookies.set("aws-waf-token", token, domain=".appsflyer.com", path="/")
            # 覆盖下一次请求的 XSRF 头
            headers["X-XSRF-TOKEN"] = token
        except Exception as e:
            logger.debug("update waf token failed: %s", e)

    def _seed_waf_from_login(sess: requests.Session, headers: Dict[str, str], username: Optional[str]) -> Optional[str]:
        """当 202 响应未带新 token 时，轻量 GET 登录页播种 aws-waf-token（无需账号密码）。"""
        try:
            # 节流：同一用户名在冷却期内不重复播种
            if not CRAWLER.get("seed_waf_on_202", False):
                return None
            if not username:
                return None
            now = time.time()
            cooldown = int(CRAWLER.get("seed_waf_cooldown_seconds", 180))
            last_ts = _SEED_WAF_LAST_TS.get(username, 0)
            if now - last_ts < cooldown:
                logger.debug("skip waf seeding due to cooldown: username=%s remain=%.0fs", username, cooldown - (now - last_ts))
                return None

            # 发起 GET 登录页以获取最新 WAF cookie
            r = sess.get(cfg.LOGIN_API, headers={
                "User-Agent": sess.headers.get("User-Agent"),
                "Referer": cfg.LOGIN_API,
                "Origin": "https://hq1.appsflyer.com",
                "Accept": "application/json, text/plain, */*",
            }, timeout=10)
            waf_new = _extract_waf_token(r)
            if waf_new:
                _update_session_waf(sess, waf_new, headers)
                _SEED_WAF_LAST_TS[username] = now
                logger.info("WAF token seeded from login (len=%s, cooldown=%ss)", len(waf_new), cooldown)
                return waf_new
        except Exception as e:
            logger.debug("seed waf from login failed: %s", e)
        return None

    for attempt in range(max_retry):
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
        # 202（排队）时尝试刷新 aws-waf-token，无需登录
        if resp.status_code == 202:
            waf_new = _extract_waf_token(resp)
            if waf_new:
                _update_session_waf(session, waf_new, req_headers)
                kwargs["headers"] = req_headers
                logger.info("WAF token refreshed from 202 (len=%s)", len(waf_new))
            else:
                # 增强版：响应未携带 token，按需播种登录页以获取最新 token
                try:
                    username = (
                        req_headers.get("x-username")
                        or req_headers.get("X-Username")
                        or base_headers.get("x-username")
                        or base_headers.get("X-Username")
                    )
                    waf_seed = _seed_waf_from_login(session, req_headers, username)
                    if waf_seed:
                        kwargs["headers"] = req_headers
                except Exception as _e:
                    logger.debug("waf seeding dispatch failed: %s", _e)
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

        # 优先使用服务端提供的排队等待时间
        delay_hdr = 0
        try:
            ra = resp.headers.get("Retry-After")
            if ra:
                delay_hdr = int(ra)
        except Exception:
            delay_hdr = 0

        delay = max(delay_hdr, _backoff(base_delay, attempt))
        logger.warning(
            "[%s] %s 返回 %s，第 %s 次重试，延时 %ss (Retry-After=%s)",
            method,
            url,
            resp.status_code,
            attempt + 1,
            delay,
            delay_hdr or "-",
        )
        time.sleep(delay)

    raise RuntimeError(f"请求 {url} 连续 {max_retry+1} 次触发限流/排队")