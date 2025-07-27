from __future__ import annotations

import logging
from datetime import datetime, timedelta
from this import d
from typing import Optional, Dict

import requests
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext
import time
from model.cookie import cookie_model
from config.settings import PLAYWRIGHT
from core.proxy import proxy_pool

logger = logging.getLogger(__name__)

AF_LOGIN_URL = "https://hq1.appsflyer.com/auth/login"


class SessionManager:
    """负责根据用户名获取可用 Session，必要时触发 Playwright 登录刷新 Cookie。"""

    def __init__(self):
        # Playwright 仅在首次调用时启动
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        # 缓存用户密码，用于 token 失效时自动刷新
        self._pwd_cache: Dict[str, tuple[str, str | None]] = {}

    # ------------------ public ------------------
    def get_session(
        self,
        username: str,
        password: str,
        *,
        user_agent: Optional[str] = PLAYWRIGHT['user_agent'],
        proxies: Optional[dict] = None,
    ) -> requests.Session:

        # 1. DB 查已有 cookie
        record = cookie_model.get_cookie_by_username(username)
        if record and not self._is_expired(record["expired_at"]):
            logger.info("cookie hit -> %s", username)
            return self._build_requests_session(record["cookies"], user_agent or record.get("user_agent"),username)
        # --- 登录重试 ---
        for attempt in range(2):
            try:
                logger.info("cookie miss, login(page+api) -> %s (try %s)", username, attempt+1)
                cookies, expired_at, ua = self._login_by_playwright(username, password, user_agent, proxies)
                break
            except Exception as e:
                logger.warning("login failed #%s -> %s", attempt+1, e)
                if attempt == 2:
                    raise
                time.sleep(60 * (attempt + 1))

        # 3. 写入 DB
        cookie_model.add_or_update_cookie(
            username=username,
            password=password,
            cookies=cookies,
            expired_at=expired_at,
            user_agent=ua,
        )

        sess = self._build_requests_session(cookies, ua, username)
        if proxies:
            sess.proxies.update(proxies)
        # 缓存密码供刷新
        self._pwd_cache[username] = (password, ua)
        return sess

    # ------------------ inner ------------------
    def _is_expired(self, expired_at: datetime) -> bool:
        return expired_at <= datetime.now()

    def _build_requests_session(self, cookies: list, user_agent: str | None, username: str | None = None) -> requests.Session:
        s = requests.Session()
        for c in cookies:
            s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path"))
        if user_agent:
            s.headers.update({"User-Agent": user_agent})
        if username:
            s.headers.update({"x-username": username, "X-Username": username})

        # 挂载响应钩子检查 token 是否过期
        s.hooks.setdefault('response', []).append(self._check_token)
        return s

    # ------------------ token 检测 ------------------
    def _check_token(self, resp: requests.Response, *args, **kwargs):

        if resp.status_code in (401, 403, 202):
            username = resp.request.headers.get('x-username') or resp.request.headers.get('X-Username')
            if not username:
                logger.warning("no username in request headers")
                return resp

            pwd_tuple = self._pwd_cache.get(username)
            if not pwd_tuple:
                return resp

            password, ua = pwd_tuple
            logger.info("aws_waf_token 失效，尝试自动刷新 -> %s", username)
            try:
                cookies, expired_at, ua_new = self._login_by_playwright(username, password, ua, )
                # 更新 DB
                cookie_model.add_or_update_cookie(username=username, password=password, cookies=cookies,
                                                 expired_at=expired_at, user_agent=ua_new)
                # 更新 session cookie
                resp.request._cookies.clear()
                for c in cookies:
                    resp.request._cookies.set(c['name'], c['value'], domain=c.get('domain'), path=c.get('path'))
            except Exception as e:
                logger.exception("自动刷新失败 -> %s", e)

        return resp

    # ------------------ playwright ------------------
    # 获取 af 界面浏览器session信息
    def _get_bw_session_by_playwright(
        self,
        username: str,
        user_agent: Optional[str],
        proxies: Optional[dict] = None,
        ) -> tuple[requests.Session, BrowserContext, dict, str]:
                # 增加重试间隔
        import time
        import random
        time.sleep(random.randint(3, 10))  # 随机延迟
        
        proxy_url = None
        if proxies:
            proxy_url = proxies.get("http") or proxies.get("https")

        pw = sync_playwright().start()
        launch_kwargs = {
            "headless": PLAYWRIGHT["headless"],
            "slow_mo": PLAYWRIGHT["slow_mo"],
            "timeout": PLAYWRIGHT["timeout"],
        }
        
        if proxy_url:
            launch_kwargs["proxy"] = {"server": proxy_url}
            
        browser = pw.chromium.launch(**launch_kwargs)
        try:
            context_args = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": user_agent,
                "locale": "en-US",
                "timezone_id": "Asia/Singapore",
            }
            ctx = browser.new_context(**context_args)
            page = ctx.new_page()
            
            # 增加页面加载超时和重试
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    page.goto(AF_LOGIN_URL, timeout=PLAYWRIGHT["timeout"], wait_until='networkidle')
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    logger.warning(f"Page load failed, retrying {attempt + 1}/{max_retries}")
                    time.sleep(10 * (attempt + 1))
            
            # 等待页面完全加载
            page.wait_for_load_state('networkidle', timeout=30000)
            
            base_cookies = ctx.cookies()
            
            import config.af_config as cfg, requests

            s = requests.Session()
            for c in base_cookies:
                s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path"))

            ua = user_agent or page.evaluate("() => navigator.userAgent")
            headers = {
                "User-Agent": ua,
                "Referer": "https://hq1.appsflyer.com/auth/login",
                "Origin": "https://hq1.appsflyer.com",
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
            }
            headers["x-username"] = username
            waf_token = next((c["value"] for c in base_cookies if c["name"] == "aws-waf-token"), "")
            if waf_token:
                headers["X-XSRF-TOKEN"] = waf_token

            if proxies:
                s.proxies.update(proxies)
            return s, ctx.cookies(), headers, ua
        finally:
            try:
                browser.close()
            except Exception:
                pass
            pw.stop()

    # 过去的全局浏览器不再使用
    def _login_by_playwright(
        self,
        username: str,
        password: str,
        user_agent: Optional[str],
        proxies: Optional[dict] = None,
    ) -> tuple[list, datetime, str]:

        import config.af_config as cfg

        s, final_cookies, headers,ua = self._get_bw_session_by_playwright(
            username,
            user_agent, 
            proxies)

        payload = {"username": username, "password": password, "keep-user-logged-in": False}
        r = s.post(cfg.LOGIN_API, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        
        for name in ["af_jwt", "auth_tkt"]:
            if name in s.cookies:
                final_cookies.append({
                    "name": name,
                    "value": s.cookies.get(name),
                    "domain": ".appsflyer.com",
                    "path": "/",
                    "httpOnly": True,
                    "secure": True,
                })
        expired_at = datetime.now() + timedelta(minutes=15)
        logger.info("login success(api) -> %s", username)
        return final_cookies, expired_at, ua

    # ------------------ graceful shutdown ------------------
    def close(self):
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()

# 单例
session_manager = SessionManager()