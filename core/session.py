from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict
import requests
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext
import time
import threading
from model.cookie import cookie_model
from config.settings import PLAYWRIGHT, SESSION_EXPIRE_MINUTES, CRAWLER, USE_PROXY
from services.otp_service import get_2fa_code_by_username
from services.auth_service import whoami_check

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
        # 缓存用户代理配置，确保命中 cookie 时与自动刷新均沿用相同代理
        self._proxy_cache: Dict[str, Optional[dict]] = {}
        # 单航道控制（按用户名）
        self._sf_lock = threading.RLock()
        self._sf_events: Dict[str, threading.Event] = {}
        self._sf_timeout = int(CRAWLER.get("singleflight_timeout_seconds", 60))

    # ------------------ public ------------------
    def get_session(
        self,
        username: str,
        password: str,
        *,
        browser_context_args: Optional[dict] = {},
        proxies: Optional[dict] = None,
    ) -> requests.Session:

        # 1. DB 查已有 cookie
        record = cookie_model.get_cookie_by_username(username)
        if record and not self._is_expired(record["expired_at"]):
            logger.info("cookie hit -> %s", username)
            # 缓存密码供后续刷新使用
            ua_cfg = browser_context_args.get("user_agent", record.get("user_agent")) or PLAYWRIGHT["user_agent"]
            ua_cfg = self._sanitize_user_agent(ua_cfg)
            self._pwd_cache[username] = (password, ua_cfg)
            sess = self._build_requests_session(
                record["cookies"],
                ua_cfg,
                username,
            )
            # 命中缓存也携带代理
            if proxies:
                sess.proxies.update(proxies)
            self._proxy_cache[username] = proxies
            return sess
        # --- 登录重试（单航道） ---
        cookies = None
        expired_at = None
        ua = self._sanitize_user_agent(browser_context_args.get("user_agent") or PLAYWRIGHT["user_agent"]) 
        max_attempts = 2

        # 单航道：同一用户名只允许一个线程执行浏览器登录
        leader, ev = self._sf_begin(f"login|{username}")
        if leader:
            for attempt in range(max_attempts):
                try:
                    logger.info("cookie miss, login(page+api) -> %s (try %s)", username, attempt + 1)
                    bc_args = dict(browser_context_args or {})
                    bc_args["user_agent"] = ua
                    cookies, expired_at, ua = self._login_by_playwright(username, password, bc_args, proxies)
                    break
                except Exception as e:
                    # 特殊处理 UA 非法字符错误：展示 UA 并直接终止登录尝试
                    if "Invalid characters found in userAgent" in str(e):
                        bad_ua = browser_context_args.get("user_agent") or PLAYWRIGHT["user_agent"]
                        logger.error("Invalid User-Agent detected, abort login -> username=%s ua=%r", username, bad_ua)
                        self._sf_end(f"login|{username}")
                        raise
                    logger.warning("login failed #%s -> %s", attempt + 1, e)
                    if attempt == max_attempts - 1:
                        self._sf_end(f"login|{username}")
                        raise
                    time.sleep(15 * (attempt + 1))
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
            self._pwd_cache[username] = (password, ua)
            self._proxy_cache[username] = proxies
            # 结束单航道
            self._sf_end(f"login|{username}")
            return sess
        else:
            # 跟随者等待登录完成后复用 DB 中的最新 cookie
            ev.wait(self._sf_timeout)
            record2 = cookie_model.get_cookie_by_username(username)
            if record2 and not self._is_expired(record2["expired_at"]):
                ua_cfg = browser_context_args.get("user_agent", record2.get("user_agent")) or PLAYWRIGHT["user_agent"]
                ua_cfg = self._sanitize_user_agent(ua_cfg)
                self._pwd_cache[username] = (password, ua_cfg)
                sess = self._build_requests_session(record2["cookies"], ua_cfg, username)
                if proxies:
                    sess.proxies.update(proxies)
                self._proxy_cache[username] = proxies
                return sess
            # 若等待超时或登录失败未写库，则尝试一次本地登录（避免永久失败）
            for attempt in range(max_attempts):
                try:
                    logger.info("follower fallback login -> %s (try %s)", username, attempt + 1)
                    bc_args = dict(browser_context_args or {})
                    bc_args["user_agent"] = ua
                    cookies, expired_at, ua = self._login_by_playwright(username, password, bc_args, proxies)
                    break
                except Exception as e:
                    if "Invalid characters found in userAgent" in str(e):
                        bad_ua = browser_context_args.get("user_agent") or PLAYWRIGHT["user_agent"]
                        logger.error("Invalid User-Agent detected, abort fallback -> username=%s ua=%r", username, bad_ua)
                        raise
                    logger.warning("fallback login failed #%s -> %s", attempt + 1, e)
                    if attempt == max_attempts - 1:
                        raise
                    time.sleep(15 * (attempt + 1))
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
            self._pwd_cache[username] = (password, ua)
            self._proxy_cache[username] = proxies
            return sess

    # ------------------ inner ------------------
    def _is_expired(self, expired_at: datetime) -> bool:
        return expired_at <= datetime.now()

    def _sanitize_user_agent(self, ua: Optional[str]) -> Optional[str]:
        if not ua:
            return ua
        try:
            cleaned = ''.join(ch for ch in str(ua) if 32 <= ord(ch) <= 126)
            cleaned = ' '.join(cleaned.split())
            return cleaned.strip()
        except Exception:
            return str(ua).strip()

    def _build_requests_session(self, cookies: list, user_agent: str | None, username: str | None = None) -> requests.Session:
        s = requests.Session()
        for c in cookies:
            s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path"))
        if user_agent:
            s.headers.update({"User-Agent": self._sanitize_user_agent(user_agent)})
        if username:
            s.headers.update({"x-username": username, "X-Username": username})

        # 挂载响应钩子检查 token 是否过期
        s.hooks.setdefault('response', []).append(self._check_token)
        return s

    # ------------------ singleflight helpers ------------------
    def _sf_begin(self, key: str) -> tuple[bool, threading.Event]:
        with self._sf_lock:
            ev = self._sf_events.get(key)
            if ev is None or ev.is_set():
                ev = threading.Event()
                ev.clear()
                self._sf_events[key] = ev
                return True, ev
            return False, ev

    def _sf_end(self, key: str) -> None:
        with self._sf_lock:
            ev = self._sf_events.get(key)
            if ev:
                ev.set()
                try:
                    del self._sf_events[key]
                except Exception:
                    pass

    # ------------------ token 检测 ------------------
    def _check_token(self, resp: requests.Response, *args, **kwargs):

        # 仅在认证失败时触发自动刷新；202 为排队，不视为 token 失效
        if resp.status_code in (401, 403):
            username = resp.request.headers.get('x-username') or resp.request.headers.get('X-Username')
            if not username:
                logger.warning("no username in request headers")
                return resp

            pwd_tuple = self._pwd_cache.get(username)
            if not pwd_tuple:
                logger.warning("no password in cache")
                return resp

            password, ua = pwd_tuple

            # 优先使用请求头中的代理信息，其次退回缓存的代理
            proxy_header = resp.request.headers.get('X-Proxy-URL')
            proxies = None
            if proxy_header:
                proxies = {"http": proxy_header, "https": proxy_header}
            else:
                proxies = self._proxy_cache.get(username) or None

            logger.info("认证失败，尝试自动刷新(单航道) -> %s proxy: %s", username, proxies)
            key = f"refresh|{username}"
            leader, ev = self._sf_begin(key)
            if leader:
                try:
                    cookies, expired_at, ua_new = self._login_by_playwright(
                        username,
                        password,
                        {"user_agent": ua} if ua else {},
                        proxies,
                    )
                    # 更新 DB
                    cookie_model.add_or_update_cookie(username=username, password=password, cookies=cookies,
                                                     expired_at=expired_at, user_agent=ua_new)
                    # 更新请求 cookie（准备重试）
                    try:
                        resp.request._cookies.clear()
                        for c in cookies:
                            resp.request._cookies.set(c['name'], c['value'], domain=c.get('domain'), path=c.get('path'))
                    except Exception:
                        pass
                except Exception as e:
                    logger.exception("自动刷新失败 -> %s", e)
                finally:
                    self._sf_end(key)
            else:
                # 跟随者等待刷新完成后，从 DB 读取最新 cookie
                ev.wait(self._sf_timeout)
                try:
                    record = cookie_model.get_cookie_by_username(username)
                    if record and not self._is_expired(record["expired_at"]):
                        resp.request._cookies.clear()
                        for c in record["cookies"]:
                            resp.request._cookies.set(c['name'], c['value'], domain=c.get('domain'), path=c.get('path'))
                except Exception:
                    pass
        return resp

    # ------------------ playwright ------------------
    # 获取 af 界面浏览器session信息
    def _get_bw_session_by_playwright(
        self,
        username: str,
        browser_context_args: Optional[dict] = {},
        proxies: Optional[dict] = None,
        ) -> tuple[requests.Session, BrowserContext, dict, str]:
                # 增加重试间隔
        
        proxy_url = None
        proxy_auth = None
        if proxies:
            proxy_url = proxies.get("http") or proxies.get("https")
            # 解析可能的账号密码形式，支持：
            # 1) http://user:pass@host:port
            # 2) https://user:pass@host:port
            # 3) host:port:user:pass（无协议）
            # 4) host:port（无账号密码）
            try:
                from urllib.parse import urlsplit
                if proxy_url:
                    if ":" in proxy_url and proxy_url.count(":") == 3 and "@" not in proxy_url:
                        # 形如 host:port:user:pass
                        host, port, user, pwd = proxy_url.split(":", 3)
                        server = f"http://{host}:{port}"
                        proxy_auth = {"server": server, "username": user, "password": pwd}
                    else:
                        # 其他情况按 URL 解析，提取 userinfo
                        if not proxy_url.startswith("http://") and not proxy_url.startswith("https://"):
                            server = f"http://{proxy_url}"
                        else:
                            server = proxy_url
                        parsed = urlsplit(server)
                        if parsed.username or parsed.password:
                            # server 需不含凭据
                            clean_server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
                            proxy_auth = {
                                "server": clean_server,
                                "username": parsed.username or "",
                                "password": parsed.password or "",
                            }
                        else:
                            proxy_auth = {"server": server}
            except Exception:
                # 解析失败
                logger.warning(f"Proxy URL parse error: {proxy_url}")

        pw = sync_playwright().start()
        
        launch_kwargs = {
            "headless": PLAYWRIGHT["headless"],
            "slow_mo": PLAYWRIGHT["slow_mo"],
            "timeout": PLAYWRIGHT["timeout"],
            # 禁用非代理 UDP 的 WebRTC，避免绕过代理/泄露本地 IP
            "args": [
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
            ],
        }
        
        if proxy_auth:
            # 使用 Playwright 的原生代理认证能力，避免弹出认证窗口
            launch_kwargs["proxy"] = proxy_auth
            
        browser = pw.chromium.launch(**launch_kwargs)
        try:
            # 规范 browser_context_args 类型（支持传入 UA 字符串）
            if not isinstance(browser_context_args, dict):
                browser_context_args = {"user_agent": str(browser_context_args)}
            else:
                browser_context_args = browser_context_args or {}
            ua_raw = browser_context_args.get("user_agent", PLAYWRIGHT["user_agent"]) or PLAYWRIGHT["user_agent"]
            ua_safe = self._sanitize_user_agent(ua_raw)
            if ua_safe != ua_raw:
                logger.debug("Sanitized UA -> username=%s before=%r after=%r", username, ua_raw, ua_safe)
            context_args = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": ua_safe or PLAYWRIGHT["user_agent"],
                "locale": "en-US",
                "timezone_id": browser_context_args.get("timezone_id", PLAYWRIGHT["timezone_id"]),
            }
            ctx = browser.new_context(**context_args)
            # 在所有页面初始化时禁用 WebRTC 相关 API，防止绕过代理与 IP 泄露
            ctx.add_init_script(
                """
                (() => {
                  const block = () => { throw new Error('WebRTC disabled'); };
                  const keys = ['RTCPeerConnection','webkitRTCPeerConnection','mozRTCPeerConnection'];
                  for (const k of keys) {
                    if (window[k]) {
                      try {
                        Object.defineProperty(window, k, { get: () => block });
                      } catch (e) {
                        window[k] = block;
                      }
                    }
                  }
                  if (navigator.mediaDevices) {
                    const md = navigator.mediaDevices;
                    ['getUserMedia','getDisplayMedia','enumerateDevices'].forEach(fn => {
                      if (typeof md[fn] === 'function') {
                        md[fn] = async () => { throw new Error('WebRTC disabled'); };
                      }
                    });
                  }
                })();
                """
            )
            page = ctx.new_page()
            # 使用浏览器上下文直接 fetch 获取出口 IP 与真实 UA（验证浏览器代理是否生效）
            try:
                ip_val = page.evaluate(
                    """
                    async () => {
                        try {
                            const r = await fetch('https://api.ipify.org?format=json', { cache: 'no-store' });
                            const j = await r.json();
                            return j.ip || null;
                        } catch (e) {
                            return null;
                        }
                    }
                    """
                )
                ua_real = page.evaluate("() => navigator.userAgent")

                if USE_PROXY and ip_val == None:
                    raise ConnectionError("Browser proxy check failed with proxy: %s", proxy_url)

                logger.info("Browser proxy check -> exit_ip=%s real_ua=%s proxies=%s", ip_val, ua_real, proxies)
            except Exception as _e:
                logger.debug("browser proxy check failed: %s", _e)
                raise ConnectionError("Browser proxy check failed")
            
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

            # 使用上下文中的 UA（如未提供则读取页面 UA）
            ua = context_args.get("user_agent") or page.evaluate("() => navigator.userAgent")
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
        browser_context_args: Optional[dict],
        proxies: Optional[dict] = None,
    ) -> tuple[list, datetime, str]:

        import config.af_config as cfg

        s, final_cookies, headers,ua = self._get_bw_session_by_playwright(
            username,
            browser_context_args, 
            proxies)

        payload = {"username": username, "password": password, "keep-user-logged-in": False}
        r = s.post(cfg.LOGIN_API, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        # 登录接口可能返回 200 但 JSON 表示失败（如用户名或密码错误）
        try:
            login_json = r.json()
            if isinstance(login_json, dict):
                # 标准失败返回：{"Message":"Invalid username or password.","NextUrl":"","LoginSuccess":false,"StatusCode":0}
                if login_json.get("LoginSuccess") is False or login_json.get("StatusCode") == 0:
                    msg = login_json.get("Message") or "Invalid username or password."
                    logger.error("Login failed for %s: %s", username, msg)
                    raise ValueError(msg)
        except ValueError:
            # 向上抛出具体的失败信息
            raise
        except Exception:
            # 非 JSON 或解析失败，继续通过 whoami 判断登录状态
            pass

        # 正常登录可能没有任何返回值；通过 whoami 判断是否已登录
        w_headers = {**headers, "Accept": "application/json, text/plain, */*"}
        whoami_json, need_otp = whoami_check(s, w_headers, timeout=30)

        if need_otp:
            logger.info("whoami 未确认登录，进入 2FA 验证阶段 -> %s", username)
            # 通过服务层（支持 email 或 pid）获取 2FA 验证码
            otp_code = get_2fa_code_by_username(username)
            logger.info("Performing 2FA check for %s and code is %s", username, otp_code)

            # 显式设置 Cookie 头，确保包含登录返回的 cookie
            cookie_dict = requests.utils.dict_from_cookiejar(s.cookies)
            s.headers["Cookie"] = "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])
            s.headers.update({
                "Referer": "https://hq1.appsflyer.com/auth/login",
                "Content-Type": "application/json;charset=UTF-8",
            })

            otp_url = "https://hq1.appsflyer.com/auth/check-otp/"
            otp_payload = {"otp-input": str(otp_code)}
            r_otp = s.post(otp_url, json=otp_payload, timeout=30)
            logger.info("2FA response for %s: status=%s, body=%s", username, r_otp.status_code, r_otp.text[:200])
            r_otp.raise_for_status()

            # LoginSuccess 出现在验证码阶段
            try:
                otp_resp = r_otp.json()
            except Exception:
                otp_resp = {}
            if otp_resp.get("LoginSuccess") is not True:
                logger.error("2FA verification failed for %s: %s", username, r_otp.text[:200])
                raise ValueError("2FA 验证失败")
            logger.info("2FA verification successful for %s", username)

            # 再次通过 whoami 确认登录状态
            whoami_json2, need_otp2 = whoami_check(s, w_headers, timeout=30)
            if need_otp2 or not whoami_json2 or not whoami_json2.get("email"):
                logger.error("whoami after OTP 未确认登录，拒绝保存 Cookie -> %s", username)
                raise ValueError("登录未确认，whoami 校验失败")
        
        # 额外的令牌校验：必须至少包含 af_jwt 或 auth_tkt 才视为登录成功
        if ("af_jwt" not in s.cookies) and ("auth_tkt" not in s.cookies):
            logger.error("登录未生成有效令牌（af_jwt/auth_tkt），拒绝保存 -> %s", username)
            raise ValueError("登录未生成有效令牌")

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
        expired_at = datetime.now() + timedelta(minutes=SESSION_EXPIRE_MINUTES)
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
