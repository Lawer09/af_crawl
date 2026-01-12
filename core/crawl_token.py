
import time
from typing import Optional

from playwright.sync_api import sync_playwright

from setting.settings import PLAYWRIGHT


def _sanitize_user_agent(ua: Optional[str]) -> Optional[str]:
    if not ua:
        return ua
    try:
        cleaned = ''.join(ch for ch in str(ua) if 32 <= ord(ch) <= 126)
        cleaned = ' '.join(cleaned.split())
        return cleaned.strip()
    except Exception:
        return str(ua).strip()


def get_aws_waf_token(
        goto_url: str,
        browser_context_args: Optional[dict] = {},
        proxies: Optional[dict] = None,
    ) -> Optional[str]:
        """
        仅用于通过 Playwright 打开登录页并提取 aws-waf-token
        """
        
        # 1. 代理参数解析
        proxy_url = None
        proxy_auth = None
        if proxies:
            proxy_url = proxies.get("http") or proxies.get("https")
            try:
                from urllib.parse import urlsplit
                if proxy_url:
                    if ":" in proxy_url and proxy_url.count(":") == 3 and "@" not in proxy_url:
                        host, port, user, pwd = proxy_url.split(":", 3)
                        server = f"http://{host}:{port}"
                        proxy_auth = {"server": server, "username": user, "password": pwd}
                    else:
                        if not proxy_url.startswith("http://") and not proxy_url.startswith("https://"):
                            server = f"http://{proxy_url}"
                        else:
                            server = proxy_url
                        parsed = urlsplit(server)
                        if parsed.username or parsed.password:
                            clean_server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
                            proxy_auth = {
                                "server": clean_server,
                                "username": parsed.username or "",
                                "password": parsed.password or "",
                            }
                        else:
                            proxy_auth = {"server": server}
            except Exception:
                raise ValueError(f"Proxy URL parse error: {proxy_url}")

        # 2. 启动 Playwright
        pw = sync_playwright().start()
        
        launch_kwargs = {
            "headless": PLAYWRIGHT["headless"],
            "slow_mo": PLAYWRIGHT["slow_mo"],
            "timeout": PLAYWRIGHT["timeout"],
            "args": [
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
            ],
        }
        
        if proxy_auth:
            launch_kwargs["proxy"] = proxy_auth
            
        browser = pw.chromium.launch(**launch_kwargs)
        
        waf_token = None
        
        try:
            # 3. 配置 Context 与 注入脚本 (防止指纹泄露)
            if not isinstance(browser_context_args, dict):
                browser_context_args = {"user_agent": str(browser_context_args)}
            else:
                browser_context_args = browser_context_args or {}
                
            ua_raw = browser_context_args.get("user_agent", PLAYWRIGHT["user_agent"]) or PLAYWRIGHT["user_agent"]
            ua_safe = _sanitize_user_agent(ua_raw)
            
            context_args = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": ua_safe or PLAYWRIGHT["user_agent"],
                "locale": "en-US",
                "timezone_id": browser_context_args.get("timezone_id", PLAYWRIGHT["timezone_id"]),
            }
            ctx = browser.new_context(**context_args)
            
            # 禁用 WebRTC
            ctx.add_init_script(
                """
                (() => {
                  const block = () => { throw new Error('WebRTC disabled'); };
                  const keys = ['RTCPeerConnection','webkitRTCPeerConnection','mozRTCPeerConnection'];
                  for (const k of keys) {
                    if (window[k]) {
                      try { Object.defineProperty(window, k, { get: () => block }); } catch (e) { window[k] = block; }
                    }
                  }
                  if (navigator.mediaDevices) {
                    const md = navigator.mediaDevices;
                    ['getUserMedia','getDisplayMedia','enumerateDevices'].forEach(fn => {
                      if (typeof md[fn] === 'function') { md[fn] = async () => { throw new Error('WebRTC disabled'); }; }
                    });
                  }
                })();
                """
            )
            page = ctx.new_page()

            # 4. 访问页面获取 Cookies
            # 增加页面加载超时和重试
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # 访问页，通常 WAF 脚本会在此页面加载时注入 Cookie
                    page.goto(goto_url, timeout=PLAYWRIGHT["timeout"], wait_until='networkidle')
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(5 * (attempt + 1))
            
            # 额外等待以确保 WAF 计算完成
            try:
                page.wait_for_load_state('networkidle', timeout=30000)
            except Exception:
                pass # 即使超时也尝试获取一下cookie

            # 5. 提取 aws-waf-token
            base_cookies = ctx.cookies()
            
            # 查找特定的 key
            waf_token = next((c["value"] for c in base_cookies if c["name"] == "aws-waf-token"), None)

            return waf_token

        except Exception as e:
            return None
            
        finally:
            try:
                browser.close()
            except Exception:
                pass
            pw.stop()