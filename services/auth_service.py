from __future__ import annotations

import logging
import requests

logger = logging.getLogger(__name__)

WHOAMI_URL = "https://hq1.appsflyer.com/cdp/whoami"

def whoami_check(session: requests.Session, headers: dict, timeout: int = 30) -> tuple[dict | None, bool]:
    """调用 whoami 接口并判断是否需要 2FA。

    返回 (whoami_json, need_otp)：
    - whoami_json: 解析后的 JSON 字典或 None（HTML/非 JSON）
    - need_otp: 是否需要进入 2FA 验证流程
    """
    w_headers = {**(headers or {}), "Accept": "application/json, text/plain, */*"}
    try:
        resp = session.get(WHOAMI_URL, headers=w_headers, timeout=timeout)
        text = resp.text or ""
        logger.info("whoami status=%s", resp.status_code)
    except Exception as e:
        logger.warning("whoami request failed: %s", e)
        return None, True

    # HTML 表示尚未登录，需要 2FA
    if "DOCTYPE html" in text or "<html" in text.lower():
        return None, True

    # 尝试解析 JSON；若缺少关键字段（email），视为未登录，需要 2FA
    try:
        j = resp.json()
        if not isinstance(j, dict) or not j.get("email"):
            return j if isinstance(j, dict) else None, True
        return j, False
    except Exception:
        return None, True

