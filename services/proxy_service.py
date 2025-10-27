from __future__ import annotations

import logging
import time
from typing import List, Dict, Optional

import requests
from urllib.parse import urlparse, urlunparse

from model.user import UserProxyDAO
import config.af_config as cfg

logger = logging.getLogger(__name__)


def _mask_proxy_for_log(proxy_url: str) -> str:
    """避免在日志中泄露代理密码，保留用户名并掩码密码。"""
    try:
        p = urlparse(proxy_url)
        netloc = p.netloc
        if '@' in netloc:
            creds, hostport = netloc.split('@', 1)
            if ':' in creds:
                user, _pwd = creds.split(':', 1)
                creds = f"{user}:***"
            else:
                creds = f"{creds}:***"
            netloc = f"{creds}@{hostport}"
            return urlunparse((p.scheme, netloc, p.path or '', p.params or '', p.query or '', p.fragment or ''))
        return proxy_url
    except Exception:
        return proxy_url


def _sanitize_proxy_url(raw: str) -> tuple[str, bool, str]:
    """清洗代理URL：移除首尾空白/包裹符，默认补 http://，并做基本校验。
    返回 (cleaned_url, is_valid, reason_if_invalid)。"""
    s = (raw or '').strip()
    # 去掉常见包裹符（反引号/引号）
    s = s.strip('`"\'')
    if not s:
        return '', False, 'empty proxy_url'
    if '://' not in s:
        s = f"http://{s}"
    try:
        p = urlparse(s)
        if p.scheme.lower() not in ('http', 'https', 'socks5', 'socks5h'):
            return s, False, f"invalid scheme: {p.scheme}"
        if not p.netloc:
            return s, False, 'missing netloc(host:port)'
        # 基本格式通过
        return s, True, ''
    except Exception as e:
        return s, False, f"parse error: {type(e).__name__}"


def _sanitize_user_agent(ua: Optional[str]) -> Optional[str]:
    """清洗 UA：移除不可打印字符并折叠空格，避免 requests 拒绝非法头。"""
    if not ua:
        return ua
    try:
        cleaned = ''.join(ch for ch in str(ua) if 32 <= ord(ch) <= 126)
        cleaned = ' '.join(cleaned.split())
        return cleaned.strip() or None
    except Exception:
        return (str(ua).strip() or None)


def _test_once(proxy_url: str, ua: Optional[str], test_url: str, timeout: int = 8) -> tuple[bool, float, int, Optional[str], Optional[str]]:
    """使用指定代理请求 test_url 一次，返回 (是否成功, 延迟ms, 状态码, 错误类型, 错误信息)。

    成功判定：HTTP 状态码 < 400（包含 2xx/3xx），视为网络连通稳定。
    """
    start = time.perf_counter()
    status = 0
    try:
        headers = {}
        ua_clean = _sanitize_user_agent(ua)
        if ua_clean:
            headers["User-Agent"] = ua_clean
        elif ua:
            logger.debug("UA sanitized to empty; skip header. raw=%r", ua)
        sess = requests.Session()
        sess.trust_env = False  # 禁用环境代理干扰
        resp = sess.get(
            test_url,
            headers=headers,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=timeout,
            allow_redirects=True,
        )
        status = resp.status_code
        ok = status < 400
        elapsed_ms = (time.perf_counter() - start) * 1000
        return ok, elapsed_ms, status, None, None
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        err_type = type(e).__name__
        err_msg = str(e)
        logger.debug("_test_once exception: %s - %s", err_type, err_msg)
        return False, elapsed_ms, status, err_type, err_msg


def validate_user_proxies_stability(
    users:list[dict],
    attempts: int = 5,
    test_url: str = "https://ipinfo.io",
    timeout: int = 8,
    sleep_between_attempts: float = 0.2,
) -> List[Dict]:
    """
    验证 UserProxyDAO 中启用代理的网络连通稳定性，打印每个代理的成功率。

    - 成功率按 HTTP 状态码 < 400 计入成功。

    返回：每个代理的统计结果列表（pid、proxy_url、attempts、success、success_rate、avg_latency_ms、statuses、errors）。
    """

    results: List[Dict] = []

    for rec in users:
        pid = rec.get("pid") or ""
        raw_proxy_url = rec.get("proxy_url") or ""
        ua = rec.get("ua") or None
        if not raw_proxy_url:
            logger.warning("Skip empty proxy_url: pid=%s id=%s", pid, rec.get("id"))
            results.append({
                "pid": pid,
                "proxy_url": raw_proxy_url,
                "attempts": 0,
                "success": 0,
                "success_rate": 0.0,
                "avg_latency_ms": None,
                "statuses": [],
                "errors": ["empty proxy_url"],
            })
            continue

        proxy_url, valid, reason = _sanitize_proxy_url(raw_proxy_url)
        masked = _mask_proxy_for_log(proxy_url)
        if not valid:
            logger.warning("Invalid proxy_url for pid=%s: %s (reason=%s)", pid, masked, reason)
            # 仍尝试测试一次，便于观察是否能用
        successes = 0
        latencies: List[float] = []
        statuses: List[int] = []
        errors: List[str] = []

        for i in range(attempts):
            ok, elapsed_ms, status, err_type, err_msg = _test_once(proxy_url, ua, test_url, timeout)
            if ok:
                successes += 1
            latencies.append(elapsed_ms)
            statuses.append(status)
            if not ok and err_type:
                # 截断错误信息，避免过长
                msg = err_msg if len(err_msg) <= 200 else (err_msg[:200] + '...')
                errors.append(f"{err_type}: {msg}")
            if sleep_between_attempts > 0:
                time.sleep(sleep_between_attempts)

        rate = round(successes / attempts, 2) if attempts > 0 else 0.0
        avg_latency = round(sum(latencies) / len(latencies), 1) if latencies else None

        logger.info(
            "Proxy stability: pid=%s url=%s attempts=%d success=%d rate=%.2f avg_latency=%.1fms statuses=%s errors=%d",
            pid,
            masked,
            attempts,
            successes,
            rate,
            (avg_latency if avg_latency is not None else -1),
            statuses,
            len(errors),
        )

        results.append({
            "pid": pid,
            "proxy_url": proxy_url,
            "attempts": attempts,
            "success": successes,
            "success_rate": rate,
            "avg_latency_ms": avg_latency,
            "statuses": statuses,
            "errors": errors,
        })

    return results



def test_all_proxy_stability(
    attempts: int = 5,
    test_url: str = "https://ipinfo.io",
    timeout: int = 8,
) -> List[Dict]:
    """
    验证所有启用代理的网络连通稳定性，打印每个代理的成功率。
    """
    records = UserProxyDAO.get_enable() or []
    if not records:
        logger.warning("No enabled proxies found in UserProxyDAO.")
        return []

    rets = validate_user_proxies_stability(users=records, attempts=attempts, test_url=test_url, timeout=timeout)
    return [{
        "pid": r.get("pid"),
        "proxy_url": r.get("proxy_url"),
        "attempts": r.get("attempts"),
        "success_rate": r.get("success_rate"),
        "avg_latency_ms": r.get("avg_latency_ms")
    } for r in rets if r.get("proxy_url")]



def validate_proxy_stability_for_pid(
    pid: str,
    attempts: int = 5,
    test_url: str = "https://ipinfo.io",
    timeout: int = 8,
) -> Dict:
    """
    验证指定 pid 的代理稳定性（便于单独排查）。
    """
    rec = UserProxyDAO.get_by_pid(pid)
    if not rec or not rec.get("proxy_url"):
        logger.warning("No proxy found for pid=%s", pid)
        return {"pid": pid, "proxy_url": None, "attempts": 0, "success": 0, "success_rate": 0.0, "avg_latency_ms": None, "statuses": [], "errors": ["no proxy"]}

    res = validate_user_proxies_stability(users=[rec], attempts=attempts, test_url=test_url, timeout=timeout)
    # 过滤出当前 pid 的结果
    for r in res:
        if r.get("pid") == pid:
            return r
    return {"pid": pid, "proxy_url": rec.get("proxy_url"), "attempts": 0, "success": 0, "success_rate": 0.0, "avg_latency_ms": None, "statuses": [], "errors": ["not tested"]}
