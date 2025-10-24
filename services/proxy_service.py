from __future__ import annotations

import logging
import time
from typing import List, Dict, Optional

import requests

from model.user import UserProxyDAO
import config.af_config as cfg

logger = logging.getLogger(__name__)


def _test_once(proxy_url: str, ua: Optional[str], test_url: str, timeout: int = 8) -> tuple[bool, float, int]:
    """使用指定代理请求 test_url 一次，返回 (是否成功, 延迟ms, 状态码)。

    成功判定：HTTP 状态码 < 400（包含 2xx/3xx），视为网络连通稳定。
    """
    start = time.perf_counter()
    status = 0
    try:
        headers = {}
        if ua:
            headers["User-Agent"] = ua
        resp = requests.get(
            test_url,
            headers=headers,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=timeout,
            allow_redirects=True,
        )
        status = resp.status_code
        ok = status < 400
        elapsed_ms = (time.perf_counter() - start) * 1000
        return ok, elapsed_ms, status
    except Exception:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return False, elapsed_ms, status


def validate_user_proxies_stability(
    attempts: int = 5,
    test_url: str = "https://ipinfo.io",
    timeout: int = 8,
    sleep_between_attempts: float = 0.2,
) -> List[Dict]:
    """
    验证 UserProxyDAO 中启用代理的网络连通稳定性，打印每个代理的成功率。

    - 成功率按 HTTP 状态码 < 400 计入成功。

    返回：每个代理的统计结果列表（pid、proxy_url、attempts、success、success_rate、avg_latency_ms、statuses）。
    """
    records = UserProxyDAO.get_enable() or []
    if not records:
        logger.warning("No enabled proxies found in UserProxyDAO.")
        return []

    results: List[Dict] = []

    for rec in records:
        pid = rec.get("pid") or ""
        proxy_url = rec.get("proxy_url") or ""
        ua = rec.get("ua") or None
        if not proxy_url:
            logger.warning("Skip empty proxy_url: pid=%s id=%s", pid, rec.get("id"))
            results.append({
                "pid": pid,
                "proxy_url": proxy_url,
                "attempts": 0,
                "success": 0,
                "success_rate": 0.0,
                "avg_latency_ms": None,
                "statuses": [],
            })
            continue

        successes = 0
        latencies: List[float] = []
        statuses: List[int] = []

        for i in range(attempts):
            ok, elapsed_ms, status = _test_once(proxy_url, ua, test_url, timeout)
            if ok:
                successes += 1
            latencies.append(elapsed_ms)
            statuses.append(status)
            if sleep_between_attempts > 0:
                time.sleep(sleep_between_attempts)

        rate = round(successes / attempts, 2) if attempts > 0 else 0.0
        avg_latency = round(sum(latencies) / len(latencies), 1) if latencies else None

        logger.info(
            "Proxy stability: pid=%s url=%s attempts=%d success=%d rate=%.2f avg_latency=%.1fms statuses=%s",
            pid,
            proxy_url,
            attempts,
            successes,
            rate,
            (avg_latency if avg_latency is not None else -1),
            statuses,
        )

        results.append({
            "pid": pid,
            "proxy_url": proxy_url,
            "attempts": attempts,
            "success": successes,
            "success_rate": rate,
            "avg_latency_ms": avg_latency,
            "statuses": statuses,
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
    rets = validate_user_proxies_stability(attempts=attempts, test_url=test_url, timeout=timeout)
    logger.info("All proxy stability: %s", rets)
    return [{
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
        return {"pid": pid, "proxy_url": None, "attempts": 0, "success": 0, "success_rate": 0.0, "avg_latency_ms": None, "statuses": []}

    res = validate_user_proxies_stability(attempts=attempts, test_url=test_url, timeout=timeout)
    # 过滤出当前 pid 的结果
    for r in res:
        if r.get("pid") == pid:
            return r
    return {"pid": pid, "proxy_url": rec.get("proxy_url"), "attempts": 0, "success": 0, "success_rate": 0.0, "avg_latency_ms": None, "statuses": []}