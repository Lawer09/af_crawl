from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional

import requests

from setting.settings import PROXY, USE_PROXY

logger = logging.getLogger(__name__)

# ================================
# IPWEB 固定常量（若后续更换服务，可在 .env 覆盖）
# ================================
SERVER = "gate2.ipweb.cc"
PORT = 7778
IP_WEB_API = "http://api.ipweb.cc:8004/api/agent/account2"


class ProxyPool:
    """线程安全的内存代理池，支持来源绑定。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._pool: List[Dict] = []  # {proxy:str, expire_time:timestamp}
        self._source_map: Dict[str, str] = {}  # source -> proxy

    # -------------------- 外部接口 --------------------
    def get_proxy(self, source: str, force_new: bool = False, default: str = "") -> str:
        """为某个 source（如 username）拿到一个可用代理"""
        if not USE_PROXY:
            return default

        with self._lock:
            # 首先检查池子，必要时补充
            self._cleanup_and_refill()

            if force_new or source not in self._source_map:
                if not self._pool:
                    logger.warning("proxy pool empty, return default -> %s", default)
                    return default
                proxy_item = self._pool.pop(0)
                self._source_map[source] = proxy_item["proxy"]

            return self._source_map[source]

    def new_proxy(self) -> Optional[str]:
        proxies = self._fetch_from_ipweb(limit=1)
        return proxies[0]["proxy"] if proxies else None

    # -------------------- 内部方法 --------------------
    def _cleanup_and_refill(self):
        now = time.time()
        self._pool = [p for p in self._pool if p["expire_time"] > now]

        # 如果池子空或数量不足，则补充
        if len(self._pool) < PROXY["default_count"] // 2:
            try:
                add = self._fetch_from_ipweb(limit=PROXY["default_count"])
                self._pool.extend(add)
            except Exception as e:
                logger.error("fetch proxy failed: %s", e)

    @staticmethod
    def _fetch_from_ipweb(country: str | None = None, times: int | None = None, limit: int = 1):
        headers = {"Token": PROXY["ipweb_token"]}
        params = {
            "country": country or PROXY["default_country"],
            "times": times or PROXY["default_times"],
            "limit": limit,
        }
        try:
            resp = requests.get(IP_WEB_API, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 200 or "data" not in data:
                raise RuntimeError(f"ipweb error: {data}")
            proxies_raw = data["data"]
            result = []
            ttl_minutes = params["times"]
            for item in proxies_raw:
                proxy_url = f"http://{item}@{SERVER}:{PORT}"
                result.append({
                    "proxy": proxy_url,
                    "expire_time": time.time() + ttl_minutes * 60,
                })
            return result
        except Exception as exc:
            logger.exception("_fetch_from_ipweb exception: %s", exc)
            raise

    # -------------------- 工具方法 --------------------
    @staticmethod
    def build_requests_proxy(proxy_url: str | None):
        if not proxy_url:
            return None
        return {"http": proxy_url, "https": proxy_url}


# 单例
proxy_pool = ProxyPool() 