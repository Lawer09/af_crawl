"""
RedisClient: Thread-safe Redis client with connection pooling and essential operations.

Configuration sources:
- Environment variables (preferred):
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    REDIS_POOL_MAXSIZE, REDIS_SSL, REDIS_DECODE_RESPONSES,
    REDIS_SOCKET_TIMEOUT, REDIS_CLIENT_NAME
- Optional config dict `REDIS` in config.settings if present.

This class follows Python best practices: type hints, docstrings, and clear error handling.
"""
from __future__ import annotations

import os
import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)


def _load_config_from_sources() -> dict:
    settings_cfg: Optional[dict] = None
    try:
        import setting.settings as settings  # type: ignore
        settings_cfg = getattr(settings, "REDIS", None)
    except Exception:
        settings_cfg = None

    def _env(name: str, default: Any) -> Any:
        val = os.getenv(name)
        return default if val is None or val == "" else val

    host = (settings_cfg or {}).get("host", _env("REDIS_HOST", "localhost"))
    port = int((settings_cfg or {}).get("port", _env("REDIS_PORT", "6379")))
    db = int((settings_cfg or {}).get("db", _env("REDIS_DB", "1")))
    password = (settings_cfg or {}).get("password", _env("REDIS_PASSWORD", "")) or None
    ssl_raw = (settings_cfg or {}).get("ssl", _env("REDIS_SSL", "false"))
    ssl = bool(str(ssl_raw).lower() in ("true", "1", "yes"))
    pool_maxsize = int((settings_cfg or {}).get("pool_maxsize", _env("REDIS_POOL_MAXSIZE", "10")))
    decode_responses = bool(str((settings_cfg or {}).get("decode_responses", _env("REDIS_DECODE_RESPONSES", "true"))).lower() in ("true", "1", "yes"))
    socket_timeout = float((settings_cfg or {}).get("socket_timeout", _env("REDIS_SOCKET_TIMEOUT", "5")))
    client_name = (settings_cfg or {}).get("client_name", _env("REDIS_CLIENT_NAME", "af_crawl"))

    return {
        "host": host,
        "port": port,
        "db": db,
        "password": password,
        "ssl": ssl,
        "pool_maxsize": pool_maxsize,
        "decode_responses": decode_responses,
        "socket_timeout": socket_timeout,
        "client_name": client_name,
    }


