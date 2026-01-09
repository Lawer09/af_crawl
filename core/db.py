from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple, Optional
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection
from config.settings import MYSQL, REPORT_MYSQL
import time
import os
import threading

logger = logging.getLogger(__name__)

_SLOW_SEC = float(os.getenv("MYSQL_SLOW_QUERY_SECONDS", "5"))

class MySQLClient:

    def __init__(self, config: dict):
        try:
            self.pool: MySQLConnectionPool = MySQLConnectionPool(
                pool_name=config["pool_name"],
                pool_size=config["pool_size"],
                **{k: v for k, v in config.items() if k not in {"pool_name", "pool_size"}}
            )
            self._initialized = True
            logger.info(
                "MySQL connection pool created: host=%s db=%s size=%s",
                config["host"], config["database"], config["pool_size"],
            )
        except mysql.connector.Error as e:
            logger.exception("[MySQL] create pool failed: %s", e)
            raise

    def get_conn(self) -> PooledMySQLConnection:
        return self.pool.get_connection()

    def select(self, sql: str, params: Tuple | Dict | None = None) -> List[Dict[str, Any]]:
        conn = self.get_conn()
        try:
            cursor = conn.cursor(dictionary=True)
            t0 = time.perf_counter()
            cursor.execute(sql, params or ())
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - t0
            if elapsed > _SLOW_SEC:
                snippet = (sql[:300] + "...") if len(sql) > 300 else sql
                try:
                    pcount = len(params) if isinstance(params, (list, tuple, dict)) else (1 if params else 0)
                except Exception:
                    pcount = 0
                logger.warning("[MySQL] slow select: %.2fs params=%d sql=%s", elapsed, pcount, snippet)
            return rows
        finally:
            cursor.close()
            conn.close()

    def fetch_one(self, sql: str, params: Tuple | Dict | None = None) -> Optional[Dict[str, Any]]:
        rows = self.select(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Tuple | Dict | None = None) -> int:
        """单条写入 / 更新 / 删除"""
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            t0 = time.perf_counter()
            cursor.execute(sql, params or ())
            affected_rows = cursor.rowcount
            conn.commit()
            elapsed = time.perf_counter() - t0
            if elapsed > _SLOW_SEC:
                snippet = (sql[:300] + "...") if len(sql) > 300 else sql
                try:
                    pcount = len(params) if isinstance(params, (list, tuple, dict)) else (1 if params else 0)
                except Exception:
                    pcount = 0
                logger.warning("[MySQL] slow execute: %.2fs affected=%d params=%d sql=%s", elapsed, affected_rows, pcount, snippet)
            return affected_rows
        except Exception as e:
            conn.rollback()
            logger.exception("[MySQL] execute failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def executemany(self, sql: str, param_list: List[Tuple | Dict]):
        if not param_list:
            return
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            t0 = time.perf_counter()
            cursor.executemany(sql, param_list)
            conn.commit()
            elapsed = time.perf_counter() - t0
            snippet = (sql[:300] + "...") if len(sql) > 300 else sql
            try:
                pcount = len(param_list)
            except Exception:
                pcount = 0
            if elapsed > _SLOW_SEC:
                logger.warning("[MySQL] slow executemany: %.2fs batch=%d sql=%s", elapsed, pcount, snippet)
        except Exception as e:
            conn.rollback()
            logger.exception("[MySQL] executemany failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

class RedisClient:

    def __init__(self, config: Optional[dict] = None) -> None:
        self._lock = threading.RLock()
        self._config = config or _load_config_from_sources()
        self._pool: redis.ConnectionPool = self._create_pool(self._config)
        self._client: redis.Redis = self._create_client(self._pool, self._config)
        logger.info(
            "Redis client initialized: host=%s port=%s db=%s pool=%s ssl=%s",
            self._config["host"], self._config["port"], self._config["db"], self._config["pool_maxsize"], self._config["ssl"]
        )

    @staticmethod
    def _create_pool(cfg: dict) -> redis.ConnectionPool:
        pool_kwargs = {
            "host": cfg["host"],
            "port": cfg["port"],
            "db": cfg["db"],
            "password": cfg["password"],
            "max_connections": cfg["pool_maxsize"],
            "socket_timeout": cfg["socket_timeout"],
        }

        if cfg["ssl"]:
            pool_kwargs["connection_class"] = redis.SSLConnection

        return redis.ConnectionPool(**pool_kwargs)

    @staticmethod
    def _create_client(pool: redis.ConnectionPool, cfg: dict) -> redis.Redis:
        return redis.Redis(
            connection_pool=pool,
            decode_responses=cfg["decode_responses"],
            client_name=cfg.get("client_name"),
        )

    # ------------------ connection handling ------------------
    def ping(self) -> bool:
        """Ping Redis server to test connectivity."""
        try:
            return bool(self._client.ping())
        except RedisError as e:
            logger.error("Redis ping failed: %s", e)
            return False

    def close(self) -> None:
        """Close client connections and disconnect pool.

        Safe to call multiple times; guarded by lock to avoid concurrent pool disconnect.
        """
        with self._lock:
            try:
                try:
                    self._client.close()
                except Exception:
                    # older redis-py may not implement close(); ignore
                    pass
                self._pool.disconnect()
                logger.info("Redis client closed and pool disconnected")
            except Exception as e:  # pragma: no cover
                logger.warning("Redis close encountered error: %s", e)

    # ------------------ key-value operations ------------------
    def set(self, key: str, value: str, *, ex: Optional[int] = None, nx: bool = False, xx: bool = False) -> bool:
        """Set a key to a value.

        Args:
            key: Redis key
            value: String value (use json.dumps for structured data)
            ex: Expire time in seconds; if provided, sets TTL
            nx: Only set if key does not exist
            xx: Only set if key already exists
        Returns:
            True if set succeeded; False otherwise.
        """
        try:
            res = self._client.set(name=key, value=value, ex=ex, nx=nx, xx=xx)
            return bool(res)
        except RedisError as e:
            logger.error("Redis set failed key=%s: %s", key, e)
            return False

    def setex(self, key: str, seconds: int, value: str) -> bool:
        """Set key to hold the value and set key to expire after `seconds`."""
        try:
            res = self._client.setex(name=key, time=seconds, value=value)
            return bool(res)
        except RedisError as e:
            logger.error("Redis setex failed key=%s: %s", key, e)
            return False

    def get(self, key: str) -> Optional[str]:
        """Get value of key (returns None if not found or on error)."""
        try:
            val = self._client.get(name=key)
            return val if val is not None else None
        except RedisError as e:
            logger.error("Redis get failed key=%s: %s", key, e)
            return None

    def delete(self, *keys: str) -> int:
        """Delete one or more keys. Returns number of keys removed."""
        try:
            return int(self._client.delete(*keys))
        except RedisError as e:
            logger.error("Redis delete failed keys=%s: %s", keys, e)
            return 0

    # ------------------ expiration management ------------------
    def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on key. Returns True if timeout set."""
        try:
            return bool(self._client.expire(name=key, time=seconds))
        except RedisError as e:
            logger.error("Redis expire failed key=%s: %s", key, e)
            return False

    def ttl(self, key: str) -> int:
        """Get the time to live for key in seconds. Returns -2 if key does not exist, -1 if key exists but has no associated expire."""
        try:
            return int(self._client.ttl(name=key))
        except RedisError as e:
            logger.error("Redis ttl failed key=%s: %s", key, e)
            return -2

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        try:
            return int(self._client.exists(key)) > 0
        except RedisError as e:
            logger.error("Redis exists failed key=%s: %s", key, e)
            return False

    # ------------------ helpers ------------------
    @property
    def client(self) -> redis.Redis:
        """Access underlying redis.Redis client (thread-safe)."""
        return self._client


# Module-level singleton, mimic db/mysql_pool style
try:
    redis_client = RedisClient()
except Exception as _e:  # pragma: no cover
    logger.warning("Redis client initialization failed: %s", _e)
    redis_client = None
# 单例
mysql_pool = MySQLClient(MYSQL)
report_mysql_pool = MySQLClient(REPORT_MYSQL)