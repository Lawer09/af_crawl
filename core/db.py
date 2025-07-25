from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple, Optional

import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

from config.settings import MYSQL

logger = logging.getLogger(__name__)


class MySQLPool:
    """MySQL 连接池包装，提供最基本的 select / execute / executemany / fetch_one 方法"""

    _instance: "MySQLPool" | None = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:  # 避免重复初始化
            return
        try:
            self.pool: MySQLConnectionPool = MySQLConnectionPool(
                pool_name=MYSQL["pool_name"],
                pool_size=MYSQL["pool_size"],
                **{k: v for k, v in MYSQL.items() if k not in {"pool_name", "pool_size"}}
            )
            self._initialized = True
            logger.info(
                "MySQL connection pool created: host=%s db=%s size=%s",
                MYSQL["host"], MYSQL["database"], MYSQL["pool_size"],
            )
        except mysql.connector.Error as e:
            logger.exception("[MySQL] create pool failed: %s", e)
            raise

    # -------------------------- 基本方法 --------------------------
    def get_conn(self) -> PooledMySQLConnection:
        return self.pool.get_connection()

    def select(self, sql: str, params: Tuple | Dict | None = None) -> List[Dict[str, Any]]:
        conn = self.get_conn()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params or ())
            return cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

    def fetch_one(self, sql: str, params: Tuple | Dict | None = None) -> Optional[Dict[str, Any]]:
        rows = self.select(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Tuple | Dict | None = None) -> None:
        """单条写入 / 更新 / 删除"""
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params or ())
            conn.commit()
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
            cursor.executemany(sql, param_list)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.exception("[MySQL] executemany failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()


# 单例
mysql_pool = MySQLPool() 