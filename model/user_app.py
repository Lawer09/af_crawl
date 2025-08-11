from __future__ import annotations

import logging
from typing import List, Dict

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class UserAppDAO:
    TABLE = "af_user_app"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        app_id VARCHAR(128) NOT NULL,
        app_name VARCHAR(255),
        platform ENUM('android','ios') DEFAULT 'android',
        timezone VARCHAR(64),
        user_type_id VARCHAR(64),
        app_status TINYINT DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_user_app (username, app_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    @classmethod
    def init_table(cls):
        mysql_pool.execute(cls.CREATE_SQL)

    @classmethod
    def save_apps(cls, apps: List[Dict]):
        if not apps:
            return
        cls.init_table()
        sql = f"""
        INSERT INTO {cls.TABLE}
            (username, app_id, app_name, platform, timezone, user_type_id, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW())
        ON DUPLICATE KEY UPDATE
            app_name=VALUES(app_name), platform=VALUES(platform), timezone=VALUES(timezone),
            user_type_id=VALUES(user_type_id), updated_at=VALUES(updated_at)
        """
        params = [
            (
                a["username"], a["app_id"], a.get("app_name"), a.get("platform"),
                a.get("timezone"), a.get("user_type_id"),
            )
            for a in apps
        ]
        mysql_pool.executemany(sql, params)

    @classmethod
    def get_user_apps(cls, username: str) -> List[Dict]:
        cls.init_table()
        sql = f"SELECT * FROM {cls.TABLE} WHERE username=%s"
        return mysql_pool.select(sql, (username,))

    @classmethod
    def get_user_app(cls, username: str, app_id: str) -> List[Dict]:
        cls.init_table()
        sql = f"SELECT * FROM {cls.TABLE} WHERE username=%s AND app_id=%s"
        return mysql_pool.select(sql, (username, app_id))

    @classmethod
    def get_all_active(cls) -> List[Dict]:
        cls.init_table()
        sql = f"SELECT * FROM {cls.TABLE} WHERE app_status=0"
        return mysql_pool.select(sql) 