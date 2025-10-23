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
        timezone VARCHAR(64) DEFAULT 'UTC',
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
    def get_list_by_pid(cls, pid: str) -> List[Dict]:
        """根据 pid 查询用户绑定的 app 列表"""
        cls.init_table()
        sql = f"SELECT * FROM {cls.TABLE} WHERE user_type_id=%s"
        return mysql_pool.select(sql, (pid,))
    
    @classmethod
    def get_recent_user_apps(cls, username: str, within_days: int = 1) -> List[Dict]:
        """查询该用户在最近 within_days 天内更新的 app 列表（使用 updated_at）。"""
        cls.init_table()
        sql = f"SELECT * FROM {cls.TABLE} WHERE username=%s AND updated_at >= NOW() - INTERVAL %s DAY"
        return mysql_pool.select(sql, (username, within_days))

    @classmethod
    def get_recent_usernames(cls, usernames: List[str], within_days: int = 1) -> set:
        """查询最近 within_days 天有更新记录的用户名集合，减少逐用户检查次数。"""
        if not usernames:
            return set()
        cls.init_table()
        placeholders = ','.join(['%s'] * len(usernames))
        sql = (
            f"SELECT DISTINCT username FROM {cls.TABLE} "
            f"WHERE username IN ({placeholders}) AND updated_at >= NOW() - INTERVAL %s DAY"
        )
        rows = mysql_pool.select(sql, tuple(usernames) + (within_days,))
        return {r['username'] for r in rows}

    @classmethod
    def get_all_active(cls) -> List[Dict]:
        cls.init_table()
        sql = f"SELECT * FROM {cls.TABLE} WHERE app_status=0"
        return mysql_pool.select(sql)