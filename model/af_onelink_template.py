from __future__ import annotations

import logging
from typing import List, Optional

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class AfOnelinkTemplateDAO:
    """_tb_auto_cfg_onelink_template 表的DAO。

    字段：id、account、password、pbDomain、status
    用途：提供获取可用域名、更新状态等方法。
    """

    TABLE = "af_onelink_template"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        pid VARCHAR(255) DEFAULT NULL,
        app_id VARCHAR(255) DEFAULT NULL,
        base_url VARCHAR(255) DEFAULT NULL,
        template_id VARCHAR(255) DEFAULT NULL,
        label VARCHAR(255) DEFAULT NULL,
        value VARCHAR(255) DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        KEY idx_pid (pid),
        KEY idx_app_id (app_id),
        KEY idx_template_id (template_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    @classmethod
    def init_table(cls) -> None:
        try:
            # mysql_pool.execute(cls.CREATE_SQL)
            logger.info(f"Table {cls.TABLE} initialized")
        except Exception as e:
            logger.exception(f"Init table {cls.TABLE} failed: {e}")

    @classmethod
    def save_all(cls, templates: List[dict]) -> Optional[dict]:
        """根据 username 获取一条待配置记录（status=0）。"""
        try:
            if not templates or len(templates) == 0:
                return None

            rows = mysql_pool.execute(
                f"INSERT INTO {cls.TABLE} (pid, app_id, base_url, template_id, label, value) VALUES (%s, %s, %s, %s, %s, %s)",
                [(template["pid"], template["app_id"], template["baseUrl"], template["id"], template["label"], template["value"])
                for template in templates]
            )
            return rows[0] if rows else None
        except Exception as e:
            logger.exception(f"Save onelink templates failed: {e}")
            return None

    @classmethod
    def delete(cls, pid: str, app_id: str) -> Optional[dict]:
        """根据 pid 和 app_id 删除所有模板。"""
        try:
            rows = mysql_pool.execute(
                f"DELETE FROM {cls.TABLE} WHERE pid=%s AND app_id=%s",
                (pid, app_id)
            )
            return rows[0] if rows else None
        except Exception as e:
            logger.exception(f"Delete onelink templates failed: {e}")
            return None

    @classmethod
    def get_templates(cls, pid:str, app_id:str) -> Optional[dict]:
        """根据 pid 和 app_id 获取模板列表。"""
        try:
            rows = mysql_pool.execute(
                f"SELECT * FROM {cls.TABLE} WHERE pid=%s AND app_id=%s",
                (pid, app_id)
            )
            return rows if rows else None
        except Exception as e:
            logger.exception(f"Get onelink templates failed: {e}")
            return None

class AfCrawlUserDAO:
    """_tb_auto_cfg_crawl_user 表的DAO。
    """

    TABLE = "af_crawl_user"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        pid VARCHAR(255) DEFAULT NULL,
        app_id VARCHAR(255) DEFAULT NULL,
        email VARCHAR(255) DEFAULT NULL,
        password VARCHAR(255) DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_pid (pid),
        KEY idx_app_id (app_id),
        KEY idx_email (email)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    @classmethod
    def init_table(cls) -> None:
        try:
            # mysql_pool.execute(cls.CREATE_SQL)
            logger.info(f"Table {cls.TABLE} initialized")
        except Exception as e:
            logger.exception(f"Init table {cls.TABLE} failed: {e}")

    @classmethod
    def save_all(cls, users: List[dict]) -> Optional[dict]:
        """根据 username 获取一条待配置记录（status=0）。"""
        try:
            rows = mysql_pool.execute(
                f"INSERT INTO {cls.TABLE} (pid, app_id, email, password) VALUES (%s, %s, %s, %s)",
                [(user["pid"], user["app_id"], user["email"], user["password"])
                for user in users]
            )
            return rows[0] if rows else None
        except Exception as e:
            logger.exception(f"Save crawl users failed: {e}")
            return None

    @classmethod
    def get_all(cls, status: int = 0) -> Optional[dict]:
        """根据 status 获取所有用户。"""
        try:
            rows = mysql_pool.execute(
                f"SELECT * FROM {cls.TABLE} WHERE status=%s",
                (status,)
            )
            return rows if rows else None
        except Exception as e:
            logger.exception(f"Get crawl users failed: {e}")
            return None