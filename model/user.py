from __future__ import annotations

import logging
from typing import List, Dict, Optional

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class UserDAO:
    """af_user 表简单封装"""

    TABLE = "af_user"

    @classmethod
    def get_enabled_users(cls) -> List[Dict]:
        sql = f"SELECT email, password, account_type FROM {cls.TABLE} WHERE enable = 1 AND account_type in ('pid','agency') AND (email IS NOT NULL AND TRIM(email) <> '') AND (password IS NOT NULL AND TRIM(password) <> '') "
        return mysql_pool.select(sql)

    @classmethod
    def get_user_by_email(cls, email: str) -> Optional[Dict]:

        try:
            rows = mysql_pool.select(f"SELECT email, password, account_type FROM {cls.TABLE} WHERE email = %s", (email,))

            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user by email: {e}")
            return None

    @classmethod
    def get_user_by_pid(cls, pid: str) -> Optional[Dict]:
        """根据 pid 查询用户（当 pid='pid'）"""
        try:
            rows = mysql_pool.select(
                f"SELECT id, email, password, account_type FROM {cls.TABLE} WHERE pid = %s",
                (pid,)
            )
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user by pid: {e}")
            return None

    @classmethod
    def get_user_id_by_pid(cls, pid: str) -> Optional[int]:
        """仅返回用户 id（便于外部关系映射）"""
        try:
            rows = mysql_pool.select(
                f"SELECT id FROM {cls.TABLE} WHERE pid = %s LIMIT 1",
                (pid,)
            )
            if rows:
                return int(rows[0]["id"])  # type: ignore
            return None
        except Exception as e:
            logger.error(f"Error fetching user id by pid: {e}")
            return None

    @classmethod
    def save_user(cls, email: str, password: str, account_type: str):
        sql = f"""
        INSERT INTO {cls.TABLE} (email, password, account_type, enable)
        VALUES (%s,%s,%s,1)
        ON DUPLICATE KEY UPDATE password=VALUES(password), account_type=VALUES(account_type)
        """
        mysql_pool.execute(sql, (email, password, account_type)) 

    @classmethod
    def get_users_by_emails(cls, emails: List[str]) -> Dict[str, Dict]:
        if not emails:
            return {}
        placeholders = ','.join(['%s'] * len(emails))
        sql = f"SELECT email, password, account_type FROM {cls.TABLE} WHERE email IN ({placeholders})"
        rows = mysql_pool.select(sql, tuple(emails))
        return {row['email']: row for row in rows}

    @classmethod
    def get_users_by_pids(cls, pids: List[str]) -> Dict[str, Dict]:
        """批量根据 pid 查询用户，返回 {pid: {email,password,account_type}}"""
        if not pids:
            return {}
        placeholders = ','.join(['%s'] * len(pids))
        sql = f"SELECT pid, email, password, account_type FROM {cls.TABLE} WHERE pid IN ({placeholders})"
        rows = mysql_pool.select(sql, tuple(pids))
        return {row['pid']: {'email': row['email'], 'password': row['password'], 'account_type': row['account_type']} for row in rows}


class UserProxyDAO:
    """_tb_static_proxy 表：用户静态代理配置（与 af_user.pid 一一对应）"""

    TABLE = "_tb_static_proxy"

    @classmethod
    def get_by_pid(cls, pid: str) -> Optional[Dict]:
        """根据 pid 查询一条代理记录"""
        try:
            sql = (
                f"SELECT id, pid, proxy_url, country, sub_at, end_at, created_at, "
                f"system_type, ua, timezone_id FROM {cls.TABLE} WHERE pid = %s LIMIT 1"
            )
            rows = mysql_pool.select(sql, (pid,))
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user proxy by pid: {e}")
            return None
    
    @classmethod
    def get_random_one(cls) -> Optional[Dict]:
        """随机获取一条未停用的代理记录"""
        try:
            sql = (
                f"SELECT id, pid, proxy_url, country, sub_at, end_at, created_at, "
                f"system_type, ua, timezone_id FROM {cls.TABLE} WHERE deactivate = 0 ORDER BY RAND() LIMIT 1"
            )
            rows = mysql_pool.select(sql)
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching random user proxy: {e}")
            return None

    @classmethod
    def get_enable(cls) -> Optional[Dict]:
        """根据所有代理记录"""
        try:
            sql = (
                f"SELECT id, pid, proxy_url, country, sub_at, end_at, created_at, "
                f"system_type, ua, timezone_id FROM {cls.TABLE} WHERE deactivate = 0"
            )
            rows = mysql_pool.select(sql)
            if rows:
                return rows
            return []
        except Exception as e:
            logger.error(f"Error fetching user proxy by pid: {e}")
            return []