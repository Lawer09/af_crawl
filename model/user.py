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
    def get_user(cls, email: str) -> Optional[Dict]:
        try:
            rows = cls.select_sql("SELECT email, password, account_type FROM af_users WHERE email = %s", (email,))
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user by email: {e}")
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