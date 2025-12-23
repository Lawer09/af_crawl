from __future__ import annotations

import logging
from typing import List, Dict, Optional
from datetime import datetime

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class GoogleAuthDAO:
    """google_auth 表封装"""

    TABLE = "google_auth"

    @classmethod
    def get_by_account(cls, account: str) -> Optional[Dict]:
        """根据 account 查询"""
        try:
            # key 是关键字，需要用反引号
            rows = mysql_pool.select(
                f"SELECT id, account, `key`, note, created_at, updated_at FROM {cls.TABLE} WHERE account = %s LIMIT 1",
                (account,)
            )
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching google_auth by account: {e}")
            return None

    @classmethod
    def get_by_own(cls, own: int) -> List[Dict]:
        """根据 own 查询，返回不包含 key 的列表"""
        try:
            rows = mysql_pool.select(
                f"SELECT id, account, note, created_at, updated_at, created_by, own FROM {cls.TABLE} WHERE own = %s",
                (own,)
            )
            return rows
        except Exception as e:
            logger.error(f"Error fetching google_auth by own={own}: {e}")
            return []

    @classmethod
    def save_auth(cls, account: str, key: str, note: str = None, created_by: int = None, own: int = None) -> int:
        """保存或更新 google_auth。如果 account 存在则更新 key，否则插入。"""
        try:
            # 检查是否存在
            existing = cls.get_by_account(account)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            if existing:
                # 更新
                sql = f"UPDATE {cls.TABLE} SET `key` = %s, updated_at = %s"
                params = [key, now_str]
                
                if note is not None:
                    sql += ", note = %s"
                    params.append(note)
                
                sql += " WHERE id = %s"
                params.append(existing['id'])
                
                return mysql_pool.execute(sql, tuple(params))
            else:
                # 插入
                # created_at is varchar in the schema provided
                sql = f"""
                INSERT INTO {cls.TABLE} (account, `key`, note, created_at, updated_at, created_by, own)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                return mysql_pool.execute(sql, (account, key, note, now_str, now_str, created_by, own))
        except Exception as e:
            logger.error(f"Error saving google_auth for account={account}: {e}")
            raise e
