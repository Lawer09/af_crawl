from __future__ import annotations

import logging
from typing import List, Optional

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class AfPbConfigDAO:
    """_tb_auto_cfg_pb 表的DAO。

    字段：id、account、password、pbDomain、status
    用途：提供获取可用域名、更新状态等方法。
    """

    TABLE = "_tb_auto_cfg_pb"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        account VARCHAR(128) DEFAULT NULL,
        password VARCHAR(128) DEFAULT NULL,
        pid VARCHAR(128) DEFAULT NULL,
        pbDomain VARCHAR(255) DEFAULT NULL,
        status INT DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_status (status),
        KEY idx_domain (pbDomain)
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
    def get_by_pid(cls, pid: str) -> Optional[dict]:
        """根据 pid 获取一条待配置记录（status=0）。"""
        try:
            rows = mysql_pool.select(
                f"SELECT id, pid, account, password, pbDomain, status FROM {cls.TABLE} WHERE status=0 AND pid=%s LIMIT 1",
                (pid,)
            )
            return rows[0] if rows else None
        except Exception as e:
            logger.exception(f"Get one available config failed: {e}")
            return None

    @staticmethod
    def _parse_domain_csv(csv_val: Optional[str]) -> List[str]:
        if not csv_val:
            return []
        parts = [p.strip() for p in str(csv_val).split(',')]
        return [p for p in parts if p]

    @classmethod
    def get_random_domain_and_config(cls) -> Optional[tuple[str, int]]:
        """从一条待配置记录的 pbDomain CSV 中随机选择一个域名，返回 (domain, config_id)。"""
        import random
        cfg_row = cls.get_one_available_config()
        if not cfg_row:
            return None
        domains = cls._parse_domain_csv(cfg_row.get('pbDomain'))
        if not domains:
            logger.warning("Available config has empty pbDomain CSV; id=%s", cfg_row.get('id'))
            return None
        domain = random.choice(domains)
        return domain, int(cfg_row.get('id'))

    @classmethod
    def mark_config_active_by_id(cls, config_id: int, status: int = 1) -> bool:
        """按配置记录 id 更新状态为给定值（默认1）。"""
        try:
            affected = mysql_pool.execute(
                f"UPDATE {cls.TABLE} SET status=%s, updated_at=NOW() WHERE id=%s",
                (status, config_id),
            )
            return affected > 0
        except Exception as e:
            logger.exception(f"Update config status failed: id={config_id}, status={status}, err={e}")
            return False