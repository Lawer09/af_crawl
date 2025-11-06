from __future__ import annotations

import logging
from typing import List, Dict, Optional

from core.db import mysql_pool

logger = logging.getLogger(__name__)

PENDING = "pending"
DONE = "done"
FAIL = "fail"
ZERO = "zero"

class AfTaskRetDAO:
    """AF 爬取结果记录表 DAO

    表结构：af_crawl_ret
    字段：
      - system_type INT
      - pid VARCHAR(30)
      - fetch_date CHAR(10)
      - app_id VARCHAR(100)
      - status VARCHAR(10)
      - start_at TIMESTAMP
      - end_at TIMESTAMP
      - ret VARCHAR(255)
      - created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """

    TABLE = "af_crawl_ret"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE}` (
      `id` INT NOT NULL AUTO_INCREMENT,
      `system_type` INT NULL DEFAULT NULL,
      `pid` VARCHAR(30) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
      `fetch_date` CHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NULL DEFAULT NULL COMMENT '数据的日期 UTC',
      `app_id` VARCHAR(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NULL DEFAULT NULL,
      `status` VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NULL DEFAULT NULL,
      `start_at` TIMESTAMP NULL DEFAULT NULL,
      `end_at` TIMESTAMP NULL DEFAULT NULL,
      `ret` VARCHAR(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NULL DEFAULT NULL COMMENT '结果信息',
      `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`) USING BTREE,
      INDEX `idx_pid`(`pid` ASC) USING BTREE,
      INDEX `idx_app_id`(`app_id` ASC) USING BTREE,
      INDEX `idx_fetch_date`(`fetch_date` ASC) USING BTREE,
      INDEX `idx_status`(`status` ASC) USING BTREE,
      INDEX `idx_created_at`(`created_at` ASC) USING BTREE
    ) ENGINE=InnoDB CHARACTER SET=utf8mb3 COLLATE=utf8mb3_general_ci COMMENT='记录af数据爬取的结果' ROW_FORMAT=Dynamic;
    """

    @classmethod
    def init_table(cls):
        """初始化表结构（存在则跳过）。"""
        try:
            mysql_pool.execute(cls.CREATE_SQL)
            logger.info("Table %s initialized.", cls.TABLE)
        except Exception as e:
            logger.exception("Init table %s failed: %s", cls.TABLE, e)

    @classmethod
    def insert(cls,
               system_type: int | None,
               pid: str,
               fetch_date: str | None,
               app_id: str | None,
               status: str | None,
               start_at,
               end_at,
               ret: str | None) -> int:
        """插入一条爬取结果记录。

        返回受影响行数。
        """
        sql = (
            f"INSERT INTO {cls.TABLE} "
            f"(system_type, pid, fetch_date, app_id, status, start_at, end_at, ret) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        params = (system_type, pid, fetch_date, app_id, status, start_at, end_at, ret)
        try:
            return mysql_pool.execute(sql, params)
        except Exception as e:
            logger.exception("AfTaskRetDAO.insert failed: pid=%s app_id=%s date=%s status=%s err=%s", pid, app_id, fetch_date, status, e)
            return 0

    @classmethod
    def insert_many(cls, rows: List[Dict]) -> int:
        """批量插入爬取结果记录。rows 为包含上述字段键的字典列表。

        返回受影响行数。
        """
        if not rows:
            return 0
        sql = (
            f"INSERT INTO {cls.TABLE} "
            f"(system_type, pid, fetch_date, app_id, status, start_at, end_at, ret) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        params = [
            (
                r.get("system_type"),
                r.get("pid"),
                r.get("fetch_date"),
                r.get("app_id"),
                r.get("status"),
                r.get("start_at"),
                r.get("end_at"),
                r.get("ret"),
            )
            for r in rows
        ]
        try:
            return mysql_pool.executemany(sql, params)
        except Exception as e:
            logger.exception("AfTaskRetDAO.insert_many failed: count=%d err=%s", len(rows), e)
            return 0

    @classmethod
    def get_by_status(cls, status: str, limit: int = 200) -> List[Dict]:
        """根据状态查询记录，按创建时间倒序返回。

        参数：
          - status: 记录状态（如 'done' / 'fail' 等）
          - limit: 返回条数限制，默认 200
        返回：记录列表（字典数组）
        """
        try:
            sql = f"SELECT * FROM {cls.TABLE} WHERE status=%s ORDER BY created_at DESC LIMIT %s"
            return mysql_pool.select(sql, (status, limit))
        except Exception as e:
            logger.exception("AfTaskRetDAO.get_by_status failed: status=%s err=%s", status, e)
            return []

    @classmethod
    def update_many(cls, rows: List[Dict]) -> int:
        """批量更新记录。每项需包含 id 以及要更新的字段：status, start_at, end_at, ret。

        返回受影响行数。
        """
        if not rows:
            return 0
        sql = f"UPDATE {cls.TABLE} SET status=%s, start_at=%s, end_at=%s, ret=%s WHERE id=%s"
        params = [
            (
                r.get("status"),
                r.get("start_at"),
                r.get("end_at"),
                r.get("ret"),
                r.get("id"),
            )
            for r in rows
            if r.get("id") is not None
        ]
        if not params:
            return 0
        try:
            return mysql_pool.executemany(sql, params)
        except Exception as e:
            logger.exception("AfTaskRetDAO.update_many failed: count=%d err=%s", len(params), e)
            return 0


class TaskDAO:
    """任务表: 支持失败延迟、重启续跑"""

    TABLE = "cl_task"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        retry INT DEFAULT 0,
        status ENUM('pending','running','failed','done') DEFAULT 'pending',
        next_run_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        priority INT DEFAULT 0 COMMENT '任务优先级',
        task_type VARCHAR(64) DEFAULT NULL COMMENT '任务类型',
        task_data JSON DEFAULT NULL COMMENT '任务数据',
        task_ret JSON DEFAULT NULL COMMENT '任务执行结果（成功/失败 app_id 及时间和原因）',
        max_retry_count INT DEFAULT 3 COMMENT '最大重试次数',
        execution_timeout INT DEFAULT 3600 COMMENT '任务执行超时时间（秒）',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_status_next (status, next_run_at),
        KEY idx_task_type (task_type),
        KEY idx_priority (priority)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    @classmethod
    def init_table(cls):
        mysql_pool.execute(cls.CREATE_SQL)
        logger.info(f"Table {cls.TABLE} initialized.")

    @classmethod
    def add_task(cls, task_type:str, task_data:str, next_run_at:str, status:str='pending', task_ret:str=None, priority:int=0, execution_timeout:int=3600, max_retry_count:int=3):
        """添加任务"""
        sql = f"""
        INSERT INTO {cls.TABLE}
            (status, task_type, task_data, task_ret, next_run_at, priority, execution_timeout, max_retry_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        params = (status, task_type, task_data, task_ret, next_run_at, priority, execution_timeout, max_retry_count)
        mysql_pool.execute(sql, params)
    
    @classmethod
    def update_task(cls, task:dict):
        """更新任务状态"""
        
        sql = f"""
        UPDATE {cls.TABLE}
        SET task_data=%s,
            task_ret=COALESCE(%s, task_ret),
            status=%s,
            next_run_at=%s,
            retry=%s,
            updated_at=NOW()
        WHERE id=%s
        """
        params = (
            task.get('task_data'),
            task.get('task_ret'),
            task.get('status'),
            task.get('next_run_at'),
            task.get('retry'),
            task.get('id')
        )

        mysql_pool.execute(sql, params)

    @classmethod
    def update_task_ret(cls, task_id: int, task_ret: str):
        """仅更新任务结果字段"""
        sql = f"UPDATE {cls.TABLE} SET task_ret=%s, updated_at=NOW() WHERE id=%s"
        mysql_pool.execute(sql, (task_ret, task_id))

    @classmethod
    def get_task(cls, task_id: int) -> Optional[dict]:
        """根据任务ID获取任务详情"""
        sql = f"SELECT * FROM {cls.TABLE} WHERE id=%s"
        result = mysql_pool.select(sql, (task_id,))
        return result[0] if result else None

    @classmethod
    def add_tasks(cls, tasks: List[Dict]):
        if not tasks:
            return False

        try:
            sql = f"""
            INSERT INTO {cls.TABLE}
                (task_type, task_data, next_run_at, priority, execution_timeout, max_retry_count)
            VALUES (%s,%s,%s,%s,%s,%s)
            """

            params = [(
                t.get('task_type'),
                t.get('task_data'),
                t.get('next_run_at'), t.get('priority', 0), 
                t.get('execution_timeout', 3600), t.get('max_retry_count', 3)
            ) for t in tasks]
            mysql_pool.executemany(sql, params)
            return True
        except Exception as e:
            logger.exception(f"Add tasks failed: {e}")
            return False

    @classmethod
    def get_pending_by_type(cls, task_type:str, limit: int = 100) -> List[Dict]:
        """
        获取待执行的任务列表（过滤重试次数）
        """
        if task_type:
            sql = f"""SELECT * FROM {cls.TABLE}
                     WHERE task_type=%s AND status='pending' AND next_run_at<=NOW() AND retry < max_retry_count
                     ORDER BY next_run_at LIMIT %s"""
            return mysql_pool.select(sql, (task_type, limit))
        else:
            sql = f"""SELECT * FROM {cls.TABLE}
                     WHERE status='pending' AND next_run_at<=NOW() AND retry < max_retry_count
                     ORDER BY next_run_at LIMIT %s"""
            return mysql_pool.select(sql, (limit,))

    @classmethod
    def get_pending(cls, limit: int = 100) -> List[Dict]:
        """
        获取待执行的任务列表（过滤重试次数）
        """
        sql = f"""SELECT * FROM {cls.TABLE}
                 WHERE status='pending' AND next_run_at<=NOW() AND retry < max_retry_count
                 ORDER BY next_run_at LIMIT %s"""
        return mysql_pool.select(sql, (limit,))

    @classmethod
    def mark_running(cls, task_id: int, device_id: Optional[str] = None):
        if device_id:
            mysql_pool.execute(
                f"UPDATE {cls.TABLE} SET status='running', assigned_device_id=%s, assigned_at=NOW() WHERE id=%s", 
                (device_id, task_id)
            )
        else:
            mysql_pool.execute(f"UPDATE {cls.TABLE} SET status='running' WHERE id=%s", (task_id,))

    @classmethod
    def mark_done(cls, task_id: int):
        mysql_pool.execute(f"UPDATE {cls.TABLE} SET status='done', updated_at=NOW() WHERE id=%s", (task_id,))

    @classmethod
    def mark_done_batch(cls, task_ids: List[int]) -> int:
        """批量标记任务为完成，返回受影响行数"""
        if not task_ids:
            return 0
        try:
            placeholders = ','.join(['%s'] * len(task_ids))
            sql = f"UPDATE {cls.TABLE} SET status='done', updated_at=NOW() WHERE id IN ({placeholders})"
            affected = mysql_pool.execute(sql, tuple(task_ids))
            return affected
        except Exception as e:
            logger.exception(f"Failed to mark done batch: ids={task_ids}, error={e}")
            return 0

    @classmethod
    def mark_retry_batch(cls, task_ids: List[int]) -> int:
        """批量增加重试次数（仅限制 pending 状态）"""
        if not task_ids:
            return 0
        try:
            placeholders = ','.join(['%s'] * len(task_ids))
            sql = f"UPDATE {cls.TABLE} SET retry=retry+1, updated_at=NOW() WHERE id IN ({placeholders}) AND status='pending'"
            affected = mysql_pool.execute(sql, tuple(task_ids))
            return affected
        except Exception as e:
            logger.exception(f"Failed to mark retry batch: ids={task_ids}, error={e}")
            return 0

    @classmethod
    def fail_task(cls, task_id: int, retry_delay_sec: int):
        mysql_pool.execute(
            f"UPDATE {cls.TABLE} SET status='failed', retry=retry+1, next_run_at=NOW()+INTERVAL %s SECOND WHERE id=%s",
            (retry_delay_sec, task_id),
        )

    @classmethod
    def zero_task(cls):
        #将所有pending的任务的 status 设置为 zero
        mysql_pool.execute(
            f"UPDATE {cls.TABLE} SET status='zero' WHERE status='pending'"
        )

    @classmethod
    def fail_task_batch(cls, task_ids: List[int], retry_delay_sec: int) -> int:
        """批量标记任务为失败，并增加重试次数"""
        if not task_ids:
            return 0
        try:
            placeholders = ','.join(['%s'] * len(task_ids))
            sql = f"UPDATE {cls.TABLE} SET status='failed', retry=retry+1, next_run_at=NOW()+INTERVAL %s SECOND WHERE id IN ({placeholders})"
            params = (retry_delay_sec, *task_ids)
            affected = mysql_pool.execute(sql, params)
            return affected
        except Exception as e:
            logger.exception(f"fail_task_batch error: ids={task_ids}, error={e}")
            return 0

    @classmethod
    def reset_all(cls):
        mysql_pool.execute(f"DELETE FROM {cls.TABLE}")

    @classmethod
    def fetch_user_pending_tasks(cls, username: str, task_type: str, limit: int = 50) -> List[Dict]:
        sql = f"""SELECT * FROM {cls.TABLE}
                 WHERE username=%s AND task_type=%s AND status='pending' AND next_run_at<=NOW()
                 ORDER BY next_run_at LIMIT %s"""
        return mysql_pool.select(sql, (username, task_type, limit))

    @classmethod
    def get_user_total_tasks(cls, username: str, task_type: str) -> int:
        sql = f"SELECT COUNT(*) as count FROM {cls.TABLE} WHERE username=%s AND task_type=%s"
        results = mysql_pool.select(sql, (username, task_type))
        result = results[0] if results else None
        return result['count'] if result else 0

    @classmethod
    def get_user_completed_tasks(cls, username: str, task_type: str) -> int:
        sql = f"SELECT COUNT(*) as count FROM {cls.TABLE} WHERE username=%s AND task_type=%s AND status='done'"
        results = mysql_pool.select(sql, (username, task_type))
        result = results[0] if results else None
        return result['count'] if result else 0

    @classmethod
    def reset_failed(cls):
        mysql_pool.execute(
            f"UPDATE {cls.TABLE} SET status='pending', assigned_device_id=NULL, assigned_at=NULL WHERE status='failed'"
        )
    
    @classmethod
    def reset_task_for_retry(cls, task_id: int) -> bool:
        """重置任务为待分配状态，用于重试"""
        try:
            sql = f"""
            UPDATE {cls.TABLE} 
            SET status='pending', assigned_device_id=NULL, assigned_at=NULL, 
                next_run_at=NOW(), updated_at=NOW()
            WHERE id=%s
            """
            result = mysql_pool.execute(sql, (task_id,))
            logger.info(f"Task {task_id} reset for retry")
            return result > 0
        except Exception as e:
            logger.exception(f"Failed to reset task for retry: task_id={task_id}, error={e}")
            return False
    
    # ---------- 分布式任务相关方法 ----------
    @classmethod
    def assign_task(cls, task_id: int, device_id: str) -> bool:
        """分配任务给设备"""
        try:
            sql = f"""
            UPDATE {cls.TABLE} 
            SET status='assigned', assigned_device_id=%s, assigned_at=NOW(), updated_at=NOW()
            WHERE id=%s AND status='pending'
            """
            result = mysql_pool.execute(sql, (device_id, task_id))
            return result > 0
        except Exception as e:
            logger.exception(f"Failed to assign task: task_id={task_id}, device_id={device_id}, error={e}")
            return False
    
    @classmethod
    def get_assignable_tasks(cls, task_type: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """获取可分配的任务"""
        try:
            if task_type:
                sql = f"""
                SELECT * FROM {cls.TABLE}
                WHERE task_type=%s AND status='pending' AND next_run_at<=NOW()
                ORDER BY priority DESC, next_run_at ASC
                LIMIT %s
                """
                return mysql_pool.select(sql, (task_type, limit))
            else:
                sql = f"""
                SELECT * FROM {cls.TABLE}
                WHERE status='pending' AND next_run_at<=NOW()
                ORDER BY priority DESC, next_run_at ASC
                LIMIT %s
                """
                return mysql_pool.select(sql, (limit,))
        except Exception as e:
            logger.exception(f"Failed to get assignable tasks: error={e}")
            return []
    
    @classmethod
    def get_device_tasks(cls, device_id: str) -> List[Dict]:
        """获取设备分配的任务"""
        try:
            sql = f"""
            SELECT * FROM {cls.TABLE}
            WHERE assigned_device_id=%s AND status IN ('assigned', 'running')
            ORDER BY assigned_at
            """
            return mysql_pool.select(sql, (device_id,))
        except Exception as e:
            logger.exception(f"Failed to get device tasks: device_id={device_id}, error={e}")
            return []
    
    @classmethod
    def release_device_tasks(cls, device_id: str) -> int:
        """释放设备的所有任务"""
        try:
            sql = f"""
            UPDATE {cls.TABLE} 
            SET status='pending', assigned_device_id=NULL, assigned_at=NULL, updated_at=NOW()
            WHERE assigned_device_id=%s AND status IN ('assigned', 'running')
            """
            return mysql_pool.execute(sql, (device_id,))
        except Exception as e:
            logger.exception(f"Failed to release device tasks: device_id={device_id}, error={e}")
            return 0
    
    @classmethod
    def get_timeout_tasks(cls, timeout_minutes: int = 60) -> List[Dict]:
        """获取超时的任务"""
        try:
            from datetime import datetime, timedelta
            timeout_time = datetime.now() - timedelta(minutes=timeout_minutes)
            
            sql = f"""
            SELECT * FROM {cls.TABLE}
            WHERE status IN ('assigned', 'running') 
              AND assigned_at < %s
            ORDER BY assigned_at
            """
            return mysql_pool.select(sql, (timeout_time,))
        except Exception as e:
            logger.exception(f"Failed to get timeout tasks: error={e}")
            return []
    
    @classmethod
    def get_task_stats(cls) -> Dict:
        """获取任务统计信息"""
        try:
            sql = f"""
            SELECT 
                status,
                COUNT(*) as count
            FROM {cls.TABLE}
            GROUP BY status
            """
            
            results = mysql_pool.select(sql)
            stats = {row['status']: row['count'] for row in results}
            
            # 添加总数
            stats['total'] = sum(stats.values())
            
            return stats
        except Exception as e:
            logger.exception(f"Failed to get task stats: error={e}")
            return {}
    
    @classmethod
    def update_task_priority(cls, task_id: int, priority: int) -> bool:
        """更新任务优先级"""
        try:
            sql = f"UPDATE {cls.TABLE} SET priority=%s, updated_at=NOW() WHERE id=%s"
            result = mysql_pool.execute(sql, (priority, task_id))
            return result > 0
        except Exception as e:
            logger.exception(f"Failed to update task priority: task_id={task_id}, priority={priority}, error={e}")
            return False

    @classmethod
    def get_last_update_time(cls):
        """获取任务表最近的更新时间（MAX(updated_at)）"""
        try:
            row = mysql_pool.fetch_one(f"SELECT MAX(updated_at) AS last_update FROM {cls.TABLE}")
            return row['last_update'] if row and row.get('last_update') else None
        except Exception as e:
            logger.exception(f"Failed to get last update time: {e}")
            return None

    @classmethod
    def exists_pending(cls) -> bool:
        """是否存在可执行的 pending 任务"""
        try:
            sql = f"SELECT 1 FROM {cls.TABLE} WHERE status='pending' AND next_run_at<=NOW() AND retry < max_retry_count LIMIT 1"
            row = mysql_pool.fetch_one(sql)
            return row is not None
        except Exception as e:
            logger.exception(f"Failed to check pending tasks: {e}")
            return False

    @classmethod
    def should_create_new_tasks(cls, interval_hours: int = 4) -> bool:
        """当无 pending 且最近更新时间超过 interval_hours 时返回 True"""
        try:
            if cls.exists_pending():
                return False
            last_update = cls.get_last_update_time()
            if not last_update:
                return True
            from datetime import datetime, timedelta
            return last_update + timedelta(hours=interval_hours) < datetime.now()
        except Exception as e:
            logger.exception(f"Failed to evaluate should_create_new_tasks: {e}")
            return False