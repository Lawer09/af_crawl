from __future__ import annotations

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from model.crawl_task import CrawlTaskDAO
from model.device import DeviceDAO

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class TaskAssignmentDAO:
    """任务分配数据访问对象"""
    
    TABLE = "cl_task_assignment"
    
    @classmethod
    def init_table(cls):
        """初始化任务分配表"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {cls.TABLE} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            task_id INT NOT NULL COMMENT '任务ID',
            device_id VARCHAR(64) NOT NULL COMMENT '分配的设备ID',
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '分配时间',
            started_at DATETIME COMMENT '开始执行时间',
            completed_at DATETIME COMMENT '完成时间',
            status ENUM('assigned', 'running', 'completed', 'failed', 'timeout') DEFAULT 'assigned',
            retry_count INT DEFAULT 0 COMMENT '重试次数',
            error_message TEXT COMMENT '错误信息',
            result_data JSON COMMENT '执行结果数据',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_task_id (task_id),
            INDEX idx_device_id (device_id),
            INDEX idx_status (status),
            INDEX idx_assigned_at (assigned_at),
            UNIQUE KEY uk_task_device (task_id, device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务分配表'
        """
        mysql_pool.execute(sql)
        logger.info(f"Table {cls.TABLE} initialized")
    
    @classmethod
    def create_assignment(cls, task_id: int, device_id: str) -> Optional[int]:
        """创建任务分配记录"""
        try:
            # 首先检查是否已存在分配记录
            existing = cls.get_assignment_by_task_device(task_id, device_id)
            if existing:
                logger.info(f"Task assignment already exists: task_id={task_id}, device_id={device_id}, assignment_id={existing['id']}")
                return existing['id']
            
            sql = f"""
            INSERT INTO {cls.TABLE} (task_id, device_id, status)
            VALUES (%s, %s, 'assigned')
            """
            mysql_pool.execute(sql, (task_id, device_id))
            
            # 获取插入的ID
            result = mysql_pool.fetch_one("SELECT LAST_INSERT_ID() as id")
            assignment_id = result['id'] if result else None
            
            logger.info(f"Task assignment created: task_id={task_id}, device_id={device_id}, assignment_id={assignment_id}")
            return assignment_id
            
        except Exception as e:
            logger.exception(f"Failed to create task assignment: task_id={task_id}, device_id={device_id}, error={e}")
            return None
    
    @classmethod
    def update_status(cls, assignment_id: int, status: str, error_message: Optional[str] = None) -> bool:
        """更新任务分配状态"""
        try:
            if status == 'running':
                sql = f"""
                UPDATE {cls.TABLE} 
                SET status = %s, started_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """
                mysql_pool.execute(sql, (status, assignment_id))
            elif status in ('completed', 'failed', 'timeout'):
                sql = f"""
                UPDATE {cls.TABLE} 
                SET status = %s, completed_at = NOW(), error_message = %s, updated_at = NOW()
                WHERE id = %s
                """
                mysql_pool.execute(sql, (status, error_message, assignment_id))
            else:
                sql = f"""
                UPDATE {cls.TABLE} 
                SET status = %s, error_message = %s, updated_at = NOW()
                WHERE id = %s
                """
                mysql_pool.execute(sql, (status, error_message, assignment_id))
            
            return True
            
        except Exception as e:
            logger.exception(f"Failed to update assignment status: assignment_id={assignment_id}, status={status}, error={e}")
            return False
    
    @classmethod
    def update_status_by_task_device(cls, task_id: int, device_id: str, status: str, 
                                   error_message: Optional[str] = None, result_data: Optional[Dict] = None) -> bool:
        """根据任务ID和设备ID更新状态"""
        try:
            import json
            
            if status == 'running':
                sql = f"""
                UPDATE {cls.TABLE} 
                SET status = %s, started_at = NOW(), updated_at = NOW()
                WHERE task_id = %s AND device_id = %s
                """
                mysql_pool.execute(sql, (status, task_id, device_id))
            elif status in ('completed', 'failed', 'timeout'):
                result_json = json.dumps(result_data) if result_data else None
                sql = f"""
                UPDATE {cls.TABLE} 
                SET status = %s, completed_at = NOW(), error_message = %s, 
                    result_data = %s, updated_at = NOW()
                WHERE task_id = %s AND device_id = %s
                """
                mysql_pool.execute(sql, (status, error_message, result_json, task_id, device_id))
            else:
                sql = f"""
                UPDATE {cls.TABLE} 
                SET status = %s, error_message = %s, updated_at = NOW()
                WHERE task_id = %s AND device_id = %s
                """
                mysql_pool.execute(sql, (status, error_message, task_id, device_id))
            
            return True
            
        except Exception as e:
            logger.exception(f"Failed to update assignment status: task_id={task_id}, device_id={device_id}, status={status}, error={e}")
            return False
    
    @classmethod
    def get_device_running_tasks(cls, device_id: str) -> List[Dict]:
        """获取设备正在运行的任务"""
        try:
            sql = f"""
            SELECT ta.id, ta.task_id, ta.device_id, ta.status, ta.assigned_at, 
                   ta.started_at, ta.retry_count, ct.task_type, ct.task_data
            FROM {cls.TABLE} ta
            JOIN {CrawlTaskDAO.TABLE} ct ON ta.task_id = ct.id
            WHERE ta.device_id = %s AND ta.status IN ('assigned', 'running')
            ORDER BY ta.assigned_at
            """
            return mysql_pool.select(sql, (device_id,))
        except Exception as e:
            logger.exception(f"Failed to get device running tasks: device_id={device_id}, error={e}")
            return []
    
    @classmethod
    def get_timeout_assignments(cls, timeout_minutes: int) -> List[Dict]:
        """获取超时的任务分配"""
        try:
            timeout_time = datetime.now() - timedelta(minutes=timeout_minutes)
            sql = f"""
            SELECT ta.id, ta.task_id, ta.device_id, ta.status, ta.assigned_at, 
                   ta.started_at, ta.retry_count
            FROM {cls.TABLE} ta
            WHERE ta.status IN ('assigned', 'running')
              AND ta.assigned_at < %s
            ORDER BY ta.assigned_at
            """
            return mysql_pool.select(sql, (timeout_time,))
        except Exception as e:
            logger.exception(f"Failed to get timeout assignments: error={e}")
            return []
    
    @classmethod
    def increment_retry_count(cls, assignment_id: int) -> bool:
        """增加重试次数"""
        try:
            sql = f"""
            UPDATE {cls.TABLE} 
            SET retry_count = retry_count + 1, updated_at = NOW()
            WHERE id = %s
            """
            mysql_pool.execute(sql, (assignment_id,))
            return True
        except Exception as e:
            logger.exception(f"Failed to increment retry count: assignment_id={assignment_id}, error={e}")
            return False
    
    @classmethod
    def get_assignment_by_task_device(cls, task_id: int, device_id: str) -> Optional[Dict]:
        """根据任务ID和设备ID获取分配记录"""
        try:
            sql = f"""
            SELECT id, task_id, device_id, assigned_at, started_at, completed_at,
                   status, retry_count, error_message, result_data
            FROM {cls.TABLE}
            WHERE task_id = %s AND device_id = %s
            """
            return mysql_pool.fetch_one(sql, (task_id, device_id))
        except Exception as e:
            logger.exception(f"Failed to get assignment: task_id={task_id}, device_id={device_id}, error={e}")
            return None
    
    @classmethod
    def get_task_assignments(cls, task_id: int) -> List[Dict]:
        """获取任务的所有分配记录"""
        try:
            sql = f"""
            SELECT ta.id, ta.task_id, ta.device_id, ta.assigned_at, ta.started_at, 
                   ta.completed_at, ta.status, ta.retry_count, ta.error_message,
                   d.device_name, d.device_type
            FROM {cls.TABLE} ta
            LEFT JOIN {DeviceDAO.TABLE} d ON ta.device_id = d.device_id
            WHERE ta.task_id = %s
            ORDER BY ta.assigned_at DESC
            """
            return mysql_pool.select(sql, (task_id,))
        except Exception as e:
            logger.exception(f"Failed to get task assignments: task_id={task_id}, error={e}")
            return []
    
    @classmethod
    def get_device_assignment_stats(cls, device_id: str, days: int = 7) -> Dict:
        """获取设备分配统计信息"""
        try:
            start_date = datetime.now() - timedelta(days=days)
            sql = f"""
            SELECT 
                COUNT(*) as total_assignments,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) as timeout,
                AVG(CASE 
                    WHEN status = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL 
                    THEN TIMESTAMPDIFF(SECOND, started_at, completed_at)
                    ELSE NULL 
                END) as avg_execution_time
            FROM {cls.TABLE}
            WHERE device_id = %s AND assigned_at >= %s
            """
            
            result = mysql_pool.fetch_one(sql, (device_id, start_date))
            if result:
                stats = dict(result)
                stats['success_rate'] = (
                    stats['completed'] / stats['total_assignments'] * 100 
                    if stats['total_assignments'] > 0 else 0
                )
                return stats
            else:
                return {
                    'total_assignments': 0,
                    'completed': 0,
                    'failed': 0,
                    'timeout': 0,
                    'avg_execution_time': None,
                    'success_rate': 0
                }
                
        except Exception as e:
            logger.exception(f"Failed to get device assignment stats: device_id={device_id}, error={e}")
            return {}
    
    @classmethod
    def cleanup_old_assignments(cls, days: int = 30) -> int:
        """清理旧的分配记录"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            sql = f"""
            DELETE FROM {cls.TABLE}
            WHERE status IN ('completed', 'failed', 'timeout')
              AND completed_at < %s
            """
            
            conn = mysql_pool.get_conn()
            try:
                cursor = conn.cursor()
                cursor.execute(sql, (cutoff_date,))
                deleted_count = cursor.rowcount
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_count} old assignment records")
                return deleted_count
            finally:
                cursor.close()
                conn.close()
            
        except Exception as e:
            logger.exception(f"Failed to cleanup old assignments: error={e}")
            return 0