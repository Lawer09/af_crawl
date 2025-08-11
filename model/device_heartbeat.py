from __future__ import annotations

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from model.device import DeviceDAO

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class DeviceHeartbeatDAO:
    """设备心跳数据访问对象"""
    
    TABLE = "cl_device_heartbeat"
    
    @classmethod
    def init_table(cls):
        """初始化设备心跳表"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {cls.TABLE} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            device_id VARCHAR(64) NOT NULL COMMENT '设备ID',
            heartbeat_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '心跳时间',
            cpu_usage DECIMAL(5,2) COMMENT 'CPU使用率(%)',
            memory_usage DECIMAL(5,2) COMMENT '内存使用率(%)',
            disk_usage DECIMAL(5,2) COMMENT '磁盘使用率(%)',
            network_status ENUM('good', 'slow', 'poor') DEFAULT 'good' COMMENT '网络状态',
            running_tasks INT DEFAULT 0 COMMENT '正在运行的任务数',
            system_load DECIMAL(5,2) COMMENT '系统负载',
            error_count INT DEFAULT 0 COMMENT '错误计数',
            status_info JSON COMMENT '状态详细信息',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_device_id (device_id),
            INDEX idx_heartbeat_time (heartbeat_time),
            INDEX idx_device_heartbeat (device_id, heartbeat_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备心跳表'
        """
        mysql_pool.execute(sql)
        logger.info(f"Table {cls.TABLE} initialized")
    
    @classmethod
    def record_heartbeat(cls, device_id: str, cpu_usage: Optional[float] = None,
                        memory_usage: Optional[float] = None, disk_usage: Optional[float] = None,
                        network_status: str = 'good', running_tasks: int = 0,
                        system_load: Optional[float] = None, error_count: int = 0,
                        status_info: Optional[Dict] = None) -> bool:
        """记录设备心跳"""
        try:
            import json
            
            status_json = json.dumps(status_info) if status_info else None
            
            sql = f"""
            INSERT INTO {cls.TABLE} 
            (device_id, heartbeat_time, cpu_usage, memory_usage, disk_usage, 
             network_status, running_tasks, system_load, error_count, status_info)
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            mysql_pool.execute(sql, (
                device_id, cpu_usage, memory_usage, disk_usage,
                network_status, running_tasks, system_load, error_count, status_json
            ))
            
            logger.debug(f"Heartbeat recorded for device: {device_id}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to record heartbeat: device_id={device_id}, error={e}")
            return False
    
    @classmethod
    def get_latest_heartbeat(cls, device_id: str) -> Optional[Dict]:
        """获取设备最新心跳"""
        try:
            sql = f"""
            SELECT device_id, heartbeat_time, cpu_usage, memory_usage, disk_usage,
                   network_status, running_tasks, system_load, error_count, status_info
            FROM {cls.TABLE}
            WHERE device_id = %s
            ORDER BY heartbeat_time DESC
            LIMIT 1
            """
            return mysql_pool.fetch_one(sql, (device_id,))
        except Exception as e:
            logger.exception(f"Failed to get latest heartbeat: device_id={device_id}, error={e}")
            return None
    
    @classmethod
    def get_device_heartbeats(cls, device_id: str, hours: int = 24) -> List[Dict]:
        """获取设备指定时间内的心跳记录"""
        try:
            start_time = datetime.now() - timedelta(hours=hours)
            sql = f"""
            SELECT device_id, heartbeat_time, cpu_usage, memory_usage, disk_usage,
                   network_status, running_tasks, system_load, error_count
            FROM {cls.TABLE}
            WHERE device_id = %s AND heartbeat_time >= %s
            ORDER BY heartbeat_time DESC
            """
            return mysql_pool.select(sql, (device_id, start_time))
        except Exception as e:
            logger.exception(f"Failed to get device heartbeats: device_id={device_id}, error={e}")
            return []
    
    @classmethod
    def get_all_latest_heartbeats(cls) -> List[Dict]:
        """获取所有设备的最新心跳"""
        try:
            sql = f"""
            SELECT h1.device_id, h1.heartbeat_time, h1.cpu_usage, h1.memory_usage, 
                   h1.disk_usage, h1.network_status, h1.running_tasks, h1.system_load, 
                   h1.error_count, d.device_name, d.device_type, d.status as device_status
            FROM {cls.TABLE} h1
            INNER JOIN (
                SELECT device_id, MAX(heartbeat_time) as max_time
                FROM {cls.TABLE}
                GROUP BY device_id
            ) h2 ON h1.device_id = h2.device_id AND h1.heartbeat_time = h2.max_time
            LEFT JOIN {DeviceDAO.TABLE} d ON h1.device_id = d.device_id
            ORDER BY h1.heartbeat_time DESC
            """
            return mysql_pool.select(sql)
        except Exception as e:
            logger.exception(f"Failed to get all latest heartbeats: error={e}")
            return []
    
    @classmethod
    def get_offline_devices(cls, timeout_minutes: int = 5) -> List[str]:
        """获取离线设备列表"""
        try:
            timeout_time = datetime.now() - timedelta(minutes=timeout_minutes)
            
            # 获取所有注册的设备
            all_devices_sql = "SELECT device_id FROM cl_device WHERE status = 'online'"
            all_devices = mysql_pool.select(all_devices_sql)
            all_device_ids = [d['device_id'] for d in all_devices]
            
            if not all_device_ids:
                return []
            
            # 获取有最近心跳的设备
            placeholders = ','.join(['%s'] * len(all_device_ids))
            recent_heartbeat_sql = f"""
            SELECT DISTINCT device_id
            FROM {cls.TABLE}
            WHERE device_id IN ({placeholders})
              AND heartbeat_time > %s
            """
            
            params = all_device_ids + [timeout_time]
            recent_devices = mysql_pool.select(recent_heartbeat_sql, params)
            recent_device_ids = [d['device_id'] for d in recent_devices]
            
            # 返回没有最近心跳的设备
            offline_devices = [device_id for device_id in all_device_ids 
                             if device_id not in recent_device_ids]
            
            return offline_devices
            
        except Exception as e:
            logger.exception(f"Failed to get offline devices: error={e}")
            return []
    
    @classmethod
    def get_device_health_stats(cls, device_id: str, hours: int = 24) -> Dict:
        """获取设备健康统计"""
        try:
            start_time = datetime.now() - timedelta(hours=hours)
            sql = f"""
            SELECT 
                COUNT(*) as heartbeat_count,
                AVG(cpu_usage) as avg_cpu,
                MAX(cpu_usage) as max_cpu,
                AVG(memory_usage) as avg_memory,
                MAX(memory_usage) as max_memory,
                AVG(disk_usage) as avg_disk,
                MAX(disk_usage) as max_disk,
                AVG(system_load) as avg_load,
                MAX(system_load) as max_load,
                SUM(error_count) as total_errors,
                AVG(running_tasks) as avg_tasks,
                MAX(running_tasks) as max_tasks
            FROM {cls.TABLE}
            WHERE device_id = %s AND heartbeat_time >= %s
            """
            
            result = mysql_pool.fetch_one(sql, (device_id, start_time))
            if result:
                stats = dict(result)
                
                # 计算健康评分 (0-100)
                health_score = 100
                if stats['avg_cpu'] and stats['avg_cpu'] > 80:
                    health_score -= 20
                elif stats['avg_cpu'] and stats['avg_cpu'] > 60:
                    health_score -= 10
                    
                if stats['avg_memory'] and stats['avg_memory'] > 90:
                    health_score -= 20
                elif stats['avg_memory'] and stats['avg_memory'] > 70:
                    health_score -= 10
                    
                if stats['total_errors'] and stats['total_errors'] > 10:
                    health_score -= 30
                elif stats['total_errors'] and stats['total_errors'] > 5:
                    health_score -= 15
                
                stats['health_score'] = max(0, health_score)
                return stats
            else:
                return {
                    'heartbeat_count': 0,
                    'health_score': 0
                }
                
        except Exception as e:
            logger.exception(f"Failed to get device health stats: device_id={device_id}, error={e}")
            return {}
    
    @classmethod
    def get_system_overview(cls, hours: int = 1) -> Dict:
        """获取系统概览统计"""
        try:
            start_time = datetime.now() - timedelta(hours=hours)
            sql = f"""
            SELECT 
                COUNT(DISTINCT device_id) as active_devices,
                AVG(cpu_usage) as avg_cpu,
                AVG(memory_usage) as avg_memory,
                AVG(disk_usage) as avg_disk,
                SUM(running_tasks) as total_running_tasks,
                SUM(error_count) as total_errors
            FROM {cls.TABLE}
            WHERE heartbeat_time >= %s
            """
            
            result = mysql_pool.fetch_one(sql, (start_time,))
            return dict(result) if result else {}
            
        except Exception as e:
            logger.exception(f"Failed to get system overview: error={e}")
            return {}
    
    @classmethod
    def cleanup_old_heartbeats(cls, days: int = 7) -> int:
        """清理旧的心跳记录"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            sql = f"""
            DELETE FROM {cls.TABLE}
            WHERE heartbeat_time < %s
            """
            
            conn = mysql_pool.get_conn()
            try:
                cursor = conn.cursor()
                cursor.execute(sql, (cutoff_date,))
                deleted_count = cursor.rowcount
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_count} old heartbeat records")
                return deleted_count
            finally:
                cursor.close()
                conn.close()
            
        except Exception as e:
            logger.exception(f"Failed to cleanup old heartbeats: error={e}")
            return 0
    
    @classmethod
    def get_device_uptime_stats(cls, device_id: str, days: int = 7) -> Dict:
        """获取设备在线时间统计"""
        try:
            start_time = datetime.now() - timedelta(days=days)
            
            # 获取心跳记录，按时间排序
            sql = f"""
            SELECT heartbeat_time
            FROM {cls.TABLE}
            WHERE device_id = %s AND heartbeat_time >= %s
            ORDER BY heartbeat_time
            """
            
            heartbeats = mysql_pool.select(sql, (device_id, start_time))
            
            if not heartbeats:
                return {
                    'total_time': days * 24 * 3600,  # 总时间（秒）
                    'online_time': 0,
                    'offline_time': days * 24 * 3600,
                    'uptime_percentage': 0.0,
                    'offline_periods': 0
                }
            
            # 计算在线时间（简化算法：相邻心跳间隔小于10分钟认为在线）
            online_time = 0
            offline_periods = 0
            last_heartbeat = None
            
            for heartbeat in heartbeats:
                current_time = heartbeat['heartbeat_time']
                
                if last_heartbeat:
                    interval = (current_time - last_heartbeat).total_seconds()
                    if interval <= 600:  # 10分钟内认为在线
                        online_time += interval
                    else:
                        offline_periods += 1
                
                last_heartbeat = current_time
            
            total_time = days * 24 * 3600
            offline_time = total_time - online_time
            uptime_percentage = (online_time / total_time) * 100 if total_time > 0 else 0
            
            return {
                'total_time': total_time,
                'online_time': online_time,
                'offline_time': offline_time,
                'uptime_percentage': round(uptime_percentage, 2),
                'offline_periods': offline_periods
            }
            
        except Exception as e:
            logger.exception(f"Failed to get device uptime stats: device_id={device_id}, error={e}")
            return {}