from __future__ import annotations

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class DeviceDAO:
    """设备管理数据访问对象"""
    
    TABLE = "cl_device"
    
    @classmethod
    def init_table(cls):
        """初始化设备表"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {cls.TABLE} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            device_id VARCHAR(64) UNIQUE NOT NULL COMMENT '设备唯一标识',
            device_name VARCHAR(128) NOT NULL COMMENT '设备名称',
            device_type ENUM('master', 'worker') NOT NULL COMMENT '设备类型',
            ip_address VARCHAR(45) NOT NULL COMMENT 'IP地址',
            port INT NOT NULL COMMENT '端口号',
            status ENUM('online', 'offline', 'busy') DEFAULT 'offline' COMMENT '设备状态',
            capabilities JSON COMMENT '设备能力配置',
            max_concurrent_tasks INT DEFAULT 5 COMMENT '最大并发任务数',
            current_tasks INT DEFAULT 0 COMMENT '当前任务数',
            last_heartbeat DATETIME COMMENT '最后心跳时间',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_device_type (device_type),
            INDEX idx_status (status),
            INDEX idx_last_heartbeat (last_heartbeat)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备注册表'
        """
        mysql_pool.execute(sql)
        logger.info(f"Table {cls.TABLE} initialized")
    
    @classmethod
    def register_device(cls, device_info: Dict) -> bool:
        """注册设备"""
        try:
            sql = f"""
            INSERT INTO {cls.TABLE} (
                device_id, device_name, device_type, ip_address, port,
                capabilities, max_concurrent_tasks, status, last_heartbeat
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'online', NOW())
            ON DUPLICATE KEY UPDATE
                device_name = VALUES(device_name),
                device_type = VALUES(device_type),
                ip_address = VALUES(ip_address),
                port = VALUES(port),
                capabilities = VALUES(capabilities),
                max_concurrent_tasks = VALUES(max_concurrent_tasks),
                status = 'online',
                last_heartbeat = NOW(),
                updated_at = NOW()
            """
            
            capabilities_json = json.dumps(device_info.get('capabilities', {}))
            
            mysql_pool.execute(sql, (
                device_info['device_id'],
                device_info['device_name'],
                device_info['device_type'],
                device_info['ip_address'],
                device_info['port'],
                capabilities_json,
                device_info.get('max_concurrent_tasks', 5)
            ))
            
            logger.info(f"Device registered: {device_info['device_id']}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to register device {device_info.get('device_id')}: {e}")
            return False
    
    @classmethod
    def update_status(cls, device_id: str, status: str) -> bool:
        """更新设备状态"""
        try:
            sql = f"UPDATE {cls.TABLE} SET status = %s, updated_at = NOW() WHERE device_id = %s"
            mysql_pool.execute(sql, (status, device_id))
            return True
        except Exception as e:
            logger.exception(f"Failed to update device status {device_id}: {e}")
            return False
    
    @classmethod
    def update_heartbeat(cls, device_id: str, system_info: Optional[Dict] = None) -> bool:
        """更新设备心跳"""
        try:
            if system_info:
                sql = f"""
                UPDATE {cls.TABLE} 
                SET last_heartbeat = NOW(), 
                    current_tasks = %s,
                    status = 'online',
                    updated_at = NOW()
                WHERE device_id = %s
                """
                mysql_pool.execute(sql, (system_info.get('active_tasks', 0), device_id))
            else:
                sql = f"""
                UPDATE {cls.TABLE} 
                SET last_heartbeat = NOW(), 
                    status = 'online',
                    updated_at = NOW()
                WHERE device_id = %s
                """
                mysql_pool.execute(sql, (device_id,))
            return True
        except Exception as e:
            logger.exception(f"Failed to update heartbeat for device {device_id}: {e}")
            return False
    
    @classmethod
    def get_available_devices(cls) -> List[Dict]:
        """获取可用设备列表"""
        try:
            sql = f"""
            SELECT device_id, device_name, device_type, ip_address, port,
                   capabilities, max_concurrent_tasks, current_tasks, status,
                   last_heartbeat
            FROM {cls.TABLE}
            WHERE status IN ('online', 'busy') 
              AND device_type = 'worker'
              AND current_tasks < max_concurrent_tasks
              AND last_heartbeat > DATE_SUB(NOW(), INTERVAL 120 SECOND)
            ORDER BY current_tasks ASC, last_heartbeat DESC
            """
            
            devices = mysql_pool.select(sql)
            
            # 解析 capabilities JSON
            for device in devices:
                if device['capabilities']:
                    try:
                        device['capabilities'] = json.loads(device['capabilities'])
                    except json.JSONDecodeError:
                        device['capabilities'] = {}
                else:
                    device['capabilities'] = {}
            
            return devices
            
        except Exception as e:
            logger.exception(f"Failed to get available devices: {e}")
            return []
    
    @classmethod
    def get_timeout_devices(cls, current_time: datetime, timeout_seconds: int) -> List[Dict]:
        """获取超时的设备"""
        try:
            timeout_time = current_time - timedelta(seconds=timeout_seconds)
            sql = f"""
            SELECT device_id, device_name, status, last_heartbeat
            FROM {cls.TABLE}
            WHERE status = 'online'
              AND (last_heartbeat IS NULL OR last_heartbeat < %s)
            """
            return mysql_pool.select(sql, (timeout_time,))
        except Exception as e:
            logger.exception(f"Failed to get timeout devices: {e}")
            return []
    
    @classmethod
    def increment_task_count(cls, device_id: str) -> bool:
        """增加设备任务计数"""
        try:
            sql = f"""
            UPDATE {cls.TABLE} 
            SET current_tasks = current_tasks + 1,
                status = CASE 
                    WHEN current_tasks + 1 >= max_concurrent_tasks THEN 'busy'
                    ELSE 'online'
                END,
                updated_at = NOW()
            WHERE device_id = %s
            """
            mysql_pool.execute(sql, (device_id,))
            return True
        except Exception as e:
            logger.exception(f"Failed to increment task count for device {device_id}: {e}")
            return False
    
    @classmethod
    def decrement_task_count(cls, device_id: str) -> bool:
        """减少设备任务计数"""
        try:
            sql = f"""
            UPDATE {cls.TABLE} 
            SET current_tasks = GREATEST(current_tasks - 1, 0),
                status = CASE 
                    WHEN GREATEST(current_tasks - 1, 0) < max_concurrent_tasks THEN 'online'
                    ELSE status
                END,
                updated_at = NOW()
            WHERE device_id = %s
            """
            mysql_pool.execute(sql, (device_id,))
            return True
        except Exception as e:
            logger.exception(f"Failed to decrement task count for device {device_id}: {e}")
            return False
    
    @classmethod
    def get_device_info(cls, device_id: str) -> Optional[Dict]:
        """获取设备信息"""
        try:
            sql = f"""
            SELECT device_id, device_name, device_type, ip_address, port,
                   capabilities, max_concurrent_tasks, current_tasks, status,
                   last_heartbeat, created_at, updated_at
            FROM {cls.TABLE}
            WHERE device_id = %s
            """
            
            result = mysql_pool.fetch_one(sql, (device_id,))
            if result and result['capabilities']:
                try:
                    result['capabilities'] = json.loads(result['capabilities'])
                except json.JSONDecodeError:
                    result['capabilities'] = {}
            
            return result
            
        except Exception as e:
            logger.exception(f"Failed to get device info for {device_id}: {e}")
            return None
    
    @classmethod
    def get_all_devices(cls) -> List[Dict]:
        """获取所有设备列表"""
        try:
            sql = f"""
            SELECT device_id, device_name, device_type, ip_address, port,
                   capabilities, max_concurrent_tasks, current_tasks, status,
                   last_heartbeat, created_at, updated_at
            FROM {cls.TABLE}
            ORDER BY device_type, device_name
            """
            
            devices = mysql_pool.select(sql)
            
            # 解析 capabilities JSON
            for device in devices:
                if device['capabilities']:
                    try:
                        device['capabilities'] = json.loads(device['capabilities'])
                    except json.JSONDecodeError:
                        device['capabilities'] = {}
                else:
                    device['capabilities'] = {}
            
            return devices
            
        except Exception as e:
            logger.exception(f"Failed to get all devices: {e}")
            return []
    
    @classmethod
    def remove_device(cls, device_id: str) -> bool:
        """移除设备"""
        try:
            sql = f"DELETE FROM {cls.TABLE} WHERE device_id = %s"
            mysql_pool.execute(sql, (device_id,))
            logger.info(f"Device removed: {device_id}")
            return True
        except Exception as e:
            logger.exception(f"Failed to remove device {device_id}: {e}")
            return False