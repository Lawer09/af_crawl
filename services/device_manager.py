from __future__ import annotations

import logging
import threading
import time
import psutil
import socket
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from model.device import DeviceDAO
from model.device_heartbeat import DeviceHeartbeatDAO
from model.crawl_task import CrawlTaskDAO
from model.task_assignment import TaskAssignmentDAO

logger = logging.getLogger(__name__)


class DeviceManager:
    """设备管理器"""
    
    def __init__(self, device_id: Optional[str] = None, device_name: Optional[str] = None,
                 heartbeat_interval: int = 30, offline_timeout: int = 300):
        self.device_id = device_id or self._generate_device_id()
        self.device_name = device_name or socket.gethostname()
        self.heartbeat_interval = heartbeat_interval
        self.offline_timeout = offline_timeout
        self.running = False
        self.heartbeat_thread = None
        self.monitor_thread = None
        self.lock = threading.Lock()
        
        logger.info(f"DeviceManager initialized: device_id={self.device_id}, device_name={self.device_name}")
    
    def _generate_device_id(self) -> str:
        """生成设备ID"""
        import uuid
        hostname = socket.gethostname()
        mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff) for elements in range(0,2*6,2)][::-1])
        return f"{hostname}_{mac}"
    
    def start(self):
        """启动设备管理器"""
        if self.running:
            logger.warning("DeviceManager is already running")
            return
        
        try:
            # 注册设备
            self._register_device()
            
            self.running = True
            
            # 启动心跳线程
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self.heartbeat_thread.start()
            
            # 启动监控线程
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            
            logger.info("DeviceManager started")
            
        except Exception as e:
            logger.exception(f"Failed to start DeviceManager: {e}")
            self.running = False
            raise
    
    def stop(self):
        """停止设备管理器"""
        self.running = False
        
        # 等待线程结束
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        
        # 更新设备状态为离线
        try:
            DeviceDAO.update_status(self.device_id, 'offline')
            logger.info("DeviceManager stopped")
        except Exception as e:
            logger.exception(f"Error updating device status on stop: {e}")
    
    def _register_device(self):
        """注册设备"""
        try:
            device_info = self._get_device_info()
            # 构建完整的设备信息字典
            full_device_info = {
                'device_id': self.device_id,
                'device_name': self.device_name,
                'device_type': device_info['device_type'],
                'ip_address': device_info['ip_address'],
                'port': device_info.get('port', 8080),
                'capabilities': device_info['capabilities'],
                'max_concurrent_tasks': device_info.get('max_concurrent_tasks', 5)
            }
            
            success = DeviceDAO.register_device(full_device_info)
            
            if success:
                logger.info(f"Device registered successfully: {self.device_id}")
            else:
                raise Exception("Failed to register device")
                
        except Exception as e:
            logger.exception(f"Error registering device: {e}")
            raise
    
    def _get_device_info(self) -> Dict:
        """获取设备信息"""
        try:
            # 获取IP地址
            ip_address = self._get_local_ip()
            
            # 获取系统信息
            cpu_count = psutil.cpu_count()
            memory_total = psutil.virtual_memory().total // (1024 * 1024 * 1024)  # GB
            
            # 设备类型判断
            device_type = 'worker'  # 默认为worker
            
            # 设备能力
            capabilities = {
                'cpu_cores': cpu_count,
                'memory_gb': memory_total,
                'platform': psutil.WINDOWS if hasattr(psutil, 'WINDOWS') else 'unknown',
                'python_version': self._get_python_version(),
                'supported_tasks': ['user_apps', 'app_data']  # 支持的任务类型
            }
            
            return {
                'device_type': device_type,
                'ip_address': ip_address,
                'capabilities': capabilities
            }
            
        except Exception as e:
            logger.exception(f"Error getting device info: {e}")
            return {
                'device_type': 'worker',
                'ip_address': '127.0.0.1',
                'capabilities': {}
            }
    
    def _get_local_ip(self) -> str:
        """获取本地IP地址"""
        try:
            # 连接到一个远程地址来获取本地IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                return s.getsockname()[0]
        except Exception:
            return '127.0.0.1'
    
    def _get_python_version(self) -> str:
        """获取Python版本"""
        import sys
        return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    
    def _heartbeat_loop(self):
        """心跳循环"""
        while self.running:
            try:
                self._send_heartbeat()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.exception(f"Error in heartbeat loop: {e}")
                time.sleep(5)
    
    def _send_heartbeat(self):
        """发送心跳"""
        try:
            # 获取系统状态
            system_info = self._get_system_status()
            
            # 获取当前运行的任务数
            running_tasks = len(CrawlTaskDAO.get_device_tasks(self.device_id))
            
            # 记录心跳
            success = DeviceHeartbeatDAO.record_heartbeat(
                device_id=self.device_id,
                cpu_usage=system_info['cpu_usage'],
                memory_usage=system_info['memory_usage'],
                disk_usage=system_info['disk_usage'],
                network_status=system_info['network_status'],
                running_tasks=running_tasks,
                system_load=system_info['system_load'],
                error_count=system_info['error_count'],
                status_info=system_info['status_info']
            )
            
            if success:
                # 更新设备最后心跳时间
                DeviceDAO.update_heartbeat(self.device_id)
                logger.debug(f"Heartbeat sent successfully: {self.device_id}")
            else:
                logger.error(f"Failed to send heartbeat: {self.device_id}")
                
        except Exception as e:
            logger.exception(f"Error sending heartbeat: {e}")
    
    def _get_system_status(self) -> Dict:
        """获取系统状态"""
        try:
            # CPU使用率
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # 内存使用率
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # 磁盘使用率
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            
            # 系统负载（Windows上使用CPU使用率代替）
            system_load = cpu_usage
            
            # 网络状态（简化判断）
            network_status = 'good'  # 可以根据网络延迟等判断
            
            # 错误计数（可以从日志或其他地方获取）
            error_count = 0
            
            # 状态详细信息
            status_info = {
                'boot_time': datetime.fromtimestamp(psutil.boot_time()).isoformat(),
                'process_count': len(psutil.pids()),
                'network_connections': len(psutil.net_connections()),
                'disk_io': dict(psutil.disk_io_counters()._asdict()) if psutil.disk_io_counters() else {},
                'network_io': dict(psutil.net_io_counters()._asdict()) if psutil.net_io_counters() else {}
            }
            
            return {
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'disk_usage': disk_usage,
                'system_load': system_load,
                'network_status': network_status,
                'error_count': error_count,
                'status_info': status_info
            }
            
        except Exception as e:
            logger.exception(f"Error getting system status: {e}")
            return {
                'cpu_usage': 0,
                'memory_usage': 0,
                'disk_usage': 0,
                'system_load': 0,
                'network_status': 'poor',
                'error_count': 1,
                'status_info': {}
            }
    
    def _monitor_loop(self):
        """监控循环"""
        while self.running:
            try:
                self._monitor_offline_devices()
                self._cleanup_old_data()
                time.sleep(60)  # 每60秒检查一次，更快检测离线设备
            except Exception as e:
                logger.exception(f"Error in monitor loop: {e}")
                time.sleep(10)
    
    def _monitor_offline_devices(self):
        """监控离线设备"""
        try:
            offline_devices = DeviceHeartbeatDAO.get_offline_devices(
                timeout_minutes=self.offline_timeout // 60
            )
            
            for device_id in offline_devices:
                logger.warning(f"Device {device_id} is offline")
                
                # 更新设备状态
                DeviceDAO.update_status(device_id, 'offline')
                
                # 释放设备的任务
                released_count = CrawlTaskDAO.release_device_tasks(device_id)
                if released_count > 0:
                    logger.info(f"Released {released_count} tasks from offline device {device_id}")
                
                # 更新任务分配状态
                device_assignments = TaskAssignmentDAO.get_device_running_tasks(device_id)
                for assignment in device_assignments:
                    TaskAssignmentDAO.update_status(
                        assignment['id'], 'failed', 'Device went offline'
                    )
                
                # 重置设备任务计数
                DeviceDAO.update_task_count(device_id, 0)
                
        except Exception as e:
            logger.exception(f"Error monitoring offline devices: {e}")
    
    def _cleanup_old_data(self):
        """清理旧数据"""
        try:
            # 清理旧的心跳记录（保留7天）
            deleted_heartbeats = DeviceHeartbeatDAO.cleanup_old_heartbeats(days=7)
            if deleted_heartbeats > 0:
                logger.info(f"Cleaned up {deleted_heartbeats} old heartbeat records")
            
            # 清理旧的任务分配记录（保留30天）
            deleted_assignments = TaskAssignmentDAO.cleanup_old_assignments(days=30)
            if deleted_assignments > 0:
                logger.info(f"Cleaned up {deleted_assignments} old assignment records")
                
        except Exception as e:
            logger.exception(f"Error cleaning up old data: {e}")
    
    def get_device_status(self) -> Dict:
        """获取设备状态"""
        try:
            # 获取设备信息
            device = DeviceDAO.get_device(self.device_id)
            if not device:
                return {'error': 'Device not found'}
            
            # 获取最新心跳
            latest_heartbeat = DeviceHeartbeatDAO.get_latest_heartbeat(self.device_id)
            
            # 获取设备任务
            device_tasks = CrawlTaskDAO.get_device_tasks(self.device_id)
            
            # 获取健康统计
            health_stats = DeviceHeartbeatDAO.get_device_health_stats(self.device_id)
            
            # 获取在线时间统计
            uptime_stats = DeviceHeartbeatDAO.get_device_uptime_stats(self.device_id)
            
            return {
                'device_info': device,
                'latest_heartbeat': latest_heartbeat,
                'running_tasks': len(device_tasks),
                'task_details': device_tasks,
                'health_stats': health_stats,
                'uptime_stats': uptime_stats,
                'manager_running': self.running
            }
            
        except Exception as e:
            logger.exception(f"Error getting device status: {e}")
            return {'error': str(e)}
    
    def update_device_capabilities(self, capabilities: Dict) -> bool:
        """更新设备能力"""
        try:
            current_device = DeviceDAO.get_device(self.device_id)
            if not current_device:
                return False
            
            # 合并现有能力和新能力
            current_capabilities = current_device.get('capabilities', {})
            if isinstance(current_capabilities, str):
                import json
                current_capabilities = json.loads(current_capabilities)
            
            current_capabilities.update(capabilities)
            
            # 更新设备信息
            return DeviceDAO.update_device_info(
                self.device_id,
                capabilities=current_capabilities
            )
            
        except Exception as e:
            logger.exception(f"Error updating device capabilities: {e}")
            return False
    
    def set_device_type(self, device_type: str) -> bool:
        """设置设备类型"""
        try:
            return DeviceDAO.update_device_info(self.device_id, device_type=device_type)
        except Exception as e:
            logger.exception(f"Error setting device type: {e}")
            return False
    
    def get_performance_metrics(self, hours: int = 24) -> Dict:
        """获取性能指标"""
        try:
            # 获取心跳历史
            heartbeats = DeviceHeartbeatDAO.get_device_heartbeats(self.device_id, hours)
            
            if not heartbeats:
                return {'error': 'No heartbeat data available'}
            
            # 计算性能指标
            cpu_values = [h['cpu_usage'] for h in heartbeats if h['cpu_usage'] is not None]
            memory_values = [h['memory_usage'] for h in heartbeats if h['memory_usage'] is not None]
            
            metrics = {
                'period_hours': hours,
                'heartbeat_count': len(heartbeats),
                'cpu_metrics': {
                    'avg': sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                    'min': min(cpu_values) if cpu_values else 0,
                    'max': max(cpu_values) if cpu_values else 0
                },
                'memory_metrics': {
                    'avg': sum(memory_values) / len(memory_values) if memory_values else 0,
                    'min': min(memory_values) if memory_values else 0,
                    'max': max(memory_values) if memory_values else 0
                },
                'task_metrics': {
                    'avg_running_tasks': sum(h['running_tasks'] for h in heartbeats) / len(heartbeats),
                    'max_running_tasks': max(h['running_tasks'] for h in heartbeats)
                }
            }
            
            return metrics
            
        except Exception as e:
            logger.exception(f"Error getting performance metrics: {e}")
            return {'error': str(e)}