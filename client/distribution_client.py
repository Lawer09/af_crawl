from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.distribution_config import get_distribution_config, DistributionConfig

logger = logging.getLogger(__name__)


class DistributionClient:
    """分布式系统客户端"""
    
    def __init__(self, config: Optional[DistributionConfig] = None):
        self.config = config or get_distribution_config()
        self.session = self._create_session()
        self.base_url = self.config.get_master_url()
        self.device_id = self.config.device_id
        
        # 连接状态
        self.connected = False
        self.last_heartbeat = None
        self.connection_errors = 0
        self.max_connection_errors = 5
        
    def _create_session(self) -> requests.Session:
        """创建HTTP会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置超时
        session.timeout = 30
        
        # 设置API密钥
        if self.config.api_key:
            session.headers.update({"Authorization": f"Bearer {self.config.api_key}"})
        
        return session
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """发起HTTP请求"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            
            # 重置连接错误计数
            self.connection_errors = 0
            self.connected = True
            
            if response.content:
                return response.json()
            else:
                return {"status": "success"}
                
        except requests.exceptions.RequestException as e:
            self.connection_errors += 1
            
            if self.connection_errors >= self.max_connection_errors:
                self.connected = False
            
            logger.error(f"Request failed: {method} {url}, error: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error in request: {e}")
            return None
    
    # 设备管理方法
    def register_device(self, device_name: str = None, device_type: str = None, 
                       ip_address: str = None, capabilities: Dict = None) -> bool:
        """注册设备"""
        if not self.device_id:
            logger.error("Device ID not configured")
            return False
        
        data = {
            "device_id": self.device_id,
            "device_name": device_name or self.config.device_name or self.device_id,
            "device_type": device_type or self.config.device_type,
            "ip_address": ip_address,
            "capabilities": capabilities or self.config.get_device_capabilities()
        }
        
        result = self._make_request("POST", "/devices/register", json=data)
        
        if result and result.get("status") == "success":
            logger.info(f"Device {self.device_id} registered successfully")
            return True
        else:
            logger.error(f"Failed to register device {self.device_id}")
            return False
    
    def send_heartbeat(self, system_metrics: Dict = None) -> bool:
        """发送心跳"""
        if not self.device_id:
            return False
        
        # 获取系统指标
        if system_metrics is None:
            system_metrics = self._get_system_metrics()
        
        data = {
            "device_id": self.device_id,
            **system_metrics
        }
        
        result = self._make_request("POST", f"/devices/{self.device_id}/heartbeat", json=data)
        
        if result and result.get("status") == "success":
            self.last_heartbeat = datetime.now()
            return True
        else:
            return False
    
    def update_device_status(self, status: str) -> bool:
        """更新设备状态"""
        if not self.device_id:
            return False
        
        result = self._make_request("PUT", f"/devices/{self.device_id}/status", json=status)
        
        return result and result.get("status") == "success"
    
    def get_device_info(self, device_id: str = None) -> Optional[Dict]:
        """获取设备信息"""
        target_device_id = device_id or self.device_id
        if not target_device_id:
            return None
        
        return self._make_request("GET", f"/devices/{target_device_id}")
    
    def get_all_devices(self, status: str = None) -> List[Dict]:
        """获取所有设备"""
        params = {"status": status} if status else {}
        result = self._make_request("GET", "/devices", params=params)
        
        if result:
            return result.get("devices", [])
        else:
            return []
    
    # 任务管理方法
    def create_task(self, task_type: str, username: str, app_id: str = None,
                   start_date: str = None, end_date: str = None, priority: int = 0,
                   task_data: Dict = None, execution_timeout: int = None,
                   max_retry_count: int = None) -> bool:
        """创建任务"""
        data = {
            "task_type": task_type,
            "username": username,
            "app_id": app_id,
            "start_date": start_date,
            "end_date": end_date,
            "priority": priority,
            "task_data": task_data,
            "execution_timeout": execution_timeout or self.config.default_task_timeout,
            "max_retry_count": max_retry_count or self.config.max_retry_count
        }
        
        result = self._make_request("POST", "/tasks", json=data)
        
        return result and result.get("status") == "success"
    
    def pull_tasks(self, limit: int = None) -> List[Dict]:
        """拉取任务"""
        if not self.device_id:
            return []
        
        limit = limit or self.config.task_pull_limit
        params = {"limit": limit}
        
        result = self._make_request("GET", f"/tasks/{self.device_id}/pull", params=params)
        
        if result:
            return result.get("tasks", [])
        else:
            return []
    
    def update_task_status(self, task_id: int, status: str, error_message: str = None,
                          result_data: Dict = None) -> bool:
        """更新任务状态"""
        if not self.device_id:
            return False
        
        data = {
            "task_id": task_id,
            "device_id": self.device_id,
            "status": status,
            "error_message": error_message,
            "result_data": result_data
        }
        
        result = self._make_request("PUT", "/tasks/status", json=data)
        
        return result and result.get("status") == "success"
    
    def get_tasks(self, status: str = None, task_type: str = None, 
                 device_id: str = None, limit: int = 100) -> List[Dict]:
        """获取任务列表"""
        params = {
            "status": status,
            "task_type": task_type,
            "device_id": device_id,
            "limit": limit
        }
        
        # 移除None值
        params = {k: v for k, v in params.items() if v is not None}
        
        result = self._make_request("GET", "/tasks", params=params)
        
        if result:
            return result.get("tasks", [])
        else:
            return []
    
    def get_task_details(self, task_id: int) -> Optional[Dict]:
        """获取任务详情"""
        return self._make_request("GET", f"/tasks/{task_id}")
    
    def assign_task(self, task_id: int, device_id: str) -> bool:
        """手动分配任务"""
        data = {
            "task_id": task_id,
            "device_id": device_id
        }
        
        result = self._make_request("POST", "/tasks/assign", json=data)
        
        return result and result.get("status") == "success"
    
    # 统计和监控方法
    def get_system_overview(self) -> Optional[Dict]:
        """获取系统概览"""
        return self._make_request("GET", "/stats/overview")
    
    def get_device_stats(self) -> Optional[Dict]:
        """获取设备统计"""
        return self._make_request("GET", "/stats/devices")
    
    def get_device_performance(self, device_id: str = None, hours: int = 24) -> Optional[Dict]:
        """获取设备性能统计"""
        target_device_id = device_id or self.device_id
        if not target_device_id:
            return None
        
        params = {"hours": hours}
        return self._make_request("GET", f"/stats/performance/{target_device_id}", params=params)
    
    # 管理方法
    def rebalance_tasks(self) -> Optional[Dict]:
        """重新平衡任务"""
        return self._make_request("POST", "/management/rebalance")
    
    def cleanup_old_data(self, days: int = 7) -> Optional[Dict]:
        """清理旧数据"""
        return self._make_request("POST", "/management/cleanup", json=days)
    
    def get_scheduler_status(self) -> Optional[Dict]:
        """获取调度器状态"""
        return self._make_request("GET", "/management/scheduler/status")
    
    # 辅助方法
    def _get_system_metrics(self) -> Dict:
        """获取系统指标"""
        try:
            import psutil
            
            # CPU使用率
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # 内存使用率
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # 磁盘使用率
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            
            # 网络状态（简化版）
            network_status = "good"  # 可以根据需要实现更复杂的网络检测
            
            # 系统负载
            try:
                system_load = psutil.getloadavg()[0]  # 1分钟平均负载
            except AttributeError:
                # Windows系统没有getloadavg
                system_load = cpu_usage / 100.0
            
            return {
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "disk_usage": disk_usage,
                "network_status": network_status,
                "running_tasks": 0,  # 需要从任务执行器获取
                "system_load": system_load,
                "error_count": self.connection_errors,
                "status_info": {
                    "connected": self.connected,
                    "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None
                }
            }
            
        except ImportError:
            logger.warning("psutil not available, using default metrics")
            return {
                "cpu_usage": 0.0,
                "memory_usage": 0.0,
                "disk_usage": 0.0,
                "network_status": "unknown",
                "running_tasks": 0,
                "system_load": 0.0,
                "error_count": self.connection_errors,
                "status_info": {
                    "connected": self.connected,
                    "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None
                }
            }
        except Exception as e:
            logger.exception(f"Error getting system metrics: {e}")
            return {
                "cpu_usage": 0.0,
                "memory_usage": 0.0,
                "disk_usage": 0.0,
                "network_status": "error",
                "running_tasks": 0,
                "system_load": 0.0,
                "error_count": self.connection_errors,
                "status_info": {
                    "connected": False,
                    "error": str(e)
                }
            }
    
    def is_connected(self) -> bool:
        """检查是否连接到主节点"""
        return self.connected
    
    def test_connection(self) -> bool:
        """测试连接"""
        try:
            result = self._make_request("GET", "/stats/overview")
            return result is not None
        except Exception:
            return False
    
    def close(self):
        """关闭客户端"""
        if self.session:
            self.session.close()


class AsyncDistributionClient:
    """异步分布式系统客户端"""
    
    def __init__(self, config: Optional[DistributionConfig] = None):
        self.config = config or get_distribution_config()
        self.base_url = self.config.get_master_url()
        self.device_id = self.config.device_id
        
        # 连接状态
        self.connected = False
        self.last_heartbeat = None
        self.connection_errors = 0
        self.max_connection_errors = 5
        
        # 心跳任务
        self._heartbeat_task = None
        self._running = False
    
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """发起异步HTTP请求"""
        import aiohttp
        
        url = f"{self.base_url}{endpoint}"
        headers = {}
        
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    
                    # 重置连接错误计数
                    self.connection_errors = 0
                    self.connected = True
                    
                    if response.content_length and response.content_length > 0:
                        return await response.json()
                    else:
                        return {"status": "success"}
                        
        except Exception as e:
            self.connection_errors += 1
            
            if self.connection_errors >= self.max_connection_errors:
                self.connected = False
            
            logger.error(f"Async request failed: {method} {url}, error: {e}")
            return None
    
    async def start_heartbeat(self):
        """启动心跳任务"""
        if self._heartbeat_task and not self._heartbeat_task.done():
            return
        
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    async def stop_heartbeat(self):
        """停止心跳任务"""
        self._running = False
        
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
    
    async def _heartbeat_loop(self):
        """心跳循环"""
        while self._running:
            try:
                # 发送心跳
                await self.send_heartbeat()
                
                # 等待下次心跳
                await asyncio.sleep(self.config.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(5)  # 错误时短暂等待
    
    async def register_device(self, device_name: str = None, device_type: str = None,
                             ip_address: str = None, capabilities: Dict = None) -> bool:
        """异步注册设备"""
        if not self.device_id:
            logger.error("Device ID not configured")
            return False
        
        data = {
            "device_id": self.device_id,
            "device_name": device_name or self.config.device_name or self.device_id,
            "device_type": device_type or self.config.device_type,
            "ip_address": ip_address,
            "capabilities": capabilities or self.config.get_device_capabilities()
        }
        
        result = await self._make_request("POST", "/devices/register", json=data)
        
        if result and result.get("status") == "success":
            logger.info(f"Device {self.device_id} registered successfully")
            return True
        else:
            logger.error(f"Failed to register device {self.device_id}")
            return False
    
    async def send_heartbeat(self, system_metrics: Dict = None) -> bool:
        """异步发送心跳"""
        if not self.device_id:
            return False
        
        # 获取系统指标
        if system_metrics is None:
            system_metrics = self._get_system_metrics()
        
        data = {
            "device_id": self.device_id,
            **system_metrics
        }
        
        result = await self._make_request("POST", f"/devices/{self.device_id}/heartbeat", json=data)
        
        if result and result.get("status") == "success":
            self.last_heartbeat = datetime.now()
            return True
        else:
            return False
    
    async def update_device_status(self, status: str) -> bool:
        """异步更新设备状态"""
        if not self.device_id:
            return False
        
        result = await self._make_request("PUT", f"/devices/{self.device_id}/status", json=status)
        
        return result and result.get("status") == "success"
    
    async def get_device_info(self, device_id: str = None) -> Optional[Dict]:
        """异步获取设备信息"""
        target_device_id = device_id or self.device_id
        if not target_device_id:
            return None
        
        return await self._make_request("GET", f"/devices/{target_device_id}")
    
    async def get_all_devices(self, status: str = None) -> List[Dict]:
        """异步获取所有设备"""
        params = {"status": status} if status else {}
        result = await self._make_request("GET", "/devices", params=params)
        
        if result:
            return result.get("devices", [])
        else:
            return []
    
    async def create_task(self, task_type: str, username: str, app_id: str = None,
                         start_date: str = None, end_date: str = None, priority: int = 0,
                         task_data: Dict = None, execution_timeout: int = None,
                         max_retry_count: int = None) -> bool:
        """异步创建任务"""
        data = {
            "task_type": task_type,
            "username": username,
            "app_id": app_id,
            "start_date": start_date,
            "end_date": end_date,
            "priority": priority,
            "task_data": task_data,
            "execution_timeout": execution_timeout or self.config.default_task_timeout,
            "max_retry_count": max_retry_count or self.config.max_retry_count
        }
        
        result = await self._make_request("POST", "/tasks", json=data)
        
        return result and result.get("status") == "success"
    
    async def get_tasks(self, status: str = None, task_type: str = None, 
                       device_id: str = None, limit: int = 100) -> List[Dict]:
        """异步获取任务列表"""
        params = {
            "status": status,
            "task_type": task_type,
            "device_id": device_id,
            "limit": limit
        }
        
        # 移除None值
        params = {k: v for k, v in params.items() if v is not None}
        
        result = await self._make_request("GET", "/tasks", params=params)
        
        if result:
            return result.get("tasks", [])
        else:
            return []
    
    async def get_task_details(self, task_id: int) -> Optional[Dict]:
        """异步获取任务详情"""
        return await self._make_request("GET", f"/tasks/{task_id}")
    
    async def assign_task(self, task_id: int, device_id: str) -> bool:
        """异步手动分配任务"""
        data = {
            "task_id": task_id,
            "device_id": device_id
        }
        
        result = await self._make_request("POST", "/tasks/assign", json=data)
        
        return result and result.get("status") == "success"
    
    async def get_system_overview(self) -> Optional[Dict]:
        """异步获取系统概览"""
        return await self._make_request("GET", "/stats/overview")
    
    async def get_device_stats(self) -> Optional[Dict]:
        """异步获取设备统计"""
        return await self._make_request("GET", "/stats/devices")
    
    async def get_device_performance(self, device_id: str = None, hours: int = 24) -> Optional[Dict]:
        """异步获取设备性能数据"""
        target_device_id = device_id or self.device_id
        if not target_device_id:
            return None
        
        params = {"hours": hours}
        return await self._make_request("GET", f"/stats/devices/{target_device_id}/performance", params=params)
    
    async def rebalance_tasks(self) -> Optional[Dict]:
        """异步重新平衡任务"""
        return await self._make_request("POST", "/management/rebalance")
    
    async def cleanup_old_data(self, days: int = 7) -> Optional[Dict]:
        """异步清理旧数据"""
        return await self._make_request("POST", "/management/cleanup", json=days)
    
    async def get_scheduler_status(self) -> Optional[Dict]:
        """异步获取调度器状态"""
        return await self._make_request("GET", "/management/scheduler/status")
    
    def is_connected(self) -> bool:
        """检查是否连接到主节点"""
        return self.connected
    
    async def close(self):
        """关闭异步客户端"""
        await self.stop_heartbeat()
    
    async def pull_tasks(self, limit: int = None) -> List[Dict]:
        """异步拉取任务"""
        if not self.device_id:
            return []
        
        limit = limit or self.config.task_pull_limit
        params = {"limit": limit}
        
        result = await self._make_request("GET", f"/tasks/{self.device_id}/pull", params=params)
        
        if result:
            return result.get("tasks", [])
        else:
            return []
    
    async def update_task_status(self, task_id: int, status: str, error_message: str = None,
                                result_data: Dict = None) -> bool:
        """异步更新任务状态"""
        if not self.device_id:
            return False
        
        data = {
            "task_id": task_id,
            "device_id": self.device_id,
            "status": status,
            "error_message": error_message,
            "result_data": result_data
        }
        
        result = await self._make_request("PUT", "/tasks/status", json=data)
        
        return result and result.get("status") == "success"
    
    def _get_system_metrics(self) -> Dict:
        """获取系统指标（同步版本）"""
        # 复用同步客户端的实现
        client = DistributionClient(self.config)
        return client._get_system_metrics()
    
    async def test_connection(self) -> bool:
        """异步测试连接"""
        try:
            result = await self._make_request("GET", "/stats/overview")
            return result is not None
        except Exception:
            return False


# 全局客户端实例
_distribution_client: Optional[DistributionClient] = None
_async_distribution_client: Optional[AsyncDistributionClient] = None


def get_distribution_client() -> DistributionClient:
    """获取分布式客户端"""
    global _distribution_client
    
    if _distribution_client is None:
        _distribution_client = DistributionClient()
    
    return _distribution_client


def get_async_distribution_client() -> AsyncDistributionClient:
    """获取异步分布式客户端"""
    global _async_distribution_client
    
    if _async_distribution_client is None:
        _async_distribution_client = AsyncDistributionClient()
    
    return _async_distribution_client


def set_distribution_client(client: DistributionClient) -> None:
    """设置分布式客户端"""
    global _distribution_client
    _distribution_client = client


def set_async_distribution_client(client: AsyncDistributionClient) -> None:
    """设置异步分布式客户端"""
    global _async_distribution_client
    _async_distribution_client = client