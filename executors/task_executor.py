from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future
import threading

from client.distribution_client import get_distribution_client, get_async_distribution_client
from setting.distribution_config import get_distribution_config

logger = logging.getLogger(__name__)


class TaskExecutor(ABC):
    """任务执行器基类"""
    
    def __init__(self, task_type: str):
        self.task_type = task_type
        self.running = False
        self.current_tasks = {}
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.start_time = None
        
    @abstractmethod
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行任务的具体实现"""
        pass
    
    def can_handle(self, task_type: str) -> bool:
        """检查是否能处理指定类型的任务"""
        return task_type == self.task_type
    
    def get_stats(self) -> Dict[str, Any]:
        """获取执行器统计信息"""
        uptime = time.time() - self.start_time if self.start_time else 0
        
        return {
            "task_type": self.task_type,
            "running": self.running,
            "current_tasks": len(self.current_tasks),
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "uptime_seconds": uptime,
            "success_rate": self.completed_tasks / max(1, self.completed_tasks + self.failed_tasks)
        }


class UserAppsTaskExecutor(TaskExecutor):
    """用户应用任务执行器"""
    
    def __init__(self):
        super().__init__("user_apps")
    
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行用户应用任务"""
        try:
            username = task_data.get('username')
            if not username:
                raise ValueError("Missing username in task data")
            
            logger.info(f"Executing user apps task for user: {username}")
            
            # 导入并执行同步任务
            from tasks.sync_user_apps import sync_user_apps
            
            result = sync_user_apps(username)
            
            return {
                "status": "success",
                "username": username,
                "synced_apps": result.get('synced_apps', 0),
                "execution_time": result.get('execution_time', 0)
            }
            
        except Exception as e:
            logger.exception(f"Error executing user apps task: {e}")
            return {
                "status": "error",
                "error_message": str(e),
                "error_type": type(e).__name__
            }


class AppDataSyncTaskExecutor(TaskExecutor):
    """App数据同步任务执行器"""
    
    def __init__(self):
        super().__init__("app_data")
    
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行数据同步任务"""
        try:
            username = task_data.get('username')
            app_id = task_data.get('app_id')
            start_date = task_data.get('start_date')
            end_date = task_data.get('end_date')
            
            if not username:
                raise ValueError("Missing username in task data")
            
            logger.info(f"Executing data sync task for user: {username}, app: {app_id}")
            
            # 导入并执行同步任务
            from tasks.sync_app_data import sync_app_data
            
            result = sync_app_data(
                username=username,
                app_id=app_id,
                start_date=start_date,
                end_date=end_date
            )
            
            return {
                "status": "success",
                "username": username,
                "app_id": app_id,
                "start_date": start_date,
                "end_date": end_date,
                "synced_records": result.get('synced_records', 0),
                "execution_time": result.get('execution_time', 0)
            }
            
        except Exception as e:
            logger.exception(f"Error executing data sync task: {e}")
            return {
                "status": "error",
                "error_message": str(e),
                "error_type": type(e).__name__
            }


class DistributedTaskExecutor:
    """分布式任务执行器管理器"""
    
    def __init__(self, max_concurrent_tasks: int = 3):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.executors = {}
        self.thread_pool = ThreadPoolExecutor(max_workers=max_concurrent_tasks)
        self.running_tasks = {}
        self.task_lock = threading.Lock()
        
        # 注册默认执行器
        self.register_executor(UserAppsTaskExecutor())
        self.register_executor(AppDataSyncTaskExecutor())
        
        # 分布式客户端
        self.client = None
        self.async_client = None
        
        # 统计信息
        self.total_executed = 0
        self.total_failed = 0
        self.start_time = time.time()
    
    def register_executor(self, executor: TaskExecutor):
        """注册任务执行器"""
        self.executors[executor.task_type] = executor
        logger.info(f"Registered task executor for type: {executor.task_type}")
    
    def get_executor(self, task_type: str) -> Optional[TaskExecutor]:
        """获取任务执行器"""
        return self.executors.get(task_type)
    
    def can_accept_task(self) -> bool:
        """检查是否可以接受新任务"""
        with self.task_lock:
            return len(self.running_tasks) < self.max_concurrent_tasks
    
    def execute_task_sync(self, task_id: int, task_type: str, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """同步执行任务"""
        executor = self.get_executor(task_type)
        if not executor:
            raise ValueError(f"No executor found for task type: {task_type}")
        
        start_time = time.time()
        
        try:
            # 记录任务开始
            with self.task_lock:
                self.running_tasks[task_id] = {
                    "task_type": task_type,
                    "start_time": start_time,
                    "status": "running"
                }
            
            logger.info(f"Starting task {task_id} of type {task_type}")
            
            # 执行任务
            result = executor.execute_task(task_data)
            
            # 计算执行时间
            execution_time = time.time() - start_time
            result["execution_time"] = execution_time
            
            # 更新统计
            if result.get("status") == "success":
                executor.completed_tasks += 1
                self.total_executed += 1
            else:
                executor.failed_tasks += 1
                self.total_failed += 1
            
            logger.info(f"Task {task_id} completed in {execution_time:.2f}s with status: {result.get('status')}")
            
            return result
            
        except Exception as e:
            logger.exception(f"Error executing task {task_id}: {e}")
            
            executor.failed_tasks += 1
            self.total_failed += 1
            
            return {
                "status": "error",
                "error_message": str(e),
                "error_type": type(e).__name__,
                "execution_time": time.time() - start_time
            }
        finally:
            # 移除运行中的任务记录
            with self.task_lock:
                self.running_tasks.pop(task_id, None)
    
    def execute_task_async(self, task_id: int, task_type: str, task_data: Dict[str, Any]) -> Future:
        """异步执行任务"""
        return self.thread_pool.submit(self.execute_task_sync, task_id, task_type, task_data)
    
    def get_running_tasks(self) -> Dict[int, Dict[str, Any]]:
        """获取正在运行的任务"""
        with self.task_lock:
            return self.running_tasks.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取执行器统计信息"""
        uptime = time.time() - self.start_time
        
        executor_stats = {}
        for task_type, executor in self.executors.items():
            executor_stats[task_type] = executor.get_stats()
        
        with self.task_lock:
            current_tasks = len(self.running_tasks)
        
        return {
            "uptime_seconds": uptime,
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "current_running_tasks": current_tasks,
            "total_executed": self.total_executed,
            "total_failed": self.total_failed,
            "success_rate": self.total_executed / max(1, self.total_executed + self.total_failed),
            "executor_stats": executor_stats
        }
    
    def start_distributed_execution(self):
        """启动分布式任务执行"""
        config = get_distribution_config()
        
        if config.mode.value == "worker":
            # 工作节点模式，启动任务拉取循环
            asyncio.create_task(self._task_pull_loop())
        elif config.mode.value == "standalone":
            # 独立模式，启动本地任务处理循环
            asyncio.create_task(self._local_task_loop())
    
    async def _task_pull_loop(self):
        """工作节点任务拉取循环"""
        if not self.async_client:
            self.async_client = get_async_distribution_client()
        
        config = get_distribution_config()
        
        while True:
            try:
                if self.can_accept_task():
                    # 拉取任务
                    tasks = await self.async_client.pull_tasks(limit=1)
                    
                    for task in tasks:
                        task_id = task['id']
                        task_type = task['task_type']
                        task_data = task.get('task_data', {})
                        
                        # 更新任务状态为运行中
                        await self.async_client.update_task_status(task_id, "running")
                        
                        # 异步执行任务
                        future = self.execute_task_async(task_id, task_type, task_data)
                        
                        # 创建任务完成回调
                        asyncio.create_task(self._handle_task_completion(task_id, future))
                
                # 等待下次拉取
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.exception(f"Error in task pull loop: {e}")
                await asyncio.sleep(10)
    
    async def _handle_task_completion(self, task_id: int, future: Future):
        """处理任务完成"""
        try:
            # 等待任务完成
            result = await asyncio.wrap_future(future)
            
            # 更新任务状态
            if result.get("status") == "success":
                await self.async_client.update_task_status(
                    task_id, "completed", result_data=result
                )
            else:
                await self.async_client.update_task_status(
                    task_id, "failed", 
                    error_message=result.get("error_message"),
                    result_data=result
                )
                
        except Exception as e:
            logger.exception(f"Error handling task completion for task {task_id}: {e}")
            
            # 标记任务失败
            try:
                await self.async_client.update_task_status(
                    task_id, "failed", error_message=str(e)
                )
            except Exception:
                pass
    
    async def _local_task_loop(self):
        """独立模式本地任务处理循环"""
        from model.task import TaskDAO
        
        while True:
            try:
                if self.can_accept_task():
                    # 获取待处理任务
                    pending_tasks = TaskDAO.fetch_pending(limit=1)
                    
                    for task in pending_tasks:
                        task_id = task['id']
                        task_type = task['task_type']
                        task_data = {
                            'username': task['username'],
                            'app_id': task.get('app_id'),
                            'start_date': task.get('start_date'),
                            'end_date': task.get('end_date')
                        }
                        
                        # 标记任务为运行中
                        TaskDAO.mark_running(task_id)
                        
                        # 异步执行任务
                        future = self.execute_task_async(task_id, task_type, task_data)
                        
                        # 创建任务完成回调
                        asyncio.create_task(self._handle_local_task_completion(task_id, future))
                
                # 等待下次检查
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.exception(f"Error in local task loop: {e}")
                await asyncio.sleep(30)
    
    async def _handle_local_task_completion(self, task_id: int, future: Future):
        """处理本地任务完成"""
        from model.task import TaskDAO
        
        try:
            # 等待任务完成
            result = await asyncio.wrap_future(future)
            
            # 更新任务状态
            if result.get("status") == "success":
                TaskDAO.mark_done(task_id)
            else:
                # 任务失败，设置重试
                TaskDAO.fail_task(task_id, retry_delay=300)  # 5分钟后重试
                
        except Exception as e:
            logger.exception(f"Error handling local task completion for task {task_id}: {e}")
            
            # 标记任务失败
            try:
                TaskDAO.fail_task(task_id, retry_delay=300)
            except Exception:
                pass
    
    def shutdown(self):
        """关闭执行器"""
        logger.info("Shutting down task executor...")
        
        # 等待所有任务完成
        self.thread_pool.shutdown(wait=True)
        
        logger.info("Task executor shutdown complete")


# 全局任务执行器实例
_task_executor: Optional[DistributedTaskExecutor] = None


def get_task_executor() -> DistributedTaskExecutor:
    """获取任务执行器"""
    global _task_executor
    
    if _task_executor is None:
        config = get_distribution_config()
        _task_executor = DistributedTaskExecutor(max_concurrent_tasks=config.concurrent_tasks)
    
    return _task_executor


def set_task_executor(executor: DistributedTaskExecutor) -> None:
    """设置任务执行器"""
    global _task_executor
    _task_executor = executor