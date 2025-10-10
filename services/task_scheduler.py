from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum

from model.crawl_task import CrawlTaskDAO
from model.device import DeviceDAO
from model.task_assignment import TaskAssignmentDAO
from services.task_dispatcher import TaskDispatcher, LoadBalanceStrategy

logger = logging.getLogger(__name__)


class SchedulerMode(Enum):
    """调度器模式"""
    MASTER = "master"    # 主节点模式
    WORKER = "worker"    # 工作节点模式
    STANDALONE = "standalone"  # 独立模式


class TaskScheduler:
    """任务调度器"""
    
    def __init__(self, mode: SchedulerMode = SchedulerMode.STANDALONE,
                 dispatch_strategy: LoadBalanceStrategy = LoadBalanceStrategy.LEAST_TASKS,
                 max_tasks_per_device: int = 5,
                 schedule_interval: int = 30):
        self.mode = mode
        self.dispatch_strategy = dispatch_strategy
        self.max_tasks_per_device = max_tasks_per_device
        self.schedule_interval = schedule_interval
        self.running = False
        self.scheduler_thread = None
        self.lock = threading.Lock()
        
        # 任务分发器（仅在master模式下使用）
        self.dispatcher = None
        if mode == SchedulerMode.MASTER:
            self.dispatcher = TaskDispatcher(
                strategy=dispatch_strategy,
                max_tasks_per_device=max_tasks_per_device,
                dispatch_interval=10
            )
        
        # 任务执行器映射
        self.task_executors: Dict[str, Callable] = {}
        
        logger.info(f"TaskScheduler initialized in {mode.value} mode")
    
    def start(self):
        """启动任务调度器"""
        if self.running:
            logger.warning("TaskScheduler is already running")
            return
        
        try:
            self.running = True
            
            # 启动分发器（master模式）
            if self.mode == SchedulerMode.MASTER and self.dispatcher:
                self.dispatcher.start()
            
            # 启动调度线程
            self.scheduler_thread = threading.Thread(target=self._schedule_loop, daemon=True)
            self.scheduler_thread.start()
            
            logger.info(f"TaskScheduler started in {self.mode.value} mode")
            
        except Exception as e:
            logger.exception(f"Failed to start TaskScheduler: {e}")
            self.running = False
            raise
    
    def stop(self):
        """停止任务调度器"""
        self.running = False
        
        # 停止分发器
        if self.dispatcher:
            self.dispatcher.stop()
        
        # 等待调度线程结束
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
        
        logger.info("TaskScheduler stopped")
    
    def register_task_executor(self, task_type: str, executor: Callable):
        """注册任务执行器"""
        self.task_executors[task_type] = executor
        logger.info(f"Task executor registered for type: {task_type}")
    
    def _schedule_loop(self):
        """调度循环"""
        while self.running:
            try:
                if self.mode == SchedulerMode.MASTER:
                    self._master_schedule()
                elif self.mode == SchedulerMode.WORKER:
                    self._worker_schedule()
                else:  # STANDALONE
                    self._standalone_schedule()
                
                time.sleep(self.schedule_interval)
                
            except Exception as e:
                logger.exception(f"Error in schedule loop: {e}")
                time.sleep(5)
    
    def _master_schedule(self):
        """主节点调度逻辑"""
        try:
            # 主节点主要负责任务分发，具体执行由TaskDispatcher处理
            # 这里可以添加一些高级调度逻辑
            
            # 检查系统负载并调整分发策略 暂时不使用，减少数据库查询操作
            # self._adjust_dispatch_strategy()
            
            # 处理优先级任务 暂时不使用
            # self._handle_priority_tasks()
            
            # 监控任务执行情况
            self._monitor_task_execution()
            
        except Exception as e:
            logger.exception(f"Error in master schedule: {e}")
    
    def _worker_schedule(self):
        """工作节点调度逻辑"""
        try:
            # 获取分配给当前设备的任务
            # 这里需要获取当前设备ID，可以从配置或环境变量获取
            device_id = self._get_current_device_id()
            
            if not device_id:
                logger.warning("No device ID found for worker mode")
                return
            
            # 获取分配的任务
            assigned_tasks = CrawlTaskDAO.get_device_tasks(device_id)
            logger.info(f"Worker {device_id} has {len(assigned_tasks)} tasks in assigned and running status")

            # 执行任务
            for task in assigned_tasks:
                if task['status'] == 'assigned':
                    logger.info(f"Worker {device_id} is executing task {task['id']} of type {task['task_type']}")
                    self._execute_task(task, device_id)
                    
        except Exception as e:
            logger.exception(f"Error in worker schedule: {e}")
    
    def _standalone_schedule(self):
        """独立模式调度逻辑"""
        try:
            # 独立模式下，直接获取待执行任务并执行
            pending_tasks = CrawlTaskDAO.get_assignable_tasks(limit=self.max_tasks_per_device)
            
            for task in pending_tasks:
                # 标记任务为运行中
                CrawlTaskDAO.mark_running(task['id'])
                
                # 执行任务
                self._execute_task(task)
                
        except Exception as e:
            logger.exception(f"Error in standalone schedule: {e}")
    
    def _execute_task(self, task: Dict, device_id: Optional[str] = None):
        """执行任务"""
        task_id = task['id']
        task_type = task['task_type']
        
        try:
            logger.info(f"Executing task {task_id} of type {task_type}")
            
            # 更新任务状态为运行中
            if device_id:
                CrawlTaskDAO.mark_running(task_id, device_id)
                TaskAssignmentDAO.update_status_by_task_device(
                    task_id, device_id, 'running'
                )
            else:
                CrawlTaskDAO.mark_running(task_id)
            
            # 获取任务执行器
            executor = self.task_executors.get(task_type)
            if not executor:
                raise Exception(f"No executor found for task type: {task_type}")
            
            # 执行任务
            start_time = time.time()
            result = executor(task)
            execution_time = time.time() - start_time
            
            # 标记任务完成
            CrawlTaskDAO.mark_done(task_id)
            
            if device_id:
                TaskAssignmentDAO.update_status_by_task_device(
                    task_id, device_id, 'completed', None, {
                        'execution_time': execution_time,
                        'result': result
                    }
                )
                # 减少设备任务计数
                DeviceDAO.decrement_task_count(device_id)
            
            logger.info(f"Task {task_id} completed successfully in {execution_time:.2f}s")
            
        except Exception as e:
            logger.exception(f"Error executing task {task_id}: {e}")
            
            # 标记任务失败
            retry_delay = self._calculate_retry_delay(task.get('retry', 0))
            CrawlTaskDAO.fail_task(task_id, retry_delay)
            
            if device_id:
                TaskAssignmentDAO.update_status_by_task_device(
                    task_id, device_id, 'failed', str(e)
                )
                # 减少设备任务计数
                DeviceDAO.decrement_task_count(device_id)
    
    def _calculate_retry_delay(self, retry_count: int) -> int:
        """计算重试延迟（指数退避）"""
        base_delay = 60  # 基础延迟60秒
        max_delay = 3600  # 最大延迟1小时
        delay = min(base_delay * (2 ** retry_count), max_delay)
        return delay
    
    def _adjust_dispatch_strategy(self):
        """调整分发策略"""
        try:
            if not self.dispatcher:
                return
            
            # 获取系统负载情况
            online_devices = DeviceDAO.get_available_devices()
            if not online_devices:
                return
            
            # 计算平均负载
            total_tasks = 0
            for device in online_devices:
                device_tasks = CrawlTaskDAO.get_device_tasks(device['device_id'])
                total_tasks += len(device_tasks)
            
            avg_load = total_tasks / len(online_devices) if online_devices else 0
            
            # 根据负载调整策略
            if avg_load > self.max_tasks_per_device * 0.8:
                # 高负载时使用最少任务策略
                if self.dispatcher.strategy != LoadBalanceStrategy.LEAST_TASKS:
                    self.dispatcher.strategy = LoadBalanceStrategy.LEAST_TASKS
                    logger.info("Switched to LEAST_TASKS strategy due to high load")
            elif avg_load < self.max_tasks_per_device * 0.3:
                # 低负载时可以使用加权策略
                if self.dispatcher.strategy != LoadBalanceStrategy.WEIGHTED:
                    self.dispatcher.strategy = LoadBalanceStrategy.WEIGHTED
                    logger.info("Switched to WEIGHTED strategy due to low load")
                    
        except Exception as e:
            logger.exception(f"Error adjusting dispatch strategy: {e}")
    
    def _handle_priority_tasks(self):
        """处理优先级任务"""
        try:
            # 获取高优先级任务
            high_priority_tasks = CrawlTaskDAO.get_assignable_tasks(limit=50)
            high_priority_tasks = [t for t in high_priority_tasks if t.get('priority', 0) > 5]
            
            if not high_priority_tasks:
                return
            
            logger.info(f"Found {len(high_priority_tasks)} high priority tasks")
            
            # 为高优先级任务寻找最佳设备
            available_devices = DeviceDAO.get_available_devices()
            
            for task in high_priority_tasks:
                # 选择负载最轻的设备
                best_device = None
                min_tasks = float('inf')
                
                for device in available_devices:
                    device_tasks = CrawlTaskDAO.get_device_tasks(device['device_id'])
                    if len(device_tasks) < min_tasks and len(device_tasks) < self.max_tasks_per_device:
                        min_tasks = len(device_tasks)
                        best_device = device
                
                # 强制分配给最佳设备
                if best_device and self.dispatcher:
                    success = self.dispatcher.force_dispatch(task['id'], best_device['device_id'])
                    if success:
                        logger.info(f"High priority task {task['id']} assigned to {best_device['device_id']}")
                        
        except Exception as e:
            logger.exception(f"Error handling priority tasks: {e}")
    
    def _monitor_task_execution(self):
        """监控任务执行情况"""
        try:
            # 获取超时任务
            timeout_tasks = CrawlTaskDAO.get_timeout_tasks(timeout_minutes=60)
            
            for task in timeout_tasks:
                logger.warning(f"Task {task['id']} has been running for too long")
                
                # 可以选择终止任务或发送警告
                # 这里先记录日志，具体处理策略可以根据需求调整
                
        except Exception as e:
            logger.exception(f"Error monitoring task execution: {e}")
    
    def _get_current_device_id(self) -> Optional[str]:
        """获取当前设备ID"""
        try:
            # 可以从配置文件、环境变量或其他地方获取
            import os
            device_id = os.environ.get('DEVICE_ID')
            if device_id:
                return device_id
            
            # 如果没有配置，生成一个
            import socket
            import uuid
            hostname = socket.gethostname()
            mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff) for elements in range(0,2*6,2)][::-1])
            return f"{hostname}_{mac}"
            
        except Exception as e:
            logger.exception(f"Error getting current device ID: {e}")
            return None
    
    def get_scheduler_stats(self) -> Dict:
        """获取调度器统计信息"""
        try:
            stats = {
                'mode': self.mode.value,
                'running': self.running,
                'schedule_interval': self.schedule_interval,
                'max_tasks_per_device': self.max_tasks_per_device,
                'registered_executors': list(self.task_executors.keys())
            }
            
            # 添加任务统计
            task_stats = CrawlTaskDAO.get_task_stats()
            stats['task_stats'] = task_stats
            
            # 添加设备统计
            online_devices = DeviceDAO.get_available_devices()
            stats['device_count'] = len(online_devices)
            
            # 添加分发器统计（master模式）
            if self.dispatcher:
                stats['dispatcher_stats'] = self.dispatcher.get_dispatch_stats()
            
            return stats
            
        except Exception as e:
            logger.exception(f"Error getting scheduler stats: {e}")
            return {'error': str(e)}
    
    def add_task(self, task_type: str, username: str, app_id: Optional[str] = None,
                start_date: Optional[str] = None, end_date: Optional[str] = None,
                priority: int = 0, task_data: Optional[Dict] = None,
                execution_timeout: int = 3600, max_retry_count: int = 3) -> bool:
        """添加任务"""
        try:
            task = {
                'task_type': task_type,
                'username': username,
                'app_id': app_id,
                'start_date': start_date,
                'end_date': end_date,
                'priority': priority,
                'task_data': task_data,
                'execution_timeout': execution_timeout,
                'max_retry_count': max_retry_count,
                'next_run_at': datetime.now()
            }
            
            CrawlTaskDAO.add_tasks([task])
            logger.info(f"Task added: {task_type} for {username}")
            return True
            
        except Exception as e:
            logger.exception(f"Error adding task: {e}")
            return False
    
    def pause_scheduler(self):
        """暂停调度器"""
        with self.lock:
            if self.running:
                self.running = False
                logger.info("Scheduler paused")
    
    def resume_scheduler(self):
        """恢复调度器"""
        with self.lock:
            if not self.running:
                self.running = True
                if not self.scheduler_thread or not self.scheduler_thread.is_alive():
                    self.scheduler_thread = threading.Thread(target=self._schedule_loop, daemon=True)
                    self.scheduler_thread.start()
                logger.info("Scheduler resumed")
    
    def rebalance_tasks(self) -> Dict:
        """重新平衡任务"""
        if self.dispatcher:
            return self.dispatcher.rebalance_tasks()
        else:
            return {'rebalanced': 0, 'message': 'Rebalancing not available in current mode'}
    
    def get_task_queue_status(self) -> Dict:
        """获取任务队列状态"""
        try:
            # 按任务类型统计
            task_stats = CrawlTaskDAO.get_task_stats()
            
            # 按优先级统计
            priority_stats = {}
            all_tasks = CrawlTaskDAO.get_assignable_tasks(limit=1000)
            for task in all_tasks:
                priority = task.get('priority', 0)
                if priority not in priority_stats:
                    priority_stats[priority] = 0
                priority_stats[priority] += 1
            
            return {
                'total_stats': task_stats,
                'priority_distribution': priority_stats,
                'queue_length': task_stats.get('pending', 0),
                'running_tasks': task_stats.get('running', 0) + task_stats.get('assigned', 0)
            }
            
        except Exception as e:
            logger.exception(f"Error getting task queue status: {e}")
            return {'error': str(e)}