from __future__ import annotations

import logging
import threading
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum

from model.device import DeviceDAO
from model.crawl_task import CrawlTaskDAO
from model.task_assignment import TaskAssignmentDAO
from model.device_heartbeat import DeviceHeartbeatDAO

logger = logging.getLogger(__name__)


class LoadBalanceStrategy(Enum):
    """负载均衡策略"""
    ROUND_ROBIN = "round_robin"  # 轮询
    LEAST_TASKS = "least_tasks"  # 最少任务
    WEIGHTED = "weighted"        # 加权
    RANDOM = "random"           # 随机


class TaskDispatcher:
    """任务分发器"""
    
    def __init__(self, strategy: LoadBalanceStrategy = LoadBalanceStrategy.LEAST_TASKS,
                 max_tasks_per_device: int = 5, dispatch_interval: int = 10):
        self.strategy = strategy
        self.max_tasks_per_device = max_tasks_per_device
        self.dispatch_interval = dispatch_interval
        self.running = False
        self.dispatch_thread = None
        self.lock = threading.Lock()
        self.round_robin_index = 0
        
        logger.info(f"TaskDispatcher initialized with strategy: {strategy.value}")
    
    def start(self):
        """启动任务分发器"""
        if self.running:
            logger.warning("TaskDispatcher is already running")
            return
        
        self.running = True
        self.dispatch_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self.dispatch_thread.start()
        logger.info("TaskDispatcher started")
    
    def stop(self):
        """停止任务分发器"""
        self.running = False
        if self.dispatch_thread:
            self.dispatch_thread.join(timeout=5)
        logger.info("TaskDispatcher stopped")
    
    def _dispatch_loop(self):
        """分发循环"""
        while self.running:
            try:
                self._dispatch_tasks()
                self._handle_timeout_tasks()
                time.sleep(self.dispatch_interval)
            except Exception as e:
                logger.exception(f"Error in dispatch loop: {e}")
                time.sleep(5)  # 出错时等待更长时间
    
    def _dispatch_tasks(self):
        """分发任务"""
        try:
            # 获取可用设备
            available_devices = self._get_available_devices()
            if not available_devices:
                logger.debug("No available devices for task dispatch")
                return
            
            # 获取待分配任务
            pending_tasks = CrawlTaskDAO.get_assignable_tasks(limit=100)
            if not pending_tasks:
                logger.debug("No pending tasks to dispatch")
                return
            
            logger.info(f"Dispatching {len(pending_tasks)} tasks to {len(available_devices)} devices")
            
            # 分发任务
            dispatched_count = 0
            for task in pending_tasks:
                device = self._select_device(available_devices, task)
                if device and self._assign_task_to_device(task, device):
                    dispatched_count += 1
                    # 更新设备任务计数
                    device['current_tasks'] = device.get('current_tasks', 0) + 1
                    
                    # 如果设备任务已满，从可用列表中移除
                    if device['current_tasks'] >= self.max_tasks_per_device:
                        available_devices = [d for d in available_devices if d['device_id'] != device['device_id']]
                        if not available_devices:
                            break
            
            if dispatched_count > 0:
                logger.info(f"Successfully dispatched {dispatched_count} tasks")
                
        except Exception as e:
            logger.exception(f"Error dispatching tasks: {e}")
    
    def _get_available_devices(self) -> List[Dict]:
        """获取可用设备列表"""
        try:
            # 获取在线设备
            online_devices = DeviceDAO.get_available_devices()
            
            available_devices = []
            for device in online_devices:
                # 检查设备当前任务数
                current_tasks = len(CrawlTaskDAO.get_device_tasks(device['device_id']))
                if current_tasks < self.max_tasks_per_device:
                    device['current_tasks'] = current_tasks
                    available_devices.append(device)
            
            return available_devices
            
        except Exception as e:
            logger.exception(f"Error getting available devices: {e}")
            return []
    
    def _select_device(self, available_devices: List[Dict], task: Dict) -> Optional[Dict]:
        """根据策略选择设备"""
        if not available_devices:
            return None
        
        try:
            if self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
                return self._select_round_robin(available_devices)
            elif self.strategy == LoadBalanceStrategy.LEAST_TASKS:
                return self._select_least_tasks(available_devices)
            elif self.strategy == LoadBalanceStrategy.WEIGHTED:
                return self._select_weighted(available_devices)
            elif self.strategy == LoadBalanceStrategy.RANDOM:
                return self._select_random(available_devices)
            else:
                return available_devices[0]  # 默认选择第一个
                
        except Exception as e:
            logger.exception(f"Error selecting device: {e}")
            return available_devices[0] if available_devices else None
    
    def _select_round_robin(self, devices: List[Dict]) -> Dict:
        """轮询选择"""
        with self.lock:
            device = devices[self.round_robin_index % len(devices)]
            self.round_robin_index += 1
            return device
    
    def _select_least_tasks(self, devices: List[Dict]) -> Dict:
        """选择任务最少的设备"""
        return min(devices, key=lambda d: d.get('current_tasks', 0))
    
    def _select_weighted(self, devices: List[Dict]) -> Dict:
        """加权选择（基于设备性能）"""
        import random
        
        # 计算权重（基于CPU和内存使用率的倒数）
        weights = []
        for device in devices:
            # 获取最新心跳信息
            heartbeat = DeviceHeartbeatDAO.get_latest_heartbeat(device['device_id'])
            if heartbeat:
                cpu_usage = float(heartbeat.get('cpu_usage', 50) or 50)
                memory_usage = float(heartbeat.get('memory_usage', 50) or 50)
                # 权重 = 100 - (CPU使用率 + 内存使用率) / 2
                weight = max(1.0, 100.0 - (cpu_usage + memory_usage) / 2.0)
            else:
                weight = 50.0  # 默认权重
            weights.append(weight)
        
        # 加权随机选择
        total_weight = sum(weights)
        if total_weight == 0:
            return devices[0]
        
        rand_val = random.uniform(0, float(total_weight))
        current_weight = 0.0
        for i, weight in enumerate(weights):
            current_weight += weight
            if rand_val <= current_weight:
                return devices[i]
        
        return devices[-1]  # 兜底
    
    def _select_random(self, devices: List[Dict]) -> Dict:
        """随机选择"""
        import random
        return random.choice(devices)
    
    def _assign_task_to_device(self, task: Dict, device: Dict) -> bool:
        """分配任务给设备"""
        try:
            task_id = task['id']
            device_id = device['device_id']
            
            # 更新任务状态为已分配
            if CrawlTaskDAO.assign_task(task_id, device_id):
                # 创建任务分配记录
                assignment_id = TaskAssignmentDAO.create_assignment(task_id, device_id)
                if assignment_id:
                    # 增加设备任务计数
                    DeviceDAO.increment_task_count(device_id)
                    logger.debug(f"Task {task_id} assigned to device {device_id}")
                    return True
                else:
                    # 回滚任务状态
                    CrawlTaskDAO.release_device_tasks(device_id)
                    logger.error(f"Failed to create assignment record for task {task_id}")
            
            return False
            
        except Exception as e:
            logger.exception(f"Error assigning task {task.get('id')} to device {device.get('device_id')}: {e}")
            return False
    
    def _handle_timeout_tasks(self):
        """处理超时任务"""
        try:
            # 获取超时的任务分配
            timeout_assignments = TaskAssignmentDAO.get_timeout_assignments(timeout_minutes=30)
            
            for assignment in timeout_assignments:
                task_id = assignment['task_id']
                device_id = assignment['device_id']
                retry_count = assignment['retry_count']
                
                logger.warning(f"Task {task_id} timeout on device {device_id}, retry_count: {retry_count}")
                
                # 更新分配状态为超时
                TaskAssignmentDAO.update_status(assignment['id'], 'timeout', 'Task execution timeout')
                
                # 减少设备任务计数
                DeviceDAO.decrement_task_count(device_id)
                
                # 检查是否需要重试
                task = CrawlTaskDAO.fetch_pending('', 1)  # 获取任务详情
                if task and retry_count < task[0].get('max_retry_count', 3):
                    # 重新分配任务
                    CrawlTaskDAO.assign_task(task_id, None)  # 清除设备分配
                    TaskAssignmentDAO.increment_retry_count(assignment['id'])
                    logger.info(f"Task {task_id} will be retried")
                else:
                    # 标记任务失败
                    CrawlTaskDAO.fail_task(task_id, 0)
                    logger.error(f"Task {task_id} failed after max retries")
                    
        except Exception as e:
            logger.exception(f"Error handling timeout tasks: {e}")
    
    def get_dispatch_stats(self) -> Dict:
        """获取分发统计信息"""
        try:
            # 获取任务统计
            task_stats = CrawlTaskDAO.get_task_stats()
            
            # 获取设备统计
            online_devices = DeviceDAO.get_available_devices()
            device_count = len(online_devices)
            
            # 获取分配统计
            total_assignments = 0
            active_assignments = 0
            for device in online_devices:
                device_tasks = CrawlTaskDAO.get_device_tasks(device['device_id'])
                total_assignments += len(device_tasks)
                active_assignments += len([t for t in device_tasks if t['status'] == 'running'])
            
            return {
                'strategy': self.strategy.value,
                'max_tasks_per_device': self.max_tasks_per_device,
                'dispatch_interval': self.dispatch_interval,
                'running': self.running,
                'device_count': device_count,
                'total_assignments': total_assignments,
                'active_assignments': active_assignments,
                'task_stats': task_stats
            }
            
        except Exception as e:
            logger.exception(f"Error getting dispatch stats: {e}")
            return {}
    
    def force_dispatch(self, task_id: int, device_id: str) -> bool:
        """强制分配任务给指定设备"""
        try:
            # 检查设备是否可用
            device = DeviceDAO.get_device(device_id)
            if not device or device['status'] != 'online':
                logger.error(f"Device {device_id} is not available")
                return False
            
            # 检查设备任务数是否已满
            current_tasks = len(CrawlTaskDAO.get_device_tasks(device_id))
            if current_tasks >= self.max_tasks_per_device:
                logger.error(f"Device {device_id} has reached max task limit")
                return False
            
            # 分配任务
            if CrawlTaskDAO.assign_task(task_id, device_id):
                assignment_id = TaskAssignmentDAO.create_assignment(task_id, device_id)
                if assignment_id:
                    DeviceDAO.increment_task_count(device_id)
                    logger.info(f"Task {task_id} force assigned to device {device_id}")
                    return True
            
            return False
            
        except Exception as e:
            logger.exception(f"Error force dispatching task {task_id} to device {device_id}: {e}")
            return False
    
    def rebalance_tasks(self) -> Dict:
        """重新平衡任务分配"""
        try:
            logger.info("Starting task rebalancing")
            
            # 获取所有在线设备及其任务
            devices = DeviceDAO.get_available_devices()
            device_tasks = {}
            total_tasks = 0
            
            for device in devices:
                tasks = CrawlTaskDAO.get_device_tasks(device['device_id'])
                device_tasks[device['device_id']] = tasks
                total_tasks += len(tasks)
            
            if not devices or total_tasks == 0:
                return {'rebalanced': 0, 'message': 'No tasks to rebalance'}
            
            # 计算平均任务数
            avg_tasks = total_tasks / len(devices)
            rebalanced_count = 0
            
            # 找出任务过多的设备
            overloaded_devices = []
            underloaded_devices = []
            
            for device_id, tasks in device_tasks.items():
                if len(tasks) > avg_tasks + 1:
                    overloaded_devices.append((device_id, tasks))
                elif len(tasks) < avg_tasks - 1:
                    underloaded_devices.append(device_id)
            
            # 重新分配任务
            for device_id, tasks in overloaded_devices:
                excess_tasks = int(len(tasks) - avg_tasks)
                for i in range(min(excess_tasks, len(underloaded_devices))):
                    if underloaded_devices:
                        target_device = underloaded_devices.pop(0)
                        task = tasks[i]
                        
                        # 重新分配任务
                        if CrawlTaskDAO.assign_task(task['id'], target_device):
                            # 更新分配记录
                            TaskAssignmentDAO.update_status_by_task_device(
                                task['id'], device_id, 'completed', 'Rebalanced to another device'
                            )
                            TaskAssignmentDAO.create_assignment(task['id'], target_device)
                            
                            # 更新设备任务计数
                            DeviceDAO.decrement_task_count(device_id)
                            DeviceDAO.increment_task_count(target_device)
                            
                            rebalanced_count += 1
                            logger.info(f"Task {task['id']} rebalanced from {device_id} to {target_device}")
            
            return {
                'rebalanced': rebalanced_count,
                'message': f'Successfully rebalanced {rebalanced_count} tasks'
            }
            
        except Exception as e:
            logger.exception(f"Error rebalancing tasks: {e}")
            return {'rebalanced': 0, 'message': f'Error: {str(e)}'}