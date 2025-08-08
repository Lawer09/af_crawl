# 分布式任务系统 - 任务流程详解

## 概述

本文档详细说明了分布式任务系统中任务的获取、分配和执行的完整流程，包括各个组件的交互方式和数据流转过程。

## 任务生命周期

```
任务创建 → 任务分配 → 任务执行 → 结果处理 → 任务完成
    ↓         ↓         ↓         ↓         ↓
  pending → assigned → running → completed/failed
```

## 1. 任务获取流程

### 1.1 任务来源

任务主要来源于以下几个渠道：

#### A. 定时任务创建
```python
# 在 tasks/sync_user_apps.py 中
def run():
    # 检查是否有待处理的用户应用同步任务
    if not CrawlTaskDAO.fetch_pending('user_apps', 1):
        # 如果没有，为所有启用用户创建任务
        users = UserDAO.get_enabled_users()
        init_tasks = [{
            'task_type': 'user_apps',
            'username': u['email'],
            'next_run_at': date.today().isoformat(),
        } for u in users]
        CrawlTaskDAO.add_tasks(init_tasks)
```

#### B. API手动创建
```python
# 通过 API 创建任务
POST /api/distribution/tasks
{
    "task_type": "app_sync",
    "username": "user@example.com",
    "priority": 1,
    "task_data": {
        "app_id": "optional_app_id"
    }
}
```

#### C. 批量任务创建
```python
# 批量创建数据同步任务
tasks = []
for user in users:
    for app in user_apps:
        tasks.append({
            'task_type': 'data_sync',
            'username': user['email'],
            'task_data': {
                'app_id': app['app_id'],
                'start_date': '2024-01-01',
                'end_date': '2024-01-31'
            }
        })
CrawlTaskDAO.add_tasks(tasks)
```

### 1.2 任务存储结构

任务存储在 `af_crawl_tasks` 表中，包含以下关键字段：

```sql
CREATE TABLE af_crawl_tasks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_type VARCHAR(50) NOT NULL,           -- 任务类型
    username VARCHAR(100) NOT NULL,           -- 用户名
    app_id VARCHAR(100),                      -- 应用ID（可选）
    start_date DATE,                          -- 开始日期
    end_date DATE,                            -- 结束日期
    status ENUM('pending', 'assigned', 'running', 'completed', 'failed') DEFAULT 'pending',
    priority INT DEFAULT 0,                   -- 任务优先级
    task_data JSON,                           -- 任务数据
    assigned_device_id VARCHAR(100),          -- 分配的设备ID
    assigned_at DATETIME,                     -- 分配时间
    execution_timeout INT DEFAULT 3600,      -- 执行超时时间
    max_retry_count INT DEFAULT 3,           -- 最大重试次数
    retry INT DEFAULT 0,                      -- 当前重试次数
    next_run_at DATETIME,                     -- 下次运行时间
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

## 2. 任务分配流程

### 2.1 Master模式下的任务分配

在Master模式下，`TaskDispatcher` 负责任务分配：

```python
# services/task_dispatcher.py
class TaskDispatcher:
    async def dispatch_tasks(self):
        """主要的任务分发循环"""
        while self.running:
            try:
                # 1. 获取可分配的任务
                assignable_tasks = CrawlTaskDAO.get_assignable_tasks(
                    limit=self.batch_size
                )
                
                # 2. 获取可用设备
                available_devices = DeviceDAO.get_available_devices()
                
                # 3. 为每个任务分配设备
                for task in assignable_tasks:
                    device = self._select_device(available_devices, task)
                    if device:
                        await self._assign_task(task, device)
                
                await asyncio.sleep(self.dispatch_interval)
                
            except Exception as e:
                logger.exception(f"Error in dispatch loop: {e}")
                await asyncio.sleep(10)
```

### 2.2 设备选择策略

系统支持多种负载均衡策略：

#### A. 最少任务策略 (LEAST_TASKS)
```python
def _select_device_least_tasks(self, devices: List[Dict], task: Dict) -> Optional[Dict]:
    """选择当前任务数最少的设备"""
    if not devices:
        return None
    
    # 按当前任务数排序
    sorted_devices = sorted(devices, key=lambda d: d.get('current_tasks', 0))
    return sorted_devices[0]
```

#### B. 轮询策略 (ROUND_ROBIN)
```python
def _select_device_round_robin(self, devices: List[Dict], task: Dict) -> Optional[Dict]:
    """轮询选择设备"""
    if not devices:
        return None
    
    device = devices[self.round_robin_index % len(devices)]
    self.round_robin_index += 1
    return device
```

#### C. 加权策略 (WEIGHTED)
```python
def _select_device_weighted(self, devices: List[Dict], task: Dict) -> Optional[Dict]:
    """根据设备权重选择"""
    if not devices:
        return None
    
    # 根据设备性能计算权重
    weights = []
    for device in devices:
        cpu_weight = (100 - device.get('cpu_usage', 50)) / 100
        memory_weight = (100 - device.get('memory_usage', 50)) / 100
        task_weight = max(0, (device.get('max_tasks', 5) - device.get('current_tasks', 0)) / device.get('max_tasks', 5))
        
        total_weight = (cpu_weight + memory_weight + task_weight) / 3
        weights.append(total_weight)
    
    # 加权随机选择
    return random.choices(devices, weights=weights)[0]
```

### 2.3 任务分配过程

```python
async def _assign_task(self, task: Dict, device: Dict):
    """将任务分配给设备"""
    try:
        task_id = task['id']
        device_id = device['device_id']
        
        # 1. 更新任务状态为已分配
        success = CrawlTaskDAO.assign_task(
            task_id=task_id,
            device_id=device_id,
            timeout_seconds=task.get('execution_timeout', 3600)
        )
        
        if success:
            # 2. 创建任务分配记录
            TaskAssignmentDAO.create_assignment(
                task_id=task_id,
                device_id=device_id,
                task_type=task['task_type'],
                priority=task.get('priority', 0),
                timeout_seconds=task.get('execution_timeout', 3600)
            )
            
            # 3. 更新设备任务计数
            DeviceDAO.increment_task_count(device_id)
            
            # 4. 记录分配日志
            logger.info(
                f"Task {task_id} assigned to device {device_id} "
                f"(type: {task['task_type']}, user: {task['username']})"
            )
            
            # 5. 更新统计信息
            self.stats['tasks_assigned'] += 1
            
        else:
            logger.warning(f"Failed to assign task {task_id} to device {device_id}")
            
    except Exception as e:
        logger.exception(f"Error assigning task {task['id']} to device {device['device_id']}: {e}")
```

## 3. 任务执行流程

### 3.1 Worker模式下的任务获取

Worker节点通过以下方式获取任务：

```python
# executors/task_executor.py
async def _task_pull_loop(self):
    """工作节点任务拉取循环"""
    while True:
        try:
            if self.can_accept_task():
                # 1. 从Master节点拉取任务
                tasks = await self.async_client.pull_tasks(limit=1)
                
                for task in tasks:
                    task_id = task['id']
                    task_type = task['task_type']
                    task_data = task.get('task_data', {})
                    
                    # 2. 更新任务状态为运行中
                    await self.async_client.update_task_status(task_id, "running")
                    
                    # 3. 异步执行任务
                    future = self.execute_task_async(task_id, task_type, task_data)
                    
                    # 4. 创建任务完成回调
                    asyncio.create_task(self._handle_task_completion(task_id, future))
            
            # 等待下次拉取
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.exception(f"Error in task pull loop: {e}")
            await asyncio.sleep(10)
```

### 3.2 任务拉取API

```python
# api/distribution_api.py
@router.get("/tasks/pull")
async def pull_tasks(
    device_id: str = Query(..., description="设备ID"),
    limit: int = Query(1, ge=1, le=10, description="拉取任务数量限制")
):
    """工作节点拉取任务"""
    try:
        # 1. 验证设备
        device = DeviceDAO.get_device(device_id)
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")
        
        if device['status'] != 'online':
            raise HTTPException(status_code=400, detail="Device is not online")
        
        # 2. 检查设备是否可以接受更多任务
        if device['current_tasks'] >= device.get('max_tasks', 5):
            return []
        
        # 3. 获取分配给该设备的待执行任务
        available_limit = min(limit, device.get('max_tasks', 5) - device['current_tasks'])
        tasks = CrawlTaskDAO.get_device_tasks(device_id, status='assigned', limit=available_limit)
        
        # 4. 返回任务列表
        return tasks
        
    except Exception as e:
        logger.exception(f"Error pulling tasks for device {device_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

### 3.3 任务执行器选择

系统根据任务类型选择相应的执行器：

```python
# executors/task_executor.py
class DistributedTaskExecutor:
    def __init__(self, max_concurrent_tasks: int = 3):
        self.executors = {}
        
        # 注册默认执行器
        self.register_executor(AppSyncTaskExecutor())      # 处理 app_sync 任务
        self.register_executor(DataSyncTaskExecutor())     # 处理 data_sync 任务
    
    def get_executor(self, task_type: str) -> Optional[TaskExecutor]:
        """根据任务类型获取执行器"""
        return self.executors.get(task_type)
    
    def execute_task_sync(self, task_id: int, task_type: str, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """同步执行任务"""
        # 1. 获取对应的执行器
        executor = self.get_executor(task_type)
        if not executor:
            raise ValueError(f"No executor found for task type: {task_type}")
        
        # 2. 执行任务
        start_time = time.time()
        try:
            result = executor.execute_task(task_data)
            execution_time = time.time() - start_time
            result["execution_time"] = execution_time
            
            # 3. 更新统计
            if result.get("status") == "success":
                executor.completed_tasks += 1
                self.total_executed += 1
            else:
                executor.failed_tasks += 1
                self.total_failed += 1
            
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
```

### 3.4 具体任务执行器

#### A. App同步任务执行器
```python
# executors/task_executor.py
class AppSyncTaskExecutor(TaskExecutor):
    def __init__(self):
        super().__init__("app_sync")
    
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行App同步任务"""
        try:
            username = task_data.get('username')
            if not username:
                raise ValueError("Missing username in task data")
            
            # 调用具体的同步函数
            from tasks.sync_user_apps import sync_user_apps
            result = sync_user_apps(username)
            
            return {
                "status": "success",
                "username": username,
                "synced_apps": result.get('synced_apps', 0),
                "execution_time": result.get('execution_time', 0)
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error_message": str(e),
                "error_type": type(e).__name__
            }
```

#### B. 数据同步任务执行器
```python
class DataSyncTaskExecutor(TaskExecutor):
    def __init__(self):
        super().__init__("data_sync")
    
    def execute_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行数据同步任务"""
        try:
            username = task_data.get('username')
            app_id = task_data.get('app_id')
            start_date = task_data.get('start_date')
            end_date = task_data.get('end_date')
            
            # 调用具体的同步函数
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
                "synced_records": result.get('synced_records', 0),
                "execution_time": result.get('execution_time', 0)
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error_message": str(e),
                "error_type": type(e).__name__
            }
```

## 4. 任务完成处理

### 4.1 任务完成回调

```python
async def _handle_task_completion(self, task_id: int, future: Future):
    """处理任务完成"""
    try:
        # 1. 等待任务完成
        result = await asyncio.wrap_future(future)
        
        # 2. 根据结果更新任务状态
        if result.get("status") == "success":
            # 任务成功完成
            await self.async_client.update_task_status(
                task_id, "completed", result_data=result
            )
        else:
            # 任务执行失败
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
```

### 4.2 任务状态更新API

```python
@router.put("/tasks/{task_id}/status")
async def update_task_status(
    task_id: int,
    status_update: TaskStatusUpdate
):
    """更新任务状态"""
    try:
        # 1. 获取任务信息
        task = CrawlTaskDAO.get_task_by_id(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        # 2. 更新任务状态
        if status_update.status == "running":
            CrawlTaskDAO.mark_running(task_id, status_update.device_id)
            
        elif status_update.status == "completed":
            CrawlTaskDAO.mark_done(task_id)
            
            # 更新任务分配记录
            TaskAssignmentDAO.complete_assignment(
                task_id, result_data=status_update.result_data
            )
            
        elif status_update.status == "failed":
            # 检查是否需要重试
            if task['retry'] < task.get('max_retry_count', 3):
                # 安排重试
                retry_delay = min(300 * (2 ** task['retry']), 3600)  # 指数退避
                CrawlTaskDAO.fail_task(task_id, retry_delay)
            else:
                # 达到最大重试次数，标记为最终失败
                CrawlTaskDAO.mark_failed(task_id)
            
            # 更新任务分配记录
            TaskAssignmentDAO.fail_assignment(
                task_id, error_message=status_update.error_message
            )
        
        # 3. 更新设备任务计数
        if task.get('assigned_device_id'):
            if status_update.status in ['completed', 'failed']:
                DeviceDAO.decrement_task_count(task['assigned_device_id'])
        
        return {"message": "Task status updated successfully"}
        
    except Exception as e:
        logger.exception(f"Error updating task {task_id} status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

## 5. 故障处理和恢复

### 5.1 任务超时处理

```python
# services/task_dispatcher.py
async def handle_timeout_tasks(self):
    """处理超时任务"""
    try:
        # 1. 获取超时的任务
        timeout_tasks = CrawlTaskDAO.get_timeout_tasks()
        
        for task in timeout_tasks:
            task_id = task['id']
            device_id = task.get('assigned_device_id')
            
            logger.warning(f"Task {task_id} timed out on device {device_id}")
            
            # 2. 释放设备资源
            if device_id:
                DeviceDAO.decrement_task_count(device_id)
            
            # 3. 重新分配任务或标记失败
            if task['retry'] < task.get('max_retry_count', 3):
                # 重置任务状态，等待重新分配
                CrawlTaskDAO.reset_task_for_retry(task_id)
                TaskAssignmentDAO.timeout_assignment(task_id)
            else:
                # 达到最大重试次数
                CrawlTaskDAO.mark_failed(task_id)
                TaskAssignmentDAO.fail_assignment(task_id, "Max retries exceeded")
            
            self.stats['tasks_timeout'] += 1
            
    except Exception as e:
        logger.exception(f"Error handling timeout tasks: {e}")
```

### 5.2 设备离线处理

```python
# services/device_manager.py
async def handle_offline_devices(self):
    """处理离线设备"""
    try:
        # 1. 获取超时的设备
        timeout_devices = DeviceDAO.get_timeout_devices()
        
        for device in timeout_devices:
            device_id = device['device_id']
            
            logger.warning(f"Device {device_id} is offline, releasing its tasks")
            
            # 2. 更新设备状态
            DeviceDAO.update_device_status(device_id, 'offline')
            
            # 3. 释放设备的所有任务
            released_tasks = CrawlTaskDAO.release_device_tasks(device_id)
            
            # 4. 更新任务分配记录
            for task_id in released_tasks:
                TaskAssignmentDAO.timeout_assignment(task_id)
            
            logger.info(f"Released {len(released_tasks)} tasks from offline device {device_id}")
            
    except Exception as e:
        logger.exception(f"Error handling offline devices: {e}")
```

## 6. 监控和统计

### 6.1 任务统计

```python
@router.get("/tasks/stats")
async def get_task_stats():
    """获取任务统计信息"""
    try:
        stats = CrawlTaskDAO.get_task_stats()
        
        return {
            "total_tasks": stats.get('total', 0),
            "pending_tasks": stats.get('pending', 0),
            "assigned_tasks": stats.get('assigned', 0),
            "running_tasks": stats.get('running', 0),
            "completed_tasks": stats.get('completed', 0),
            "failed_tasks": stats.get('failed', 0),
            "success_rate": stats.get('success_rate', 0),
            "avg_execution_time": stats.get('avg_execution_time', 0)
        }
        
    except Exception as e:
        logger.exception(f"Error getting task stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

### 6.2 设备性能监控

```python
@router.get("/devices/{device_id}/stats")
async def get_device_stats(device_id: str):
    """获取设备统计信息"""
    try:
        # 1. 获取设备基本信息
        device = DeviceDAO.get_device(device_id)
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")
        
        # 2. 获取设备任务统计
        task_stats = TaskAssignmentDAO.get_device_stats(device_id)
        
        # 3. 获取设备健康状态
        health_stats = DeviceHeartbeatDAO.get_device_health(device_id)
        
        return {
            "device_info": device,
            "task_stats": task_stats,
            "health_stats": health_stats,
            "uptime": DeviceHeartbeatDAO.get_device_uptime(device_id)
        }
        
    except Exception as e:
        logger.exception(f"Error getting device {device_id} stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

## 7. 配置和优化

### 7.1 性能调优参数

```json
{
  "concurrent_tasks": 3,              // 每个设备的并发任务数
  "dispatch_interval": 10,            // 任务分发间隔（秒）
  "heartbeat_interval": 30,           // 心跳间隔（秒）
  "task_timeout": 3600,               // 任务超时时间（秒）
  "device_timeout": 300,              // 设备超时时间（秒）
  "max_retry_count": 3,               // 最大重试次数
  "load_balance_strategy": "least_tasks", // 负载均衡策略
  "batch_size": 10                    // 批处理大小
}
```

### 7.2 监控指标

- **任务指标**: 总任务数、完成率、失败率、平均执行时间
- **设备指标**: 在线设备数、CPU使用率、内存使用率、任务负载
- **系统指标**: 吞吐量、响应时间、错误率、资源利用率

## 8. 总结

分布式任务系统的完整流程包括：

1. **任务创建**: 通过定时任务、API或批量方式创建任务
2. **任务分配**: Master节点根据负载均衡策略将任务分配给可用设备
3. **任务获取**: Worker节点主动拉取分配给自己的任务
4. **任务执行**: 根据任务类型选择相应执行器进行处理
5. **结果处理**: 更新任务状态，处理成功/失败情况
6. **故障恢复**: 处理超时、设备离线等异常情况
7. **监控统计**: 实时监控系统运行状态和性能指标

整个系统设计具有高可用性、可扩展性和容错性，能够有效处理大规模爬虫任务的分布式执行。