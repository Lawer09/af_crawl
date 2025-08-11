# 分布式爬虫系统启动执行流程文档

## 概述

本文档详细描述了分布式爬虫系统中 `python main.py distribute master` 和 `python main.py distribute worker` 两种模式的启动执行流程。

## 系统架构

分布式爬虫系统采用主从架构：
- **Master节点**：负责任务调度、分发和设备管理
- **Worker节点**：负责执行具体的爬虫任务
- **Standalone节点**：独立运行模式，集成了master和worker功能

## Master节点启动流程

### 命令格式
```bash
python main.py distribute master [其他参数]
```

### 启动流程详解

#### 1. 命令行解析阶段
- **入口文件**: `main.py`
- **解析器**: `_parse_args()` 函数解析命令行参数
- **参数验证**: 验证必需参数和可选参数

#### 2. 配置初始化阶段
- **配置加载**: 通过 `DistributionCLI.load_config()` 加载配置
- **设备ID处理**: 
  - 如果提供了 `--device-id`，验证格式
  - 如果未提供，自动生成设备ID（格式：`master-{timestamp}-{random}`）
- **设备名称生成**: 根据设备ID生成友好的设备名称

#### 3. 服务初始化阶段
**调用**: `init_distribution_services(mode="master", device_id=config.device_id)`

**初始化步骤**:
1. **数据库表初始化**:
   ```python
   DeviceDAO.init_table()           # 设备信息表
   TaskAssignmentDAO.init_table()   # 任务分配表
   DeviceHeartbeatDAO.init_table()  # 设备心跳表
   ```

2. **任务调度器初始化**:
   ```python
   _task_scheduler = TaskScheduler(mode=SchedulerMode.MASTER)
   ```
   - 创建任务分发器 `TaskDispatcher`
   - 设置负载均衡策略（默认：最少任务优先）
   - 配置最大并发任务数

3. **任务执行器注册**:
   ```python
   _register_task_executors(_task_scheduler)
   ```
   - 注册 `UserAppsTaskExecutor` (处理 user_apps 任务)
   - 注册 `AppSyncTaskExecutor` (处理 app_sync 任务)
   - 注册 `DataSyncTaskExecutor` (处理 data_sync 任务)

4. **设备管理器初始化**:
   ```python
   _device_manager = DeviceManager(device_id=config.device_id)
   ```

#### 4. 服务启动阶段
**调用**: `start_distribution_services()`

**启动步骤**:
1. **设备管理器启动**:
   - 注册当前设备到数据库
   - 启动心跳线程（定期发送设备状态）
   - 启动监控线程（监控离线设备）

2. **任务调度器启动**:
   - 启动任务分发器（master模式特有）
   - 启动调度循环线程
   - 开始执行 `_master_schedule()` 调度逻辑

#### 5. 主循环阶段
**Master调度逻辑** (`_master_schedule()`):
1. **任务分发**: 将待分配任务分发给可用的worker设备
2. **负载均衡**: 根据设备负载情况调整任务分配
3. **设备监控**: 监控worker设备状态和任务执行情况
4. **故障处理**: 处理设备离线和任务失败情况

#### 6. API服务
Master节点同时提供REST API服务：
- 设备注册和管理接口
- 任务创建和状态查询接口
- 系统统计和监控接口
- 负载均衡和任务重分配接口

## Worker节点启动流程

### 命令格式
```bash
python main.py distribute worker --master-host localhost [其他参数]
```

### 启动流程详解

#### 1. 命令行解析阶段
- **必需参数**: `--master-host` (主节点地址)
- **可选参数**: `--master-port`, `--device-id`, `--device-name` 等

#### 2. 配置初始化阶段
- **主节点连接配置**: 设置master节点的地址和端口
- **设备ID处理**: 类似master节点，但生成格式为 `worker-{timestamp}-{random}`
- **并发任务配置**: 设置worker可同时执行的任务数量

#### 3. 服务初始化阶段
**调用**: `init_distribution_services(mode="worker", device_id=config.device_id)`

**初始化步骤**:
1. **数据库表初始化**: 同master节点
2. **任务调度器初始化**:
   ```python
   _task_scheduler = TaskScheduler(mode=SchedulerMode.WORKER)
   ```
   - Worker模式下不创建任务分发器
   - 专注于任务执行而非分发

3. **任务执行器注册**: 同master节点
4. **设备管理器初始化**: 同master节点

#### 4. 异步服务启动阶段
**调用**: `asyncio.run(self._run_worker_async(config))`

**异步启动步骤**:
1. **获取异步客户端**:
   ```python
   client = get_async_distribution_client()
   ```

2. **设备注册**:
   ```python
   success = await client.register_device()
   ```
   - 向master节点注册当前worker设备
   - 提供设备能力信息（CPU、内存、网络等）

3. **状态更新**:
   ```python
   await client.update_device_status("online")
   ```

4. **心跳启动**:
   ```python
   await client.start_heartbeat()
   ```
   - 定期向master发送心跳信息
   - 包含系统资源使用情况
   - 包含当前运行任务数量

5. **服务启动**:
   ```python
   start_distribution_services()
   ```

#### 5. 主循环阶段
**Worker调度逻辑** (`_worker_schedule()`):
1. **任务拉取**: 定期从master节点拉取待执行任务
2. **任务执行**: 执行分配到的爬虫任务
3. **状态报告**: 向master报告任务执行状态
4. **资源监控**: 监控本地资源使用情况

**异步主循环**:
```python
while self.running:
    try:
        await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
        break
    except asyncio.TimeoutError:
        continue
```

#### 6. 优雅关闭
当接收到关闭信号时：
1. **状态更新**: `await client.update_device_status("offline")`
2. **停止心跳**: `await client.stop_heartbeat()`
3. **停止服务**: `stop_distribution_services()`

## 关键组件说明

### TaskScheduler (任务调度器)
- **Master模式**: 负责任务分发和负载均衡
- **Worker模式**: 负责任务执行和状态报告
- **调度策略**: 支持多种负载均衡算法

### DeviceManager (设备管理器)
- **设备注册**: 自动注册设备到分布式系统
- **心跳机制**: 定期发送设备状态和资源信息
- **离线检测**: 监控设备在线状态

### TaskDispatcher (任务分发器)
- **仅在Master模式使用**
- **负载均衡**: 根据设备负载分配任务
- **故障转移**: 处理设备离线时的任务重分配

### DistributionClient (分布式客户端)
- **异步通信**: Worker与Master之间的异步通信
- **API调用**: 封装REST API调用
- **连接管理**: 管理与Master的连接状态

## 配置文件

### Master配置示例
```json
{
  "mode": "master",
  "device_id": "master-001",
  "master_host": "0.0.0.0",
  "master_port": 7989,
  "dispatch_interval": 10,
  "load_balance_strategy": "least_tasks",
  "max_tasks_per_device": 5
}
```

### Worker配置示例
```json
{
  "mode": "worker",
  "device_id": "worker-001",
  "master_host": "localhost",
  "master_port": 7989,
  "concurrent_tasks": 3,
  "heartbeat_interval": 30
}
```

## 监控和日志

### 启动日志示例

**Master节点启动日志**:
```
2024-01-01 10:00:00 - INFO - Starting master node...
2024-01-01 10:00:01 - INFO - Distribution services initialized in master mode
2024-01-01 10:00:02 - INFO - TaskScheduler started in master mode
2024-01-01 10:00:03 - INFO - DeviceManager started
2024-01-01 10:00:04 - INFO - Master node started on localhost:7989
```

**Worker节点启动日志**:
```
2024-01-01 10:01:00 - INFO - Starting worker node...
2024-01-01 10:01:01 - INFO - Distribution services initialized in worker mode
2024-01-01 10:01:02 - INFO - Device registered successfully
2024-01-01 10:01:03 - INFO - Worker node started
2024-01-01 10:01:04 - INFO - Device ID: worker-001
2024-01-01 10:01:05 - INFO - Master: localhost:7989
```

## 故障处理

### 常见启动问题

1. **数据库连接失败**
   - 检查数据库配置
   - 确认数据库服务状态

2. **Master节点连接失败** (Worker)
   - 检查master_host和master_port配置
   - 确认Master节点已启动
   - 检查网络连接

3. **设备ID冲突**
   - 使用不同的device_id
   - 或删除冲突的设备记录

4. **端口占用** (Master)
   - 更改监听端口
   - 或停止占用端口的进程

### 健康检查

可以通过以下方式检查系统状态：

```bash
# 查看系统状态
python main.py distribute status --master-host localhost --master-port 7989

# 查看设备列表
curl http://localhost:7989/api/distribution/devices

# 查看任务状态
curl http://localhost:7989/api/distribution/tasks
```

## 总结

分布式爬虫系统的启动流程包括配置解析、服务初始化、组件启动和主循环运行四个主要阶段。Master节点专注于任务调度和设备管理，Worker节点专注于任务执行和状态报告。通过合理的配置和监控，可以构建一个稳定可靠的分布式爬虫系统。