# Standalone 模式执行流程文档

## 概述

本文档详细介绍了执行 `python main.py distribute standalone --device-id master-stand-01` 命令后的完整执行流程。

## 命令参数说明

```bash
python main.py distribute standalone --device-id master-stand-01
```

### 参数解析
- `distribute`: 主命令，表示启动分布式任务系统
- `standalone`: 子命令，表示启动独立节点模式
- `--device-id master-stand-01`: 必需参数，指定设备ID
- `--device-name`: 可选参数，设备名称（默认为主机名）
- `--dispatch-interval`: 可选参数，任务分发间隔，默认10秒
- `--concurrent-tasks`: 可选参数，并发任务数，默认5个
- `--enable-monitoring`: 可选参数，启用性能监控
- `--config`: 可选参数，配置文件路径

## 执行流程详解

### 1. 启动阶段 (main.py)

#### 1.1 参数解析
- 使用 `argparse` 解析命令行参数
- 验证必需参数 `device-id` 是否提供
- 设置默认值：`dispatch-interval=10`, `concurrent-tasks=5`

#### 1.2 启动信息打印
```python
def _print_startup_info():
    logger.info("启动配置信息:")
    logger.info("代理状态: %s", "开启" if USE_PROXY else "关闭")
    logger.info("进程数: %d", CRAWLER["processes"])
    logger.info("每进程线程数: %d", CRAWLER["threads_per_process"])
    logger.info("最大重试次数: %d", CRAWLER["max_retry"])
    logger.info("重试延迟: %d秒", CRAWLER["retry_delay_seconds"])
```

#### 1.3 分布式CLI调用
- 创建 `DistributionCLI` 实例
- 调用 `cli.run_standalone(args)` 方法

### 2. CLI处理阶段 (distribution_cli.py)

#### 2.1 配置加载
```python
config_kwargs = {
    'mode': DistributionMode.STANDALONE,
    'device_id': args.device_id,
    'device_name': args.device_name,
    'dispatch_interval': args.dispatch_interval,
    'concurrent_tasks': args.concurrent_tasks,
    'enable_performance_monitoring': args.enable_monitoring
}
config = self.load_config(args.config, **config_kwargs)
```

#### 2.2 信号处理设置
- 设置 SIGINT (Ctrl+C) 和 SIGTERM 信号处理器
- 确保优雅关闭

### 3. 服务初始化阶段 (distribution_api.py)

#### 3.1 数据库表初始化
```python
def init_distribution_services(mode="standalone", device_id=None):
    # 初始化数据库表
    DeviceDAO.init_table()           # 设备表
    TaskAssignmentDAO.init_table()   # 任务分配表
    DeviceHeartbeatDAO.init_table()  # 设备心跳表
```

#### 3.2 组件初始化
- **TaskScheduler**: 初始化为 `STANDALONE` 模式
- **DeviceManager**: 使用指定的 `device_id` 初始化

### 4. 服务启动阶段

#### 4.1 DeviceManager 启动流程

##### 4.1.1 设备注册
```python
def _register_device(self):
    device_info = self._get_device_info()
    full_device_info = {
        'device_id': self.device_id,
        'device_name': self.device_name,
        'device_type': device_info['device_type'],
        'ip_address': device_info['ip_address'],
        'port': device_info.get('port', 8080),
        'capabilities': device_info['capabilities'],
        'max_concurrent_tasks': device_info.get('max_concurrent_tasks', 5)
    }
    DeviceDAO.register_device(full_device_info)
```

##### 4.1.2 系统信息收集
- CPU核心数
- 内存总量
- 平台信息
- Python版本
- 本地IP地址
- 支持的任务类型

##### 4.1.3 后台线程启动
- **心跳线程**: 每30秒发送一次心跳
- **监控线程**: 监控离线设备和数据清理

#### 4.2 TaskScheduler 启动流程

##### 4.2.1 调度器配置
- 模式: `STANDALONE`
- 调度间隔: 30秒
- 最大任务数: 5个
- 负载均衡策略: `LEAST_TASKS`

##### 4.2.2 调度线程启动
```python
def _standalone_schedule(self):
    # 获取待执行任务
    pending_tasks = CrawlTaskDAO.fetch_pending('', limit=self.max_tasks_per_device)
    
    for task in pending_tasks:
        # 标记任务为运行中
        CrawlTaskDAO.mark_running(task['id'])
        
        # 执行任务
        self._execute_task(task)
```

### 5. 运行时阶段

#### 5.1 心跳机制
- **频率**: 每30秒一次
- **内容**: 
  - CPU使用率
  - 内存使用率
  - 磁盘使用率
  - 网络状态
  - 运行中的任务数
  - 系统负载
  - 错误计数

#### 5.2 任务调度
- **频率**: 每30秒一次
- **逻辑**: 
  1. 从数据库获取待执行任务
  2. 标记任务状态为"运行中"
  3. 执行任务
  4. 更新任务状态

#### 5.3 监控功能
- **离线设备检测**: 检测超时设备并标记为离线
- **数据清理**: 清理过期的心跳记录和任务分配记录
- **性能监控**: 收集和记录系统性能指标

### 6. 任务执行流程

#### 6.1 任务类型
- `user_apps`: 用户应用列表同步
- `app_data`: 应用数据同步

#### 6.2 执行步骤
1. 从任务队列获取任务
2. 验证任务参数
3. 调用对应的任务执行器
4. 处理执行结果
5. 更新任务状态
6. 记录执行日志

### 7. 关闭流程

#### 7.1 信号处理
- 接收到 SIGINT 或 SIGTERM 信号
- 设置 `running = False`

#### 7.2 优雅关闭
1. 停止任务调度器
2. 等待当前任务完成
3. 停止设备管理器
4. 更新设备状态为"离线"
5. 关闭数据库连接
6. 退出程序

## 关键组件说明

### DeviceManager
- **职责**: 设备注册、心跳发送、状态监控
- **线程**: 心跳线程、监控线程
- **数据**: 设备信息、系统状态、性能指标

### TaskScheduler
- **职责**: 任务调度、执行管理
- **模式**: STANDALONE（独立模式）
- **策略**: 最少任务优先

### 数据库表
- **cl_device**: 设备信息表
- **cl_task_assignment**: 任务分配表
- **cl_device_heartbeat**: 设备心跳表
- **cl_crawl_task**: 爬虫任务表

## 配置参数

### 默认配置
- 心跳间隔: 30秒
- 离线超时: 300秒（5分钟）
- 调度间隔: 30秒
- 最大并发任务: 5个
- 任务分发间隔: 10秒

### 可调参数
- `--dispatch-interval`: 任务分发间隔
- `--concurrent-tasks`: 并发任务数
- `--enable-monitoring`: 性能监控开关

## 日志输出示例

```
2025-08-11 15:01:34,453 [INFO] __main__ - 启动配置信息:
2025-08-11 15:01:40,909 [INFO] core.db - MySQL connection pool created
2025-08-11 15:01:42,723 [INFO] model.device - Table cl_device initialized
2025-08-11 15:01:44,578 [INFO] model.task_assignment - Table cl_task_assignment initialized
2025-08-11 15:01:46,498 [INFO] model.device_heartbeat - Table cl_device_heartbeat initialized
2025-08-11 15:01:46,499 [INFO] services.task_scheduler - TaskScheduler initialized in standalone mode
2025-08-11 15:01:46,500 [INFO] services.device_manager - DeviceManager initialized
2025-08-11 15:01:48,371 [INFO] model.device - Device registered: master-stand-01
2025-08-11 15:01:48,610 [INFO] services.device_manager - DeviceManager started
2025-08-11 15:01:49,055 [INFO] services.task_scheduler - TaskScheduler started in standalone mode
2025-08-11 15:01:49,576 [INFO] api.distribution_api - Distribution services started
2025-08-11 15:01:49,834 [INFO] cli.distribution_cli - Standalone node started
```

## 故障排除

### 常见问题
1. **数据库连接失败**: 检查数据库配置和网络连接
2. **设备注册失败**: 检查设备ID是否重复
3. **任务执行失败**: 检查任务参数和执行器注册
4. **心跳超时**: 检查网络连接和系统负载

### 监控指标
- 设备在线状态
- 任务执行成功率
- 系统资源使用率
- 心跳延迟
- 错误日志统计

## 总结

Standalone 模式是一个完全自包含的分布式节点，它集成了任务调度、设备管理、心跳监控等所有功能。适用于单机部署或小规模分布式场景，提供了完整的任务执行和监控能力。