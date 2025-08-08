# 分布式任务系统使用指南

## 概述

本分布式任务系统为AF爬虫项目提供了可扩展的任务分发和执行能力。系统支持三种运行模式：

- **Master模式**: 主节点，负责任务分发和系统协调
- **Worker模式**: 工作节点，从主节点获取并执行任务
- **Standalone模式**: 独立模式，单机运行所有功能

## 快速开始

### 1. 初始化系统

首先运行初始化脚本来设置数据库表和配置文件：

```bash
python scripts/init_distribution.py
```

### 2. 配置系统

根据需要修改配置文件：

- `config/distribution_master.json` - 主节点配置
- `config/distribution_worker.json` - 工作节点配置  
- `config/distribution_standalone.json` - 独立模式配置

### 3. 启动系统

#### 独立模式（推荐用于开发和小规模部署）

```bash
python main.py distribute --mode standalone
```

#### 分布式模式

启动主节点：
```bash
python main.py distribute --mode master --host 0.0.0.0 --port 8000
```

启动工作节点：
```bash
python main.py distribute --mode worker --device-id worker-001 --device-name "Worker Node 1"
```

### 4. 访问Web界面

系统启动后，可以通过以下地址访问Web管理界面：

```
http://localhost:8000
```

## 系统架构

### 核心组件

1. **TaskDispatcher**: 任务分发器，负责将任务分配给可用设备
2. **DeviceManager**: 设备管理器，管理设备注册、心跳和状态
3. **TaskScheduler**: 任务调度器，根据模式执行不同的调度逻辑
4. **TaskExecutor**: 任务执行器，执行具体的爬虫任务

### 数据库表结构

- `af_device`: 设备信息表
- `af_task_assignment`: 任务分配表
- `af_device_heartbeat`: 设备心跳表
- `af_crawl_tasks`: 爬虫任务表（扩展了分布式字段）

## 配置说明

### 基本配置

```json
{
  "mode": "master|worker|standalone",
  "device_id": "设备唯一标识",
  "device_name": "设备显示名称",
  "host": "监听地址",
  "port": 8000,
  "master_url": "主节点URL（工作节点需要）"
}
```

### 任务调度配置

```json
{
  "concurrent_tasks": 3,
  "task_timeout": 3600,
  "max_retry_count": 3,
  "dispatch_interval": 10,
  "load_balance_strategy": "least_tasks|round_robin|weighted|random"
}
```

### 监控配置

```json
{
  "heartbeat_interval": 30,
  "timeout_check_interval": 60,
  "device_timeout": 300,
  "enable_monitoring": true
}
```

## API接口

### 设备管理

- `POST /api/distribution/devices/register` - 注册设备
- `GET /api/distribution/devices` - 获取设备列表
- `PUT /api/distribution/devices/{device_id}/status` - 更新设备状态
- `POST /api/distribution/devices/{device_id}/heartbeat` - 发送心跳

### 任务管理

- `POST /api/distribution/tasks` - 创建任务
- `GET /api/distribution/tasks` - 获取任务列表
- `PUT /api/distribution/tasks/{task_id}/assign` - 分配任务
- `PUT /api/distribution/tasks/{task_id}/status` - 更新任务状态
- `GET /api/distribution/tasks/pull` - 拉取任务（工作节点使用）

### 监控接口

- `GET /api/distribution/system/overview` - 系统概览
- `GET /api/distribution/devices/{device_id}/stats` - 设备统计
- `GET /api/distribution/tasks/stats` - 任务统计

## 命令行工具

### 基本命令

```bash
# 启动主节点
python main.py distribute --mode master

# 启动工作节点
python main.py distribute --mode worker --device-id worker-001

# 启动独立模式
python main.py distribute --mode standalone

# 检查系统状态
python main.py distribute --status
```

### 高级选项

```bash
# 指定配置文件
python main.py distribute --mode master --config config/my_config.json

# 指定监听地址和端口
python main.py distribute --mode master --host 0.0.0.0 --port 8080

# 设置设备信息
python main.py distribute --mode worker --device-id worker-001 --device-name "My Worker"
```

## 负载均衡策略

### 1. 轮询 (Round Robin)
按顺序将任务分配给设备

### 2. 最少任务 (Least Tasks)
优先分配给当前任务数最少的设备

### 3. 加权分配 (Weighted)
根据设备性能权重分配任务

### 4. 随机分配 (Random)
随机选择可用设备

## 监控和日志

### 系统监控

- 设备状态监控
- 任务执行监控
- 性能指标监控
- 错误率监控

### 日志级别

- `DEBUG`: 详细调试信息
- `INFO`: 一般信息
- `WARNING`: 警告信息
- `ERROR`: 错误信息
- `CRITICAL`: 严重错误

## 故障处理

### 常见问题

1. **设备离线**
   - 检查网络连接
   - 检查心跳配置
   - 查看设备日志

2. **任务超时**
   - 调整任务超时时间
   - 检查任务执行逻辑
   - 增加重试次数

3. **负载不均衡**
   - 调整负载均衡策略
   - 检查设备性能
   - 优化任务分配

### 故障恢复

系统具备自动故障恢复能力：

- 设备离线时自动释放其任务
- 任务超时时自动重新分配
- 失败任务自动重试

## 性能优化

### 1. 并发设置

根据硬件资源调整并发任务数：

```json
{
  "concurrent_tasks": 5
}
```

### 2. 心跳间隔

平衡监控精度和网络开销：

```json
{
  "heartbeat_interval": 30
}
```

### 3. 任务超时

根据任务复杂度设置合理超时时间：

```json
{
  "task_timeout": 3600
}
```

## 安全考虑

### API密钥

设置强密码作为API密钥：

```json
{
  "api_key": "your-secure-api-key-here"
}
```

### 网络安全

- 使用HTTPS（生产环境）
- 配置防火墙规则
- 限制访问IP范围

## 扩展开发

### 自定义任务执行器

```python
from executors.task_executor import TaskExecutor

class CustomTaskExecutor(TaskExecutor):
    def __init__(self):
        super().__init__("custom_task")
    
    def execute_task(self, task_data):
        # 实现自定义任务逻辑
        return {"status": "success"}
```

### 自定义负载均衡策略

```python
from services.task_dispatcher import LoadBalanceStrategy

class CustomLoadBalancer:
    def select_device(self, devices, task):
        # 实现自定义选择逻辑
        return selected_device
```

## 部署建议

### 开发环境

使用独立模式进行开发和测试：

```bash
python main.py distribute --mode standalone
```

### 生产环境

1. 部署一个主节点
2. 根据负载部署多个工作节点
3. 配置负载均衡器
4. 设置监控和告警

### Docker部署

```dockerfile
FROM python:3.9
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "main.py", "distribute", "--mode", "standalone"]
```

## 故障排除

### 检查系统状态

```bash
python main.py distribute --status
```

### 查看日志

```bash
tail -f logs/distribution.log
```

### 重置系统

```bash
python scripts/init_distribution.py
```

## 联系支持

如果遇到问题，请：

1. 查看日志文件
2. 检查配置文件
3. 参考本文档
4. 提交Issue到项目仓库