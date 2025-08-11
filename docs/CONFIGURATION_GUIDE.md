# 分布式任务系统配置指南

## 概述

本文档详细介绍分布式任务系统的配置选项，包括各个配置项的含义、用法、默认值和最佳实践建议。

## 配置文件结构

配置文件采用 JSON 格式，包含以下几个主要部分：

```json
{
  "mode": "master|worker|standalone",
  "device_id": "设备唯一标识",
  "device_name": "设备显示名称",
  "master_host": "主节点地址",
  "master_port": 8000,
  "task_timeout": 3600,
  "concurrent_tasks": 3,
  "enable_monitoring": true
}
```

## 基本配置

### 运行模式配置

#### `mode`
- **类型**: 字符串
- **可选值**: `"master"`, `"worker"`, `"standalone"`
- **默认值**: `"standalone"`
- **说明**: 节点运行模式
  - `master`: 主节点，负责任务分发和调度
  - `worker`: 工作节点，执行具体任务
  - `standalone`: 独立节点，既分发又执行任务

#### `device_id`
- **类型**: 字符串
- **默认值**: 根据模式自动生成（如 `"master-001"`）
- **说明**: 设备唯一标识符，在集群中必须唯一
- **自动生成规则**:
  - `master`: `master-{hostname}` 或 `master-{datacenter}-{hostname}`
  - `worker`: `worker-{hostname}` 或 `worker-{datacenter}-{hostname}`
  - `standalone`: `standalone-{hostname}`
- **自定义格式**: 支持数据中心和后缀，如 `"master-dc1-001"`, `"worker-server01"`
- **验证规则**: 只允许字母、数字、连字符和下划线
- **建议**: 使用有意义的命名规则，便于识别和管理

#### `device_name`
- **类型**: 字符串
- **默认值**: 根据模式和device_id自动生成
- **说明**: 设备显示名称，用于监控界面展示
- **自动生成规则**:
  - `master`: `"Master Node"` 或基于device_id的描述性名称
  - `worker`: `"Worker Node"` 或基于device_id的描述性名称
  - `standalone`: `"Standalone Node"` 或基于device_id的描述性名称
- **示例**: `"Master Node (dc1-001)"`, `"Worker Node (server01)"`

#### `device_type`
- **类型**: 字符串
- **默认值**: `"worker"`
- **说明**: 设备类型标识，用于分类和统计

## 网络配置

### 主节点连接配置

#### `master_host`
- **类型**: 字符串
- **默认值**: `"localhost"`
- **说明**: 主节点的主机地址
- **适用模式**: Worker, Standalone（作为客户端时）
- **示例**: `"192.168.1.100"`, `"master.example.com"`

#### `master_port`
- **类型**: 整数
- **默认值**: `8000`
- **说明**: 主节点的端口号
- **范围**: 1-65535

#### `master_api_prefix`
- **类型**: 字符串
- **默认值**: `"/api/distribution"`
- **说明**: API 路径前缀

### SSL/TLS 配置

#### `enable_ssl`
- **类型**: 布尔值
- **默认值**: `false`
- **说明**: 是否启用 SSL/TLS 加密

#### `ssl_cert_path`
- **类型**: 字符串
- **默认值**: `null`
- **说明**: SSL 证书文件路径
- **要求**: 当 `enable_ssl` 为 `true` 时必须提供

#### `ssl_key_path`
- **类型**: 字符串
- **默认值**: `null`
- **说明**: SSL 私钥文件路径
- **要求**: 当 `enable_ssl` 为 `true` 时必须提供

## 任务调度配置

### 时间间隔配置

#### `dispatch_interval`
- **类型**: 整数（秒）
- **默认值**: Master: `5`, Standalone: `10`
- **说明**: 任务分发检查间隔
- **适用模式**: Master, Standalone
- **建议**: 根据任务量调整，高负载时可适当减小

#### `heartbeat_interval`
- **类型**: 整数（秒）
- **默认值**: `30`
- **说明**: 心跳发送间隔
- **适用模式**: Worker, Standalone
- **建议**: 不宜过小，避免网络开销过大

#### `task_timeout_check_interval`
- **类型**: 整数（秒）
- **默认值**: `60`
- **说明**: 任务超时检查间隔
- **适用模式**: Master, Standalone

#### `device_timeout_threshold`
- **类型**: 整数（秒）
- **默认值**: `180`
- **说明**: 设备离线判定阈值
- **适用模式**: Master, Standalone
- **建议**: 设置为心跳间隔的 3-6 倍

### 负载均衡配置

#### `load_balance_strategy`
- **类型**: 字符串
- **可选值**: `"round_robin"`, `"least_tasks"`, `"weighted"`, `"random"`
- **默认值**: `"least_tasks"`
- **说明**: 负载均衡策略
  - `round_robin`: 轮询分配
  - `least_tasks`: 最少任务优先
  - `weighted`: 加权分配（基于设备性能）
  - `random`: 随机分配
- **适用模式**: Master, Standalone

#### `max_tasks_per_device`
- **类型**: 整数
- **默认值**: Master: `10`, Worker: `5`, Standalone: `5`
- **说明**: 每个设备的最大并发任务数
- **建议**: 根据设备性能调整

## 任务执行配置

### 任务处理配置

#### `default_task_timeout`
- **类型**: 整数（秒）
- **默认值**: `3600`
- **说明**: 默认任务超时时间
- **建议**: 根据任务复杂度调整

#### `task_pull_limit`
- **类型**: 整数
- **默认值**: Master: `10`, Worker: `5`, Standalone: `5`
- **说明**: 单次拉取任务的最大数量
- **适用模式**: Worker, Standalone

#### `concurrent_tasks`
- **类型**: 整数
- **默认值**: Master: `5`, Worker: `3`, Standalone: `5`
- **说明**: 并发执行任务数
- **建议**: 根据 CPU 核心数和内存大小调整

### 重试配置

#### `task_retry_delay`
- **类型**: 整数（秒）
- **默认值**: `300`
- **说明**: 任务重试延迟时间
- **建议**: 使用指数退避策略

#### `max_retry_count`
- **类型**: 整数
- **默认值**: `3`
- **说明**: 最大重试次数
- **建议**: 根据任务重要性调整

## 监控配置

### 性能监控

#### `enable_performance_monitoring`
- **类型**: 布尔值
- **默认值**: Master: `true`, Worker: `true`, Standalone: `false`
- **说明**: 是否启用性能监控
- **影响**: 会收集 CPU、内存等系统指标

#### `heartbeat_data_retention_days`
- **类型**: 整数
- **默认值**: `7`
- **说明**: 心跳数据保留天数
- **建议**: 根据存储空间和监控需求调整

#### `assignment_data_retention_days`
- **类型**: 整数
- **默认值**: `30`
- **说明**: 任务分配记录保留天数
- **建议**: 用于审计和分析，建议保留较长时间

## 高级配置

### 自动扩展配置

#### `enable_auto_scaling`
- **类型**: 布尔值
- **默认值**: Master: `true`, Worker: `false`, Standalone: `false`
- **说明**: 是否启用自动扩展
- **适用模式**: Master

#### `auto_scaling_threshold`
- **类型**: 浮点数
- **默认值**: `0.8`
- **说明**: 自动扩展触发阈值（CPU 使用率）
- **范围**: 0.0-1.0

### 任务优先级配置

#### `enable_task_priority`
- **类型**: 布尔值
- **默认值**: `true`
- **说明**: 是否启用任务优先级

#### `high_priority_threshold`
- **类型**: 整数
- **默认值**: `8`
- **说明**: 高优先级任务阈值
- **范围**: 1-10

### 故障处理配置

#### `enable_failover`
- **类型**: 布尔值
- **默认值**: Master: `true`, Worker: `true`, Standalone: `false`
- **说明**: 是否启用故障转移

#### `failover_timeout`
- **类型**: 整数（秒）
- **默认值**: `60`
- **说明**: 故障转移超时时间

#### `enable_task_redistribution`
- **类型**: 布尔值
- **默认值**: Master: `true`, Standalone: `false`
- **说明**: 是否启用任务重分发
- **适用模式**: Master, Standalone

#### `redistribution_delay`
- **类型**: 整数（秒）
- **默认值**: `120`
- **说明**: 任务重分发延迟时间

### 安全配置

#### `api_key`
- **类型**: 字符串
- **默认值**: `null`
- **说明**: API 访问密钥
- **建议**: 生产环境中必须设置

## 配置模板

系统提供三种预定义的配置模板：

### Master 节点模板

```python
from config.distribution_config import DistributionConfig

# 使用默认模板
config = DistributionConfig.get_master_template()

# 自定义参数
config = DistributionConfig.get_master_template(
    device_id="master-prod",
    device_name="生产环境主节点",
    dispatch_interval=3,
    max_tasks_per_device=20
)
```

### Worker 节点模板

```python
# 使用默认模板
config = DistributionConfig.get_worker_template()

# 自定义参数
config = DistributionConfig.get_worker_template(
    device_id="worker-01",
    device_name="工作节点1",
    master_host="192.168.1.100",
    master_port=8000,
    concurrent_tasks=8
)
```

### Standalone 节点模板

```python
# 使用默认模板
config = DistributionConfig.get_standalone_template()

# 自定义参数
config = DistributionConfig.get_standalone_template(
    device_id="standalone-dev",
    device_name="开发环境独立节点",
    concurrent_tasks=10,
    enable_performance_monitoring=True
)
```

## 配置文件操作

### 保存配置

```python
# 创建配置
config = DistributionConfig.get_master_template(
    device_id="master-001",
    device_name="主节点"
)

# 保存到文件
config.save_to_file("config/master.json")
```

### 加载配置

```python
# 从文件加载
config = DistributionConfig.load_from_file("config/master.json")

# 验证配置
if config.validate():
    print("配置有效")
```

### 使用函数方式

```python
from config.distribution_config import (
    load_distribution_config_from_file,
    save_distribution_config_to_file
)

# 加载配置
config = load_distribution_config_from_file("config/worker.json")

# 保存配置
save_distribution_config_to_file(config, "config/worker_backup.json")
```

## 最佳实践

### 1. 环境分离

为不同环境使用不同的配置文件：

```
config/
├── development.json    # 开发环境
├── testing.json       # 测试环境
├── staging.json       # 预发布环境
└── production.json    # 生产环境
```

### 2. 性能调优

#### CPU 密集型任务
```json
{
  "concurrent_tasks": 4,
  "max_tasks_per_device": 8,
  "task_timeout": 7200
}
```

#### I/O 密集型任务
```json
{
  "concurrent_tasks": 10,
  "max_tasks_per_device": 20,
  "task_timeout": 1800
}
```

### 3. 高可用配置

```json
{
  "enable_failover": true,
  "enable_task_redistribution": true,
  "heartbeat_interval": 15,
  "device_timeout_threshold": 90,
  "max_retry_count": 5
}
```

### 4. 安全配置

```json
{
  "enable_ssl": true,
  "ssl_cert_path": "/etc/ssl/certs/server.crt",
  "ssl_key_path": "/etc/ssl/private/server.key",
  "api_key": "your-secure-api-key"
}
```

### 5. 监控配置

```json
{
  "enable_performance_monitoring": true,
  "heartbeat_data_retention_days": 30,
  "assignment_data_retention_days": 90
}
```

## 故障排除

### 常见配置错误

1. **Worker 无法连接 Master**
   - 检查 `master_host` 和 `master_port`
   - 确认网络连通性
   - 检查防火墙设置

2. **任务执行超时**
   - 调整 `default_task_timeout`
   - 检查 `concurrent_tasks` 设置
   - 监控系统资源使用情况

3. **设备频繁离线**
   - 调整 `heartbeat_interval`
   - 增加 `device_timeout_threshold`
   - 检查网络稳定性

4. **任务分发不均匀**
   - 尝试不同的 `load_balance_strategy`
   - 调整 `max_tasks_per_device`
   - 检查设备性能差异

### 配置验证

使用内置验证功能检查配置：

```python
try:
    config = DistributionConfig.load_from_file("config.json")
    config.validate()
    print("配置验证通过")
except ValueError as e:
    print(f"配置错误: {e}")
```

## 总结

合理的配置是分布式任务系统稳定运行的基础。建议：

1. 从模板开始，根据实际需求调整
2. 在测试环境中验证配置
3. 监控系统运行状态，及时调优
4. 定期备份配置文件
5. 使用版本控制管理配置变更

通过本指南，您应该能够为不同的部署场景创建合适的配置，并根据实际运行情况进行优化调整。

## CLI 命令使用说明

### 基本命令格式

```bash
# 启动主节点
python main.py distribute master [--device-id DEVICE_ID] [其他选项]

# 启动工作节点
python main.py distribute worker [--device-id DEVICE_ID] [其他选项]

# 启动独立模式
python main.py distribute standalone [--device-id DEVICE_ID] [其他选项]

# 生成配置文件
python main.py distribute config [--device-id DEVICE_ID] [其他选项]
```

### device_id 参数说明

#### 自动生成（推荐）

当不提供 `--device-id` 参数时，系统会自动生成：

```bash
# 自动生成 device_id
python main.py distribute master
# 生成示例: master-mk (基于主机名)

python main.py distribute worker
# 生成示例: worker-mk

python main.py distribute standalone
# 生成示例: standalone-mk
```

#### 手动指定

```bash
# 指定自定义 device_id
python main.py distribute master --device-id master-prod-001
python main.py distribute worker --device-id worker-dc1-server01
python main.py distribute standalone --device-id standalone-dev
```

#### 高级生成选项

通过环境变量控制生成行为：

```bash
# 指定数据中心
export DATACENTER=dc1
python main.py distribute master
# 生成: master-dc1-mk

# 指定自定义后缀
export DEVICE_SUFFIX=001
python main.py distribute worker
# 生成: worker-mk-001

# 直接指定 device_id
export DEVICE_ID=my-custom-device
python main.py distribute master
# 使用: my-custom-device
```

### 配置文件生成

```bash
# 生成主节点配置（自动生成 device_id）
python main.py distribute config --mode master --output config/master.json

# 生成工作节点配置（指定 device_id）
python main.py distribute config --mode worker --device-id worker-001 --output config/worker.json

# 生成独立模式配置
python main.py distribute config --mode standalone --output config/standalone.json
```

### 验证和调试

```bash
# 检查系统状态
python main.py distribute status

# 验证配置文件
python main.py distribute config --validate config/master.json

# 显示当前配置
python main.py distribute config --show
```

### 常用命令示例

```bash
# 快速启动开发环境
python main.py distribute standalone

# 启动生产环境主节点
python main.py distribute master --device-id master-prod --host 0.0.0.0 --port 8000

# 启动工作节点连接到主节点
python main.py distribute worker --master-host 192.168.1.100 --master-port 8000

# 生成完整的配置文件
python main.py distribute config --mode master --device-id master-001 --device-name "生产主节点" --output config/production.json
```