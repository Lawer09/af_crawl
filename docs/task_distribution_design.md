# 任务分发系统设计文档

## 1. 概述

### 1.1 背景
当前 AppsFlyer 数据爬虫系统采用单机任务调度模式，所有任务在单个节点上执行。为了提高系统的处理能力和可扩展性，需要设计一个分布式任务分发系统，支持多设备协同工作。

### 1.2 目标
- 实现主从设备架构的任务分发机制
- 提高任务处理的并发能力和系统吞吐量
- 支持动态扩容和故障转移
- 保证任务的可靠性和一致性

### 1.3 设计原则
- **高可用性**: 主设备故障时能够快速恢复
- **负载均衡**: 任务在各设备间均匀分配
- **容错性**: 设备故障不影响整体系统运行
- **可扩展性**: 支持动态添加和移除设备
- **一致性**: 确保任务不重复执行或丢失

## 2. 系统架构

### 2.1 整体架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   主设备 (Master)  │    │   从设备 (Worker)  │    │   从设备 (Worker)  │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ 分发服务     │ │    │ │ 任务执行器   │ │    │ │ 任务执行器   │ │
│ │ (Dispatcher) │ │    │ │ (Executor)   │ │    │ │ (Executor)   │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ 任务调度器   │ │    │ │ 心跳服务     │ │    │ │ 心跳服务     │ │
│ │ (Scheduler)  │ │    │ │ (Heartbeat)  │ │    │ │ (Heartbeat)  │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │                 │    │                 │
│ │ 监控服务     │ │    │                 │    │                 │
│ │ (Monitor)    │ │    │                 │    │                 │
│ └─────────────┘ │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   共享数据库     │
                    │   (MySQL)       │
                    │                 │
                    │ ┌─────────────┐ │
                    │ │ 任务队列表   │ │
                    │ │ 设备注册表   │ │
                    │ │ 任务分配表   │ │
                    │ │ 心跳记录表   │ │
                    │ └─────────────┘ │
                    └─────────────────┘
```

### 2.2 核心组件

#### 2.2.1 主设备 (Master)
- **分发服务 (Dispatcher)**: 负责任务分发和负载均衡
- **任务调度器 (Scheduler)**: 生成和管理任务
- **监控服务 (Monitor)**: 监控系统状态和设备健康

#### 2.2.2 从设备 (Worker)
- **任务执行器 (Executor)**: 执行具体的爬虫任务
- **心跳服务 (Heartbeat)**: 维持与主设备的连接

#### 2.2.3 共享存储
- **任务队列表**: 存储待执行的任务
- **设备注册表**: 记录所有设备信息
- **任务分配表**: 记录任务分配关系
- **心跳记录表**: 记录设备心跳信息

## 3. 数据库设计

### 3.1 设备注册表 (af_device)

```sql
CREATE TABLE af_device (
    id INT PRIMARY KEY AUTO_INCREMENT,
    device_id VARCHAR(64) UNIQUE NOT NULL COMMENT '设备唯一标识',
    device_name VARCHAR(128) NOT NULL COMMENT '设备名称',
    device_type ENUM('master', 'worker') NOT NULL COMMENT '设备类型',
    ip_address VARCHAR(45) NOT NULL COMMENT 'IP地址',
    port INT NOT NULL COMMENT '端口号',
    status ENUM('online', 'offline', 'busy') DEFAULT 'offline' COMMENT '设备状态',
    capabilities JSON COMMENT '设备能力配置',
    max_concurrent_tasks INT DEFAULT 5 COMMENT '最大并发任务数',
    current_tasks INT DEFAULT 0 COMMENT '当前任务数',
    last_heartbeat DATETIME COMMENT '最后心跳时间',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_device_type (device_type),
    INDEX idx_status (status),
    INDEX idx_last_heartbeat (last_heartbeat)
);
```

### 3.2 任务分配表 (af_task_assignment)

```sql
CREATE TABLE af_task_assignment (
    id INT PRIMARY KEY AUTO_INCREMENT,
    task_id INT NOT NULL COMMENT '任务ID',
    device_id VARCHAR(64) NOT NULL COMMENT '分配的设备ID',
    assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '分配时间',
    started_at DATETIME COMMENT '开始执行时间',
    completed_at DATETIME COMMENT '完成时间',
    status ENUM('assigned', 'running', 'completed', 'failed', 'timeout') DEFAULT 'assigned',
    retry_count INT DEFAULT 0 COMMENT '重试次数',
    error_message TEXT COMMENT '错误信息',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES af_crawl_task(id),
    FOREIGN KEY (device_id) REFERENCES af_device(device_id),
    INDEX idx_task_id (task_id),
    INDEX idx_device_id (device_id),
    INDEX idx_status (status),
    INDEX idx_assigned_at (assigned_at)
);
```

### 3.3 心跳记录表 (af_device_heartbeat)

```sql
CREATE TABLE af_device_heartbeat (
    id INT PRIMARY KEY AUTO_INCREMENT,
    device_id VARCHAR(64) NOT NULL,
    heartbeat_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    cpu_usage DECIMAL(5,2) COMMENT 'CPU使用率',
    memory_usage DECIMAL(5,2) COMMENT '内存使用率',
    disk_usage DECIMAL(5,2) COMMENT '磁盘使用率',
    active_tasks INT DEFAULT 0 COMMENT '活跃任务数',
    system_info JSON COMMENT '系统信息',
    FOREIGN KEY (device_id) REFERENCES af_device(device_id),
    INDEX idx_device_id (device_id),
    INDEX idx_heartbeat_time (heartbeat_time)
);
```

### 3.4 扩展现有任务表

```sql
-- 为 af_crawl_task 表添加分发相关字段
ALTER TABLE af_crawl_task ADD COLUMN priority INT DEFAULT 5 COMMENT '任务优先级(1-10)';
ALTER TABLE af_crawl_task ADD COLUMN estimated_duration INT COMMENT '预估执行时间(秒)';
ALTER TABLE af_crawl_task ADD COLUMN required_capabilities JSON COMMENT '所需设备能力';
ALTER TABLE af_crawl_task ADD COLUMN max_retry_count INT DEFAULT 3 COMMENT '最大重试次数';
```

## 4. 核心服务设计

### 4.1 分发服务 (Dispatcher)

#### 4.1.1 服务接口

```python
class TaskDispatcher:
    """任务分发服务"""
    
    def __init__(self):
        self.load_balancer = LoadBalancer()
        self.device_manager = DeviceManager()
        
    async def get_task(self, device_id: str) -> Optional[Dict]:
        """设备请求任务"""
        
    async def submit_result(self, task_id: int, device_id: str, result: Dict) -> bool:
        """提交任务结果"""
        
    async def report_heartbeat(self, device_id: str, status: Dict) -> bool:
        """设备心跳上报"""
        
    async def register_device(self, device_info: Dict) -> bool:
        """设备注册"""
```

#### 4.1.2 负载均衡策略

```python
class LoadBalancer:
    """负载均衡器"""
    
    def select_device(self, task: Dict, available_devices: List[Dict]) -> Optional[str]:
        """选择最适合的设备执行任务"""
        # 策略1: 最少任务数优先
        # 策略2: 设备能力匹配
        # 策略3: 任务优先级考虑
        # 策略4: 地理位置就近原则
        
    def calculate_device_score(self, device: Dict, task: Dict) -> float:
        """计算设备适合度分数"""
        score = 0.0
        
        # 负载权重 (40%)
        load_factor = 1 - (device['current_tasks'] / device['max_concurrent_tasks'])
        score += load_factor * 0.4
        
        # 能力匹配权重 (30%)
        capability_match = self._check_capability_match(device, task)
        score += capability_match * 0.3
        
        # 历史成功率权重 (20%)
        success_rate = self._get_device_success_rate(device['device_id'])
        score += success_rate * 0.2
        
        # 响应时间权重 (10%)
        response_time_factor = self._get_response_time_factor(device['device_id'])
        score += response_time_factor * 0.1
        
        return score
```

### 4.2 设备管理服务

```python
class DeviceManager:
    """设备管理服务"""
    
    def __init__(self):
        self.heartbeat_timeout = 60  # 心跳超时时间(秒)
        
    async def register_device(self, device_info: Dict) -> bool:
        """注册设备"""
        
    async def update_device_status(self, device_id: str, status: str) -> bool:
        """更新设备状态"""
        
    async def get_available_devices(self) -> List[Dict]:
        """获取可用设备列表"""
        
    async def check_device_health(self) -> None:
        """检查设备健康状态"""
        current_time = datetime.now()
        timeout_devices = DeviceDAO.get_timeout_devices(current_time, self.heartbeat_timeout)
        
        for device in timeout_devices:
            await self._handle_device_timeout(device)
            
    async def _handle_device_timeout(self, device: Dict) -> None:
        """处理设备超时"""
        # 1. 标记设备为离线
        DeviceDAO.update_status(device['device_id'], 'offline')
        
        # 2. 重新分配该设备的任务
        assigned_tasks = TaskAssignmentDAO.get_device_running_tasks(device['device_id'])
        for task in assigned_tasks:
            await self._reassign_task(task)
```

### 4.3 任务调度服务

```python
class TaskScheduler:
    """任务调度服务"""
    
    def __init__(self):
        self.dispatcher = TaskDispatcher()
        
    async def schedule_tasks(self) -> None:
        """调度任务"""
        # 1. 获取待分配的任务
        pending_tasks = CrawlTaskDAO.get_pending_tasks()
        
        # 2. 按优先级排序
        sorted_tasks = sorted(pending_tasks, key=lambda x: x['priority'], reverse=True)
        
        # 3. 分配任务
        for task in sorted_tasks:
            await self._assign_task(task)
            
    async def _assign_task(self, task: Dict) -> bool:
        """分配单个任务"""
        available_devices = await self.dispatcher.device_manager.get_available_devices()
        
        if not available_devices:
            logger.warning(f"No available devices for task {task['id']}")
            return False
            
        # 选择最适合的设备
        selected_device = self.dispatcher.load_balancer.select_device(task, available_devices)
        
        if selected_device:
            # 创建任务分配记录
            TaskAssignmentDAO.create_assignment(task['id'], selected_device)
            # 更新设备任务计数
            DeviceDAO.increment_task_count(selected_device)
            return True
            
        return False
```

## 5. API 设计

### 5.1 分发服务 API

#### 5.1.1 设备注册
```http
POST /api/v1/devices/register
Content-Type: application/json

{
    "device_id": "worker-001",
    "device_name": "Worker Node 1",
    "device_type": "worker",
    "ip_address": "192.168.1.100",
    "port": 8001,
    "capabilities": {
        "supported_tasks": ["sync_apps", "sync_data"],
        "max_concurrent": 5,
        "proxy_support": true
    }
}
```

#### 5.1.2 获取任务
```http
GET /api/v1/tasks/next?device_id=worker-001

Response:
{
    "task_id": 12345,
    "task_type": "sync_data",
    "task_data": {
        "username": "user@example.com",
        "app_id": "com.example.app",
        "start_date": "2024-01-01",
        "end_date": "2024-01-01"
    },
    "priority": 7,
    "timeout": 1800,
    "retry_count": 0
}
```

#### 5.1.3 提交结果
```http
POST /api/v1/tasks/{task_id}/result
Content-Type: application/json

{
    "device_id": "worker-001",
    "status": "completed",
    "result": {
        "records_processed": 1500,
        "execution_time": 120
    },
    "error_message": null
}
```

#### 5.1.4 心跳上报
```http
POST /api/v1/devices/{device_id}/heartbeat
Content-Type: application/json

{
    "timestamp": "2024-01-01T12:00:00Z",
    "status": "online",
    "system_info": {
        "cpu_usage": 45.2,
        "memory_usage": 68.5,
        "disk_usage": 32.1,
        "active_tasks": 3
    }
}
```

### 5.2 管理 API

#### 5.2.1 设备列表
```http
GET /api/v1/admin/devices

Response:
{
    "devices": [
        {
            "device_id": "master-001",
            "device_name": "Master Node",
            "device_type": "master",
            "status": "online",
            "current_tasks": 0,
            "last_heartbeat": "2024-01-01T12:00:00Z"
        },
        {
            "device_id": "worker-001",
            "device_name": "Worker Node 1",
            "device_type": "worker",
            "status": "online",
            "current_tasks": 3,
            "last_heartbeat": "2024-01-01T12:00:00Z"
        }
    ]
}
```

#### 5.2.2 任务分配状态
```http
GET /api/v1/admin/tasks/assignments

Response:
{
    "assignments": [
        {
            "task_id": 12345,
            "device_id": "worker-001",
            "status": "running",
            "assigned_at": "2024-01-01T11:55:00Z",
            "started_at": "2024-01-01T11:56:00Z"
        }
    ]
}
```

## 6. 配置设计

### 6.1 主设备配置

```python
# config/distribution.py

DISTRIBUTION = {
    # 分发服务配置
    'enabled': True,
    'mode': 'master',  # master | worker
    
    # 服务器配置
    'server': {
        'host': '0.0.0.0',
        'port': 8000,
        'workers': 4
    },
    
    # 主设备特有配置
    'master': {
        'scheduler_interval': 30,  # 任务调度间隔(秒)
        'health_check_interval': 60,  # 健康检查间隔(秒)
        'task_timeout': 1800,  # 任务超时时间(秒)
        'max_retry_count': 3,  # 最大重试次数
    },
    
    # 负载均衡配置
    'load_balancer': {
        'strategy': 'least_tasks',  # least_tasks | round_robin | weighted
        'weights': {
            'load_factor': 0.4,
            'capability_match': 0.3,
            'success_rate': 0.2,
            'response_time': 0.1
        }
    }
}
```

### 6.2 从设备配置

```python
# 从设备配置
DISTRIBUTION = {
    'enabled': True,
    'mode': 'worker',
    
    # 主设备连接配置
    'master': {
        'host': '192.168.1.10',
        'port': 8000,
        'api_key': 'your-api-key'
    },
    
    # 从设备配置
    'worker': {
        'device_id': 'worker-001',
        'device_name': 'Worker Node 1',
        'max_concurrent_tasks': 5,
        'heartbeat_interval': 30,  # 心跳间隔(秒)
        'task_poll_interval': 10,  # 任务轮询间隔(秒)
        'capabilities': {
            'supported_tasks': ['sync_apps', 'sync_data'],
            'proxy_support': True,
            'browser_support': True
        }
    }
}
```

## 7. 实现计划

### 7.1 第一阶段：基础架构
- [ ] 数据库表结构设计和创建
- [ ] 设备注册和管理模块
- [ ] 基础的分发服务 API
- [ ] 心跳机制实现

### 7.2 第二阶段：任务分发
- [ ] 任务分配算法实现
- [ ] 负载均衡策略
- [ ] 任务状态跟踪
- [ ] 失败重试机制

### 7.3 第三阶段：监控和管理
- [ ] 设备健康监控
- [ ] 任务执行监控
- [ ] Web 管理界面
- [ ] 告警机制

### 7.4 第四阶段：优化和扩展
- [ ] 性能优化
- [ ] 故障转移机制
- [ ] 动态扩容支持
- [ ] 安全机制完善

## 8. 部署方案

### 8.1 主设备部署
```bash
# 启动主设备
python main.py --mode=master

# 或使用配置文件
export DISTRIBUTION_MODE=master
python main.py
```

### 8.2 从设备部署
```bash
# 启动从设备
python main.py --mode=worker --master-host=192.168.1.10

# 或使用配置文件
export DISTRIBUTION_MODE=worker
export MASTER_HOST=192.168.1.10
python main.py
```

### 8.3 Docker 部署
```dockerfile
# Dockerfile
FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

```yaml
# docker-compose.yml
version: '3.8'

services:
  master:
    build: .
    environment:
      - DISTRIBUTION_MODE=master
      - MYSQL_HOST=db
    ports:
      - "8000:8000"
    depends_on:
      - db
      
  worker1:
    build: .
    environment:
      - DISTRIBUTION_MODE=worker
      - MASTER_HOST=master
      - DEVICE_ID=worker-001
    depends_on:
      - master
      
  worker2:
    build: .
    environment:
      - DISTRIBUTION_MODE=worker
      - MASTER_HOST=master
      - DEVICE_ID=worker-002
    depends_on:
      - master
      
  db:
    image: mysql:8.0
    environment:
      - MYSQL_ROOT_PASSWORD=password
      - MYSQL_DATABASE=af_crawl
    volumes:
      - mysql_data:/var/lib/mysql
      
volumes:
  mysql_data:
```

## 9. 监控和运维

### 9.1 监控指标
- **设备指标**: 在线设备数、设备负载、心跳延迟
- **任务指标**: 任务队列长度、执行成功率、平均执行时间
- **系统指标**: CPU、内存、网络使用率
- **业务指标**: 数据同步量、错误率、SLA 达成率

### 9.2 告警规则
- 设备离线超过 5 分钟
- 任务失败率超过 10%
- 任务队列积压超过 100 个
- 系统资源使用率超过 80%

### 9.3 日志管理
```python
# 分布式日志配置
LOGGING = {
    'version': 1,
    'handlers': {
        'distributed': {
            'class': 'logging.handlers.SysLogHandler',
            'address': ('log-server', 514),
            'formatter': 'distributed'
        }
    },
    'formatters': {
        'distributed': {
            'format': '[{device_id}] {asctime} {levelname} {name} - {message}',
            'style': '{'
        }
    }
}
```

## 10. 安全考虑

### 10.1 认证和授权
- API 密钥认证
- 设备证书验证
- 角色权限控制

### 10.2 网络安全
- HTTPS 通信
- 内网隔离
- 防火墙配置

### 10.3 数据安全
- 敏感数据加密
- 访问日志记录
- 数据备份策略

## 11. 性能优化

### 11.1 数据库优化
- 索引优化
- 分区表设计
- 连接池调优

### 11.2 网络优化
- 连接复用
- 数据压缩
- 批量操作

### 11.3 缓存策略
- Redis 缓存热点数据
- 本地缓存设备信息
- 任务结果缓存

## 12. 故障处理

### 12.1 主设备故障
- 自动故障检测
- 备用主设备切换
- 数据一致性保证

### 12.2 从设备故障
- 任务重新分配
- 设备自动重连
- 故障设备隔离

### 12.3 网络故障
- 断线重连机制
- 离线任务缓存
- 网络恢复后同步

## 13. 测试策略

### 13.1 单元测试
- 核心算法测试
- API 接口测试
- 数据库操作测试

### 13.2 集成测试
- 主从设备通信测试
- 任务分发流程测试
- 故障恢复测试

### 13.3 性能测试
- 并发任务处理能力
- 系统资源消耗
- 网络延迟影响

### 13.4 压力测试
- 大量设备接入
- 高频任务分发
- 异常情况模拟

## 14. 总结

本设计文档详细描述了 AppsFlyer 数据爬虫系统的分布式任务分发架构。通过主从设备模式，系统可以实现：

1. **水平扩展**: 支持动态添加工作节点
2. **负载均衡**: 智能任务分配算法
3. **高可用性**: 故障检测和自动恢复
4. **监控运维**: 完善的监控和管理机制

该架构设计遵循微服务原则，具有良好的可扩展性和维护性，能够满足大规模数据爬取的需求。