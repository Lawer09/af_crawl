# 启动脚本使用说明

本项目提供了多个启动脚本，支持传统模式和新增的分布式模式。

## 脚本文件说明

### 1. `run_simple.sh` (推荐)
基于原始脚本结构，添加了分布式模式支持的简化版本。
- 保持原有的环境检查和虚拟环境管理逻辑
- 添加了完整的分布式命令支持
- 提供详细的帮助信息和命令验证
- 兼容原有的使用习惯

### 2. `run.sh`
功能完整的高级版本，提供更多特性。
- 模块化的函数设计
- 更详细的错误处理
- 交互式命令输入
- 完整的参数验证

### 3. `run.ps1`
Windows PowerShell版本，适用于Windows环境。
- 现代化的PowerShell语法
- 完整的错误处理和日志记录
- 实时输出监控
- 跨平台兼容性

### 4. `run.bat`
Windows批处理版本，适用于传统Windows环境。
- 兼容旧版Windows系统
- 简单易用的批处理语法
- 完整的命令支持

## 支持的命令模式

### 传统模式
```bash
# 同步用户应用列表
./run_simple.sh sync_apps

# 同步应用数据（默认1天）
./run_simple.sh sync_data

# 同步最近7天的数据
./run_simple.sh sync_data --days 7

# 启动Web管理界面
./run_simple.sh web
```

### 分布式模式

#### 主节点 (Master)
```bash
# 启动主节点（默认配置）
./run_simple.sh distribute master

# 指定设备ID和端口
./run_simple.sh distribute master --device-id master-001 --port 7989

# 指定监听地址和设备名称
./run_simple.sh distribute master --host 0.0.0.0 --device-name "主控节点"

# 使用配置文件
./run_simple.sh distribute master --config config/master.yaml
```

#### 工作节点 (Worker)
```bash
# 连接到本地主节点
./run_simple.sh distribute worker --master-host localhost

# 连接到远程主节点
./run_simple.sh distribute worker --master-host 192.168.1.100 --master-port 7989

# 指定工作节点信息
./run_simple.sh distribute worker --device-id worker-001 --device-name "工作节点1" --master-host 192.168.1.100

# 使用配置文件
./run_simple.sh distribute worker --config config/worker.yaml
```

#### 独立节点 (Standalone)
```bash
# 启动独立节点（默认配置）
./run_simple.sh distribute standalone

# 指定并发任务数和分发间隔
./run_simple.sh distribute standalone --concurrent-tasks 3 --dispatch-interval 5

# 启用性能监控
./run_simple.sh distribute standalone --enable-monitoring --device-name "独立节点"

# 使用配置文件
./run_simple.sh distribute standalone --config config/standalone.yaml
```

#### 状态查询 (Status)
```bash
# 查询本地主节点状态
./run_simple.sh distribute status

# 查询远程主节点状态
./run_simple.sh distribute status --master-host 192.168.1.100 --master-port 7989
```

## 分布式模式参数说明

### 通用参数
- `--device-id ID`: 设备唯一标识符（可选，未提供时自动生成）
- `--device-name NAME`: 设备显示名称
- `--config FILE`: 配置文件路径

### 主节点专用参数
- `--host HOST`: 监听地址（默认：localhost）
- `--port PORT`: 监听端口（默认：7989）

### 工作节点专用参数
- `--master-host HOST`: 主节点地址（必需）
- `--master-port PORT`: 主节点端口（默认：7989）

### 独立节点专用参数
- `--dispatch-interval N`: 任务分发间隔秒数（默认：10）
- `--concurrent-tasks N`: 并发任务数（默认：5）
- `--enable-monitoring`: 启用性能监控

### 状态查询专用参数
- `--master-host HOST`: 要查询的主节点地址
- `--master-port PORT`: 要查询的主节点端口

## 使用建议

### 开发环境
推荐使用 `run_simple.sh`，它保持了原有脚本的简洁性，同时支持所有新功能。

### 生产环境
- Linux/macOS: 使用 `run.sh` 或 `run_simple.sh`
- Windows: 使用 `run.ps1`（推荐）或 `run.bat`

### 分布式部署建议

1. **小规模部署**（1-3个节点）
   ```bash
   # 节点1：独立节点
   ./run_simple.sh distribute standalone --concurrent-tasks 5
   ```

2. **中等规模部署**（4-10个节点）
   ```bash
   # 主节点
   ./run_simple.sh distribute master --host 0.0.0.0 --port 7989
   
   # 工作节点（在其他机器上）
   ./run_simple.sh distribute worker --master-host 主节点IP --device-name "工作节点1"
   ```

3. **大规模部署**（10+个节点）
   建议使用配置文件管理，并启用监控：
   ```bash
   # 主节点
   ./run_simple.sh distribute master --config config/production_master.yaml
   
   # 工作节点
   ./run_simple.sh distribute worker --config config/production_worker.yaml
   ```

## 日志和监控

所有脚本都会生成日志文件 `run.log`，包含：
- 时间戳
- 执行状态
- 错误信息
- Python脚本输出

可以使用以下命令实时查看日志：
```bash
tail -f run.log
```

## 故障排除

### 常见问题

1. **Python环境问题**
   - 确保Python 3.7+已安装
   - 检查虚拟环境是否正确创建
   - 验证依赖包是否安装完整

2. **网络连接问题**
   - 检查防火墙设置
   - 验证端口是否被占用
   - 确认主节点地址和端口正确

3. **权限问题**
   - 确保脚本有执行权限：`chmod +x run_simple.sh`
   - 检查日志文件写入权限

4. **配置问题**
   - 验证配置文件格式正确
   - 检查配置文件路径是否存在
   - 确认配置参数有效

### 调试模式

可以通过设置环境变量启用详细日志：
```bash
export DEBUG=1
./run_simple.sh distribute master
```

## 迁移指南

### 从原始脚本迁移

如果你之前使用的是原始的启动脚本，可以无缝迁移到新脚本：

```bash
# 原来的用法
./old_script.sh sync_apps
./old_script.sh sync_data

# 新的用法（完全兼容）
./run_simple.sh sync_apps
./run_simple.sh sync_data

# 新增的分布式功能
./run_simple.sh distribute master
./run_simple.sh distribute worker --master-host 192.168.1.100
```

所有原有的参数和功能都保持不变，只是增加了新的分布式模式支持。