# 任务管理指南

本文档介绍AF爬虫系统的任务管理功能，包括任务初始化、维护、监控等操作。

## 📋 目录

- [快速开始](#快速开始)
- [任务管理工具](#任务管理工具)
- [任务初始化脚本](#任务初始化脚本)
- [常用操作](#常用操作)
- [故障排除](#故障排除)

## 🚀 快速开始

### 方式一：使用图形化管理工具（推荐）

```bash
# Windows用户
task_manager.bat

# 或者直接运行
python scripts/task_manager.py
```

### 方式二：使用命令行工具

```bash
# 查看帮助
python scripts/init_tasks.py --help

# 初始化用户应用任务
python scripts/init_tasks.py --init-user-apps

# 初始化应用数据任务
python scripts/init_tasks.py --init-app-data --days 7
```

## 🛠️ 任务管理工具

### 图形化管理界面

任务管理工具提供了友好的交互界面，包含以下功能模块：

#### 1. 任务仪表板
- 📊 实时任务统计
- 📈 最近24小时任务趋势
- 📋 按任务类型分类统计
- 🎯 成功率分析

#### 2. 任务初始化
- 🚀 初始化用户应用同步任务
- 📱 初始化应用数据同步任务
- 🔄 批量初始化所有任务

#### 3. 任务维护
- 🔧 重置失败任务
- ⏰ 恢复超时任务
- 🗑️ 清理过期任务
- 🔄 重置所有任务

#### 4. 任务监控
- 📡 实时监控
- 🔄 查看运行中任务
- ❌ 查看失败任务
- ⏰ 查看超时任务

#### 5. 批量操作
- 📦 批量创建用户应用任务
- 📊 批量创建应用数据任务
- 🔄 批量重置指定用户任务
- 🗑️ 批量删除指定类型任务

#### 6. 自定义任务
- 🎯 创建单个任务
- 📋 查看任务详情
- ⚡ 修改任务优先级

## 📜 任务初始化脚本

### 基本用法

```bash
# 显示帮助信息
python scripts/init_tasks.py --help

# 初始化用户应用任务
python scripts/init_tasks.py --init-user-apps

# 初始化应用数据任务（默认1天）
python scripts/init_tasks.py --init-app-data

# 初始化应用数据任务（指定天数）
python scripts/init_tasks.py --init-app-data --days 7

# 强制重新创建任务
python scripts/init_tasks.py --init-user-apps --force
```

### 任务维护

```bash
# 重置失败任务
python scripts/init_tasks.py --reset-failed

# 重置指定类型的失败任务
python scripts/init_tasks.py --reset-failed --task-type user_apps

# 恢复超时任务（默认2小时）
python scripts/init_tasks.py --recover-timeout

# 恢复超时任务（指定小时数）
python scripts/init_tasks.py --recover-timeout --timeout-hours 4

# 清理过期任务（默认30天）
python scripts/init_tasks.py --clean-old

# 清理过期任务（指定天数）
python scripts/init_tasks.py --clean-old --days 7
```

### 任务查询

```bash
# 查看任务统计
python scripts/init_tasks.py --stats

# 创建自定义任务
python scripts/init_tasks.py --create-task \
  --task-type app_data \
  --username test@example.com \
  --app-id com.example.app \
  --start-date 2024-01-01 \
  --end-date 2024-01-01 \
  --priority 1
```

## 🔧 常用操作

### 1. 日常任务初始化

```bash
# 每日初始化流程
python scripts/init_tasks.py --init-user-apps
python scripts/init_tasks.py --init-app-data --days 1
```

### 2. 批量数据补录

```bash
# 补录过去7天的数据
python scripts/init_tasks.py --init-app-data --days 7 --force
```

### 3. 系统维护

```bash
# 重置失败任务
python scripts/init_tasks.py --reset-failed

# 恢复超时任务
python scripts/init_tasks.py --recover-timeout

# 清理30天前的任务
python scripts/init_tasks.py --clean-old --days 30
```

### 4. 监控和统计

```bash
# 查看任务统计
python scripts/init_tasks.py --stats

# 使用图形化界面监控
python scripts/task_manager.py
```

## 📊 任务类型说明

### user_apps 任务
- **用途**: 同步用户的应用列表
- **频率**: 通常每日执行一次
- **数据**: 获取用户安装的应用信息

### app_data 任务
- **用途**: 同步应用的使用数据
- **频率**: 按日期范围执行
- **数据**: 获取应用的使用时长、启动次数等

## 🔍 故障排除

### 常见问题

#### 1. 数据库连接失败
```
❌ 数据库连接失败: (2003, "Can't connect to MySQL server")
```
**解决方案**:
- 检查数据库服务是否启动
- 验证数据库连接配置
- 确认网络连接正常

#### 2. 任务创建失败
```
❌ 创建任务失败: 用户不存在或应用无效
```
**解决方案**:
- 检查用户是否存在且已启用
- 验证应用ID是否正确
- 确认用户应用关联关系

#### 3. 任务执行超时
```
⏰ 发现 X 个超时任务
```
**解决方案**:
- 使用恢复超时任务功能
- 检查执行设备状态
- 调整任务超时时间

### 日志查看

```bash
# 查看任务执行日志
tail -f logs/task_scheduler.log

# 查看设备管理日志
tail -f logs/device_manager.log

# 查看API日志
tail -f logs/api.log
```

### 性能优化

#### 1. 任务分批处理
```bash
# 分批初始化大量任务
python scripts/init_tasks.py --init-app-data --days 1
# 等待一段时间后继续
python scripts/init_tasks.py --init-app-data --days 1 --force
```

#### 2. 调整并发数
```bash
# 在standalone模式下调整并发任务数
python main.py distribute standalone --device-id worker-01 --concurrent-tasks 5
```

#### 3. 优先级管理
```bash
# 创建高优先级任务
python scripts/init_tasks.py --create-task \
  --task-type user_apps \
  --username important@example.com \
  --priority 10
```

## 📈 最佳实践

### 1. 定时任务设置

建议设置以下定时任务：

```bash
# 每日凌晨2点初始化用户应用任务
0 2 * * * cd /path/to/af_crawl && python scripts/init_tasks.py --init-user-apps

# 每日凌晨3点初始化应用数据任务
0 3 * * * cd /path/to/af_crawl && python scripts/init_tasks.py --init-app-data --days 1

# 每小时重置失败任务
0 * * * * cd /path/to/af_crawl && python scripts/init_tasks.py --reset-failed

# 每日清理过期任务
0 4 * * * cd /path/to/af_crawl && python scripts/init_tasks.py --clean-old --days 30
```

### 2. 监控告警

```bash
# 检查失败任务数量
failed_count=$(python scripts/init_tasks.py --stats | grep "failed" | awk '{print $2}')
if [ $failed_count -gt 100 ]; then
    echo "警告：失败任务数量过多 ($failed_count)"
fi
```

### 3. 数据备份

```bash
# 备份任务数据
mysqldump -u username -p database_name cl_crawl_tasks > tasks_backup_$(date +%Y%m%d).sql
```

## 🔗 相关文档

- [任务流程指南](TASK_FLOW_GUIDE.md)
- [分布式系统设计](task_distribution_design.md)
- [独立模式执行流程](STANDALONE_EXECUTION_FLOW.md)
- [API文档](../api/README.md)