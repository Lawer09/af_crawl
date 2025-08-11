# AppsFlyer 数据爬虫系统

## 项目概述

AppsFlyer 数据爬虫系统是一个用于自动化抓取和同步 AppsFlyer 平台数据的 Python 应用程序。该系统支持多用户、多应用的数据同步，并提供 Web 管理界面进行任务管理和数据查看。

## 主要功能

### 1. 用户应用同步
- 自动获取用户的应用列表
- 支持 PID 和 Agency 两种账户类型
- 批量处理多个用户账户

### 2. 数据同步
- 定时同步应用的点击量和安装量数据
- 支持历史数据回溯（可指定天数）
- 数据迁移到报表数据库

### 3. Web 管理界面
- 用户和应用管理
- 任务状态监控
- 数据同步控制
- 实时日志查看

### 4. 任务调度
- 基于数据库的任务队列
- 失败重试机制
- 并发处理支持

## 技术架构

### 后端技术栈
- **Python 3.13+**: 主要开发语言
- **FastAPI**: Web 框架，提供 REST API
- **MySQL**: 数据存储
- **Playwright**: 浏览器自动化
- **Requests**: HTTP 客户端
- **Uvicorn**: ASGI 服务器

### 前端技术栈
- **Bootstrap 5**: UI 框架
- **Jinja2**: 模板引擎
- **JavaScript**: 前端交互

## 项目结构

```
af_crawl/
├── config/                 # 配置模块
│   ├── __init__.py
│   ├── af_config.py        # AppsFlyer API 配置
│   └── settings.py         # 主配置文件
├── core/                   # 核心模块
│   ├── db.py              # 数据库连接池
│   ├── logger.py          # 日志配置
│   ├── proxy.py           # 代理管理
│   └── session.py         # 会话管理
├── model/                  # 数据模型
│   ├── af_data.py         # AppsFlyer 数据模型
│   ├── cookie.py          # Cookie 模型
│   ├── crawl_task.py      # 爬虫任务模型
│   ├── user.py            # 用户模型
│   ├── user_app.py        # 用户应用模型
│   └── user_app_data.py   # 用户应用数据模型
├── services/               # 业务服务层
│   ├── __init__.py
│   ├── app_service.py     # 应用服务
│   ├── data_service.py    # 数据服务
│   └── login_service.py   # 登录服务
├── tasks/                  # 任务模块
│   ├── __init__.py
│   ├── sync_app_data.py   # 数据同步任务
│   └── sync_user_apps.py  # 应用同步任务
├── templates/              # 模板文件
│   └── index.html         # 主页模板
├── utils/                  # 工具模块
│   ├── __init__.py
│   └── retry.py           # 重试工具
├── static/                 # 静态资源
├── main.py                # 命令行入口
├── web_app.py             # Web 应用入口
├── requirements.txt       # 依赖包列表
├── .env                   # 环境变量配置
└── .gitignore            # Git 忽略文件
```

## 安装和配置

### 1. 环境要求
- Python 3.13+
- MySQL 5.7+
- Chrome/Chromium 浏览器（用于 Playwright）

### 2. 安装依赖
```bash
pip install -r requirements.txt
playwright install chromium
```

### 3. 环境配置
创建 `.env` 文件并配置以下参数：

```env
# 主数据库配置
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=af_crawl
MYSQL_POOL_SIZE=5

# 代理配置（可选）
USE_PROXY=false
IPWEB_TOKEN=your_proxy_token
PROXY_COUNTRY=SG
PROXY_TIMES=15
PROXY_COUNT=3
PROXY_TIMEOUT=30
PROXY_MAX_FAILURES=3

# Playwright 配置
USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
PW_HEADLESS=true
PW_SLOWMO=1000
PW_TIMEOUT=180000

# 爬虫配置
CRAWLER_PROCESSES=1
CRAWLER_THREADS=1
CRAWLER_MAX_RETRY=5
CRAWLER_RETRY_DELAY=300
```

### 4. 数据库初始化
系统会自动创建所需的数据表，包括：
- `af_user`: 用户账户表
- `af_user_app`: 用户应用表
- `af_user_app_data`: 应用数据表
- `af_data`: 报表数据表
- `af_crawl_task`: 任务队列表
- `af_cookie`: Cookie 存储表

## 使用方法

### 命令行模式

#### 同步用户应用列表
```bash
python main.py sync_apps
```

#### 同步应用数据
```bash
# 同步最近1天的数据
python main.py sync_data

# 同步最近7天的数据
python main.py sync_data --days 7
```

### Web 界面模式

#### 启动 Web 服务
```bash
python web_app.py
```

访问 `http://localhost:8000` 打开管理界面。

#### Web 界面功能
- **用户管理**: 查看已配置的用户账户
- **应用管理**: 查看用户的应用列表
- **数据查看**: 查看应用的数据统计
- **任务管理**: 查看和执行同步任务
- **手动同步**: 手动触发数据同步任务

## 核心模块说明

### 1. 配置管理 (config/)
- `settings.py`: 主配置文件，包含数据库、代理、爬虫等配置
- `af_config.py`: AppsFlyer API 相关配置

### 2. 数据库层 (core/db.py)
- 基于连接池的 MySQL 操作封装
- 支持主数据库和报表数据库双连接
- 提供 select、execute、executemany 等基础操作

### 3. 数据模型 (model/)
- 使用 DAO 模式封装数据库操作
- 每个模型对应一个数据表
- 提供 CRUD 操作和业务逻辑

### 4. 业务服务 (services/)
- `login_service.py`: 处理 AppsFlyer 登录和会话管理
- `app_service.py`: 处理应用列表获取和保存
- `data_service.py`: 处理应用数据抓取和保存

### 5. 任务调度 (tasks/)
- `sync_user_apps.py`: 同步用户应用列表任务
- `sync_app_data.py`: 同步应用数据任务
- 支持并发处理和失败重试

## 数据流程

### 1. 用户应用同步流程
```
1. 获取启用的用户列表
2. 为每个用户创建同步任务
3. 并发执行任务：
   - 登录 AppsFlyer
   - 获取应用列表
   - 保存到数据库
4. 更新任务状态
```

### 2. 数据同步流程
```
1. 获取用户应用列表
2. 为每个应用创建数据同步任务
3. 并发执行任务：
   - 登录 AppsFlyer
   - 获取指定日期的数据
   - 保存到数据库
4. 数据迁移到报表库
5. 更新任务状态
```

## 监控和日志

### 日志配置
- 日志级别：INFO
- 日志格式：时间戳 + 级别 + 模块 + 消息
- 日志输出：控制台 + 文件（可配置）

### 监控指标
- 任务执行状态
- 数据同步成功率
- 错误统计和重试次数
- 系统性能指标

## 错误处理

### 1. 网络错误
- 自动重试机制
- 代理轮换（如启用）
- 超时处理

### 2. 登录失败
- Cookie 缓存和复用
- 账户状态检查
- 验证码处理（手动）

### 3. 数据错误
- 数据格式验证
- 重复数据处理
- 事务回滚

## 性能优化

### 1. 并发处理
- 多进程 + 多线程架构
- 可配置的并发数量
- 任务队列管理

### 2. 数据库优化
- 连接池管理
- 批量操作
- 索引优化

### 3. 缓存机制
- Cookie 缓存
- 会话复用
- 数据去重

## 部署建议

### 1. 生产环境
- 使用 Docker 容器化部署
- 配置反向代理（Nginx）
- 设置定时任务（Cron）
- 监控和告警

### 2. 安全考虑
- 敏感信息环境变量化
- 数据库访问权限控制
- API 接口鉴权
- 日志脱敏

## 常见问题

### 1. 登录失败
- 检查用户名密码是否正确
- 确认账户未被锁定
- 检查网络连接
- 查看是否需要验证码

### 2. 数据同步失败
- 检查 AppsFlyer API 状态
- 确认应用权限
- 检查日期范围
- 查看错误日志

### 3. 性能问题
- 调整并发参数
- 优化数据库查询
- 检查网络延迟
- 监控系统资源