@echo off
REM AppsFlyer 爬虫系统启动脚本 (Windows版本)
REM 支持传统模式和分布式模式

setlocal enabledelayedexpansion

REM 配置项
set "REPO_DIR=%~dp0"
set "PYTHON_CMD=python"
set "MAIN_SCRIPT=main.py"
set "LOG_FILE=%REPO_DIR%run.log"

REM 获取当前时间戳
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "LOG_TS=%dt:~0,4%-%dt:~4,2%-%dt:~6,2% %dt:~8,2%:%dt:~10,2%:%dt:~12,2%"

echo [%LOG_TS%] === AppsFlyer 爬虫系统启动脚本 ===

REM 切换到脚本目录
cd /d "%REPO_DIR%" || (
    echo [%LOG_TS%] 无法进入目录 %REPO_DIR%
    exit /b 1
)

REM 显示当前系统 Python 信息
echo [%LOG_TS%] 检查Python环境...
where %PYTHON_CMD% >nul 2>&1 || (
    echo [%LOG_TS%] 错误: 未找到Python，请确保Python已安装并添加到PATH
    pause
    exit /b 1
)

echo [%LOG_TS%] 当前系统 Python 路径:
where %PYTHON_CMD%
echo [%LOG_TS%] 当前系统 Python 版本:
%PYTHON_CMD% --version

REM 创建虚拟环境并安装依赖
if exist "requirements.txt" (
    if not exist "venv" (
        echo [%LOG_TS%] 创建虚拟环境...
        %PYTHON_CMD% -m venv venv || (
            echo 虚拟环境创建失败
            pause
            exit /b 1
        )
    )
    echo [%LOG_TS%] 激活虚拟环境并安装依赖...
    call venv\Scripts\activate.bat
    echo [%LOG_TS%] 当前虚拟环境 Python 版本:
    python --version
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt || echo [%LOG_TS%] 依赖安装失败
) else (
    echo [%LOG_TS%] 未找到 requirements.txt，跳过依赖安装
)

REM 检查主程序
if not exist "%MAIN_SCRIPT%" (
    echo [%LOG_TS%] 主Python脚本 %MAIN_SCRIPT% 不存在
    pause
    exit /b 1
)

REM 显示使用帮助
if "%1"=="--help" goto :show_help
if "%1"=="-h" goto :show_help
if "%1"=="/?" goto :show_help

REM 检查参数
if "%1"=="" goto :interactive_mode

REM 验证命令
call :validate_command "%1" "%2"
if errorlevel 1 (
    echo.
    echo 使用 %0 --help 查看完整帮助信息
    pause
    exit /b 1
)

REM 构建并执行Python命令
call :execute_command %*
exit /b %errorlevel%

:show_help
echo 使用方法: %0 ^<命令^> [参数...]
echo.
echo 支持的命令:
echo   传统模式:
echo     sync_apps                    - 同步用户 App 列表
echo     sync_data [--days N]         - 同步最近 N 天数据（默认1天）
echo     web                          - 启动Web管理界面
echo.
echo   分布式模式:
echo     distribute master [选项]     - 启动分布式主节点
echo     distribute worker [选项]     - 启动分布式工作节点
echo     distribute standalone [选项] - 启动独立节点
echo     distribute status [选项]     - 查看系统状态
echo.
echo   分布式选项:
echo     --device-id ID              - 设备ID（可选，未提供时自动生成）
echo     --device-name NAME          - 设备名称
echo     --host HOST                 - 监听地址（master模式，默认localhost）
echo     --port PORT                 - 监听端口（默认7989）
echo     --master-host HOST          - 主节点地址（worker模式，必需）
echo     --master-port PORT          - 主节点端口（worker模式，默认7989）
echo     --dispatch-interval N       - 任务分发间隔秒数（standalone模式，默认10）
echo     --concurrent-tasks N        - 并发任务数（standalone模式，默认5）
echo     --enable-monitoring         - 启用性能监控（standalone模式）
echo     --config FILE               - 配置文件路径
echo.
echo 示例:
echo   %0 sync_apps
echo   %0 sync_data --days 7
echo   %0 web
echo   %0 distribute master --device-id master-001 --port 7989
echo   %0 distribute worker --device-id worker-001 --master-host 192.168.1.100
echo   %0 distribute standalone --concurrent-tasks 3 --enable-monitoring
echo   %0 distribute status --master-host 192.168.1.100
pause
exit /b 0

:interactive_mode
echo 请输入要执行的命令，或使用 --help 查看帮助:
echo 可用命令: sync_apps, sync_data, web, distribute
echo.
set /p "CMD=请输入命令: "
if "!CMD!"=="" (
    echo 未输入命令，退出
    pause
    exit /b 1
)
REM 重新调用脚本
call "%0" !CMD!
exit /b %errorlevel%

:validate_command
set "cmd=%~1"
set "subcmd=%~2"

if "%cmd%"=="sync_apps" exit /b 0
if "%cmd%"=="sync_data" exit /b 0
if "%cmd%"=="web" exit /b 0

if "%cmd%"=="distribute" (
    if "%subcmd%"=="master" exit /b 0
    if "%subcmd%"=="worker" exit /b 0
    if "%subcmd%"=="standalone" exit /b 0
    if "%subcmd%"=="status" exit /b 0
    echo 错误: 无效的分布式子命令: %subcmd%
    echo 有效的分布式子命令: master, worker, standalone, status
    exit /b 1
)

echo 错误: 无效的命令: %cmd%
echo 有效命令: sync_apps, sync_data, web, distribute
exit /b 1

:execute_command
REM 获取当前时间戳
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "timestamp=%dt:~0,4%-%dt:~4,2%-%dt:~6,2% %dt:~8,2%:%dt:~10,2%:%dt:~12,2%"

REM 构建Python命令
set "python_command=python %MAIN_SCRIPT%"
set "args=%*"
set "python_command=%python_command% %args%"

echo [%timestamp%] 启动命令: %python_command%
echo [%timestamp%] 日志文件: %LOG_FILE%
echo [%timestamp%] ===========================================

REM 根据命令类型提供特定信息
set "cmd=%1"
set "subcmd=%2"

if "%cmd%"=="sync_apps" (
    echo [%timestamp%] 开始同步用户应用列表...
) else if "%cmd%"=="sync_data" (
    echo [%timestamp%] 开始同步应用数据...
) else if "%cmd%"=="web" (
    echo [%timestamp%] 启动Web管理界面...
    echo [%timestamp%] 访问地址: http://localhost:8080
) else if "%cmd%"=="distribute" (
    if "%subcmd%"=="master" (
        echo [%timestamp%] 启动分布式主节点...
        echo [%timestamp%] 主节点将负责任务调度和分发
    ) else if "%subcmd%"=="worker" (
        echo [%timestamp%] 启动分布式工作节点...
        echo [%timestamp%] 工作节点将连接到主节点执行任务
    ) else if "%subcmd%"=="standalone" (
        echo [%timestamp%] 启动独立节点...
        echo [%timestamp%] 独立节点集成了主节点和工作节点功能
    ) else if "%subcmd%"=="status" (
        echo [%timestamp%] 查询系统状态...
    )
)

REM 执行Python命令并记录日志
%python_command% 2>&1 | call :log_output

set "exit_code=%errorlevel%"

REM 获取结束时间戳
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "end_timestamp=%dt:~0,4%-%dt:~4,2%-%dt:~6,2% %dt:~8,2%:%dt:~10,2%:%dt:~12,2%"

if %exit_code% equ 0 (
    echo [%end_timestamp%] 命令执行完成
) else (
    echo [%end_timestamp%] 命令执行失败，退出码: %exit_code%
)

REM 如果不是后台运行，暂停以查看结果
if not "%NOPAUSE%"=="1" pause

exit /b %exit_code%

:log_output
REM 读取管道输入并添加时间戳
for /f "delims=" %%i in ('more') do (
    for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
    set "log_ts=!dt:~0,4!-!dt:~4,2!-!dt:~6,2! !dt:~8,2!:!dt:~10,2!:!dt:~12,2!"
    echo [!log_ts!] %%i
    echo [!log_ts!] %%i >> "%LOG_FILE%"
)
exit /b 0