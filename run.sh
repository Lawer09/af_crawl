#!/bin/bash

# AppsFlyer 爬虫系统启动脚本
# 支持传统模式、分布式模式及后台运行

set -e
set -o pipefail
LOG_TS=$(date +"%Y-%m-%d %H:%M:%S")

# 配置项
REPO_DIR="/root/af_crawl/af_crawl"
PYTHON_CMD="python3.13"
MAIN_SCRIPT="main.py"

echo "[$LOG_TS] === AppsFlyer 爬虫系统启动脚本 ==="

cd "$REPO_DIR" || { echo "[$LOG_TS] 无法进入目录 $REPO_DIR"; exit 1; }

# 显示当前系统 Python 信息
echo "[$LOG_TS] 当前系统 Python 路径: $(command -v $PYTHON_CMD)"
echo "[$LOG_TS] 当前系统 Python 版本: $($PYTHON_CMD --version)"

# 创建虚拟环境并安装依赖
if [ -f "requirements.txt" ]; then
    if [ ! -d "venv" ]; then
        echo "[$LOG_TS] 创建虚拟环境..."
        $PYTHON_CMD -m venv venv || { echo "虚拟环境创建失败"; exit 1; }
    fi
    echo "[$LOG_TS] 激活虚拟环境并安装依赖..."
    source venv/bin/activate
    echo "[$LOG_TS] 当前虚拟环境 Python 路径: $(which python)"
    echo "[$LOG_TS] 当前虚拟环境 Python 版本: $(python --version)"
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt || echo "[$LOG_TS] 依赖安装失败"
else
    echo "[$LOG_TS] 未找到 requirements.txt，跳过依赖安装"
fi

# 检查主程序
if [ ! -f "$MAIN_SCRIPT" ]; then
    echo "[$LOG_TS] 主Python脚本 $MAIN_SCRIPT 不存在"
    exit 1
fi

# 获取当前脚本所在目录（无论从哪里执行）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/run.log"
PID_FILE="$SCRIPT_DIR/run.pid"

# 获取时间戳函数
timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

# 显示使用帮助（新增--background说明）
show_help() {
    echo "使用方法: $0 <命令> [参数...] [--background]"
    echo ""
    echo "通用选项:"
    echo "  --background                  - 后台运行程序（终端关闭后继续执行）"
    echo ""
    echo "支持的命令:"
    echo "  传统模式:"
    echo "    sync_apps                    - 同步用户 App 列表"
    echo "    sync_data [--days N]         - 同步最近 N 天数据（默认1天）"
    echo "    web                          - 启动Web管理界面"
    echo ""
    echo "  计划任务:"
    echo "    cron [选项]                  - 启动统一定时任务入口"
    echo "      --apps                    - 启动应用列表定时更新"
    echo "      --apps-interval-minutes M - 应用任务执行间隔(分钟)，默认1440"
    echo "      --data                    - 启动应用数据定时更新"
    echo "      --data-interval-hours H   - 数据任务执行间隔(小时)，默认24"
    echo ""
    echo "  分布式模式:"
    echo "    distribute master [选项]     - 启动分布式主节点"
    echo "    distribute worker [选项]     - 启动分布式工作节点"
    echo "    distribute standalone [选项] - 启动独立节点"
    echo "    distribute status [选项]     - 查看系统状态"
    echo ""
    echo "  分布式选项:"
    echo "    --device-id ID              - 设备ID（可选，未提供时自动生成）"
    echo "    --device-name NAME          - 设备名称"
    echo "    --host HOST                 - 监听地址（master模式，默认localhost）"
    echo "    --port PORT                 - 监听端口（默认7989）"
    echo "    --master-host HOST          - 主节点地址（worker模式，必需）"
    echo "    --master-port PORT          - 主节点端口（worker模式，默认7989）"
    echo "    --dispatch-interval N       - 任务分发间隔秒数（standalone模式，默认10）"
    echo "    --concurrent-tasks N        - 并发任务数（standalone模式，默认5）"
    echo "    --enable-monitoring         - 启用性能监控（standalone模式）"
    echo "    --config FILE               - 配置文件路径"
    echo ""
    echo "示例 (后台运行):"
    echo "  $0 sync_apps --background"
    echo "  $0 web --background"
    echo "  $0 distribute standalone --concurrent-tasks 3 --background"
}

# 验证命令
validate_command() {
    local cmd="$1"
    local subcmd="$2"
    
    case "$cmd" in
        "sync_apps"|"sync_data"|"web"|"cron")
            return 0
            ;;
        "distribute")
            case "$subcmd" in
                "master"|"worker"|"standalone"|"status")
                    return 0
                    ;;
                *)
                    echo "错误: 无效的分布式子命令: $subcmd"
                    echo "有效的分布式子命令: master, worker, standalone, status"
                    return 1
                    ;;
            esac
            ;;
        *)
            echo "错误: 无效的命令: $cmd"
            echo "有效命令: sync_apps, sync_data, web, cron, distribute"
            return 1
            ;;
    esac
}

# 优先处理脚本自身选项（--background）
BACKGROUND=0
# 临时存储所有参数，用于过滤
ALL_ARGS=("$@")

# 检查是否包含--background选项
for arg in "${ALL_ARGS[@]}"; do
    if [ "$arg" = "--background" ]; then
        BACKGROUND=1
        # 从参数列表中移除--background
        ALL_ARGS=("${ALL_ARGS[@]/--background}")
    fi
done

# 重新解析命令、子命令和额外参数（已移除--background）
CMD=${ALL_ARGS[0]:-""}
SUBCMD=${ALL_ARGS[1]:-""}

# 提取剩余参数（排除CMD和SUBCMD）
if [ ${#ALL_ARGS[@]} -ge 2 ]; then
    EXTRA_ARGS=("${ALL_ARGS[@]:2}")
else
    EXTRA_ARGS=()
fi

# 如果没有命令，交互输入
if [ -z "$CMD" ]; then
    echo "请输入要执行的命令:"
    echo "可用命令: sync_apps, sync_data, web, cron, distribute"
    echo "使用 --help 查看详细帮助，添加 --background 可后台运行"
    read -r CMD
    
    if [ "$CMD" = "distribute" ]; then
        echo "请输入分布式子命令 (master/worker/standalone/status):"
        read -r SUBCMD
    fi
fi

# 检查帮助
if [ "$CMD" = "--help" ] || [ "$CMD" = "-h" ]; then
    show_help
    exit 0
fi

# 验证命令
if ! validate_command "$CMD" "$SUBCMD"; then
    echo ""
    echo "使用 $0 --help 查看完整帮助信息"
    exit 1
fi

# 主逻辑（支持后台运行）
main() {
    local cmd="$CMD"
    local subcmd="$SUBCMD"
    
    # 构建Python命令
    local python_args=("$MAIN_SCRIPT" "$cmd")
    if [ -n "$subcmd" ]; then
        python_args+=("$subcmd")
    fi
    if [ -n "$EXTRA_ARGS" ]; then
        python_args+=($EXTRA_ARGS)
    fi
    
    local command_str="python ${python_args[*]}"
    echo "[$(timestamp)] 启动命令: $command_str"
    echo "[$(timestamp)] 日志文件: $LOG_FILE"
    echo "[$(timestamp)] ==========================================="
    
    # 命令类型提示
    case "$cmd" in
        "sync_apps") echo "[$(timestamp)] 开始同步用户应用列表..." ;;
        "sync_data") echo "[$(timestamp)] 开始同步应用数据..." ;;
        "web") echo "[$(timestamp)] 启动Web管理界面..."; echo "[$(timestamp)] 访问地址: http://localhost:8880" ;;
        "cron") echo "[$(timestamp)] 启动统一定时任务..." ;;
        "distribute")
            case "$subcmd" in
                "master") echo "[$(timestamp)] 启动分布式主节点..." ;;
                "worker") echo "[$(timestamp)] 启动分布式工作节点..." ;;
                "standalone") echo "[$(timestamp)] 启动独立节点..." ;;
                "status") echo "[$(timestamp)] 查询系统状态..." ;;
            esac
            ;;
    esac
    
    # 执行命令（区分前台/后台）
    if [ $BACKGROUND -eq 1 ]; then
        # 后台运行：用nohup确保终端关闭后继续运行，输出重定向到日志
        # 在子shell中直接使用 date 生成时间戳，并同时打印子shellPID，便于后续定位与关闭
        nohup sh -c "PID_TAG=\$$; $command_str 2>&1 | while IFS= read -r line; do printf '[%s][%s] %s\n' \"\$(date '+%Y-%m-%d %H:%M:%S')\" \"$PID_TAG\" \"\$line\"; done >> \"$LOG_FILE\" 2>&1" &
        local PID=$!  # 获取后台进程ID
        echo "$PID" > "$PID_FILE"
        echo "[$(timestamp)] 程序已在后台启动，进程ID: $PID"
        echo "[$(timestamp)] 后台PID已写入: $PID_FILE"
        echo "[$(timestamp)] 停止程序可执行: kill $PID"
        echo "[$(timestamp)] 查看日志: tail -f $LOG_FILE"
    else
        # 前台运行：保持终端输出
        eval "$command_str" 2>&1 | while IFS= read -r line; do
            echo "[$(timestamp)] $line" | tee -a "$LOG_FILE"
        done
        local exit_code=${PIPESTATUS[0]}
        
        if [ $exit_code -eq 0 ]; then
            echo "[$(timestamp)] 命令执行完成"
        else
            echo "[$(timestamp)] 命令执行失败，退出码: $exit_code"
        fi
        return $exit_code
    fi
}

# 执行主函数
main