#!/bin/bash

# 检查依赖并运行Python脚本
# 支持传统模式和分布式模式

set -e
set -o pipefail
LOG_TS=$(date +"%Y-%m-%d %H:%M:%S")

# 配置项
REPO_DIR="/root/crawl/ausk"
PYTHON_CMD="python3.13"
MAIN_SCRIPT="main.py"

echo "[$LOG_TS] === 准备执行Python脚本 ==="

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

# 获取时间戳函数
timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

# 显示帮助信息
show_help() {
    echo "使用方法: $0 <命令> [子命令] [参数...]"
    echo ""
    echo "支持的命令:"
    echo "  传统模式:"
    echo "    sync_apps                    - 同步用户 App 列表"
    echo "    sync_data [--days N]         - 同步最近 N 天数据（默认1天）"
    echo "    web                          - 启动Web管理界面"
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
    echo "示例:"
    echo "  $0 sync_apps"
    echo "  $0 sync_data --days 7"
    echo "  $0 web"
    echo "  $0 distribute master --device-id master-001 --port 7989"
    echo "  $0 distribute worker --device-id worker-001 --master-host 192.168.1.100"
    echo "  $0 distribute standalone --concurrent-tasks 3 --enable-monitoring"
    echo "  $0 distribute status --master-host 192.168.1.100"
}

# 验证命令
validate_command() {
    local cmd=$1
    local subcmd=$2
    
    case "$cmd" in
        "sync_apps"|"sync_data"|"web")
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
        "-h"|"--help"|"help")
            show_help
            exit 0
            ;;
        *)
            echo "错误: 无效的命令: $cmd"
            echo "有效命令: sync_apps, sync_data, web, distribute"
            return 1
            ;;
    esac
}

# 读取要传给python的命令参数，没传则交互输入
CMD=$1
SUBCMD=$2

# 检查帮助参数
if [ "$CMD" = "--help" ] || [ "$CMD" = "-h" ] || [ "$CMD" = "help" ]; then
    show_help
    exit 0
fi

if [ -z "$CMD" ]; then
    echo "请输入要执行的命令:"
    echo "可用命令: sync_apps, sync_data, web, distribute"
    echo "使用 --help 查看详细帮助"
    read -r CMD
    
    if [ "$CMD" = "distribute" ]; then
        echo "请输入分布式子命令 (master/worker/standalone/status):"
        read -r SUBCMD
    fi
fi

# 验证命令
if ! validate_command "$CMD" "$SUBCMD"; then
    echo ""
    echo "使用 $0 --help 查看完整帮助信息"
    exit 1
fi

# 构建Python命令参数
PYTHON_ARGS="$CMD"
if [ -n "$SUBCMD" ]; then
    PYTHON_ARGS="$PYTHON_ARGS $SUBCMD"
fi

# 添加其他参数（跳过前两个参数）
shift 2 2>/dev/null || shift $# 2>/dev/null
if [ $# -gt 0 ]; then
    PYTHON_ARGS="$PYTHON_ARGS $*"
fi

echo "[$(timestamp)] 启动Python脚本 $MAIN_SCRIPT $PYTHON_ARGS ..."

# 根据命令类型提供特定信息
case "$CMD" in
    "sync_apps")
        echo "[$(timestamp)] 开始同步用户应用列表..."
        ;;
    "sync_data")
        echo "[$(timestamp)] 开始同步应用数据..."
        ;;
    "web")
        echo "[$(timestamp)] 启动Web管理界面..."
        echo "[$(timestamp)] 访问地址: http://localhost:8080"
        ;;
    "distribute")
        case "$SUBCMD" in
            "master")
                echo "[$(timestamp)] 启动分布式主节点..."
                echo "[$(timestamp)] 主节点将负责任务调度和分发"
                ;;
            "worker")
                echo "[$(timestamp)] 启动分布式工作节点..."
                echo "[$(timestamp)] 工作节点将连接到主节点执行任务"
                ;;
            "standalone")
                echo "[$(timestamp)] 启动独立节点..."
                echo "[$(timestamp)] 独立节点集成了主节点和工作节点功能"
                ;;
            "status")
                echo "[$(timestamp)] 查询系统状态..."
                ;;
        esac
        ;;
esac

# 运行python脚本，输出带时间戳写入日志，也打印到终端
python "$MAIN_SCRIPT" $PYTHON_ARGS 2>&1 | while IFS= read -r line; do
  echo "[$(timestamp)] $line" | tee -a "$LOG_FILE"
done

echo "[$(timestamp)] Python脚本执行完成"