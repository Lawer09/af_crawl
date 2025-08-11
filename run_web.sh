#!/bin/bash

# AppsFlyer 爬虫系统 Web 服务启动脚本
# 专门用于启动 Web 管理界面

set -e
set -o pipefail
LOG_TS=$(date +"%Y-%m-%d %H:%M:%S")

# 配置项
REPO_DIR="/root/crawl/ausk"
PYTHON_CMD="python3.13"
MAIN_SCRIPT="main.py"

echo "[$LOG_TS] === AppsFlyer 爬虫系统 Web 服务启动脚本 ==="

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
LOG_FILE="$SCRIPT_DIR/run_web.log"

# 获取时间戳函数
timestamp() {
    date +"%Y-%m-%d %H:%M:%S"
}

# 显示使用帮助
show_help() {
    echo "使用方法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  --help, -h              - 显示此帮助信息"
    echo "  --host HOST             - 监听地址 默认 0.0.0.0 "
    echo "  --port PORT             - 监听端口 默认 8080"
    echo ""
    echo "示例:"
    echo "  $0                      - 使用默认配置启动 Web 服务"
    echo "  $0 --host 127.0.0.1     - 仅本地访问"
    echo "  $0 --port 8000          - 使用端口 8000"
    echo ""
    echo "启动后可通过以下地址访问:"
    echo "  http://localhost:8080   - Web 管理界面"
    echo "  http://localhost:8080/api/distribution/management/scheduler/status - API 状态"
}

# 解析命令行参数
EXTRA_ARGS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --host|--port)
            EXTRA_ARGS="$EXTRA_ARGS $1 $2"
            shift 2
            ;;
        *)
            EXTRA_ARGS="$EXTRA_ARGS $1"
            shift
            ;;
    esac
done

# 主逻辑
main() {
    # 构建Python命令
    local python_args=("$MAIN_SCRIPT" "web")
    
    # 添加额外参数
    if [ -n "$EXTRA_ARGS" ]; then
        python_args+=($EXTRA_ARGS)
    fi
    
    local command_str="python ${python_args[*]}"
    echo "[$(timestamp)] 启动命令: $command_str"
    echo "[$(timestamp)] 日志文件: $LOG_FILE"
    echo "[$(timestamp)] ==========================================="
    echo "[$(timestamp)] 启动 Web 管理界面..."
    echo "[$(timestamp)] 访问地址: http://localhost:8080"
    echo "[$(timestamp)] API 地址: http://localhost:8080/api/distribution/management/scheduler/status"
    echo "[$(timestamp)] 按 Ctrl+C 停止服务"
    echo "[$(timestamp)] ==========================================="
    
    # 运行python脚本，输出带时间戳写入日志，也打印到终端
    eval "$command_str" 2>&1 | while IFS= read -r line; do
        echo "[$(timestamp)] $line" | tee -a "$LOG_FILE"
    done
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -eq 0 ]; then
        echo "[$(timestamp)] Web 服务正常停止"
    else
        echo "[$(timestamp)] Web 服务异常停止，退出码: $exit_code"
    fi
    
    return $exit_code
}

# 执行主函数
main