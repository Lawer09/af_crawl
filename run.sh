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
        "web") echo "[$(timestamp)] 启动Web API..."; echo "[$(timestamp)] 访问地址: http://localhost:8880" ;;
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
        # 若存在历史后台进程，先行关闭
        if [ -f "$PID_FILE" ]; then
            OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
            if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" >/dev/null 2>&1; then
                echo "[$(timestamp)] 检测到已有后台进程(PID=$OLD_PID)，先关闭..."
                kill "$OLD_PID" >/dev/null 2>&1 || true
                # 等待最多10秒，若仍存活则强制关闭
                for i in $(seq 1 10); do
                    if kill -0 "$OLD_PID" >/dev/null 2>&1; then
                        sleep 1
                    else
                        break
                    fi
                done
                if kill -0 "$OLD_PID" >/dev/null 2>&1; then
                    echo "[$(timestamp)] 旧进程仍在，强制关闭..."
                    kill -9 "$OLD_PID" >/dev/null 2>&1 || true
                fi
                echo "[$(timestamp)] 已关闭旧进程: $OLD_PID"
            fi
            rm -f "$PID_FILE"
        fi
        # 后台运行：用 nohup 直接启动 Python 进程，并记录其 PID（避免只记录包装 shell 的 PID）
        # 使用 exec 将 bash -c 替换为实际的 python 进程，确保 $! 即为 Python 进程 PID
        nohup bash -c "exec $command_str" >> "$LOG_FILE" 2>&1 &
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