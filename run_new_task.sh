#!/bin/bash

# 创建今日应用数据任务：激活虚拟环境并执行 main.py create_today_tasks
set -e
set -o pipefail

# 目录与文件
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$SCRIPT_DIR"
MAIN_SCRIPT="main.py"
LOG_FILE="$SCRIPT_DIR/run_new_task.log"

# 时间戳
ts() { date +"%Y-%m-%d %H:%M:%S"; }

echo "[$(ts)] === 创建今日应用数据任务 ==="
cd "$REPO_DIR" || { echo "[$(ts)] 无法进入目录 $REPO_DIR"; exit 1; }

# 选择系统 Python
choose_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
  elif command -v python >/dev/null 2>&1; then
    echo "python"
  else
    echo ""
  fi
}

PY_SYS_CMD=$(choose_python)
if [ -z "$PY_SYS_CMD" ]; then
  echo "[$(ts)] 未找到系统 Python，请安装 python 或 python3"
  exit 1
fi

echo "[$(ts)] 系统 Python: $(command -v $PY_SYS_CMD) ($( $PY_SYS_CMD --version ))"

# 创建并激活虚拟环境，安装依赖
if [ ! -d "venv" ]; then
  echo "[$(ts)] 创建虚拟环境..."
  $PY_SYS_CMD -m venv venv || { echo "[$(ts)] 虚拟环境创建失败"; exit 1; }
fi

activate_venv() {
  if [ -f "venv/bin/activate" ]; then
    # POSIX/Linux
    # shellcheck disable=SC1091
    source venv/bin/activate
  elif [ -f "venv/Scripts/activate" ]; then
    # Windows Git Bash/WSL 混合场景
    # shellcheck disable=SC1091
    source venv/Scripts/activate
  else
    echo "[$(ts)] 找不到虚拟环境激活脚本"
    exit 1
  fi
}

echo "[$(ts)] 激活虚拟环境..."
activate_venv

echo "[$(ts)] 虚拟环境 Python: $(which python) ($( python --version ))"

if [ -f "requirements.txt" ]; then
  echo "[$(ts)] 安装依赖..."
  python -m pip install --upgrade pip || true
  python -m pip install -r requirements.txt || echo "[$(ts)] 依赖安装失败，继续尝试运行"
else
  echo "[$(ts)] 未找到 requirements.txt，跳过依赖安装"
fi

# 检查主程序
if [ ! -f "$MAIN_SCRIPT" ]; then
  echo "[$(ts)] 主脚本 $MAIN_SCRIPT 不存在"
  exit 1
fi

# 前台执行创建今日任务，并输出到日志
CMD="python $MAIN_SCRIPT create_today_tasks"

echo "[$(ts)] 启动命令: $CMD"
set +e
$CMD 2>&1 | tee -a "$LOG_FILE"
RET=$?
set -e

echo "[$(ts)] 命令退出码: $RET"
if [ $RET -eq 0 ]; then
  echo "[$(ts)] 创建今日任务完成。"
else
  echo "[$(ts)] 创建今日任务执行失败，请查阅日志：$LOG_FILE"
fi

echo "[$(ts)] 如需开始处理队列中的任务，请运行 ./run_task.sh"