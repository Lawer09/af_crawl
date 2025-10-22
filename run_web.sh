#!/bin/bash

# 运行 Web：激活虚拟环境、关闭旧进程、后台启动 main.py web
set -e
set -o pipefail

# 目录与文件
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$SCRIPT_DIR"
MAIN_SCRIPT="main.py"
LOG_FILE="$SCRIPT_DIR/run_web.log"
PID_FILE="$SCRIPT_DIR/run_web.pid"

# 时间戳
ts() { date +"%Y-%m-%d %H:%M:%S"; }

echo "[$(ts)] === 启动 Web 服务脚本 ==="
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

# 关闭旧后台进程
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" >/dev/null 2>&1; then
    echo "[$(ts)] 检测到已有后台进程(PID=$OLD_PID)，先关闭..."
    kill "$OLD_PID" >/dev/null 2>&1 || true
    for i in $(seq 1 10); do
      if kill -0 "$OLD_PID" >/dev/null 2>&1; then
        sleep 1
      else
        break
      fi
    done
    if kill -0 "$OLD_PID" >/dev/null 2>&1; then
      echo "[$(ts)] 旧进程仍在，强制关闭..."
      kill -9 "$OLD_PID" >/dev/null 2>&1 || true
    fi
    echo "[$(ts)] 已关闭旧进程: $OLD_PID"
  fi
  rm -f "$PID_FILE"
fi

# 后台启动 Web 服务
CMD="python $MAIN_SCRIPT web"

echo "[$(ts)] 启动命令: $CMD"
nohup bash -c "exec $CMD" >> "$LOG_FILE" 2>&1 &
NEW_PID=$!

echo "$NEW_PID" > "$PID_FILE"
echo "[$(ts)] 程序已在后台启动，PID=$NEW_PID"
echo "[$(ts)] 日志: tail -f $LOG_FILE"
echo "[$(ts)] 停止: kill $NEW_PID"