#!/usr/bin/env bash
set -euo pipefail

# Daily data cron runner: stop existing 'main.py/mian.py cron --data' and start a new one.
# 参考 run.sh 的虚拟环境处理：优先激活 venv，再运行 Python。
# 安装/依赖应由 run.sh 完成；本脚本仅负责在定时任务中重启。
#
# 示例（每天 1:00 运行）：
#   crontab -l > /tmp/mycron || true
#   echo "0 1 * * * /path/to/af_crawl/scripts/daily_data_cron.sh >> /var/log/daily_data_cron.cron.log 2>&1" >> /tmp/mycron
#   crontab /tmp/mycron
#   rm -f /tmp/mycron

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

log_ts() { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(log_ts)] $*"; }

# 处理虚拟环境（参照 run.sh）
PY_BIN=""
if [ -f "$ROOT_DIR/requirements.txt" ]; then
  if [ -d "$ROOT_DIR/venv" ]; then
    log "激活虚拟环境: $ROOT_DIR/venv"
    # 激活 venv：后续使用 python 即为虚拟环境 python
    # shellcheck disable=SC1091
    source "$ROOT_DIR/venv/bin/activate"
    PY_BIN="python"
    log "当前虚拟环境 Python 路径: $(which python)"
    log "当前虚拟环境 Python 版本: $(python --version 2>&1)"
  else
    log "未发现 venv，回退系统 Python"
  fi
fi

# 若未激活 venv，则选择系统 Python
if [ -z "$PY_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PY_BIN="python3"
  else
    PY_BIN="python"
  fi
  log "使用系统 Python: $(command -v "$PY_BIN")"
  log "系统 Python 版本: $($PY_BIN --version 2>&1)"
fi

# 自动选择入口文件 main.py 或 mian.py
ENTRY_FILE=""
if [ -f "$ROOT_DIR/main.py" ]; then
  ENTRY_FILE="$ROOT_DIR/main.py"
elif [ -f "$ROOT_DIR/mian.py" ]; then
  ENTRY_FILE="$ROOT_DIR/mian.py"
else
  log "未在 $ROOT_DIR 找到入口文件 main.py 或 mian.py"
  exit 1
fi
log "使用入口文件: $ENTRY_FILE"

# 关闭当前正在运行的 cron --data 进程（安全检测，避免 pipefail 在无匹配时退出）
# 使用 ps + awk 直接匹配，awk 在无匹配时仍返回 0
# 使用 -ww 获取完整参数，避免截断导致匹配失败
PIDS=$(ps -ww -eo pid,args --no-header | awk '/(main|mian)\.py/ && /cron/ && /--data/ {print $1}' || true)
MATCHED=$(ps -ww -eo pid,args --no-header | awk '/(main|mian)\.py/ && /cron/ && /--data/' || true)
log "进程匹配行: $MATCHED"
if [ -n "$PIDS" ]; then
  log "检测到正在运行的 cron --data 进程: $PIDS，准备关闭"
  kill $PIDS || true
  sleep 2
  for PID in $PIDS; do
    if kill -0 "$PID" 2>/dev/null; then
      log "强制关闭 PID $PID"
      kill -9 "$PID" || true
    fi
  done
else
  log "未检测到正在运行的 cron --data 进程"
fi

# 后台启动新的 cron --data
cd "$ROOT_DIR"
CMD="$PY_BIN $ENTRY_FILE cron --data"
log "启动: $CMD"
nohup bash -c "exec $CMD" > "$LOG_DIR/daily_data_cron.out" 2>&1 &
NEW_PID=$!
log "已后台启动 cron --data，PID=$NEW_PID，日志: $LOG_DIR/daily_data_cron.out"