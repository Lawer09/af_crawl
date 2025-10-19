#!/usr/bin/env bash
set -euo pipefail

# Simple runner to start `main.py/mian.py cron --data` in background.
# 支持自动激活 venv（若存在），并将输出写到 logs/run_cron_data.out。

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

log_ts() { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(log_ts)] $*"; }

# 优先激活 venv
PY_BIN=""
if [ -f "$ROOT_DIR/requirements.txt" ] && [ -d "$ROOT_DIR/venv" ]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/venv/bin/activate"
  PY_BIN="python"
  log "已激活 venv: $(which python)；版本: $(python --version 2>&1)"
fi

# 若未激活 venv，则选择系统 Python
if [ -z "$PY_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PY_BIN="python3"
  else
    PY_BIN="python"
  fi
  log "使用系统 Python: $(command -v "$PY_BIN")；版本: $($PY_BIN --version 2>&1)"
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
log "入口文件: $ENTRY_FILE"

cd "$ROOT_DIR"
CMD="$PY_BIN $ENTRY_FILE cron --data"
log "启动: $CMD"
nohup bash -c "exec $CMD" > "$LOG_DIR/run_cron_data.out" 2>&1 &
NEW_PID=$!
log "已后台启动 cron --data，PID=$NEW_PID，日志: $LOG_DIR/run_cron_data.out"