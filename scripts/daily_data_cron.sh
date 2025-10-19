#!/usr/bin/env bash
set -euo pipefail

# Daily data cron runner: stops any running 'main.py cron --data' and starts a new one in background.
# Schedule example (run at 1:00 every day):
#   crontab -l > /tmp/mycron || true
#   echo "0 1 * * * /path/to/af_crawl/scripts/daily_data_cron.sh >> /var/log/daily_data_cron.cron.log 2>&1" >> /tmp/mycron
#   crontab /tmp/mycron
#   rm -f /tmp/mycron

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

# Detect python interpreter (prefer venv)
PY_BIN=""
if [ -x "$ROOT_DIR/venv/bin/python" ]; then
  PY_BIN="$ROOT_DIR/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY_BIN="python3"
else
  PY_BIN="python"
fi

# Kill running cron --data processes
PIDS=$(ps -ef | grep -E "[p]ython(3)? .*main\.py .*cron.*--data" | awk '{print $2}')
if [ -n "$PIDS" ]; then
  echo "Killing running cron --data processes: $PIDS"
  kill $PIDS || true
  sleep 2
  for PID in $PIDS; do
    if kill -0 "$PID" 2>/dev/null; then
      echo "Force killing PID $PID"
      kill -9 "$PID" || true
    fi
  done
fi

# Start a new background process
cd "$ROOT_DIR"
echo "Starting: $PY_BIN main.py cron --data"
nohup "$PY_BIN" "$ROOT_DIR/main.py" cron --data > "$LOG_DIR/daily_data_cron.out" 2>&1 &
NEW_PID=$!
echo "Started cron --data with PID $NEW_PID, logs at $LOG_DIR/daily_data_cron.out"