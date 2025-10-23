#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root and move into it
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Allow override via env var; otherwise auto-detect python
PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "$PYTHON_BIN" ]; then
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    echo "❌ Python 未找到，请安装或在环境变量中设置 PYTHON_BIN。"
    exit 1
  fi
fi

# Try to activate virtualenv if present
if [ -f "$ROOT_DIR/venv/bin/activate" ]; then
  # Linux/macOS
  source "$ROOT_DIR/venv/bin/activate"
elif [ -f "$ROOT_DIR/venv/Scripts/activate" ]; then
  # Windows (Git Bash)
  source "$ROOT_DIR/venv/Scripts/activate"
fi

# 正确传递参数：分别传入脚本和子命令，避免把它们合并成一个路径
"$PYTHON_BIN" "$ROOT_DIR/main.py" sync_apps