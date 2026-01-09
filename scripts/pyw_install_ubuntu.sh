#!/bin/bash
set -e
set -o pipefail

LOG_TS=$(date '+%Y-%m-%d %H:%M:%S')
log() {
  echo "[$LOG_TS] $1"
}

# 1. 激活虚拟环境
log "激活 Python 虚拟环境..."
source venv/bin/activate

# 2. 安装 Python Playwright 模块（如未安装）
log "安装 Playwright Python 模块（如未安装）..."
pip install --upgrade pip
pip install playwright

# 3. 安装系统运行时依赖（Playwright 浏览器所需）
sudo apt update
log "安装 Playwright 所需系统依赖..."
python -m playwright install-deps

# 4. 安装浏览器核心文件（Chromium 等）
log "执行 playwright install 下载浏览器..."
python -m playwright install

log "✅ Playwright 安装完成！"

