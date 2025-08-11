#!/bin/bash

# AppsFlyer 爬虫系统 - 分布式工作节点启动脚本
# 快捷启动工作节点模式

set -e
set -o pipefail

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 调用主启动脚本
exec "$SCRIPT_DIR/run_simple.sh" distribute worker "$@"