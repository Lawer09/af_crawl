#!/bin/bash

# AppsFlyer 爬虫系统 - 独立节点启动脚本
# 快捷启动独立节点模式（集成主节点和工作节点功能）

set -e
set -o pipefail

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 调用主启动脚本
exec sh "$SCRIPT_DIR/run_simple.sh" distribute standalone "$@"