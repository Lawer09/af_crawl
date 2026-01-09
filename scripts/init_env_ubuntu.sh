#!/bin/bash
set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK_DIR="$SCRIPT_DIR/.init_env_ubuntu.lock"
STAMP_FILE="$SCRIPT_DIR/.apt_update_stamp"

# 并发锁：避免重复/并发运行
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "=== 已有实例在运行，退出以避免并发 ==="
  exit 0
fi
trap 'rmdir "$LOCK_DIR"' EXIT

# 开始更新系统包列表（带缓存）
echo "=== 开始更新系统包列表（带缓存） ==="
NEED_UPDATE=1
if [ -f "$STAMP_FILE" ]; then
  LAST_TS=$(stat -c %Y "$STAMP_FILE" 2>/dev/null || echo 0)
  NOW_TS=$(date +%s)
  if [ $((NOW_TS - LAST_TS)) -lt 3600 ]; then
    NEED_UPDATE=0
  fi
fi
# 强制更新：导出 FORCE_APT_UPDATE=1 时忽略缓存
if [ "${FORCE_APT_UPDATE:-0}" = "1" ]; then
  NEED_UPDATE=1
fi
if [ "$NEED_UPDATE" -eq 1 ]; then
  sudo apt update -y
  date > "$STAMP_FILE"
else
  echo "=== 60分钟内已更新过 apt，跳过 ==="
fi

echo "=== 安装添加PPA所需工具 ==="
sudo apt install -y software-properties-common

echo "=== 检查并添加deadsnakes PPA源 ==="
if ! grep -Rqs "deadsnakes/ppa" /etc/apt/sources.list.d /etc/apt/sources.list 2>/dev/null; then
  sudo add-apt-repository -y ppa:deadsnakes/ppa
  sudo apt update -y
  date > "$STAMP_FILE"
else
  echo "=== 已存在 deadsnakes PPA，跳过添加 ==="
fi

echo "=== 安装Python 3.10及依赖（pip、venv、开发库） ==="
sudo apt install -y \
  python3.10 \
  python3-pip \
  python3.10-dev \
  python3.10-venv

echo "=== 验证安装结果 ==="
echo "Python 3.10版本："
python3.10 --version

echo "pip 3.10版本："
python3.10 -m pip --version

echo "=== 安装完成！使用时请通过 python3.10 或 pip 命令调用 ==="

# 选择可用的 Python 解释器（优先使用刚安装的 python3.10）
choose_python() {
  if command -v python3.10 >/dev/null 2>&1; then
    echo "python3.10"
  elif command -v python3 >/dev/null 2>&1; then
    echo "python3"
  elif command -v python >/dev/null 2>&1; then
    echo "python"
  else
    echo ""
  fi
}

# 确保 venv 组件已安装（优先 python3.10-venv，失败则回退 python3-venv）
ensure_venv_pkg() {
  if dpkg -s python3.10-venv >/dev/null 2>&1 || dpkg -s python3-venv >/dev/null 2>&1; then
    return
  fi
  echo "=== 安装 venv 组件（python3.10-venv 或 python3-venv） ==="
  sudo apt update -y || true
  sudo apt install -y python3.10-venv || sudo apt install -y python3-venv || {
    echo "安装 venv 组件失败，请手动执行: sudo apt install -y python3.10-venv 或 sudo apt install -y python3-venv";
    exit 1;
  }
}

PY_CMD=$(choose_python)
if [ -z "$PY_CMD" ]; then
  echo "未找到 Python，请确认已安装 python3.10 或 python3"
  exit 1
fi

echo "=== 使用解释器: $(command -v $PY_CMD) ($($PY_CMD --version)) ==="

# 创建虚拟环境（失败时安装 venv 组件并重试）
if [ ! -d "venv" ]; then
  echo "=== 创建虚拟环境 venv ==="
  $PY_CMD -m venv venv || {
    echo "=== venv 创建失败，尝试安装 venv 组件并重试 ==="
    ensure_venv_pkg
    $PY_CMD -m venv venv || { echo "创建虚拟环境失败"; exit 1; }
  }
else
  echo "=== 已存在虚拟环境 venv，跳过创建 ==="
fi

# 检测 venv Python 版本一致性（可通过 REBUILD_VENV=1 强制重建）
if [ -x "venv/bin/python" ]; then
  VENV_VER=$(venv/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' || echo "")
  SYS_VER=$($PY_CMD -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
  if [ "$VENV_VER" != "$SYS_VER" ]; then
    echo "=== venv 版本($VENV_VER)与系统($SYS_VER)不一致 ==="
    if [ "${REBUILD_VENV:-0}" = "1" ]; then
      echo "=== REBUILD_VENV=1，重新创建 venv ==="
      rm -rf venv
      $PY_CMD -m venv venv || { echo "重新创建虚拟环境失败"; exit 1; }
    else
      echo "=== 跳过重建（设置 REBUILD_VENV=1 可重新创建） ==="
    fi
  fi
fi

# 激活虚拟环境（Linux/WSL）
if [ -f "venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
elif [ -f "venv/Scripts/activate" ]; then
  # Windows Git Bash/WSL 混合场景
  # shellcheck disable=SC1091
  source venv/Scripts/activate
else
  echo "找不到虚拟环境激活脚本"
  exit 1
fi

echo "=== 虚拟环境 Python: $(which python) ($(python --version)) ==="

# 安装依赖
if [ -f "requirements.txt" ]; then
  echo "=== 升级 pip/setuptools/wheel 并安装项目依赖 ==="
  python -m pip install --upgrade pip setuptools wheel || true
  python -m pip install -r requirements.txt || echo "依赖安装失败，稍后可手动执行: source venv/bin/activate && pip install -r requirements.txt"
else
  echo "=== 未找到 requirements.txt，跳过依赖安装 ==="
fi

echo "=== 环境准备完成！ ==="