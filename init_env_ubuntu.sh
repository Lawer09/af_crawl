#!/bin/bash

# 安装Python 3.10及相关依赖（含pip、venv）
echo "=== 开始更新系统包列表 ==="
sudo apt update -y

echo "=== 安装添加PPA所需工具 ==="
sudo apt install -y software-properties-common

echo "=== 添加deadsnakes PPA源 ==="
sudo add-apt-repository -y ppa:deadsnakes/ppa

echo "=== 再次更新包列表（识别新添加的Python源） ==="
sudo apt update -y

echo "=== 安装Python 3.10及依赖（pip、venv、开发库） ==="
sudo apt install -y \
  python3.10 \
  python3.10-pip \
  python3.10-dev \
  python3.10-venv

echo "=== 验证安装结果 ==="
echo "Python 3.10版本："
python3.10 --version

echo "pip 3.10版本："
pip3.10 --version

echo "=== 安装完成！使用时请通过 python3.10 或 pip3.10 命令调用 ==="

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

PY_CMD=$(choose_python)
if [ -z "$PY_CMD" ]; then
  echo "未找到 Python，请确认已安装 python3.10 或 python3"
  exit 1
fi

echo "=== 使用解释器: $(command -v $PY_CMD) ($($PY_CMD --version)) ==="

# 创建虚拟环境
if [ ! -d "venv" ]; then
  echo "=== 创建虚拟环境 venv ==="
  $PY_CMD -m venv venv || { echo "创建虚拟环境失败"; exit 1; }
else
  echo "=== 已存在虚拟环境 venv，跳过创建 ==="
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