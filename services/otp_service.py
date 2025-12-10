from __future__ import annotations

import logging
import io
import threading
from urllib.parse import urlparse, parse_qs

from model.user import UserDAO
from utils import get_totp_token

logger = logging.getLogger(__name__)

# 可选依赖：二维码识别
try:
    from qreader import QReader  # type: ignore
    import numpy as np  # type: ignore
    from PIL import Image  # type: ignore
except Exception:
    QReader = None  # type: ignore
    np = None  # type: ignore
    Image = None  # type: ignore

# 单例：QReader 实例仅加载一次，避免重复下载/加载模型
_qreader_instance: QReader | None = None
_qreader_lock = threading.RLock()

def _get_qreader() -> QReader:
    """获取 QReader 单例，首次调用时初始化。"""
    if QReader is None:
        # 统一依赖缺失的报错信息
        raise ValueError("未安装二维码识别依赖，请安装 qreader、Pillow、numpy")
    with _qreader_lock:
        global _qreader_instance
        if _qreader_instance is None:
            _qreader_instance = QReader()
        return _qreader_instance


def get_2fa_code_by_pid(pid: str) -> str:
    """根据 pid 获取 2FA 验证码。

    - 从 `af_user` 表读取 `2fa_key`
    - 未找到用户或密钥为空时抛出明确的 ValueError
    """
    user = UserDAO.get_user_by_pid(pid)
    if not user:
        raise ValueError(f"User with pid={pid} not found.")

    key = user.get("2fa_key")
    if not key or not str(key).strip():
        raise ValueError(f"pid {pid} 未添加密钥")

    return get_totp_token(str(key))


def get_2fa_code_by_username(username: str) -> str:
    """根据用户名（email）或 pid 获取 2FA 验证码。

    - 优先按 email 查询用户记录，需包含 `pid` 与 `2fa_key`
    - 若 `2fa_key` 为空或缺失，则返回明确提示：`pid 未添加密钥` 或 `用户未添加密钥`
    - 若 email 查询不到，兼容传入的 username 可能是 pid
    """
    user = UserDAO.get_user_by_email(username)
    if not user:
        # 兼容传入的 username 可能是 pid 的情况
        user = UserDAO.get_user_by_pid(username)
        if not user:
            raise ValueError(f"User {username} not found.")

    key = user.get("2fa_key")
    if not key or not str(key).strip():
        pid = user.get("pid") or ""
        if pid:
            raise ValueError(f"pid {pid} 未添加密钥")
        raise ValueError("用户未添加密钥")

    return get_totp_token(str(key))


def _parse_otpauth_secret(data: str) -> str | None:
    """从 otpauth 文本中解析 secret 参数。
    支持形如：
    otpauth://totp/Issuer:account?algorithm=SHA1&digits=6&issuer=AppsFlyer&period=30&secret=XXXXX
    """
    if not data:
        return None
    data = data.strip()
    # 快速路径：直接找 'secret='
    idx = data.lower().find("secret=")
    if idx >= 0:
        candidate = data[idx + len("secret="):]
        # 截断到下一个 & 或字符串末尾
        end_idx = candidate.find("&")
        secret = candidate[:end_idx] if end_idx >= 0 else candidate
        return secret.strip() or None
    # 走标准解析
    try:
        # urlparse 能解析 query 参数，scheme 为 otpauth
        parsed = urlparse(data)
        qs = parse_qs(parsed.query or "")
        sec = qs.get("secret", [None])[0]
        return sec
    except Exception:
        return None


def save_2fa_secret_from_qr(pid: str, image_bytes: bytes) -> dict:
    """识别二维码图片，解析 otpauth 文本中的 secret，并写入 af_user.2fa_key。

    返回：{"status": "success", "pid": pid, "secret": secret}
    错误抛出 ValueError，message 为中文提示。
    """
    if not pid or not pid.strip():
        raise ValueError("pid 不能为空")
    user = UserDAO.get_user_by_pid(pid)
    if not user:
        raise ValueError(f"pid {pid} 不存在")

    if not image_bytes:
        raise ValueError("未上传图片或图片内容为空")

    # 识别二维码（使用 qreader 替代 opencv）
    if np is None or Image is None:
        raise ValueError("未安装二维码识别依赖，请安装 qreader、Pillow、numpy")
    try:
        # 将字节转换为 RGB ndarray
        pil_img = Image.open(io.BytesIO(image_bytes))
        img_rgb = np.array(pil_img.convert("RGB"))

        # 使用 QReader 解码（返回 tuple[str | None, ...]）
        qreader = _get_qreader()
        decoded_tuple = qreader.detect_and_decode(image=img_rgb)
        # 选择第一个包含 otpauth 的结果
        data = ""
        for item in decoded_tuple or ():
            if item and isinstance(item, str) and "otpauth://" in item:
                data = item
                break
        if not data:
            # 若未筛到 otpauth，尝试拿第一个非空字符串
            for item in decoded_tuple or ():
                if item and isinstance(item, str):
                    data = item
                    break
        if not data:
            raise ValueError("未识别到二维码或格式不正确")
    except ValueError:
        raise
    except Exception as e:
        logger.exception("QR decode failed: %s", e)
        raise ValueError("二维码识别失败")

    # 解析 secret
    secret = _parse_otpauth_secret(data)
    if not secret:
        raise ValueError("不是二维码信息或格式不正确（未找到 secret 参数）")

    # 入库
    affected = UserDAO.update_2fa_key_by_pid(pid, secret)
    if affected <= 0:
        raise ValueError("写入密钥失败，请稍后重试")
    logger.info("Updated 2FA secret for pid=%s", pid)
    return {"status": "success", "pid": pid, "secret": secret}


def save_2fa_secret(pid: str, secret_or_otpauth: str) -> dict:
    """根据 pid 和密钥参数直接保存密钥。

    - `secret_or_otpauth` 可为纯 `secret` 或完整 `otpauth://...` 文本
    - 成功返回：{"status": "success", "pid": pid, "secret": secret}
    - 失败抛 ValueError，detail 文案与 QR 接口保持一致
    """
    if not pid or not pid.strip():
        raise ValueError("pid 不能为空")
    user = UserDAO.get_user_by_pid(pid)
    if not user:
        raise ValueError(f"pid {pid} 不存在")

    if not secret_or_otpauth or not str(secret_or_otpauth).strip():
        raise ValueError("secret 不能为空")

    secret = str(secret_or_otpauth).strip()
    # 如果传入的是 otpauth 文本，解析 secret
    if "otpauth://" in secret:
        parsed = _parse_otpauth_secret(secret)
        if not parsed:
            raise ValueError("不是二维码信息或格式不正确（未找到 secret 参数）")
        secret = parsed.strip()

    if not secret:
        raise ValueError("secret 不能为空")

    affected = UserDAO.update_2fa_key_by_pid(pid, secret)
    if affected <= 0:
        raise ValueError("写入密钥失败，请稍后重试")
    logger.info("Saved 2FA secret directly for pid=%s", pid)
    return {"status": "success", "pid": pid, "secret": secret}
