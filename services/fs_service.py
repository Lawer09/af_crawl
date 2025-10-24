
import logging
import time
import hmac
import hashlib
import base64
from typing import Optional

import requests

from config.settings import FEISHU, SYSTEM_TYPE

FEISHU_BOT_HOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/067033b4-ac8d-4f41-85ec-4852df148932"

logger = logging.getLogger(__name__)


def _post_json(url: str, payload: dict, timeout: int = 10) -> bool:
    """统一的 POST JSON 发送封装，返回发送是否成功。"""
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        body = resp.text or ""
        if 200 <= resp.status_code < 300:
            # 飞书通常返回 {"StatusCode":0, "StatusMessage":"success"}
            try:
                data = resp.json()
                status_code = data.get("StatusCode") or data.get("code") or 0
                if int(status_code) == 0:
                    logger.debug("飞书消息发送成功")
                    return True
                logger.error("飞书返回非成功码: %s, body=%s", status_code, body[:200])
                return False
            except Exception:
                # 某些情况下可能没有标准 JSON 返回，HTTP 2xx 即认为成功
                logger.info("飞书消息发送成功(无标准JSON返回)，status=%s", resp.status_code)
                return True
        else:
            logger.error("飞书消息发送失败，HTTP=%s, body=%s", resp.status_code, body[:200])
            return False
    except Exception as e:
        logger.error("飞书消息发送异常: %s", e, exc_info=True)
        return False


def send_feishu_text(webhook_url: Optional[str], content: str) -> bool:
    """发送飞书文本消息（未启用签名校验的机器人）。

    :param webhook_url: 机器人 Webhook，如果为 None 则使用 FEISHU_BOT_HOOK
    :param content: 文本内容
    :return: 是否发送成功
    """
    url = webhook_url or FEISHU_BOT_HOOK
    payload = {
        "msg_type": "text",
        "content": {"text": content},
    }
    return _post_json(url, payload)


def gen_feishu_sign(secret: str, timestamp: int) -> str:
    """生成飞书签名：Base64(HMAC-SHA256(timestamp + "\n" + secret))."""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def send_feishu_text_with_sign(webhook_url: Optional[str], secret: str, content: str) -> bool:
    """发送飞书文本消息（启用签名校验的机器人）。

    :param webhook_url: 机器人 Webhook，如果为 None 则使用 FEISHU_BOT_HOOK
    :param secret: 机器人签名密钥
    :param content: 文本内容
    :return: 是否发送成功
    """
    url = webhook_url or FEISHU_BOT_HOOK
    ts = int(time.time())
    sign = gen_feishu_sign(secret, ts)
    payload = {
        "msg_type": "text",
        "content": {"text": content},
        "timestamp": str(ts),
        "sign": sign,
    }
    return _post_json(url, payload)


def send_message(content: str, webhook_url: Optional[str] = None, secret: Optional[str] = None) -> bool:
    """便捷方法：根据是否提供 secret 自动选择签名/非签名发送。"""
    if secret:
        return send_feishu_text_with_sign(webhook_url, secret, content)
    return send_feishu_text(webhook_url, content)


def send_sys_notify(content:str):
    if not FEISHU.get("sys_notify_webhook"):
        logger.error("飞书系统通知 Webhook 未配置")
        return False

    return send_feishu_text(FEISHU.get("sys_notify_webhook"), f"[{SYSTEM_TYPE}] {content}")
