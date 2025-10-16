# af 相关的验证
from ast import List
from services.login_service import get_session_by_pid
import config.af_config as cfg
from utils.retry import request_with_retry
import logging

logger = logging.getLogger(__name__)


def get_user_prt_list(pid:str) ->List[str]:
    """获取 pid 已授权的 prt 列表"""

    sess = get_session_by_pid(pid)
    headers = {
        "Referer": "https://hq1.appsflyer.com/security-center/agency-allowlist",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }
    try:
        resp = request_with_retry(sess, "GET", cfg.AF_PRT_AUTH_API, headers=headers, timeout=30)
    except Exception as e:
        logger.warning("fetch_apps request failed for %s -> %s; skip", pid, e)
        return False

    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError as e:
        # 非 JSON 或空响应时，记录细节并返回空列表，避免调度失败
        ct = resp.headers.get("Content-Type")
        logger.error(
            "Failed to parse JSON response for pid %s  from %s: %s. status=%s, content-type=%s, body=%s",
            pid,
            cfg.AF_PRT_AUTH_API,
            e,
            resp.status_code,
            ct,
            (resp.text or "")[:200],
        )
        return False

    return data


def is_prt_valid(pid:str, prt:str)->bool:
    """检查 prt 是否有效"""

    sess = get_session_by_pid(pid)
    headers = {
        "Referer": "https://hq1.appsflyer.com/security-center/agency-allowlist",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }
    try:
        resp = request_with_retry(sess, "GET", cfg.AF_PRT_PRT_VALID_API + prt, headers=headers, timeout=15)
    except Exception as e:
        logger.warning("is_prt_valid request failed for %s -> %s; skip", pid, e)
        return False

    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError as e:
        # 非 JSON 或空响应时，记录细节并返回空列表，避免调度失败
        ct = resp.headers.get("Content-Type")
        logger.error(
            "Failed to parse JSON response for pid %s  from %s: %s. status=%s, content-type=%s, body=%s",
            pid,
            cfg.AF_PRT_PRT_VALID_API + prt,
            e,
            resp.status_code,
            ct,
            (resp.text or "")[:200],
        )
        return False

    return data == "true"


def add_user_prt(pid:str, prt_list:List[str])->str:
    """添加 prt 到 pid 授权列表"""
    sess = get_session_by_pid(pid)
    headers = {
        "Origin": "https://hq1.appsflyer.com",
        "Referer": "https://hq1.appsflyer.com/security-center/agency-allowlist",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }
    try:
        resp = request_with_retry(sess, "POST", cfg.AF_PRT_AUTH_API, json=prt_list, headers=headers, timeout=30)
    except Exception as e:
        logger.warning("add_user_prt request failed for %s -> %s; skip", pid, e)
        return "add fail"

    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError as e:
        # 非 JSON 或空响应时，记录细节并返回空列表，避免调度失败
        ct = resp.headers.get("Content-Type")
        logger.error(
            "Failed to parse JSON response for pid %s  from %s: %s. status=%s, content-type=%s, body=%s",
            pid,
            cfg.AF_PRT_AUTH_API,
            e,
            resp.status_code,
            ct,
            (resp.text or "")[:200],
        )
        return "add fail"
    return data


def prt_auth(pid:str, prt:str) -> str:
    """ pid 增加 prt 用于 Authorized agencies """
    
    # 检查 prt 是否有效
    if not is_prt_valid(pid, prt):
        return f"prt {prt} is invalid"

    # 获取 pid 已授权的 prt 列表
    prt_list = get_user_prt_list(pid)
    if not prt_list:
        return f"failed to get prt list for pid {pid}"

    # 该pid已经添加了prt，无需重复添加
    if prt in prt_list:
        return True
    
    prt_list.append(prt)
    # 添加 prt
    return add_user_prt(pid, prt_list)
