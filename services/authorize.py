# af 相关的验证
from services.login_service import get_session_by_pid
import config.af_config as cfg
from utils.retry import request_with_retry
import logging

logger = logging.getLogger(__name__)

def prt_auth(pid:str, prt:str) -> bool:
    """ pid 增加 prt 用于 Authorized agencies """
    sess = get_session_by_pid(pid)
    headers = {
        "Referer": "https://hq1.appsflyer.com/security-center/agency-allowlist",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }
    try:
        resp = request_with_retry(sess, "GET", cfg.AF_PRT_AUTH_LIST_API, headers=headers, timeout=30)
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
            cfg.AF_PRT_AUTH_LIST_API,
            e,
            resp.status_code,
            ct,
            (resp.text or "")[:200],
        )
        return False

    prt_list:list = data

    print(prt_list)
    # 该pid已经添加了prt，无需重复添加
    if prt in prt_list:
        return True
    
    # 添加 prt
    
