# af 相关的验证
import json
from services.login_service import get_session_by_pid, get_session_by_user
from model.user import PidConfigDAO, UserDAO
from model.af_handshake import AfHandshakeDAO
import config.af_config as cfg
from utils.retry import request_with_retry
import logging
from model.af_pb_config import AfPbConfigDAO

logger = logging.getLogger(__name__)


def get_user_prt_list(pid:str, username:str, password:str) ->list[str]:
    """获取 pid 已授权的 prt 列表"""

    sess = get_session_by_user(username, password, pid)
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
        raise Exception(f"prt valid failed for {pid} -> {e}")

    resp.raise_for_status()
    try:
        data = resp.json()
        logger.info(f"{prt} is valid: {data}")
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
        raise Exception(e)

    return data


def add_user_prt(pid:str,username:str, password:str, prt_list:list[str]):
    """添加 prt 到 pid 授权列表"""
    sess = get_session_by_user(username, password, pid)
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
        raise Exception(f"failed for {pid} -> {e}")

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
        raise Exception("add fail")

    if not data:
        raise Exception("add fail")

    return prt_list

def prt_auth(pid:str, prt:str):
    """ pid 增加 prt 用于 Authorized agencies """
    
    # 检查 prt 是否为空
    if not prt:
        raise Exception("prt is empty")

    # 先从数据库握手表查该 pid 关联的所有 prt 做比较
    user = UserDAO.get_user_by_pid(pid)
    user_id = user["id"] if user and user["id"] else None
    if not user_id:
        raise Exception(f"No af_user.id found for pid {pid}")

    # 检查 prt 是否有效
    if not is_prt_valid(pid, prt):
        raise Exception(f"prt {prt} is invalid")

    # 获取 pid 已授权的 prt 列表
    prt_list = get_user_prt_list(pid, user["email"], user["password"])
    if not prt_list:
        raise Exception(f"failed to get prt list for pid {pid}")

    # 若远端已包含该 prt，则无需重复添加，但同步握手关系
    if prt in prt_list:
        logger.info(f"prt {prt} already in list for pid {pid}")
        try:
            # 同步整表到远端列表
            ret = AfHandshakeDAO.sync_user_prts(user_id, prt_list, status=1)
            logger.info(f"prt {prt} sync to remote success for pid {pid}: {ret}")
        except Exception as e:
            logger.warning("Handshake sync failed (already exists): pid=%s prt=%s -> %s", pid, prt, e)
        return prt_list
    
    prt_list.append(prt)
    # 添加 prt 到 pid 授权列表
    result = add_user_prt(pid, user["email"], user["password"], prt_list)
    try:
        AfHandshakeDAO.sync_user_prts(user_id, result, status=1)
    except Exception as e:
        logger.warning("Handshake sync failed: pid=%s prt=%s -> %s", pid, prt, e)
    return result


def set_pb_config(username:str, password:str, pid:str):
    """设置 AppsFlyer PB 配置：
    - 先写入 Attribution Postbacks 基础配置
    - 设置 Regular Postbacks（install/inappevent）
    - 分别激活 install/inappevent
    - 设置 AP（Advanced Postbacks）
    - 成功后更新所用域名的状态为 1
    """
    import json
    import random
    import time

    # 获取一个的域名（来自 _tb_auto_cfg_pb）
    pb_config = AfPbConfigDAO.get_by_account(username)
    if not pb_config:
        logger.error("No active pb_config found in _tb_auto_cfg_pb; aborting PB config for pid %s", pid)
        raise Exception(f"{pid} no active pb_config ")
    
    def get_random_domain(csv_val:str):
        if not csv_val:
            return []
        parts = [p.strip() for p in str(csv_val).split(',')]
        domains = [p for p in parts if p]
        return random.choice(domains) if domains else None

    domain, cfg_id = get_random_domain(pb_config["pb_domain"]), pb_config["id"]

    if not domain:
        logger.error("No available domain found in pbDomain for pid %s", pid)
        raise Exception(f"{pid} no available domain in pbDomain")

    sess = get_session_by_user(username, password, pid)
    headers = {
        "Origin": "https://hq1.appsflyer.com",
        "Referer": "https://hq1.appsflyer.com/partners/integrations",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
    }

    # 枚举/常量（按用户要求在方法内定义）
    POSTBACK_PARAM_CONFIG_URL = f"https://hq1.appsflyer.com/partners/postbacks/{pid}/attribution"
    POSTBACK_PARAM_CONFIG = (
        '{"skadIds":[],"install":{"value":"&appname={app_name}&af_ip={ip}&af_lang={lang}&af_os={os}&af_ua={ua}&af_siteid={mp_siteid}&af_sub_siteid={sub_siteid}&af_ad={_c_}&af_ad_id={_u_}&af_adset={_c_}&af_channel={channel}&clickid={clickid}&advertising_id={gaid}&md5_advertising_id={md5_gaid}&android_id={aaid}&md5_android_id={md5_aaid}&idfa={idfa}&idfv={idfv}&af_model={device_model}&af_os_version={os_version}&imei={imei}","isActive":true}}'
    )

    POSTBACK_PARAM_URL = f"https://hq1.appsflyer.com/partners/postbacks/{pid}/regular"
    POSTBACK_PARAM = (
        '{"skadIds":[],'
        '"install":{"value":"http://%s?tag=%s&event=installation&mp_siteid=$$sdk(af_siteid)&_c_=$$sdk(af_ad)&_u_=$$sdk(af_ad_id)&_c_=$$sdk(af_adset)&affid=$$sdk(af_adset_id)&Android_Device_ID=$$sdk(android-device-id)&appid=$$sdk(app-id)&appname=$$sdk(app-name)&os_version=$$sdk(app-version-new)&attributed_type=$$sdk(attributed-touch-type)&subid7=$$sdk(blocked-reason)&subid8=$$sdk(blocked-reason-value)&subid9=$$sdk(blocked-sub-reason)&campaign=$$sdk(c)&affid=$$sdk(af_c_id)&geo=$$sdk(country-code)&idfv=$$sdk(vendorId)&idfa=$$sdk(ios-device-id)&installtime=$$sdk(install-ts)&Is_primary_attribution=$$sdk(is-primary)&lang=$$sdk(language)&Retargeting_conversion_type=$$sdk(retargeting-conversion-type)&sub_siteid=$$sdk(af_sub_siteid)&clickid=$$click(clickid)","isActive":true},'
        '"inappevent":{"value":"http://%s?tag=%s&mp_siteid=$$sdk(af_siteid)&_c_=$$sdk(af_ad)&_u_=$$sdk(af_ad_id)&_c_=$$sdk(af_adset)&affid=$$sdk(af_adset_id)&Android_Device_ID=$$sdk(android-device-id)&appid=$$sdk(app-id)&appname=$$sdk(app-name)&os_version=$$sdk(app-version-new)&attributed_type=$$sdk(attributed-touch-type)&subid7=$$sdk(blocked-reason)&subid8=$$sdk(blocked-reason-value)&subid9=$$sdk(blocked-sub-reason)&campaign=$$sdk(c)&affid=$$sdk(af_c_id)&geo=$$sdk(country-code)&idfv=$$sdk(vendorId)&idfa=$$sdk(ios-device-id)&installtime=$$sdk(install-ts)&Is_primary_attribution=$$sdk(is-primary)&lang=$$sdk(language)&Retargeting_conversion_type=$$sdk(retargeting-conversion-type)&sub_siteid=$$sdk(af_sub_siteid)&clickid=$$click(clickid)&event=$$sdk(event-name)&eventtime=$$sdk(timestamp)&eventid=$$sdk(mapped-iae)","isActive":true}}'
    )

    POSTBACK_PARAM_INSTALL_ACTIVE_URL = f"https://hq1.appsflyer.com/partners/integrations/activate/{pid}/install"
    POSTBACK_PARAM_EVENT_ACTIVE_URL = f"https://hq1.appsflyer.com/partners/integrations/activate/{pid}/inappevent"

    AP_URL = f"https://hq1.appsflyer.com/partners/postbacks/{pid}/ap"
    AP_PARAMS = (
        '{"skadIds":[],'
        '"install":{"value":"http://%s?tag=%s_advanced&event=installation&mp_siteid=$$sdk(af_siteid)&_c_=$$sdk(af_sub_siteid)&subid=$$sdk(af_c_id)&geo=$$sdk(country-code)&_u_=$$sdk(af_ad_id)&subid3=$$sdk(c)&_c_=$$sdk(af_adset_id)&eventtime=$$sdk(install-ts-hour-floor)&clicktime=$$sdk(click-ts-hour-floor)&_c_=$$sdk(af_ad)&subid7=$$sdk(blocked-reason)&subid8=$$sdk(blocked-reason-value)&subid9=$$sdk(blocked-sub-reason)&Is-first-event=$$sdk(is-first)&Is_primary_attribution=$$sdk(is-primary)&Is-reattribution=$$sdk(is-reattr)&Is-reengagement=$$sdk(is-reengage)&Is-rejected=$$sdk(is-rejected)&Is-retargeting=$$sdk(is-retarget)&Is-s2s=$$sdk(is-s2s-0-or-1)&postbackid=$$sdk(random-str)","isActive":true},'
        '"inappevent":{"value":"http://%s?tag=%s_advanced&mp_siteid=$$sdk(af_siteid)&_c_=$$sdk(af_sub_siteid)&subid=$$sdk(af_c_id)&geo=$$sdk(country-code)&_u_=$$sdk(af_ad_id)&subid3=$$sdk(c)&_c_=$$sdk(af_adset_id)&eventtime=$$sdk(install-ts-hour-floor)&clicktime=$$sdk(click-ts-hour-floor)&_c_=$$sdk(af_ad)&subid7=$$sdk(blocked-reason)&subid8=$$sdk(blocked-reason-value)&subid9=$$sdk(blocked-sub-reason)&Is-first-event=$$sdk(is-first)&Is_primary_attribution=$$sdk(is-primary)&Is-reattribution=$$sdk(is-reattr)&Is-reengagement=$$sdk(is-reengage)&Is-rejected=$$sdk(is-rejected)&Is-retargeting=$$sdk(is-retarget)&Is-s2s=$$sdk(is-s2s-0-or-1)&postbackid=$$sdk(random-str)&eventid=$$sdk(mapped-iae)","isActive":true}}'
    )

    # 1) Attribution 基础配置
    try:
        resp = request_with_retry(sess, "PUT", POSTBACK_PARAM_CONFIG_URL, data=POSTBACK_PARAM_CONFIG, headers=headers, timeout=30)
        resp.raise_for_status()
        logger.info("设置af pb操作返回结果 result:%s", (resp.text or "")[:300])
        time.sleep(1)
    except Exception as e:
        logger.warning("PUT attribution config failed for %s -> %s", pid, e)
        raise

    # 2) Regular Postbacks（install/inappevent）
    postBackParams = POSTBACK_PARAM % (domain, pid, domain, pid)
    try:
        r2 = request_with_retry(sess, "PUT", POSTBACK_PARAM_URL, data=postBackParams, headers=headers, timeout=30)
        r2.raise_for_status()
        logger.info("POSTBACK_PARAM 配置返回结果:%s", (r2.text or "")[:300])
        time.sleep(0.5)
    except Exception as e:
        logger.warning("PUT regular postbacks failed for %s -> %s", pid, e)
        raise

    # 3) 激活 install / inappevent
    # 解析返回 JSON，取 install/inappevent 的值写入激活接口
    try:
        pb_json = r2.json()
    except ValueError:
        try:
            pb_json = json.loads(r2.text or "{}")
            
        except Exception:
            pb_json = {}
    install_val = pb_json.get("install")
    inappevent_val = pb_json.get("inappevent")

    active_map = {"install": install_val, "inappevent": ""}
    try:
        r3 = request_with_retry(sess, "PUT", POSTBACK_PARAM_INSTALL_ACTIVE_URL, json=active_map, headers=headers, timeout=20)
        r3.raise_for_status()
        logger.info("install active 返回结果 %s", (r3.text or "")[:300])
        time.sleep(0.5)
    except Exception as e:
        logger.warning("Activate install failed for %s -> %s", pid, e)
        raise

    active_map = {"install": "", "inappevent": inappevent_val}
    try:
        r4 = request_with_retry(sess, "PUT", POSTBACK_PARAM_EVENT_ACTIVE_URL, json=active_map, headers=headers, timeout=20)
        r4.raise_for_status()
        logger.info("event active 返回结果 %s", (r4.text or "")[:300])
        time.sleep(0.5)
    except Exception as e:
        logger.warning("Activate inappevent failed for %s -> %s", pid, e)
        raise

    # 4) AP（Advanced Postbacks）
    apParams = AP_PARAMS % (domain, pid, domain, pid)
    try:
        r5 = request_with_retry(sess, "PUT", AP_URL, data=apParams, headers=headers, timeout=30)
        r5.raise_for_status()
        logger.info("ap返回结果result:%s", (r5.text or "")[:300])
        time.sleep(0.5)
    except Exception as e:
        logger.warning("PUT AP postbacks failed for %s -> %s", pid, e)
        raise

    # 5) 标记状态为 1（成功）
    try:
        AfPbConfigDAO.mark_config_active_by_id(cfg_id, status=1)
    except Exception as e:
        logger.warning("Mark config active failed: id=%s, pid=%s, err=%s", cfg_id, pid, e)
        raise


def set_adv_privacy(username:str = None, password:str = None, pid:str = None, append_note:str = ''):
    """设置广告隐私中的部分参数"""
    note = ''
    config_note = ''
    if pid is None and username is not None:
        user = UserDAO.get_user_by_email(username)
        pid_config = PidConfigDAO.get_by_email(pid)
        if user is None or user["pid"] is None or pid_config is None:
            raise ValueError(f"User or User Pid not found for email: {username}")
        if user:
            pid = user["pid"]
            username = user["email"]
            password = user["password"]
            note = user["note"]
        if pid_config:
            pid = pid_config["pid"]
            config_note = pid_config["note"]
            username = pid_config["email"]
            password = pid_config["password"]

    if pid is not None and (username is None or password is None):
        user = UserDAO.get_user_by_pid(pid)
        pid_config = PidConfigDAO.get_by_pid(pid)
        if user is None or user["email"] is None or user["password"] is None or pid_config is None:
            raise ValueError(f"User or User Credentials not found for pid: {pid}")
        if user:
            note = user["note"]
            username = user["email"]
            password = user["password"]
        if pid_config:
            config_note = pid_config["note"]
            username = pid_config["email"]
            password = pid_config["password"]

    if pid is None or username is None or password is None:
        raise ValueError("pid, username, password are required")
    
    # 获取已配置的信息
    try:
        URL = f"https://hq1.appsflyer.com/partners/postbacks/{pid}/ap"
        HEADERS = {
            "Referer": URL
        }
        logger.info("GET adv privacy url:%s, headers:%s pid:%s", URL, HEADERS, pid)
        sess = get_session_by_user(username, password, pid)
        resp = request_with_retry(sess, "GET", URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        resp_json = resp.json()

        if resp_json["inappevent"]["value"] == '':
            logger.warning("inappevent value is empty for %s", pid)
            return

        inappevent_val = json.loads(resp_json["inappevent"]["value"])
        install_val = json.loads(resp_json["install"]["value"])

        logger.info("GET adv privacy data success")

        append_arg = "$$sdk(af_siteid)&privacy_params=$$sdk(af_sub_siteid)&subid=$$sdk(af_c_id)&geo=$$sdk(country-code)&_u_=$$sdk(af_ad_id)&subid3=$$sdk(c)&adsetid=$$sdk(af_adset_id)&eventtime=$$sdk(install-ts-hour-floor)&clicktime=$$sdk(click-ts-hour-floor)&_c_=$$sdk(af_ad)&subid7=$$sdk(blocked-reason)&subid8=$$sdk(blocked-reason-value)&subid9=$$sdk(blocked-sub-reason)&Is-first-event=$$sdk(is-first)&Is_primary_attribution=$$sdk(is-primary)&Is-reattribution=$$sdk(is-reattr)&Is-reengagement=$$sdk(is-reengage)&Is-rejected=$$sdk(is-rejected)&Is-retargeting=$$sdk(is-retarget)&Is-s2s=$$sdk(is-s2s-0-or-1)&postbackid=$$sdk(random-str)&eventid=$$sdk(mapped-iae)&task_time=$$sdk(action-type)"
        inappevent_val= inappevent_val[0] + append_arg
        install_val = install_val[0] + append_arg
        new_data = {
            "install": {
                "isActive":True,
                "value" : install_val
            },
            "inappevent": {
                "isActive":True,
                "value" : inappevent_val
            }
        }
        resp = request_with_retry(sess, "PUT", URL, json=new_data, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        logger.info("SET adv privacy data success")

        PidConfigDAO.update_note_by_pid(pid, config_note or '' + append_note)
        UserDAO.update_note_by_pid(pid, note or '' + append_note)

    except Exception as e:
        logger.warning("SET adv privacy failed for %s -> %s", pid, e)
        PidConfigDAO.update_note_by_pid(pid, config_note or '' + "|刷新失败")
        UserDAO.update_note_by_pid(pid, note or '' + "|刷新失败")
        raise