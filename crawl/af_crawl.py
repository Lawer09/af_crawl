from . import crawl_tool as ct
import exception
import urllib3
import config.af_config as cfg
import setting
import log
import proxy
import stores_m as stores
from utils import timeGen
import time
# 禁用HTTPS证书验证警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MAX_LOGIN_COUNT = 3

def af_login(af_user, use_proxies = True):
    log.set_header(f"{af_user['email']} PID_FLAG: {is_pid_user(af_user)}")
    try:
        cookie_str, proxy_url, login_count = stores.cookies.get_cookie_proxy(af_user["email"])
        
        # 当此的ip次数过多，等待10分钟时间后重试
        if login_count >= MAX_LOGIN_COUNT:
            raise exception.CrawlRetryErr(10*60)
        
        if use_proxies and not proxy_url:
            cookie_str = None

        if not cookie_str:
            login_data = {
                "keep-user-logged-in": False,
                "username": af_user["email"], 
                "password": af_user["password"],
            }
            proxy_url = proxy.get_new_proxy() if use_proxies else None
            proxies = proxy.build_requests_proxy(proxy_url)
            cookie_str = ct.login_request(cfg.LOGIN_API, login_data, proxies=proxies)
            stores.cookies.save_cookie_proxy(af_user["email"], cookie_str, proxy_url)
            stores.cookies.increment_login_count(af_user["email"])
            log.info(f"登录成功，proxy: {proxy_url}")
            
        return {"Cookie": cookie_str}, proxy.build_requests_proxy(proxy_url)
    
    except exception.CrawlLoginErr as e:
        raise exception.CrawlErr(e)
    
def get_home_apps(af_user, use_proxies = setting.USE_PROXY):
    """获取用户所有app信息, prt用户包含timezone 和prt ，pid用户得通过app_id进一步获取"""

    cookie_header, proxies = af_login(af_user, use_proxies)
    if not cookie_header:
        return None
    try:
        json_data = {}
        username = af_user["email"]
        _is_pid_user = is_pid_user(af_user)

        proxies = proxies if use_proxies else None

        if _is_pid_user:
            json_data = ct.get(cfg.HOME_APP_URL_PID, cookie_header, proxies)
        else:
            cookie_header["Referer"] = "https://hq1.appsflyer.com/apps/myapps"
            json_data = ct.get(cfg.HOME_APP_URL_PRT, cookie_header)
        apps = []

        if _is_pid_user:
            if "data" not in json_data:
                raise exception.CrawlErr(json_data)
            # format
            for app in json_data["data"]:
                apps.append({
                    "username": username,
                    "app_id": app["app_id"],
                    "platform": app["platform"],
                    "timezone": None,
                    "user_type_id": None
                })
        else:
            if "apps" not in json_data or "user" not in json_data:
                raise exception.CrawlErr(f"格式错误{json_data}")
            prt = json_data["user"]["agencyId"]
            # format
            for app in json_data["apps"]:
                if app["deleted"] == True:
                    continue
                apps.append({
                    "username": username,
                    "app_id": app["id"],
                    "platform": app["platform"],
                    "timezone": app["localization"]["timezone"],
                    "user_type_id": prt
                })

        return apps
    
    except exception.CrawlRetryErr as e:
        raise e 
    
    except exception.CrawlErr as e:
        raise e
    
    except Exception as e:
        raise exception.CrawlErr(e)
    
def is_pid_user(af_user):
    return af_user["account_type"] == "pid"


def login_out(af_user):
    log.info("退出登录")
    stores.cookies.delete_cookie(af_user["email"])
    log.set_header("")


def get_app_info(af_user, appid, use_proxies = setting.USE_PROXY):
    """主要获取app时区,附带获取pid 或 prt 信息，根据当前角色"""
    if not appid:
        raise exception.CrawlErr("appid不能为空")
        
    cookie_header, proxies = af_login(af_user, use_proxies)
    if not cookie_header:
        return None
    proxies = proxies if use_proxies else None

    url = cfg.APP_INFO + appid
    _is_pid_user = is_pid_user(af_user)

    try:
        json_data = ct.get(url, cookie_header, proxies)
        if json_data and "apps" in json_data and "user" in json_data:
            app_info = {
                "timezone" : json_data["apps"][0]["localization"]["timezone"],
            }
            pid_or_prt = {"pid":json_data["user"]["adnetId"]} if _is_pid_user else {"prt":json_data["user"]["agencyId"]}
            app_info.update(pid_or_prt)

            return app_info
        
        log.error(f"返回的数据格式错误，{json_data}")
        return None
    
    except exception.CrawlRetryErr as e:
        raise e
    
    except exception.CrawlErr as e:
        raise e
    
    except Exception as e:
        raise exception.CrawlErr(e)



def get_table_data_new(af_user, appid, start_date, end_date, use_proxies = setting.USE_PROXY):
    """获取table数据, offer_id, af_clicks, af_install"""

    cookie_header, proxies = af_login(af_user, use_proxies)
    if not cookie_header:
        return None
    
    proxies = proxies if use_proxies else None

    cookie_header["Referer"] = cfg.NEW_TABLE_API_REFERER

    try:
        filter_data = cfg.NEW_TABLE_API_PARAM.copy()
        filter_data["dates"] = {
            "start":start_date,
            "end":end_date
        }
        filter_data["filters"]["app-id"] = [appid]
        filter_data["groupings"] = ["adset", "filter_data"]
        json_data = ct.post(cfg.NEW_TABLE_API, filter_data, cookie_header, proxies)
        
        table_data = []

        if not json_data or "data" not in json_data:
            return table_data
        
        # 获取每一列数据
        for adset_data in json_data.get("data", []):
            if "adset" not in adset_data or adset_data["adset"] == "None":
                continue
            adset_value = adset_data["adset"]
            
            # 判断adset是否是正常值
            if not adset_value.isdigit():
                # raise exception.CrawlErr(f"adset值异常 {adset_data}")
                continue
            
            table_data_item = {
                "offer_id" : adset_value,
                "af_clicks" : adset_data.get("filtersGranularityMetricIdClicksPeriod", 0),
                "af_install" : adset_data.get("attributionSourceAppsflyerFiltersGranularityMetricIdInstallsUaPeriod", 0)
            }
            table_data.append(table_data_item)
        return table_data
    
    except exception.CrawlRetryErr as e:
        raise e
    
    except exception.CrawlErr as e:
        raise e
    
    except Exception as e:
        raise exception.CrawlErr(e)



# 获取应用下table统计数据, offer_id-clicks-installs  pid 或者 prt 通过查询数据库
def get_table_data_f(af_user, appid, start_date, end_date, use_proxies = setting.USE_PROXY):
    """获取table数据, offer_id, af_clicks, af_install"""

    cookie_header, proxies = af_login(af_user, use_proxies)
    if not cookie_header:
        return None
    
    proxies = proxies if use_proxies else None

    _is_pid_user = is_pid_user(af_user)

    cookie_header["Referer"] = "https://hq1.appsflyer.com/unified-ltv/dashboard"

    try:
        # 首先查询 adset
        filter_data = cfg.GROUP_FILTER_PID.copy() if _is_pid_user else cfg.GROUP_FILTER_PRT.copy()
        filter_data["filters"]["app_id"] = [appid]
        filter_data["start_date"] = start_date
        filter_data["end_date"] = end_date
        filter_data["limit"] = cfg.TABLE_DATA_COUNT_LIMIT
        filter_data["groupings"] = ["adset", "filter_data"]
        json_data = ct.post(cfg.TABLE_URL, filter_data, cookie_header, proxies)
        
        table_data = []

        if not json_data or "data" not in json_data:
            return table_data
        
        # 获取每一列数据
        for adset_data in json_data.get("data", []):
            if "adset" not in adset_data or adset_data["adset"] == "None":
                continue
            adset_value = adset_data["adset"]
            
            # 判断adset是否是正常值
            if not adset_value.isdigit():
                # raise exception.CrawlErr(f"adset值异常 {adset_data}")
                continue
            
            table_data_item = {
                "offer_id" : adset_data["adset"],
                "af_clicks" : adset_data.get("clicks", 0),
                "af_install" : adset_data.get("installs", 0)
            }
            table_data.append(table_data_item)
        return table_data
    
    except exception.CrawlRetryErr as e:
        raise e
    
    except exception.CrawlErr as e:
        raise e
    
    except Exception as e:
        raise exception.CrawlErr(e)


# 获取应用下table统计数据, offer_id-prt|pid-clicks-installs
# 首先group查询offer_id(adset)获取pid 或 prt 需要展开查询
def get_table_data(af_user, appid, start_date, end_date, use_proxies = setting.USE_PROXY):
    """获取table数据"""

    cookie_header, proxies = af_login(af_user, use_proxies)
    if not cookie_header:
        return None
    
    proxies = proxies if use_proxies else None

    _is_pid_user = is_pid_user(af_user)

    cookie_header["Referer"] = "https://hq1.appsflyer.com/unified-ltv/dashboard"

    try:
        # 首先查询 adset
        filter_data = cfg.GROUP_FILTER_PID.copy() if _is_pid_user else cfg.GROUP_FILTER_PRT.copy()
        filter_data["filters"]["app_id"] = [appid]
        filter_data["start_date"] = start_date
        filter_data["end_date"] = end_date
        filter_data["limit"] = cfg.TABLE_DATA_COUNT_LIMIT
        filter_data["groupings"] = ["adset", "filter_data"]
        json_data = ct.post(cfg.TABLE_URL, filter_data, cookie_header, proxies)
        
        table_data = []

        if not json_data or "data" not in json_data:
            return table_data
        
        # 获取每一列数据
        for adset_data in json_data.get("data", []):
            if "adset" not in adset_data or adset_data["adset"] == "None":
                continue
            adset = adset_data["adset"]
            table_data_item = {
                "offer_id" : adset_data["adset"],
                "af_clicks" : adset_data.get("clicks", 0),
                "af_install" : adset_data.get("installs", 0)
            }
            # 展开获取prt 或 pid
            filter_data["filters"].update({"adset": [{"adset": [adset], "filter_origin": "ltv"}]})
            filter_data["groupings"] = ["partner"] if _is_pid_user else ["media_source"]
            
            expand_data = ct.post(cfg.TABLE_EXPAND_URL, filter_data, cookie_header, proxies)
            expand_item = expand_data.get("data", [{"partner":"None", "media_source":"None"}])
            pid_or_prt = {"pid": expand_item[0].get("partner", "None")} if _is_pid_user else {"prt": expand_item[0].get("media_source", "None")}
            table_data_item.update(pid_or_prt)
            table_data.append(table_data_item)

        return table_data
    
    except exception.CrawlFrequentErr as e:
        if not setting.USE_PROXY:
            raise exception.IPLimitError(e)
        login_out(af_user)
        return get_table_data(af_user, appid, start_date, end_date)
    
    except exception.IPLimitError as e:
        raise exception.IPLimitError(e)
    
    except Exception as e:
        log.error("get_table_data fail "+ e)
        return None
   
if __name__ == "__main__":
    af_user = {
        "email":"anitaz@mobisoapad.com",
        "password":"Sappoc@123",
        "account_type":"pid",
    }

    print(get_home_apps(af_user, use_proxies=False)) 
    pass
   
