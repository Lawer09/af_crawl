from __future__ import annotations

import logging
import schedule
import time

from model.af_onelink_template import AfOnelinkTemplateDAO, AfCrawlUserDAO
from services.af_config_service import get_onlink_templates
from services.fs_service import send_feishu_text

logger = logging.getLogger(__name__)

FS_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/067033b4-ac8d-4f41-85ec-4852df148932"

def remove_duplicate_by_field(arr1, arr2, field):
    """
    从arr2中剔除与arr1指定字段值相同的元素
    :param arr1: 参考数组（字典数组）
    :param arr2: 待剔除的数组（字典数组）
    :param field: 指定的字段名（如 'id'、'name'）
    :return: 剔除后的arr2
    """
    # 提取arr1中指定字段的所有值，存入集合（O(1)查询）
    arr1_field_values = {item[field] for item in arr1 if field in item}
    # 筛选arr2中指定字段值不在arr1中的元素
    filtered_arr2 = [item for item in arr2 if field not in item or item[field] not in arr1_field_values]
    return filtered_arr2

def remove_duplicate_bidirectional(arr1, arr2, field):
    """
    双向剔除：arr1和arr2都剔除指定字段值相同的元素
    :param arr1: 第一个字典数组
    :param arr2: 第二个字典数组
    :param field: 指定字段名
    :return: (剔除后的arr1, 剔除后的arr2)
    """
    # 提取两个数组的指定字段值集合
    arr1_fields = {item[field] for item in arr1 if field in item}
    arr2_fields = {item[field] for item in arr2 if field in item}
    
    # 找出两个集合的交集（重复的字段值）
    duplicate_fields = arr1_fields & arr2_fields
    
    # 分别筛选两个数组，剔除重复字段值的元素
    filtered_arr1 = [item for item in arr1 if field not in item or item[field] not in duplicate_fields]
    filtered_arr2 = [item for item in arr2 if field not in item or item[field] not in duplicate_fields]
    
    return filtered_arr1, filtered_arr2

def build_onlink_templates_change_notify(old_templates: dict, new_templates: dict) -> str:
    """构建onelink模板变化通知。"""
    old_templates_str = [f"{template['label']} {template['base_url']} \n" for template in old_templates]    
    new_templates_str = [f"{template['label']} {template['base_url']} \n" for template in new_templates]
    return f"旧模板:\n{old_templates_str}\n新模板:\n{new_templates_str}"


def crawl_users_onelink_templates_job() -> None:
    """定时任务：每天更新onelink模板信息。"""
    crawl_users = AfCrawlUserDAO.get_all()
    if crawl_users:
        for user in crawl_users:
            templates = get_onlink_templates(user["username"], user["password"], user["app_id"], user["pid"])
            if templates:
                try:
                    existing_templates = AfOnelinkTemplateDAO.get_templates(user["pid"], user["app_id"])
                    if existing_templates:
                        existing_templates, templates = remove_duplicate_bidirectional(existing_templates, templates, "base_url")
                        AfOnelinkTemplateDAO.delete(user["pid"], user["app_id"])
                        change_notify = build_onlink_templates_change_notify(existing_templates, templates)
                        send_feishu_text(FS_WEBHOOK, f"pid {user['pid']} {user['username']} 的onelink模板发生变化\n {change_notify}")
                    AfOnelinkTemplateDAO.save_all(templates)
                except Exception as e:
                    logger.error(f"Failed to save onelink templates for user {user['username']}: {e}")
            else:
                logger.error(f"Failed to get onelink templates for user {user['username']}")
    else:
        logger.info("No crawl users found.")

schedule.every().day.at("01:00").do(crawl_users_onelink_templates_job)

def run_jobs() -> None:
    """运行定时任务。"""
    while True:
        schedule.run_pending()
        time.sleep(1)  # 每1秒检查一次任务


def run_jobs_once() -> None:
    """运行定时任务。"""
    crawl_users_onelink_templates_job()