from __future__ import annotations

"""命令行入口：
$ python main.py sync_apps          # 同步用户 App 列表
$ python main.py sync_data --days 7 # 同步最近 7 天数据
"""

import argparse
import logging

from core.logger import setup_logging  # noqa
from config.settings import USE_PROXY, CRAWLER

logger = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser(description="AppsFlyer Crawler")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("sync_apps", help="同步用户 App 列表")

    p_data = sub.add_parser("sync_data", help="同步用户 App 数据")
    p_data.add_argument("--days", type=int, default=1, help="向前同步的天数，默认 1")

    return parser.parse_args()


def _print_startup_info():
    """打印启动配置信息"""
    logger.info("=" * 50)
    logger.info("启动配置信息:")
    logger.info("代理状态: %s", "开启" if USE_PROXY else "关闭")
    logger.info("进程数: %d", CRAWLER["processes"])
    logger.info("每进程线程数: %d", CRAWLER["threads_per_process"])
    logger.info("最大重试次数: %d", CRAWLER["max_retry"])
    logger.info("重试延迟: %d秒", CRAWLER["retry_delay_seconds"])
    logger.info("=" * 50)


if __name__ == "__main__":
    args = _parse_args()

    # 打印启动信息
    _print_startup_info()

    if args.command == "sync_apps":
        from tasks.sync_user_apps import run as sync_apps_run

        logger.info("=== sync_apps start ===")
        sync_apps_run()

    elif args.command == "sync_data":
        from tasks.sync_app_data import run as sync_data_run

        logger.info("=== sync_data start days=%d ===", args.days)
        sync_data_run(days=args.days)

    else:
        logger.error("unknown command")


