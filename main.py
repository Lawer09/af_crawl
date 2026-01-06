from __future__ import annotations

"""命令行入口：
$ python main.py sync_apps          # 同步用户 App 列表
$ python main.py sync_data --days 7 # 同步最近 7 天数据
$ python main.py web               # 启动Web管理界面
$ python main.py distribute master  # 启动分布式主节点
$ python main.py distribute worker --master-host localhost --port 7989  # 启动分布式工作节点

"""

import argparse
import logging
import sys
from pathlib import Path

from core.logger import setup_logging  # noqa
from config.settings import USE_PROXY, CRAWLER

logger = logging.getLogger(__name__)

def _parse_args():
    parser = argparse.ArgumentParser(description="AppsFlyer Crawler")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("sync_apps", help="同步用户 App 列表")
    p_apps_cron = sub.add_parser("sync_apps_cron", help="定时同步用户 App 列表")
    p_apps_cron.add_argument("--interval-minutes", type=int, default=60, help="执行间隔(分钟)，默认60")

    p_data = sub.add_parser("sync_data", help="同步用户 App 数据")
    p_data.add_argument("--days", type=int, default=1, help="向前同步的天数，默认 1")
    p_data_cron = sub.add_parser("sync_data_cron", help="定时同步用户 App 数据（每日）")
    p_data_cron.add_argument("--interval-hours", type=int, default=24, help="执行间隔(小时)，默认24")
    # 统一定时任务入口：可选择同时启动多个定时任务
    p_cron = sub.add_parser("cron", help="统一定时任务入口")
    p_cron.add_argument("--apps", action="store_true", help="启动应用列表更新定时任务")
    p_cron.add_argument("--apps-interval-minutes", type=int, default=24, help="应用任务执行间隔(分钟)，默认24小时")
    p_cron.add_argument("--data", action="store_true", help="启动应用数据更新定时任务")
    p_cron.add_argument("--data-interval-hours", type=int, default=24, help="数据任务执行间隔(小时)，默认24")
    
    sub.add_parser("web", help="启动Web管理界面")
    
    # 分布式命令
    p_distribute = sub.add_parser("distribute", help="分布式任务系统")
    distribute_sub = p_distribute.add_subparsers(dest="distribute_command", required=True)
    
    # Master节点
    p_master = distribute_sub.add_parser("master", help="启动主节点")
    p_master.add_argument("--device-id", help="设备ID（可选，未提供时自动生成）")
    p_master.add_argument("--device-name", help="设备名称")
    p_master.add_argument("--host", default="localhost", help="监听地址")
    p_master.add_argument("--port", type=int, default=7989, help="监听端口")
    p_master.add_argument("--config", help="配置文件路径")
    
    # Worker节点
    p_worker = distribute_sub.add_parser("worker", help="启动工作节点")
    p_worker.add_argument("--device-id", help="设备ID（可选，未提供时自动生成）")
    p_worker.add_argument("--device-name", help="设备名称")
    p_worker.add_argument("--master-host", help="主节点地址（可选，未提供时从配置文件读取）")
    p_worker.add_argument("--master-port", type=int, default=7989, help="主节点端口")
    p_worker.add_argument("--heartbeat-interval", type=int, default=30, help="心跳间隔(秒)")
    p_worker.add_argument("--task-pull-limit", type=int, default=5, help="任务拉取限制")
    p_worker.add_argument("--concurrent-tasks", type=int, default=3, help="并发任务数")
    p_worker.add_argument("--enable-monitoring", action="store_true", help="启用性能监控")
    p_worker.add_argument("--api-key", help="API密钥")
    p_worker.add_argument("--config", help="配置文件路径")
    
    # Standalone节点
    p_standalone = distribute_sub.add_parser("standalone", help="启动独立节点")
    p_standalone.add_argument("--device-id", help="设备ID（可选，未提供时自动生成）")
    p_standalone.add_argument("--device-name", help="设备名称")
    p_standalone.add_argument("--dispatch-interval", type=int, default=10, help="任务分发间隔(秒)")
    p_standalone.add_argument("--concurrent-tasks", type=int, default=5, help="并发任务数")
    p_standalone.add_argument("--enable-monitoring", action="store_true", help="启用性能监控")
    p_standalone.add_argument("--config", help="配置文件路径")
    
    # 状态查询
    p_status = distribute_sub.add_parser("status", help="查看系统状态")
    p_status.add_argument("--master-host", default="localhost", help="主节点地址")
    p_status.add_argument("--master-port", type=int, default=7989, help="主节点端口")

    sub.add_parser("task", help="本地任务处理")
    sub.add_parser("create_tasks", help="创建应用数据任务")

    sub.add_parser("init_data", help="创建应用数据任务")

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
        from services import app_service
        logger.info("=== sync_apps start ===")
        app_service.update_user_apps()

    elif args.command == "web":
        from web_app import app
        import uvicorn
        
        logger.info("=== Starting web server ===")
        uvicorn.run(app, host="0.0.0.0", port=8880)
    
    elif args.command == "distribute":
        # 导入分布式CLI
        from cli.distribution_cli import DistributionCLI
        
        cli = DistributionCLI()
        
        if args.distribute_command == "master":
            logger.info("=== Starting distribution master ===")
            sys.exit(cli.run_master(args))
        elif args.distribute_command == "worker":
            logger.info("=== Starting distribution worker ===")
            sys.exit(cli.run_worker(args))
        elif args.distribute_command == "standalone":
            logger.info("=== Starting distribution standalone ===")
            sys.exit(cli.run_standalone(args))
        elif args.distribute_command == "status":
            sys.exit(cli.show_status(args))
        else:
            logger.error("unknown distribute command: %s", args.distribute_command)
            sys.exit(1)

    elif args.command == "cron":
        from schedulers.app_jobs import start_update_apps_scheduler, run_data
        started = False
        if args.apps:
            start_update_apps_scheduler(interval_minutes=args.apps_interval_minutes)
            started = True
        if args.data:
            run_data()
            started = True
        if not started:
            logger.error("请至少选择一个定时任务，例如: --apps 或 --data")
            sys.exit(1)
        # try:
        #     while True:
        #         time.sleep(60)
        # except KeyboardInterrupt:
        #     logger.info("cron stopped by user")
    
    elif args.command == "task":
        from tasks import task_manager
        import faulthandler
        Path("logs").mkdir(exist_ok=True)
        _dump_file = open("logs/thread_dump.log", "a")
        try:
            faulthandler.dump_traceback_later(600, repeat=True, file=_dump_file)
        except Exception:
            logger.warning("faulthandler 初始化失败，跳过线程栈转储")
        task_manager.run()

    elif args.command == "create_tasks":
        from services import task_service
        logger.info("=== create_tasks start ===")
        task_service.create_af_now_task()
    
    elif args.command == "sched":
        from schedulers.af_jobs import run_jobs
        logger.info("=== run_jobs start ===")
        run_jobs()

    elif args.command == "init_data":
        from scripts import system_init

    else:
        logger.error("unknown command: %s", args.command)
        sys.exit(1)


