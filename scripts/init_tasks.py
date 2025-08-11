#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务初始化脚本

功能：
1. 初始化用户应用同步任务 (user_apps)
2. 初始化应用数据同步任务 (app_data)
3. 重置失败的任务
4. 恢复超时的任务
5. 清理过期任务
6. 批量创建任务

使用方法：
    python scripts/init_tasks.py --help
    python scripts/init_tasks.py init-user-apps
    python scripts/init_tasks.py init-app-data --days 7
    python scripts/init_tasks.py reset-failed
    python scripts/init_tasks.py recover-timeout
    python scripts/init_tasks.py clean-old --days 30
    python scripts/init_tasks.py stats
"""

import sys
import os
import argparse
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.crawl_task import CrawlTaskDAO
from model.user import UserDAO
from model.user_app import UserAppDAO
from core.db import mysql_pool

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaskInitializer:
    """任务初始化器"""
    
    def __init__(self):
        """初始化"""
        # 确保数据库表存在
        CrawlTaskDAO.init_table()
        UserAppDAO.init_table()
        
    def init_user_apps_tasks(self, force: bool = False) -> int:
        """
        初始化用户应用同步任务
        
        Args:
            force: 是否强制重新创建任务（即使已存在）
            
        Returns:
            创建的任务数量
        """
        logger.info("开始初始化用户应用同步任务...")
        
        # 检查是否已有待处理的任务
        if not force:
            existing_tasks = CrawlTaskDAO.fetch_pending('user_apps', 1)
            if existing_tasks:
                logger.info(f"已存在 {len(existing_tasks)} 个待处理的用户应用同步任务，跳过初始化")
                return 0
        
        # 获取所有启用的用户
        users = UserDAO.get_enabled_users()
        if not users:
            logger.warning("没有找到启用的用户")
            return 0
            
        logger.info(f"找到 {len(users)} 个启用用户")
        
        # 创建任务
        init_tasks = []
        for user in users:
            task = {
                'task_type': 'user_apps',
                'username': user['email'],
                'next_run_at': datetime.now().isoformat(),
                'priority': 1,
                'execution_timeout': 1800,  # 30分钟
                'max_retry_count': 3
            }
            init_tasks.append(task)
        
        if init_tasks:
            CrawlTaskDAO.add_tasks(init_tasks)
            logger.info(f"成功创建 {len(init_tasks)} 个用户应用同步任务")
            
        return len(init_tasks)
    
    def init_app_data_tasks(self, days: int = 1, force: bool = False) -> int:
        """
        初始化应用数据同步任务
        
        Args:
            days: 同步的天数
            force: 是否强制重新创建任务
            
        Returns:
            创建的任务数量
        """
        logger.info(f"开始初始化应用数据同步任务（{days}天）...")
        
        # 检查是否已有待处理的任务
        if not force:
            existing_tasks = CrawlTaskDAO.fetch_pending('app_data', 1)
            if existing_tasks:
                logger.info(f"已存在 {len(existing_tasks)} 个待处理的应用数据同步任务，跳过初始化")
                return 0
        
        # 获取所有活跃的用户应用
        apps = UserAppDAO.get_all_active()
        if not apps:
            logger.warning("没有找到活跃的用户应用")
            return 0
            
        logger.info(f"找到 {len(apps)} 个活跃应用")
        
        # 生成日期范围
        def daterange(days: int):
            """生成日期范围"""
            today = date.today()
            for i in range(days):
                d = today - timedelta(days=i + 1)
                yield d.isoformat(), d.isoformat()
        
        # 创建任务
        init_tasks = []
        for app in apps:
            for start_date_str, end_date_str in daterange(days):
                task = {
                    'task_type': 'app_data',
                    'username': app['username'],
                    'app_id': app['app_id'],
                    'start_date': start_date_str,
                    'end_date': end_date_str,
                    'next_run_at': datetime.now().isoformat(),
                    'priority': 0,
                    'execution_timeout': 3600,  # 1小时
                    'max_retry_count': 3
                }
                init_tasks.append(task)
        
        if init_tasks:
            CrawlTaskDAO.add_tasks(init_tasks)
            logger.info(f"成功创建 {len(init_tasks)} 个应用数据同步任务")
            
        return len(init_tasks)
    
    def reset_failed_tasks(self, task_type: Optional[str] = None) -> int:
        """
        重置失败的任务
        
        Args:
            task_type: 任务类型，None表示所有类型
            
        Returns:
            重置的任务数量
        """
        logger.info(f"开始重置失败的任务（类型：{task_type or '全部'}）...")
        
        try:
            if task_type:
                sql = f"""
                UPDATE {CrawlTaskDAO.TABLE} 
                SET status='pending', retry=0, assigned_device_id=NULL, assigned_at=NULL, 
                    next_run_at=NOW(), updated_at=NOW()
                WHERE status='failed' AND task_type=%s
                """
                result = mysql_pool.execute(sql, (task_type,))
            else:
                sql = f"""
                UPDATE {CrawlTaskDAO.TABLE} 
                SET status='pending', retry=0, assigned_device_id=NULL, assigned_at=NULL, 
                    next_run_at=NOW(), updated_at=NOW()
                WHERE status='failed'
                """
                result = mysql_pool.execute(sql)
            
            logger.info(f"成功重置 {result} 个失败任务")
            return result
            
        except Exception as e:
            logger.exception(f"重置失败任务时出错: {e}")
            return 0
    
    def recover_timeout_tasks(self, timeout_hours: int = 2) -> int:
        """
        恢复超时的任务
        
        Args:
            timeout_hours: 超时小时数
            
        Returns:
            恢复的任务数量
        """
        logger.info(f"开始恢复超时任务（超时时间：{timeout_hours}小时）...")
        
        try:
            timeout_time = datetime.now() - timedelta(hours=timeout_hours)
            
            sql = f"""
            UPDATE {CrawlTaskDAO.TABLE} 
            SET status='pending', assigned_device_id=NULL, assigned_at=NULL, 
                next_run_at=NOW(), updated_at=NOW()
            WHERE status IN ('assigned', 'running') 
              AND (assigned_at IS NULL OR assigned_at < %s)
            """
            
            result = mysql_pool.execute(sql, (timeout_time,))
            logger.info(f"成功恢复 {result} 个超时任务")
            return result
            
        except Exception as e:
            logger.exception(f"恢复超时任务时出错: {e}")
            return 0
    
    def clean_old_tasks(self, days: int = 30, status_list: List[str] = None) -> int:
        """
        清理过期任务
        
        Args:
            days: 保留天数
            status_list: 要清理的状态列表，默认为['done', 'failed']
            
        Returns:
            清理的任务数量
        """
        if status_list is None:
            status_list = ['done', 'failed']
            
        logger.info(f"开始清理 {days} 天前的任务（状态：{status_list}）...")
        
        try:
            cutoff_time = datetime.now() - timedelta(days=days)
            
            # 构建状态条件
            status_placeholders = ','.join(['%s'] * len(status_list))
            
            sql = f"""
            DELETE FROM {CrawlTaskDAO.TABLE} 
            WHERE status IN ({status_placeholders}) 
              AND updated_at < %s
            """
            
            params = status_list + [cutoff_time]
            result = mysql_pool.execute(sql, params)
            
            logger.info(f"成功清理 {result} 个过期任务")
            return result
            
        except Exception as e:
            logger.exception(f"清理过期任务时出错: {e}")
            return 0
    
    def get_task_stats(self) -> Dict:
        """
        获取任务统计信息
        
        Returns:
            任务统计字典
        """
        try:
            # 按状态统计
            sql = f"""
            SELECT status, COUNT(*) as count
            FROM {CrawlTaskDAO.TABLE}
            GROUP BY status
            """
            status_stats = mysql_pool.select(sql)
            
            # 按任务类型统计
            sql = f"""
            SELECT task_type, status, COUNT(*) as count
            FROM {CrawlTaskDAO.TABLE}
            GROUP BY task_type, status
            """
            type_stats = mysql_pool.select(sql)
            
            # 最近24小时统计
            sql = f"""
            SELECT status, COUNT(*) as count
            FROM {CrawlTaskDAO.TABLE}
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            GROUP BY status
            """
            recent_stats = mysql_pool.select(sql)
            
            return {
                'status_stats': {row['status']: row['count'] for row in status_stats},
                'type_stats': type_stats,
                'recent_24h_stats': {row['status']: row['count'] for row in recent_stats}
            }
            
        except Exception as e:
            logger.exception(f"获取任务统计时出错: {e}")
            return {}
    
    def create_custom_task(self, task_type: str, username: str, 
                          app_id: Optional[str] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None,
                          priority: int = 0,
                          task_data: Optional[Dict] = None) -> bool:
        """
        创建自定义任务
        
        Args:
            task_type: 任务类型
            username: 用户名
            app_id: 应用ID
            start_date: 开始日期
            end_date: 结束日期
            priority: 优先级
            task_data: 任务数据
            
        Returns:
            是否创建成功
        """
        try:
            task = {
                'task_type': task_type,
                'username': username,
                'app_id': app_id,
                'start_date': start_date,
                'end_date': end_date,
                'priority': priority,
                'task_data': task_data,
                'next_run_at': datetime.now().isoformat(),
                'execution_timeout': 3600,
                'max_retry_count': 3
            }
            
            CrawlTaskDAO.add_tasks([task])
            logger.info(f"成功创建自定义任务: {task_type} for {username}")
            return True
            
        except Exception as e:
            logger.exception(f"创建自定义任务时出错: {e}")
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='任务初始化脚本')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 初始化用户应用任务
    parser_user_apps = subparsers.add_parser('init-user-apps', help='初始化用户应用同步任务')
    parser_user_apps.add_argument('--force', action='store_true', help='强制重新创建任务')
    
    # 初始化应用数据任务
    parser_app_data = subparsers.add_parser('init-app-data', help='初始化应用数据同步任务')
    parser_app_data.add_argument('--days', type=int, default=1, help='同步天数（默认1天）')
    parser_app_data.add_argument('--force', action='store_true', help='强制重新创建任务')
    
    # 重置失败任务
    parser_reset = subparsers.add_parser('reset-failed', help='重置失败的任务')
    parser_reset.add_argument('--task-type', help='任务类型（可选）')
    
    # 恢复超时任务
    parser_recover = subparsers.add_parser('recover-timeout', help='恢复超时的任务')
    parser_recover.add_argument('--hours', type=int, default=2, help='超时小时数（默认2小时）')
    
    # 清理过期任务
    parser_clean = subparsers.add_parser('clean-old', help='清理过期任务')
    parser_clean.add_argument('--days', type=int, default=30, help='保留天数（默认30天）')
    parser_clean.add_argument('--status', nargs='+', default=['done', 'failed'], 
                             help='要清理的状态（默认：done failed）')
    
    # 任务统计
    parser_stats = subparsers.add_parser('stats', help='显示任务统计信息')
    
    # 创建自定义任务
    parser_custom = subparsers.add_parser('create-task', help='创建自定义任务')
    parser_custom.add_argument('--task-type', required=True, help='任务类型')
    parser_custom.add_argument('--username', required=True, help='用户名')
    parser_custom.add_argument('--app-id', help='应用ID')
    parser_custom.add_argument('--start-date', help='开始日期（YYYY-MM-DD）')
    parser_custom.add_argument('--end-date', help='结束日期（YYYY-MM-DD）')
    parser_custom.add_argument('--priority', type=int, default=0, help='优先级（默认0）')
    
    # 全部初始化
    parser_init_all = subparsers.add_parser('init-all', help='初始化所有任务')
    parser_init_all.add_argument('--days', type=int, default=1, help='应用数据同步天数（默认1天）')
    parser_init_all.add_argument('--force', action='store_true', help='强制重新创建任务')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 创建初始化器
    initializer = TaskInitializer()
    
    try:
        if args.command == 'init-user-apps':
            count = initializer.init_user_apps_tasks(force=args.force)
            print(f"✅ 成功创建 {count} 个用户应用同步任务")
            
        elif args.command == 'init-app-data':
            count = initializer.init_app_data_tasks(days=args.days, force=args.force)
            print(f"✅ 成功创建 {count} 个应用数据同步任务")
            
        elif args.command == 'reset-failed':
            count = initializer.reset_failed_tasks(task_type=args.task_type)
            print(f"✅ 成功重置 {count} 个失败任务")
            
        elif args.command == 'recover-timeout':
            count = initializer.recover_timeout_tasks(timeout_hours=args.hours)
            print(f"✅ 成功恢复 {count} 个超时任务")
            
        elif args.command == 'clean-old':
            count = initializer.clean_old_tasks(days=args.days, status_list=args.status)
            print(f"✅ 成功清理 {count} 个过期任务")
            
        elif args.command == 'stats':
            stats = initializer.get_task_stats()
            print("\n📊 任务统计信息:")
            print("\n按状态统计:")
            for status, count in stats.get('status_stats', {}).items():
                print(f"  {status}: {count}")
            
            print("\n按类型和状态统计:")
            for row in stats.get('type_stats', []):
                print(f"  {row['task_type']} - {row['status']}: {row['count']}")
            
            print("\n最近24小时统计:")
            for status, count in stats.get('recent_24h_stats', {}).items():
                print(f"  {status}: {count}")
                
        elif args.command == 'create-task':
            success = initializer.create_custom_task(
                task_type=args.task_type,
                username=args.username,
                app_id=args.app_id,
                start_date=args.start_date,
                end_date=args.end_date,
                priority=args.priority
            )
            if success:
                print("✅ 成功创建自定义任务")
            else:
                print("❌ 创建自定义任务失败")
                
        elif args.command == 'init-all':
            print("🚀 开始初始化所有任务...")
            
            # 初始化用户应用任务
            user_apps_count = initializer.init_user_apps_tasks(force=args.force)
            print(f"✅ 用户应用同步任务: {user_apps_count} 个")
            
            # 初始化应用数据任务
            app_data_count = initializer.init_app_data_tasks(days=args.days, force=args.force)
            print(f"✅ 应用数据同步任务: {app_data_count} 个")
            
            total_count = user_apps_count + app_data_count
            print(f"\n🎉 总共创建 {total_count} 个任务")
            
    except KeyboardInterrupt:
        print("\n❌ 操作被用户中断")
    except Exception as e:
        logger.exception(f"执行命令时出错: {e}")
        print(f"❌ 执行失败: {e}")


if __name__ == '__main__':
    main()