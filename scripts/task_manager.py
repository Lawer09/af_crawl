#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务管理工具

提供简单易用的任务管理界面，包括：
1. 任务状态查看
2. 快速任务操作
3. 任务监控
4. 批量操作

使用方法：
    python scripts/task_manager.py
"""

import sys
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.crawl_task import CrawlTaskDAO
from model.user import UserDAO
from model.user_app import UserAppDAO
from scripts.init_tasks import TaskInitializer


class TaskManager:
    """任务管理器"""
    
    def __init__(self):
        self.initializer = TaskInitializer()
        
    def show_dashboard(self):
        """显示任务仪表板"""
        print("\n" + "="*60)
        print("🚀 AF爬虫任务管理系统")
        print("="*60)
        
        # 获取统计信息
        stats = self.initializer.get_task_stats()
        status_stats = stats.get('status_stats', {})
        
        # 显示总体统计
        total = sum(status_stats.values())
        print(f"\n📊 任务总览 (总计: {total} 个任务)")
        print("-" * 40)
        
        status_icons = {
            'pending': '⏳',
            'assigned': '📋',
            'running': '🔄',
            'done': '✅',
            'failed': '❌'
        }
        
        for status in ['pending', 'assigned', 'running', 'done', 'failed']:
            count = status_stats.get(status, 0)
            icon = status_icons.get(status, '📄')
            percentage = (count / total * 100) if total > 0 else 0
            print(f"{icon} {status.upper():<8}: {count:>6} ({percentage:5.1f}%)")
        
        # 显示最近24小时统计
        recent_stats = stats.get('recent_24h_stats', {})
        recent_total = sum(recent_stats.values())
        if recent_total > 0:
            print(f"\n📈 最近24小时 (新增: {recent_total} 个任务)")
            print("-" * 40)
            for status, count in recent_stats.items():
                icon = status_icons.get(status, '📄')
                print(f"{icon} {status.upper():<8}: {count:>6}")
        
        # 显示按类型统计
        type_stats = stats.get('type_stats', [])
        if type_stats:
            print("\n📋 按任务类型统计")
            print("-" * 40)
            type_summary = {}
            for row in type_stats:
                task_type = row['task_type']
                if task_type not in type_summary:
                    type_summary[task_type] = {'total': 0, 'done': 0, 'failed': 0, 'pending': 0}
                type_summary[task_type]['total'] += row['count']
                type_summary[task_type][row['status']] = row['count']
            
            for task_type, summary in type_summary.items():
                total_type = summary['total']
                done = summary.get('done', 0)
                failed = summary.get('failed', 0)
                pending = summary.get('pending', 0)
                success_rate = (done / total_type * 100) if total_type > 0 else 0
                print(f"📦 {task_type:<12}: {total_type:>4} 总计 | {done:>3} 完成 | {failed:>3} 失败 | {pending:>3} 待处理 | {success_rate:5.1f}% 成功率")
    
    def show_menu(self):
        """显示主菜单"""
        print("\n" + "="*60)
        print("🛠️  操作菜单")
        print("="*60)
        print("1️⃣  初始化任务")
        print("2️⃣  任务维护")
        print("3️⃣  任务监控")
        print("4️⃣  批量操作")
        print("5️⃣  自定义任务")
        print("6️⃣  系统信息")
        print("0️⃣  退出系统")
        print("-" * 60)
    
    def show_init_menu(self):
        """显示初始化菜单"""
        print("\n🚀 任务初始化")
        print("-" * 30)
        print("1. 初始化用户应用同步任务")
        print("2. 初始化应用数据同步任务")
        print("3. 初始化所有任务")
        print("0. 返回主菜单")
        
        choice = input("\n请选择操作: ").strip()
        
        if choice == '1':
            force = input("是否强制重新创建? (y/N): ").strip().lower() == 'y'
            print("\n⏳ 正在初始化用户应用同步任务...")
            count = self.initializer.init_user_apps_tasks(force=force)
            print(f"✅ 成功创建 {count} 个用户应用同步任务")
            
        elif choice == '2':
            days = input("请输入同步天数 (默认1): ").strip()
            days = int(days) if days.isdigit() else 1
            force = input("是否强制重新创建? (y/N): ").strip().lower() == 'y'
            print(f"\n⏳ 正在初始化应用数据同步任务({days}天)...")
            count = self.initializer.init_app_data_tasks(days=days, force=force)
            print(f"✅ 成功创建 {count} 个应用数据同步任务")
            
        elif choice == '3':
            days = input("请输入应用数据同步天数 (默认1): ").strip()
            days = int(days) if days.isdigit() else 1
            force = input("是否强制重新创建? (y/N): ").strip().lower() == 'y'
            print("\n🚀 正在初始化所有任务...")
            
            user_apps_count = self.initializer.init_user_apps_tasks(force=force)
            app_data_count = self.initializer.init_app_data_tasks(days=days, force=force)
            
            total_count = user_apps_count + app_data_count
            print(f"✅ 用户应用同步任务: {user_apps_count} 个")
            print(f"✅ 应用数据同步任务: {app_data_count} 个")
            print(f"🎉 总共创建 {total_count} 个任务")
    
    def show_maintenance_menu(self):
        """显示维护菜单"""
        print("\n🔧 任务维护")
        print("-" * 30)
        print("1. 重置失败任务")
        print("2. 恢复超时任务")
        print("3. 清理过期任务")
        print("4. 重置所有任务")
        print("0. 返回主菜单")
        
        choice = input("\n请选择操作: ").strip()
        
        if choice == '1':
            task_type = input("请输入任务类型 (留空表示所有类型): ").strip() or None
            print("\n⏳ 正在重置失败任务...")
            count = self.initializer.reset_failed_tasks(task_type=task_type)
            print(f"✅ 成功重置 {count} 个失败任务")
            
        elif choice == '2':
            hours = input("请输入超时小时数 (默认2): ").strip()
            hours = int(hours) if hours.isdigit() else 2
            print(f"\n⏳ 正在恢复超时任务({hours}小时)...")
            count = self.initializer.recover_timeout_tasks(timeout_hours=hours)
            print(f"✅ 成功恢复 {count} 个超时任务")
            
        elif choice == '3':
            days = input("请输入保留天数 (默认30): ").strip()
            days = int(days) if days.isdigit() else 30
            confirm = input(f"确认清理 {days} 天前的已完成和失败任务? (y/N): ").strip().lower()
            if confirm == 'y':
                print(f"\n⏳ 正在清理 {days} 天前的过期任务...")
                count = self.initializer.clean_old_tasks(days=days)
                print(f"✅ 成功清理 {count} 个过期任务")
            else:
                print("❌ 操作已取消")
                
        elif choice == '4':
            confirm = input("⚠️  确认重置所有任务? 这将删除所有任务数据! (yes/N): ").strip()
            if confirm == 'yes':
                print("\n⏳ 正在重置所有任务...")
                CrawlTaskDAO.reset_all()
                print("✅ 所有任务已重置")
            else:
                print("❌ 操作已取消")
    
    def show_monitoring_menu(self):
        """显示监控菜单"""
        print("\n📊 任务监控")
        print("-" * 30)
        print("1. 实时监控")
        print("2. 查看运行中任务")
        print("3. 查看失败任务")
        print("4. 查看超时任务")
        print("0. 返回主菜单")
        
        choice = input("\n请选择操作: ").strip()
        
        if choice == '1':
            self.real_time_monitor()
        elif choice == '2':
            self.show_running_tasks()
        elif choice == '3':
            self.show_failed_tasks()
        elif choice == '4':
            self.show_timeout_tasks()
    
    def real_time_monitor(self):
        """实时监控"""
        print("\n📡 实时任务监控 (按 Ctrl+C 退出)")
        print("=" * 50)
        
        try:
            while True:
                # 清屏（Windows和Unix兼容）
                os.system('cls' if os.name == 'nt' else 'clear')
                
                print(f"📡 实时任务监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)
                
                stats = self.initializer.get_task_stats()
                status_stats = stats.get('status_stats', {})
                
                # 显示实时状态
                for status in ['pending', 'assigned', 'running', 'done', 'failed']:
                    count = status_stats.get(status, 0)
                    print(f"{status.upper():<8}: {count:>6}")
                
                print("\n按 Ctrl+C 退出监控")
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n✅ 监控已停止")
    
    def show_running_tasks(self):
        """显示运行中任务"""
        try:
            from core.db import mysql_pool
            sql = f"""
            SELECT id, task_type, username, app_id, assigned_device_id, assigned_at
            FROM {CrawlTaskDAO.TABLE}
            WHERE status = 'running'
            ORDER BY assigned_at DESC
            LIMIT 20
            """
            tasks = mysql_pool.select(sql)
            
            print("\n🔄 运行中的任务")
            print("-" * 80)
            if tasks:
                print(f"{'ID':<6} {'类型':<12} {'用户':<20} {'应用ID':<12} {'设备ID':<15} {'开始时间':<20}")
                print("-" * 80)
                for task in tasks:
                    assigned_at = task['assigned_at'].strftime('%m-%d %H:%M:%S') if task['assigned_at'] else 'N/A'
                    print(f"{task['id']:<6} {task['task_type']:<12} {task['username']:<20} {task['app_id'] or 'N/A':<12} {task['assigned_device_id'] or 'N/A':<15} {assigned_at:<20}")
            else:
                print("暂无运行中的任务")
                
        except Exception as e:
            print(f"❌ 获取运行中任务失败: {e}")
    
    def show_failed_tasks(self):
        """显示失败任务"""
        try:
            from core.db import mysql_pool
            sql = f"""
            SELECT id, task_type, username, app_id, retry, updated_at
            FROM {CrawlTaskDAO.TABLE}
            WHERE status = 'failed'
            ORDER BY updated_at DESC
            LIMIT 20
            """
            tasks = mysql_pool.select(sql)
            
            print("\n❌ 失败的任务")
            print("-" * 80)
            if tasks:
                print(f"{'ID':<6} {'类型':<12} {'用户':<20} {'应用ID':<12} {'重试次数':<8} {'失败时间':<20}")
                print("-" * 80)
                for task in tasks:
                    updated_at = task['updated_at'].strftime('%m-%d %H:%M:%S') if task['updated_at'] else 'N/A'
                    print(f"{task['id']:<6} {task['task_type']:<12} {task['username']:<20} {task['app_id'] or 'N/A':<12} {task['retry']:<8} {updated_at:<20}")
            else:
                print("暂无失败的任务")
                
        except Exception as e:
            print(f"❌ 获取失败任务失败: {e}")
    
    def show_timeout_tasks(self):
        """显示超时任务"""
        try:
            timeout_tasks = CrawlTaskDAO.get_timeout_tasks(timeout_minutes=120)  # 2小时超时
            
            print("\n⏰ 超时的任务")
            print("-" * 80)
            if timeout_tasks:
                print(f"{'ID':<6} {'类型':<12} {'用户':<20} {'应用ID':<12} {'设备ID':<15} {'分配时间':<20}")
                print("-" * 80)
                for task in timeout_tasks:
                    assigned_at = task['assigned_at'].strftime('%m-%d %H:%M:%S') if task['assigned_at'] else 'N/A'
                    print(f"{task['id']:<6} {task['task_type']:<12} {task['username']:<20} {task['app_id'] or 'N/A':<12} {task['assigned_device_id'] or 'N/A':<15} {assigned_at:<20}")
            else:
                print("暂无超时的任务")
                
        except Exception as e:
            print(f"❌ 获取超时任务失败: {e}")
    
    def show_batch_menu(self):
        """显示批量操作菜单"""
        print("\n📦 批量操作")
        print("-" * 30)
        print("1. 批量创建用户应用任务")
        print("2. 批量创建应用数据任务")
        print("3. 批量重置指定用户任务")
        print("4. 批量删除指定类型任务")
        print("0. 返回主菜单")
        
        choice = input("\n请选择操作: ").strip()
        
        if choice == '1':
            usernames = input("请输入用户名列表 (用逗号分隔): ").strip().split(',')
            usernames = [u.strip() for u in usernames if u.strip()]
            if usernames:
                self.batch_create_user_app_tasks(usernames)
            else:
                print("❌ 用户名列表为空")
                
        elif choice == '2':
            username = input("请输入用户名: ").strip()
            app_ids = input("请输入应用ID列表 (用逗号分隔): ").strip().split(',')
            app_ids = [a.strip() for a in app_ids if a.strip()]
            days = input("请输入同步天数 (默认1): ").strip()
            days = int(days) if days.isdigit() else 1
            
            if username and app_ids:
                self.batch_create_app_data_tasks(username, app_ids, days)
            else:
                print("❌ 参数不完整")
                
        elif choice == '3':
            username = input("请输入用户名: ").strip()
            if username:
                self.batch_reset_user_tasks(username)
            else:
                print("❌ 用户名为空")
                
        elif choice == '4':
            task_type = input("请输入任务类型: ").strip()
            if task_type:
                confirm = input(f"确认删除所有 {task_type} 类型的任务? (yes/N): ").strip()
                if confirm == 'yes':
                    self.batch_delete_tasks_by_type(task_type)
                else:
                    print("❌ 操作已取消")
            else:
                print("❌ 任务类型为空")
    
    def batch_create_user_app_tasks(self, usernames: List[str]):
        """批量创建用户应用任务"""
        try:
            tasks = []
            for username in usernames:
                task = {
                    'task_type': 'user_apps',
                    'username': username,
                    'next_run_at': datetime.now().isoformat(),
                    'priority': 1
                }
                tasks.append(task)
            
            if tasks:
                CrawlTaskDAO.add_tasks(tasks)
                print(f"✅ 成功创建 {len(tasks)} 个用户应用任务")
            
        except Exception as e:
            print(f"❌ 批量创建用户应用任务失败: {e}")
    
    def batch_create_app_data_tasks(self, username: str, app_ids: List[str], days: int):
        """批量创建应用数据任务"""
        try:
            tasks = []
            from datetime import date, timedelta
            
            for app_id in app_ids:
                for i in range(days):
                    d = date.today() - timedelta(days=i + 1)
                    task = {
                        'task_type': 'app_data',
                        'username': username,
                        'app_id': app_id,
                        'start_date': d.isoformat(),
                        'end_date': d.isoformat(),
                        'next_run_at': datetime.now().isoformat(),
                        'priority': 0
                    }
                    tasks.append(task)
            
            if tasks:
                CrawlTaskDAO.add_tasks(tasks)
                print(f"✅ 成功创建 {len(tasks)} 个应用数据任务")
            
        except Exception as e:
            print(f"❌ 批量创建应用数据任务失败: {e}")
    
    def batch_reset_user_tasks(self, username: str):
        """批量重置用户任务"""
        try:
            from core.db import mysql_pool
            sql = f"""
            UPDATE {CrawlTaskDAO.TABLE} 
            SET status='pending', retry=0, assigned_device_id=NULL, assigned_at=NULL, 
                next_run_at=NOW(), updated_at=NOW()
            WHERE username=%s AND status IN ('failed', 'running', 'assigned')
            """
            result = mysql_pool.execute(sql, (username,))
            print(f"✅ 成功重置用户 {username} 的 {result} 个任务")
            
        except Exception as e:
            print(f"❌ 批量重置用户任务失败: {e}")
    
    def batch_delete_tasks_by_type(self, task_type: str):
        """批量删除指定类型任务"""
        try:
            from core.db import mysql_pool
            sql = f"DELETE FROM {CrawlTaskDAO.TABLE} WHERE task_type=%s"
            result = mysql_pool.execute(sql, (task_type,))
            print(f"✅ 成功删除 {result} 个 {task_type} 类型的任务")
            
        except Exception as e:
            print(f"❌ 批量删除任务失败: {e}")
    
    def show_custom_task_menu(self):
        """显示自定义任务菜单"""
        print("\n🎯 自定义任务")
        print("-" * 30)
        print("1. 创建单个任务")
        print("2. 查看任务详情")
        print("3. 修改任务优先级")
        print("0. 返回主菜单")
        
        choice = input("\n请选择操作: ").strip()
        
        if choice == '1':
            self.create_custom_task()
        elif choice == '2':
            task_id = input("请输入任务ID: ").strip()
            if task_id.isdigit():
                self.show_task_detail(int(task_id))
            else:
                print("❌ 无效的任务ID")
        elif choice == '3':
            task_id = input("请输入任务ID: ").strip()
            priority = input("请输入新的优先级: ").strip()
            if task_id.isdigit() and priority.isdigit():
                self.update_task_priority(int(task_id), int(priority))
            else:
                print("❌ 无效的参数")
    
    def create_custom_task(self):
        """创建自定义任务"""
        print("\n📝 创建自定义任务")
        print("-" * 30)
        
        task_type = input("任务类型 (user_apps/app_data): ").strip()
        username = input("用户名: ").strip()
        app_id = input("应用ID (可选): ").strip() or None
        start_date = input("开始日期 (YYYY-MM-DD, 可选): ").strip() or None
        end_date = input("结束日期 (YYYY-MM-DD, 可选): ").strip() or None
        priority = input("优先级 (默认0): ").strip()
        priority = int(priority) if priority.isdigit() else 0
        
        if task_type and username:
            success = self.initializer.create_custom_task(
                task_type=task_type,
                username=username,
                app_id=app_id,
                start_date=start_date,
                end_date=end_date,
                priority=priority
            )
            if success:
                print("✅ 自定义任务创建成功")
            else:
                print("❌ 自定义任务创建失败")
        else:
            print("❌ 任务类型和用户名不能为空")
    
    def show_task_detail(self, task_id: int):
        """显示任务详情"""
        try:
            from core.db import mysql_pool
            sql = f"SELECT * FROM {CrawlTaskDAO.TABLE} WHERE id=%s"
            tasks = mysql_pool.select(sql, (task_id,))
            
            if tasks:
                task = tasks[0]
                print(f"\n📋 任务详情 (ID: {task_id})")
                print("-" * 40)
                print(f"任务类型: {task['task_type']}")
                print(f"用户名: {task['username']}")
                print(f"应用ID: {task['app_id'] or 'N/A'}")
                print(f"开始日期: {task['start_date'] or 'N/A'}")
                print(f"结束日期: {task['end_date'] or 'N/A'}")
                print(f"状态: {task['status']}")
                print(f"优先级: {task['priority']}")
                print(f"重试次数: {task['retry']}")
                print(f"分配设备: {task['assigned_device_id'] or 'N/A'}")
                print(f"创建时间: {task['created_at']}")
                print(f"更新时间: {task['updated_at']}")
            else:
                print(f"❌ 未找到ID为 {task_id} 的任务")
                
        except Exception as e:
            print(f"❌ 获取任务详情失败: {e}")
    
    def update_task_priority(self, task_id: int, priority: int):
        """更新任务优先级"""
        try:
            success = CrawlTaskDAO.update_task_priority(task_id, priority)
            if success:
                print(f"✅ 任务 {task_id} 的优先级已更新为 {priority}")
            else:
                print(f"❌ 更新任务 {task_id} 的优先级失败")
                
        except Exception as e:
            print(f"❌ 更新任务优先级失败: {e}")
    
    def show_system_info(self):
        """显示系统信息"""
        print("\n💻 系统信息")
        print("-" * 40)
        
        try:
            # 用户统计
            users = UserDAO.get_enabled_users()
            print(f"启用用户数: {len(users)}")
            
            # 应用统计
            apps = UserAppDAO.get_all_active()
            print(f"活跃应用数: {len(apps)}")
            
            # 数据库连接状态
            from core.db import mysql_pool
            print(f"数据库连接: ✅ 正常")
            
            # 任务表状态
            sql = f"SHOW TABLE STATUS LIKE '{CrawlTaskDAO.TABLE}'"
            table_info = mysql_pool.select(sql)
            if table_info:
                info = table_info[0]
                print(f"任务表大小: {info.get('Data_length', 0) // 1024 // 1024} MB")
                print(f"任务表行数: {info.get('Rows', 0)}")
            
        except Exception as e:
            print(f"❌ 获取系统信息失败: {e}")
    
    def run(self):
        """运行主程序"""
        try:
            while True:
                self.show_dashboard()
                self.show_menu()
                
                choice = input("\n请选择操作 (0-6): ").strip()
                
                if choice == '0':
                    print("\n👋 感谢使用AF爬虫任务管理系统！")
                    break
                elif choice == '1':
                    self.show_init_menu()
                elif choice == '2':
                    self.show_maintenance_menu()
                elif choice == '3':
                    self.show_monitoring_menu()
                elif choice == '4':
                    self.show_batch_menu()
                elif choice == '5':
                    self.show_custom_task_menu()
                elif choice == '6':
                    self.show_system_info()
                else:
                    print("❌ 无效选择，请重新输入")
                
                if choice != '0':
                    input("\n按回车键继续...")
                    
        except KeyboardInterrupt:
            print("\n\n👋 程序已退出")
        except Exception as e:
            print(f"\n❌ 程序运行出错: {e}")


def main():
    """主函数"""
    manager = TaskManager()
    manager.run()


if __name__ == '__main__':
    main()