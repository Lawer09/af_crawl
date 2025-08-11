#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from model.crawl_task import CrawlTaskDAO
from model.device import DeviceDAO
from model.task_assignment import TaskAssignmentDAO

def check_task_status():
    print("=== 任务状态检查 ===")
    
    # 检查待分配任务
    pending_tasks = CrawlTaskDAO.get_assignable_tasks(limit=10)
    print(f"待分配任务数量: {len(pending_tasks)}")
    
    if pending_tasks:
        print("\n前5个待分配任务:")
        for task in pending_tasks[:5]:
            print(f"  任务ID: {task['id']}, 类型: {task['task_type']}, 状态: {task['status']}, 用户: {task['username']}")
    
    # 检查所有任务状态分布
    print("\n=== 任务状态分布 ===")
    from core.db import mysql_pool
    
    status_sql = "SELECT status, COUNT(*) as count FROM cl_crawl_tasks GROUP BY status"
    status_results = mysql_pool.select(status_sql)
    
    for result in status_results:
        print(f"  {result['status']}: {result['count']} 个任务")
    
    # 检查设备状态
    print("\n=== 设备状态 ===")
    devices = DeviceDAO.get_all_devices()
    print(f"设备总数: {len(devices)}")
    
    for device in devices:
        print(f"  设备ID: {device['device_id']}, 状态: {device['status']}, 当前任务: {device['current_tasks']}, 最大任务: {device['max_concurrent_tasks']}")
    
    # 检查任务分配记录
    print("\n=== 任务分配记录 ===")
    assignment_sql = "SELECT status, COUNT(*) as count FROM cl_task_assignment GROUP BY status"
    assignment_results = mysql_pool.select(assignment_sql)
    
    for result in assignment_results:
        print(f"  {result['status']}: {result['count']} 个分配记录")

if __name__ == "__main__":
    check_task_status()