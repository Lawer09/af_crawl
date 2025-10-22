from __future__ import annotations

import logging
from typing import Dict, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel

from model.device import DeviceDAO
from model.task import TaskDAO
from model.task_assignment import TaskAssignmentDAO
from model.device_heartbeat import DeviceHeartbeatDAO
from services import af_config_service
from services.task_dispatcher import TaskDispatcher, LoadBalanceStrategy
from services.device_manager import DeviceManager
from services.task_scheduler import TaskScheduler, SchedulerMode
from services.data_service import fetch_by_pid_and_offer_id,fetch_with_overall_report_counts, sync_all_user_app_data_latest_to_af_data
from services.app_service import fetch_app_by_pid

logger = logging.getLogger(__name__)

# 创建API路由器
router = APIRouter(prefix="/api/distribution", tags=["distribution"])

# 全局变量存储服务实例
_task_scheduler: Optional[TaskScheduler] = None
_device_manager: Optional[DeviceManager] = None


# Pydantic模型
class DeviceRegistration(BaseModel):
    device_id: str
    device_name: str
    device_type: str = "worker"
    ip_address: Optional[str] = None
    capabilities: Optional[Dict] = None


class TaskRequest(BaseModel):
    task_type: str
    username: str
    app_id: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    priority: int = 0
    task_data: Optional[Dict] = None
    execution_timeout: int = 3600
    max_retry_count: int = 3


class HeartbeatData(BaseModel):
    device_id: str
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    disk_usage: Optional[float] = None
    network_status: str = "good"
    running_tasks: int = 0
    system_load: Optional[float] = None
    error_count: int = 0
    status_info: Optional[Dict] = None


class TaskAssignment(BaseModel):
    task_id: int
    device_id: str


class TaskStatusUpdate(BaseModel):
    task_id: int
    device_id: str
    status: str
    error_message: Optional[str] = None
    result_data: Optional[Dict] = None


# pid的prt认证添加
@router.get("/user/auth/prt")
def set_pid_auth_prt(
    pid: str = Query(..., description="用户PID"),
    prt: str = Query(..., description="用户PRT（用于认证）"),
):
    """添加pid的prt认证"""
    try:
        # 调用数据服务添加认证
        prt_list = af_config_service.prt_auth(pid, prt)
        return {"status": "success", "data": prt_list}
    except Exception as e:
        logger.error(f"Error adding prt auth for pid {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 通过 pid 查询用户账号并获取数据（GET）
@router.get("/user/app/data")
def get_user_app_data_by_pid(
    pid: str = Query(..., description="用户PID（存储于af_user.email，当account_type='pid')"),
    app_id: str = Query(..., description="应用ID"),
    aff_id: str = Query(..., description="aff ID"),
    offer_id: Optional[str] = Query(None, description="offer ID（可选）"),
    date: str = Query(..., description="日期，YYYY-MM-DD"),
):
    """通过 pid 获取账号信息后拉取用户 app 数据"""
    try:
        rows = fetch_by_pid_and_offer_id(
            pid=pid,
            app_id=app_id,
            aff_id=aff_id,
            offer_id=offer_id,
            start_date=date,
            end_date=date,
        )
        
        return {"status": "success", "data": rows}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching user app data by pid: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/user/app/data/gap")
def get_user_app_data_gap_by_pid(
    pid: str = Query(..., description="用户PID"),
    app_id: str = Query(..., description="应用ID"),
    aff_id: Optional[str] = Query(None, description="aff ID（可选）"),
    offer_id: Optional[str] = Query(None, description="offer ID（可选）"),
    date: str = Query(..., description="日期，YYYY-MM-DD")
):
    """通过 pid 获取账号信息后拉取指定日期的用户 app 数据及gap。"""
    try:
        rows = fetch_with_overall_report_counts(
            pid=pid,
            app_id=app_id,
            aff_id=aff_id,
            offer_id=offer_id,
            date=date,
        )
        
        return {"status": "success", "data": rows}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching user app data by pid: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/user/app")
def get_user_app_by_pid(
    pid: str = Query(..., description="用户PID"),
):
    """通过 pid 获取用户 app 列表"""
    try:
        apps = fetch_app_by_pid(pid)
        return {"status": "success", "data": apps}
    except Exception as e:
        logger.exception(f"Error fetching user app data by pid: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 配置 af pb
@router.get("/user/auth/pb")
def set_af_pb_config(
    username: str = Query(..., description="用户名"),
    password: str = Query(..., description="密码"),
    pid: str = Query(..., description="用户PID")
):
    """通过 pid 配置 af pb 认证"""
    try:    
        rows = af_config_service.set_pb_config(
            pid=pid,
            username=username,
            password=password,
        )
        return {"status": "success", "data": rows}

    except HTTPException:
        raise
    
    except Exception as e:
        logger.exception(f"Error setting pb config for pid {pid}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 设备管理接口
@router.post("/devices/register")
async def register_device(device: DeviceRegistration):
    """注册设备"""
    try:
        # 构造设备信息字典
        device_info = {
            'device_id': device.device_id,
            'device_name': device.device_name,
            'device_type': device.device_type,
            'ip_address': device.ip_address or '0.0.0.0',
            'port': 0,  # 默认端口
            'capabilities': device.capabilities or {},
            'max_concurrent_tasks': 5  # 默认并发任务数
        }
        
        success = DeviceDAO.register_device(device_info)
        
        if success:
            return {"status": "success", "message": "Device registered successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to register device")
            
    except Exception as e:
        logger.exception(f"Error registering device: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/devices")
async def get_devices(status: Optional[str] = Query(None)):
    """获取设备列表"""
    try:
        if status:
            devices = DeviceDAO.get_devices_by_status(status)
        else:
            devices = DeviceDAO.get_all_devices()
        
        return {"devices": devices}
        
    except Exception as e:
        logger.exception(f"Error getting devices: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/devices/{device_id}")
async def get_device(device_id: str):
    """获取设备详情"""
    try:
        device = DeviceDAO.get_device(device_id)
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")
        
        # 获取设备最新心跳
        latest_heartbeat = DeviceHeartbeatDAO.get_latest_heartbeat(device_id)
        
        # 获取设备任务
        device_tasks = TaskDAO.get_device_tasks(device_id)
        
        # 获取设备统计
        assignment_stats = TaskAssignmentDAO.get_device_assignment_stats(device_id)
        health_stats = DeviceHeartbeatDAO.get_device_health_stats(device_id)
        
        return {
            "device": device,
            "latest_heartbeat": latest_heartbeat,
            "running_tasks": device_tasks,
            "assignment_stats": assignment_stats,
            "health_stats": health_stats
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting device {device_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/devices/{device_id}/heartbeat")
async def send_heartbeat(device_id: str, heartbeat: HeartbeatData):
    """发送设备心跳"""
    try:
        # 更新设备心跳时间
        DeviceDAO.update_heartbeat(device_id)
        
        # 记录心跳数据
        success = DeviceHeartbeatDAO.record_heartbeat(
            device_id=device_id,
            cpu_usage=heartbeat.cpu_usage,
            memory_usage=heartbeat.memory_usage,
            disk_usage=heartbeat.disk_usage,
            network_status=heartbeat.network_status,
            running_tasks=heartbeat.running_tasks,
            system_load=heartbeat.system_load,
            error_count=heartbeat.error_count,
            status_info=heartbeat.status_info
        )
        
        if success:
            return {"status": "success", "message": "Heartbeat recorded"}
        else:
            raise HTTPException(status_code=400, detail="Failed to record heartbeat")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error recording heartbeat for device {device_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/devices/{device_id}/status")
async def update_device_status(device_id: str, status: str = Body(..., embed=True)):
    """更新设备状态"""
    try:
        success = DeviceDAO.update_device_status(device_id, status)
        
        if success:
            return {"status": "success", "message": "Device status updated"}
        else:
            raise HTTPException(status_code=400, detail="Failed to update device status")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error updating device status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 任务管理接口
@router.post("/tasks")
async def create_task(task: TaskRequest):
    """创建任务"""
    try:
        task_data = {
            'task_type': task.task_type,
            'username': task.username,
            'app_id': task.app_id,
            'start_date': task.start_date,
            'end_date': task.end_date,
            'priority': task.priority,
            'task_data': task.task_data,
            'execution_timeout': task.execution_timeout,
            'max_retry_count': task.max_retry_count,
            'next_run_at': datetime.now()
        }
        
        TaskDAO.add_tasks([task_data])
        
        return {"status": "success", "message": "Task created successfully"}
        
    except Exception as e:
        logger.exception(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks")
async def get_tasks(status: Optional[str] = Query(None), 
                   task_type: Optional[str] = Query(None),
                   device_id: Optional[str] = Query(None),
                   limit: int = Query(100, le=1000)):
    """获取任务列表"""
    try:
        if device_id:
            tasks = TaskDAO.get_device_tasks(device_id)
        elif task_type:
            if status == 'pending':
                tasks = TaskDAO.get_assignable_tasks(task_type, limit)
            else:
                tasks = TaskDAO.fetch_pending(task_type, limit)
        else:
            tasks = TaskDAO.get_assignable_tasks(limit=limit)
        
        return {"tasks": tasks}
        
    except Exception as e:
        logger.exception(f"Error getting tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}")
async def get_task(task_id: int):
    """获取任务详情"""
    try:
        # 获取任务分配记录
        assignments = TaskAssignmentDAO.get_task_assignments(task_id)
        
        if not assignments:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return {"task_id": task_id, "assignments": assignments}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/assign")
async def assign_task(assignment: TaskAssignment):
    """手动分配任务"""
    try:
        # 检查设备是否可用
        device = DeviceDAO.get_device(assignment.device_id)
        if not device or device['status'] != 'online':
            raise HTTPException(status_code=400, detail="Device is not available")
        
        # 分配任务
        success = TaskDAO.assign_task(assignment.task_id, assignment.device_id)
        
        if success:
            # 创建分配记录
            assignment_id = TaskAssignmentDAO.create_assignment(
                assignment.task_id, assignment.device_id
            )
            
            if assignment_id:
                DeviceDAO.increment_task_count(assignment.device_id)
                return {"status": "success", "assignment_id": assignment_id}
            else:
                raise HTTPException(status_code=400, detail="Failed to create assignment record")
        else:
            raise HTTPException(status_code=400, detail="Failed to assign task")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error assigning task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/status")
async def update_task_status(status_update: TaskStatusUpdate):
    """更新任务状态"""
    try:
        success = TaskAssignmentDAO.update_status_by_task_device(
            task_id=status_update.task_id,
            device_id=status_update.device_id,
            status=status_update.status,
            error_message=status_update.error_message,
            result_data=status_update.result_data
        )
        
        if success:
            # 更新任务表状态
            if status_update.status == 'completed':
                TaskDAO.mark_done(status_update.task_id)
                DeviceDAO.decrement_task_count(status_update.device_id)
            elif status_update.status == 'failed':
                TaskDAO.fail_task(status_update.task_id, 300)  # 5分钟后重试
                DeviceDAO.decrement_task_count(status_update.device_id)
            
            return {"status": "success", "message": "Task status updated"}
        else:
            raise HTTPException(status_code=400, detail="Failed to update task status")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error updating task status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{device_id}/pull")
async def pull_tasks(device_id: str, limit: int = Query(5, le=10)):
    """设备拉取任务"""
    try:
        # 检查设备是否在线
        device = DeviceDAO.get_device(device_id)
        if not device or device['status'] != 'online':
            raise HTTPException(status_code=400, detail="Device is not online")
        
        # 获取设备当前任务
        current_tasks = TaskDAO.get_device_tasks(device_id)
        available_slots = max(0, limit - len(current_tasks))
        
        if available_slots == 0:
            return {"tasks": []}
        
        # 获取分配给该设备的待执行任务
        assigned_tasks = [t for t in current_tasks if t['status'] == 'assigned']
        
        return {"tasks": assigned_tasks[:available_slots]}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error pulling tasks for device {device_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 统计和监控接口
@router.get("/stats/overview")
async def get_system_overview():
    """获取系统概览"""
    try:
        # 任务统计
        task_stats = TaskDAO.get_task_stats()
        
        # 设备统计
        all_devices = DeviceDAO.get_all_devices()
        online_devices = [d for d in all_devices if d['status'] == 'online']
        
        # 系统概览
        system_overview = DeviceHeartbeatDAO.get_system_overview()
        
        return {
            "task_stats": task_stats,
            "device_stats": {
                "total": len(all_devices),
                "online": len(online_devices),
                "offline": len(all_devices) - len(online_devices)
            },
            "system_overview": system_overview
        }
        
    except Exception as e:
        logger.exception(f"Error getting system overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/devices")
async def get_device_stats():
    """获取设备统计"""
    try:
        # 获取所有设备的最新心跳
        latest_heartbeats = DeviceHeartbeatDAO.get_all_latest_heartbeats()
        
        return {"device_heartbeats": latest_heartbeats}
        
    except Exception as e:
        logger.exception(f"Error getting device stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/performance/{device_id}")
async def get_device_performance(device_id: str, hours: int = Query(24, le=168)):
    """获取设备性能统计"""
    try:
        # 获取设备心跳历史
        heartbeats = DeviceHeartbeatDAO.get_device_heartbeats(device_id, hours)
        
        # 获取设备健康统计
        health_stats = DeviceHeartbeatDAO.get_device_health_stats(device_id, hours)
        
        # 获取设备分配统计
        assignment_stats = TaskAssignmentDAO.get_device_assignment_stats(device_id, hours // 24)
        
        return {
            "device_id": device_id,
            "period_hours": hours,
            "heartbeats": heartbeats,
            "health_stats": health_stats,
            "assignment_stats": assignment_stats
        }
        
    except Exception as e:
        logger.exception(f"Error getting device performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 管理接口
@router.post("/management/rebalance")
async def rebalance_tasks():
    """重新平衡任务"""
    try:
        global _task_scheduler
        
        if _task_scheduler:
            result = _task_scheduler.rebalance_tasks()
            return result
        else:
            raise HTTPException(status_code=503, detail="Task scheduler not available")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error rebalancing tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/management/cleanup")
async def cleanup_old_data(days: int = Body(7, embed=True)):
    """清理旧数据"""
    try:
        # 清理心跳记录
        deleted_heartbeats = DeviceHeartbeatDAO.cleanup_old_heartbeats(days)
        
        # 清理分配记录
        deleted_assignments = TaskAssignmentDAO.cleanup_old_assignments(days * 4)  # 保留更长时间
        
        return {
            "status": "success",
            "deleted_heartbeats": deleted_heartbeats,
            "deleted_assignments": deleted_assignments
        }
        
    except Exception as e:
        logger.exception(f"Error cleaning up old data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/management/scheduler/status")
async def get_scheduler_status():
    """获取调度器状态"""
    try:
        global _task_scheduler
        
        if _task_scheduler:
            stats = _task_scheduler.get_scheduler_stats()
            return stats
        else:
            return {"status": "not_running"}
            
    except Exception as e:
        logger.exception(f"Error getting scheduler status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _register_task_executors(scheduler: TaskScheduler):
    """注册任务执行器"""
    try:
        # 导入全局任务执行器
        from executors.task_executor import get_task_executor
        
        # 获取全局分布式任务执行器
        task_executor = get_task_executor()
        
        # 封装全局任务执行，接受task参数
        def execute_user_apps_task(task):
            """执行用户应用同步任务"""
            return task_executor.execute_task_sync(
                task_id=task['id'],
                task_type=task['task_type'],
                task_data={
                    'username': task['username'],
                    'app_id': task.get('app_id'),
                    'start_date': task.get('start_date'),
                    'end_date': task.get('end_date'),
                    'task_data': task.get('task_data')
                }
            )
        
        def execute_data_sync_task(task):
            """执行数据同步任务"""
            return task_executor.execute_task_sync(
                task_id=task['id'],
                task_type=task['task_type'],
                task_data={
                    'username': task['username'],
                    'app_id': task.get('app_id'),
                    'start_date': task.get('start_date'),
                    'end_date': task.get('end_date'),
                    'task_data': task.get('task_data')
                }
            )
        
        # 注册执行器
        scheduler.register_task_executor('user_apps', execute_user_apps_task)
        scheduler.register_task_executor('app_data', execute_data_sync_task)  # 添加app_data任务类型支持
        
        logger.info("Task executors registered successfully")
        
    except Exception as e:
        logger.exception(f"Error registering task executors: {e}")
        raise


# 初始化函数
def init_distribution_services(mode: str = "standalone", device_id: Optional[str] = None):
    """初始化分布式服务"""

     # api中的全局任务和设备管理，实际上会调executors全局任务执行器注册的方法
    global _task_scheduler, _device_manager
    
    try:
        # 确定调度器模式
        if mode.lower() == "master":
            scheduler_mode = SchedulerMode.MASTER
        elif mode.lower() == "worker":
            scheduler_mode = SchedulerMode.WORKER
        else:
            scheduler_mode = SchedulerMode.STANDALONE
        
        # 初始化任务调度器
        _task_scheduler = TaskScheduler(mode=scheduler_mode)
        
        # 注册任务执行器
        _register_task_executors(_task_scheduler)
        
        # 初始化设备管理器
        if device_id:
            _device_manager = DeviceManager(device_id=device_id)
        else:
            _device_manager = DeviceManager()
        
        logger.info(f"Distribution services initialized in {mode} mode")
        
    except Exception as e:
        logger.exception(f"Error initializing distribution services: {e}")
        raise


def start_distribution_services():
    """启动分布式服务"""

    # api中的全局任务和设备管理
    global _task_scheduler, _device_manager
    
    try:
        if _device_manager:
            _device_manager.start()
        
        if _task_scheduler:
            _task_scheduler.start()
        
        logger.info("Distribution services started")
        
    except Exception as e:
        logger.exception(f"Error starting distribution services: {e}")
        raise


def stop_distribution_services():
    """停止分布式服务"""

    global _task_scheduler, _device_manager
    
    try:
        if _task_scheduler:
            _task_scheduler.stop()
        
        if _device_manager:
            _device_manager.stop()
        
        logger.info("Distribution services stopped")
        
    except Exception as e:
        logger.exception(f"Error stopping distribution services: {e}")


@router.post("/sync-all-latest")
def sync_all_af_data_latest():
    """同步 af_user_app_data 中最新的 days=1 数据到 af_data（全量）。"""
    try:
        updated = sync_all_user_app_data_latest_to_af_data()
        return {"status": "success", "updated": updated}
    except Exception as e:
        logger.exception(f"Error syncing latest user_app_data to af_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))