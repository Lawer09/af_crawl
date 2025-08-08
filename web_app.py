from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import uvicorn
from typing import List, Dict
from datetime import datetime, date
import os

from model.user import UserDAO
from model.user_app import UserAppDAO
from model.user_app_data import UserAppDataDAO
from model.crawl_task import CrawlTaskDAO
from tasks.sync_app_data import run as run_sync_task
from api.distribution_api import router as distribution_router, init_distribution_services
from config.distribution_config import get_distribution_config

app = FastAPI(title="App数据同步系统", description="用户APP数据同步管理界面")

# 包含分布式API路由
app.include_router(distribution_router)

# 创建必要的目录
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)

# 静态文件和模板
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """主页"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/users")
async def get_users():
    """获取用户列表"""
    users = UserDAO.get_enabled_users()
    return [{"email": u["email"], "account_type": u["account_type"]} for u in users]

@app.get("/api/users/{username}/apps")
async def get_user_apps(username: str):
    """获取用户的APP列表"""
    apps = UserAppDAO.get_user_apps(username)
    return [{
        "id": app["id"],
        "app_id": app["app_id"],
        "app_name": app["app_name"],
        "platform": app["platform"],
        "timezone": app["timezone"],
        "user_type_id": app["user_type_id"],
        "app_status": app["app_status"]
    } for app in apps]

@app.get("/api/users/{username}/apps/{app_id}/data")
async def get_app_data(username: str, app_id: str):
    """获取APP数据列表"""
    try:
        data = UserAppDataDAO.get_app_data(username, app_id)
        return [{
            "date": str(d["date"]),
            "installs": d["installs"],
            "sessions": d["sessions"],
            "revenue": float(d["revenue"]) if d["revenue"] else 0
        } for d in data]
    except Exception as e:
        return []

@app.get("/api/tasks")
async def get_tasks():
    """获取任务列表"""
    tasks = CrawlTaskDAO.fetch_pending('app_data', 100)
    return [{
        "id": task["id"],
        "task_type": task["task_type"],
        "username": task["username"],
        "app_id": task["app_id"],
        "start_date": str(task["start_date"]),
        "end_date": str(task["end_date"]),
        "status": task["status"],
        "retry": task["retry"],
        "created_at": str(task["created_at"])
    } for task in tasks]

@app.post("/api/sync-data")
async def create_sync_task(request: Dict):
    """创建数据同步任务"""
    try:
        username = request["username"]
        app_ids = request["app_ids"]
        start_date = request["start_date"]
        end_date = request["end_date"]
        
        tasks = []
        for app_id in app_ids:
            tasks.append({
                'task_type': 'app_data',
                'username': username,
                'app_id': app_id,
                'start_date': start_date,
                'end_date': end_date,
                'next_run_at': datetime.now().isoformat()
            })
        
        CrawlTaskDAO.add_tasks(tasks)
        return {"message": f"成功创建 {len(tasks)} 个任务", "task_count": len(tasks)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/tasks/run")
async def run_tasks():
    """手动触发任务执行"""
    try:
        run_sync_task(days=1)
        return {"message": "任务执行已启动"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def startup_event():
    """应用启动时初始化分布式服务"""
    try:
        # 获取分布式配置
        config = get_distribution_config()
        
        # 初始化分布式服务（默认为standalone模式）
        init_distribution_services(mode="standalone", device_id=config.device_id)
        
        print(f"Distribution services initialized in {config.mode.value} mode")
        
    except Exception as e:
        print(f"Warning: Failed to initialize distribution services: {e}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)