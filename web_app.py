from fastapi import FastAPI
import uvicorn

from api.distribution_api import router as distribution_router, init_distribution_services
from config.distribution_config import get_distribution_config

app = FastAPI(title="分布式任务系统API", description="分布式爬虫任务管理API")

# 包含分布式API路由
app.include_router(distribution_router)

if __name__ == "__main__":
    config = get_distribution_config()
    uvicorn.run(app, host=config.master_host, port=config.master_port)