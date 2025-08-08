#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""分布式系统初始化脚本"""

import logging
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from model.device import DeviceDAO
from model.task_assignment import TaskAssignmentDAO
from model.device_heartbeat import DeviceHeartbeatDAO
from model.crawl_task import CrawlTaskDAO
from config.distribution_config import DistributionConfig, DistributionMode
from core.logger import setup_logging

logger = logging.getLogger(__name__)


def init_database_tables():
    """初始化分布式系统相关的数据库表"""
    logger.info("Initializing distribution system database tables...")
    
    try:
        # 初始化设备表
        logger.info("Initializing device table...")
        DeviceDAO.init_table()
        
        # 初始化任务分配表
        logger.info("Initializing task assignment table...")
        TaskAssignmentDAO.init_table()
        
        # 初始化设备心跳表
        logger.info("Initializing device heartbeat table...")
        DeviceHeartbeatDAO.init_table()
        
        # 确保爬虫任务表已初始化并包含分布式字段
        logger.info("Initializing crawl task table...")
        CrawlTaskDAO.init_table()
        
        logger.info("All database tables initialized successfully")
        
    except Exception as e:
        logger.exception(f"Error initializing database tables: {e}")
        raise


def create_default_configs():
    """创建默认配置文件"""
    logger.info("Creating default configuration files...")
    
    config_dir = project_root / "config"
    config_dir.mkdir(exist_ok=True)
    
    try:
        # 创建主节点配置
        master_config = DistributionConfig.get_master_template()
        master_config_path = config_dir / "distribution_master.json"
        master_config.save_to_file(str(master_config_path))
        logger.info(f"Created master config: {master_config_path}")
        
        # 创建工作节点配置
        worker_config = DistributionConfig.get_worker_template()
        worker_config_path = config_dir / "distribution_worker.json"
        worker_config.save_to_file(str(worker_config_path))
        logger.info(f"Created worker config: {worker_config_path}")
        
        # 创建独立节点配置
        standalone_config = DistributionConfig.get_standalone_template()
        standalone_config_path = config_dir / "distribution_standalone.json"
        standalone_config.save_to_file(str(standalone_config_path))
        logger.info(f"Created standalone config: {standalone_config_path}")
        
        logger.info("Default configuration files created successfully")
        
    except Exception as e:
        logger.exception(f"Error creating configuration files: {e}")
        raise


def setup_environment():
    """设置环境变量"""
    logger.info("Setting up environment variables...")
    
    # 设置默认环境变量（如果不存在）
    env_vars = {
        "DISTRIBUTION_MODE": "standalone",
        "DISTRIBUTION_DEVICE_ID": "default-device",
        "DISTRIBUTION_DEVICE_NAME": "Default Device",
        "DISTRIBUTION_HOST": "0.0.0.0",
        "DISTRIBUTION_PORT": "8000",
        "DISTRIBUTION_MASTER_URL": "http://localhost:8000",
        "DISTRIBUTION_API_KEY": "your-api-key-here",
        "DISTRIBUTION_HEARTBEAT_INTERVAL": "30",
        "DISTRIBUTION_TASK_TIMEOUT": "3600",
        "DISTRIBUTION_MAX_RETRY": "3",
        "DISTRIBUTION_CONCURRENT_TASKS": "3"
    }
    
    for key, default_value in env_vars.items():
        if key not in os.environ:
            os.environ[key] = default_value
            logger.info(f"Set environment variable: {key}={default_value}")
        else:
            logger.info(f"Environment variable already set: {key}={os.environ[key]}")


def verify_installation():
    """验证安装是否成功"""
    logger.info("Verifying installation...")
    
    try:
        # 测试数据库连接
        from core.db import mysql_pool
        with mysql_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if result[0] != 1:
                raise Exception("Database connection test failed")
        logger.info("Database connection: OK")
        
        # 测试表是否存在
        tables_to_check = [
            DeviceDAO.TABLE,
            TaskAssignmentDAO.TABLE, 
            DeviceHeartbeatDAO.TABLE,
            CrawlTaskDAO.TABLE
        ]
        
        with mysql_pool.get_connection() as conn:
            cursor = conn.cursor()
            for table in tables_to_check:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                result = cursor.fetchone()
                if not result:
                    raise Exception(f"Table {table} not found")
                logger.info(f"Table {table}: OK")
        
        # 测试配置加载
        from config.distribution_config import get_distribution_config
        config = get_distribution_config()
        logger.info(f"Configuration loaded: mode={config.mode.value}")
        
        logger.info("Installation verification completed successfully")
        return True
        
    except Exception as e:
        logger.exception(f"Installation verification failed: {e}")
        return False


def cleanup_old_data():
    """清理旧数据（可选）"""
    logger.info("Cleaning up old data...")
    
    try:
        # 清理超时的设备
        DeviceDAO.cleanup_timeout_devices()
        
        # 清理旧的心跳记录（保留最近7天）
        DeviceHeartbeatDAO.cleanup_old_heartbeats(days=7)
        
        # 清理旧的任务分配记录（保留最近30天）
        TaskAssignmentDAO.cleanup_old_assignments(days=30)
        
        logger.info("Old data cleanup completed")
        
    except Exception as e:
        logger.exception(f"Error during cleanup: {e}")
        # 清理失败不应该阻止初始化过程


def main():
    """主函数"""
    # 设置日志
    setup_logging()
    
    logger.info("Starting distribution system initialization...")
    
    try:
        # 1. 设置环境变量
        setup_environment()
        
        # 2. 初始化数据库表
        init_database_tables()
        
        # 3. 创建默认配置文件
        create_default_configs()
        
        # 4. 清理旧数据
        cleanup_old_data()
        
        # 5. 验证安装
        if verify_installation():
            logger.info("Distribution system initialization completed successfully!")
            print("\n✅ Distribution system initialization completed successfully!")
            print("\nNext steps:")
            print("1. Review and modify configuration files in the config/ directory")
            print("2. Set appropriate environment variables for your deployment")
            print("3. Start the system using: python main.py distribute --mode master|worker|standalone")
            print("4. Access the web interface at: http://localhost:8000")
        else:
            logger.error("Distribution system initialization failed during verification")
            print("\n❌ Distribution system initialization failed during verification")
            print("Please check the logs for more details.")
            sys.exit(1)
            
    except Exception as e:
        logger.exception(f"Distribution system initialization failed: {e}")
        print(f"\n❌ Distribution system initialization failed: {e}")
        print("Please check the logs for more details.")
        sys.exit(1)


if __name__ == "__main__":
    main()