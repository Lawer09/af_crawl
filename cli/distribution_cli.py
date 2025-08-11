#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分布式任务系统命令行工具

用法:
    python -m cli.distribution_cli master --device-id master-001
    python -m cli.distribution_cli worker --device-id worker-001 --master-host localhost
    python -m cli.distribution_cli standalone --device-id standalone-001
    python -m cli.distribution_cli status
    python -m cli.distribution_cli config --mode master --output config.json
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.distribution_config import (
    DistributionConfig, DistributionMode, LoadBalanceStrategy,
    get_distribution_config, set_distribution_config,
    load_distribution_config_from_file, save_distribution_config_to_file,
    create_config_template
)
from client.distribution_client import get_distribution_client, get_async_distribution_client
from api.distribution_api import (
    init_distribution_services, start_distribution_services, stop_distribution_services
)
from services.task_scheduler import SchedulerMode
from utils.device_id_generator import generate_device_id, validate_device_id

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DistributionCLI:
    """分布式系统命令行接口"""
    
    def __init__(self):
        self.config: Optional[DistributionConfig] = None
        self.running = False
        self.shutdown_event = asyncio.Event()
    
    def _ensure_device_id(self, device_id: Optional[str], mode: str) -> str:
        """确保device_id存在，如果没有则自动生成"""
        if device_id:
            # 验证提供的device_id
            if not validate_device_id(device_id):
                logger.warning(f"Invalid device_id format: {device_id}, generating a new one")
                device_id = None
            else:
                logger.info(f"Using provided device_id: {device_id}")
                return device_id
        
        # 自动生成device_id
        generated_id = generate_device_id(mode)
        logger.info(f"Auto-generated device_id for {mode} mode: {generated_id}")
        return generated_id
    
    def _generate_device_name(self, device_id: str, mode: str) -> str:
        """根据device_id和模式生成设备名称"""
        if mode == 'master':
            if 'datacenter' in device_id or 'dc' in device_id:
                return f"Master Node ({device_id.split('-')[-1]})"
            return "Master Node"
        elif mode == 'worker':
            if 'server' in device_id:
                return f"Worker Node ({device_id.split('-')[-1]})"
            return "Worker Node"
        elif mode == 'standalone':
            return "Standalone Node"
        else:
            return f"Device ({device_id})"
        
    def setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self.running = False
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def load_config(self, config_file: Optional[str] = None, **kwargs) -> DistributionConfig:
        """加载配置"""
        if config_file and os.path.exists(config_file):
            logger.info(f"Loading config from {config_file}")
            config = load_distribution_config_from_file(config_file)
        else:
            config = get_distribution_config()
        
        # 应用命令行参数覆盖
        for key, value in kwargs.items():
            if value is not None and hasattr(config, key):
                setattr(config, key, value)
        
        # 验证配置
        config.validate()
        set_distribution_config(config)
        
        self.config = config
        return config
    
    def run_master(self, args):
        """运行主节点"""
        logger.info("Starting master node...")
        
        # 确保device_id存在
        device_id = self._ensure_device_id(args.device_id, "master")
        device_name = args.device_name or self._generate_device_name(device_id, "master")
        
        # 加载配置
        config_kwargs = {
            'mode': DistributionMode.MASTER,
            'device_id': device_id,
            'device_name': args.device_name,
            'master_host': args.host,
            'master_port': args.port,
            'dispatch_interval': args.dispatch_interval,
            'heartbeat_interval': args.heartbeat_interval,
            'load_balance_strategy': LoadBalanceStrategy(args.load_balance_strategy) if args.load_balance_strategy else None,
            'max_tasks_per_device': args.max_tasks_per_device,
            'enable_performance_monitoring': args.enable_monitoring,
            'api_key': args.api_key
        }
        
        config = self.load_config(args.config, **config_kwargs)
        
        try:
            # 初始化服务
            init_distribution_services(mode="master", device_id=config.device_id)
            
            # 启动服务
            start_distribution_services()
            
            logger.info(f"Master node started on {config.master_host}:{config.master_port}")
            logger.info(f"Device ID: {config.device_id}")
            logger.info(f"Load balance strategy: {config.load_balance_strategy.value}")
            
            # 设置信号处理
            self.setup_signal_handlers()
            self.running = True
            
            # 主循环
            while self.running:
                time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.exception(f"Error running master node: {e}")
            return 1
        finally:
            logger.info("Stopping master node...")
            stop_distribution_services()
        
        return 0
    
    def run_worker(self, args):
        """运行工作节点"""
        logger.info("Starting worker node...")
        
        # 确保device_id存在
        device_id = self._ensure_device_id(args.device_id, "worker")
        device_name = args.device_name or self._generate_device_name(device_id, "worker")
        
        # 加载配置
        config_kwargs = {
            'mode': DistributionMode.WORKER,
            'device_id': device_id,
            'device_name': device_name,
            'master_host': args.master_host,
            'master_port': args.master_port,
            'heartbeat_interval': args.heartbeat_interval,
            'task_pull_limit': args.task_pull_limit,
            'concurrent_tasks': args.concurrent_tasks,
            'enable_performance_monitoring': args.enable_monitoring,
            'api_key': args.api_key
        }
        
        config = self.load_config(args.config, **config_kwargs)
        
        try:
            # 初始化服务
            init_distribution_services(mode="worker", device_id=config.device_id)
            
            # 启动异步工作循环
            asyncio.run(self._run_worker_async(config))
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.exception(f"Error running worker node: {e}")
            return 1
        
        return 0
    
    async def _run_worker_async(self, config: DistributionConfig):
        """异步运行工作节点"""
        client = get_async_distribution_client()
        
        try:
            # 注册设备
            success = await client.register_device()
            if not success:
                logger.error("Failed to register device")
                return
            
            # 更新设备状态为在线
            await client.update_device_status("online")
            
            # 启动心跳
            await client.start_heartbeat()
            
            # 启动服务
            start_distribution_services()
            
            logger.info(f"Worker node started")
            logger.info(f"Device ID: {config.device_id}")
            logger.info(f"Master: {config.master_host}:{config.master_port}")
            logger.info(f"Concurrent tasks: {config.concurrent_tasks}")
            
            # 设置信号处理
            self.setup_signal_handlers()
            self.running = True
            
            # 主循环
            while self.running:
                try:
                    # 等待关闭信号或超时
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
                    break
                except asyncio.TimeoutError:
                    continue
            
        except Exception as e:
            logger.exception(f"Error in worker async loop: {e}")
        finally:
            logger.info("Stopping worker node...")
            
            # 更新设备状态为离线
            try:
                await client.update_device_status("offline")
            except Exception:
                pass
            
            # 停止心跳
            await client.stop_heartbeat()
            
            # 停止服务
            stop_distribution_services()
    
    def run_standalone(self, args):
        """运行独立节点"""
        logger.info("Starting standalone node...")
        
        # 加载配置
        config_kwargs = {
            'mode': DistributionMode.STANDALONE,
            'device_id': args.device_id,
            'device_name': args.device_name,
            'dispatch_interval': args.dispatch_interval,
            'concurrent_tasks': args.concurrent_tasks,
            'enable_performance_monitoring': args.enable_monitoring
        }
        
        config = self.load_config(args.config, **config_kwargs)
        
        try:
            # 初始化服务
            init_distribution_services(mode="standalone", device_id=config.device_id)
            
            # 启动服务
            start_distribution_services()
            
            logger.info(f"Standalone node started")
            logger.info(f"Device ID: {config.device_id}")
            logger.info(f"Concurrent tasks: {config.concurrent_tasks}")
            
            # 设置信号处理
            self.setup_signal_handlers()
            self.running = True
            
            # 主循环
            while self.running:
                time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.exception(f"Error running standalone node: {e}")
            return 1
        finally:
            logger.info("Stopping standalone node...")
            stop_distribution_services()
        
        return 0
    
    def show_status(self, args):
        """显示系统状态"""
        try:
            client = get_distribution_client()
            
            # 测试连接
            if not client.test_connection():
                print("❌ Cannot connect to master node")
                return 1
            
            print("✅ Connected to master node")
            print()
            
            # 获取系统概览
            overview = client.get_system_overview()
            if overview:
                print("📊 System Overview:")
                task_stats = overview.get('task_stats', {})
                device_stats = overview.get('device_stats', {})
                
                print(f"  Tasks:")
                for status, count in task_stats.items():
                    print(f"    {status}: {count}")
                
                print(f"  Devices:")
                print(f"    Total: {device_stats.get('total', 0)}")
                print(f"    Online: {device_stats.get('online', 0)}")
                print(f"    Offline: {device_stats.get('offline', 0)}")
                print()
            
            # 获取设备列表
            devices = client.get_all_devices()
            if devices:
                print("🖥️  Devices:")
                for device in devices:
                    status_icon = "🟢" if device['status'] == 'online' else "🔴"
                    print(f"  {status_icon} {device['device_id']} ({device['device_name']})")
                    print(f"      Type: {device['device_type']}")
                    print(f"      Status: {device['status']}")
                    print(f"      Tasks: {device.get('current_tasks', 0)}")
                    if device.get('last_heartbeat'):
                        print(f"      Last heartbeat: {device['last_heartbeat']}")
                    print()
            
            # 获取调度器状态
            scheduler_status = client.get_scheduler_status()
            if scheduler_status:
                print("⚙️  Scheduler Status:")
                print(f"  Status: {scheduler_status.get('status', 'unknown')}")
                if 'stats' in scheduler_status:
                    stats = scheduler_status['stats']
                    print(f"  Dispatched tasks: {stats.get('dispatched_tasks', 0)}")
                    print(f"  Failed dispatches: {stats.get('failed_dispatches', 0)}")
                    print(f"  Active devices: {stats.get('active_devices', 0)}")
            
        except Exception as e:
            logger.exception(f"Error getting status: {e}")
            print(f"❌ Error: {e}")
            return 1
        
        return 0
    
    def generate_config(self, args):
        """生成配置文件"""
        try:
            # 确保device_id存在
            device_id = args.device_id
            if args.mode and not device_id:
                device_id = self._ensure_device_id(device_id, args.mode)
            
            # 生成设备名称（如果未提供）
            device_name = args.device_name
            if args.mode and not device_name:
                device_name = self._generate_device_name(device_id, args.mode)
            
            # 创建配置模板
            template = create_config_template(
                mode=args.mode,
                device_id=device_id,
                device_name=device_name
            )
            
            # 应用额外参数
            if args.master_host:
                template['master_host'] = args.master_host
            if args.master_port:
                template['master_port'] = args.master_port
            if args.concurrent_tasks:
                template['concurrent_tasks'] = args.concurrent_tasks
            
            # 创建配置对象
            config = DistributionConfig.from_dict(template)
            
            if args.output:
                # 保存到文件
                save_distribution_config_to_file(config, args.output)
                print(f"✅ Configuration saved to {args.output}")
            else:
                # 输出到控制台
                print(json.dumps(config.to_dict(), indent=2, ensure_ascii=False))
            
        except Exception as e:
            logger.exception(f"Error generating config: {e}")
            print(f"❌ Error: {e}")
            return 1
        
        return 0
    
    def test_connection(self, args):
        """测试连接"""
        try:
            # 加载配置
            config_kwargs = {
                'master_host': args.master_host,
                'master_port': args.master_port,
                'api_key': args.api_key
            }
            
            config = self.load_config(args.config, **config_kwargs)
            
            client = get_distribution_client()
            
            print(f"Testing connection to {config.get_master_url()}...")
            
            if client.test_connection():
                print("✅ Connection successful")
                
                # 获取系统概览
                overview = client.get_system_overview()
                if overview:
                    print(f"📊 System is running with {overview.get('device_stats', {}).get('total', 0)} devices")
                
                return 0
            else:
                print("❌ Connection failed")
                return 1
                
        except Exception as e:
            logger.exception(f"Error testing connection: {e}")
            print(f"❌ Error: {e}")
            return 1


def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="分布式任务系统命令行工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s master --device-id master-001
  %(prog)s worker --device-id worker-001 --master-host 192.168.1.100
  %(prog)s standalone --device-id standalone-001
  %(prog)s status
  %(prog)s config --mode master --output master-config.json
        """
    )
    
    parser.add_argument('--config', '-c', help='配置文件路径')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # Master命令
    master_parser = subparsers.add_parser('master', help='启动主节点')
    master_parser.add_argument('--device-id', help='设备ID（可选，未提供时自动生成）')
    master_parser.add_argument('--device-name', help='设备名称')
    master_parser.add_argument('--host', default='localhost', help='监听地址')
    master_parser.add_argument('--port', type=int, default=7989, help='监听端口')
    master_parser.add_argument('--dispatch-interval', type=int, default=10, help='任务分发间隔(秒)')
    master_parser.add_argument('--heartbeat-interval', type=int, default=30, help='心跳间隔(秒)')
    master_parser.add_argument('--load-balance-strategy', 
                              choices=['round_robin', 'least_tasks', 'weighted', 'random'],
                              default='least_tasks', help='负载均衡策略')
    master_parser.add_argument('--max-tasks-per-device', type=int, default=5, help='每设备最大任务数')
    master_parser.add_argument('--enable-monitoring', action='store_true', help='启用性能监控')
    master_parser.add_argument('--api-key', help='API密钥')
    
    # Worker命令
    worker_parser = subparsers.add_parser('worker', help='启动工作节点')
    worker_parser.add_argument('--device-id', help='设备ID（可选，未提供时自动生成）')
    worker_parser.add_argument('--device-name', help='设备名称')
    worker_parser.add_argument('--master-host', required=True, help='主节点地址')
    worker_parser.add_argument('--master-port', type=int, default=7989, help='主节点端口')
    worker_parser.add_argument('--heartbeat-interval', type=int, default=30, help='心跳间隔(秒)')
    worker_parser.add_argument('--task-pull-limit', type=int, default=5, help='任务拉取限制')
    worker_parser.add_argument('--concurrent-tasks', type=int, default=3, help='并发任务数')
    worker_parser.add_argument('--enable-monitoring', action='store_true', help='启用性能监控')
    worker_parser.add_argument('--api-key', help='API密钥')
    
    # Standalone命令
    standalone_parser = subparsers.add_parser('standalone', help='启动独立节点')
    standalone_parser.add_argument('--device-id', help='设备ID（可选，未提供时自动生成）')
    standalone_parser.add_argument('--device-name', help='设备名称')
    standalone_parser.add_argument('--dispatch-interval', type=int, default=10, help='任务分发间隔(秒)')
    standalone_parser.add_argument('--concurrent-tasks', type=int, default=5, help='并发任务数')
    standalone_parser.add_argument('--enable-monitoring', action='store_true', help='启用性能监控')
    
    # Status命令
    status_parser = subparsers.add_parser('status', help='显示系统状态')
    status_parser.add_argument('--master-host', default='localhost', help='主节点地址')
    status_parser.add_argument('--master-port', type=int, default=7989, help='主节点端口')
    status_parser.add_argument('--api-key', help='API密钥')
    
    # Config命令
    config_parser = subparsers.add_parser('config', help='生成配置文件')
    config_parser.add_argument('--mode', required=True, 
                              choices=['master', 'worker', 'standalone'],
                              help='运行模式')
    config_parser.add_argument('--device-id', help='设备ID（可选，未提供时根据模式自动生成）')
    config_parser.add_argument('--device-name', help='设备名称')
    config_parser.add_argument('--master-host', help='主节点地址')
    config_parser.add_argument('--master-port', type=int, help='主节点端口')
    config_parser.add_argument('--concurrent-tasks', type=int, help='并发任务数')
    config_parser.add_argument('--output', '-o', help='输出文件路径')
    
    # Test命令
    test_parser = subparsers.add_parser('test', help='测试连接')
    test_parser.add_argument('--master-host', default='localhost', help='主节点地址')
    test_parser.add_argument('--master-port', type=int, default=7989, help='主节点端口')
    test_parser.add_argument('--api-key', help='API密钥')
    
    return parser


def main():
    """主函数"""
    parser = create_parser()
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 检查命令
    if not args.command:
        parser.print_help()
        return 1
    
    cli = DistributionCLI()
    
    try:
        if args.command == 'master':
            return cli.run_master(args)
        elif args.command == 'worker':
            return cli.run_worker(args)
        elif args.command == 'standalone':
            return cli.run_standalone(args)
        elif args.command == 'status':
            return cli.show_status(args)
        elif args.command == 'config':
            return cli.generate_config(args)
        elif args.command == 'test':
            return cli.test_connection(args)
        else:
            print(f"Unknown command: {args.command}")
            return 1
            
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())