from __future__ import annotations

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

# 延迟导入以避免循环依赖
def _get_device_id_generator():
    try:
        from utils.device_id_generator import generate_device_id
        return generate_device_id
    except ImportError:
        return None


class DistributionMode(Enum):
    """分布式模式枚举"""
    MASTER = "master"
    WORKER = "worker"
    STANDALONE = "standalone"


class LoadBalanceStrategy(Enum):
    """负载均衡策略枚举"""
    ROUND_ROBIN = "round_robin"
    LEAST_TASKS = "least_tasks"
    WEIGHTED = "weighted"
    RANDOM = "random"


@dataclass
class DistributionConfig:
    """分布式配置类"""
    
    # 基本配置
    mode: DistributionMode = DistributionMode.STANDALONE
    device_id: Optional[str] = None
    device_name: Optional[str] = None
    device_type: str = "worker"
    
    # 主节点配置
    master_host: str = "0.0.0.0"
    master_port: int = 8000
    master_api_prefix: str = "/api/distribution"
    
    # 任务调度配置
    dispatch_interval: int = 10  # 秒
    heartbeat_interval: int = 30  # 秒
    task_timeout_check_interval: int = 60  # 秒
    device_timeout_threshold: int = 180  # 秒
    
    # 负载均衡配置
    load_balance_strategy: LoadBalanceStrategy = LoadBalanceStrategy.LEAST_TASKS
    max_tasks_per_device: int = 5
    task_retry_delay: int = 300  # 秒
    max_retry_count: int = 3
    
    # 任务执行配置
    default_task_timeout: int = 3600  # 秒
    task_pull_limit: int = 5
    concurrent_tasks: int = 3
    
    # 监控配置
    enable_performance_monitoring: bool = True
    heartbeat_data_retention_days: int = 7
    assignment_data_retention_days: int = 30
    
    # 安全配置
    api_key: Optional[str] = None
    enable_ssl: bool = False
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None
    
    # 高级配置
    enable_auto_scaling: bool = False
    auto_scaling_threshold: float = 0.8  # CPU使用率阈值
    enable_task_priority: bool = True
    high_priority_threshold: int = 8
    
    # 故障处理配置
    enable_failover: bool = True
    failover_timeout: int = 60  # 秒
    enable_task_redistribution: bool = True
    redistribution_delay: int = 120  # 秒
    
    @classmethod
    def from_env(cls) -> 'DistributionConfig':
        """从环境变量创建配置"""
        config = cls()
        
        # 基本配置
        if mode_str := os.getenv('DISTRIBUTION_MODE'):
            try:
                config.mode = DistributionMode(mode_str.lower())
            except ValueError:
                pass
        
        config.device_id = os.getenv('DEVICE_ID')
        
        # 如果device_id未设置且有模式信息，自动生成
        if not config.device_id and config.mode:
            generate_device_id = _get_device_id_generator()
            if generate_device_id:
                config.device_id = generate_device_id(config.mode.value)
        
        config.device_name = os.getenv('DEVICE_NAME')
        config.device_type = os.getenv('DEVICE_TYPE', config.device_type)
        
        # 主节点配置
        config.master_host = os.getenv('MASTER_HOST', config.master_host)
        if master_port := os.getenv('MASTER_PORT'):
            try:
                config.master_port = int(master_port)
            except ValueError:
                pass
        
        config.master_api_prefix = os.getenv('MASTER_API_PREFIX', config.master_api_prefix)
        
        # 任务调度配置
        if dispatch_interval := os.getenv('DISPATCH_INTERVAL'):
            try:
                config.dispatch_interval = int(dispatch_interval)
            except ValueError:
                pass
        
        if heartbeat_interval := os.getenv('HEARTBEAT_INTERVAL'):
            try:
                config.heartbeat_interval = int(heartbeat_interval)
            except ValueError:
                pass
        
        if timeout_check_interval := os.getenv('TASK_TIMEOUT_CHECK_INTERVAL'):
            try:
                config.task_timeout_check_interval = int(timeout_check_interval)
            except ValueError:
                pass
        
        if device_timeout := os.getenv('DEVICE_TIMEOUT_THRESHOLD'):
            try:
                config.device_timeout_threshold = int(device_timeout)
            except ValueError:
                pass
        
        # 负载均衡配置
        if strategy_str := os.getenv('LOAD_BALANCE_STRATEGY'):
            try:
                config.load_balance_strategy = LoadBalanceStrategy(strategy_str.lower())
            except ValueError:
                pass
        
        if max_tasks := os.getenv('MAX_TASKS_PER_DEVICE'):
            try:
                config.max_tasks_per_device = int(max_tasks)
            except ValueError:
                pass
        
        if retry_delay := os.getenv('TASK_RETRY_DELAY'):
            try:
                config.task_retry_delay = int(retry_delay)
            except ValueError:
                pass
        
        if max_retry := os.getenv('MAX_RETRY_COUNT'):
            try:
                config.max_retry_count = int(max_retry)
            except ValueError:
                pass
        
        # 任务执行配置
        if task_timeout := os.getenv('DEFAULT_TASK_TIMEOUT'):
            try:
                config.default_task_timeout = int(task_timeout)
            except ValueError:
                pass
        
        if pull_limit := os.getenv('TASK_PULL_LIMIT'):
            try:
                config.task_pull_limit = int(pull_limit)
            except ValueError:
                pass
        
        if concurrent := os.getenv('CONCURRENT_TASKS'):
            try:
                config.concurrent_tasks = int(concurrent)
            except ValueError:
                pass
        
        # 监控配置
        if monitoring := os.getenv('ENABLE_PERFORMANCE_MONITORING'):
            config.enable_performance_monitoring = monitoring.lower() in ('true', '1', 'yes')
        
        if heartbeat_retention := os.getenv('HEARTBEAT_DATA_RETENTION_DAYS'):
            try:
                config.heartbeat_data_retention_days = int(heartbeat_retention)
            except ValueError:
                pass
        
        if assignment_retention := os.getenv('ASSIGNMENT_DATA_RETENTION_DAYS'):
            try:
                config.assignment_data_retention_days = int(assignment_retention)
            except ValueError:
                pass
        
        # 安全配置
        config.api_key = os.getenv('API_KEY')
        
        if enable_ssl := os.getenv('ENABLE_SSL'):
            config.enable_ssl = enable_ssl.lower() in ('true', '1', 'yes')
        
        config.ssl_cert_path = os.getenv('SSL_CERT_PATH')
        config.ssl_key_path = os.getenv('SSL_KEY_PATH')
        
        # 高级配置
        if auto_scaling := os.getenv('ENABLE_AUTO_SCALING'):
            config.enable_auto_scaling = auto_scaling.lower() in ('true', '1', 'yes')
        
        if scaling_threshold := os.getenv('AUTO_SCALING_THRESHOLD'):
            try:
                config.auto_scaling_threshold = float(scaling_threshold)
            except ValueError:
                pass
        
        if task_priority := os.getenv('ENABLE_TASK_PRIORITY'):
            config.enable_task_priority = task_priority.lower() in ('true', '1', 'yes')
        
        if priority_threshold := os.getenv('HIGH_PRIORITY_THRESHOLD'):
            try:
                config.high_priority_threshold = int(priority_threshold)
            except ValueError:
                pass
        
        # 故障处理配置
        if failover := os.getenv('ENABLE_FAILOVER'):
            config.enable_failover = failover.lower() in ('true', '1', 'yes')
        
        if failover_timeout := os.getenv('FAILOVER_TIMEOUT'):
            try:
                config.failover_timeout = int(failover_timeout)
            except ValueError:
                pass
        
        if redistribution := os.getenv('ENABLE_TASK_REDISTRIBUTION'):
            config.enable_task_redistribution = redistribution.lower() in ('true', '1', 'yes')
        
        if redistribution_delay := os.getenv('REDISTRIBUTION_DELAY'):
            try:
                config.redistribution_delay = int(redistribution_delay)
            except ValueError:
                pass
        
        return config
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DistributionConfig':
        """从字典创建配置"""
        config = cls()
        
        for key, value in config_dict.items():
            if hasattr(config, key):
                # 处理枚举类型
                if key == 'mode' and isinstance(value, str):
                    try:
                        value = DistributionMode(value.lower())
                    except ValueError:
                        continue
                elif key == 'load_balance_strategy' and isinstance(value, str):
                    try:
                        value = LoadBalanceStrategy(value.lower())
                    except ValueError:
                        continue
                
                setattr(config, key, value)
        
        return config
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {}
        
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        
        return result
    
    def get_master_url(self) -> str:
        """获取主节点URL"""
        protocol = "https" if self.enable_ssl else "http"
        return f"{protocol}://{self.master_host}:{self.master_port}{self.master_api_prefix}"
    
    def validate(self) -> bool:
        """验证配置"""
        # 检查必需的配置
        if self.mode == DistributionMode.WORKER and not self.device_id:
            raise ValueError("Worker mode requires device_id")
        
        if self.mode == DistributionMode.WORKER and not self.master_host:
            raise ValueError("Worker mode requires master_host")
        
        # 检查数值范围
        if self.dispatch_interval <= 0:
            raise ValueError("dispatch_interval must be positive")
        
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        
        if self.max_tasks_per_device <= 0:
            raise ValueError("max_tasks_per_device must be positive")
        
        if self.task_pull_limit <= 0:
            raise ValueError("task_pull_limit must be positive")
        
        if self.concurrent_tasks <= 0:
            raise ValueError("concurrent_tasks must be positive")
        
        # 检查SSL配置
        if self.enable_ssl:
            if not self.ssl_cert_path or not self.ssl_key_path:
                raise ValueError("SSL enabled but cert/key paths not provided")
        
        return True
    
    def get_device_capabilities(self) -> Dict[str, Any]:
        """获取设备能力配置"""
        return {
            "max_concurrent_tasks": self.concurrent_tasks,
            "supported_task_types": ["app_sync", "data_sync"],  # 可根据需要扩展
            "performance_monitoring": self.enable_performance_monitoring,
            "auto_scaling": self.enable_auto_scaling,
            "task_priority": self.enable_task_priority
        }
    
    def save_to_file(self, config_path: str) -> None:
        """保存配置到文件"""
        import json
        import os
        
        try:
            # 验证配置
            self.validate()
            
            # 确保目录存在
            config_dir = os.path.dirname(config_path)
            if config_dir and not os.path.exists(config_dir):
                os.makedirs(config_dir, exist_ok=True)
            
            # 保存配置
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            raise ValueError(f"Failed to save config to {config_path}: {e}")
    
    @classmethod
    def load_from_file(cls, config_path: str) -> 'DistributionConfig':
        """从文件加载配置"""
        import json
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_dict = json.load(f)
            
            config = cls.from_dict(config_dict)
            config.validate()
            
            return config
            
        except Exception as e:
            raise ValueError(f"Failed to load config from {config_path}: {e}")
    
    @classmethod
    def get_master_template(cls, device_id: str = None, device_name: str = None, **kwargs) -> 'DistributionConfig':
        """获取Master节点配置模板"""
        template_dict = MASTER_CONFIG_TEMPLATE.copy()
        
        # 应用自定义参数
        if device_id:
            template_dict["device_id"] = device_id
        if device_name:
            template_dict["device_name"] = device_name
        
        # 应用额外的关键字参数
        template_dict.update(kwargs)
        
        return cls.from_dict(template_dict)
    
    @classmethod
    def get_worker_template(cls, device_id: str = None, device_name: str = None, master_host: str = None, master_port: int = None, **kwargs) -> 'DistributionConfig':
        """获取Worker节点配置模板"""
        template_dict = WORKER_CONFIG_TEMPLATE.copy()
        
        # 应用自定义参数
        if device_id:
            template_dict["device_id"] = device_id
        if device_name:
            template_dict["device_name"] = device_name
        if master_host:
            template_dict["master_host"] = master_host
        if master_port:
            template_dict["master_port"] = master_port
        
        # 应用额外的关键字参数
        template_dict.update(kwargs)
        
        return cls.from_dict(template_dict)
    
    @classmethod
    def get_standalone_template(cls, device_id: str = None, device_name: str = None, **kwargs) -> 'DistributionConfig':
        """获取Standalone节点配置模板"""
        template_dict = STANDALONE_CONFIG_TEMPLATE.copy()
        
        # 应用自定义参数
        if device_id:
            template_dict["device_id"] = device_id
        if device_name:
            template_dict["device_name"] = device_name
        
        # 应用额外的关键字参数
        template_dict.update(kwargs)
        
        return cls.from_dict(template_dict)


# 全局配置实例
_distribution_config: Optional[DistributionConfig] = None


def get_distribution_config() -> DistributionConfig:
    """获取分布式配置"""
    global _distribution_config
    
    if _distribution_config is None:
        _distribution_config = DistributionConfig.from_env()
    
    return _distribution_config


def set_distribution_config(config: DistributionConfig) -> None:
    """设置分布式配置"""
    global _distribution_config
    
    config.validate()
    _distribution_config = config


def load_distribution_config_from_file(config_path: str) -> DistributionConfig:
    """从文件加载分布式配置"""
    import json
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
        
        config = DistributionConfig.from_dict(config_dict)
        config.validate()
        
        return config
        
    except Exception as e:
        raise ValueError(f"Failed to load config from {config_path}: {e}")


def save_distribution_config_to_file(config: DistributionConfig, config_path: str) -> None:
    """保存分布式配置到文件"""
    import json
    
    try:
        config.validate()
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config.to_dict(), f, indent=2, ensure_ascii=False)
            
    except Exception as e:
        raise ValueError(f"Failed to save config to {config_path}: {e}")


# 预定义配置模板
MASTER_CONFIG_TEMPLATE = {
    "mode": "master",
    "device_id": "master-001",
    "device_name": "Master Node",
    "device_type": "master",
    "master_host": "localhost",
    "master_port": 8000,
    "master_api_prefix": "/api/distribution",
    "dispatch_interval": 5,
    "heartbeat_interval": 30,
    "task_timeout_check_interval": 60,
    "device_timeout_threshold": 180,
    "load_balance_strategy": "least_tasks",
    "max_tasks_per_device": 10,
    "task_retry_delay": 300,
    "max_retry_count": 3,
    "default_task_timeout": 3600,
    "task_pull_limit": 10,
    "concurrent_tasks": 5,
    "enable_performance_monitoring": True,
    "heartbeat_data_retention_days": 7,
    "assignment_data_retention_days": 30,
    "enable_ssl": False,
    "enable_auto_scaling": True,
    "auto_scaling_threshold": 0.8,
    "enable_task_priority": True,
    "high_priority_threshold": 8,
    "enable_failover": True,
    "failover_timeout": 60,
    "enable_task_redistribution": True,
    "redistribution_delay": 120
}

WORKER_CONFIG_TEMPLATE = {
    "mode": "worker",
    "device_id": "worker-001",
    "device_name": "Worker Node 1",
    "device_type": "worker",
    "master_host": "localhost",
    "master_port": 8000,
    "master_api_prefix": "/api/distribution",
    "heartbeat_interval": 30,
    "max_tasks_per_device": 5,
    "task_retry_delay": 300,
    "max_retry_count": 3,
    "default_task_timeout": 3600,
    "task_pull_limit": 5,
    "concurrent_tasks": 3,
    "enable_performance_monitoring": True,
    "heartbeat_data_retention_days": 7,
    "assignment_data_retention_days": 30,
    "enable_ssl": False,
    "enable_auto_scaling": False,
    "enable_task_priority": True,
    "high_priority_threshold": 8,
    "enable_failover": True,
    "failover_timeout": 60
}

STANDALONE_CONFIG_TEMPLATE = {
    "mode": "standalone",
    "device_id": "standalone-001",
    "device_name": "Standalone Node",
    "device_type": "standalone",
    "master_host": "localhost",
    "master_port": 8000,
    "master_api_prefix": "/api/distribution",
    "dispatch_interval": 10,
    "heartbeat_interval": 30,
    "task_timeout_check_interval": 60,
    "device_timeout_threshold": 180,
    "load_balance_strategy": "least_tasks",
    "max_tasks_per_device": 5,
    "task_retry_delay": 300,
    "max_retry_count": 3,
    "default_task_timeout": 3600,
    "task_pull_limit": 5,
    "concurrent_tasks": 5,
    "enable_performance_monitoring": False,
    "heartbeat_data_retention_days": 7,
    "assignment_data_retention_days": 30,
    "enable_ssl": False,
    "enable_auto_scaling": False,
    "auto_scaling_threshold": 0.8,
    "enable_task_priority": True,
    "high_priority_threshold": 8,
    "enable_failover": False,
    "failover_timeout": 60,
    "enable_task_redistribution": False,
    "redistribution_delay": 120
}


def create_config_template(mode: str, device_id: str = None, device_name: str = None) -> Dict[str, Any]:
    """创建配置模板"""
    if mode.lower() == "master":
        template = MASTER_CONFIG_TEMPLATE.copy()
    elif mode.lower() == "worker":
        template = WORKER_CONFIG_TEMPLATE.copy()
    elif mode.lower() == "standalone":
        template = STANDALONE_CONFIG_TEMPLATE.copy()
    else:
        raise ValueError(f"Unknown mode: {mode}")
    
    # 处理device_id
    if device_id:
        template["device_id"] = device_id
    else:
        # 自动生成device_id
        generate_device_id = _get_device_id_generator()
        if generate_device_id:
            template["device_id"] = generate_device_id(mode.lower())
    
    # 处理device_name
    if device_name:
        template["device_name"] = device_name
    elif not device_name and template.get("device_id"):
        # 根据device_id和模式生成设备名称
        device_id_val = template["device_id"]
        if mode.lower() == 'master':
            if 'datacenter' in device_id_val or 'dc' in device_id_val:
                template["device_name"] = f"Master Node ({device_id_val.split('-')[-1]})"
            else:
                template["device_name"] = "Master Node"
        elif mode.lower() == 'worker':
            if 'server' in device_id_val:
                template["device_name"] = f"Worker Node ({device_id_val.split('-')[-1]})"
            else:
                template["device_name"] = "Worker Node"
        elif mode.lower() == 'standalone':
            template["device_name"] = "Standalone Node"
    
    return template