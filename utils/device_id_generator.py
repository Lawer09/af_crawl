#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Device ID 自动生成工具

根据配置指南的描述，实现根据模式自动生成有意义的device_id：
- master模式: master-001, master-datacenter1
- worker模式: worker-001, worker-server01  
- standalone模式: standalone-001, master-stand-01

使用方法:
    from utils.device_id_generator import DeviceIdGenerator
    
    generator = DeviceIdGenerator()
    device_id = generator.generate(mode='master')
    device_id = generator.generate(mode='worker', suffix='server01')
"""

import socket
import uuid
import re
from typing import Optional, Dict
from datetime import datetime


class DeviceIdGenerator:
    """Device ID 生成器"""
    
    def __init__(self):
        self.hostname = socket.gethostname()
        self.mac_address = self._get_mac_address()
        self.ip_address = self._get_local_ip()
    
    def _get_mac_address(self) -> str:
        """获取MAC地址"""
        try:
            mac = uuid.getnode()
            mac_str = ':'.join(['{:02x}'.format((mac >> elements) & 0xff) 
                               for elements in range(0, 2*6, 2)][::-1])
            return mac_str.replace(':', '')
        except Exception:
            return 'unknown'
    
    def _get_local_ip(self) -> str:
        """获取本地IP地址"""
        try:
            # 连接到一个远程地址来获取本地IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                return s.getsockname()[0]
        except Exception:
            return '127.0.0.1'
    
    def _sanitize_hostname(self) -> str:
        """清理主机名，移除特殊字符"""
        # 只保留字母、数字和连字符
        sanitized = re.sub(r'[^a-zA-Z0-9-]', '', self.hostname.lower())
        # 限制长度
        return sanitized[:20] if sanitized else 'unknown'
    
    def generate(self, mode: str, suffix: Optional[str] = None, 
                datacenter: Optional[str] = None, 
                use_hostname: bool = True,
                use_timestamp: bool = False) -> str:
        """
        生成device_id
        
        Args:
            mode: 运行模式 ('master', 'worker', 'standalone')
            suffix: 自定义后缀 (如 'datacenter1', 'server01')
            datacenter: 数据中心标识
            use_hostname: 是否使用主机名
            use_timestamp: 是否使用时间戳
        
        Returns:
            生成的device_id
        """
        mode = mode.lower()
        
        # 基础前缀
        if mode == 'master':
            prefix = 'master'
        elif mode == 'worker':
            prefix = 'worker'
        elif mode == 'standalone':
            prefix = 'standalone'
        else:
            prefix = 'device'
        
        # 构建ID组件
        components = [prefix]
        
        # 添加数据中心标识
        if datacenter:
            components.append(datacenter)
        
        # 添加主机名或自定义后缀
        if suffix:
            components.append(suffix)
        elif use_hostname:
            hostname = self._sanitize_hostname()
            if hostname and hostname != 'unknown':
                components.append(hostname)
        
        # 添加时间戳
        if use_timestamp:
            timestamp = datetime.now().strftime('%m%d%H%M')
            components.append(timestamp)
        
        # 如果没有足够的组件，添加默认编号
        if len(components) == 1:
            components.append('001')
        
        return '-'.join(components)
    
    def generate_master_id(self, datacenter: Optional[str] = None, 
                          suffix: Optional[str] = None) -> str:
        """
        生成master节点ID
        
        Examples:
            master-001
            master-datacenter1
            master-dc1-001
        """
        return self.generate('master', suffix=suffix, datacenter=datacenter)
    
    def generate_worker_id(self, server_name: Optional[str] = None,
                          datacenter: Optional[str] = None) -> str:
        """
        生成worker节点ID
        
        Examples:
            worker-001
            worker-server01
            worker-dc1-server01
        """
        return self.generate('worker', suffix=server_name, datacenter=datacenter)
    
    def generate_standalone_id(self, suffix: Optional[str] = None) -> str:
        """
        生成standalone节点ID
        
        Examples:
            standalone-001
            master-stand-01 (兼容现有命名)
        """
        if suffix and 'stand' in suffix:
            # 兼容现有的 master-stand-01 格式
            return f"master-{suffix}"
        return self.generate('standalone', suffix=suffix)
    
    def generate_unique_id(self, mode: str, base_name: Optional[str] = None) -> str:
        """
        生成唯一ID（包含MAC地址或IP信息）
        
        Args:
            mode: 运行模式
            base_name: 基础名称
        
        Returns:
            唯一的device_id
        """
        # 使用MAC地址后4位作为唯一标识
        unique_suffix = self.mac_address[-4:] if self.mac_address != 'unknown' else self.ip_address.split('.')[-1]
        
        if base_name:
            suffix = f"{base_name}-{unique_suffix}"
        else:
            suffix = unique_suffix
        
        return self.generate(mode, suffix=suffix)
    
    def validate_device_id(self, device_id: str) -> bool:
        """
        验证device_id格式
        
        Args:
            device_id: 要验证的device_id
        
        Returns:
            是否有效
        """
        if not device_id:
            return False
        
        # 检查长度
        if len(device_id) > 64:
            return False
        
        # 检查字符（只允许字母、数字、连字符、下划线）
        if not re.match(r'^[a-zA-Z0-9_-]+$', device_id):
            return False
        
        # 检查是否以字母开头
        if not device_id[0].isalpha():
            return False
        
        return True
    
    def suggest_device_names(self, mode: str, count: int = 5) -> Dict[str, str]:
        """
        建议device_id和对应的显示名称
        
        Args:
            mode: 运行模式
            count: 建议数量
        
        Returns:
            {device_id: device_name} 字典
        """
        suggestions = {}
        
        for i in range(1, count + 1):
            if mode == 'master':
                device_id = f"master-{i:03d}"
                device_name = f"Master Node {i}"
            elif mode == 'worker':
                device_id = f"worker-{i:03d}"
                device_name = f"Worker Node {i}"
            elif mode == 'standalone':
                device_id = f"standalone-{i:03d}"
                device_name = f"Standalone Node {i}"
            else:
                device_id = f"device-{i:03d}"
                device_name = f"Device {i}"
            
            suggestions[device_id] = device_name
        
        # 添加基于主机名的建议
        hostname = self._sanitize_hostname()
        if hostname and hostname != 'unknown':
            if mode == 'master':
                device_id = f"master-{hostname}"
                device_name = f"Master Node ({hostname})"
            elif mode == 'worker':
                device_id = f"worker-{hostname}"
                device_name = f"Worker Node ({hostname})"
            elif mode == 'standalone':
                device_id = f"standalone-{hostname}"
                device_name = f"Standalone Node ({hostname})"
            else:
                device_id = f"device-{hostname}"
                device_name = f"Device ({hostname})"
            
            suggestions[device_id] = device_name
        
        return suggestions
    
    def get_device_info(self) -> Dict[str, str]:
        """
        获取设备信息
        
        Returns:
            设备信息字典
        """
        return {
            'hostname': self.hostname,
            'mac_address': self.mac_address,
            'ip_address': self.ip_address,
            'sanitized_hostname': self._sanitize_hostname()
        }


# 全局实例
_device_id_generator = DeviceIdGenerator()


def generate_device_id(mode: str, **kwargs) -> str:
    """
    便捷函数：生成device_id
    
    Args:
        mode: 运行模式
        **kwargs: 其他参数
    
    Returns:
        生成的device_id
    """
    return _device_id_generator.generate(mode, **kwargs)


def generate_master_id(**kwargs) -> str:
    """便捷函数：生成master节点ID"""
    return _device_id_generator.generate_master_id(**kwargs)


def generate_worker_id(**kwargs) -> str:
    """便捷函数：生成worker节点ID"""
    return _device_id_generator.generate_worker_id(**kwargs)


def generate_standalone_id(**kwargs) -> str:
    """便捷函数：生成standalone节点ID"""
    return _device_id_generator.generate_standalone_id(**kwargs)


def validate_device_id(device_id: str) -> bool:
    """便捷函数：验证device_id"""
    return _device_id_generator.validate_device_id(device_id)


def suggest_device_names(mode: str, count: int = 5) -> Dict[str, str]:
    """便捷函数：建议device_id和名称"""
    return _device_id_generator.suggest_device_names(mode, count)


if __name__ == '__main__':
    # 测试代码
    generator = DeviceIdGenerator()
    
    print("设备信息:")
    info = generator.get_device_info()
    for key, value in info.items():
        print(f"  {key}: {value}")
    
    print("\n生成示例:")
    print(f"Master ID: {generator.generate_master_id()}")
    print(f"Worker ID: {generator.generate_worker_id()}")
    print(f"Standalone ID: {generator.generate_standalone_id()}")
    
    print(f"\nMaster ID (datacenter): {generator.generate_master_id(datacenter='dc1')}")
    print(f"Worker ID (server): {generator.generate_worker_id(server_name='server01')}")
    print(f"Standalone ID (custom): {generator.generate_standalone_id(suffix='stand-01')}")
    
    print("\n建议的Master节点:")
    suggestions = generator.suggest_device_names('master', 3)
    for device_id, device_name in suggestions.items():
        print(f"  {device_id} -> {device_name}")