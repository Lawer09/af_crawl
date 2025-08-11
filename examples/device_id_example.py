#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Device ID 自动生成功能使用示例

演示如何在不同场景下使用device_id自动生成功能：
1. CLI命令行使用
2. 配置文件生成
3. 环境变量配置
4. 编程方式使用
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.device_id_generator import (
    generate_device_id, generate_master_id, generate_worker_id, 
    generate_standalone_id, validate_device_id
)
from config.distribution_config import (
    DistributionConfig, create_config_template, DistributionMode
)
from cli.distribution_cli import DistributionCLI

def example_1_basic_generation():
    """示例1: 基本的device_id生成"""
    print("=== 示例1: 基本device_id生成 ===")
    
    # 为不同模式生成device_id
    master_id = generate_device_id('master')
    worker_id = generate_device_id('worker')
    standalone_id = generate_device_id('standalone')
    
    print(f"Master device_id: {master_id}")
    print(f"Worker device_id: {worker_id}")
    print(f"Standalone device_id: {standalone_id}")
    print()

def example_2_advanced_generation():
    """示例2: 高级device_id生成（带数据中心和后缀）"""
    print("=== 示例2: 高级device_id生成 ===")
    
    # 生成带数据中心的device_id
    master_dc = generate_master_id(datacenter='dc1')
    worker_dc = generate_worker_id(datacenter='dc2')
    
    print(f"Master (数据中心DC1): {master_dc}")
    print(f"Worker (数据中心DC2): {worker_dc}")
    
    # 生成带自定义后缀的device_id
    master_prod = generate_master_id(suffix='prod')
    worker_server = generate_worker_id(server_name='web01')
    standalone_dev = generate_standalone_id(suffix='dev')
    
    print(f"Master (生产环境): {master_prod}")
    print(f"Worker (Web服务器): {worker_server}")
    print(f"Standalone (开发环境): {standalone_dev}")
    print()

def example_3_config_template():
    """示例3: 配置模板自动生成"""
    print("=== 示例3: 配置模板自动生成 ===")
    
    # 不提供device_id，自动生成
    master_config = create_config_template('master')
    print(f"Master配置:")
    print(f"  device_id: {master_config['device_id']}")
    print(f"  device_name: {master_config['device_name']}")
    print(f"  device_type: {master_config['device_type']}")
    
    # 提供自定义device_id
    worker_config = create_config_template('worker', 
                                          device_id='my-worker-001',
                                          device_name='My Worker Node')
    print(f"\nWorker配置（自定义）:")
    print(f"  device_id: {worker_config['device_id']}")
    print(f"  device_name: {worker_config['device_name']}")
    print(f"  device_type: {worker_config['device_type']}")
    print()

def example_4_environment_config():
    """示例4: 环境变量配置"""
    print("=== 示例4: 环境变量配置 ===")
    
    # 设置环境变量（模拟）
    original_mode = os.environ.get('DISTRIBUTION_MODE')
    original_device_id = os.environ.get('DEVICE_ID')
    
    try:
        # 只设置模式，不设置device_id
        os.environ['DISTRIBUTION_MODE'] = 'worker'
        if 'DEVICE_ID' in os.environ:
            del os.environ['DEVICE_ID']
        
        config = DistributionConfig.from_env()
        print(f"从环境变量创建的配置:")
        print(f"  mode: {config.mode}")
        print(f"  device_id: {config.device_id} (自动生成)")
        print(f"  device_name: {config.device_name}")
        
        # 设置自定义device_id
        os.environ['DEVICE_ID'] = 'env-worker-123'
        config_custom = DistributionConfig.from_env()
        print(f"\n使用自定义device_id:")
        print(f"  device_id: {config_custom.device_id} (来自环境变量)")
        
    finally:
        # 恢复原始环境变量
        if original_mode:
            os.environ['DISTRIBUTION_MODE'] = original_mode
        elif 'DISTRIBUTION_MODE' in os.environ:
            del os.environ['DISTRIBUTION_MODE']
            
        if original_device_id:
            os.environ['DEVICE_ID'] = original_device_id
        elif 'DEVICE_ID' in os.environ:
            del os.environ['DEVICE_ID']
    
    print()

def example_5_cli_integration():
    """示例5: CLI集成使用"""
    print("=== 示例5: CLI集成使用 ===")
    
    cli = DistributionCLI()
    
    # 自动生成device_id
    device_id = cli._ensure_device_id(None, 'master')
    device_name = cli._generate_device_name(device_id, 'master')
    
    print(f"CLI自动生成:")
    print(f"  device_id: {device_id}")
    print(f"  device_name: {device_name}")
    
    # 验证现有device_id
    valid_id = cli._ensure_device_id('my-master-001', 'master')
    print(f"\n验证有效device_id: my-master-001 -> {valid_id}")
    
    # 修正无效device_id
    invalid_id = 'invalid@device#id'
    corrected_id = cli._ensure_device_id(invalid_id, 'master')
    print(f"修正无效device_id: {invalid_id} -> {corrected_id}")
    print()

def example_6_validation():
    """示例6: device_id验证"""
    print("=== 示例6: device_id验证 ===")
    
    test_ids = [
        'master-001',           # 有效
        'worker-dc1-server01',  # 有效
        'standalone-dev',       # 有效
        'invalid@id',           # 无效：包含特殊字符
        'toolongdeviceidname',  # 可能无效：太长
        'master',               # 可能无效：太短
        'MASTER-001',           # 有效：大写
    ]
    
    for device_id in test_ids:
        is_valid = validate_device_id(device_id)
        status = "✅ 有效" if is_valid else "❌ 无效"
        print(f"{device_id:<25} {status}")
    
    print()

def main():
    """主函数"""
    print("Device ID 自动生成功能使用示例")
    print("=" * 50)
    
    try:
        example_1_basic_generation()
        example_2_advanced_generation()
        example_3_config_template()
        example_4_environment_config()
        example_5_cli_integration()
        example_6_validation()
        
        print("=" * 50)
        print("✅ 所有示例运行完成！")
        print()
        print("使用提示:")
        print("1. CLI命令现在支持可选的--device-id参数")
        print("2. 未提供device_id时会根据模式自动生成")
        print("3. 可以通过环境变量DEVICE_ID设置自定义值")
        print("4. 支持数据中心和自定义后缀的高级生成")
        print("5. 自动验证device_id格式并在无效时重新生成")
        
    except Exception as e:
        print(f"❌ 示例运行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())