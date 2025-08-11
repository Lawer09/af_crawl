#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试device_id自动生成功能
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

from utils.device_id_generator import (
    generate_device_id, validate_device_id, suggest_device_names,
    generate_master_id, generate_worker_id, generate_standalone_id
)
from config.distribution_config import (
    DistributionConfig, create_config_template, DistributionMode
)
from cli.distribution_cli import DistributionCLI

def test_device_id_generator():
    """测试device_id生成器"""
    print("=== 测试device_id生成器 ===")
    
    # 测试基本生成
    for mode in ['master', 'worker', 'standalone']:
        device_id = generate_device_id(mode)
        print(f"{mode.capitalize()} device_id: {device_id}")
        print(f"  验证结果: {validate_device_id(device_id)}")
        
        # 获取建议名称
        suggestions = suggest_device_names(mode, 1)
        if suggestions:
            first_suggestion = list(suggestions.items())[0]
            print(f"  建议名称: {first_suggestion[1]}")
        print()
    
    # 测试数据中心生成
    print("=== 测试数据中心device_id ===")
    master_dc_id = generate_master_id(datacenter='dc1')
    print(f"Master (DC1) device_id: {master_dc_id}")
    print(f"  验证结果: {validate_device_id(master_dc_id)}")
    
    worker_dc_id = generate_worker_id(datacenter='dc1')
    print(f"Worker (DC1) device_id: {worker_dc_id}")
    print(f"  验证结果: {validate_device_id(worker_dc_id)}")
    print()
    
    # 测试后缀生成
    print("=== 测试后缀device_id ===")
    master_suffix_id = generate_master_id(suffix='prod')
    print(f"Master (PROD) device_id: {master_suffix_id}")
    print(f"  验证结果: {validate_device_id(master_suffix_id)}")
    
    worker_suffix_id = generate_worker_id(server_name='prod')
    print(f"Worker (PROD) device_id: {worker_suffix_id}")
    print(f"  验证结果: {validate_device_id(worker_suffix_id)}")
    
    standalone_suffix_id = generate_standalone_id(suffix='prod')
    print(f"Standalone (PROD) device_id: {standalone_suffix_id}")
    print(f"  验证结果: {validate_device_id(standalone_suffix_id)}")
    print()

def test_config_template():
    """测试配置模板自动生成"""
    print("=== 测试配置模板自动生成 ===")
    
    for mode in ['master', 'worker', 'standalone']:
        print(f"\n--- {mode.upper()} 模式 ---")
        
        # 不提供device_id，应该自动生成
        template = create_config_template(mode)
        print(f"自动生成的device_id: {template['device_id']}")
        print(f"自动生成的device_name: {template['device_name']}")
        
        # 提供自定义device_id
        custom_device_id = f"custom-{mode}-001"
        template_custom = create_config_template(mode, device_id=custom_device_id)
        print(f"自定义device_id: {template_custom['device_id']}")
        print(f"自动生成的device_name: {template_custom['device_name']}")

def test_distribution_config():
    """测试DistributionConfig自动生成"""
    print("=== 测试DistributionConfig自动生成 ===")
    
    # 测试from_env（模拟环境变量）
    import os
    
    for mode in ['master', 'worker', 'standalone']:
        print(f"\n--- {mode.upper()} 模式 ---")
        
        # 设置模式环境变量，不设置device_id
        os.environ['DISTRIBUTION_MODE'] = mode
        if 'DEVICE_ID' in os.environ:
            del os.environ['DEVICE_ID']
        
        config = DistributionConfig.from_env()
        print(f"模式: {config.mode}")
        print(f"自动生成的device_id: {config.device_id}")
        print(f"设备名称: {config.device_name}")
        print(f"设备类型: {config.device_type}")
    
    # 清理环境变量
    if 'DISTRIBUTION_MODE' in os.environ:
        del os.environ['DISTRIBUTION_MODE']

def test_cli_integration():
    """测试CLI集成"""
    print("=== 测试CLI集成 ===")
    
    cli = DistributionCLI()
    
    for mode in ['master', 'worker', 'standalone']:
        print(f"\n--- {mode.upper()} 模式 ---")
        
        # 测试_ensure_device_id方法
        device_id = cli._ensure_device_id(None, mode)
        print(f"自动生成的device_id: {device_id}")
        
        # 测试_generate_device_name方法
        device_name = cli._generate_device_name(device_id, mode)
        print(f"生成的device_name: {device_name}")
        
        # 测试验证现有device_id
        existing_id = f"existing-{mode}-123"
        validated_id = cli._ensure_device_id(existing_id, mode)
        print(f"验证现有device_id: {existing_id} -> {validated_id}")
        
        # 测试无效device_id
        invalid_id = "invalid@id#123"
        corrected_id = cli._ensure_device_id(invalid_id, mode)
        print(f"修正无效device_id: {invalid_id} -> {corrected_id}")

def main():
    """主函数"""
    print("Device ID 自动生成功能测试")
    print("=" * 50)
    
    try:
        test_device_id_generator()
        print("\n" + "=" * 50)
        
        test_config_template()
        print("\n" + "=" * 50)
        
        test_distribution_config()
        print("\n" + "=" * 50)
        
        test_cli_integration()
        
        print("\n" + "=" * 50)
        print("✅ 所有测试完成！")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())