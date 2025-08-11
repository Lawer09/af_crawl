<#
.SYNOPSIS
    AppsFlyer 爬虫系统 - 分布式主节点启动脚本
    快捷启动主节点模式

.DESCRIPTION
    这是一个快捷脚本，用于启动分布式主节点模式
    所有参数将直接传递给主启动脚本

.EXAMPLE
    .\run_master.ps1
    .\run_master.ps1 --device-id master-001 --port 7989
#>

param(
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$Arguments
)

# 获取脚本所在目录
$ScriptDir = $PSScriptRoot

# 调用主启动脚本
$mainScript = Join-Path $ScriptDir "run.ps1"
& $mainScript "distribute" "master" @Arguments