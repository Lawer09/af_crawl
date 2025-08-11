<#
.SYNOPSIS
    AppsFlyer 爬虫系统 - 分布式工作节点启动脚本
    快捷启动工作节点模式

.DESCRIPTION
    这是一个快捷脚本，用于启动分布式工作节点模式
    所有参数将直接传递给主启动脚本

.EXAMPLE
    .\run_worker.ps1 --master-host localhost
    .\run_worker.ps1 --master-host 192.168.1.100 --device-name "工作节点1"
#>

param(
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$Arguments
)

# 获取脚本所在目录
$ScriptDir = $PSScriptRoot

# 调用主启动脚本
$mainScript = Join-Path $ScriptDir "run.ps1"
& $mainScript "distribute" "worker" @Arguments