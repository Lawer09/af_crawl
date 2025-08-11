<#
.SYNOPSIS
    AppsFlyer 爬虫系统 - 独立节点启动脚本
    快捷启动独立节点模式

.DESCRIPTION
    这是一个快捷脚本，用于启动独立节点模式
    独立节点集成了主节点和工作节点的功能
    所有参数将直接传递给主启动脚本

.EXAMPLE
    .\run_alone.ps1
    .\run_alone.ps1 --concurrent-tasks 3 --enable-monitoring
#>

param(
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$Arguments
)

# 获取脚本所在目录
$ScriptDir = $PSScriptRoot

# 调用主启动脚本
$mainScript = Join-Path $ScriptDir "run.ps1"
& $mainScript "distribute" "standalone" @Arguments