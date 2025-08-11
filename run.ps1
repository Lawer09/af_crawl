<#
.SYNOPSIS
    AppsFlyer 爬虫系统启动脚本 (PowerShell版本)
    支持传统模式和分布式模式

.DESCRIPTION
    这个脚本用于启动AppsFlyer爬虫系统的各种模式：
    - 传统模式：sync_apps, sync_data, web
    - 分布式模式：distribute master/worker/standalone/status

.PARAMETER Command
    要执行的命令

.PARAMETER SubCommand
    分布式模式的子命令

.PARAMETER Arguments
    传递给Python脚本的其他参数

.EXAMPLE
    .\run.ps1 sync_apps
    .\run.ps1 sync_data --days 7
    .\run.ps1 web
    .\run.ps1 distribute master --device-id master-001
    .\run.ps1 distribute worker --master-host localhost
    .\run.ps1 distribute standalone --concurrent-tasks 3
#>

param(
    [Parameter(Position=0)]
    [string]$Command,
    
    [Parameter(Position=1)]
    [string]$SubCommand,
    
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$Arguments
)

# 配置项
$RepoDir = $PSScriptRoot
$PythonCmd = "python"
$MainScript = "main.py"
$LogFile = Join-Path $RepoDir "run.log"

# 获取时间戳函数
function Get-Timestamp {
    return Get-Date -Format "yyyy-MM-dd HH:mm:ss"
}

# 写入日志函数
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Timestamp
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Host $logMessage
    Add-Content -Path $LogFile -Value $logMessage
}

# 显示帮助信息
function Show-Help {
    Write-Host "使用方法: .\run.ps1 <命令> [参数...]"
    Write-Host ""
    Write-Host "支持的命令:"
    Write-Host "  传统模式:"
    Write-Host "    sync_apps                    - 同步用户 App 列表"
    Write-Host "    sync_data [--days N]         - 同步最近 N 天数据（默认1天）"
    Write-Host "    web                          - 启动Web管理界面"
    Write-Host ""
    Write-Host "  分布式模式:"
    Write-Host "    distribute master [选项]     - 启动分布式主节点"
    Write-Host "    distribute worker [选项]     - 启动分布式工作节点"
    Write-Host "    distribute standalone [选项] - 启动独立节点"
    Write-Host "    distribute status [选项]     - 查看系统状态"
    Write-Host ""
    Write-Host "  分布式选项:"
    Write-Host "    --device-id ID              - 设备ID（可选，未提供时自动生成）"
    Write-Host "    --device-name NAME          - 设备名称"
    Write-Host "    --host HOST                 - 监听地址（master模式，默认localhost）"
    Write-Host "    --port PORT                 - 监听端口（默认7989）"
    Write-Host "    --master-host HOST          - 主节点地址（worker模式，必需）"
    Write-Host "    --master-port PORT          - 主节点端口（worker模式，默认7989）"
    Write-Host "    --dispatch-interval N       - 任务分发间隔秒数（standalone模式，默认10）"
    Write-Host "    --concurrent-tasks N        - 并发任务数（standalone模式，默认5）"
    Write-Host "    --enable-monitoring         - 启用性能监控（standalone模式）"
    Write-Host "    --config FILE               - 配置文件路径"
    Write-Host ""
    Write-Host "示例:"
    Write-Host "  .\run.ps1 sync_apps"
    Write-Host "  .\run.ps1 sync_data --days 7"
    Write-Host "  .\run.ps1 web"
    Write-Host "  .\run.ps1 distribute master --device-id master-001 --port 7989"
    Write-Host "  .\run.ps1 distribute worker --device-id worker-001 --master-host 192.168.1.100"
    Write-Host "  .\run.ps1 distribute standalone --concurrent-tasks 3 --enable-monitoring"
    Write-Host "  .\run.ps1 distribute status --master-host 192.168.1.100"
}

# 验证命令
function Test-Command {
    param([string]$Cmd, [string]$SubCmd)
    
    $validCommands = @("sync_apps", "sync_data", "web", "distribute")
    $validDistributeSubCommands = @("master", "worker", "standalone", "status")
    
    if ($Cmd -notin $validCommands) {
        Write-Host "错误: 无效的命令: $Cmd" -ForegroundColor Red
        Write-Host "有效命令: $($validCommands -join ', ')" -ForegroundColor Yellow
        return $false
    }
    
    if ($Cmd -eq "distribute") {
        if ($SubCmd -notin $validDistributeSubCommands) {
            Write-Host "错误: 无效的分布式子命令: $SubCmd" -ForegroundColor Red
            Write-Host "有效的分布式子命令: $($validDistributeSubCommands -join ', ')" -ForegroundColor Yellow
            return $false
        }
    }
    
    return $true
}

# 初始化环境
function Initialize-Environment {
    Write-Log "=== AppsFlyer 爬虫系统启动脚本 ==="
    
    # 切换到脚本目录
    Set-Location $RepoDir
    Write-Log "工作目录: $RepoDir"
    
    # 检查Python
    try {
        $pythonPath = (Get-Command $PythonCmd -ErrorAction Stop).Source
        $pythonVersion = & $PythonCmd --version 2>&1
        Write-Log "Python路径: $pythonPath"
        Write-Log "Python版本: $pythonVersion"
    }
    catch {
        Write-Log "错误: 未找到Python，请确保Python已安装并添加到PATH" "ERROR"
        exit 1
    }
    
    # 检查虚拟环境
    if (Test-Path "requirements.txt") {
        if (-not (Test-Path "venv")) {
            Write-Log "创建虚拟环境..."
            & $PythonCmd -m venv venv
            if ($LASTEXITCODE -ne 0) {
                Write-Log "虚拟环境创建失败" "ERROR"
                exit 1
            }
        }
        
        Write-Log "激活虚拟环境并安装依赖..."
        & ".\venv\Scripts\Activate.ps1"
        
        $venvPythonVersion = & python --version 2>&1
        Write-Log "虚拟环境Python版本: $venvPythonVersion"
        
        & python -m pip install --upgrade pip
        & python -m pip install -r requirements.txt
        
        if ($LASTEXITCODE -ne 0) {
            Write-Log "依赖安装失败" "WARN"
        }
    }
    else {
        Write-Log "未找到 requirements.txt，跳过依赖安装" "WARN"
    }
    
    # 检查主程序
    if (-not (Test-Path $MainScript)) {
        Write-Log "主Python脚本 $MainScript 不存在" "ERROR"
        exit 1
    }
}

# 执行命令
function Invoke-PythonCommand {
    param([string[]]$CmdArgs)
    
    # 构建Python命令
    $pythonCommand = @($MainScript) + $CmdArgs
    $commandString = "python " + ($pythonCommand -join " ")
    
    Write-Log "启动命令: $commandString"
    Write-Log "日志文件: $LogFile"
    Write-Log "==========================================="
    
    # 根据命令类型提供特定信息
    switch ($Command) {
        "sync_apps" {
            Write-Log "开始同步用户应用列表..."
        }
        "sync_data" {
            Write-Log "开始同步应用数据..."
        }
        "web" {
            Write-Log "启动Web管理界面..."
            Write-Log "访问地址: http://localhost:8080"
        }
        "distribute" {
            switch ($SubCommand) {
                "master" {
                    Write-Log "启动分布式主节点..."
                    Write-Log "主节点将负责任务调度和分发"
                }
                "worker" {
                    Write-Log "启动分布式工作节点..."
                    Write-Log "工作节点将连接到主节点执行任务"
                }
                "standalone" {
                    Write-Log "启动独立节点..."
                    Write-Log "独立节点集成了主节点和工作节点功能"
                }
                "status" {
                    Write-Log "查询系统状态..."
                }
            }
        }
    }
    
    # 执行Python命令
    try {
        $process = Start-Process -FilePath "python" -ArgumentList $pythonCommand -NoNewWindow -PassThru -RedirectStandardOutput "temp_output.txt" -RedirectStandardError "temp_error.txt"
        
        # 实时读取输出
        $outputJob = Start-Job -ScriptBlock {
            param($OutputFile, $LogFile)
            while ($true) {
                if (Test-Path $OutputFile) {
                    $content = Get-Content $OutputFile -Tail 10 -Wait
                    foreach ($line in $content) {
                        if ($line) {
                            $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                            $logMessage = "[$timestamp] $line"
                            Write-Output $logMessage
                            Add-Content -Path $LogFile -Value $logMessage
                        }
                    }
                }
                Start-Sleep -Milliseconds 100
            }
        } -ArgumentList "temp_output.txt", $LogFile
        
        # 等待进程完成
        $process.WaitForExit()
        $exitCode = $process.ExitCode
        
        # 停止输出监控作业
        Stop-Job $outputJob -Force
        Remove-Job $outputJob -Force
        
        # 清理临时文件
        if (Test-Path "temp_output.txt") { Remove-Item "temp_output.txt" -Force }
        if (Test-Path "temp_error.txt") { Remove-Item "temp_error.txt" -Force }
        
        if ($exitCode -eq 0) {
            Write-Log "命令执行完成"
        }
        else {
            Write-Log "命令执行失败，退出码: $exitCode" "ERROR"
        }
        
        return $exitCode
    }
    catch {
        Write-Log "执行命令时发生错误: $($_.Exception.Message)" "ERROR"
        return 1
    }
}

# 交互模式
function Start-InteractiveMode {
    Write-Host "请输入要执行的命令，或使用 -Help 查看帮助:" -ForegroundColor Cyan
    Write-Host "可用命令: sync_apps, sync_data, web, distribute" -ForegroundColor Yellow
    Write-Host ""
    
    $userInput = Read-Host "请输入命令"
    
    if ([string]::IsNullOrWhiteSpace($userInput)) {
        Write-Host "未输入命令，退出" -ForegroundColor Red
        exit 1
    }
    
    # 解析用户输入
    $inputArgs = $userInput -split ' '
    $script:Command = $inputArgs[0]
    
    if ($inputArgs.Length -gt 1) {
        $script:SubCommand = $inputArgs[1]
        $script:Arguments = $inputArgs[2..($inputArgs.Length-1)]
    }
}

# 主函数
function Main {
    # 检查帮助参数
    if ($Command -in @("-Help", "--help", "-h", "/?"))) {
        Show-Help
        return 0
    }
    
    # 如果没有提供命令，进入交互模式
    if ([string]::IsNullOrWhiteSpace($Command)) {
        Start-InteractiveMode
    }
    
    # 验证命令
    if (-not (Test-Command $Command $SubCommand)) {
        Write-Host ""
        Write-Host "使用 .\run.ps1 -Help 查看完整帮助信息" -ForegroundColor Cyan
        return 1
    }
    
    # 初始化环境
    Initialize-Environment
    
    # 构建命令参数
    $cmdArgs = @()
    if ($Command) { $cmdArgs += $Command }
    if ($SubCommand) { $cmdArgs += $SubCommand }
    if ($Arguments) { $cmdArgs += $Arguments }
    
    # 执行命令
    $exitCode = Invoke-PythonCommand $cmdArgs
    
    return $exitCode
}

# 执行主函数
try {
    $exitCode = Main
    exit $exitCode
}
catch {
    Write-Host "脚本执行过程中发生未处理的错误: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "错误详情: $($_.Exception.StackTrace)" -ForegroundColor Red
    exit 1
}