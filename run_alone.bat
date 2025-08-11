@echo off
REM AppsFlyer 爬虫系统 - 独立节点启动脚本
REM 快捷启动独立节点模式（集成主节点和工作节点功能）

setlocal enabledelayedexpansion

REM 获取脚本所在目录
set "SCRIPT_DIR=%~dp0"

REM 调用主启动脚本
call "%SCRIPT_DIR%run.bat" distribute standalone %*