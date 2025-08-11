@echo off
REM AppsFlyer 爬虫系统 - 分布式主节点启动脚本
REM 快捷启动主节点模式

setlocal enabledelayedexpansion

REM 获取脚本所在目录
set "SCRIPT_DIR=%~dp0"

REM 调用主启动脚本
call "%SCRIPT_DIR%run.bat" distribute master %*