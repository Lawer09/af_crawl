@echo off
REM AppsFlyer crawl system - Distributed worker node startup script
REM Quick start worker node mode

setlocal enabledelayedexpansion

REM Get script directory
set "SCRIPT_DIR=%~dp0"

REM Call main startup script
call "%SCRIPT_DIR%run.bat" distribute worker %*