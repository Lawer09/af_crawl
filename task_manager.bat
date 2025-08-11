@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo.
echo ========================================
echo    AF爬虫任务管理系统
echo ========================================
echo.
python scripts\task_manager.py
pause