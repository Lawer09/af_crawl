@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo.
echo ========================================
echo    AF CRAWL TASK
echo ========================================
echo.
python scripts\task_manager.py
pause