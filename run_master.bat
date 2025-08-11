@echo off

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"

call "%SCRIPT_DIR%run.bat" distribute master %*