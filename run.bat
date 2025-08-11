@echo off
REM AppsFlyer crawl system startup script (Windows version)
REM Supports traditional and distributed modes

setlocal enabledelayedexpansion

REM Configuration
set "REPO_DIR=%~dp0"
set "PYTHON_CMD=python"
set "MAIN_SCRIPT=main.py"
set "LOG_FILE=%REPO_DIR%run.log"

REM Get current timestamp
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "LOG_TS=%dt:~0,4%-%dt:~4,2%-%dt:~6,2% %dt:~8,2%:%dt:~10,2%:%dt:~12,2%"

echo [%LOG_TS%] === AppsFlyer Crawl System Startup Script ===

REM Change to script directory
cd /d "%REPO_DIR%" || (
    echo [%LOG_TS%] Cannot enter directory %REPO_DIR%
    exit /b 1
)

REM Display current system Python info
echo [%LOG_TS%] Checking Python environment...
where %PYTHON_CMD% >nul 2>&1 || (
    echo [%LOG_TS%] Error: Python not found, please ensure Python is installed and added to PATH
    pause
    exit /b 1
)

echo [%LOG_TS%] Current system Python path:
where %PYTHON_CMD%
echo [%LOG_TS%] Current system Python version:
%PYTHON_CMD% --version

REM Create virtual environment and install dependencies
if exist "requirements.txt" (
    if not exist "venv" (
        echo [%LOG_TS%] Creating virtual environment...
        %PYTHON_CMD% -m venv venv || (
            echo Virtual environment creation failed
            pause
            exit /b 1
        )
    )
    echo [%LOG_TS%] Activating virtual environment and installing dependencies...
    call venv\Scripts\activate.bat
    echo [%LOG_TS%] Current virtual environment Python version:
    python --version
    echo [%LOG_TS%] Installing dependencies...
    pip install -r requirements.txt || (
        echo Dependency installation failed
        pause
        exit /b 1
    )
) else (
    echo [%LOG_TS%] requirements.txt not found, skipping dependency installation
)

REM Parameter validation
if "%1"=="" (
    echo [%LOG_TS%] No command specified, entering interactive mode
    goto :interactive_mode
)

if "%1"=="-h" goto :show_help
if "%1"=="--help" goto :show_help
if "%1"=="help" goto :show_help

REM Check parameters
if "%1"=="sync_apps" (
    set "cmd=sync_apps"
    set "subcmd="
) else if "%1"=="sync_data" (
    set "cmd=sync_data"
    set "subcmd="
) else if "%1"=="web" (
    set "cmd=web"
    set "subcmd="
) else if "%1"=="distribute" (
    if "%2"=="" (
        echo [%LOG_TS%] Error: distribute command requires subcommand
        echo Available subcommands: master, worker, standalone, status
        exit /b 1
    )
    if "%2"=="master" (
        set "cmd=distribute"
        set "subcmd=master"
    ) else if "%2"=="worker" (
        set "cmd=distribute"
        set "subcmd=worker"
    ) else if "%2"=="standalone" (
        set "cmd=distribute"
        set "subcmd=standalone"
    ) else if "%2"=="status" (
        set "cmd=distribute"
        set "subcmd=status"
    ) else (
        echo [%LOG_TS%] Error: Invalid distribute subcommand: %2
        echo Available subcommands: master, worker, standalone, status
        exit /b 1
    )
) else (
    echo [%LOG_TS%] Error: Invalid command: %1
    echo Available commands: sync_apps, sync_data, web, distribute
    exit /b 1
)

REM Build Python command
set "python_command=%PYTHON_CMD% %MAIN_SCRIPT%"
if not "%cmd%"=="" set "python_command=%python_command% %cmd%"
if not "%subcmd%"=="" set "python_command=%python_command% %subcmd%"

REM Add remaining parameters
shift
if not "%subcmd%"=="" shift
:param_loop
if not "%1"=="" (
    set "python_command=%python_command% %1"
    shift
    goto :param_loop
)

REM Display command info
if "%cmd%"=="sync_apps" (
    echo [%LOG_TS%] Starting sync_apps mode - Synchronizing user applications
) else if "%cmd%"=="sync_data" (
    echo [%LOG_TS%] Starting sync_data mode - Synchronizing application data
) else if "%cmd%"=="web" (
    echo [%LOG_TS%] Starting web mode - Web interface
    echo [%LOG_TS%] Access address: http://localhost:8080
) else if "%subcmd%"=="master" (
    echo [%LOG_TS%] Starting distribute master mode - Distributed master node
) else if "%subcmd%"=="worker" (
    echo [%LOG_TS%] Starting distribute worker mode - Distributed worker node
) else if "%subcmd%"=="standalone" (
    echo [%LOG_TS%] Starting distribute standalone mode - Standalone distributed node
) else if "%subcmd%"=="status" (
    echo [%LOG_TS%] Checking distribute status - Distributed system status
)

REM Execute command and log output
echo [%LOG_TS%] Executing command: %python_command%
echo [%LOG_TS%] Command execution started >> "%LOG_FILE%"
%python_command% 2>&1 | (
    setlocal enabledelayedexpansion
    for /f "delims=" %%i in ('findstr ".*"') do (
        REM Get current timestamp and add to each line
        for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
        set "LOG_TS=!dt:~0,4!-!dt:~4,2!-!dt:~6,2! !dt:~8,2!:!dt:~10,2!:!dt:~12,2!"
        echo [!LOG_TS!] %%i
        echo [!LOG_TS!] %%i >> "%LOG_FILE%"
    )
)

set "exit_code=%ERRORLEVEL%"
echo [%LOG_TS%] Command execution completed
echo [%LOG_TS%] Exit code: %exit_code%
echo [%LOG_TS%] Check results in log file: %LOG_FILE%
if %exit_code% neq 0 (
    echo [%LOG_TS%] Command execution failed, please check log for details
    pause
)
exit /b %exit_code%

:interactive_mode
echo Please enter the command to execute, or use -h to view help:
echo Available commands: sync_apps, sync_data, web, distribute
echo.
set /p "user_input=Please enter command: "
if "%user_input%"=="" (
    echo No command entered, exiting
    exit /b 0
)
if "%user_input%"=="-h" goto :show_help
if "%user_input%"=="--help" goto :show_help
if "%user_input%"=="help" goto :show_help
if "%user_input%"=="exit" exit /b 0
if "%user_input%"=="quit" exit /b 0

REM Parse user input and re-execute script
echo [%LOG_TS%] Executing user command: %user_input%
call "%~f0" %user_input%
exit /b %ERRORLEVEL%

:show_help
echo.
echo AppsFlyer Crawl System Startup Script
echo.
echo Usage:
echo   %~nx0 [COMMAND] [OPTIONS]
echo.
echo Traditional Commands:
echo   sync_apps          Synchronize user applications
echo   sync_data          Synchronize application data
echo   web                Start web interface (http://localhost:8080)
echo.
echo Distributed Commands:
echo   distribute master [OPTIONS]     Start distributed master node
echo   distribute worker [OPTIONS]     Start distributed worker node
echo   distribute standalone [OPTIONS] Start standalone distributed node
echo   distribute status [OPTIONS]     Check distributed system status
echo.
echo Distributed Master Options:
echo   --port PORT                     Master service port (default: 7989)
echo   --device-name NAME              Device name
echo   --config CONFIG_FILE            Configuration file path
echo.
echo Distributed Worker Options:
echo   --master-host HOST              Master node host address (required)
echo   --master-port PORT              Master node port (default: 7989)
echo   --device-name NAME              Device name
echo   --config CONFIG_FILE            Configuration file path
echo.
echo Distributed Standalone Options:
echo   --device-name NAME              Device name
echo   --config CONFIG_FILE            Configuration file path
echo.
echo General Options:
echo   -h, --help, help               Show this help message
echo.
echo Examples:
echo   %~nx0 sync_apps
echo   %~nx0 web
echo   %~nx0 distribute master --port 7989
echo   %~nx0 distribute worker --master-host localhost
echo   %~nx0 distribute standalone
echo   %~nx0 distribute status
echo.
exit /b 0