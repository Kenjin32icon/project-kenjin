@echo off
setlocal EnableDelayedExpansion

echo =========================================================================
echo   PROJECT KENJIN - Windows Startup Sequence
echo =========================================================================

:: 1. Check for Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not in PATH. Please install Python 3.10+ from python.org.
    pause
    exit /b
)

:: 2. Check for Node.js (npm)
npm --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Node.js is not installed or not in PATH. Please install Node.js from nodejs.org.
    pause
    exit /b
)

:: 3. Check for .env file
if not exist ".env" (
    echo [ERROR] .env file is missing in the root directory! 
    echo Please copy your database and API credentials into a .env file before starting.
    pause
    exit /b
)

:: 4. Setup Python Virtual Environment
set VENV_PATH=orchestrator\.venv
if not exist "%VENV_PATH%" (
    echo ==^> Creating Python virtual environment...
    python -m venv "%VENV_PATH%"
    echo ==^> Installing Python dependencies...
    call "%VENV_PATH%\Scripts\activate.bat"
    python -m pip install --upgrade pip
    if exist "orchestrator\requirements.txt" (
        pip install -r orchestrator\requirements.txt
    )
) else (
    call "%VENV_PATH%\Scripts\activate.bat"
)

:: 5. Install Electron Dependencies
echo ==^> Checking UI dependencies...
cd electron-app
if not exist "node_modules" (
    echo ==^> Installing Electron packages...
    call npm install
)
cd ..

:: 6. Launch Services
echo ==^> Launching FBS MetaTrader 5...
:: Note: Adjust this path if FBS MT5 is installed elsewhere on the target machine
start "" "C:\Program Files\FBS MetaTrader 5\terminal64.exe"

echo ==^> Starting Kenjin Orchestrator API...
start "Kenjin Orchestrator (FastAPI)" cmd /c "call %VENV_PATH%\Scripts\activate.bat && uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --env-file .env"

:: Wait a few seconds for the API to boot
timeout /t 5 /nobreak >nul

echo ==^> Launching Electron Frontend App...
cd electron-app
start "Kenjin UI" cmd /c "npm start"
cd ..

echo =========================================================================
echo   Project Kenjin is live!
echo   - Keep the command prompt windows open while running.
echo   - Close the command prompt windows to stop the system.
echo =========================================================================
pause