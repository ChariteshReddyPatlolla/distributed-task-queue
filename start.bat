@echo off
setlocal enabledelayedexpansion

echo =====================================================================
echo  High-Concurrency Distributed Task Queue ^& Rate-Limited API Gateway
echo =====================================================================
echo.

:: 1. Check for .env file
if not exist "%~dp0.env" (
    echo [*] .env not found. Copying from .env.example...
    copy "%~dp0.env.example" "%~dp0.env" >nul
    echo [OK] Created .env configuration.
) else (
    echo [OK] .env configuration found.
)

:: 2. Detect Docker environment (Native Windows vs WSL 2)
echo [*] Detecting Docker engine...
docker info >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    set RUN_MODE=NATIVE
    echo [OK] Docker engine detected natively on Windows.
    goto DOCKER_FOUND
)

wsl -d Ubuntu -e docker info >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    set RUN_MODE=WSL
    echo [OK] Docker engine detected running inside WSL Ubuntu.
    goto DOCKER_FOUND
)

echo [ERROR] Docker is not running!
echo Please start Docker Desktop or start dockerd inside WSL, then re-run this script.
pause
exit /b 1

:DOCKER_FOUND

:: 3. Boot cluster with 3 workers
echo.
echo [*] Launching cluster (PostgreSQL, Redis, Gateway, Dispatcher, 3 Workers)...
if "%RUN_MODE%"=="NATIVE" goto RUN_NATIVE
goto RUN_WSL

:RUN_NATIVE
docker compose up -d --scale worker=3
goto POST_LAUNCH

:RUN_WSL
wsl -d Ubuntu -e bash -c "cd \"$(wslpath '%~dp0')\" && docker compose up -d --scale worker=3"
goto POST_LAUNCH

:POST_LAUNCH
if !ERRORLEVEL! NEQ 0 (
    echo [ERROR] Failed to start cluster containers.
    pause
    exit /b 1
)

:: 4. Wait for Gateway readiness
echo.
echo [*] Waiting for API Gateway to become healthy...
set RETRIES=15
:WAIT_LOOP
ping 127.0.0.1 -n 3 >nul
curl -s -f http://localhost:8000/health >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    goto READY
)
set /a RETRIES-=1
if !RETRIES! GTR 0 (
    goto WAIT_LOOP
)

:READY
echo [OK] Cluster is fully operational!
echo.
echo =====================================================================
echo  Service Endpoints ^& Dashboard
echo =====================================================================
echo  - API Gateway:        http://localhost:8000
echo  - Interactive Docs:   http://localhost:8000/docs
echo  - Health Endpoint:    http://localhost:8000/health
echo  - Prometheus Metrics: http://localhost:8000/metrics
echo.
echo  Default API Keys for Testing:
echo  - dev-key-1
echo  - bench-key-1
echo.
echo  Useful Management Commands:
echo  - View logs:          docker compose logs -f
echo  - Scale workers:      docker compose up -d --scale worker=5
echo  - Run tests:          pytest tests/ -v
echo  - Stop cluster:       docker compose down
echo =====================================================================
echo.

:: Test health endpoint output
echo [*] Current cluster health status:
curl -s http://localhost:8000/health
echo.
echo.
echo Press any key to close this launcher...
pause >nul
