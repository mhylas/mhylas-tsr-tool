@echo off
:: Check if port 3000 is already in use
netstat -ano | findstr ":3000 " >nul 2>&1
if %errorlevel%==0 (
    echo Server already running on port 3000.
) else (
    echo Starting TSR Dashboard server...
    start "" /min powershell.exe -ExecutionPolicy Bypass -File "%~dp0serve.ps1"
    timeout /t 2 /nobreak >nul
)
:: Open in Chrome (falls back to default browser)
start "" "http://localhost:3000/tsr-dashboard.html"
exit
