@echo off
rem Runs the Slurm scheduler web service with an auto-restart loop.
rem The loop is the recovery path for the scheduler watchdog's os._exit as
rem well as for unexpected crashes.
cd /d "%~dp0.."
if not defined SLURM_SCHEDULER_CONFIG set SLURM_SCHEDULER_CONFIG=config/app.yaml
if not exist logs mkdir logs
:loop
rem Guard against duplicate instances: if another process already owns
rem 0.0.0.0:8000, wait instead of entering a bind-failure crash loop.
rem (Match 0.0.0.0:8000 exactly -- Tailscale also listens on :8000 on its
rem own 100.x/IPv6 addresses and must not trip this guard.)
netstat -an | findstr /c:"0.0.0.0:8000 " | findstr LISTENING >nul
if %errorlevel%==0 (
  echo [%date% %time%] start_web.cmd: port 8000 already owned by another instance; waiting 30s >> logs\web.log
  timeout /t 30 /nobreak >nul
  goto loop
)
echo [%date% %time%] start_web.cmd: starting slurm_scheduler >> logs\web.log
.venv\Scripts\python.exe -m slurm_scheduler >> logs\web.log 2>&1
echo [%date% %time%] start_web.cmd: exited with code %errorlevel%; restarting in 5s >> logs\web.log
timeout /t 5 /nobreak >nul
goto loop
