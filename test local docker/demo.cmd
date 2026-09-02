@echo off
setlocal
if "%~1"=="" (
  echo Usage: demo.cmd ^<mode^>
  echo Example: demo.cmd basic-entities
  exit /b 2
)

echo [1/2] Resetting demo topics and database...
docker compose -p kafka-training run --rm --build app init
if errorlevel 1 exit /b %errorlevel%

echo [2/2] Running demo: %~1
docker compose -p kafka-training run --rm app %~1
exit /b %errorlevel%
