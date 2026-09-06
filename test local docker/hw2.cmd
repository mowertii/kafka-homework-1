@echo off
setlocal

echo [1/3] Resetting demo topics and database (ONE TIME, before producer/consumer)...
docker compose -p kafka-training run --rm --build app init
if errorlevel 1 exit /b %errorlevel%

echo.
echo Waiting 5s for partition leaders to stabilize after topic recreation...
timeout /t 5 /nobreak >nul

echo.
echo [2/3] Running PRODUCER-SAFE (acks=all, retries, idempotence)...
docker compose -p kafka-training run --rm app producer-safe
if errorlevel 1 exit /b %errorlevel%

echo.
echo [3/3] Running CONSUMER-SAFE (manual commit after processing)...
docker compose -p kafka-training run --rm app consumer-safe consumer-safe-1 consumer-safe-group
exit /b %errorlevel%
