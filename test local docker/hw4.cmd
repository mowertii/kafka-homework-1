@echo off
setlocal

echo ============================================================
echo HOMEWORK 4 - Idempotent Consumer + Inbox Pattern
echo ============================================================
echo.

echo [1/3] Recreating topics and tables...
docker compose -p kafka-training run --rm --build app init
if errorlevel 1 exit /b %errorlevel%

echo.
echo Waiting 3s for partition leaders to stabilize...
ping 127.0.0.1 -n 4 >nul

echo.
echo [2/3] Running PRODUCER-DUP (5 unique + 1 duplicated x3)...
docker compose -p kafka-training run --rm app producer-dup
if errorlevel 1 exit /b %errorlevel%

echo.
echo [3/3] Running CONSUMER-IDEMPOTENT (inbox pattern)...
echo       Duplicates will be detected by eventId and skipped.
docker compose -p kafka-training run --rm app consumer-idempotent consumer-idempotent-1 consumer-idempotent-group
if errorlevel 1 exit /b %errorlevel%

echo.
echo ============================================================
echo HOMEWORK 4 COMPLETED!
echo ============================================================
echo.
echo Verify DB state:
echo   docker exec -it kafka-training-postgres psql -U demo -d kafkademo -c "select count(*) from hw4_processed_orders;"
echo   docker exec -it kafka-training-postgres psql -U demo -d kafkademo -c "select count(*) from inbox;"
echo.
echo Expected:
echo   hw4_processed_orders = 6 rows (5 unique + 1 duplicated order)
echo   inbox                 = 6 rows (6 unique eventIds)

exit /b %errorlevel%