@echo off
setlocal

echo ============================================================
echo HOMEWORK 3 - Error Handling: Retry Topics, DLT, Backoff
echo ============================================================
echo.

echo [1/5] Recreating topics and tables...
docker compose -p kafka-training run --rm --build app init
if errorlevel 1 exit /b %errorlevel%

echo.
echo Waiting 5s for partition leaders to stabilize...
ping 127.0.0.1 -n 6 >nul

echo.
echo [2/5] Running PRODUCER...
docker compose -p kafka-training run --rm app producer
if errorlevel 1 exit /b %errorlevel%

echo.
echo [3/5] Running MAIN CONSUMER (orders.events)...
echo       orderId=5 will fail and go to orders.retry.1 (attempt=1)
docker compose -p kafka-training run --rm app consumer-retry consumer-main consumer-retry-group orders.events
if errorlevel 1 exit /b %errorlevel%

echo.
echo [4/5] Running RETRY-1 CONSUMER (orders.retry.1)...
echo       orderId=5 will fail again and go to orders.retry.2 (attempt=2)
docker compose -p kafka-training run --rm app consumer-retry consumer-retry-1 consumer-retry-group orders.retry.1
if errorlevel 1 exit /b %errorlevel%

echo.
echo [5/5] Running RETRY-2 CONSUMER (orders.retry.2)...
echo       orderId=5 will fail and go to orders.dlt (attempt=3)
docker compose -p kafka-training run --rm app consumer-retry consumer-retry-2 consumer-retry-group orders.retry.2
if errorlevel 1 exit /b %errorlevel%

echo.
echo ============================================================
echo HOMEWORK 3 COMPLETED!
echo ============================================================
echo.
echo Check DLT:
echo   docker exec -it kafka-training-broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic orders.dlt --from-beginning --property print.key=true --max-messages 10

exit /b %errorlevel%