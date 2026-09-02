@echo off
setlocal
echo Building Java app and starting Kafka/PostgreSQL if needed...
docker compose -p kafka-training run --rm --build app init
exit /b %errorlevel%
