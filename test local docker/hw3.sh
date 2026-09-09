#!/bin/bash
# hw3.sh - скрипт для запуска ДЗ №3 в Git Bash

echo "============================================================"
echo "HOMEWORK 3 - Error Handling: Retry Topics, DLT, Backoff"
echo "============================================================"
echo

echo "[1/5] Recreating topics and tables..."
docker compose -p kafka-training run --rm --build app init
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "Waiting 5s for partition leaders to stabilize..."
sleep 5

echo
echo "[2/5] Running PRODUCER..."
docker compose -p kafka-training run --rm app producer
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "[3/5] Running MAIN CONSUMER (orders.events)..."
echo "      orderId=5 will fail and go to orders.retry.1 (attempt=1)"
docker compose -p kafka-training run --rm app consumer-retry consumer-main consumer-retry-group orders.events
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "[4/5] Running RETRY-1 CONSUMER (orders.retry.1)..."
echo "      orderId=5 will fail again and go to orders.retry.2 (attempt=2)"
docker compose -p kafka-training run --rm app consumer-retry consumer-retry-1 consumer-retry-group orders.retry.1
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "[5/5] Running RETRY-2 CONSUMER (orders.retry.2)..."
echo "      orderId=5 will fail and go to orders.dlt (attempt=3)"
docker compose -p kafka-training run --rm app consumer-retry consumer-retry-2 consumer-retry-group orders.retry.2
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "============================================================"
echo "HOMEWORK 3 COMPLETED!"
echo "============================================================"
echo
echo "Check DLT:"
echo "  docker exec -it kafka-training-broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic orders.dlt --from-beginning --property print.key=true --max-messages 10"