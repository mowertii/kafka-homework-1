#!/bin/bash
# hw4.sh - запуск ДЗ №4 (Idempotent Consumer + Inbox)

echo "============================================================"
echo "HOMEWORK 4 - Idempotent Consumer + Inbox Pattern"
echo "============================================================"
echo

echo "[1/3] Recreating topics and tables..."
docker compose -p kafka-training run --rm --build app init
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "Waiting 3s for partition leaders to stabilize..."
sleep 3

echo
echo "[2/3] Running PRODUCER-DUP (5 unique + 1 duplicated x3)..."
docker compose -p kafka-training run --rm app producer-dup
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "[3/3] Running CONSUMER-IDEMPOTENT (inbox pattern)..."
echo "      Duplicates will be detected by eventId and skipped."
docker compose -p kafka-training run --rm app consumer-idempotent consumer-idempotent-1 consumer-idempotent-group
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "============================================================"
echo "HOMEWORK 4 COMPLETED!"
echo "============================================================"
echo
echo "Verify DB state:"
echo "  docker exec -it kafka-training-postgres psql -U demo -d kafkademo -c 'select count(*) from hw4_processed_orders;'"
echo "  docker exec -it kafka-training-postgres psql -U demo -d kafkademo -c 'select count(*) from inbox;'"
echo
echo "Expected:"
echo "  hw4_processed_orders = 6 rows (5 unique + 1 duplicated order)"
echo "  inbox                 = 6 rows (6 unique eventIds)"