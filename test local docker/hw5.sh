#!/bin/bash
# hw5.sh - запуск ДЗ №5 (Transactional Outbox + Failure Simulation)

echo "============================================================"
echo "HOMEWORK 5 - Transactional Outbox + Failure Simulation"
echo "============================================================"
echo

echo "[1/4] Recreating topics and tables..."
docker compose -p kafka-training run --rm --build app init
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "Waiting 3s for partition leaders to stabilize..."
sleep 3

echo
echo "[2/4] STEP 1 - Outbox + SIMULATED FAILURE"
echo "      Save order + outbox in one DB transaction,"
echo "      then fail to send to Kafka (broker unavailable)."
docker compose -p kafka-training run --rm app outbox-fail
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "[3/4] STEP 2 - RETRY from outbox (after 'recovery')"
echo "      Read pending events from outbox, send to Kafka,"
echo "      mark published=true."
docker compose -p kafka-training run --rm app outbox-relay
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "[4/4] VERIFY DB state:"
docker exec -it kafka-training-postgres psql -U demo -d kafkademo -c "select id, aggregate_id, event_type, status, published from outbox order by created_at;"
if [ $? -ne 0 ]; then exit 1; fi

echo
echo "============================================================"
echo "HOMEWORK 5 COMPLETED!"
echo "============================================================"
echo
echo "Expected:"
echo "  - outbox row with published=true (событие доставлено)"
echo "  - Kafka orders.events содержит OrderCreated для hw5-order-fail-1"