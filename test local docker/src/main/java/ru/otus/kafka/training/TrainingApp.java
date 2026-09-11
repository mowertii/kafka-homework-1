package ru.otus.kafka.training;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.header.Header;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TrainingApp {
    // ============================================================
    // 🎯 ДЗ №5: Флаг для имитации сбоя Kafka
    // ============================================================
    // Когда true — producer "падает" при отправке (эмулируем недоступность брокера).
    // Это позволяет проверить, что событие остаётся в outbox с published=false.
    // ============================================================
    private static final AtomicBoolean KAFKA_FAILURE_MODE = new AtomicBoolean(false);    
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String BOOTSTRAP = env("BOOTSTRAP_SERVERS", "localhost:9094");
    private static final String JDBC_URL = env("JDBC_URL", "jdbc:postgresql://localhost:5433/kafkademo");
    private static final String JDBC_USER = env("JDBC_USER", "demo");
    private static final String JDBC_PASSWORD = env("JDBC_PASSWORD", "demo");

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args[0].equals("help")) { help(); return; }
        log("bootstrap.servers=" + BOOTSTRAP);
        
        switch (args[0]) {
            case "init" -> init();
            case "basic-entities" -> basicEntities();
            case "partition-order" -> partitionOrder();
            case "consumer-groups" -> consumerGroups();
            case "event-styles" -> eventStyles();
            case "outbox" -> outbox();
            case "inbox" -> inbox();
            case "cqrs-saga" -> cqrsSaga();
            case "retry-dlt" -> retryDlt();
            case "contract" -> contractEvolution();
            case "bad-shared-group" -> badSharedGroup();
            case "good-separate-groups" -> goodSeparateGroups();
            case "bad-universal-topic" -> badUniversalTopic();
            case "audit-replay" -> auditReplay();
            case "producer" -> runProducer();            // Производитель
            case "consumer" -> runConsumer(args);        // Потребитель
            case "producer-safe" -> runProducerSafe();   // продюсер с настройками надежности
            case "consumer-safe" -> runConsumerSafe(args); // консьюмер с ручным коммитом 
            case "consumer-retry" -> runConsumerWithRetry(args); // консьюмер с повторным чтением
            case "producer-dup" -> runProducerWithDuplicates(); // ДЗ №4
            case "consumer-idempotent" -> runConsumerIdempotent(args); // ДЗ №4
            case "outbox-fail" -> runOutboxWithFailure();     // ДЗ №5: Outbox + сбой
            case "outbox-relay" -> runOutboxRelay();          // ДЗ №5: повторная отправка
            case "all" -> { /* ... */ }
            default -> { System.err.println("Unknown mode: " + args[0]); help(); System.exit(2); }
        }
    }

    private static void help() {
        System.out.println("Modes: init, basic-entities, partition-order, consumer-groups, event-styles, outbox, inbox, cqrs-saga, retry-dlt, contract, bad-shared-group, good-separate-groups, bad-universal-topic, audit-replay, producer, consumer, producer-safe, consumer-safe, producer-dup, consumer-idempotent, consumer-retry, outbox-fail, outbox-relay, all");
    }

    private static void init() throws Exception {
        log("Creating topics and database tables");
        createTopics(Map.ofEntries(
            Map.entry("orders.events", 3), Map.entry("orders.notifications", 3), Map.entry("orders.state", 3),
            Map.entry("orders.retry.1", 3), Map.entry("orders.retry.2", 3), Map.entry("orders.dlt", 3),
            Map.entry("all.events", 3), Map.entry("payments.events", 3), Map.entry("warehouse.events", 3),
            Map.entry("readmodel.orders", 3), Map.entry("audit.events", 3)
        ));
        try (Connection c = db()) {
            st(c, "create table if not exists orders(id varchar primary key, status varchar not null, amount int not null, updated_at timestamptz not null default now())");
            // ============================================================
            // 🎯 ДЗ №5: таблица outbox со статусом для безопасной relay-обработки
            // ============================================================
            // published  — историческое поле (оставлено для совместимости со сценариями ДЗ №3/№4)
            // status     — расширенный статус обработки:
            //   'pending'     — ждёт отправки (default)
            //   'processing'  — взято relay'ем в работу (защита от гонки между инстансами)
            //   'published'   — успешно отправлено в Kafka
            //   'failed'      — постоянная ошибка, требует ручного разбора
            // ============================================================
            st(c, "create table if not exists outbox(" +
                "id uuid primary key, " +
                "aggregate_id varchar not null, " +
                "event_type varchar not null, " +
                "payload text not null, " +
                "published boolean not null default false, " +
                "status varchar not null default 'pending', " +
                "created_at timestamptz not null default now())");
            st(c, "create table if not exists inbox(event_id uuid primary key, processed_at timestamptz not null default now())");
            st(c, "create table if not exists billing_payments(order_id varchar primary key, amount int not null, created_at timestamptz not null default now())");
            st(c, "create table if not exists order_projection(order_id varchar primary key, status varchar not null, amount int not null, last_event_id varchar not null, updated_at timestamptz not null default now())");
            st(c, "create table if not exists hw4_processed_orders(order_id varchar primary key, event_id uuid not null, amount int not null, processed_at timestamptz not null default now())");
            st(c, "truncate table orders, outbox, inbox, billing_payments, order_projection, hw4_processed_orders");
        }
        log("Ready. Topics and DB tables have been reset. Run any example mode.");
    }

    private static void basicEntities() throws Exception {
        banner("SLIDE 7 - broker/topic/partition/producer/consumer/group");
        String topic = "orders.events";
        try (KafkaProducer<String, String> p = producer()) {
            for (int i = 1; i <= 6; i++) {
                String key = "order-" + (i % 3);
                String value = event("OrderPlaced", key, Map.of("amount", 100 + i));
                RecordMetadata md = p.send(new ProducerRecord<>(topic, key, value)).get();
                log("PRODUCER wrote key=%s to %s-%d offset=%d".formatted(key, md.topic(), md.partition(), md.offset()));
            }
        }
        consumeFixed("architecture-demo", topic, 6, Duration.ofSeconds(8));
        log("Meaning: topic is a named log, partition is ordered shard, offset is position, group owns reading progress.");
    }

    private static void partitionOrder() throws Exception {
        banner("SLIDE 8 - key controls partition and per-entity ordering");
        String topic = "orders.events";
        try (KafkaProducer<String, String> p = producer()) {
            List<String> sequence = List.of("Created", "Paid", "Packed", "Shipped");
            for (String status : sequence) send(p, topic, "order-42", event("Order" + status, "order-42", Map.of("status", status)));
            for (String status : sequence) send(p, topic, null, event("Order" + status, "order-no-key", Map.of("status", status)));
        }
        log("Reading exact partitions to show where records landed:");
        printTopicPartitions(topic, Duration.ofSeconds(5));
        log("Meaning: order-42 stays in one partition, so Kafka can preserve its relative order. null key is not an ordering strategy.");
    }

    private static void consumerGroups() throws Exception {
        banner("SLIDE 9 - one group scales one business function; another group reads independently");
        try (KafkaProducer<String, String> p = producer()) {
            for (int i = 0; i < 9; i++) send(p, "orders.events", "order-" + i, event("OrderPaid", "order-" + i, Map.of("amount", 10 + i)));
        }
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch latch = new CountDownLatch(3);
        pool.submit(() -> consumerWorker("billing-service", "billing-1", 4, latch));
        pool.submit(() -> consumerWorker("billing-service", "billing-2", 4, latch));
        pool.submit(() -> consumerWorker("fraud-service", "fraud-1", 8, latch));
        latch.await(12, TimeUnit.SECONDS);
        pool.shutdownNow();
        log("Meaning: billing-1 and billing-2 share partitions. fraud-service is a separate business function and receives its own copy stream.");
    }

    private static void eventStyles() throws Exception {
        banner("SLIDE 12 - notification vs event-carried state transfer");
        try (KafkaProducer<String, String> p = producer()) {
            send(p, "orders.notifications", "order-777", event("OrderPaid", "order-777", Map.of("note", "only fact + id")));
            send(p, "orders.state", "customer-19", event("CustomerChanged", "customer-19", Map.of("name", "ACME", "segment", "B2B", "status", "ACTIVE", "version", 5)));
        }
        consumeFixed("notification-consumer", "orders.notifications", 1, Duration.ofSeconds(5));
        consumeFixed("state-transfer-consumer", "orders.state", 1, Duration.ofSeconds(5));
        log("Meaning: notification is small but may force a synchronous lookup; state transfer duplicates data but makes consumers autonomous.");
    }

    private static void outbox() throws Exception {
        banner("SLIDE 13 - Transactional Outbox removes dual-write hole");
        try (Connection c = db()) {
            st(c, "delete from outbox"); st(c, "delete from orders where id='order-outbox-1'");
            c.setAutoCommit(false); // НАЧИНАЕМ ТРАНЗАКЦИЮ
            // 1. Сохраняем заказ
            try (PreparedStatement order = c.prepareStatement("insert into orders(id,status,amount) values(?,?,?)");
                // 2. Сохраняем событие в outbox
                PreparedStatement out = c.prepareStatement("insert into outbox(id,aggregate_id,event_type,payload) values(?::uuid,?,?,?)")) {
                order.setString(1, "order-outbox-1"); order.setString(2, "PAID"); order.setInt(3, 1200); order.executeUpdate();
                String eventId = UUID.randomUUID().toString();
                out.setString(1, eventId); out.setString(2, "order-outbox-1"); out.setString(3, "OrderPaid");
                out.setString(4, eventWithId(eventId, "OrderPaid", "order-outbox-1", Map.of("amount", 1200)));
                out.executeUpdate();
                c.commit(); // КОММИТИМ ТРАНЗАКЦИЮ
                log("DB transaction committed: order row + outbox row exist together. Nothing has been sent to Kafka yet.");
            } catch (Exception e) { c.rollback(); throw e; }
        }
        relayOutboxOnce();
        consumeFixed("outbox-downstream", "orders.events", 1, Duration.ofSeconds(5));
        log("Meaning: the business transaction does not depend on broker availability; relay can repeat publish until success.");
    }

    /**
     * ============================================================
     * 🎯 ДЗ №5, ЗАДАНИЯ 1 и 3: Transactional Outbox + воспроизведение сбоя
     * ============================================================
     *
     * Что мы демонстрируем:
     *
     * 1. Бизнес-операция (создание заказа) и запись события в outbox
     *    выполняются в ОДНОЙ транзакции БД.
     *    → Если упадёт что-то одно, откатится всё целиком.
     *
     * 2. Первая попытка отправки в Kafka ИСКУССТВЕННО "проваливается".
     *    → Заказ остаётся в orders, событие — в outbox с published=false.
     *    → Kafka НЕ получил событие.
     *
     * 3. Это ключевая демонстрация проблемы dual-write:
     *    save() + producer.send() без Outbox → потеря события при сбое.
     *
     * Что мы получаем:
     * ✅ Доказательство, что бизнес-транзакция не зависит от доступности Kafka.
     * ✅ Событие НЕ потеряно, лежит в outbox и ждёт повтора.
     * ✅ Заказ создан (БД консистентна), но downstream ещё не знает о нём.
     * ============================================================
     */
    private static void runOutboxWithFailure() throws Exception {
        banner("ДЗ №5 / ШАГ 1 - Outbox + ВОСПРОИЗВЕДЕНИЕ СБОЯ отправки в Kafka");

        String orderId = "hw5-order-fail-1";
        String eventId = UUID.randomUUID().toString();

        // ------------------------------------------------------------
        // ШАГ 1.1: Транзакция в БД — заказ + событие в outbox
        // ------------------------------------------------------------
        try (Connection c = db()) {
            // Чистим предыдущие данные для повторного запуска
            st(c, "delete from outbox where aggregate_id='" + orderId + "'");
            st(c, "delete from orders where id='" + orderId + "'");

            c.setAutoCommit(false); // НАЧИНАЕМ ТРАНЗАКЦИЮ

            try (
                PreparedStatement orderPs = c.prepareStatement(
                    "insert into orders(id, status, amount) values (?, ?, ?)");
                PreparedStatement outboxPs = c.prepareStatement(
                    "insert into outbox(id, aggregate_id, event_type, payload, published) values (?::uuid, ?, ?, ?, false)")
            ) {
                // 1. Сохраняем заказ
                orderPs.setString(1, orderId);
                orderPs.setString(2, "CREATED");
                orderPs.setInt(3, 1500);
                orderPs.executeUpdate();
                log("📝 [БД] orders: id=%s status=CREATED amount=1500".formatted(orderId));

                // 2. Сохраняем событие в outbox (published=false)
                String payload = eventWithId(eventId, "OrderCreated", orderId, Map.of("amount", 1500));
                outboxPs.setString(1, eventId);
                outboxPs.setString(2, orderId);
                outboxPs.setString(3, "OrderCreated");
                outboxPs.setString(4, payload);
                outboxPs.executeUpdate();
                log("📝 [БД] outbox: id=%s eventType=OrderCreated published=false".formatted(eventId));

                c.commit(); // КОММИТИМ ТРАНЗАКЦИЮ
                log("✅ [БД] Транзакция закоммичена: order + outbox записаны АТОМАРНО. Kafka ещё не знает.");
            } catch (Exception e) {
                c.rollback();
                log("❌ [БД] Транзакция откачена: " + e.getMessage());
                throw e;
            }
        }

        // ------------------------------------------------------------
        // ШАГ 1.2: Попытка отправки в Kafka → ИСКУССТВЕННЫЙ СБОЙ
        // ------------------------------------------------------------
        log("");
        log("🔥 [СБОЙ] Включаем KAFKA_FAILURE_MODE=true (эмулируем недоступность брокера)");
        KAFKA_FAILURE_MODE.set(true);

        try {
            relayOutboxOnceWithFailure(); // ← метод, который упадёт
        } catch (Exception e) {
            log("❌ [KAFKA] Отправка ПРОВАЛИЛАСЬ: " + e.getMessage());
            log("   → Событие ОСТАЁТСЯ в outbox с published=false");
        } finally {
            KAFKA_FAILURE_MODE.set(false); // выключаем режим сбоя
            log("🔧 [СБОЙ] KAFKA_FAILURE_MODE=false (сбой больше не воспроизводится)");
        }

        // ------------------------------------------------------------
        // ШАГ 1.3: Проверяем состояние БД после сбоя
        // ------------------------------------------------------------
        log("");
        log("🔍 [ПРОВЕРКА] Состояние после сбоя:");
        try (Connection c = db()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "select id, status, amount from orders where id = ?")) {
                ps.setString(1, orderId);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    log("   orders:  id=%s status=%s amount=%d ✅ (заказ сохранён)"
                        .formatted(rs.getString(1), rs.getString(2), rs.getInt(3)));
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "select id, event_type, published, status from outbox where aggregate_id = ?")) {
                ps.setString(1, orderId);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    log("   outbox:  id=%s eventType=%s published=%s status=%s ⏳ (ждёт отправки)"
                        .formatted(rs.getString(1), rs.getString(2),
                                rs.getBoolean(3), rs.getString(4)));
                }
            }
        }

        // Проверяем, что Kafka не получила событие
        log("");
        log("🔍 [ПРОВЕРКА] Kafka: событие НЕ должно быть в orders.events");
        consumeFixedExpectingNone("hw5-verify-failure", "orders.events", orderId);

        log("");
        log("═══════════════════════════════════════════════════════════════");
        log("🎯 РЕЗУЛЬТАТ ШАГА 1: Заказ сохранён, событие НЕ потеряно, Kafka не в курсе.");
        log("   → Запустите 'outbox-relay' для повторной отправки.");
        log("═══════════════════════════════════════════════════════════════");
    }
    /**
     * ============================================================
     * 🎯 ДЗ №5, ЗАДАНИЯ 2 и 3: Повторная отправка из outbox
     * ============================================================
     *
     * Что демонстрируем:
     *
     * 1. Publisher читает НЕОБРАБОТАННЫЕ записи из outbox
     *    (WHERE published = false).
     *
     * 2. Отправляет их в Kafka, помечает published = true.
     *
     * 3. Это происходит ПОСЛЕ "восстановления" — то есть после того,
     *    как мы отключили KAFKA_FAILURE_MODE.
     *
     * 4. Событие, "застрявшее" в outbox на шаге 1, наконец доходит до Kafka.
     *
     * Что мы получаем:
     * ✅ Событие НЕ потеряно — Outbox гарантирует at-least-once доставку.
     * ✅ Kafka получает событие с задержкой, но получает.
     * ✅ Бизнес-данные и события в Kafka в конечном счёте консистентны.
     * ============================================================
     */
    private static void runOutboxRelay() throws Exception {
        banner("ДЗ №5 / ШАГ 2 - Повторная отправка из outbox (после 'восстановления')");

        log("🔧 [СОСТОЯНИЕ] KAFKA_FAILURE_MODE=false → Kafka снова доступна");

        // ------------------------------------------------------------
        // ШАГ 2.1: Проверяем, что в outbox есть pending-событие
        // ------------------------------------------------------------
        log("");
        log("🔍 [ПРОВЕРКА] pending-события в outbox:");
        try (Connection c = db();
            PreparedStatement ps = c.prepareStatement(
                "select id, aggregate_id, event_type, status from outbox " +
                "where status in ('pending', 'processing')")) {
            ResultSet rs = ps.executeQuery();
            int pending = 0;
            while (rs.next()) {
                pending++;
                log("   ⏳ id=%s aggregateId=%s eventType=%s status=%s"
                    .formatted(rs.getString(1), rs.getString(2),
                            rs.getString(3), rs.getString(4)));
            }
            if (pending == 0) {
                log("   (нет pending/processing событий — сначала запустите 'outbox-fail')");
                return;
            }
            log("   Всего к обработке: " + pending);
        }

        // ------------------------------------------------------------
        // ШАГ 2.2: Отправляем pending-события в Kafka
        // ------------------------------------------------------------
        log("");
        log("🚀 [RELAY] Отправляем pending-события в Kafka...");
        relayOutboxOnce(); // ← ваш существующий метод, он работает без сбоя

        // ------------------------------------------------------------
        // ШАГ 2.3: Проверяем состояние после успешной отправки
        // ------------------------------------------------------------
        log("");
        log("🔍 [ПРОВЕРКА] Состояние после успешной отправки:");
        try (Connection c = db();
            PreparedStatement ps = c.prepareStatement(
                "select id, aggregate_id, event_type, published, status from outbox " +
                "where status != 'published'")) {
            ResultSet rs = ps.executeQuery();
            int stillPending = 0;
            while (rs.next()) {
                stillPending++;
                log("   ❌ id=%s published=%s status=%s (не должно быть!)"
                    .formatted(rs.getString(1), rs.getBoolean(4), rs.getString(5)));
            }
            if (stillPending == 0) {
                log("   ✅ Все события отправлены (status='published', published=true)");
            }
        }

        // ------------------------------------------------------------
        // ШАГ 2.4: Проверяем, что Kafka получила событие
        // ------------------------------------------------------------
        log("");
        log("🔍 [ПРОВЕРКА] Kafka: событие ДОЛЖНО быть в orders.events");
        // ============================================================
        // 🎯 ВАЖНО: даём consumer'у время на rebalance + чтение
        // ============================================================
        // Только что созданный consumer сначала проходит rebalance
        // (назначение партиций) — это занимает 1-3 секунды.
        // Только после этого poll() начнёт возвращать сообщения.
        // Поэтому увеличиваем таймаут до 15 секунд.
        // ============================================================
        consumeFixed("hw5-verify-relay", "orders.events", 1, Duration.ofSeconds(15));

        log("");
        log("═══════════════════════════════════════════════════════════════");
        log("🎯 РЕЗУЛЬТАТ ШАГА 2: Событие доставлено в Kafka через outbox.");
        log("   → Система восстановилась без потери данных.");
        log("═══════════════════════════════════════════════════════════════");
    }

    private static void inbox() throws Exception {
        banner("SLIDE 14 - Inbox/idempotent consumer makes duplicate delivery harmless");
        String eventId = UUID.randomUUID().toString();
        try (Connection c = db()) { st(c, "delete from inbox"); st(c, "delete from billing_payments where order_id='order-inbox-1'"); }
        try (KafkaProducer<String, String> p = producer()) {
            String payload = eventWithId(eventId, "OrderPaid", "order-inbox-1", Map.of("amount", 2400));
            send(p, "orders.events", "order-inbox-1", payload);
            send(p, "orders.events", "order-inbox-1", payload);
        }
        processBillingWithInbox(2);
        try (Connection c = db(); ResultSet rs = c.createStatement().executeQuery("select count(*) from billing_payments where order_id='order-inbox-1'")) {
            rs.next(); log("billing_payments rows for order-inbox-1 = " + rs.getInt(1));
        }
        log("Meaning: Kafka may redeliver, but business side effect happens once because eventId is stored transactionally.");
    }

    private static void cqrsSaga() throws Exception {
        banner("SLIDE 15 - CQRS projection plus choreography-style saga");
        try (Connection c = db()) { st(c, "delete from order_projection where order_id='saga-42'"); }
        try (KafkaProducer<String, String> p = producer()) {
            // 1. Заказ создан
            send(p, "orders.events", "saga-42", event("OrderCreated", "saga-42", Map.of("amount", 990)));
        }
        // Choreography: services react and emit next facts.
        // 2. Ждем, пока Payment Service обработает и создаст PaymentReserved
        JsonNode orderCreated = takeOne("orders.events", "payment-service", Duration.ofSeconds(5));
        try (KafkaProducer<String, String> p = producer()) { send(p, "payments.events", "saga-42", event("PaymentReserved", "saga-42", Map.of("paymentId", "pay-42"))); }
        // 3. Ждем, пока Warehouse Service обработает и создаст StockReserved
        JsonNode paymentReserved = takeOne("payments.events", "warehouse-service", Duration.ofSeconds(5));
        try (KafkaProducer<String, String> p = producer()) { send(p, "warehouse.events", "saga-42", event("StockReserved", "saga-42", Map.of("sku", "book-1"))); }
        // 4. Ждем, пока Order Service обработает и подтвердит заказ
        JsonNode stockReserved = takeOne("warehouse.events", "order-service", Duration.ofSeconds(5));
        try (KafkaProducer<String, String> p = producer()) { send(p, "orders.events", "saga-42", event("OrderConfirmed", "saga-42", Map.of("status", "CONFIRMED", "amount", 990))); }
        projectOrders(4);
        queryProjection("saga-42");
        log("Meaning: each service owns one step. Read model is rebuilt from events and is eventually consistent.");
    }
    // Retry Topic
    private static void retryDlt() throws Exception {
        banner("SLIDE 16 - bounded retry topics and DLT with reason");
        // Отправляем 4 сообщения с разными сценариями
        try (KafkaProducer<String, String> p = producer()) {
            // 1. OK - обработается сразу
            send(p, "orders.events", "ok-1", event("OrderPaid", "ok-1", Map.of("case", "ok")));
            // 2. Временная ошибка - перейдет в retry.1
            send(p, "orders.events", "temp-1", event("OrderPaid", "temp-1", Map.of("case", "temporary")));
            // 3. Долгая временная ошибка - перейдет в retry.2
            send(p, "orders.events", "slow-1", event("OrderPaid", "slow-1", Map.of("case", "temporary-long")));
            // 4. Фатальная ошибка - сразу в DLT
            send(p, "orders.events", "bad-1", event("OrderPaid", "bad-1", Map.of("case", "poison", "schema", "invalid business field")));
        }
        // Обработка с 3 уровнями retry
        processWithRetry("orders.events", "orders.retry.1", "orders.dlt", "retry-demo-main", 4, 1);
        processWithRetry("orders.retry.1", "orders.retry.2", "orders.dlt", "retry-demo-r1", 2, 2);
        processWithRetry("orders.retry.2", "orders.dlt", "orders.dlt", "retry-demo-r2", 1, 3);
        // Проверяем, что в DLT попало только одно сообщение
        consumeFixed("dlt-ops", "orders.dlt", 1, Duration.ofSeconds(5));
        log("Meaning: retries are finite and observable; DLT has failed messages plus reason headers, not just a silent trash bin.");
    }

    private static void contractEvolution() throws Exception {
        banner("SLIDE 17 - event contract and schema compatibility");
        String v1 = "{\"eventId\":\"%s\",\"eventType\":\"CustomerChanged\",\"eventVersion\":1,\"occurredAt\":\"%s\",\"payload\":{\"customerId\":\"c-1\",\"name\":\"ACME\"}}".formatted(UUID.randomUUID(), Instant.now());
        String v2Compatible = "{\"eventId\":\"%s\",\"eventType\":\"CustomerChanged\",\"eventVersion\":2,\"occurredAt\":\"%s\",\"payload\":{\"customerId\":\"c-1\",\"name\":\"ACME\",\"segment\":\"B2B\"}}".formatted(UUID.randomUUID(), Instant.now());
        String v3Breaking = "{\"eventId\":\"%s\",\"eventType\":\"CustomerChanged\",\"eventVersion\":3,\"occurredAt\":\"%s\",\"payload\":{\"id\":\"c-1\",\"fullName\":\"ACME Ltd\"}}".formatted(UUID.randomUUID(), Instant.now());
        for (String msg : List.of(v1, v2Compatible, v3Breaking)) validateOldConsumerContract(msg);
        log("Meaning: adding optional fields is usually compatible; renaming/removing fields breaks consumers that are already deployed.");
    }

    private static void badSharedGroup() throws Exception {
        banner("SLIDES 18–19 anti-pattern - shared consumer group for different business functions");
        try (KafkaProducer<String, String> p = producer()) {
            for (int i=0;i<6;i++) send(p, "orders.events", "bad-sg-"+i, event("OrderPaid", "bad-sg-"+i, Map.of("amount", i)));
        }
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        pool.submit(() -> consumerWorker("orders-shared", "billing-function", 3, latch));
        pool.submit(() -> consumerWorker("orders-shared", "notification-function", 3, latch));
        latch.await(10, TimeUnit.SECONDS); pool.shutdownNow();
        log("Meaning: billing and notification split the stream. Each business function misses records handled by the other one.");
    }

    private static void goodSeparateGroups() throws Exception {
        banner("SLIDE 24 refactor - separate consumer group per business function");
        try (KafkaProducer<String, String> p = producer()) {
            for (int i=0;i<4;i++) send(p, "orders.events", "good-g-"+i, event("OrderPaid", "good-g-"+i, Map.of("amount", i)));
        }
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        pool.submit(() -> consumerWorker("billing-service-refactored", "billing", 4, latch));
        pool.submit(() -> consumerWorker("notification-service-refactored", "notification", 4, latch));
        latch.await(10, TimeUnit.SECONDS); pool.shutdownNow();
        log("Meaning: both business functions see every event. Trade-off: total downstream load increases.");
    }

    private static void badUniversalTopic() throws Exception {
        banner("SLIDE 18 anti-pattern - one universal topic");
        try (KafkaProducer<String, String> p = producer()) {
            send(p, "all.events", "order-1", event("OrderPaid", "order-1", Map.of("amount", 100)));
            send(p, "all.events", "customer-1", event("CustomerBlocked", "customer-1", Map.of("reason", "AML")));
            send(p, "all.events", "invoice-1", event("InvoiceSent", "invoice-1", Map.of("email", "user@example.org")));
        }
        consumeFixed("universal-topic-consumer", "all.events", 3, Duration.ofSeconds(5));
        log("Meaning: consumer has to know unrelated event types. Ownership, ACL, retention and compatibility become impossible to reason about cleanly.");
    }

    private static void auditReplay() throws Exception {
        banner("SLIDE 20 / 25 - audit log and replay into a projection");
        // 1. Очищаем проекцию
        try (Connection c = db()) { st(c, "delete from order_projection where order_id like 'replay-%'"); }
        // 2. Отправляем события в топик audit.events
        try (KafkaProducer<String, String> p = producer()) {
            send(p, "audit.events", "replay-1", event("OrderCreated", "replay-1", Map.of("amount", 10)));
            send(p, "audit.events", "replay-1", event("OrderPaid", "replay-1", Map.of("amount", 10)));
            send(p, "audit.events", "replay-2", event("OrderCreated", "replay-2", Map.of("amount", 20)));
        }
        // 3. ВЕРСИЯ 1 - строим проекцию (может быть с багом)
        projectFromTopic("audit.events", "audit-replay-v1", 3);
        queryProjection("replay-1");
        log("Now pretend read-model code was fixed. Use a NEW group to replay same retained log:");
        // 4. ВЕРСИЯ 2 - снова строим проекцию с НОВОЙ группой
        // Новая группа = перечитываем все события с начала!
        projectFromTopic("audit.events", "audit-replay-v2", 3);
        queryProjection("replay-2");
        log("Meaning: retained log can rebuild read models. This is a system property you do not get from a transient queue by default.");
    }
    /**
     * Режим PRODUCER
     * Отправляет 12 сообщений в топик orders с разными userId в качестве ключей
     * 
     * 1. Метод демонстрирует, как key влияет на распределение по партициям
     * 2. Показывает, что одинаковый key → всегда одна партиция
     * 3. Выводит key, partition, offset для каждого сообщения
     * 
     * Почему используем существующие методы?
     * - producer() уже создает KafkaProducer с правильными настройками
     * - send() уже отправляет сообщение и выводит метаданные
     * - Не нужно дублировать код!
     */
    private static void runProducer() throws Exception {
        banner("PRODUCER - отправка сообщений в топик orders");
        
        String topic = "orders.events"; // Имя топика согласно ТЗ
        
        try (KafkaProducer<String, String> p = producer()) {
            // Отправляем 12 сообщений (минимум 10 по ТЗ)
            for (int i = 1; i <= 12; i++) {
                // userId: 10, 20, 30, 10, 20, 30, ...
                // Почему так? Чтобы увидеть, что одинаковые userId попадают в одну партицию
                int userId = switch (i % 3) {
                    case 0 -> 30;
                    case 1 -> 10;
                    default -> 20;
                };
                
                // Формируем JSON согласно ТЗ
                // Используем существующий метод event(), который создает событие с нужной структурой
                String value = event(
                    "OrderPlaced",                              // eventType
                    "order-" + i,                               // aggregateId (используем как orderId)
                    Map.of(
                        "orderId", i,                           // orderId
                        "userId", userId,                       // userId
                        "product", getProductForOrder(i)        // product
                    )
                );
                
                // Ключ = userId (превращаем в строку, так как сериализатор строковый)
                String key = String.valueOf(userId);
                
                // Отправляем с синхронным ожиданием (.get())
                // Используем существующий send(), который уже выводит:
                // topic, partition, offset, key, eventType
                send(p, topic, key, value);
            }
        }
        
        log("Все сообщения отправлены. Используйте consumer для чтения.");
    }

    /**
     * Вспомогательный метод для выбора продукта по номеру заказа
     * Просто для разнообразия данных
     */
    private static String getProductForOrder(int orderId) {
        String[] products = {"Keyboard", "Mouse", "Monitor", "Laptop", "Headphones", 
                            "Tablet", "Printer", "Scanner", "Speaker", "Camera", 
                            "Drone", "Smartwatch"};
        return products[(orderId - 1) % products.length];
    }

    /**
     * Режим CONSUMER для домашнего задания
     * Читает сообщения из топика orders
     * 
     * Аргументы командной строки:
     * - args[1]: имя consumer (для вывода в логах)
     * - args[2]: group.id (для демонстрации consumer groups)
     * 
     * Примеры запуска:
     * consumer consumer-1 order-group-1   // первый consumer в группе 1
     * consumer consumer-2 order-group-1   // второй consumer в группе 1
     * consumer consumer-3 order-group-2   // consumer в другой группе
     * 
     * Почему используем существующие методы?
     * - consumer(group) создает Consumer с правильными настройками
     * - В нем уже есть auto.offset.reset=earliest (читаем с начала)
     * - В нем уже есть enable.auto.commit=false (ручной контроль)
     * 
     * Зачем нужен ручной commitSync()?
     * - Чтобы гарантировать, что оффсет сохранится только после обработки
     * - Для демонстрации работы consumer groups
     */
    private static void runConsumer(String[] args) throws Exception {
        // Получаем имя consumer и group.id из аргументов
        // Используем args
        String consumerName = args.length > 1 ? args[1] : "consumer-1";
        String groupId = args.length > 2 ? args[2] : "order-group-1";
        
        banner("CONSUMER - name=" + consumerName + ", group=" + groupId);
        
        String topic = "orders.events";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //читаем с самого начала (нужно для демонстрации)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //ручной контроль (ключевое требование ДЗ)
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            
            int count = 0;
            long until = System.currentTimeMillis() + 15000;
            
            while (System.currentTimeMillis() < until && count < 12) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    JsonNode json = JSON.readTree(record.value());
                    JsonNode payload = json.path("payload");
                    System.out.printf("[%s] CONSUMER: group=%s, key=%s, partition=%d, offset=%d, orderId=%d, userId=%d, product=%s%n",
                        consumerName, groupId, record.key(), record.partition(), record.offset(),
                        payload.path("orderId").asInt(), payload.path("userId").asInt(),
                        payload.path("product").asText("unknown"));
                }
                consumer.commitSync();
            }
            
            log("Consumer '" + consumerName + "' завершил работу. Прочитано сообщений: " + count);
        }
    }
    /**
     * Режим PRODUCER с настройками надежности для ДЗ №2
     * 
     * Настройки надежности:
     * - acks=all - подтверждение от всех реплик в ISR
     * - retries=5 - повторные попытки при ошибках
     * - enable.idempotence=true - защита от дублей
     * 
     * Зачем:
     * - acks=all: гарантирует, что сообщение не потеряется при падении брокера
     * - retries: спасает от временных сетевых проблем
     * - idempotence: Producer не создаст дубли даже при повторных отправках
     */
    private static void runProducerSafe() throws Exception {
        banner("PRODUCER SAFE - надежная доставка с acks=all, retries, idempotence");
        
        String topic = "orders.events";
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Настройки надежности
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // props.put(ProducerConfig.RETRIES_CONFIG, 5); сознательно не задаём явным маленьким числом:
        // при enable.idempotence=true клиент сам выставляет retries практически без ограничения,
        // а реальным лимитом служит delivery.timeout.ms ниже.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                int userId = switch (i % 3) {
                    case 0 -> 30;
                    case 1 -> 10;
                    default -> 20;
                };
                
                String value = event(
                    "OrderPlaced",
                    "order-" + i,
                    Map.of(
                        "orderId", i,
                        "userId", userId,
                        "product", getProductForOrder(i)
                    )
                );
                
                String key = String.valueOf(userId);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("[PRODUCER-SAFE] ✅ SUCCESS: key=%s, partition=%d, offset=%d, orderId=%d%n",
                        key, metadata.partition(), metadata.offset(), i);
                } catch (Exception e) {
                    System.err.printf("[PRODUCER-SAFE] ❌ ERROR: key=%s, orderId=%d, error=%s%n",
                        key, i, e.getMessage());
                }
            }
        }
        
        log("Все сообщения отправлены с настройками надежности.");
    }
    /**
     * Режим CONSUMER SAFE для ДЗ №2
     * Настройки:
     * - enable.auto.commit=false - ручной контроль offset
     * - commitSync() только после успешной обработки
     * - auto.offset.reset=earliest - читаем с начала
     * 
     * Аргументы командной строки:
     * - args[1]: имя consumer
     * - args[2]: group.id
     * 
     * Пример:
     * consumer-safe consumer-1 consumer-safe-group
     */
    private static void runConsumerSafe(String[] args) throws Exception {
        String consumerName = args.length > 1 ? args[1] : "consumer-safe-1";
        String groupId = args.length > 2 ? args[2] : "consumer-safe-group";
        
        banner("CONSUMER SAFE - name=" + consumerName + ", group=" + groupId);
        
        String topic = "orders.events";
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        
        // 👇 КЛЮЧЕВОЙ МОМЕНТ: читаем с самого начала
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // 👇 Ручной контроль offset (требование ДЗ)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            
            log("Подписались на топик: " + topic);
            log("Ожидаем сообщения... (таймаут 15 секунд)");
            log("Начинаем чтение...");
            
            int count = 0;
            long until = System.currentTimeMillis() + 15000;
            
            while (System.currentTimeMillis() < until) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    // 👇 Отладка: видно, что consumer работает, но сообщений нет
                    log("Нет сообщений, продолжаем ждать...");
                    continue;
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode json = JSON.readTree(record.value());
                        JsonNode payload = json.path("payload");
                        
                        // Вывод информации о сообщении
                        System.out.printf("[%s] PROCESSING: key=%s, partition=%d, offset=%d, orderId=%d, userId=%d%n",
                            consumerName,
                            record.key(),
                            record.partition(),
                            record.offset(),
                            payload.path("orderId").asInt(),
                            payload.path("userId").asInt()
                        );
                        
                        // ✅ Успешная обработка → коммитим offset
                        consumer.commitSync();
                        count++;
                        
                        System.out.printf("[%s] ✅ COMMITTED: offset=%d%n", consumerName, record.offset());
                        
                    } catch (Exception e) {
                        // ❌ Ошибка обработки - offset НЕ КОММИТИМ!
                        System.err.printf("[%s] ❌ PROCESSING ERROR: offset=%d, error=%s%n",
                            consumerName, record.offset(), e.getMessage());
                    }
                }
            }
            
            log("Consumer '" + consumerName + "' завершил работу. Обработано сообщений: " + count);
        }
    }
    /**
     * Режим CONSUMER с обработкой ошибок через Retry и DLT для ДЗ №3
     * 
     * Использование:
     * consumer-retry consumer-name consumer-group topic
     * 
     * Примеры:
     * consumer-retry consumer-main consumer-retry-group orders.events
     * consumer-retry consumer-retry-1 consumer-retry-group orders.retry.1
     * consumer-retry consumer-retry-2 consumer-retry-group orders.retry.2
     */
    private static void runConsumerWithRetry(String[] args) throws Exception {
        String consumerName = args.length > 1 ? args[1] : "consumer-retry-1";
        String groupId = args.length > 2 ? args[2] : "consumer-retry-group";
        String sourceTopic = args.length > 3 ? args[3] : "orders.events";
        
        banner("CONSUMER RETRY - name=" + consumerName + ", group=" + groupId + ", topic=" + sourceTopic);
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // ============================================================
        // 🎯 ОПРЕДЕЛЯЕМ УРОВЕНЬ RETRY И ЦЕЛЕВОЙ ТОПИК
        // ============================================================
        int retryLevel = 0;
        String targetTopic = "orders.retry.1";
        if (sourceTopic.equals("orders.retry.1")) {
            retryLevel = 1;
            targetTopic = "orders.retry.2";
        } else if (sourceTopic.equals("orders.retry.2")) {
            retryLevel = 2;
            targetTopic = "orders.dlt";
        } else if (sourceTopic.equals("orders.dlt")) {
            retryLevel = 3;
            targetTopic = null; // ВАЖНО: не отправляем дальше!
        }
        
        int backoffMs = retryLevel == 0 ? 0 : retryLevel * 3000; // 0s, 3s, 6s
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
            
            consumer.subscribe(List.of(sourceTopic));
            
            log("Подписались на топик: " + sourceTopic);
            log("Уровень retry: " + retryLevel + ", backoff: " + backoffMs + "ms");
            if (targetTopic != null) {
                log("При ошибке отправляем в: " + targetTopic);
            } else {
                log("⚠️ Это DLT-консьюмер. Сообщения только читаем, дальше не отправляем!");
            }
            
            int count = 0;
            long until = System.currentTimeMillis() + 30000;
            
            while (System.currentTimeMillis() < until) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    continue;
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode json = JSON.readTree(record.value());
                        JsonNode payload = json.path("payload");
                        
                        int orderId = payload.path("orderId").asInt();
                        int userId = payload.path("userId").asInt();
                        
                        // ============================================================
                        // 🎯 ИСКУССТВЕННАЯ ОШИБКА ДЛЯ orderId = 5
                        // ============================================================
                        if (orderId == 5) {
                            throw new RuntimeException("Искусственная ошибка для orderId=5 (тестируем Retry/DLT)");
                        }
                        
                        // ============================================================
                        // ✅ УСПЕШНАЯ ОБРАБОТКА
                        // ============================================================
                        int attempt = 0;
                        Iterable<Header> attemptHeaders = record.headers().headers("x-attempt");
                        if (attemptHeaders.iterator().hasNext()) {
                            attempt = Integer.parseInt(new String(attemptHeaders.iterator().next().value()));
                        }
                        
                        System.out.printf("[%s] ✅ УСПЕШНО: key=%s, partition=%d, offset=%d, orderId=%d, userId=%d, product=%s, попытка=%d%n",
                            consumerName,
                            record.key(),
                            record.partition(),
                            record.offset(),
                            orderId,
                            userId,
                            payload.path("product").asText("unknown"),
                            attempt
                        );
                        
                        consumer.commitSync();
                        count++;
                        
                    } catch (Exception e) {
                        // ============================================================
                        // ❌ ОШИБКА
                        // ============================================================
                        int orderId = 0;
                        try {
                            JsonNode json = JSON.readTree(record.value());
                            JsonNode payload = json.path("payload");
                            orderId = payload.path("orderId").asInt();
                        } catch (Exception ignore) {}
                        
                        int attempt = 1;
                        Iterable<Header> headers = record.headers().headers("x-attempt");
                        if (headers.iterator().hasNext()) {
                            attempt = Integer.parseInt(new String(headers.iterator().next().value())) + 1;
                        }
                        
                        System.err.printf("[%s] ❌ ОШИБКА: orderId=%d, попытка=%d, ошибка=%s%n",
                            consumerName, orderId, attempt, e.getMessage());
                        
                        // ============================================================
                        // 🎯 ЕСЛИ targetTopic != null → ОТПРАВЛЯЕМ ДАЛЬШЕ
                        // ============================================================
                        if (targetTopic != null) {
                            System.out.printf("[%s] 🔄 RETRY: orderId=%d, попытка=%d → отправляем в %s (backoff %dms)%n",
                                consumerName, orderId, attempt, targetTopic, backoffMs);
                            
                            ProducerRecord<String, String> retryRecord = new ProducerRecord<>(
                                targetTopic,
                                record.key(),
                                record.value()
                            );
                            retryRecord.headers().add("x-attempt", String.valueOf(attempt).getBytes());
                            retryRecord.headers().add("x-original-topic", sourceTopic.getBytes());
                            retryRecord.headers().add("x-error", e.getMessage().getBytes());
                            retryRecord.headers().add("x-error-time", Instant.now().toString().getBytes());
                            retryRecord.headers().add("x-retry-count", String.valueOf(attempt).getBytes());
                            
                            try {
                                Thread.sleep(backoffMs);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                            producer.send(retryRecord).get();
                            
                        } else {
                            // ============================================================
                            // 💀 targetTopic == null → ЭТО DLT
                            // ============================================================
                            System.out.printf("[%s] 💀 DLT: orderId=%d, попытка=%d (все попытки исчерпаны, сообщение отправлено в orders.dlt)%n",
                                consumerName, orderId, attempt);
                        }
                        
                        consumer.commitSync();
                    }
                }
            }
            
            log("Consumer '" + consumerName + "' завершил работу. Обработано успешно: " + count);
        }
    }

    /**
     * Создает свойства для Producer (используется в consumer-retry)
     */
    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        return props;
    }
    /**
     * ДЗ №4, Задание 1: Producer, который отправляет несколько уникальных событий
     * и намеренно дублирует одно из них (тот же eventId) несколько раз подряд.
     *
     * Почему дубль — это именно ОДИНАКОВЫЙ eventId, а не просто "ещё одно похожее сообщение"?
     * В реальной жизни Kafka может сама доставить одно и то же сообщение дважды, без нашего умысла:
     * - producer не дождался ack вовремя и сделал retry, хотя брокер сообщение уже принял
     *   (типичный случай без enable.idempotence на стороне producer);
     * - consumer обработал сообщение, но упал/перезапустился ДО commitSync() — при перезапуске
     *   он читает с последнего закоммиченного offset и получает то же сообщение снова.
     * В обоих случаях eventId (если он присвоен один раз и не меняется) остаётся одинаковым —
     * именно по нему consumer и должен опознать дубль, а не по содержимому сообщения.
     */
    private static void runProducerWithDuplicates() throws Exception {
        banner("PRODUCER DUP — уникальные события + намеренный дубль по eventId");

        String topic = "orders.events";

        try (KafkaProducer<String, String> p = producer()) {
            // 1. Пять обычных заказов — у каждого свой уникальный eventId
            //    (event() сам генерирует новый UUID при каждом вызове)
            for (int i = 1; i <= 5; i++) {
                String aggregateId = "hw4-order-" + i;
                String value = event("OrderCreated", aggregateId, Map.of("amount", 100 * i));
                send(p, topic, aggregateId, value);
            }

            // 2. Одно сообщение с ФИКСИРОВАННЫМ eventId — генерируем JSON один раз,
            //    а отправляем этот же самый payload несколько раз подряд.
            String duplicatedEventId = UUID.randomUUID().toString();
            String duplicatedOrderId = "hw4-order-DUP";
            String duplicatedPayload = eventWithId(duplicatedEventId, "OrderCreated", duplicatedOrderId, Map.of("amount", 777));

            log("Отправляем eventId=" + duplicatedEventId + " ТРИ раза подряд (эмулируем повторную доставку)");
            for (int attempt = 1; attempt <= 3; attempt++) {
                RecordMetadata md = p.send(new ProducerRecord<>(topic, duplicatedOrderId, duplicatedPayload)).get();
                log("dup-send attempt=%d partition=%d offset=%d eventId=%s".formatted(attempt, md.partition(), md.offset(), duplicatedEventId));
            }
        }

        log("Готово: 5 уникальных событий + 1 событие продублировано 3 раза (один и тот же eventId).");
    }

    /**
     * ДЗ №4, Задание 2+3: идемпотентный consumer с Inbox Pattern.
     *
     * Бизнес-операция (запись в hw4_processed_orders) и отметка "это eventId уже обработан"
     * (запись в inbox) выполняются в ОДНОЙ транзакции БД — если что-то из двух не запишется,
     * откатится всё целиком, и при повторном чтении сообщения обработка честно начнётся заново.
     *
     * Порядок проверки: сначала select по inbox — если запись уже есть, бизнес-логику НЕ повторяем.
     * Настоящая защита от гонки при этом — PRIMARY KEY на inbox.event_id: если бы два процесса
     * попытались вставить один и тот же event_id параллельно, второй словил бы constraint violation.
     * В этой демонстрации конкуренции нет: сообщения с одинаковым ключом (orderId) всегда попадают
     * в одну и ту же партицию и обрабатываются одним consumer'ом строго последовательно — поэтому
     * простого select-затем-insert достаточно, отдельный SELECT ... FOR UPDATE тут не добавляет
     * реальной защиты (FOR UPDATE не блокирует ЕЩЁ НЕ СУЩЕСТВУЮЩУЮ строку).
     *
     * Kafka offset коммитится ПОСЛЕ завершения транзакции в БД — то есть после того, как мы точно
     * знаем исход: либо обработали и записали, либо корректно распознали дубль и ничего не делали.
     */
    private static void runConsumerIdempotent(String[] args) throws Exception {
        String consumerName = args.length > 1 ? args[1] : "consumer-idempotent-1";
        String groupId = args.length > 2 ? args[2] : "consumer-idempotent-group";

        banner("CONSUMER IDEMPOTENT — name=" + consumerName + ", group=" + groupId);

        String topic = "orders.events";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // ручной коммит — только после успешной транзакции в БД

        int processed = 0;
        int duplicates = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            log("Подписались на топик: " + topic);

            long until = System.currentTimeMillis() + 20000;

            while (System.currentTimeMillis() < until) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    log("Нет сообщений, продолжаем ждать...");
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    JsonNode event = JSON.readTree(record.value());
                    String eventId = event.path("eventId").asText();
                    String orderId = event.path("aggregateId").asText();
                    int amount = event.path("payload").path("amount").asInt(0);

                    try (Connection db = db()) {
                        db.setAutoCommit(false); // проверка дубля и бизнес-операция должны быть одной транзакцией

                        boolean alreadyProcessed = exists(db, "select 1 from inbox where event_id=?::uuid", eventId);

                        if (alreadyProcessed) {
                            // ЭТО ДУБЛЬ: eventId уже встречался и обработан ранее.
                            // Бизнес-операцию НЕ повторяем — в этом и есть идемпотентность.
                            log("[%s] ⚠️ DUPLICATE SKIPPED: eventId=%s orderId=%s — уже обработано, пропускаем"
                                .formatted(consumerName, eventId, orderId));
                            duplicates++;
                            db.commit(); // транзакция пустая, но закрываем её явно
                        } else {
                            try (PreparedStatement ins = db.prepareStatement(
                                    "insert into hw4_processed_orders(order_id, event_id, amount) values (?, ?::uuid, ?) on conflict(order_id) do nothing");
                                PreparedStatement inbox = db.prepareStatement(
                                    "insert into inbox(event_id) values (?::uuid)")) {
                                ins.setString(1, orderId);
                                ins.setString(2, eventId);
                                ins.setInt(3, amount);
                                ins.executeUpdate();

                                inbox.setString(1, eventId);
                                inbox.executeUpdate();
                            }
                            db.commit(); // бизнес-операция и отметка "обработано" фиксируются атомарно
                            processed++;
                            log("[%s] ✅ PROCESSED: eventId=%s orderId=%s amount=%d — записано в hw4_processed_orders и inbox"
                                .formatted(consumerName, eventId, orderId, amount));
                        }
                    } catch (Exception e) {
                        log("[%s] ❌ DB ERROR: eventId=%s error=%s — offset НЕ коммитим, прочитаем это сообщение снова"
                            .formatted(consumerName, eventId, e.getMessage()));
                        continue; // пропускаем commitSync ниже — Kafka передаст это сообщение повторно
                    }

                    consumer.commitSync();
                }
            }

            log("Consumer '" + consumerName + "' завершил работу. Обработано новых: " + processed + ", пропущено дублей: " + duplicates);
        }
    }    

    // ---------- Kafka helpers ----------
    private static KafkaProducer<String, String> producer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.RETRIES_CONFIG, "5");
        return new KafkaProducer<>(p);
    }

    private static KafkaConsumer<String, String> consumer(String group) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group + "-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(p);
    }

    private static KafkaConsumer<String, String> stableGroupConsumer(String group) {
        KafkaConsumer<String,String> c = consumer(group);
        c.close();
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(p);
    }

    private static void send(KafkaProducer<String,String> p, String topic, String key, String value) throws Exception {
        RecordMetadata md = p.send(new ProducerRecord<>(topic, key, value)).get();
        log("send topic=%s partition=%d offset=%d key=%s eventType=%s".formatted(topic, md.partition(), md.offset(), key, JSON.readTree(value).path("eventType").asText()));
    }

    /**
     * ============================================================
     * 🎯 consumeFixed с диагностикой для ДЗ №5
     * ============================================================
     * Читает до expected сообщений из топика за maxWait.
     *
     * ВАЖНО: при первом poll() новый consumer проходит rebalance
     * (назначение партиций). Это может занять 1-3 секунды, поэтому
     * poll() возвращает пусто. Мы продолжаем poll'ить в цикле,
     * пока не истечёт maxWait или не прочитаем expected.
     * ============================================================
     */
    private static void consumeFixed(String group, String topic, int expected, Duration maxWait) throws Exception {
        try (KafkaConsumer<String,String> c = consumer(group)) {
            c.subscribe(List.of(topic));
            long until = System.currentTimeMillis() + maxWait.toMillis();
            int count = 0;
            long lastLog = 0;

            while (System.currentTimeMillis() < until && count < expected) {
                ConsumerRecords<String,String> records = c.poll(Duration.ofMillis(500));

                // Диагностика: раз в 2 секунды пишем, что мы живы и ждём
                if (records.isEmpty() && System.currentTimeMillis() - lastLog > 2000) {
                    log("   ... consumer ждёт сообщений (group=%s, topic=%s, прочитано=%d/%d)"
                        .formatted(group, topic, count, expected));
                    lastLog = System.currentTimeMillis();
                }

                for (ConsumerRecord<String,String> r : records) {
                    count++;
                    log("CONSUME group=%s topic=%s partition=%d offset=%d key=%s eventType=%s"
                        .formatted(group, r.topic(), r.partition(), r.offset(),
                                r.key(), JSON.readTree(r.value()).path("eventType").asText()));
                }
                c.commitSync();
            }
            if (count < expected) {
                log("Read %d/%d records before timeout.".formatted(count, expected));
            }
        }
    }

    /**
     * ============================================================
     * 🎯 Быстрая проверка наличия сообщения в топике (без rebalance)
     * ============================================================
     * Вместо subscribe() использует assign() на все партиции —
     * это НЕ запускает consumer group rebalance и работает моментально.
     *
     * Читает все партиции с offset=earliest до тех пор, пока не найдёт
     * сообщение с указанным aggregateId или не истечёт timeout.
     * ============================================================
     */
    private static void consumeFixedExpectingOne(String group, String topic, String expectedAggregateId, Duration maxWait) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // GROUP_ID не нужен — мы используем assign(), а не subscribe()

        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(props)) {
            // Получаем все партиции топика через AdminClient
            List<TopicPartition> partitions;
            try (AdminClient admin = AdminClient.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP))) {
                partitions = admin.describeTopics(List.of(topic)).allTopicNames().get()
                    .get(topic).partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            }

            // assign() — без rebalance, моментально
            c.assign(partitions);
            c.seekToBeginning(partitions);

            long until = System.currentTimeMillis() + maxWait.toMillis();
            int found = 0;
            while (System.currentTimeMillis() < until && found == 0) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    JsonNode n = JSON.readTree(r.value());
                    if (n.path("aggregateId").asText().equals(expectedAggregateId)) {
                        found++;
                        log("CONSUME topic=%s partition=%d offset=%d key=%s aggregateId=%s eventType=%s"
                            .formatted(r.topic(), r.partition(), r.offset(), r.key(),
                                    expectedAggregateId, n.path("eventType").asText()));
                    }
                }
            }
            if (found == 0) {
                log("❌ [KAFKA] Не найдено сообщение для aggregateId=%s за %ds"
                    .formatted(expectedAggregateId, maxWait.toSeconds()));
            } else {
                log("✅ [KAFKA] Сообщение найдено для aggregateId=%s".formatted(expectedAggregateId));
            }
        }
    }

    private static void printTopicPartitions(String topic, Duration maxWait) throws Exception {
        try (KafkaConsumer<String,String> c = consumer("partition-inspector")) {
            List<TopicPartition> tps = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2));
            c.assign(tps); c.seekToEnd(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            for (TopicPartition tp: tps) c.seek(tp, Math.max(0, end.get(tp) - 12));
            long until = System.currentTimeMillis() + maxWait.toMillis();
            while (System.currentTimeMillis() < until) {
                ConsumerRecords<String,String> records = c.poll(Duration.ofMillis(500));
                if (records.isEmpty()) break;
                for (ConsumerRecord<String,String> r : records) {
                    JsonNode node = JSON.readTree(r.value());
                    log("partition=%d offset=%d key=%s eventType=%s status=%s".formatted(r.partition(), r.offset(), r.key(), node.path("eventType").asText(), node.path("payload").path("status").asText("-")));
                }
            }
        }
    }

    private static void consumerWorker(String group, String name, int maxRecords, CountDownLatch latch) {
        try (KafkaConsumer<String,String> c = stableGroupConsumer(group)) {
            c.subscribe(List.of("orders.events"));
            int n = 0; long until = System.currentTimeMillis() + 8000;
            while (System.currentTimeMillis() < until && n < maxRecords) {
                for (ConsumerRecord<String,String> r : c.poll(Duration.ofMillis(500))) {
                    n++;
                    log("%s in group=%s got partition=%d key=%s eventType=%s".formatted(name, group, r.partition(), r.key(), JSON.readTree(r.value()).path("eventType").asText()));
                }
                c.commitSync();
            }
            log(name + " finished with " + n + " records");
        } catch (Exception e) { log(name + " failed: " + e.getMessage()); }
        finally { latch.countDown(); }
    }

    private static JsonNode takeOne(String topic, String group, Duration wait) throws Exception {
        try (KafkaConsumer<String,String> c = consumer(group)) {
            c.subscribe(List.of(topic));
            long until = System.currentTimeMillis() + wait.toMillis();
            while (System.currentTimeMillis() < until) {
                for (ConsumerRecord<String,String> r : c.poll(Duration.ofMillis(500))) {
                    JsonNode n = JSON.readTree(r.value());
                    log("%s consumed %s for key=%s".formatted(group, n.path("eventType").asText(), r.key()));
                    c.commitSync();
                    return n;
                }
            }
            throw new IllegalStateException("No record in " + topic);
        }
    }

    private static void createTopics(Map<String,Integer> topics) throws Exception {
        Properties p = new Properties(); p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        try (AdminClient admin = AdminClient.create(p)) {
            Set<String> existing = admin.listTopics().names().get();
            List<String> toDelete = topics.keySet().stream().filter(existing::contains).toList();
            if (!toDelete.isEmpty()) {
                log("Resetting topics: " + toDelete);
                admin.deleteTopics(toDelete).all().get(30, TimeUnit.SECONDS);
                long until = System.currentTimeMillis() + 30000;
                while (System.currentTimeMillis() < until) {
                    Set<String> now = admin.listTopics().names().get();
                    if (Collections.disjoint(now, toDelete)) break;
                    Thread.sleep(500);
                }
            }
            List<NewTopic> create = new ArrayList<>();
            topics.forEach((name, partitions) -> create.add(new NewTopic(name, partitions, (short)1)));
            admin.createTopics(create).all().get(30, TimeUnit.SECONDS);
            log("Topics present: " + topics.keySet());
        }
    }

    // ---------- Pattern helpers ----------
    /**
     * ============================================================
     * 🎯 ДЗ №5: relay с батчингом
     * ============================================================
     *
     * ❌ ЧТО БЫЛО ПЛОХО В СТАРОЙ ВЕРСИИ:
     *
     *   try (Connection c = db()) {
     *       c.setAutoCommit(false);
     *       SELECT ... FOR UPDATE SKIP LOCKED          ← открываем транзакцию
     *       for (each row) {
     *           producer.send(...).get();              ← синхронная отправка в Kafka
     *           UPDATE published=true;                 ← внутри той же транзакции
     *       }
     *       c.commit();                                ← закрываем транзакцию
     *   }
     *
     *   Проблема: транзакция БД держится ОТКРЫТОЙ вместе с блокировками FOR UPDATE
     *   на всё время синхронной отправки в Kafka. Если брокер медленный (100 ms на
     *   сообщение) и в батче 1000 событий — транзакция открыта 100 секунд.
     *
     *   Последствия для PostgreSQL:
     *     - долгие блокировки строк в outbox;
     *     - bloat и рост WAL;
     *     - параллельные relay'и ждут (даже с SKIP LOCKED строки "заняты");
     *     - при ошибке в середине — откат всей транзакции, а уже отправленные
     *       в Kafka события не будут помечены published=true → дубли.
     *
     * ✅ ЧТО СТАЛО (эта версия):
     *
     *   1. Читаем НЕБОЛЬШИМИ БАТЧАМИ (LIMIT 100).
     *   2. Транзакция №1: SELECT + UPDATE status='processing' + COMMIT.
     *      → блокировки снимаются сразу, транзакция короткая.
     *   3. Отправка в Kafka — ВНЕ транзакции БД.
     *   4. Транзакция №2: UPDATE status='published', published=true.
     *   5. При ошибке — UPDATE status='pending' (вернуть в очередь).
     *
     *   Что это даёт:
     *     ✅ Долгие блокировки в PostgreSQL исключены.
     *     ✅ Медленный брокер не влияет на другие транзакции.
     *     ✅ Событие не теряется — at-least-once сохраняется.
     *     ✅ Статус 'processing' защищает от гонки между relay-инстансами.
     *     ✅ Идемпотентность: если упадём после send, но до UPDATE — при
     *        повторном запуске событие уйдёт ещё раз (дубль), но не потеряется.
     *        Это правильный трейд-офф: "лучше дубль, чем потеря".
     *
     * ⚠️ Известное ограничение (см. README):
     *   Если упадём между send и UPDATE status='published' — при следующем
     *   запуске событие отправится повторно. Чтобы этого избежать, нужен
     *   либо exactly-once producer (транзакции Kafka), либо Debezium CDC.
     *   Для учебного ДЗ достаточно at-least-once + идемпотентного consumer
     *   (см. ДЗ №4 — Inbox Pattern).
     * ============================================================
     */
    private static void relayOutboxOnce() throws Exception {
        // ------------------------------------------------------------
        // Параметры батчинга
        // ------------------------------------------------------------
        // BATCH_SIZE = 100 — компромисс между количеством round-trip'ов
        // и размером транзакции. В production подбирается под нагрузку.
        // ------------------------------------------------------------
        final int BATCH_SIZE = 100;
        final String TARGET_TOPIC = "orders.events";

        long totalSent = 0;
        long totalFailed = 0;

        log("🚀 [RELAY] Запуск relay с батчингом (BATCH_SIZE=%d)".formatted(BATCH_SIZE));

        try (KafkaProducer<String, String> p = producer()) {
            while (true) {
                // ============================================================
                // ТРАНЗАКЦИЯ №1: короткая — прочитать батч и пометить processing
                // ============================================================
                // Задача: захватить батч pending-событий и сразу освободить блокировки.
                // После COMMIT строки помечены 'processing' — другие relay'и
                // их уже не увидят (status != 'pending').
                // ============================================================
                List<OutboxBatchRow> batch = new ArrayList<>();

                try (Connection c = db()) {
                    c.setAutoCommit(false); // НАЧАЛИ ТРАНЗАКЦИЮ №1

                    // Шаг 1.1: захватываем батч pending-событий
                    // FOR UPDATE SKIP LOCKED — параллельные relay'и не ждут друг друга
                    try (PreparedStatement ps = c.prepareStatement(
                            "select id, aggregate_id, payload " +
                            "from outbox " +
                            "where status = 'pending' " +
                            "order by created_at " +
                            "limit ? " +
                            "for update skip locked")) {
                        ps.setInt(1, BATCH_SIZE);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                batch.add(new OutboxBatchRow(
                                    rs.getString("id"),
                                    rs.getString("aggregate_id"),
                                    rs.getString("payload")));
                            }
                        }
                    }

                    if (batch.isEmpty()) {
                        c.commit();
                        break; // нет больше pending — выходим из цикла
                    }

                    // Шаг 1.2: помечаем батч как 'processing'
                    // Это "резервирует" строки — другие relay'и их не возьмут,
                    // даже после снятия блокировки FOR UPDATE.
                    try (PreparedStatement upd = c.prepareStatement(
                            "update outbox set status = 'processing' where id = ?::uuid")) {
                        for (OutboxBatchRow row : batch) {
                            upd.setString(1, row.id);
                            upd.addBatch();
                        }
                        upd.executeBatch();
                    }

                    c.commit(); // КОММИТ ТРАНЗАКЦИИ №1 — блокировки сняты!
                    log("📥 [RELAY] Взят батч: %d событий (status='processing')".formatted(batch.size()));
                }

                // ============================================================
                // ОТПРАВКА В KAFKA — вне транзакции БД
                // ============================================================
                // Здесь мы больше не держим никаких блокировок.
                // Даже если Kafka "тормозит" 30 секунд — PostgreSQL не страдает.
                // ============================================================
                List<String> sentIds = new ArrayList<>();
                List<String> failedIds = new ArrayList<>();

                for (OutboxBatchRow row : batch) {
                    try {
                        send(p, TARGET_TOPIC, row.key, row.payload);
                        sentIds.add(row.id);
                    } catch (Exception e) {
                        log("❌ [RELAY] send failed for id=%s: %s".formatted(row.id, e.getMessage()));
                        failedIds.add(row.id);
                    }
                }

                // ============================================================
                // ТРАНЗАКЦИЯ №2: короткая — обновить статусы по результату
                // ============================================================
                try (Connection c = db()) {
                    c.setAutoCommit(false); // НАЧАЛИ ТРАНЗАКЦИЮ №2

                    // Шаг 2.1: успешно отправленные → published=true
                    if (!sentIds.isEmpty()) {
                        try (PreparedStatement upd = c.prepareStatement(
                                "update outbox set status = 'published', published = true " +
                                "where id = ?::uuid")) {
                            for (String id : sentIds) {
                                upd.setString(1, id);
                                upd.addBatch();
                            }
                            upd.executeBatch();
                        }
                    }

                    // Шаг 2.2: неуспешные → возвращаем в 'pending' для следующей попытки
                    if (!failedIds.isEmpty()) {
                        try (PreparedStatement upd = c.prepareStatement(
                                "update outbox set status = 'pending' where id = ?::uuid")) {
                            for (String id : failedIds) {
                                upd.setString(1, id);
                                upd.addBatch();
                            }
                            upd.executeBatch();
                        }
                    }

                    c.commit(); // КОММИТ ТРАНЗАКЦИИ №2
                }

                totalSent += sentIds.size();
                totalFailed += failedIds.size();

                log("📤 [RELAY] Батч завершён: отправлено=%d, ошибок=%d".formatted(sentIds.size(), failedIds.size()));

                // Если взяли меньше BATCH_SIZE — это был последний батч
                if (batch.size() < BATCH_SIZE) break;
            }
        }

        log("✅ [RELAY] Завершено. Всего отправлено: %d, ошибок: %d".formatted(totalSent, totalFailed));
    }

    /**
     * ============================================================
     * 🎯 ДЗ №5: вспомогательный record для строки батча outbox
     * ============================================================
     */
    private record OutboxBatchRow(String id, String key, String payload) {}

    /**
     * ============================================================
     * 🎯 ДЗ №5: relay с имитацией сбоя Kafka
     * ============================================================
     *
     * Отличается от relayOutboxOnce() тем, что при KAFKA_FAILURE_MODE=true
     * "падает" перед отправкой, эмулируя недоступность брокера.
     *
     * ВАЖНО: мы падаем ДО публикации события, поэтому в outbox
     * ничего не меняется — событие остаётся с published=false.
     * ============================================================
     */
    /**
     * ============================================================
     * 🎯 ДЗ №5: relay с имитацией сбоя Kafka
     * ============================================================
     * Отличается от relayOutboxOnce() тем, что при KAFKA_FAILURE_MODE=true
     * "падает" ДО открытия транзакции, эмулируя недоступность брокера.
     *
     * ВАЖНО: мы падаем ДО любых изменений в БД, поэтому:
     *   - status остаётся 'pending'
     *   - published остаётся false
     *   - событие не теряется, ждёт следующего relay
     * ============================================================
     */
    private static void relayOutboxOnceWithFailure() throws Exception {
        if (KAFKA_FAILURE_MODE.get()) {
            // Эмулируем сбой: producer не может подключиться к брокеру.
            // Никаких транзакций, никаких изменений в БД — всё остаётся как было.
            throw new RuntimeException(
                "Искусственный сбой: Kafka broker недоступен (KAFKA_FAILURE_MODE=true)");
        }
        relayOutboxOnce();
    }

    /**
     * ============================================================
     * 🎯 ДЗ №5: проверка, что событие НЕ появилось в Kafka
     * ============================================================
     *
     * Читает топик с новой группой в течение wait-периода.
     * Если найден eventType=OrderCreated с нужным aggregateId — сообщает об ошибке.
     * Если ничего не найдено — это ОЖИДАЕМЫЙ результат (сбой сработал).
     * ============================================================
     */
    private static void consumeFixedExpectingNone(String group, String topic, String expectedAggregateId) throws Exception {
        try (KafkaConsumer<String, String> c = consumer(group)) {
            c.subscribe(List.of(topic));
            long until = System.currentTimeMillis() + 3000; // ждём 3 секунды
            int found = 0;
            while (System.currentTimeMillis() < until) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    JsonNode n = JSON.readTree(r.value());
                    if (n.path("aggregateId").asText().equals(expectedAggregateId)) {
                        found++;
                        log("   ❌ НАЙДЕНО в Kafka (не должно быть!): aggregateId=%s eventType=%s"
                            .formatted(expectedAggregateId, n.path("eventType").asText()));
                    }
                }
                c.commitSync();
            }
            if (found == 0) {
                log("   ✅ Kafka НЕ получила событие для aggregateId=%s (сбой сработал корректно)"
                    .formatted(expectedAggregateId));
            } else {
                log("   ❌ ОШИБКА: событие всё-таки попало в Kafka, хотя не должно было!");
            }
        }
    }

    private static void processBillingWithInbox(int expected) throws Exception {
        try (KafkaConsumer<String,String> c = consumer("billing-inbox-demo")) {
            c.subscribe(List.of("orders.events"));
            int seen = 0; long until = System.currentTimeMillis() + 8000;
            while (System.currentTimeMillis() < until && seen < expected) {
                for (ConsumerRecord<String,String> r : c.poll(Duration.ofMillis(500))) {
                    JsonNode e = JSON.readTree(r.value());
                    if (!e.path("eventType").asText().equals("OrderPaid") || !e.path("aggregateId").asText().equals("order-inbox-1")) continue;
                    seen++;
                    try (Connection db = db()) {
                        db.setAutoCommit(false);
                        // Consumer проверяет, не обрабатывал ли уже это событие
                        String eventId = e.path("eventId").asText();
                        if (exists(db, "select 1 from inbox where event_id=?::uuid", eventId)) {
                            log("duplicate eventId=%s -> skip business side effect".formatted(eventId)); 
                            db.rollback(); // < Пропускаем дубль 
                            continue; 
                        }
                        // 1. Делаем бизнес-логику (создаем платеж)
                        try (PreparedStatement pay = db.prepareStatement("insert into billing_payments(order_id, amount) values (?, ?) on conflict(order_id) do nothing");
                            // 2. Записываем eventId в inbox, чтобы не обрабатывать дубли
                            PreparedStatement inbox = db.prepareStatement("insert into inbox(event_id) values (?::uuid)")) {
                            pay.setString(1, e.path("aggregateId").asText()); pay.setInt(2, e.path("payload").path("amount").asInt()); pay.executeUpdate();
                            inbox.setString(1, eventId); inbox.executeUpdate();
                            db.commit(); // < Всё в одной транзакции!
                            log("applied payment once, marked eventId=" + eventId);
                        }
                    }
                }
                c.commitSync();
            }
        }
    }

    private static void projectOrders(int expected) throws Exception { projectFromTopic("orders.events", "orders-readmodel-demo", expected); }
    private static void projectFromTopic(String topic, String group, int expected) throws Exception {
        // Создаем Consumer с указанной группой
        // ВАЖНО: Если группа новая - читаем с начала (auto.offset.reset=earliest)
        try (KafkaConsumer<String,String> c = consumer(group)) {
            c.subscribe(List.of(topic));
            int count=0; long until = System.currentTimeMillis()+8000;
            while (System.currentTimeMillis()<until && count<expected) {
                for (ConsumerRecord<String,String> r : c.poll(Duration.ofMillis(500))) {
                    JsonNode e = JSON.readTree(r.value());
                    String type = e.path("eventType").asText();
                    String orderId = e.path("aggregateId").asText();
                    // Обрабатываем только события заказов
                    if (!type.startsWith("Order")) continue;
                    // Определяем статус
                    String status = switch (type) { case "OrderCreated" -> "CREATED"; case "OrderPaid" -> "PAID"; case "OrderConfirmed" -> "CONFIRMED"; default -> type; };
                    int amount = e.path("payload").path("amount").asInt(0);
                    // 👇 БИЗНЕС-ЛОГИКА: обновляем Read Model
                    // Если здесь был баг - его исправляют в новой версии кода
                    try (Connection db = db(); PreparedStatement ps = db.prepareStatement("insert into order_projection(order_id,status,amount,last_event_id) values(?,?,?,?) on conflict(order_id) do update set status=excluded.status, amount=greatest(order_projection.amount, excluded.amount), last_event_id=excluded.last_event_id, updated_at=now()")) {
                        ps.setString(1, orderId); ps.setString(2, status); ps.setInt(3, amount); ps.setString(4, e.path("eventId").asText()); ps.executeUpdate();
                    }
                    count++; log("projection upsert orderId=%s status=%s from %s".formatted(orderId, status, type));
                }
                c.commitSync();
            }
        }
    }

    private static void queryProjection(String orderId) throws Exception {
        try (Connection c = db(); PreparedStatement ps = c.prepareStatement("select order_id,status,amount,last_event_id from order_projection where order_id=?")) {
            ps.setString(1, orderId); ResultSet rs = ps.executeQuery();
            while (rs.next()) log("READ MODEL row: orderId=%s status=%s amount=%d lastEvent=%s".formatted(rs.getString(1), rs.getString(2), rs.getInt(3), rs.getString(4)));
        }
    }

    private static void processWithRetry(String inTopic, String nextTopic, String dltTopic, String group, int expected, int attempt) throws Exception {
        try (KafkaConsumer<String,String> c = consumer(group); KafkaProducer<String,String> p = producer()) {
            c.subscribe(List.of(inTopic));
            int count=0; long until = System.currentTimeMillis()+7000;
            while (System.currentTimeMillis()<until && count<expected) {
                for (ConsumerRecord<String,String> r : c.poll(Duration.ofMillis(500))) {
                    JsonNode e = JSON.readTree(r.value());
                    String kase = e.path("payload").path("case").asText("ok"); count++;
                    // 👇 ЛОГИКА РЕШЕНИЯ: успех или новая попытка?
                    if ("ok".equals(kase) || ("temporary".equals(kase) && attempt >= 2) || ("temporary-long".equals(kase) && attempt >= 3)) {
                        // ✅ Успех!
                        log("processed ok after attempt=%d key=%s case=%s".formatted(attempt, r.key(), kase));
                    // ❌ Ошибка - отправляем дальше по цепочке
                    } else {
                        String target = "poison".equals(kase) ? dltTopic : nextTopic;
                        // Копируем сообщение в новый топик
                        ProducerRecord<String,String> pr = new ProducerRecord<>(target, r.key(), r.value());
                        // 👇 ДОБАВЛЯЕМ ЗАГОЛОВОК С ПРИЧИНОЙ!
                        pr.headers().add(new RecordHeader("x-error", ("case=" + kase + "; attempt=" + attempt).getBytes(StandardCharsets.UTF_8)));
                        p.send(pr).get();
                        log("failed key=%s case=%s -> %s".formatted(r.key(), kase, target));
                    }
                }
                c.commitSync();
            }
        }
    }

    private static void validateOldConsumerContract(String msg) throws Exception {
        JsonNode n = JSON.readTree(msg);
        JsonNode p = n.path("payload");
        boolean compatible = p.has("customerId") && p.has("name");
        log("old-consumer sees eventVersion=%d compatible=%s payload=%s".formatted(n.path("eventVersion").asInt(), compatible, p));
        if (!compatible) log("BREAKING CHANGE: old consumer cannot find customerId/name");
    }

    // ---------- DB/helpers ----------
    private static Connection db() throws SQLException { return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD); }
    private static void st(Connection c, String sql) throws SQLException { try (Statement s = c.createStatement()) { s.execute(sql); } }
    private static boolean exists(Connection c, String sql, String arg) throws SQLException { try (PreparedStatement ps = c.prepareStatement(sql)) { ps.setString(1, arg); try (ResultSet rs = ps.executeQuery()) { return rs.next(); } } }

    private static String event(String type, String aggregateId, Map<String,Object> payload) throws Exception { return eventWithId(UUID.randomUUID().toString(), type, aggregateId, payload); }
    private static String eventWithId(String id, String type, String aggregateId, Map<String,Object> payload) throws Exception {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("eventId", id); m.put("eventType", type); m.put("eventVersion", 1); m.put("occurredAt", Instant.now().toString()); m.put("producer", "training-app"); m.put("correlationId", UUID.randomUUID().toString()); m.put("aggregateId", aggregateId); m.put("payload", payload);
        return JSON.writeValueAsString(m);
    }

    private static String env(String name, String def) { String v = System.getenv(name); return v == null || v.isBlank() ? def : v; }
    private static void log(String s) { System.out.println("[demo] " + s); }
    private static void banner(String s) { System.out.println("\n========== " + s + " =========="); }
}
