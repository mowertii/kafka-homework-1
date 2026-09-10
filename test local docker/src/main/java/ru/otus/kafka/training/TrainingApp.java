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
            case "producer-dup" -> runProducerWithDuplicates();
            case "consumer-idempotent" -> runConsumerIdempotent(args);
            case "all" -> { /* ... */ }
            default -> { System.err.println("Unknown mode: " + args[0]); help(); System.exit(2); }
        }
    }

    private static void help() {
        System.out.println("Modes: init, basic-entities, partition-order, consumer-groups, event-styles, outbox, inbox, cqrs-saga, retry-dlt, contract, bad-shared-group, good-separate-groups, bad-universal-topic, audit-replay, producer, consumer, producer-safe, consumer-safe, producer-dup, consumer-idempotent, consumer-retry, all");
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
            st(c, "create table if not exists outbox(id uuid primary key, aggregate_id varchar not null, event_type varchar not null, payload text not null, published boolean not null default false, created_at timestamptz not null default now())");
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

    private static void consumeFixed(String group, String topic, int expected, Duration maxWait) throws Exception {
        try (KafkaConsumer<String,String> c = consumer(group)) {
            c.subscribe(List.of(topic));
            long until = System.currentTimeMillis() + maxWait.toMillis();
            int count = 0;
            while (System.currentTimeMillis() < until && count < expected) {
                ConsumerRecords<String,String> records = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String,String> r : records) {
                    count++;
                    log("CONSUME group=%s topic=%s partition=%d offset=%d key=%s eventType=%s".formatted(group, r.topic(), r.partition(), r.offset(), r.key(), JSON.readTree(r.value()).path("eventType").asText()));
                }
                c.commitSync();
            }
            if (count < expected) log("Read %d/%d records before timeout. Previous runs may have advanced offsets; use random groups in code to avoid that.".formatted(count, expected));
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
    private static void relayOutboxOnce() throws Exception {
        try (Connection c = db(); KafkaProducer<String,String> p = producer()) {
            c.setAutoCommit(false);
            // Забираем неопубликованные события (с блокировкой строк)
            try (PreparedStatement ps = c.prepareStatement("select id, aggregate_id, payload from outbox where published=false order by created_at for update skip locked")) {
                // ^ Защита от конкурентных релеев
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String id = rs.getString(1); String key = rs.getString(2); String payload = rs.getString(3);
                    // Отправляем в Kafka
                    send(p, "orders.events", key, payload);
                    // Помечаем как опубликованное
                    try (PreparedStatement upd = c.prepareStatement("update outbox set published=true where id=?::uuid")) { upd.setString(1, id); upd.executeUpdate(); }
                }
            }
            c.commit(); // < Фиксируем все изменения
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
