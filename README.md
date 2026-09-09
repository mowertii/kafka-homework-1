# ДЗ №2. Надёжная доставка: acks, retries, idempotent producer, manual offset commit

## Клонирование
git clone git@github.com:mowertii/kafka-training.git
cd kafka-training
git checkout feature/homework-2
## Как запустить проект

Требуется Docker и Docker Compose.

1. Из корня проекта запустить единый скрипт:

   ```
   hw2.cmd
   ```

   Скрипт делает три шага **по порядку, в рамках одного запуска**:
   1. `init` — один раз пересоздаёт топики Kafka и таблицы в Postgres (важно: `init` **удаляет** топик `orders.events`, поэтому его нельзя гонять между producer и consumer — иначе consumer прочитает пустой топик).
   2. `producer-safe` — отправляет 10 сообщений с надёжными настройками.
   3. `consumer-safe` — вычитывает их с ручным коммитом offset.

2. Если нужно перезапустить только producer или только consumer (без пересоздания топиков), запускать напрямую, **не трогая init**:

   ```
   docker compose -p kafka-training run --rm app producer-safe
   docker compose -p kafka-training run --rm app consumer-safe consumer-safe-1 consumer-safe-group
   ```

## Задание 1. Producer

Класс `TrainingApp`, метод `runProducerSafe()`. Настройки:

```java
         // Настройки надежности
         props.put(ProducerConfig.ACKS_CONFIG, "all");
         props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
         // props.put(ProducerConfig.RETRIES_CONFIG, 5); сознательно не задаём явным маленьким числом:
         // при enable.idempotence=true клиент сам выставляет retries практически без ограничения,
         // а реальным лимитом служит delivery.timeout.ms ниже.
         props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
         props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
```

Отправляется 10 сообщений (`for (int i = 1; i <= 10; i++)`), для каждого в лог выводятся key, partition, offset и результат отправки:

```java
RecordMetadata metadata = producer.send(record).get();
System.out.printf("[PRODUCER-SAFE] ✅ SUCCESS: key=%s, partition=%d, offset=%d, orderId=%d%n", ...);
```
При ошибке — отдельная ветка `catch` с логом `❌ ERROR`.

## Задание 2. Consumer

Метод `runConsumerSafe()`. Настройки:

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
```

Обработка перед коммитом:

```java
// обработка записи (вывод в лог)
consumer.commitSync();
count++;
```

Если при обработке сообщения возникает исключение — `commitSync()` не вызывается, offset не сдвигается, сообщение будет вычитано повторно при следующем запуске этой же consumer group.

### Проверка корректности фиксации offset

Запусти `consumer-safe` дважды подряд с одной и той же group (`consumer-safe-group`), не запуская `init` между запусками:

```
docker compose -p kafka-training run --rm app consumer-safe consumer-safe-1 consumer-safe-group
docker compose -p kafka-training run --rm app consumer-safe consumer-safe-2 consumer-safe-group
```

Ожидаемый результат: первый запуск читает все 10 сообщений и коммитит offset, второй запуск с той же group читает 0 сообщений — offset уже зафиксирован на конце топика. Это подтверждает, что коммит происходит именно после обработки, а не "заранее".

## Результаты проверки

### 1. Producer — отправка 10 сообщений

```
[demo] bootstrap.servers=kafka:19092
========== PRODUCER SAFE — надежная доставка с acks=all, retries, idempotence ==========
[PRODUCER-SAFE] ✅ SUCCESS: key=10, partition=1, offset=0, orderId=1
[PRODUCER-SAFE] ✅ SUCCESS: key=20, partition=1, offset=1, orderId=2
[PRODUCER-SAFE] ✅ SUCCESS: key=30, partition=0, offset=0, orderId=3
[PRODUCER-SAFE] ✅ SUCCESS: key=10, partition=1, offset=2, orderId=4
[PRODUCER-SAFE] ✅ SUCCESS: key=20, partition=1, offset=3, orderId=5
[PRODUCER-SAFE] ✅ SUCCESS: key=30, partition=0, offset=1, orderId=6
[PRODUCER-SAFE] ✅ SUCCESS: key=10, partition=1, offset=4, orderId=7
[PRODUCER-SAFE] ✅ SUCCESS: key=20, partition=1, offset=5, orderId=8
[PRODUCER-SAFE] ✅ SUCCESS: key=30, partition=0, offset=2, orderId=9
[PRODUCER-SAFE] ✅ SUCCESS: key=10, partition=1, offset=6, orderId=10
[demo] Все сообщения отправлены с настройками надежности.
```

Все 10 сообщений отправлены успешно (`✅ SUCCESS`), для каждого залогированы key, partition, offset.

> **Примечание.** В одном из первых прогонов (до финальной версии `hw2.cmd`) наблюдалась ошибка на первом сообщении: `NotLeaderOrFollowerException` — Kafka ещё не успела определить лидера партиции сразу после пересоздания топика командой `init`. Проблема была устранена двумя правками: (1) убран жёсткий `RETRIES_CONFIG=5` — при `enable.idempotence=true` клиент сам использует практически неограниченные retries, а реальным лимитом служит `delivery.timeout.ms=120000`; (2) в `hw2.cmd` между `init` и `producer-safe` добавлена пауза 5 секунд для стабилизации лидера партиции. После этого все 10 сообщений отправляются успешно каждый раз.

### 2. Consumer — первый запуск (группа `consumer-safe-group`)

```
[demo] bootstrap.servers=kafka:19092
========== CONSUMER SAFE — name=consumer-safe-1, group=consumer-safe-group ==========
[demo] Подписались на топик: orders.events
[demo] Ожидаем сообщения... (таймаут 15 секунд)
[demo] Начинаем чтение...
[consumer-safe-1] PROCESSING: key=10, partition=1, offset=0, orderId=1, userId=10
[consumer-safe-1] ✅ COMMITTED: offset=0
[consumer-safe-1] PROCESSING: key=20, partition=1, offset=1, orderId=2, userId=20
[consumer-safe-1] ✅ COMMITTED: offset=1
[consumer-safe-1] PROCESSING: key=10, partition=1, offset=2, orderId=4, userId=10
[consumer-safe-1] ✅ COMMITTED: offset=2
[consumer-safe-1] PROCESSING: key=20, partition=1, offset=3, orderId=5, userId=20
[consumer-safe-1] ✅ COMMITTED: offset=3
[consumer-safe-1] PROCESSING: key=10, partition=1, offset=4, orderId=7, userId=10
[consumer-safe-1] ✅ COMMITTED: offset=4
[consumer-safe-1] PROCESSING: key=20, partition=1, offset=5, orderId=8, userId=20
[consumer-safe-1] ✅ COMMITTED: offset=5
[consumer-safe-1] PROCESSING: key=10, partition=1, offset=6, orderId=10, userId=10
[consumer-safe-1] ✅ COMMITTED: offset=6
[consumer-safe-1] PROCESSING: key=30, partition=0, offset=0, orderId=3, userId=30
[consumer-safe-1] ✅ COMMITTED: offset=0
[consumer-safe-1] PROCESSING: key=30, partition=0, offset=1, orderId=6, userId=30
[consumer-safe-1] ✅ COMMITTED: offset=1
[consumer-safe-1] PROCESSING: key=30, partition=0, offset=2, orderId=9, userId=30
[consumer-safe-1] ✅ COMMITTED: offset=2
[demo] Нет сообщений, продолжаем ждать...
[demo] Consumer 'consumer-safe-1' завершил работу. Обработано сообщений: 10
```

Все 10 сообщений вычитаны и обработаны. По логу видно чередование `PROCESSING → ✅ COMMITTED` для каждой записи — это подтверждает требование "offset подтверждается только после успешной обработки сообщения": коммит идёт сразу после обработки, запись за записью, а не пачкой в конце.

### 3. Consumer — повторный запуск той же группой (`consumer-safe-group`), подтверждение фиксации offset

Запущено той же группой, **без повторного `init`** (топик не трогали):

```bash
docker compose -p kafka-training run --rm app consumer-safe consumer-2 consumer-safe-group
```

```
[demo] bootstrap.servers=kafka:19092
========== CONSUMER SAFE — name=consumer-2, group=consumer-safe-group ==========
[demo] Подписались на топик: orders.events
[demo] Ожидаем сообщения... (таймаут 15 секунд)
[demo] Начинаем чтение...
[demo] Нет сообщений, продолжаем ждать...
[demo] Нет сообщений, продолжаем ждать...
...
[demo] Consumer 'consumer-2' завершил работу. Обработано сообщений: 0
```

**Это и есть подтверждение корректной фиксации offset.** Топик не пересоздавался между двумя запусками — сообщения физически остались в партициях. Но второй consumer с той же group читает 0 сообщений, потому что offset для group `consumer-safe-group` уже был закоммичен в конец топика после первого прогона (`consumer.commitSync()` вызывался после обработки каждой записи). Если бы commit не срабатывал корректно (например, коммитился до обработки и терялся при сбое, либо не коммитился вовсе), второй запуск с той же группой заново прочитал бы все 10 сообщений с начала.

## Задание 3. Вопросы

**Что даёт `acks=all`?**
Producer получает подтверждение только после того, как запись зафиксирована всеми репликами в ISR (in-sync replicas), а не только лидером. Это защищает от потери сообщения при падении лидера сразу после записи — новый лидер, избранный из ISR, гарантированно уже содержит эту запись. В данном демо-стенде брокер один (`replication factor=1`), поэтому реального выигрыша в отказоустойчивости от репликации здесь нет — но `acks=all` всё равно меняет семантику ack на «наиболее строгую», что важно продемонстрировать как правильную настройку для продакшена с несколькими брокерами.

**Зачем нужны `retries`?**
Чтобы producer сам, без вмешательства приложения, повторял отправку при временных сбоях (кратковременная недоступность брокера, выбор нового лидера партиции, таймаут сети), вместо того чтобы сразу считать сообщение потерянным и пробрасывать ошибку наверх.

**Какую проблему решает `enable.idempotence`?**
Без идемпотентности повтор отправки при `retries` может привести к дублированию сообщения в партиции (если первая попытка на самом деле дошла до брокера, но ack не успел вернуться до таймаута). `enable.idempotence=true` присваивает producer'у PID и sequence number для каждой партиции — брокер по ним отбрасывает дубликаты повторной отправки, гарантируя ровно одну запись на партицию при retries.

**Почему commit offset выполняется после обработки сообщения?**
Если закоммитить offset до обработки, а обработка после этого упадёт (или упадёт сам consumer), сообщение будет считаться прочитанным и потеряется безвозвратно — offset уже сдвинут. Коммит после успешной обработки даёт at-least-once семантику: при сбое между обработкой и коммитом сообщение будет вычитано повторно, что безопаснее потери данных (при условии, что обработка идемпотентна либо допускает повтор).

🛠️ Технологии
Компонент	Версия
Java	21
Apache Kafka	4.3.1 (KRaft mode)
PostgreSQL	16
Docker Compose	latest
Maven	3.9.9

👨‍🎓 Автор
Имя: [Ilyas]
Курс: Otus Kafka Training
Дата: [2026-09-06]
