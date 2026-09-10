# ДЗ №4 - Идемпотентный consumer и Inbox Pattern

## Как запустить проект

Требуется Docker и Docker Compose.

```bash
hw4.cmd
```

Скрипт выполняет три шага одним прогоном:
1. `init` - пересоздаёт топики и таблицы (в т.ч. `inbox` и `hw4_processed_orders`).
2. `producer-dup` - отправляет 5 обычных заказов с уникальными `eventId` и ещё одно сообщение с фиксированным `eventId`, отправленное 3 раза подряд.
3. `consumer-idempotent` - вычитывает все 8 сообщений за один проход: первую копию дублированного `eventId` обрабатывает как новую, следующие две - распознаёт как дубли и пропускает.

## Задание 1. Producer

Метод `runProducerWithDuplicates()`. Отправляет:
- 5 сообщений `OrderCreated` с уникальными `eventId` (`hw4-order-1` … `hw4-order-5`);
- 1 сообщение с **фиксированным** `eventId`, отправленное **3 раза подряд** - это и есть намеренный дубль.

## Задание 2. Consumer + Inbox

Метод `runConsumerIdempotent()`:
- `enable.auto.commit=false` - offset коммитится только вручную;
- перед бизнес-операцией проверяется наличие `eventId` в таблице `inbox`;
- если `eventId` уже есть - бизнес-операция (запись в `hw4_processed_orders`) **не выполняется повторно**;
- если `eventId` новый - запись в `hw4_processed_orders` и отметка в `inbox` выполняются в **одной транзакции** (`db.setAutoCommit(false)` → оба `INSERT` → `db.commit()`);
- `consumer.commitSync()` вызывается только после того, как транзакция в БД однозначно завершилась (успешно обработали или корректно распознали дубль).

## Задание 3. Проверка в логах

В выводе `consumer-idempotent` должно быть видно чередование:
```
[consumer-idempotent-1] ✅ PROCESSED: eventId=... orderId=hw4-order-DUP amount=777 - записано в hw4_processed_orders и inbox
[consumer-idempotent-1] ⚠️ DUPLICATE SKIPPED: eventId=... orderId=hw4-order-DUP - уже обработано, пропускаем
[consumer-idempotent-1] ⚠️ DUPLICATE SKIPPED: eventId=... orderId=hw4-order-DUP - уже обработано, пропускаем
```
- один и тот же `eventId` встречается три раза, но `✅ PROCESSED` только один раз.

### Результаты проверки

```
========== PRODUCER DUP - уникальные события + намеренный дубль по eventId ==========
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
[demo] send topic=orders.events partition=0 offset=0 key=hw4-order-1 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=1 key=hw4-order-2 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=2 key=hw4-order-3 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=3 key=hw4-order-4 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=4 key=hw4-order-5 eventType=OrderCreated
[demo] Отправляем eventId=1cd7db62-0305-46bd-83b2-43be0879ba61 ТРИ раза подряд (эмулируем повторную
доставку)
[demo] dup-send attempt=1 partition=2 offset=0 eventId=1cd7db62-0305-46bd-83b2-43be0879ba61
[demo] dup-send attempt=2 partition=2 offset=1 eventId=1cd7db62-0305-46bd-83b2-43be0879ba61
[demo] dup-send attempt=3 partition=2 offset=2 eventId=1cd7db62-0305-46bd-83b2-43be0879ba61
[demo] Готово: 5 уникальных событий + 1 событие продублировано 3 раза (один и тот же eventId).

```

```
========== CONSUMER IDEMPOTENT - name=consumer-idempotent-1, group=consumer-idempotent-group =======
===
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
[demo] Подписались на топик: orders.events
[demo] Нет сообщений, продолжаем ждать...
[demo] Нет сообщений, продолжаем ждать...
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=0667c8ba-c0dc-4453-aeec-fc751e491477 orderId=hw4
-order-1 amount=100 - записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=32891d3d-57d5-4b24-9de5-eb275e69bab1 orderId=hw4
-order-2 amount=200 - записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=45507fb6-6d5d-4320-8293-8db7930cb21c orderId=hw4
-order-3 amount=300 - записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=8c05b8a3-f5d8-4ef2-a4de-64abb125178f orderId=hw4
-order-4 amount=400 - записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=bd2dbfc7-8309-4282-97e4-e5fa79f21f44 orderId=hw4
-order-5 amount=500 - записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=1cd7db62-0305-46bd-83b2-43be0879ba61 orderId=hw4
-order-DUP amount=777 - записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ⚠️ DUPLICATE SKIPPED: eventId=1cd7db62-0305-46bd-83b2-43be0879ba61 or
derId=hw4-order-DUP - уже обработано, пропускаем
[demo] [consumer-idempotent-1] ⚠️ DUPLICATE SKIPPED: eventId=1cd7db62-0305-46bd-83b2-43be0879ba61 or
derId=hw4-order-DUP - уже обработано, пропускаем
[demo] Нет сообщений, продолжаем ждать...
[demo] Consumer 'consumer-idempotent-1' завершил работу. Обработано новых: 6, пропущено дублей: 2

```

## Задание 4. Вопросы

**Почему Kafka может доставить сообщение повторно?**

Kafka гарантирует **at-least-once** доставку по умолчанию, а не *exactly-once* - то есть сообщение может быть доставлено один раз или больше, но никогда не будет потеряно молча. Основные причины повторной доставки:
- **Producer делает retry.** Если producer не получил `ack` вовремя (таймаут, кратковременная недоступность брокера) - но брокер на самом деле уже принял сообщение, - producer повторяет отправку. Без `enable.idempotence=true` это создаёт физический дубль в партиции (два разных offset с одинаковыми данными).
- **Consumer не успел закоммитить offset.** Consumer читает сообщение, начинает его обрабатывать, но падает или перезапускается (crash, rebalance, deploy) до вызова `commitSync()`. При следующем запуске он продолжает с последнего **закоммиченного** offset - то есть получает уже обработанное сообщение снова.
- **Ребалансировка группы.** Если партиция переходит другому consumer'у в момент, когда предыдущий владелец уже обработал сообщение, но не успел закоммитить offset, новый владелец партиции прочитает то же сообщение.

Ни один из этих случаев не является багом Kafka - это осознанный компромисс: гарантировать «не потеряем» дешевле и надёжнее, чем гарантировать «доставим ровно один раз» на уровне брокера, поэтому защита от дублей - задача потребителя.

**Как Inbox делает consumer идемпотентным?**

Inbox Pattern превращает «сообщение может прийти больше одного раза» в «бизнес-эффект от сообщения произойдёт ровно один раз», не полагаясь на то, что Kafka что-то дедуплицирует сама:

1. У каждого события есть стабильный `eventId`, который не меняется при повторной доставке.
2. Перед выполнением бизнес-операции consumer проверяет: есть ли уже такой `eventId` в таблице `inbox`.
3. Если есть - операция уже выполнялась, повторять её нельзя (иначе, например, платёж спишется дважды).
4. Если нет - бизнес-операция и запись `eventId` в `inbox` выполняются в **одной транзакции БД**. Это ключевой момент: если бы это были два отдельных шага, между ними мог случиться сбой (сохранили результат, но не отметили в inbox - при повторной доставке сделаем операцию ещё раз; или наоборот), а одна транзакция гарантирует, что либо случится и то, и другое, либо не случится ничего.

В результате Kafka может доставить сообщение сколько угодно раз - но БД физически не позволит бизнес-операции выполниться повторно для того же `eventId`.

## Известное ограничение (для честности)

Проверка `select 1 from inbox where event_id=...` не использует `FOR UPDATE`, потому что `FOR UPDATE` не может заблокировать **ещё не существующую** строку - это не дало бы реальной защиты от гонки. Настоящая защита от параллельной обработки одного и того же `eventId` - это `PRIMARY KEY` на `inbox.event_id`: при попытке вставить дубль параллельно транзакция получит constraint violation. В данной демонстрации конкуренции нет: сообщения с одинаковым ключом всегда попадают в одну партицию и обрабатываются одним consumer'ом строго последовательно, поэтому простой проверки "select, потом insert" достаточно. Для сценария с несколькими параллельными инстансами того же consumer'а (что на практике не бывает в рамках одной партиции) стоило бы дополнительно ловить `SQLException` с кодом `23505` (unique violation) на `INSERT` и трактовать его как обнаруженный дубль.

---

## 👨‍🎓 Автор

**Имя:** [Ilyas]  
**Курс:** Otus "Администрирование платформы Apache Kafka"
**Дата:** [2026-09-10]