# 📝 ДЗ №4 — Идемпотентный Consumer и Inbox Pattern

## 📌 Описание

Домашнее задание посвящено **идемпотентной обработке** сообщений в Kafka с использованием паттерна **Inbox**.

- **Идемпотентность** — повторная обработка одного и того же события не изменяет результат
- **Inbox Pattern** — таблица для хранения `eventId` уже обработанных событий
- **Транзакционность** — проверка дубля и бизнес-операция выполняются в одной транзакции БД

**Цель работы:**  
Научиться строить Kafka-consumer, который корректно переживает повторную доставку сообщений, не нарушая согласованность бизнес-данных.

---

## ⚙️ Требования

| Компонент | Версия |
|-----------|--------|
| **Docker** | 20.10+ |
| **Docker Compose** | 2.0+ |
| **Java** | 21 (в контейнере) |
| **Kafka** | 4.3.1 (KRaft) |
| **PostgreSQL** | 16 |

---

## 🚀 Запуск

### 1. Клонирование
```bash
git clone git@github.com:mowertii/kafka-training.git
cd kafka-training
```

## Запуск ДЗ №4
Windows (CMD/PowerShell):

```cmd
hw4.cmd
```
Linux / macOS / Git Bash:
```bash
./hw4.sh
```
## Что произойдёт автоматически
Шаг	Команда	Действие	Результат
1	init	Создание топиков и таблиц	11 топиков + 6 таблиц
2	producer-dup	Отправка 5 уникальных + 1 дублированного ×3	8 сообщений в orders.events
3	consumer-idempotent	Обработка с Inbox Pattern	6 новых + 2 пропущенных дубля

## 📊 Таблицы БД
Таблица |	Назначение
orders |	Основная таблица заказов
outbox |	Transactional Outbox
inbox |	Обработанные eventId (ключевое для HW4)
billing_payments |	Платежи
order_projection |	CQRS read-model
hw4_processed_orders |	Результаты обработки событий HW4

## 📈 Реальные логи выполнения
# Producer DUP
```text
========== PRODUCER DUP — уникальные события + намеренный дубль по eventId ==========
[demo] send topic=orders.events partition=0 offset=0 key=hw4-order-1 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=1 key=hw4-order-2 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=2 key=hw4-order-3 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=3 key=hw4-order-4 eventType=OrderCreated
[demo] send topic=orders.events partition=0 offset=4 key=hw4-order-5 eventType=OrderCreated
[demo] Отправляем eventId=1cd7db62-0305-46bd-83b2-43be0879ba61 ТРИ раза подряд
[demo] dup-send attempt=1 partition=2 offset=0 eventId=1cd7db62-0305-46bd-83b2-43be0879ba61
[demo] dup-send attempt=2 partition=2 offset=1 eventId=1cd7db62-0305-46bd-83b2-43be0879ba61
[demo] dup-send attempt=3 partition=2 offset=2 eventId=1cd7db62-0305-46bd-83b2-43be0879ba61
[demo] Готово: 5 уникальных событий + 1 событие продублировано 3 раза.
```
# Consumer IDEMPOTENT
```text
========== CONSUMER IDEMPOTENT — name=consumer-idempotent-1, group=consumer-idempotent-group ==========
[demo] Подписались на топик: orders.events
[demo] Нет сообщений, продолжаем ждать...
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=0667c8ba-... orderId=hw4-order-1 amount=100 — записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=32891d3d-... orderId=hw4-order-2 amount=200 — записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=45507fb6-... orderId=hw4-order-3 amount=300 — записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=8c05b8a3-... orderId=hw4-order-4 amount=400 — записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=bd2dbfc7-... orderId=hw4-order-5 amount=500 — записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ✅ PROCESSED: eventId=1cd7db62-... orderId=hw4-order-DUP amount=777 — записано в hw4_processed_orders и inbox
[demo] [consumer-idempotent-1] ⚠️ DUPLICATE SKIPPED: eventId=1cd7db62-... orderId=hw4-order-DUP — уже обработано, пропускаем
[demo] [consumer-idempotent-1] ⚠️ DUPLICATE SKIPPED: eventId=1cd7db62-... orderId=hw4-order-DUP — уже обработано, пропускаем
[demo] Consumer 'consumer-idempotent-1' завершил работу. Обработано новых: 6, пропущено дублей: 2
```
# Ключевые строки:
✅ PROCESSED — 6 раз (5 уникальных + 1 первое из дублей)
⚠️ DUPLICATE SKIPPED — 2 раза (повторные доставки)
📊 Итог: Обработано новых: 6, пропущено дублей: 2

## ❓ Вопросы и ответы (из ТЗ)
### Какую проблему решает Transactional Outbox?
* Проблема dual-write. Когда приложение должно одновременно:
Сохранить данные в свою БД (например, создать заказ)
Отправить событие в Kafka (например, OrderCreated)
— оно пишет в две разные системы, и между этими записями нет атомарности. Это классическая проблема distributed systems, известная как dual-write problem.

**Что может пойти не так:**

| Сценарий | Что произошло | Последствие |
|----------|---------------|-------------|
| 1 | `save()` OK, `send()` FAIL | Заказ в БД есть, Kafka не знает → downstream рассинхронизирован |
| 2 | `save()` OK, приложение упало перед `send()` | То же самое |
| 3 | `save()` FAIL, `send()` OK | Событие об уже не существующем заказе → ещё хуже |
| 4 | `save()` OK, `send()` OK, но ack потерялся | Дубль события в Kafka |

# Transactional Outbox решает эту проблему так:
Бизнес-данные и событие пишутся в одну БД — в одной транзакции. Либо оба INSERT'а проходят, либо ни один.
Событие пока не уходит в Kafka — оно просто лежит в таблице outbox со статусом published=false.
Отдельный publisher (relay) читает outbox и отправляет события в Kafka после успешного коммита бизнес-транзакции.
Если Kafka недоступна — publisher ретраит, а событие остаётся в outbox до успеха.

Итог: приложение больше не зависит от доступности Kafka в момент бизнес-операции. Событие гарантированно попадёт в Kafka (at-least-once), даже если брокер временно лежит.

# Почему недостаточно последовательно выполнить save() и producer.send()?
### Почему недостаточно последовательно выполнить `save()` и `producer.send()`?

Потому что **между этими двумя вызовами нет транзакции**. Это две независимые операции над двумя разными системами (БД и Kafka), и между ними может произойти что угодно.

**Некорректный код:**

```java
// ❌ ПЛОХО
orderRepository.save(order);       // ← строка 1
kafkaProducer.send(event);         // ← строка 2
```

**Проблемы:**

1. **Разрыв между строками 1 и 2.**  
   Если приложение упадёт (crash, OOM, kill -9), сеть отвалится, или БД/Kafka временно недоступны **после** `save()`, но **до** `send()` — событие никогда не уйдёт в Kafka. Заказ в БД есть, downstream ничего не знает.

2. **Обратный порядок — тоже плохо.**  
   Если сначала `send()`, потом `save()`, и `save()` упадёт — событие в Kafka уже улетело, но заказа нет. Consumers начнут обрабатывать событие о несуществующем заказе.

3. **Нет ретраев и идемпотентности.**  
   Даже если обернуть `send()` в `try/catch` и повторять при ошибке — нет гарантии, что повторная отправка не создаст дубль (Kafka по умолчанию at-least-once). А если приложение упадёт в момент ретрая — событие потеряется.

4. **Нет «точки истины».**  
   Приложение не знает, что именно уже отправлено, а что — нет. Нет таблицы, по которой можно восстановиться после сбоя.

**С Transactional Outbox:**

```java
// ✅ ХОРОШО
@Transactional
void createOrder(Order order) {
    orderRepository.save(order);
    outboxRepository.save(new OutboxEvent(order)); // ← в одной транзакции!
}
// Отдельный publisher читает outbox и шлёт в Kafka
```

- **Атомарность:** либо и заказ, и событие записаны, либо ничего.
- **Восстановление:** после сбоя publisher видит все `published=false` записи и отправляет их.
- **Идемпотентность:** publisher может ретраить сколько угодно — он просто помечает запись `published=true` после успеха.
- **Развязка:** бизнес-транзакция не зависит от доступности Kafka.

**Ключевая мысль:** `save()` + `producer.send()` — это **распределённая транзакция без координатора**. Такие транзакции **не работают** без дополнительных паттернов (Outbox, Saga, 2PC). Outbox — самый простой и надёжный способ решить эту проблему без распределённых транзакций.

---

## 👨‍🎓 Автор

**Имя:** [Ilyas]  
**Курс:** Otus "Администрирование платформы Apache Kafka"  
**Дата:** 2026-09-11  