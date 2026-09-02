# Kafka Homework 1 — Basic Producer/Consumer

## 📌 Описание
Домашнее задание по теме "Базовый producer/consumer, ключи сообщений, partitioning и consumer groups".

**Цель работы:**  
Реализовать взаимодействие producer и consumer в Kafka с использованием ключей сообщений, partitioning и consumer groups для проверки распределения сообщений между partitions и организации независимого чтения данных.

---

## 📁 Структура проекта

```
kafka-training/
├── src/
│   └── main/
│       └── java/
│           └── ru/
│               └── otus/
│                   └── kafka/
│                       └── training/
│                           └── TrainingApp.java   # Основной класс с producer и consumer
├── docker-compose.yml       # Docker Compose с Kafka (KRaft) и PostgreSQL
├── Dockerfile               # Сборка Java-приложения
├── pom.xml                  # Maven зависимости
├── demo.cmd                 # Запуск демо-режимов
├── start.cmd                # Запуск инфраструктуры
├── status.cmd               # Статус контейнеров
├── stop.cmd                 # Остановка и очистка
└── README.md                # Этот файл
```

---

## ⚙️ Требования

- **Docker Desktop** (с WSL2)
- **Java 21** (для локального запуска)
- **Git** (для клонирования)

---

## 🚀 Запуск

### 1. Клонирование репозитория
```bash
git clone git@github.com:username/kafka-homework-1.git
cd kafka-homework-1
```

### 2. Запуск инфраструктуры
```bash
start.cmd
```

### 3. Инициализация (создание топиков и таблиц)
```bash
demo.cmd init
```

### 4. Запуск Producer
```bash
demo.cmd producer
```

**Вывод Producer:**  
Каждое сообщение выводит:
- `topic` — имя топика
- `partition` — номер партиции
- `offset` — смещение в партиции
- `key` — userId (используется для маршрутизации)

### 5. Запуск Consumer

**Первый consumer (группа order-group-1):**
```bash
demo.cmd consumer consumer-1 order-group-1
```

**Второй consumer (та же группа) — в новой консоли:**
```bash
demo.cmd consumer consumer-2 order-group-1
```

**Третий consumer (другая группа) — в новой консоли:**
```bash
demo.cmd consumer consumer-3 order-group-2
```

---

## ✅ Результаты проверки

### 1. Распределение сообщений по партициям

| userId | Партиция | Количество сообщений |
|--------|----------|---------------------|
| 10     | 0        | 4                   |
| 20     | 1        | 4                   |
| 30     | 2        | 4                   |

**Вывод:** Все сообщения с одинаковым `userId` попали в одну партицию.  
Это подтверждает, что `key` используется для вычисления партиции (`hash(key) % partitions`).

---

### 2. Consumer Group 1 (`order-group-1`)

| Consumer  | Полученные партиции |
|-----------|---------------------|
| consumer-1 | 0, 2                |
| consumer-2 | 1                   |

**Вывод:** Два consumer в одной группе делят партиции между собой.  
Каждый consumer обрабатывает свою часть сообщений.

---

### 3. Consumer Group 2 (`order-group-2`)

| Consumer  | Полученные партиции |
|-----------|---------------------|
| consumer-3 | 0, 1, 2             |

**Вывод:** Consumer с новой группой получает все партиции независимо от других групп.  
Это позволяет разным сервисам читать одни и те же данные без конфликтов.

---

## 📊 Логи выполнения

### Producer
```
[demo] send topic=orders.events partition=0 offset=0 key=10 eventType=OrderPlaced
[demo] send topic=orders.events partition=1 offset=0 key=20 eventType=OrderPlaced
[demo] send topic=orders.events partition=2 offset=0 key=30 eventType=OrderPlaced
[demo] send topic=orders.events partition=0 offset=1 key=10 eventType=OrderPlaced
...
```

### Consumer (group=order-group-1)
```
[consumer-1] CONSUMER: group=order-group-1, key=10, partition=0, offset=0, orderId=1, userId=10, product=Keyboard
[consumer-2] CONSUMER: group=order-group-1, key=20, partition=1, offset=0, orderId=2, userId=20, product=Mouse
[consumer-1] CONSUMER: group=order-group-1, key=30, partition=2, offset=0, orderId=3, userId=30, product=Monitor
...
```

### Consumer (group=order-group-2)
```
[consumer-3] CONSUMER: group=order-group-2, key=10, partition=0, offset=0, orderId=1, userId=10, product=Keyboard
[consumer-3] CONSUMER: group=order-group-2, key=20, partition=1, offset=0, orderId=2, userId=20, product=Mouse
[consumer-3] CONSUMER: group=order-group-2, key=30, partition=2, offset=0, orderId=3, userId=30, product=Monitor
...
```

---

## 🧠 Выводы

1. **Partitioning по ключу:**  
   Сообщения с одинаковым `key` всегда попадают в одну партицию.  
   Это гарантирует порядок обработки для одного `userId`.

2. **Consumer Groups:**  
   - В одной группе — партиции делятся между consumer (масштабирование).  
   - В разных группах — каждая группа получает все сообщения (разные бизнес-функции).

3. **Offset:**  
   Каждое сообщение имеет уникальный offset внутри партиции.  
   Offset позволяет consumer управлять позицией чтения.

4. **Auto-offset-reset=earliest:**  
   Позволяет новой группе прочитать все сообщения с начала.

---

## 🛠️ Технологии

| Компонент | Версия |
|-----------|--------|
| Java | 21 |
| Apache Kafka | 4.3.1 (KRaft mode) |
| PostgreSQL | 16 |
| Docker Compose | latest |
| Maven | 3.9.9 |

---

## 📝 Комментарии по коду

### Producer
- Использует `userId` в качестве `key`
- Отправляет 12 сообщений с разными `userId` (10, 20, 30)
- Выводит `topic`, `partition`, `offset` для каждого сообщения

### Consumer
- Поддерживает фиксированную `group.id` (без UUID)
- Читает с самого начала (`auto.offset.reset=earliest`)
- Использует ручной `commitSync()` для контроля оффсетов
- Выводит имя consumer, group, key, partition, offset и содержимое

---

## 📎 Ссылки

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [KRaft Mode (без ZooKeeper)](https://kafka.apache.org/documentation/#kraft)
- [Consumer Groups Explained](https://www.conduktor.io/kafka/kafka-consumer-groups/)

---

## 👨‍🎓 Автор

**Имя:** [Ilyas]  
**Курс:** Otus Kafka Training  
**Дата:** [2026-09-02]