# Kafka Homework 1 - Basic Producer/Consumer

## Описание
Домашнее задание по теме "Базовый producer/consumer, ключи сообщений, partitioning и consumer groups".

## Выполненные задачи

### 1. Настройка Kafka
- Запущен Kafka через Docker Compose (KRaft mode)
- Создан топик `orders.events` с 3 партициями

### 2. Реализация Producer
- Отправлено 12 сообщений в топик `orders.events`
- В качестве ключа используется `userId`
- Вывод информации о каждой отправке: key, partition, offset

### 3. Реализация Consumer
- Чтение сообщений из топика `orders.events`
- Вывод: имя consumer, group.id, key, partition, offset, сообщение

### 4. Проверка Consumer Groups
- Запущено 2 consumer с одинаковым `group.id=order-group-1`
- Партиции распределились между ними
- Запущен consumer с `group.id=order-group-2`
- Новая группа прочитала все сообщения независимо

### 5. Проверка Partitioning
- Сообщения с одинаковым `userId` попадают в одну партицию
- userId=10 → partition 0
- userId=20 → partition 1
- userId=30 → partition 2

## Результаты проверки

### Consumer Group 1 (order-group-1)
consumer-1: получил партиции 0 и 2
consumer-2: получил партицию 1

### Consumer Group 2 (order-group-2)
consumer-3: получил все партиции (0, 1, 2)

### Partitioning
| userId | Partition | Количество сообщений |
|--------|-----------|---------------------|
| 10     | 0         | 4                   |
| 20     | 1         | 4                   |
| 30     | 2         | 4                   |

## Запуск

```bash
# Поднять инфраструктуру
docker-compose up -d

# Инициализация (создание топиков)
./demo.cmd init

# Запуск Producer
./demo.cmd producer

# Запуск Consumer (в разных консолях)
./demo.cmd consumer consumer-1 order-group-1
./demo.cmd consumer consumer-2 order-group-1
./demo.cmd consumer consumer-3 order-group-2

##*Технологии*
Java 21

Apache Kafka 4.3.1

Docker Compose

PostgreSQL 16 (для outbox/inbox паттернов)

