# Kafka Training Course

Репозиторий для выполнения домашних заданий по курсу Kafka.

---

## Домашние задания

| # | Название | Ветка | Статус |
|---|----------|-------|--------|
| 1 | Basic Producer/Consumer | [feature/homework-1](https://github.com/mowertii/kafka-training/tree/feature/homework-1) | ✅ Готово |
| 2 | Reliable Delivery | [feature/homework-2](https://github.com/mowertii/kafka-training/tree/feature/homework-2) | ✅ Готово |
| 3 | Error Handling: Retry, DLT, Backoff | [feature/homework-3](https://github.com/mowertii/kafka-training/tree/feature/homework-3) | ✅ Готово |
| 4 | Idempotent consumer and inbox; replay and deduplication | [feature/homework-4](https://github.com/mowertii/kafka-training/tree/feature/homework-4) | ✅ Готово |

---

## Как работать

```bash
# Клонируем репозиторий
git clone git@github.com:mowertii/kafka-training.git
cd kafka-training

# Переключаемся на нужное ДЗ
git checkout feature/homework-1

# Читаем README.md внутри ветки
# Запускаем проект
./start.cmd
./demo.cmd init
./demo.cmd producer
или одним файлом (если есть)
hw2.cmd, hw3.cmd etc
