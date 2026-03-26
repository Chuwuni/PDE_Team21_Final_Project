# PDE Team 21 

## Обзор

Этот проект реализует комплексный конвейер обработки данных для аналитики с использованием современных технологий в области инженерии данных. Решение демонстрирует эффективные практики в области обработки, хранения и визуализации данных.

## Архитектура 

В решении используются следующие технологии:

- **Apache Airflow** — оркестрация и планирование задач
- **PostgreSQL** — хранение данных в хранилище
- **MinIO** — S3-совместимое озеро данных (data lake)
- **pgAdmin** — интерфейс управления базой данных
- **Jupyter** — интерактивная разработка и анализ

## Начало работы

### Требования

- Docker и Docker Compose (v2.0+)
- Git
- Python 3.9+ (для локальной разработки)

### Установка и запуск


```bash
git clone https://github.com/Chuwuni/PDE_Team21_Final_Project.git -> cd PDE_Team21_Final_Project

docker-compose up -d
```

### Работа в UI

http://localhost:8080/ - для Airflow UI (логин: airflow / пароль: airflow)

Другие локальные порты при необходимости:

pgAdmin: http://localhost:5050 (admin@admin.com / admin)

MinIO Console: http://localhost:9001 (minioadmin / minioadmin)



