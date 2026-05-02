# Loan-Approval
# 🚀 Scalable Loan Approval Prediction System

Данный проект представляет собой событийно-ориентированную (event-driven) микросервисную систему для автоматического предсказания одобрения кредита в реальном времени. 

Проект разработан в рамках творческого экзамена по дисциплине **«Scientific Research in the Field of Information Technology»** (Kozybayev University / University of Arizona).

## 📋 Описание проекта (Project Description)
Система принимает финансовые данные пользователя (доход, статус занятости, кредитный рейтинг и запрашиваемую сумму) и с помощью обученной модели машинного обучения (Logistic Regression) выдает решение об одобрении кредита. 

Главная особенность проекта — использование **Apache Kafka** в качестве брокера сообщений. Это позволяет полностью отвязать веб-сервер (Node.js) от вычислительно сложного модуля машинного обучения (Python), обеспечивая высокую пропускную способность, отказоустойчивость и возможность горизонтального масштабирования консюмеров при высоких нагрузках.

## ✨ Ключевые возможности (Features)
* **Real-time предсказания:** Мгновенная доставка результатов на клиентскую часть с использованием WebSockets (Socket.io).
* **Горизонтальное масштабирование:** Архитектура Kafka позволяет запускать несколько экземпляров ML-консюмеров в одной Consumer Group для параллельной обработки запросов без потери данных.
* **Безопасность:** Реализована регистрация и авторизация пользователей с помощью JWT-токенов.
* **Сохранение истории (Logging):** Все входящие запросы и результаты предсказаний логируются в реляционную базу данных PostgreSQL.
* **Нагрузочное тестирование:** Проект оптимизирован и протестирован с помощью Artillery (держит нагрузку 10 req/sec без ошибок с задержкой < 8ms).

## 🛠 Технологический стек (Tech Stack)
* **Backend:** Node.js, Express.js
* **Message Broker:** Apache Kafka (KRaft mode)
* **Machine Learning:** Python 3, scikit-learn, joblib, numpy
* **Database:** PostgreSQL, node-postgres (pg)
* **Real-time Communication:** Socket.io
* **Load Testing:** Artillery

## 🏗 Архитектура системы (How it works)
1. **Клиент** отправляет POST-запрос с финансовыми данными на Node.js сервер.
2. **Node.js Producer** валидирует данные и отправляет сообщение в топик Kafka `dss-ml-model-input`.
3. **Python ML Consumer** (или несколько консюмеров параллельно) подхватывает сообщение из топика, масштабирует признаки (StandardScaler) и прогоняет их через ML-модель.
4. **Python ML Producer** отправляет результат (Yes/No) в топики `dss-ml-model-output` и `dss-ml-model-output-logs`.
5. **Node.js Consumer** получает результат, сохраняет логи в PostgreSQL и мгновенно отправляет ответ клиенту через WebSocket.

## 🚀 Запуск проекта (Getting Started)

### 1. Запуск Apache Kafka (без Zookeeper / KRaft)
```bash
# Форматирование хранилища (выполняется один раз)
kafka-storage.bat format -t <ВАШ_UUID> -c .\config\server.properties

# Запуск сервера
kafka-server-start.bat .\config\server.properties
