Короткий пайплайн:
- Python качает JSON с http://api.open-notify.org/astros.json и вставляет “сырые” данные в ClickHouse (только INSERT).
- Materialized View парсит массив people в таблицу space.people.
- Дедупликация - силами ReplacingMergeTree + OPTIMIZE.
- dbt-модели: current_people(актуальные люди) и people_by_craft(агрегация).

1.	Поднимаем CH в докер:
docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 -v clickhouse_data:/var/lib/clickhouse clickhouse/clickhouse-server:latest
<img width="974" height="357" alt="image" src="https://github.com/user-attachments/assets/9a54d0c8-316e-43e4-bb0f-d3ecaa40f1f7" />

volume для сохранения данных (-v clickhouse_data:/var/lib/clickhouse)
орты для клиента (9000) и HTTP-интерфейса (8123)

Устанавливаем библиотеку pip install clickhouse-connect requests для вставки данных в сыром виде json.
После запуска main.py

<img width="488" height="101" alt="image" src="https://github.com/user-attachments/assets/2d3a4235-8405-4bfa-9f4f-c3c7e78a73f6" />

 
Ретрай нарастающий таймаут 1, 2, 4, 8, 16 сек
200 — успех, 429 и 5xx — пробуем ещё, прочие 4xx — сразу ошибка
Только insert

Для sql-запросов(Script.sql) использовал dbeaver
Одинаковый json даёт одинаковый payload_hash
ReplacingMergeTree оставляет последнюю версию при слияниях
OPTIMIZE чтобы увидеть дедупликацию сразу(чтобы не ждать фоновые слияния и сразу увидеть, как ReplacingMergeTree убрал дубли) 

<img width="974" height="93" alt="image" src="https://github.com/user-attachments/assets/c6648422-33a4-4c29-98db-dc92f8fb4419" />

2 задание

<img width="651" height="386" alt="image" src="https://github.com/user-attachments/assets/abe8531f-a3ba-4cf3-bfff-c3ae92f0b22b" />

3 задание


4. Устанавливаем dbt pip install dbt-core dbt-clickhouse
Инициализируем проект dbt init space_project
Настраиваем profiles.yml, project.yml(все модели будут view), sources.yml(чтобы ссылаться  на таблицы как source).
Модель 1 - уникальные люди - models/current_people.sql – это view которая отдаёт одну запись на человека(самую свежую)
Модель 2 - количество уникальных людей по craft - models/people_by_craft.sql
Запуск:
dbt debug – проверить соединение
dbt run – создать view 
dbt ls – посмотреть список моделей

<img width="526" height="196" alt="image" src="https://github.com/user-attachments/assets/12dbecda-d80f-46c6-8485-49a7dc3c3a11" />

 

