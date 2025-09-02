CREATE DATABASE IF NOT EXISTS space;

/* Сырая таблица: храним исходный json как строку
 payload_hash - ключ для дедупликации одинаковых json */
CREATE TABLE space.raw_data
(
    raw_json String,
    _inserted_at DateTime DEFAULT now(),
    payload_hash UInt64 MATERIALIZED cityHash64(raw_json)
)
ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY (payload_hash);

/* Таблица людей, куда пишет MV
ReplacingMergeTree защитит от дублей по (craft,name),
версия - _inserted_at (оставит последнюю) */
CREATE TABLE space.people
(
    craft         String,
    name          String,
    _inserted_at  DateTime
)
ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY (craft, name);

/*  MV: разворачиваем массив people из сырого json */
CREATE MATERIALIZED VIEW space.raw_data_mv
TO space.people AS
SELECT
    JSONExtractString(person, 'craft') AS craft,
    JSONExtractString(person, 'name')  AS name,
    _inserted_at
FROM
(
    SELECT
        _inserted_at,
        arrayJoin(JSONExtractArrayRaw(raw_json, 'people')) AS person
    FROM space.raw_data
);


SELECT * FROM space.raw_data;
SELECT * FROM space.people ORDER BY _inserted_at DESC;
OPTIMIZE TABLE space.raw_data FINAL;
OPTIMIZE TABLE space.people FINAL;


SELECT * FROM space.current_people;
SELECT * FROM space.people_by_craft;
