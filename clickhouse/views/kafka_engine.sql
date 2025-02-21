CREATE TABLE kafka.`1c_cb_data`
(
    `object_id` String COMMENT 'Идентификатор объекта',
    `data_update` Array(Tuple(String, String)) COMMENT 'Данные на обновление/вставку',
    `data_delete` Array(Tuple(String, String)) COMMENT 'Данные на удаление',
    `timestamp` DateTime64(3, 'UTC') COMMENT 'Дата загрузки строки в 1С',
    `_dateload` DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') COMMENT 'Дата загрузки строки в таблицу',
    `_topic` String COMMENT 'Топик в kafka',
    `_partition` UInt64 COMMENT 'Партиция в kafka',
    `_offset` UInt64 COMMENT 'Оффсет сообщения в kafka',
    `_timestamp` DateTime64(3) COMMENT 'Дата загрузки строки в kafka'
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(_dateload)
ORDER BY (toYYYYMMDD(_dateload), toYYYYMMDD(timestamp), object_id)
TTL toDate(_dateload) + toIntervalDay(7)
SETTINGS index_granularity = 8192;

CREATE TABLE kafka.`1c_cb_queue`
(
    `message` String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '1,2,3',
    kafka_topic_list = 'external_cdc-dns-m_sink_fcs',
    kafka_group_name = 'example', kafka_format = 'RawBLOB';

CREATE MATERIALIZED VIEW kafka.`1c_cb_consumer` TO kafka.`1c_cb_data`
(
    `object_id` String,
    `data_update` Array(Tuple(String, String)),
    `data_delete` Array(Tuple(String, String)),
    `timestamp` Float64,
    `_topic` String,
    `_partition` UInt64,
    `_offset` UInt64,
    `_timestamp` DateTime64(3)
) AS
WITH _json_data AS
    (
        SELECT
            queue.message AS json,
            JSONExtract(JSONExtract(json, 'source', 'String'), 'table', 'String') AS object_id,
            JSONExtractKeysAndValues(json, 'after', 'String') AS update_data_arr,
            JSONExtractKeysAndValues(json, 'before', 'String') AS delete_data_arr,
            JSONExtractUInt(json, 'ts_ms') AS timestamp,
            queue._topic AS _topic,
            queue._partition AS _partition,
            queue._offset AS _offset,
            queue._timestamp AS _timestamp
        FROM kafka.`1c_cb_queue` AS queue
    )
SELECT
    _json_data.object_id AS object_id,
    _json_data.update_data_arr AS data_update,
    _json_data.delete_data_arr AS data_delete,
    _json_data.timestamp / 1000 AS timestamp,
    _json_data._topic AS _topic,
    _json_data._partition AS _partition,
    _json_data._offset AS _offset,
    _json_data._timestamp AS _timestamp
FROM _json_data;