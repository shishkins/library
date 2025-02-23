/*
 DDL/DML запросы для создания реплицированного query_log между нодами CH с буффером, который не позволяет нагружаться ZooKeeper-у
 */

DROP TABLE IF EXISTS default.replicated_query_log ON CLUSTER "{cluster}";

CREATE TABLE default.replicated_query_log ON CLUSTER "{cluster}"
(
    `type` Enum8('QueryStart' = 1, 'QueryFinish' = 2, 'ExceptionBeforeStart' = 3, 'ExceptionWhileProcessing' = 4),
    `event_date` Date,
    `event_time` DateTime,
    `event_time_microseconds` DateTime64(6),
    `query_start_time` DateTime,
    `query_start_time_microseconds` DateTime64(6),
    `query_duration_ms` UInt64,
    `read_rows` UInt64,
    `read_bytes` UInt64,
    `written_rows` UInt64,
    `written_bytes` UInt64,
    `result_rows` UInt64,
    `result_bytes` UInt64,
    `memory_usage` UInt64,
    `current_database` String,
    `query` String,
    `formatted_query` String,
    `normalized_query_hash` UInt64,
    `query_kind` LowCardinality(String),
    `databases` Array(LowCardinality(String)),
    `tables` Array(LowCardinality(String)),
    `columns` Array(LowCardinality(String)),
    `projections` Array(LowCardinality(String)),
    `views` Array(LowCardinality(String)),
    `exception_code` Int32,
    `exception` String,
    `stack_trace` String,
    `is_initial_query` UInt8,
    `user` String,
    `query_id` String,
    `address` IPv6,
    `port` UInt16,
    `initial_user` String,
    `initial_query_id` String,
    `initial_address` IPv6,
    `initial_port` UInt16,
    `initial_query_start_time` DateTime,
    `initial_query_start_time_microseconds` DateTime64(6),
    `interface` UInt8,
    `is_secure` UInt8,
    `os_user` String,
    `client_hostname` String,
    `client_name` String,
    `client_revision` UInt32,
    `client_version_major` UInt32,
    `client_version_minor` UInt32,
    `client_version_patch` UInt32,
    `http_method` UInt8,
    `http_user_agent` String,
    `http_referer` String,
    `forwarded_for` String,
    `quota_key` String,
    `distributed_depth` UInt64,
    `revision` UInt32,
    `log_comment` String,
    `thread_ids` Array(UInt64),
    `ProfileEvents` Map(String, UInt64),
    `Settings` Map(String, String),
    `used_aggregate_functions` Array(String),
    `used_aggregate_function_combinators` Array(String),
    `used_database_engines` Array(String),
    `used_data_type_families` Array(String),
    `used_dictionaries` Array(String),
    `used_formats` Array(String),
    `used_functions` Array(String),
    `used_storages` Array(String),
    `used_table_functions` Array(String),
    `used_row_policies` Array(LowCardinality(String)),
    `transaction_id` Tuple(UInt64, UInt64, UUID),
    `AsyncReadCounters` Map(String, UInt64),
    `ProfileEvents.Names` Array(String) ALIAS mapKeys(ProfileEvents),
    `ProfileEvents.Values` Array(UInt64) ALIAS mapValues(ProfileEvents),
    `Settings.Names` Array(String) ALIAS mapKeys(Settings),
    `Settings.Values` Array(String) ALIAS mapValues(Settings),
    `hostname` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{uuid}}/{{shard}}', '{{replica}}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
TTL event_time + INTERVAL 3 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE default.replicated_query_log_buffer ON CLUSTER fcs
(
    `type` Enum8('QueryStart' = 1, 'QueryFinish' = 2, 'ExceptionBeforeStart' = 3, 'ExceptionWhileProcessing' = 4),
    `event_date` Date,
    `event_time` DateTime,
    `event_time_microseconds` DateTime64(6),
    `query_start_time` DateTime,
    `query_start_time_microseconds` DateTime64(6),
    `query_duration_ms` UInt64,
    `read_rows` UInt64,
    `read_bytes` UInt64,
    `written_rows` UInt64,
    `written_bytes` UInt64,
    `result_rows` UInt64,
    `result_bytes` UInt64,
    `memory_usage` UInt64,
    `current_database` String,
    `query` String,
    `formatted_query` String,
    `normalized_query_hash` UInt64,
    `query_kind` LowCardinality(String),
    `databases` Array(LowCardinality(String)),
    `tables` Array(LowCardinality(String)),
    `columns` Array(LowCardinality(String)),
    `projections` Array(LowCardinality(String)),
    `views` Array(LowCardinality(String)),
    `exception_code` Int32,
    `exception` String,
    `stack_trace` String,
    `is_initial_query` UInt8,
    `user` String,
    `query_id` String,
    `address` IPv6,
    `port` UInt16,
    `initial_user` String,
    `initial_query_id` String,
    `initial_address` IPv6,
    `initial_port` UInt16,
    `initial_query_start_time` DateTime,
    `initial_query_start_time_microseconds` DateTime64(6),
    `interface` UInt8,
    `is_secure` UInt8,
    `os_user` String,
    `client_hostname` String,
    `client_name` String,
    `client_revision` UInt32,
    `client_version_major` UInt32,
    `client_version_minor` UInt32,
    `client_version_patch` UInt32,
    `http_method` UInt8,
    `http_user_agent` String,
    `http_referer` String,
    `forwarded_for` String,
    `quota_key` String,
    `distributed_depth` UInt64,
    `revision` UInt32,
    `log_comment` String,
    `thread_ids` Array(UInt64),
    `ProfileEvents` Map(String, UInt64),
    `Settings` Map(String, String),
    `used_aggregate_functions` Array(String),
    `used_aggregate_function_combinators` Array(String),
    `used_database_engines` Array(String),
    `used_data_type_families` Array(String),
    `used_dictionaries` Array(String),
    `used_formats` Array(String),
    `used_functions` Array(String),
    `used_storages` Array(String),
    `used_table_functions` Array(String),
    `used_row_policies` Array(LowCardinality(String)),
    `transaction_id` Tuple(UInt64, UInt64, UUID),
    `AsyncReadCounters` Map(String, UInt64),
    `ProfileEvents.Names` Array(String) ALIAS mapKeys(ProfileEvents),
    `ProfileEvents.Values` Array(UInt64) ALIAS mapValues(ProfileEvents),
    `Settings.Names` Array(String) ALIAS mapKeys(Settings),
    `Settings.Values` Array(String) ALIAS mapValues(Settings),
    `hostname` String
)
ENGINE = Buffer('default', 'replicated_query_log', 1, 120, 121, 1, 500, 1, 500000000);

CREATE ROW POLICY denie_buffer ON CLUSTER "fcs" ON default.replicated_query_log_buffer FOR SELECT USING 0 TO ALL; -- чтобы не уронить оперативку на ноде CH при обращении к буфферу

/*
 запрос к каждой ноде CH
 */
insert into default.replicated_query_log
select *, hostname() from system.query_log
where event_time < %(_timestamp)s;

CREATE MATERIALIZED VIEW default.`query_log_mv_trigger` ON CLUSTER "fcs" TO default.replicated_query_log_buffer
AS
SELECT *, hostname() AS hostname
FROM system.query_log;

-- ATTACH TABLE default.`query_log_mv_trigger` ON CLUSTER "{cluster}";

-- DETACH VIEW default.`query_log_mv_trigger` ON CLUSTER "{cluster}";