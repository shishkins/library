CREATE SERVER pg_host
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'pg-host', dbname 'dep_spb', port '5432');

CREATE USER MAPPING
    FOR public
    SERVER pg_host
    OPTIONS (user 'cdc_foreign_tables', password '*********'); -- service user credentials

CREATE FOREIGN TABLE schema.table (
    product_id uuid NULL,
    *
)
SERVER pg_host
OPTIONS (table_name 'remote_table', schema_name 'remote_schema'); --

-- for clickhouse same usage, but not postgres_fdw. instead of this - clickhouse_fdw