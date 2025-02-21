create_write_audit_function = '''
CREATE OR REPLACE FUNCTION public.write_audit()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    schema_name TEXT := 'history_tables';
    table_name TEXT := CONCAT(TG_TABLE_NAME, '_history');
    curr_user TEXT := SESSION_USER;
   curr_time timestamp := now();
BEGIN
    --
    -- Добавление строки в audit таблицу, которая отражает операцию, выполняемую в основной таблице,
    -- для определения типа операции применяется специальная переменная TG_OP.
    --
	SET ROLE "sudo";
    IF (curr_user = 'orp_airflow') THEN
        RETURN NULL;
    END IF;
    IF (TG_OP = 'DELETE') THEN
        EXECUTE 
            'INSERT INTO ' || 
            quote_ident(schema_name) || '.' || quote_ident(table_name) ||
            ' SELECT  $3, $2, session_user, $1.*'
        USING OLD, curr_time, 'DELETE';
    ELSIF (TG_OP = 'UPDATE') THEN
        EXECUTE 
            'INSERT INTO ' || 
            quote_ident(schema_name) || '.' || quote_ident(table_name) ||
            ' SELECT  $3, $2, session_user, $1.*'
        USING NEW, curr_time, 'UPDATE';
    ELSIF (TG_OP = 'INSERT') THEN
        EXECUTE 
            'INSERT INTO ' || 
            quote_ident(schema_name) || '.' || quote_ident(table_name) ||
            ' SELECT  $3, $2, session_user, $1.*'
        USING NEW, curr_time, 'INSERT';
    END IF;
    RETURN NULL; -- возвращаемое значение для триггера AFTER игнорируется
END;
$function$
;

ALTER FUNCTION public.write_audit() OWNER TO engineer;
'''


function_to_protect_ddl = '''
CREATE OR REPLACE FUNCTION public.tp_lock_delete_func()
 RETURNS TRIGGER
 LANGUAGE plpgsql
AS $function$
DECLARE 
    table_name TEXT := CONCAT(quote_ident(TG_TABLE_SCHEMA), '.', quote_ident(TG_TABLE_NAME) );
    all_count_rows TEXT;
    to_delete_rows int;
BEGIN
    EXECUTE 
        'SELECT COUNT(*) FROM ' || table_name
    INTO all_count_rows
    USING NULL;
   SELECT count(*)
   INTO to_delete_rows
   FROM old_table;

    IF all_count_rows::bigint = 0 THEN 
        RAISE EXCEPTION 'Delete all rows from : % not allowed, tried to delete: % rows',
                                                      table_name,  to_delete_rows;
    END IF;
    RETURN NULL;
END;
$function$
;
'''

create_protection_trigger_ddl = '''
CREATE TRIGGER "_lock_delete_{table}" 
AFTER DELETE ON "{schema}"."{table}"
REFERENCING OLD TABLE AS old_table
FOR EACH STATEMENT EXECUTE PROCEDURE public.tp_lock_delete_func();
'''
#
delete_trigger_to_protect = '''
DROP TRIGGER "_lock_delete_{table}" ON "{schema}"."{table}";
'''

create_history_table = '''
CREATE TABLE IF NOT EXISTS history_tables."{table}_history" AS 
SELECT
    'INSERT'::TEXT AS history_op,
    NOW()::timestamp AS history_timestamp,
    SESSION_USER::text AS author_op,
    tbl.*
FROM 
    {schema}.{table} tbl
;
'''

create_comment_history_table = '''
COMMENT ON TABLE history_tables."{table}_history" IS 'Таблица-история, наполняемая при срабатывании триггера. 
Настроен на операции DELETE, INSERT или UPDATE.
Таблица, из которой пишется история:
{schema}.{table}'
'''

create_history_trigger = '''
CREATE TRIGGER "_write_history_{table}"
AFTER INSERT OR UPDATE OR DELETE ON "{schema}"."{table}"
FOR EACH ROW EXECUTE FUNCTION public.write_audit();
'''

delete_history_trigger = '''
DROP TRIGGER "_write_history_{table}" ON "{schema}"."{table}";
'''

check_exiting_trigger = '''
SELECT EXISTS(
    SELECT 
        trg.tgname ,
        c.relname ,
        n.nspname 
    FROM 
    pg_trigger trg
    LEFT JOIN 
        pg_class c ON c."oid" = trg.tgrelid 
    LEFT JOIN 
        pg_catalog.pg_namespace n ON n."oid" = c.relnamespace 
    WHERE 
        trg.tgname ILIKE '%_{trigger}%'
        AND c.relname  = '{table}'
        AND n.nspname = '{schema}'
    )
;
'''

check_exiting_history = '''
   SELECT EXISTS(
    SELECT 
        c.relname ,
        n.nspname 
    FROM 
    pg_class c
    LEFT JOIN 
        pg_catalog.pg_namespace n ON n."oid" = c.relnamespace 
    WHERE 
        c.relname  = '{table}_history'
        AND n.nspname = 'history_tables'
    )
;
'''

check_depending_objects = '''
WITH RECURSIVE view_deps AS (
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
WHERE NOT (dependent_ns.nspname = source_ns.nspname AND dependent_view.relname = source_table.relname)
UNION
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
INNER JOIN view_deps vd
    ON vd.dependent_schema = source_ns.nspname
    AND vd.dependent_view = source_table.relname
    AND NOT (dependent_ns.nspname = vd.dependent_schema AND dependent_view.relname = vd.dependent_view)
)
SELECT 
	dependent_schema AS schema_name,
	dependent_view AS view_name
FROM view_deps
WHERE source_table = '{table_name}'
ORDER BY source_schema, source_table;
'''