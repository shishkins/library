

get_relation_query = '''SELECT nspname                         AS schema_name,
       relname                         AS rel_name,
       CASE relkind
           WHEN 'm' THEN 'materialized_view'
           WHEN 'v' THEN 'view'
           END                         AS object_type,
       relkind                         AS relkind,
       pg_get_viewdef(pg_class."oid")  AS view_text,
       obj_description(pg_class."oid") AS comment
FROM pg_class,
     pg_namespace
WHERE relnamespace = pg_namespace.oid
  AND relkind IN ('v', 'm') -- нужные типы объектов
  AND pg_namespace.nspname IN {schemas}
ORDER BY schema_name,
         object_type
'''

get_relation_comments = '''SELECT nspname                          AS schema_name,
       relname                          AS rel_name,
       CASE relkind
           WHEN 'm' THEN 'materialized_view'
           WHEN 'v' THEN 'view'
           END                          AS object_type,
       a.attname,
       relkind                          AS relkind,
       col_description(c.oid, a.attnum) AS comment
FROM pg_class AS c
INNER JOIN
pg_namespace AS n
    ON n."oid" = c.relnamespace
INNER JOIN
pg_attribute a
    ON c.oid = a.attrelid
WHERE relkind IN ('v', 'm') -- нужные типы объектов
  AND n.nspname IN {schemas}
'''

get_relation_indexes = '''SELECT pg_get_indexdef(c."oid") AS index_text,
       ind.indrelid,
       t.relname                AS rel_name,
       n.nspname                AS schema_name
FROM pg_class AS c
JOIN
pg_index AS ind
    ON ind.indexrelid = c."oid"
JOIN
pg_class AS t
    ON t."oid" = ind.indrelid
JOIN
pg_namespace AS n
    ON n."oid" = t.relnamespace
WHERE c.relkind = 'i'
  AND n.nspname IN {schemas}
'''

insert_data = '''
INSERT INTO tech_info.views_ddl_backup (schema_name, view_name, ddl_text, last_backup_date)
VALUES (%(schema_name)s, %(view_name)s, %(ddl_text)s, NOW());
'''

check_data = '''SELECT DISTINCT ON (schema_name, view_name) ddl_text AS ddl,
last_backup_date::date AS last_backup_date
FROM tech_info.views_ddl_backup t
WHERE t.schema_name = %(schema_name)s
    AND t.view_name = %(view_name)s
ORDER BY
    schema_name, 
    view_name, 
    t.last_backup_date DESC;
'''