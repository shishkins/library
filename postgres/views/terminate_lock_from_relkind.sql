SELECT pg_terminate_backend(pid) AS is_terminate,
       pid,
       usename,
       state,
       query_start
FROM pg_stat_activity
WHERE pid IN (
    SELECT pid
    FROM pg_locks l
    JOIN
    pg_class t
        ON l.relation = t.oid
        AND t.relkind = 'm'
        AND t.relname = '{table}'
    )