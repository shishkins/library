WITH RECURSIVE views AS (
                SELECT v.oid::regclass AS view,
                        v.relkind = 'm' AS is_materialized,
                        1 AS level
                FROM pg_depend AS d
                    JOIN pg_rewrite AS r
                        ON r.oid = d.objid
                    JOIN pg_class AS v
                        ON v.oid = r.ev_class
                WHERE v.relkind IN ('v', 'm')
                    AND d.classid = 'pg_rewrite'::regclass
                    AND d.refclassid = 'pg_class'::regclass
                    AND d.deptype = 'n'
                    AND d.refobjid = '"{schema}"."{obj}"'::regclass
                UNION
                -- add the views that depend on these
                SELECT v.oid::regclass,
                        v.relkind = 'm',
                        views.level + 1
                FROM views
                    JOIN pg_depend AS d
                        ON d.refobjid = views.view
                    JOIN pg_rewrite AS r
                        ON r.oid = d.objid
                    JOIN pg_class AS v
                        ON v.oid = r.ev_class
                WHERE v.relkind IN ('v', 'm')
                    AND d.classid = 'pg_rewrite'::regclass
                    AND d.refclassid = 'pg_class'::regclass
                    AND d.deptype = 'n'
                    AND v.oid <> views.view  -- avoid loop
                ),
            all_views as (
                SELECT
                    schemaname "table_schema",
                    matviewname "table_name",
                    matviewowner "owner"
                FROM
                    pg_matviews
                UNION
                SELECT
                    schemaname "table_schema",
                    viewname "table_name",
                    viewowner "owner"
                FROM
                    pg_views
            ),
            definition as (
                SELECT
                    format(
                        'CREATE %s VIEW %s AS %s',
                        CASE
                            WHEN is_materialized
                                THEN ' MATERIALIZED'
                            ELSE ''
                        END,
                        ('"' || av.table_schema || '"."' || av.table_name || '"'),
                        pg_get_viewdef(('"' || av.table_schema || '"."' || av.table_name || '"'))
                    ) AS "ddl",
                    v.level as lvl,
                    av.table_schema as "schema",
                    av.table_name as "name",
                    av.owner as owner,
                    v.is_materialized as "materialized"
                FROM
                    views as v
                INNER JOIN
                    all_views as av
                ON
                    ('"' || av.table_schema || '"."' || av.table_name || '"')::regclass::oid = v.view::regclass::oid
                GROUP BY
                    v.is_materialized,
                    v.level,
                    av.table_schema,
                    av.table_name,
                    av."owner"
                ORDER BY
                    max(v.level)
            )
            SELECT
                ddl,
                schema,
                name,
                lvl,
                owner,
                materialized
            FROM
                definition;