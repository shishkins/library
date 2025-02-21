list_schemas = """
SELECT 
	n.nspname 
FROM 
	pg_catalog.pg_namespace n
WHERE 
	n.nspname 
"""

list_schemas_all = """
SELECT 
	n.nspname 
FROM 
	pg_catalog.pg_namespace n
WHERE 
	n.nspname NOT IN (
	'dbo', 'remote', 'fcs_bad', 'pg_catalog', 'information_schema', 'pg_toast', 'cdc._utils'
	)
"""

ddl_view_get_status = '''
SELECT 
    max(last_backup_date)
FROM 
    tech_info.views_ddl_backup vdb 
'''

get_list_of_objects = '''
SELECT
	c.relname,
	n.nspname,
	c.relkind
FROM
    pg_class c
LEFT JOIN 
    pg_namespace n
    on n."oid" = c.relnamespace 
WHERE 
	c.relkind IN ('v', 'm')
	AND n.nspname IN {schemas}
'''
