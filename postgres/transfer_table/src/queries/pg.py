transfer_status = '''
SELECT EXISTS (
SELECT *
FROM 
	"{schema}"."{table}" 
)
'''

get_data_query = '''
SELECT 
    *
FROM 
    {schema}.{table}
'''
get_table_owner = '''
    SELECT a.rolname 
    FROM 
    pg_class c 
    LEFT JOIN 
    pg_authid a ON a."oid" = c.relowner 
   	LEFT JOIN 
   		pg_catalog.pg_namespace n ON n."oid" = c.relnamespace
    WHERE c.relname = '{table}' AND n.nspname = '{schema}';
'''

limit_depth = """
WHERE "{depth_column}" > current_date - '{depth_months} months' :: INTERVAL
"""
