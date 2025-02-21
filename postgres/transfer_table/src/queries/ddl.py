get_ddl_query = '''
select * from public.pg_get_tabledef(
'{schema}',
'{table}', 
false, 
'PKEY_INTERNAL', 
'COMMENTS', 
'FKEYS_INTERNAL', 
'INCLUDE_TRIGGERS'
);
'''

add_owner = '''
ALTER TABLE 
"{schema}"."{table}"
OWNER TO "{owner}";
'''

delete_data_query = '''
DROP TABLE IF EXISTS {schema}.{table};
'''