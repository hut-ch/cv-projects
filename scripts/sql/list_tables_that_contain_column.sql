------------------------------------------------------------------
-- This query will return all tables that contain the specified 
-- column. Simply replace <COLUIMN_NAME> with the required name
-- you can also refine the table list bt uncommenting the filter 
-- and adding in part of the table name
------------------------------------------------------------------

select
	t.table_schema
	,t.table_name
from 
	information_schema.tables t
	inner join information_schema.columns c 
		on c.table_name = t.table_name 
        and c.table_schema = t.table_schema
where 
	c.column_name = '<COLUIMN_NAME>'
    --and (t.table_name like '%fact%')
    and t.table_schema not in ('information_schema', 'pg_catalog')
    and t.table_type = 'BASE TABLE'
order by 
	t.table_schema;
