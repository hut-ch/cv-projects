-- get list of table licks in redshift

select 
  t.*
  ,l.lock_owner_start_ts
  ,l.lock_owner_end_ts	
  ,l.lock_status
  ,l.last_update 
  ,l.lock_owner
  ,l.lock_owner_pid
from 
  stv_locks l
  left join (
    select 
      distinct(id) table_id
      ,trim(datname)   db_name
      ,trim(nspname)   schema_name
      ,trim(relname)   table_name
    from 
      stv_tbl_perm
      join pg_class on pg_class.oid = stv_tbl_perm.id
      join pg_namespace on pg_namespace.oid = relnamespace
      join pg_database on pg_database.oid = stv_tbl_perm.db_id
  ) t on l.table_id = t.table_id
order by l.lock_owner_start_ts
;
 
