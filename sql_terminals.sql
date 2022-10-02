insert into de1m.mndg_stg_del_terminals ( terminal_id )
select terminal_id from de1m.mndg_stg_terminals;

merge into de1m.mndg_dwh_dim_terminals_hist tgt
using de1m.mndg_stg_terminals stg
on( stg.terminal_id = tgt.terminal_id and deleted_flg = 'N' )
when matched then 
  update set tgt.effective_to = to_date ( '@file', 'YYYY-MM-DD' ) - interval '1' second
  where 1=1
  and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
  and (1=0
  or stg.terminal_type <> tgt.terminal_type 
  or stg.terminal_city <> tgt.terminal_city
  or stg.terminal_address <> tgt.terminal_address
  or ( stg.terminal_type is null and tgt.terminal_type is not null )
  or ( stg.terminal_city is not null and tgt.terminal_city is null )
  or ( stg.terminal_address is not null and tgt.terminal_address is null )
  )
when not matched then 
  insert ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg ) 
    values (
      stg.terminal_id,
      stg.terminal_type,
      stg.terminal_city,
      stg.terminal_address,
      to_date('@file', 'YYYY-MM-DD'),
      to_date( '31.12.9999', 'DD.MM.YYYY' ),
      'N'
      );   

insert into de1m.mndg_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg ) 
select
  stg.terminal_id,
  stg.terminal_type,
  stg.terminal_city,
  stg.terminal_address,
  to_date('@file', 'YYYY-MM-DD'),
  to_date( '31.12.9999', 'DD.MM.YYYY' ),
  'N'
from de1m.mndg_dwh_dim_terminals_hist tgt
inner join de1m.mndg_stg_terminals stg
on ( stg.terminal_id = tgt.terminal_id and deleted_flg = 'N' )
where 1=0
  or stg.terminal_type <> tgt.terminal_type 
  or stg.terminal_city <> tgt.terminal_city
  or stg.terminal_address <> tgt.terminal_address
  or ( stg.terminal_type is null and tgt.terminal_type is not null )
  or ( stg.terminal_city is not null and tgt.terminal_city is null )
  or ( stg.terminal_address is not null and tgt.terminal_address is null );

insert into de1m.mndg_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg ) 
select
  tgt.terminal_id,
  tgt.terminal_type,
  tgt.terminal_city,
  tgt.terminal_address,
  current_date, 
  to_date( '31.12.9999', 'DD.MM.YYYY' ), 
  'Y'
from de1m.mndg_dwh_dim_terminals_hist tgt
left join de1m.mndg_stg_del_terminals stg
on ( stg.terminal_id = tgt.terminal_id )
where stg.terminal_id is null;

update de1m.mndg_dwh_dim_terminals_hist tgt
set tgt.effective_to = current_date - interval '1' second
where tgt.terminal_id not in (select terminal_id from de1m.mndg_stg_terminals)
and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';

merge into de1m.mndg_meta_project trg
using ( select 'XLSX' schema_name, 'TERMINALS' table_name, to_date ( '@file', 'YYYY-MM-DD' ) max_update_dt from dual ) src
  on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'XLSX', 'TERMINALS', coalesce( src.max_update_dt, to_date( '1800.01.01', 'YYYY.MM.DD' ) ) );

truncate table de1m.mndg_stg_terminals;

truncate table de1m.mndg_stg_del_terminals;

commit;