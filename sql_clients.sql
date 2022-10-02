truncate table de1m.mndg_stg_clients;

truncate table de1m.mndg_stg_del_clients;

insert into de1m.mndg_stg_clients ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt )
select
  client_id,
  last_name,
  first_name,
  patronymic,
  date_of_birth,
  passport_num,
  passport_valid_to,
  phone,
  create_dt,
  update_dt from bank.clients
where coalesce (update_dt, create_dt ) > coalesce( ( 
    select max_update_dt
    from de1m.mndg_meta_project
    where schema_name = 'BANK' and table_name = 'CLIENTS'
), to_date( '1800.01.01', 'YYYY.MM.DD' ) );

insert into de1m.mndg_stg_del_clients ( client_id )
select client_id from de1m.mndg_stg_clients;

merge into de1m.mndg_dwh_dim_clients_hist tgt
using de1m.mndg_stg_clients stg
on( stg.client_id = tgt.client_id and deleted_flg = 'N' )
when matched then 
  update set tgt.effective_to = coalesce (stg.update_dt, stg.create_dt) - interval '1' second
  where 1=1
  and tgt.effective_to = to_date ( '31.12.9999', 'DD.MM.YYYY' )
  and (1=0
    or stg.last_name <> tgt.last_name 
    or stg.first_name <> tgt.first_name
    or stg.patronymic <> tgt.patronymic
    or stg.date_of_birth <> tgt.date_of_birth
    or stg.passport_num <> tgt.passport_num
    or stg.passport_valid_to <> tgt.passport_valid_to
    or stg.phone <> tgt.phone
    or ( stg.last_name is null and tgt.last_name is not null )
    or ( stg.first_name is not null and tgt.first_name is null )
    or ( stg.patronymic is not null and tgt.patronymic is null )
    or ( stg.date_of_birth is not null and tgt.date_of_birth is null )
    or ( stg.passport_num is not null and tgt.passport_num is null )
    or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
    or ( stg.phone is not null and tgt.phone is null )
  )
when not matched then 
  insert ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg ) 
  values (
    stg.client_id,
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    stg.create_dt,
    to_date( '31.12.9999', 'DD.MM.YYYY' ),
    'N' );

insert into de1m.mndg_dwh_dim_clients_hist ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg ) 
select
  stg.client_id,
  stg.last_name,
  stg.first_name,
  stg.patronymic,
  stg.date_of_birth,
  stg.passport_num,
  stg.passport_valid_to,
  stg.phone,
  coalesce (stg.update_dt, stg.create_dt), 
  to_date( '31.12.9999', 'DD.MM.YYYY' ), 
  'N'
from de1m.mndg_dwh_dim_clients_hist tgt
inner join de1m.mndg_stg_clients stg
on ( stg.client_id = tgt.client_id and deleted_flg = 'N' )
where 1=0
  or stg.last_name <> tgt.last_name 
  or stg.first_name <> tgt.first_name
  or stg.patronymic <> tgt.patronymic
  or stg.date_of_birth <> tgt.date_of_birth
  or stg.passport_num <> tgt.passport_num
  or stg.passport_valid_to <> tgt.passport_valid_to
  or stg.phone <> tgt.phone
  or ( stg.last_name is null and tgt.last_name is not null )
  or ( stg.first_name is not null and tgt.first_name is null )
  or ( stg.patronymic is not null and tgt.patronymic is null )
  or ( stg.date_of_birth is not null and tgt.date_of_birth is null )
  or ( stg.passport_num is not null and tgt.passport_num is null )
  or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
  or ( stg.phone is not null and tgt.phone is null );

insert into de1m.mndg_dwh_dim_clients_hist ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg ) 
select
  tgt.client_id,
  tgt.last_name,
  tgt.first_name,
  tgt.patronymic,
  tgt.date_of_birth,
  tgt.passport_num,
  tgt.passport_valid_to,
  tgt.phone,
  current_date, 
  to_date( '31.12.9999', 'DD.MM.YYYY' ), 
  'Y'
from de1m.mndg_dwh_dim_clients_hist tgt
left join de1m.mndg_stg_del_clients stg
on ( stg.client_id = tgt.client_id )
where stg.client_id is null;

update de1m.mndg_dwh_dim_clients_hist tgt
set tgt.effective_to = current_date - interval '1' second
where tgt.client_id not in (select client_id from de1m.mndg_stg_clients)
and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';

merge into de1m.mndg_meta_project trg
using ( select 'BANK' schema_name, 'CLIENTS' table_name, ( select max( update_dt ) from de1m.mndg_stg_clients ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'BANK', 'CLIENTS', coalesce( src.max_update_dt, to_date( '1800.01.01', 'YYYY.MM.DD' ) ) );

commit;