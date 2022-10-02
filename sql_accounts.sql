truncate table de1m.mndg_stg_accounts;

truncate table de1m.mndg_stg_del_accounts;

insert into de1m.mndg_stg_accounts ( account, valid_to, client, create_dt, update_dt )
select 
  account,
  valid_to,
  client,
  create_dt,
  update_dt from bank.accounts
where coalesce (update_dt, create_dt ) > coalesce( ( 
    select max_update_dt
    from de1m.mndg_meta_project
    where schema_name = 'BANK' and table_name = 'ACCOUNTS'
), to_date( '1800.01.01', 'YYYY.MM.DD' ) );

insert into de1m.mndg_stg_del_accounts ( account )
select account from de1m.mndg_stg_accounts;

merge into de1m.mndg_dwh_dim_accounts_hist tgt
using de1m.mndg_stg_accounts stg
on ( stg.account = tgt.account and deleted_flg = 'N' )
when matched then 
  update set tgt.effective_to = coalesce (stg.update_dt, stg.create_dt) - interval '1' second
  where 1=1
  and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
  and (1=0
    or stg.valid_to <> tgt.valid_to
    or stg.client <> tgt.client
    or ( stg.valid_to is null and tgt.valid_to is not null )
    or ( stg.client is not null and tgt.client is null )
  )
when not matched then 
  insert ( account, valid_to, client, effective_from, effective_to, deleted_flg ) 
  values (
    stg.account,
    stg.valid_to,
    stg.client,
    stg.create_dt,
    to_date( '31.12.9999', 'DD.MM.YYYY' ),
    'N'
    );
    
insert into de1m.mndg_dwh_dim_accounts_hist ( account, valid_to, client, effective_from, effective_to, deleted_flg ) 
select
  stg.account,
  stg.valid_to,
  stg.client,
  coalesce (stg.update_dt, stg.create_dt),
  to_date( '31.12.9999', 'DD.MM.YYYY' ),
  'N'
from de1m.mndg_dwh_dim_accounts_hist tgt
inner join de1m.mndg_stg_accounts stg
on ( stg.account = tgt.account and deleted_flg = 'N' )
where 1=0
  or stg.valid_to <> tgt.valid_to
  or stg.client <> tgt.client
  or ( stg.valid_to is null and tgt.valid_to is not null )
  or ( stg.client is not null and tgt.client is null );

insert into de1m.mndg_dwh_dim_accounts_hist ( account, valid_to, client, effective_from, effective_to, deleted_flg ) 
select
  tgt.account,
  tgt.valid_to,
  tgt.client,
  current_date, 
  to_date( '31.12.9999', 'DD.MM.YYYY' ), 
  'Y'
from de1m.mndg_dwh_dim_accounts_hist tgt
left join de1m.mndg_stg_del_accounts stg
on ( stg.account = tgt.account )
where stg.account is null;

update de1m.mndg_dwh_dim_accounts_hist tgt
set tgt.effective_to = current_date - interval '1' second
where tgt.account not in (select account from de1m.mndg_stg_accounts)
and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';

merge into de1m.mndg_meta_project trg
using ( select 'BANK' schema_name, 'ACCOUNTS' table_name, ( select max( update_dt ) from de1m.mndg_stg_accounts ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'BANK', 'ACCOUNTS', coalesce( src.max_update_dt, to_date( '1800.01.01', 'YYYY.MM.DD' ) ) );

commit;