truncate table de1m.mndg_stg_cards;

truncate table de1m.mndg_stg_del_cards;

insert into de1m.mndg_stg_cards ( card_num, account, create_dt, update_dt )
select 
  card_num,
  account,
  create_dt,
  update_dt from bank.cards
where coalesce ( update_dt, create_dt ) > coalesce( ( 
    select max_update_dt
    from de1m.mndg_meta_project
    where schema_name = 'BANK' and table_name = 'CARDS'
), to_date( '1800.01.01', 'YYYY.MM.DD' ) );

insert into de1m.mndg_stg_del_cards ( card_num )
select card_num from de1m.mndg_stg_cards;

merge into de1m.mndg_dwh_dim_cards_hist tgt
using de1m.mndg_stg_cards stg
on( stg.card_num = tgt.card_num and deleted_flg = 'N' )
when matched then 
  update set tgt.effective_to = coalesce ( stg.update_dt, stg.create_dt ) - interval '1' second
  where 1=1
  and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
  and (1=0
    or stg.account <> tgt.account 
    or ( stg.account is null and tgt.account is not null )
  )
when not matched then 
  insert ( card_num, account, effective_from, effective_to, deleted_flg ) 
  values (
    stg.card_num,
    stg.account,
    stg.create_dt,
    to_date( '31.12.9999', 'DD.MM.YYYY' ),
    'N'
    );

insert into de1m.mndg_dwh_dim_cards_hist ( card_num, account, effective_from, effective_to, deleted_flg ) 
select
  stg.card_num, 
  stg.account, 
  coalesce (stg.update_dt, stg.create_dt), 
  to_date( '31.12.9999', 'DD.MM.YYYY' ), 
  'N'
from de1m.mndg_dwh_dim_cards_hist tgt
inner join de1m.mndg_stg_cards stg
on ( tgt.card_num = stg.card_num and deleted_flg = 'N' )
where 1=0
  or stg.account <> tgt.account 
  or ( stg.account is null and tgt.account is not null );

insert into de1m.mndg_dwh_dim_cards_hist ( card_num, account, effective_from, effective_to, deleted_flg ) 
select
  tgt.card_num, 
  tgt.account,
  current_date, 
  to_date( '31.12.9999', 'DD.MM.YYYY' ), 
  'Y'
from de1m.mndg_dwh_dim_cards_hist tgt
left join de1m.mndg_stg_del_cards stg
on ( stg.card_num = tgt.card_num )
where stg.card_num is null;

update de1m.mndg_dwh_dim_cards_hist tgt
set tgt.effective_to = current_date - interval '1' second
where tgt.card_num not in (select card_num from de1m.mndg_stg_cards)
and tgt.effective_to = to_date( '31.12.9999', 'DD.MM.YYYY' )
and tgt.deleted_flg = 'N';

merge into de1m.mndg_meta_project trg
using ( select 'BANK' schema_name, 'CARDS' table_name, ( select max( update_dt ) from de1m.mndg_stg_cards ) max_update_dt from dual ) src
on ( trg.schema_name = src.schema_name and trg.table_name = src.table_name )
when matched then 
    update set trg.max_update_dt = src.max_update_dt
    where src.max_update_dt is not null
when not matched then 
    insert ( schema_name, table_name, max_update_dt )
    values ( 'BANK', 'CARDS', coalesce( src.max_update_dt, to_date( '1800.01.01', 'YYYY.MM.DD' ) ) );

commit;