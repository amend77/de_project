insert into de1m.MNDG_rep_fraud
select
    trn.transaction_date as event_dt,
    clt.passport_num as passport,
    clt.last_name || ' ' || clt.first_name || ' ' || clt.patronymic as fio,
    clt.phone,
    case
    when clt.passport_num in (select passport_num from de1m.mndg_dwh_fact_pssprt_blcklst)
        or clt.passport_valid_to < trn.transaction_date
    then '1'
    when act.valid_to < trn.transaction_date
    then '2'
    end as event_type, 
    to_date ( '@file', 'YYYY-MM-DD')  as report_dt
from de1m.mndg_dwh_fact_transactions trn
left join de1m.mndg_dwh_dim_cards_hist crd
on trn.card_num = trim(crd.card_num)
left join de1m.mndg_dwh_dim_accounts_hist act
on crd.account = act.account
left join de1m.mndg_dwh_dim_clients_hist clt
on act.client = clt.client_id
left join de1m.mndg_dwh_dim_terminals_hist tml
on trn.terminal = tml.terminal_id
where clt.passport_num in (select passport_num from de1m.mndg_dwh_fact_pssprt_blcklst)
    or clt.passport_valid_to < trn.transaction_date
    or act.valid_to < trn.transaction_date;

insert into de1m.mndg_rep_fraud
select
    x.transaction_date as event_dt,
    x.passport_num as passport,
    x.last_name || ' ' || x.first_name || ' ' || x.patronymic as fio,
    x.phone,
    '3' as event_type,
    to_date ( '@file', 'YYYY-MM-DD')  as report_dt
from
    (select
        trn.transaction_id,
        trn.transaction_date as transaction_date,
        lead(trn.transaction_date) over (partition by crd.card_num order by trn.transaction_date) next_date,
        trn.amount ,
        trn.oper_type ,
        trn.oper_result ,
        ter.terminal_city current_city,
        lag(ter.terminal_city) over (partition by crd.card_num order by trn.transaction_date) next_city,
        ter.terminal_type ,
        act.account ,
        clt.last_name ,
        clt.first_name,
        clt.patronymic ,
        clt.passport_num,
        clt.phone        
    from de1m.mndg_dwh_fact_transactions trn
    left join de1m.mndg_dwh_dim_cards_hist crd
    on trn.card_num = trim(crd.card_num)
    left join de1m.mndg_dwh_dim_accounts_hist act
    on crd.account = act.account
    left join de1m.mndg_dwh_dim_clients_hist clt
    on act.client = clt.client_id
    left join de1m.mndg_dwh_dim_terminals_hist ter
    on trn.terminal = ter.terminal_id
    where clt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
    ) x
where current_city <> next_city
      and oper_type <> 'PAYMENT'
      and next_date - transaction_date < 1;

commit;