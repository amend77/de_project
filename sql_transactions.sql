merge into de1m.mndg_dwh_fact_transactions tgt
using de1m.mndg_stg_transactions stg
on( stg.transaction_id = tgt.transaction_id)
when not matched then 
    insert ( transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal ) 
    values ( stg.transaction_id, stg.transaction_date, stg.amount, stg.card_num, stg.oper_type, stg.oper_result, stg.terminal );

truncate table de1m.mndg_stg_transactions;