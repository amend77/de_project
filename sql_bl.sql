merge into de1m.mndg_dwh_fact_pssprt_blcklst tgt
using de1m.mndg_stg_pssprt_blcklst stg
on( stg.passport_num = tgt.passport_num )
when not matched then 
    insert ( passport_num, entry_dt ) 
    values ( stg.passport_num, stg.entry_dt );

truncate table de1m.mndg_stg_pssprt_blcklst;