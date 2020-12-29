\c postgres

select create_wlm_operator_info(1);

select count(*) from gs_wlm_plan_operator_info;

select gather_encoding_info('regression');

select count(*) from gs_wlm_plan_encoding_table;