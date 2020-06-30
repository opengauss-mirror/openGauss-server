/*
################################################################################
# TESTCASE NAME : encode_tpch
# COMPONENT(S)  : plan encoding
# MODIFIED BY   : WHO            WHEN          COMMENT
#               : -------------- ------------- ---------------------------------
#               :                1-9-2019      created
################################################################################
*/
\c postgres

select create_wlm_operator_info(1);

select count(*) from gs_wlm_plan_operator_info;

select gather_encoding_info('regression');

select count(*) from gs_wlm_plan_encoding_table;
