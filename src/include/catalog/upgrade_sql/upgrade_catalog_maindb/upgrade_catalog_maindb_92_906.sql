-- this table will be add column
-- openGauss=# select table_schema, table_name,column_name from information_schema.columns where column_name = 'db_time';
--  table_schema |          table_name           | column_name
-- --------------+-------------------------------+-------------
--  pg_catalog   | gs_wlm_session_query_info_all | db_time --table
--  pg_catalog   | statement_history             | db_time --unlogged table, update in post-upgrade

ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt1_q bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt2_simple_query bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt3_analyze_rewrite bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt4_plan_query bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt5_light_query bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt6_p bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt7_b bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt8_e bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt9_d bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt10_s bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt11_c bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt12_u bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt13_before_query bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column srt14_after_query bigint;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all add column rtt_unknown bigint;
