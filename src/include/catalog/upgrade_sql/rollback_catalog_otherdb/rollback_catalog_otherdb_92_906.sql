-- this table will be remove column
-- openGauss=# select table_schema, table_name,column_name from information_schema.columns where column_name = 'db_time';
--  table_schema |          table_name           | column_name
-- --------------+-------------------------------+-------------
--  pg_catalog   | gs_wlm_session_query_info_all | db_time --table
--  pg_catalog   | statement_history             | db_time --unlogged table, update in post-upgrade

ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt1_q cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt2_simple_query cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt3_analyze_rewrite cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt4_plan_query cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt5_light_query cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt6_p cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt7_b cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt8_e cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt9_d cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt10_s cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt11_c cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt12_u cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt13_before_query cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists srt14_after_query cascade ;
ALTER TABLE pg_catalog.gs_wlm_session_query_info_all  drop column if exists rtt_unknown cascade ;
