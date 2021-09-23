DROP FUNCTION IF EXISTS pg_catalog.pg_user_iostat() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5013;
CREATE OR REPLACE FUNCTION pg_catalog.pg_user_iostat(OUT userid oid, OUT min_curr_iops integer, OUT max_curr_iops integer, OUT min_peak_iops integer, OUT max_peak_iops integer, OUT io_limits integer, OUT io_priority text, OUT curr_io_limits integer) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_wlm_user_iostat_info';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_wlm_session_iostat_info(integer) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5014;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_wlm_session_iostat_info(IN unuseattr integer, OUT threadid bigint, OUT maxcurr_iops integer, OUT mincurr_iops integer, OUT maxpeak_iops integer, OUT minpeak_iops integer, OUT iops_limits integer, OUT io_priority integer, OUT curr_io_limits integer) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_wlm_session_iostat_info';

DROP VIEW IF EXISTS pg_catalog.pg_session_iostat cascade;
CREATE OR REPLACE VIEW pg_catalog.pg_session_iostat AS
    SELECT
        S.query_id,
        T.mincurr_iops as mincurriops,
        T.maxcurr_iops as maxcurriops,
        T.minpeak_iops as minpeakiops,
        T.maxpeak_iops as maxpeakiops,
        T.iops_limits as io_limits,
        CASE WHEN T.io_priority = 0 THEN 'None'::text
             WHEN T.io_priority = 10 THEN 'Low'::text
             WHEN T.io_priority = 20 THEN 'Medium'::text
             WHEN T.io_priority = 50 THEN 'High'::text END AS io_priority,
        S.query,
        S.node_group,
        T.curr_io_limits as curr_io_limits
FROM pg_stat_activity_ng AS S,  pg_stat_get_wlm_session_iostat_info(0) AS T
WHERE S.pid = T.threadid;

DROP VIEW IF EXISTS dbe_perf.statement_iostat_complex_runtime cascade;
CREATE OR REPLACE VIEW dbe_perf.statement_iostat_complex_runtime AS
   SELECT
     S.query_id,
     T.mincurr_iops as mincurriops,
     T.maxcurr_iops as maxcurriops,
     T.minpeak_iops as minpeakiops,
     T.maxpeak_iops as maxpeakiops,
     T.iops_limits as io_limits,
     CASE WHEN T.io_priority = 0 THEN 'None'::text
          WHEN T.io_priority = 20 THEN 'Low'::text
          WHEN T.io_priority = 50 THEN 'Medium'::text
          WHEN T.io_priority = 80 THEN 'High'::text END AS io_priority,
     S.query,
     S.node_group,
     T.curr_io_limits as curr_io_limits
   FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_session_iostat_info(0) AS T
     WHERE S.pid = T.threadid;