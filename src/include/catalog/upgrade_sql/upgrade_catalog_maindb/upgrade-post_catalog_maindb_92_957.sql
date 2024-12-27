-- create gs_stat_walsender
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walsender(int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2864;
CREATE FUNCTION pg_catalog.gs_stat_walsender(
    in  operation                   int4 default 2,
    out is_enable_stat              boolean,
    out channel                     text,
    out cur_time                    timestamptz,
    out send_times                  xid,
    out first_send_time             timestamptz,
    out last_send_time              timestamptz,
    out last_reset_time             timestamptz,
    out avg_send_interval           xid,
    out since_last_send_interval    xid
)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 as 'gs_stat_walsender' stable;

-- create gs_stat_walreceiver
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walreceiver(int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2865;
CREATE FUNCTION pg_catalog.gs_stat_walreceiver(
    in  operation                 int4 default 2,
    out is_enable_stat            boolean,
    out buffer_current_size       xid,
    out buffer_full_times         xid,
    out wake_writer_times         xid,
    out avg_wake_interval         xid,
    out since_last_wake_interval  xid,
    out first_wake_time           timestamptz,
    out last_wake_time            timestamptz,
    out last_reset_time           timestamptz,
    out cur_time                  timestamptz
)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 as 'gs_stat_walreceiver' stable;

-- create gs_stat_walrecvwriter
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walrecvwriter(int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2868;
CREATE FUNCTION pg_catalog.gs_stat_walrecvwriter(
    in  operation           int4 default 2,
    out is_enable_stat      boolean,
    out total_write_bytes   xid,
    out write_times         xid,
    out total_write_time    xid,
    out avg_write_time      xid,
    out avg_write_bytes     xid,
    out total_sync_bytes    xid,
    out sync_times          xid,
    out total_sync_time     xid,
    out avg_sync_time       xid,
    out avg_sync_bytes      xid,
    out current_xlog_segno  xid,
    out inited_xlog_segno   xid,
    out last_reset_time     timestamptz,
    out cur_time            timestamptz
)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 as 'gs_stat_walrecvwriter' stable;

DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select * from pg_conversion where conname = 'gb18030_2022_to_utf8' limit 1) into ans;
    if ans = true THEN
        comment on conversion gb18030_2022_to_utf8 is 'conversion for GB18030_2022 to UTF8';
    end if;
END$$;

DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select * from pg_conversion where conname = 'utf8_to_gb18030_2022' limit 1) into ans;
    if ans = true THEN
        comment on conversion utf8_to_gb18030_2022 is 'conversion for UTF8 to GB18030_2022';
    end if;
END$$;


DROP FUNCTION IF EXISTS pg_catalog.gs_hot_standby_space_info() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6218;
CREATE OR REPLACE FUNCTION pg_catalog.gs_hot_standby_space_info
(  OUT base_page_file_num xid,
   OUT base_page_total_size xid,
   OUT lsn_info_meta_file_num xid,
   OUT lsn_info_meta_total_size xid,
   OUT block_info_meta_file_num xid,
   OUT block_info_meta_total_size xid
   )
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STABLE as 'gs_hot_standby_space_info';

DROP FUNCTION IF EXISTS pg_catalog.btvarstrequalimage(oid) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4608;
CREATE OR REPLACE FUNCTION pg_catalog.btvarstrequalimage("any") RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT AS 'btvarstrequalimage';

DROP FUNCTION IF EXISTS pg_catalog.btequalimage(oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4609;
CREATE OR REPLACE FUNCTION pg_catalog.btequalimage("any") RETURNS boolean LANGUAGE INTERNAL IMMUTABLE STRICT AS 'btequalimage';

DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select nspname from pg_namespace where nspname='coverage' limit 1) into ans;
    if ans = true THEN
        DROP table IF EXISTS coverage.proc_coverage;
        DROP SEQUENCE IF EXISTS coverage.proc_coverage_coverage_id_seq;
        DROP SCHEMA IF EXISTS coverage cascade;
    end if;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4994;
CREATE SCHEMA coverage;
COMMENT ON schema coverage IS 'coverage schema';

CREATE SEQUENCE coverage.proc_coverage_coverage_id_seq START 1;
CREATE unlogged table coverage.proc_coverage(
	coverage_id bigint NOT NULL DEFAULT nextval('coverage.proc_coverage_coverage_id_seq'::regclass),
	pro_oid oid NOT NULL,
	pro_name text NOT NULL,
	db_name text NOT NULL,
	pro_querys text NOT NULL,
	pro_canbreak bool[] NOT NULL,
	coverage int[] NOT NULL
) WITH (orientation=row, compression=no);
REVOKE ALL on table coverage.proc_coverage FROM public;

DROP FUNCTION IF EXISTS pg_catalog.generate_procoverage_report(int8, int8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5734;
CREATE OR REPLACE FUNCTION pg_catalog.generate_procoverage_report(int8, int8) RETURNS text
LANGUAGE INTERNAL STRICT as 'generate_procoverage_report';

-- add this. dropped by 92_906
DO $gs_slow_query_info$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select relname from pg_class where relname='gs_slow_query_info' and relnamespace=4988 limit 1) into ans;
    if ans = true THEN
        CREATE OR REPLACE FUNCTION dbe_perf.global_slow_query_info_bytime(text, TIMESTAMP, TIMESTAMP, int)
        RETURNS setof dbe_perf.gs_slow_query_info
        AS $$
DECLARE
                row_data dbe_perf.gs_slow_query_info%rowtype;
                row_name record;
                query_str text;
                query_str_nodes text;
                query_str_cn text;
                BEGIN
                                IF $1 IN ('start_time', 'finish_time') THEN
                                ELSE
                                         raise WARNING 'Illegal character entered for function, colname must be start_time or finish_time';
                                        return;
                                END IF;
                                --Get all the node names
                                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
                                query_str_cn := 'SELECT * FROM dbe_perf.gs_slow_query_info where '||$1||'>'''''||$2||''''' and '||$1||'<'''''||$3||''''' limit '||$4;
                                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                                                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_cn||''';';
                                                FOR row_data IN EXECUTE(query_str) LOOP
                                                                return next row_data;
                                                END LOOP;
                                END LOOP;
                                return;
                END; $$
        LANGUAGE 'plpgsql' NOT FENCED;
    end if;
END$gs_slow_query_info$;
