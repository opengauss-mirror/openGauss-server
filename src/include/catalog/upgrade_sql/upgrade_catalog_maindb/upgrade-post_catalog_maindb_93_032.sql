DECLARE
con int;
BEGIN
    select count(*) from pg_proc where proname='json_object' and oid=3400 into con;
    if con = 1 then
        DROP FUNCTION IF EXISTS pg_catalog.json_object() CASCADE;
    end if;
END;
/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3400;
CREATE FUNCTION pg_catalog.json_object()
RETURNS json
LANGUAGE INTERNAL
IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
as 'json_object';

DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
-----------------------------------------------------------------------------------------------------------------------------------------------------
	DROP VIEW IF EXISTS DBE_PERF.local_active_session cascade;
	DROP FUNCTION IF EXISTS pg_catalog.get_local_active_session(OUT sampleid bigint, OUT sample_time timestamp with time zone, OUT need_flush_sample boolean, OUT databaseid oid, OUT thread_id bigint, OUT sessionid bigint, OUT start_time timestamp with time zone, OUT event text, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT userid oid, OUT application_name text, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT query_id bigint, OUT unique_query_id bigint, OUT user_id oid, OUT cn_id integer, OUT unique_query text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT wait_status text, OUT global_sessionid text) cascade;

	SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5721;
	CREATE OR REPLACE FUNCTION pg_catalog.get_local_active_session
		(OUT sampleid bigint, OUT sample_time timestamp with time zone, OUT need_flush_sample boolean, OUT databaseid oid, OUT thread_id bigint, OUT sessionid bigint, OUT start_time timestamp with time zone, OUT event text, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT userid oid, OUT application_name text, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT query_id bigint, OUT unique_query_id bigint, OUT user_id oid, OUT cn_id integer, OUT unique_query text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT wait_status text, OUT global_sessionid text, OUT xact_start_time timestamp with time zone, OUT query_start_time timestamp with time zone, OUT state text)
		RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED NOT SHIPPABLE ROWS 100 as 'get_local_active_session';
    CREATE OR REPLACE VIEW DBE_PERF.local_active_session AS
	  WITH RECURSIVE
		las(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
			tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
			user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state)
		  AS (select t.* from get_local_active_session() as t),
		tt(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
		   tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
		   user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state, final_block_sessionid, level, head)
		  AS(SELECT las.*, las.block_sessionid AS final_block_sessionid, 1 AS level, array_append('{}', las.sessionid) AS head FROM las
			UNION ALL
			 SELECT tt.sampleid, tt.sample_time, tt.need_flush_sample, tt.databaseid, tt.thread_id, tt.sessionid, tt.start_time, tt.event, tt.lwtid, tt.psessionid,
					tt.tlevel, tt.smpid, tt.userid, tt.application_name, tt.client_addr, tt.client_hostname, tt.client_port, tt.query_id, tt.unique_query_id,
					tt.user_id, tt.cn_id, tt.unique_query, tt.locktag, tt.lockmode, tt.block_sessionid, tt.wait_status, tt.global_sessionid, tt.xact_start_time, tt.query_start_time, tt.state,                                                                                                                 las.block_sessionid AS final_block_sessionid, tt.level + 1 AS level, array_append(tt.head, las.sessionid) AS head
			 FROM tt INNER JOIN las ON tt.final_block_sessionid = las.sessionid
			 WHERE las.sampleid = tt.sampleid AND (las.block_sessionid IS NOT NULL OR las.block_sessionid != 0)
			   AND las.sessionid != all(head) AND las.sessionid != las.block_sessionid)
	  SELECT sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
			 tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
			 user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, final_block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state FROM tt
		WHERE level = (SELECT MAX(level) FROM tt t1 WHERE t1.sampleid =  tt.sampleid AND t1.sessionid = tt.sessionid);
  end if;
END;
/