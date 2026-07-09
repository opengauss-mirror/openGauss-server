DO $DO$
DECLARE
    has_sys      boolean;
    has_perf     boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_catalog.pg_namespace where nspname='sys' limit 1) into has_sys;
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into has_perf;

    if has_perf = true then
        DROP VIEW IF EXISTS dbe_perf.locks cascade;
    end if;

    if has_sys = true then
        DROP VIEW IF EXISTS sys.sysprocesses cascade;
    end if;

    DROP VIEW IF EXISTS pg_catalog.pg_locks cascade;
	DROP FUNCTION IF EXISTS pg_catalog.pg_lock_status(OUT locktype text, OUT database oid, OUT relation oid, OUT page integer, OUT tuple smallint, OUT bucket integer, OUT virtualxid text, OUT transactionid xid, OUT classid oid, OUT objid oid, OUT objsubid smallint, OUT virtualtransaction text, OUT pid bigint, OUT sessionid bigint, OUT mode text, OUT granted boolean, OUT fastpath boolean, OUT locktag text, OUT global_sessionid text, OUT waitstart timestamptz) cascade;
	SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1371;
	CREATE OR REPLACE FUNCTION pg_catalog.pg_lock_status
	(
		OUT locktype text,
		OUT database oid,
		OUT relation oid,
		OUT page integer,
		OUT tuple smallint,
		OUT bucket integer,
		OUT virtualxid text,
		OUT transactionid xid,
		OUT classid oid,
		OUT objid oid,
		OUT objsubid smallint,
		OUT virtualtransaction text,
		OUT pid bigint,
		OUT sessionid bigint,
		OUT mode text,
		OUT granted boolean,
		OUT fastpath boolean,
		OUT locktag text,
		OUT global_sessionid text
	)
	RETURNS setof record LANGUAGE INTERNAL VOLATILE STRICT NOT FENCED as 'pg_lock_status';

    COMMENT ON FUNCTION pg_catalog.pg_lock_status
    (
        OUT locktype text,
        OUT database oid,
        OUT relation oid,
        OUT page integer,
        OUT tuple smallint,
        OUT bucket integer,
        OUT virtualxid text,
        OUT transactionid xid,
        OUT classid oid,
        OUT objid oid,
        OUT objsubid smallint,
        OUT virtualtransaction text,
        OUT pid bigint,
        OUT sessionid bigint,
        OUT mode text,
        OUT granted boolean,
        OUT fastpath boolean,
        OUT locktag text,
        OUT global_sessionid text
    ) IS 'view system lock information';


	CREATE OR REPLACE VIEW pg_catalog.pg_locks AS
		SELECT * FROM pg_catalog.pg_lock_status() AS L;
	
    if has_perf = true then
        CREATE OR REPLACE VIEW dbe_perf.locks AS
            SELECT * FROM pg_catalog.pg_lock_status() AS L;
    end if;

    if has_sys = true then
        CREATE OR REPLACE VIEW sys.sysprocesses AS
        SELECT
            blocked_activity.pid AS spid,
            CAST(NULL AS SMALLINT) AS kpid,
            (SELECT blocking_activity.pid
            FROM pg_locks blocked
            JOIN pg_locks blocking
            ON blocking.locktype = blocked.locktype
            AND blocking.database IS NOT DISTINCT FROM blocked.database
            AND blocking.relation IS NOT DISTINCT FROM blocked.relation
            AND blocking.page IS NOT DISTINCT FROM blocked.page
            AND blocking.tuple IS NOT DISTINCT FROM blocked.tuple
            AND blocking.virtualxid IS NOT DISTINCT FROM blocked.virtualxid
            AND blocking.transactionid IS NOT DISTINCT FROM blocked.transactionid
            AND blocking.classid IS NOT DISTINCT FROM blocked.classid
            AND blocking.objid IS NOT DISTINCT FROM blocked.objid
            AND blocking.objsubid IS NOT DISTINCT FROM blocked.objsubid
            AND blocking.pid != blocked.pid
            AND blocking.granted = true
            AND blocked.granted = false
            JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking.pid
            WHERE blocked.pid = blocked_activity.pid
            LIMIT 1
        ) AS blocked,
        CAST(NULL AS VARBINARY(2)) AS waittype,
        CAST(0 AS BIGINT) AS waittime,
        CAST(NULL AS NCHAR(32)) AS lastwaittype,
        CAST(NULL AS NCHAR(256)) AS waitresource,
        blocked_activity.datid AS dbid,
        blocked_activity.usesysid AS uid,
        0 AS cpu,
        CAST(0 AS BIGINT) AS physical_io,
        0 AS memusage,
        blocked_activity.backend_start AS login_time,
        blocked_activity.query_start AS last_batch,
        CAST(0 AS SMALLINT) AS ecid,
        CAST(0 AS SMALLINT) AS open_tran,
        CAST(blocked_activity.state AS NCHAR(30)) AS status,
        CAST(CAST(blocked_activity.usesysid AS INT) AS VARBINARY(86)) AS sid,
        CAST(blocked_activity.client_hostname AS NCHAR(128)) AS hostname,
        CAST(blocked_activity.application_name AS NCHAR(128)) AS program_name,
        CAST(NULL AS NCHAR(10)) AS hostprocess,
        blocked_activity.query AS cmd,
        CAST(NULL AS NCHAR(128)) AS nt_domain,
        CAST(NULL AS NCHAR(128)) AS nt_username,
        CAST(NULL AS NCHAR(12)) AS net_address,
        CAST(NULL AS NCHAR(12)) AS net_library,
        CAST(blocked_activity.usename AS NCHAR(128)) AS loginame,
        CAST(NULL AS VARBINARY(128)) AS context_info,
        CAST(NULL AS VARBINARY(20)) AS sql_handle,
        0 AS stmt_start,
        0 AS stmt_end,
        blocked_activity.query_id AS request_id
        FROM pg_stat_activity blocked_activity;
    end if;

END$DO$;
