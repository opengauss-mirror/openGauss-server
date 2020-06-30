--
--unlock the cluster
--The query content must be the same as the values of LOCK_CLUSTER_SQL and WAITLOCK_CLUSTER_SQL in the local/LocalQuery.py file.
--The value must be the same.
--
DECLARE
	result                BOOL;
--begin unlock the cluster sql
BEGIN
        FOR i in (select * from pg_stat_activity where query like 'select case (select pgxc_lock_for_backup()) when true then (select pg_sleep(%)::text) end;' or query like 'select case (select count(*) from pg_advisory_lock(65535,65535)) when true then (select pg_sleep(%)::text) end;')
        LOOP
        --set info  datid datname pid
        RAISE INFO 'datid: %, datname: %, pid: %', i.datid, i.datname, i.pid;
        --set info  usesysid usename application_name
        RAISE INFO 'usesysid: %, usename: %, application_name: %', i.usesysid, i.usename, i.application_name;
        --set info  client_addr client_hostname client_port
        RAISE INFO 'client_addr: %, client_hostname: %, client_port: %', i.client_addr, i.client_hostname, i.client_port;
        --set info  backend_start xact_start
        RAISE INFO 'backend_start: %, xact_start: %', i.backend_start, i.xact_start;
        --set info  query_start state_change
        RAISE INFO 'query_start: %, state_change: %', i.query_start, i.state_change;
        --set info  waiting state
        RAISE INFO 'waiting: %, state: %', i.waiting, i.state;
        --set info  query
        RAISE INFO 'query: %', i.query;
        --set result false
        result := false;
        --SELECT pg_cancel_backend
        SET xc_maintenance_mode = on; SELECT pg_cancel_backend(i.pid) INTO result; RESET xc_maintenance_mode;
        RAISE INFO 'cancel command result: %', result;
        END LOOP;
END;
/