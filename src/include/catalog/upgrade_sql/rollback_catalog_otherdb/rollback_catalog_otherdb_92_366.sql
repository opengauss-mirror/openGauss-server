--delete system relation pg_catalog.gs_model_warehouse and its indexes
DROP INDEX IF EXISTS pg_toast.pg_toast_3991_index;
DROP TYPE IF EXISTS pg_toast.pg_toast_3991;
DROP TABLE IF EXISTS pg_toast.pg_toast_3991;
DROP INDEX IF EXISTS pg_catalog.gs_model_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_model_name_index;
DROP TYPE IF EXISTS pg_catalog.gs_model_warehouse;
DROP TABLE IF EXISTS pg_catalog.gs_model_warehouse;

do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='db4ai' limit 1)
    LOOP
        if ans = true then
	    DROP FUNCTION IF EXISTS db4ai.create_snapshot;
	    DROP FUNCTION IF EXISTS db4ai.create_snapshot_internal;
            DROP FUNCTION IF EXISTS db4ai.prepare_snapshot;
            DROP FUNCTION IF EXISTS db4ai.prepare_snapshot_internal;
            DROP FUNCTION IF EXISTS db4ai.sample_snapshot;
            DROP FUNCTION IF EXISTS db4ai.publish_snapshot;
            DROP FUNCTION IF EXISTS db4ai.archive_snapshot;
            DROP FUNCTION IF EXISTS db4ai.manage_snapshot_internal;
            DROP FUNCTION IF EXISTS db4ai.purge_snapshot;
            DROP FUNCTION IF EXISTS db4ai.purge_snapshot_internal;
	    DROP TABLE IF EXISTS db4ai.snapshot;
	    DROP TYPE IF EXISTS db4ai.snapshot_name;
        end if;
        exit;
    END LOOP;
END$$;
 
DROP SCHEMA IF EXISTS db4ai cascade;