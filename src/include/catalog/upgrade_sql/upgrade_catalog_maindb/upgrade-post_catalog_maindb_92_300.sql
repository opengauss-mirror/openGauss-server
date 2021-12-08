DROP FUNCTION IF EXISTS pg_catalog.gs_get_archive_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3796;
CREATE FUNCTION pg_catalog.gs_get_archive_status(OUT archive_standby TEXT, OUT last_task_lsn TEXT, OUT "last_arch_time" TIMESTAMPTZ, OUT archive_path TEXT)
RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE ROWS 1  STRICT as 'gs_get_archive_status';
CREATE OR REPLACE VIEW pg_catalog.gs_archive_status AS SELECT * FROM gs_get_archive_status();
