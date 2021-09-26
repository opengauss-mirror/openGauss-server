DROP FUNCTION IF EXISTS pg_catalog.pg_start_backup(IN backupid TEXT, IN fast BOOL, IN exclusive BOOL) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_stop_backup(IN exclusive BOOL) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6203;
CREATE OR REPLACE FUNCTION pg_catalog.pg_start_backup
(IN backupid pg_catalog.text,
IN fast pg_catalog.bool,
IN exclusive pg_catalog.bool)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT as 'pg_start_backup_v2';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6204;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stop_backup
(IN exclusive pg_catalog.bool,
out lsn pg_catalog.text,
out labelfile pg_catalog.text,
out spcmapfile pg_catalog.text)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT as 'pg_stop_backup_v2';
