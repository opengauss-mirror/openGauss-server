DROP FUNCTION IF EXISTS pg_catalog.gs_xlog_keepers();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,9040;
CREATE OR REPLACE FUNCTION pg_catalog.gs_xlog_keepers
(out keeptype pg_catalog.text,
out keepsegment pg_catalog.text,
out describe pg_catalog.text)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT as 'gs_xlog_keepers';
