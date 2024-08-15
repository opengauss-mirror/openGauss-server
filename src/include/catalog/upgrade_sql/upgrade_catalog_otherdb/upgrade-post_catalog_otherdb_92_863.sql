DROP FUNCTION IF EXISTS pg_catalog.gs_xlog_keepers() CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.gs_xlog_keepers
(out keeptype pg_catalog.text,
out keepsegment pg_catalog.text,
out describe pg_catalog.text)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT ROWS 1000 NOT SHIPPABLE as 'gs_xlog_keepers';