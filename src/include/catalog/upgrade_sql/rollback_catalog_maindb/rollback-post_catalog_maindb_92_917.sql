-- drop wal statistics functions
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walsender(int4) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walreceiver(int4) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walrecvwriter(int4) CASCADE;
