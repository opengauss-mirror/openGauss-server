DROP VIEW IF EXISTS pg_catalog.ss_transaction_sync_status CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.ss_transaction_sync_stat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6225;
CREATE OR REPLACE FUNCTION pg_catalog.ss_transaction_sync_stat(
    OUT message_type text,
    OUT transfer_type text,
    OUT times bigint,
    OUT total_cost bigint,
    OUT average_cost bigint
) RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE ROWS 8
AS $function$ss_transaction_sync_stat$function$;

COMMENT ON FUNCTION pg_catalog.ss_transaction_sync_stat() IS 'statistics: UB and DMS transaction sync latency in nanoseconds';

CREATE OR REPLACE VIEW pg_catalog.ss_transaction_sync_status AS
    SELECT * FROM pg_catalog.ss_transaction_sync_stat();
