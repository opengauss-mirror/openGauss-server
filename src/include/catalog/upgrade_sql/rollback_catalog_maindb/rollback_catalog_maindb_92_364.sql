-- 1. drop blockchain schema
DROP SCHEMA IF EXISTS blockchain;

-- 2. drop types
DROP TYPE IF EXISTS pg_catalog.hash16 CASCADE;
DROP TYPE IF EXISTS pg_catalog._hash16 CASCADE;
DROP TYPE IF EXISTS pg_catalog.hash32 CASCADE;
DROP TYPE IF EXISTS pg_catalog._hash32 CASCADE;

-- 3. drop functions
DROP FUNCTION IF EXISTS pg_catalog.get_dn_hist_relhash(text, text);
DROP FUNCTION IF EXISTS pg_catalog.ledger_gchain_check(text, text);
DROP FUNCTION IF EXISTS pg_catalog.ledger_hist_archive(text, text);
DROP FUNCTION IF EXISTS pg_catalog.ledger_gchain_archive();
DROP FUNCTION IF EXISTS pg_catalog.ledger_hist_check(text, text);
DROP FUNCTION IF EXISTS pg_catalog.ledger_hist_repair(text, text);
DROP FUNCTION IF EXISTS pg_catalog.ledger_gchain_repair(text, text);

-- 4. drop system relation gs_global_chain and its indexes.
DROP INDEX IF EXISTS pg_toast.pg_toast_5818_index;
DROP TYPE IF EXISTS pg_toast.pg_toast_5818;
DROP TABLE IF EXISTS pg_toast.pg_toast_5818;
DROP INDEX IF EXISTS pg_catalog.gs_global_chain_relid_index;
DROP TYPE IF EXISTS pg_catalog.gs_global_chain;
DROP TABLE IF EXISTS pg_catalog.gs_global_chain;
