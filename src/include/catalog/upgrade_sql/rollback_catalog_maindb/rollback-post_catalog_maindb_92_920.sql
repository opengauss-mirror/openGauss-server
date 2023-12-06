DROP VIEW IF EXISTS pg_catalog.pg_publication_tables;
CREATE VIEW pg_catalog.pg_publication_tables AS
    SELECT
        P.pubname AS pubname,
        N.nspname AS schemaname,
        C.relname AS tablename
    FROM pg_publication P, pg_class C
         JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.oid IN (SELECT relid FROM pg_catalog.pg_get_publication_tables(P.pubname));

-- drop reform info functions
DROP FUNCTION IF EXISTS pg_catalog.query_node_reform_info_from_dms() CASCADE;
-- drop drc info functions
DROP FUNCTION IF EXISTS pg_catalog.query_all_drc_info() CASCADE;