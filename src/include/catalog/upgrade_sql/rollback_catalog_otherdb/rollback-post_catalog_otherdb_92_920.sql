DROP VIEW IF EXISTS pg_catalog.pg_publication_tables;
CREATE VIEW pg_catalog.pg_publication_tables AS
    SELECT
        P.pubname AS pubname,
        N.nspname AS schemaname,
        C.relname AS tablename
    FROM pg_publication P, pg_class C
         JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.oid IN (SELECT relid FROM pg_catalog.pg_get_publication_tables(P.pubname));
