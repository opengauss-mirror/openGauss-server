DROP VIEW IF EXISTS pg_catalog.pg_publication_tables;
CREATE VIEW pg_catalog.pg_publication_tables AS
    SELECT
        gpt.pubname AS pubname,
        N.nspname AS schemaname,
        C.relname AS tablename
    FROM (SELECT
         P.pubname,
         pg_catalog.pg_get_publication_tables(P.pubname) relid
         FROM pg_publication P) gpt,
         pg_class C
         JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.oid = gpt.relid;
