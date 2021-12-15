-- deleting system table pg_publication

DROP INDEX IF EXISTS pg_catalog.pg_publication_oid_index;
DROP INDEX IF EXISTS pg_catalog.pg_publication_pubname_index;
DROP TYPE IF EXISTS pg_catalog.pg_publication;
DROP TABLE IF EXISTS pg_catalog.pg_publication;

-- deleting system table pg_publication_rel

DROP INDEX IF EXISTS pg_catalog.pg_publication_rel_oid_index;
DROP INDEX IF EXISTS pg_catalog.pg_publication_rel_map_index;
DROP TYPE IF EXISTS pg_catalog.pg_publication_rel;
DROP TABLE IF EXISTS pg_catalog.pg_publication_rel;