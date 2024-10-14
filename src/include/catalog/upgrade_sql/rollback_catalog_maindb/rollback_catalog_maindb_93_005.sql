--rollback TABLE
DROP INDEX IF EXISTS pg_catalog.pg_object_type_index;
DROP INDEX IF EXISTS pg_catalog.pg_object_type_oid_index;

DROP TYPE IF EXISTS pg_catalog.pg_object_type;
DROP TABLE IF EXISTS pg_catalog.pg_object_type;