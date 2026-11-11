SET search_path TO information_schema;

CREATE OR REPLACE VIEW schemata AS
    SELECT CAST(pg_catalog.current_database() AS sql_identifier) AS catalog_name,
           CAST(n.nspname AS sql_identifier) AS schema_name,
           CAST(u.rolname AS sql_identifier) AS schema_owner,
           CAST(null AS sql_identifier) AS default_character_set_catalog,
           CAST(null AS sql_identifier) AS default_character_set_schema,
           CAST(pg_encoding_to_char(d.encoding) AS sql_identifier) AS default_character_set_name,
           CAST(null AS character_data) AS sql_path,
           CAST(d.datcollate AS varchar(64)) AS default_collation_name,
           CAST('NO' AS varchar(3)) AS default_encryption
    FROM pg_namespace n, pg_authid u, pg_catalog.pg_database d
    WHERE n.nspowner = u.oid
          AND (pg_catalog.pg_has_role(n.nspowner, 'USAGE')
               OR (d.datcompatibility = 'B'
                   AND (pg_catalog.has_schema_privilege(n.oid, 'CREATE, USAGE')
                        OR EXISTS (SELECT 1 FROM pg_class c
                                   WHERE c.relnamespace = n.oid
                                         AND NOT pg_catalog.pg_is_other_temp_schema(n.oid)
                                         AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
                                              OR pg_catalog.has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                                              OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))))))
          AND d.datname = current_database();

SET search_path TO DEFAULT;
