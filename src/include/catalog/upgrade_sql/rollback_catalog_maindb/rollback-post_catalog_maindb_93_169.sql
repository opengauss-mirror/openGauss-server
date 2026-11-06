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
          AND pg_catalog.pg_has_role(n.nspowner, 'USAGE')
          AND d.datname = current_database();

SET search_path TO DEFAULT;
