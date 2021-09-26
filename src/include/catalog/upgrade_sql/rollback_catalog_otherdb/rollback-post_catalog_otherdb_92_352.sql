DROP VIEW IF EXISTS pg_catalog.pg_control_system cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_control_system() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3441;
CREATE FUNCTION pg_catalog.pg_control_system
(
OUT "pg_control_version" int,
OUT "catalog_version_no" int,
OUT "system_identifier" bigint,
OUT "pg_control_last_modified" timestamp
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_control_system';

CREATE VIEW pg_catalog.pg_control_system AS SELECT * FROM pg_control_system();
