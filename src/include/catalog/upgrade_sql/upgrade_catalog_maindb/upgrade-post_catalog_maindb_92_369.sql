--step1 drop the pgxc_version()
DROP FUNCTION IF EXISTS PG_CATALOG.pgxc_version();

--step2 add new sys function opengauss_version()
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 90;
CREATE FUNCTION pg_catalog.opengauss_version() RETURNS text LANGUAGE INTERNAL as 'opengauss_version';
