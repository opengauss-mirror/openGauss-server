DROP FUNCTION IF EXISTS pg_catalog.get_synchronised_standby_ip() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1234;
CREATE FUNCTION pg_catalog.get_synchronised_standby_ip(OUT standby_ip text) RETURNS text LANGUAGE INTERNAL STABLE as 'get_synchronised_standby_ip';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;