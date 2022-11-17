SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4990;
CREATE FUNCTION pg_catalog.lo_from_bytea(oid, bytea) RETURNS oid LANGUAGE INTERNAL VOLATILE AS 'lo_from_bytea';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4991;
CREATE FUNCTION pg_catalog.lo_get(oid) RETURNS bytea LANGUAGE INTERNAL VOLATILE AS 'lo_get';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4992;
CREATE FUNCTION pg_catalog.lo_get(oid, int8, int4) RETURNS bytea LANGUAGE INTERNAL VOLATILE AS 'lo_get_fragment';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4993;
CREATE FUNCTION pg_catalog.lo_lseek64(int4, int8, int4) RETURNS int8 LANGUAGE INTERNAL VOLATILE AS 'lo_lseek64';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4994;
CREATE FUNCTION pg_catalog.lo_put(oid, int8, bytea) RETURNS void LANGUAGE INTERNAL VOLATILE AS 'lo_put';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4995;
CREATE FUNCTION pg_catalog.lo_tell64(int4) RETURNS int8 LANGUAGE INTERNAL VOLATILE AS 'lo_tell64';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4996;
CREATE FUNCTION pg_catalog.lo_truncate64(int4, int8) RETURNS int4 LANGUAGE INTERNAL VOLATILE AS 'lo_truncate64';
