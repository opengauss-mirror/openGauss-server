DROP FUNCTION IF EXISTS pg_catalog.lo_from_bytea(oid, bytea) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4990;
CREATE FUNCTION pg_catalog.lo_from_bytea(oid, bytea) RETURNS oid LANGUAGE INTERNAL VOLATILE STRICT AS 'lo_from_bytea';

DROP FUNCTION IF EXISTS pg_catalog.lo_get(oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4991;
CREATE FUNCTION pg_catalog.lo_get(oid) RETURNS bytea LANGUAGE INTERNAL VOLATILE STRICT AS 'lo_get';

DROP FUNCTION IF EXISTS pg_catalog.lo_get(oid, int8, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4992;
CREATE FUNCTION pg_catalog.lo_get(oid, int8, int4) RETURNS bytea LANGUAGE INTERNAL VOLATILE STRICT AS 'lo_get_fragment';

DROP FUNCTION IF EXISTS pg_catalog.lo_lseek64(int4, int8, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4993;
CREATE FUNCTION pg_catalog.lo_lseek64(int4, int8, int4) RETURNS int8 LANGUAGE INTERNAL VOLATILE STRICT AS 'lo_lseek64';

DROP FUNCTION IF EXISTS pg_catalog.lo_put(oid, int8, bytea) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4994;
CREATE FUNCTION pg_catalog.lo_put(oid, int8, bytea) RETURNS void LANGUAGE INTERNAL VOLATILE STRICT AS 'lo_put';

DROP FUNCTION IF EXISTS pg_catalog.lo_tell64(int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4995;
CREATE FUNCTION pg_catalog.lo_tell64(int4) RETURNS int8 LANGUAGE INTERNAL VOLATILE STRICT AS 'lo_tell64';

DROP FUNCTION IF EXISTS pg_catalog.lo_truncate64(int4, int8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4996;
CREATE FUNCTION pg_catalog.lo_truncate64(int4, int8) RETURNS int4 LANGUAGE INTERNAL VOLATILE STRICT AS 'lo_truncate64';
