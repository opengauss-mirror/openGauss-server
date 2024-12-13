/*------ add sys fuction subtype_in ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9462;
CREATE FUNCTION pg_catalog.subtype_in(cstring, oid, int4)
RETURNS "any" LANGUAGE INTERNAL as 'subtype_in' STABLE;
COMMENT ON FUNCTION pg_catalog.subtype_in(cstring, oid, int4) IS 'I/O';

/*------ add sys fuction subtype_recv ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9463;
CREATE FUNCTION pg_catalog.subtype_recv(internal, oid, int4)
RETURNS "any" LANGUAGE INTERNAL as 'subtype_recv' STABLE;
COMMENT ON FUNCTION pg_catalog.subtype_recv(internal, oid, int4) IS 'I/O';