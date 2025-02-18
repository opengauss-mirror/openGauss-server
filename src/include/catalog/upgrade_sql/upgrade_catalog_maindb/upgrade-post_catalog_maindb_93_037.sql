DROP TYPE IF EXISTS pg_catalog.bfile;
DROP TYPE IF EXISTS pg_catalog._bfile;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 91, 3203, b;
CREATE TYPE pg_catalog.bfile;

DROP FUNCTION IF EXISTS pg_catalog.bfilein(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4470;
CREATE FUNCTION pg_catalog.bfilein(cstring)
RETURNS bfile
LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED
AS 'bfilein';
COMMENT ON FUNCTION pg_catalog.bfilein(cstring) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.bfileout(bfile) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4471;
CREATE FUNCTION pg_catalog.bfileout(bfile)
RETURNS CSTRING
LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED
AS 'bfileout';
COMMENT ON FUNCTION pg_catalog.bfileout(bfile) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.bfilerecv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4472;
CREATE FUNCTION pg_catalog.bfilerecv(internal)
RETURNS bfile
LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED
AS 'bfilerecv';
COMMENT ON FUNCTION pg_catalog.bfilerecv(internal) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.bfilesend(bfile) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4473;
CREATE FUNCTION pg_catalog.bfilesend(bfile)
RETURNS bytea
LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED
AS 'bfilesend';
COMMENT ON FUNCTION pg_catalog.bfilesend(bfile) IS 'I/O';

CREATE TYPE pg_catalog.bfile (
    internallength = VARIABLE,
    input = bfilein,
    output = bfileout,
    receive = bfilerecv,
    send = bfilesend
);

DROP FUNCTION IF EXISTS pg_catalog.bfilename(location text, filename text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4474;
CREATE FUNCTION pg_catalog.bfilename(location text, filename text)
RETURNS bfile
LANGUAGE INTERNAL IMMUTABLE NOT FENCED
AS 'bfilename';
COMMENT ON FUNCTION pg_catalog.bfilename(location text, filename text) IS 'get a bfile locator';