/* contrib/gms_lob/gms_lob--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_lob" to load this file. \quit
create schema gms_lob;
GRANT USAGE ON SCHEMA gms_lob TO PUBLIC;

-- GMS_LOB Constants - Basic 
CREATE OR REPLACE FUNCTION gms_lob."CALL"() returns int
as $$
    begin
        return 12;
    end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gms_lob.FILE_READONLY() returns int
as $$
    begin
        return 0;
    end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gms_lob.LOB_READONLY() returns BINARY_INTEGER
as $$
    begin
        return 0;
    end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gms_lob.LOB_READWRITE() returns BINARY_INTEGER
as $$
    begin
        return 1;
    end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gms_lob.LOBMAXSIZE() returns numeric
as $$
    begin
        return 18446744073709551615;
    end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gms_lob.SESSION() returns INTEGER
as $$
    begin
        return 10;
    end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gms_lob.createtemporary(INOUT lob_loc BLOB, cache boolean, dur INTEGER DEFAULT 10, lobname text DEFAULT ':') 
RETURNS BLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_createtemporary'
LANGUAGE C IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.createtemporary(INOUT lob_loc CLOB, cache boolean, dur INTEGER DEFAULT 10, lobname text DEFAULT ':') 
RETURNS CLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_createtemporary'
LANGUAGE C IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.freetemporary(INOUT lob_loc BLOB, lobname text DEFAULT ':')
RETURNS BLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_freetemporary'
LANGUAGE C STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.freetemporary(INOUT lob_loc CLOB, lobname text DEFAULT ':')
RETURNS CLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_freetemporary'
LANGUAGE C STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.read(lob_loc BLOB, INOUT amount INTEGER, "offset" bigint, INOUT buffer raw, lobname text DEFAULT ':')
RETURNS record
AS 'MODULE_PATHNAME', 'gms_lob_og_read_blob'
LANGUAGE C NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.read(lob_loc CLOB, INOUT amount INTEGER, "offset" bigint, INOUT buffer varchar, lobname text DEFAULT ':')
RETURNS record
AS 'MODULE_PATHNAME', 'gms_lob_og_read_clob'
LANGUAGE C NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.write(INOUT lob_loc BLOB, amount numeric, "offset" numeric, buffer raw, lobname text DEFAULT ':')
RETURNS BLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_write_blob'
LANGUAGE C IMMUTABLE NOT FENCED;
CREATE OR REPLACE FUNCTION gms_lob.write(INOUT lob_loc CLOB, amount numeric, "offset" numeric, buffer varchar, lobname text DEFAULT ':')
RETURNS CLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_write_clob'
LANGUAGE C IMMUTABLE NOT FENCED;


CREATE OR REPLACE FUNCTION gms_lob.isopen(lob_loc BLOB, lobname text DEFAULT ':')
RETURNS INTEGER
AS 'MODULE_PATHNAME', 'gms_lob_og_isopen'
LANGUAGE C STRICT NOT FENCED;
CREATE OR REPLACE FUNCTION gms_lob.isopen(lob_loc CLOB, lobname text DEFAULT ':')
RETURNS INTEGER
AS 'MODULE_PATHNAME', 'gms_lob_og_isopen'
LANGUAGE C STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.open(INOUT lob_loc BLOB, open_mode INTEGER, lobname text DEFAULT ':')
RETURNS BLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_open'
LANGUAGE C NOT FENCED;
CREATE OR REPLACE FUNCTION gms_lob.open(INOUT lob_loc CLOB, open_mode INTEGER, lobname text DEFAULT ':')
RETURNS CLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_open'
LANGUAGE C NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.append(INOUT dest_lob BLOB, src_lob BLOB, lobname text DEFAULT ':') 
RETURNS BLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_append_blob'
LANGUAGE C IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.append(INOUT dest_lob CLOB, src_lob CLOB, lobname text DEFAULT ':')
RETURNS CLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_append_clob'
LANGUAGE C IMMUTABLE NOT FENCED;
CREATE OR REPLACE FUNCTION gms_lob.close(INOUT lob_loc BLOB, lobname text DEFAULT ':')
RETURNS BLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_close'
LANGUAGE C STRICT NOT FENCED;
CREATE OR REPLACE FUNCTION gms_lob.close(INOUT lob_loc CLOB, lobname text DEFAULT ':')
RETURNS CLOB
AS 'MODULE_PATHNAME', 'gms_lob_og_close'
LANGUAGE C STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.getlength(lob_loc BLOB, lobname text DEFAULT ':')
RETURNS INTEGER
AS 'MODULE_PATHNAME', 'gms_lob_og_bloblength'
LANGUAGE C IMMUTABLE STRICT NOT FENCED;
CREATE OR REPLACE FUNCTION gms_lob.getlength(lob_loc CLOB, lobname text DEFAULT ':')
RETURNS INTEGER
AS 'MODULE_PATHNAME', 'gms_lob_og_cloblength'
LANGUAGE C IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION gms_lob.getlength(lobname text DEFAULT ':')
RETURNS void
AS 'MODULE_PATHNAME', 'gms_lob_og_null'
LANGUAGE C IMMUTABLE STRICT NOT FENCED;