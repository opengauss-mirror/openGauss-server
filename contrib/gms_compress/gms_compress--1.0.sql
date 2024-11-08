/* contrib/gms_compress/gms_compress--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_compress" to load this file. \quit

CREATE SCHEMA gms_compress;
GRANT USAGE ON SCHEMA gms_compress TO PUBLIC;

set behavior_compat_options='proc_outparam_override';
--These functions and procedures compress data using Lempel-Ziv compression algorithm.
CREATE OR REPLACE FUNCTION gms_compress.lz_compress(src IN raw, quality IN integer := 6)
RETURNS raw PACKAGE
AS 'MODULE_PATHNAME','gms_lz_compress'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_compress.lz_compress(src IN blob, quality IN integer := 6)
RETURNS blob PACKAGE
AS 'MODULE_PATHNAME','gms_lz_compress'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE gms_compress.lz_compress(src IN blob, dst INOUT blob, quality IN integer := 6) PACKAGE as
begin 
    dst := gms_compress.lz_compress(src, quality);
end;

CREATE OR REPLACE FUNCTION gms_compress.lz_compress_open(src INOUT blob, quality IN integer := 6)
RETURNS integer
AS 'MODULE_PATHNAME','gms_lz_compress_open'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_compress.lz_compress_close(handle IN integer)
RETURNS blob PACKAGE
AS '$libdir/gms_compress', 'gms_lz_compress_close'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE gms_compress.lz_compress_close(handle IN integer, dst INOUT blob) PACKAGE AS
begin 
    dst := gms_compress.lz_compress_close(handle);
end;

CREATE OR REPLACE FUNCTION gms_compress.lz_uncompress_open(src IN blob)
RETURNS integer
AS 'MODULE_PATHNAME','gms_lz_uncompress_open'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_compress.uncompress_close(handle IN integer)
RETURNS void
AS '$libdir/gms_compress', 'gms_lz_uncompress_close'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE gms_compress.lz_uncompress_close(handle IN integer) AS
begin 
    gms_compress.uncompress_close(handle);
end;

CREATE OR REPLACE FUNCTION gms_compress.compress_add(handle IN integer, src IN raw)
RETURNS void PACKAGE
AS '$libdir/gms_compress', 'gms_lz_compress_add'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE gms_compress.lz_compress_add(handle IN integer, dst INOUT blob, src IN raw) PACKAGE AS
begin 
    IF src IS NULL THEN
        RAISE EXCEPTION 'parameter cannot be NULL';
    END IF;
    gms_compress.compress_add(handle, src);
end;

CREATE OR REPLACE FUNCTION gms_compress.uncompress_extract(handle IN integer)
RETURNS raw PACKAGE
AS '$libdir/gms_compress', 'gms_lz_uncompress_extract'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE gms_compress.lz_uncompress_extract(handle IN integer, dst INOUT raw) PACKAGE AS
begin 
    dst := gms_compress.uncompress_extract(handle);
end;

CREATE OR REPLACE FUNCTION gms_compress.isopen(handle IN integer)
RETURNS boolean
AS '$libdir/gms_compress', 'gms_isopen'
LANGUAGE C;

----These functions and procedures decompress data using the Lempel-Ziv decompression algorithm.

CREATE OR REPLACE FUNCTION gms_compress.lz_uncompress(src IN raw)
RETURNS raw PACKAGE
AS 'MODULE_PATHNAME','gms_lz_uncompress'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_compress.lz_uncompress(src IN blob)
RETURNS blob PACKAGE
AS 'MODULE_PATHNAME','gms_lz_uncompress'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE gms_compress.lz_uncompress(src IN blob, dst INOUT blob) PACKAGE as
begin 
    IF src IS NULL OR dst IS NULL THEN
        RAISE EXCEPTION 'parameter cannot be NULL';
    END IF;
    dst := gms_compress.lz_uncompress(src);
end;
