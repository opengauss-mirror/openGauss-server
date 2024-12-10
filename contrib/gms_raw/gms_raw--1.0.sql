/* contrib/gms_raw/gms_raw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_raw" to load this file. \quit

-- gms_raw package begin
CREATE SCHEMA gms_raw;
GRANT USAGE ON SCHEMA gms_raw TO PUBLIC;

CREATE FUNCTION gms_raw.bit_and(r1 in raw, r2 in raw)
    returns raw
AS 'MODULE_PATHNAME','bit_and'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.bit_or(r1 in raw, r2 in raw)
    returns raw
AS 'MODULE_PATHNAME','bit_or'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.bit_xor(r1 in raw, r2 in raw)
    returns raw
AS 'MODULE_PATHNAME','bit_xor'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.bit_complement(r1 in raw)
    returns raw
AS 'MODULE_PATHNAME','bit_complement'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_from_binary_double(n in binary_double, endianess in integer default 1)
    returns raw
AS 'MODULE_PATHNAME','cast_from_binary_double'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_from_binary_float(n in float, endianess in integer default 1)
    returns raw
AS 'MODULE_PATHNAME','cast_from_binary_float'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_from_binary_integer(n in bigint, endianess in integer default 1)
    returns raw
AS 'MODULE_PATHNAME','cast_from_binary_integer'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_from_number(n in number)
    returns raw
AS 'MODULE_PATHNAME','cast_from_number'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_to_binary_double(r in raw, endianess in integer default 1)
    returns binary_double
AS 'MODULE_PATHNAME','cast_to_binary_double'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_to_binary_float(r in raw, endianess in integer default 1)
    returns float4
AS 'MODULE_PATHNAME','cast_to_binary_float'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_to_binary_integer(r in raw, endianess in integer default 1)
    returns binary_integer
AS 'MODULE_PATHNAME','cast_to_binary_integer'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_to_number(r in raw)
    returns number
AS 'MODULE_PATHNAME','cast_to_number'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_to_nvarchar2(r in raw)
    returns nvarchar2
AS 'MODULE_PATHNAME','cast_to_nvarchar2'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_to_raw(c in varchar2)
    returns raw
AS 'MODULE_PATHNAME','cast_to_raw'
LANGUAGE C STRICT STABLE NOT FENCED;

CREATE FUNCTION gms_raw.cast_to_varchar2(r in raw)
    returns varchar2
AS 'MODULE_PATHNAME','cast_to_varchar2'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION gms_raw.compare(r1 in raw, r2 in raw, pad in raw default null)
    returns number
AS 'MODULE_PATHNAME','compare'
LANGUAGE C IMMUTABLE NOT FENCED;

CREATE FUNCTION gms_raw.concat(
    r1 in raw default null,
    r2 in raw default null,
    r3 in raw default null,
    r4 in raw default null,
    r5 in raw default null,
    r6 in raw default null,
    r7 in raw default null,
    r8 in raw default null,
    r9 in raw default null,
    r10 in raw default null,
    r11 in raw default null,
    r12 in raw default null
    )
    returns raw
AS 'MODULE_PATHNAME','concat'
LANGUAGE C STABLE NOT FENCED;

CREATE FUNCTION gms_raw.convert(r in raw, to_charset in varchar2, from_charset in varchar2)
    returns raw
AS 'MODULE_PATHNAME','convert'
LANGUAGE C STABLE NOT FENCED;

CREATE FUNCTION gms_raw.copies(r in raw, n in number)
    returns raw
AS 'MODULE_PATHNAME','copies'
LANGUAGE C STABLE NOT FENCED;

CREATE FUNCTION gms_raw.reverse(r in raw)
    returns raw
AS 'MODULE_PATHNAME','reverse'
LANGUAGE C STABLE NOT FENCED;

CREATE FUNCTION gms_raw.translate(r in raw, from_set in raw, to_set in raw)
    returns raw
AS 'MODULE_PATHNAME','func_translate'
LANGUAGE C STABLE NOT FENCED;

CREATE FUNCTION gms_raw.transliterate(r in raw, to_set in raw default null, from_set in raw default null,
    pad in raw default null)
    returns raw
AS 'MODULE_PATHNAME','transliterate'
LANGUAGE C STABLE NOT FENCED;

CREATE FUNCTION gms_raw.xrange(start_byte in raw default null, end_byte in raw default null)
    returns raw
AS 'MODULE_PATHNAME','xrange'
LANGUAGE C STABLE NOT FENCED;

-- gms_raw package end
