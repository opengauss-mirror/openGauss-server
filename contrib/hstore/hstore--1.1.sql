/* contrib/hstore/hstore--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hstore" to load this file. \quit

CREATE TYPE hstore;

CREATE FUNCTION hstore_in(cstring)
RETURNS hstore
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION hstore_out(hstore)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION hstore_recv(internal)
RETURNS hstore
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION hstore_send(hstore)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE TYPE hstore (
        INTERNALLENGTH = -1,
        INPUT = hstore_in,
        OUTPUT = hstore_out,
        RECEIVE = hstore_recv,
        SEND = hstore_send,
        STORAGE = extended
);

CREATE FUNCTION hstore_version_diag(hstore)
RETURNS integer
AS 'MODULE_PATHNAME','hstore_version_diag'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION fetchval(hstore,text)
RETURNS text
AS 'MODULE_PATHNAME','hstore_fetchval'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION slice_array(hstore,text[])
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_slice_to_array'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION slice(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_slice_to_hstore'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION isexists(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION exist(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION exists_any(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_any'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION exists_all(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_all'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION isdefined(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_defined'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION defined(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_defined'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION delete(hstore,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION delete(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_array'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION delete(hstore,hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_hstore'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION hs_concat(hstore,hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_concat'
LANGUAGE C IMMUTABLE NOT FENCED;;

CREATE FUNCTION hs_contains(hstore,hstore)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_contains'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION hs_contained(hstore,hstore)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_contained'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION tconvert(text,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_text'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_text'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text[],text[])
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_arrays'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; allows (keys,null)

CREATE FUNCTION hstore(text[])
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_array'
LANGUAGE C IMMUTABLE STRICT NOT FENCED;;

CREATE CAST (text[] AS hstore)
  WITH FUNCTION hstore(text[]);

CREATE FUNCTION hstore(record)
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_record'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; allows (null::recordtype)

CREATE FUNCTION hstore_to_array(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_to_array'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION hstore_to_matrix(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_to_matrix'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION akeys(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_akeys'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION avals(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_avals'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION skeys(hstore)
RETURNS setof text
AS 'MODULE_PATHNAME','hstore_skeys'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION svals(hstore)
RETURNS setof text
AS 'MODULE_PATHNAME','hstore_svals'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION each(IN hs hstore,
    OUT key text,
    OUT value text)
RETURNS SETOF record
AS 'MODULE_PATHNAME','hstore_each'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;
