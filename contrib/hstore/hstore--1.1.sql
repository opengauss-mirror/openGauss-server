/* contrib/hstore/hstore--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hstore" to load this file. \quit

CREATE TYPE hstore;

CREATE FUNCTION pg_catalog.hstore_in(cstring)
RETURNS hstore
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION pg_catalog.hstore_out(hstore)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION pg_catalog.hstore_recv(internal)
RETURNS hstore
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.hstore_send(hstore)
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

CREATE FUNCTION pg_catalog.hstore_version_diag(hstore)
RETURNS integer
AS 'MODULE_PATHNAME','hstore_version_diag'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.fetchval(hstore,text)
RETURNS text
AS 'MODULE_PATHNAME','hstore_fetchval'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.slice_array(hstore,text[])
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_slice_to_array'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.slice(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_slice_to_hstore'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.isexists(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.exist(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.exists_any(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_any'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.exists_all(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_all'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.isdefined(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_defined'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.defined(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_defined'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.delete(hstore,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.delete(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_array'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.delete(hstore,hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_hstore'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.hs_concat(hstore,hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_concat'
LANGUAGE C IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.hs_contains(hstore,hstore)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_contains'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.hs_contained(hstore,hstore)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_contained'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.tconvert(text,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_text'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION pg_catalog.hstore(text,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_text'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION pg_catalog.hstore(text[],text[])
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_arrays'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; allows (keys,null)

CREATE FUNCTION pg_catalog.hstore(text[])
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_array'
LANGUAGE C IMMUTABLE STRICT NOT FENCED;;

CREATE CAST (text[] AS hstore)
  WITH FUNCTION pg_catalog.hstore(text[]);

CREATE FUNCTION pg_catalog.hstore(record)
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_record'
LANGUAGE C IMMUTABLE NOT FENCED;; -- not STRICT; allows (null::recordtype)

CREATE FUNCTION pg_catalog.hstore_to_array(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_to_array'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.hstore_to_matrix(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_to_matrix'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.akeys(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_akeys'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.avals(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_avals'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.skeys(hstore)
RETURNS setof text
AS 'MODULE_PATHNAME','hstore_skeys'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.svals(hstore)
RETURNS setof text
AS 'MODULE_PATHNAME','hstore_svals'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;

CREATE FUNCTION pg_catalog.each(IN hs hstore,
    OUT key text,
    OUT value text)
RETURNS SETOF record
AS 'MODULE_PATHNAME','hstore_each'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;;
