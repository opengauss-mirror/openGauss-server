/* contrib/gms_match/gms_match--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_match" to load this file. \quit

-- gms_match package begin
CREATE SCHEMA gms_match;
GRANT USAGE ON SCHEMA gms_match TO PUBLIC;
create or replace function gms_match.edit_distance(s1 in varchar2, s2 in varchar2)
    returns integer
AS 'MODULE_PATHNAME','edit_distance'
LANGUAGE C IMMUTABLE NOT FENCED;

create or replace function gms_match.edit_distance_similarity(s1 in varchar2, s2 in varchar2)
    returns integer
AS 'MODULE_PATHNAME','edit_distance_similarity'
LANGUAGE C IMMUTABLE NOT FENCED;

-- gms_match package end
