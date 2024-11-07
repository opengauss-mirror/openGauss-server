/* contrib/gms_i18n/gms_i18n--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_i18n" to load this file. \quit

CREATE SCHEMA gms_i18n;
GRANT USAGE ON SCHEMA gms_i18n TO PUBLIC;

CREATE OR REPLACE FUNCTION gms_i18n.raw_to_char(IN rawdata raw, IN charset varchar2 DEFAULT NULL)
RETURNS varchar2
AS 'MODULE_PATHNAME', 'gms_i18n_raw_to_char'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION gms_i18n.string_to_raw(IN strdata varchar2, IN charset varchar2 DEFAULT NULL)
RETURNS raw
AS 'MODULE_PATHNAME', 'gms_i18n_string_to_raw'
LANGUAGE C IMMUTABLE;