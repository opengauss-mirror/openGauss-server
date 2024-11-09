/* contrib/gms_output/gms_output--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_output" to load this file. \quit

CREATE SCHEMA gms_inaddr;
GRANT USAGE ON SCHEMA gms_inaddr TO PUBLIC;

CREATE OR REPLACE FUNCTION gms_inaddr.get_host_address(text default 'localhost')
RETURNS text
AS 'MODULE_PATHNAME','gms_inaddr_get_host_address'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_inaddr.get_host_name(text default '127.0.0.1')
RETURNS text
AS 'MODULE_PATHNAME','gms_inaddr_get_host_name'
LANGUAGE C;
