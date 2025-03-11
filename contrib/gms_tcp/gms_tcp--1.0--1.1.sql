/* contrib/gms_tcp/gms_tcp--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION gms_tcp UPDATE TO '1.1'" to load this file. \quit

DROP FUNCTION IF EXISTS gms_tcp.available_real;
--
-- function: available
-- determines the number of bytes available for reading from a tcp/ip connection.
--
CREATE FUNCTION gms_tcp.available_real(c       in gms_tcp.connection,
                                       timeout in int)
RETURNS integer
AS 'MODULE_PATHNAME','gms_tcp_available_real'
LANGUAGE C NOT FENCED;
