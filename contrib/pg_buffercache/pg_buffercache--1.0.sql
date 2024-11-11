/* contrib/pg_buffercache/pg_buffercache--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_buffercache" to load this file. \quit

-- Create a view for convenient access.
CREATE VIEW pg_buffercache AS
	SELECT * FROM pg_buffercache_pages();

-- Don't want these to be available to public.
REVOKE ALL ON pg_buffercache FROM PUBLIC;
