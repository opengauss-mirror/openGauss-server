-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ndpplugin" to load this file. \quit

CREATE FUNCTION pg_catalog.ndpplugin_invoke()
    RETURNS VOID AS '$libdir/ndpplugin','ndpplugin_invoke' LANGUAGE C STRICT;

CREATE FUNCTION pg_catalog.pushdown_statistics()
RETURNS TABLE (query bigint, total_pushdown_page bigint, back_to_gauss bigint, received_scan bigint, received_agg bigint, failed_backend_handle bigint, failed_sendback bigint)
AS '$libdir/ndpplugin', 'pushdown_statistics'
LANGUAGE C STRICT;