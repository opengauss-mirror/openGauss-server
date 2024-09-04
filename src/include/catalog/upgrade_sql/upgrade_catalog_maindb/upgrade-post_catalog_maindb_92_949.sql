DROP FUNCTION IF EXISTS pg_catalog.publication_deparse_table_rewrite() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4644;
CREATE FUNCTION pg_catalog.publication_deparse_table_rewrite () 
RETURNS event_trigger 
LANGUAGE INTERNAL VOLATILE STRICT NOT FENCED
AS 'publication_deparse_table_rewrite';
