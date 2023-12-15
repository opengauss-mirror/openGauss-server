DROP FUNCTION IF EXISTS pg_catalog.publication_deparse_ddl_command_end() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4642;
CREATE FUNCTION pg_catalog.publication_deparse_ddl_command_end () 
RETURNS event_trigger 
LANGUAGE INTERNAL VOLATILE STRICT NOT FENCED
AS 'publication_deparse_ddl_command_end';

DROP FUNCTION IF EXISTS pg_catalog.publication_deparse_ddl_command_start() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4643;
CREATE FUNCTION pg_catalog.publication_deparse_ddl_command_start () 
RETURNS event_trigger 
LANGUAGE INTERNAL VOLATILE STRICT NOT FENCED
AS 'publication_deparse_ddl_command_start';

DROP FUNCTION IF EXISTS pg_catalog.pubddl_decode(pubddl bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4648;
CREATE FUNCTION pg_catalog.pubddl_decode (pubddl bigint) 
RETURNS text 
LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED
AS 'pubddl_decode';