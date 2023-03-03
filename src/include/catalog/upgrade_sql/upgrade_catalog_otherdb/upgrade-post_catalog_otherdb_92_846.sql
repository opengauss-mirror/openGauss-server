-- upgrade TYPE
DROP TYPE IF EXISTS pg_catalog.event_trigger;
DROP TYPE IF EXISTS pg_catalog.pg_ddl_command;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3838, 0, p;
CREATE TYPE pg_catalog.event_trigger;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5729, 0, p;
CREATE TYPE pg_catalog.pg_ddl_command;
-- upgrade FUNCTIONS
DROP FUNCTION IF EXISTS pg_catalog.pg_identify_object(IN classid oid, IN objid oid, IN subobjid int4, OUT type text, OUT schema text, OUT name text, OUT identity text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5725;
CREATE OR REPLACE FUNCTION pg_catalog.pg_identify_object(IN classid oid, IN objid oid, IN subobjid int4, OUT type text, OUT schema text, OUT name text, OUT identity text) RETURNS record AS 'pg_identify_object' LANGUAGE INTERNAL VOLATILE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.pg_get_object_address(IN type text, IN name text[], IN args text[], OUT classid oid, OUT objid oid, OUT subobjid int4);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5728;
CREATE OR REPLACE FUNCTION pg_catalog.pg_get_object_address(IN type text, IN name text[], IN args text[], OUT classid oid, OUT objid oid, OUT subobjid int4) RETURNS record AS 'pg_get_object_address' LANGUAGE INTERNAL VOLATILE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.event_trigger_in(cstring);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3594;
CREATE OR REPLACE FUNCTION pg_catalog.event_trigger_in(cstring) RETURNS event_trigger LANGUAGE INTERNAL stable as 'event_trigger_in';

DROP FUNCTION IF EXISTS pg_catalog.event_trigger_out(event_trigger);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3595;
CREATE OR REPLACE FUNCTION pg_catalog.event_trigger_out(event_trigger) RETURNS cstring LANGUAGE INTERNAL stable as 'event_trigger_out';

DROP FUNCTION IF EXISTS pg_catalog.pg_ddl_command_in(cstring);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5801;
CREATE OR REPLACE FUNCTION pg_catalog.pg_ddl_command_in(cstring) RETURNS pg_ddl_command LANGUAGE INTERNAL STRICT IMMUTABLE as 'pg_ddl_command_in';

DROP FUNCTION IF EXISTS pg_catalog.pg_ddl_command_out(pg_ddl_command);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5803;
CREATE OR REPLACE FUNCTION pg_catalog.pg_ddl_command_out(pg_ddl_command) RETURNS cstring LANGUAGE INTERNAL STRICT IMMUTABLE as 'pg_ddl_command_out';

DROP FUNCTION IF EXISTS pg_catalog.pg_ddl_command_recv(internal);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5807;
CREATE OR REPLACE FUNCTION pg_catalog.pg_ddl_command_recv(internal) RETURNS pg_ddl_command LANGUAGE INTERNAL STRICT IMMUTABLE as 'pg_ddl_command_recv';

DROP FUNCTION IF EXISTS pg_catalog.pg_ddl_command_send(pg_ddl_command);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5809;
CREATE OR REPLACE FUNCTION pg_catalog.pg_ddl_command_send(pg_ddl_command) RETURNS bytea LANGUAGE INTERNAL STRICT IMMUTABLE as 'pg_ddl_command_send';

DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_ddl_commands(OUT classid oid, OUT objid oid, OUT objsubid int4, OUT command_tag text, OUT object_type text, OUT schema_name text, OUT object_identity text,  OUT in_extension bool, OUT command pg_ddl_command );
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8852;
CREATE OR REPLACE FUNCTION pg_catalog.pg_event_trigger_ddl_commands(OUT classid oid, OUT objid oid, OUT objsubid int4, OUT command_tag text, OUT object_type text, OUT schema_name text, OUT object_identity text,  OUT in_extension bool, OUT command pg_ddl_command ) RETURNS record LANGUAGE INTERNAL STRICT VOLATILE as 'pg_event_trigger_ddl_commands';

DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_dropped_objects(OUT classid oid, OUT objid oid, OUT objsubid int4, OUT original bool, OUT normal bool, OUT is_temporary bool, OUT object_type text,  OUT schema_name text, OUT object_name text, OUT object_identity text, OUT address_names text[], OUT address_args text[] );
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5724;
CREATE OR REPLACE FUNCTION pg_catalog.pg_event_trigger_dropped_objects(OUT classid oid, OUT objid oid, OUT objsubid int4, OUT original bool, OUT normal bool, OUT is_temporary bool, OUT object_type text,  OUT schema_name text, OUT object_name text, OUT object_identity text, OUT address_names text[], OUT address_args text[] ) RETURNS record LANGUAGE INTERNAL STRICT VOLATILE as 'pg_event_trigger_dropped_objects';

DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_table_rewrite_oid(out oid oid);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5726;
CREATE OR REPLACE FUNCTION pg_catalog.pg_event_trigger_table_rewrite_oid(out oid oid) RETURNS oid LANGUAGE INTERNAL STRICT VOLATILE as 'pg_event_trigger_table_rewrite_oid';

DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_table_rewrite_reason();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5727;
CREATE OR REPLACE FUNCTION pg_catalog.pg_event_trigger_table_rewrite_reason() RETURNS int4 LANGUAGE INTERNAL STRICT VOLATILE as 'pg_event_trigger_table_rewrite_reason';

--upgrade type
CREATE TYPE pg_catalog.event_trigger (input=event_trigger_in,output=event_trigger_out,internallength=4,passedbyvalue,CATEGORY=p);
CREATE TYPE pg_catalog.pg_ddl_command(input=pg_ddl_command_in, output=pg_ddl_command_out, RECEIVE = pg_ddl_command_recv, SEND = pg_ddl_command_send,internallength=4,passedbyvalue,CATEGORY=p);