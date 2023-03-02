--rollback function
DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_ddl_commands(OUT classid oid, OUT objid oid, OUT objsubid int4, OUT command_tag text, OUT object_type text, OUT schema_name text, OUT object_identity text,  OUT in_extension bool, OUT command pg_ddl_command );
DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_dropped_objects(OUT classid oid, OUT objid oid, OUT objsubid int4, OUT original bool, OUT normal bool, OUT is_temporary bool, OUT object_type text,  OUT schema_name text, OUT object_name text, OUT object_identity text, OUT address_names text[], OUT address_args text[] );
DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_table_rewrite_oid(out oid oid);
DROP FUNCTION IF EXISTS pg_catalog.pg_event_trigger_table_rewrite_reason();
DROP FUNCTION IF EXISTS pg_catalog.pg_identify_object(IN classid oid, IN objid oid, IN subobjid int4, OUT type text, OUT schema text, OUT name text, OUT identity text);
DROP FUNCTION IF EXISTS pg_catalog.pg_get_object_address(IN type text, IN name text[], IN args text[], OUT classid oid, OUT objid oid, OUT subobjid int4);

--rollback type
DROP FUNCTION IF EXISTS event_trigger_in(cstring) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'event_trigger' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS event_trigger_out(pg_catalog.event_trigger) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog.event_trigger;

DROP FUNCTION IF EXISTS pg_ddl_command_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_ddl_command_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'pg_ddl_command' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_ddl_command_out(pg_catalog.pg_ddl_command) CASCADE;
        DROP FUNCTION IF EXISTS pg_ddl_command_send(pg_catalog.pg_ddl_command) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog.pg_ddl_command;