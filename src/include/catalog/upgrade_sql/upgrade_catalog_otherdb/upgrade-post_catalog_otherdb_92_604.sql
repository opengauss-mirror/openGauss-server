do $$
DECLARE ans boolean;
BEGIN
    select case when oid = 3807 then true else false end into ans from pg_type where typname = '_jsonb';
    if ans = false then
        DROP TYPE IF EXISTS pg_catalog.jsonb;
        DROP TYPE IF EXISTS pg_catalog._jsonb;
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3802, 3807, b;
        CREATE TYPE pg_catalog.jsonb;
        CREATE TYPE pg_catalog.jsonb (input=jsonb_in, output=jsonb_out, RECEIVE = jsonb_recv, SEND = jsonb_send, STORAGE=EXTENDED, category='C');
        COMMENT ON TYPE pg_catalog.jsonb IS 'json binary';
        COMMENT ON TYPE pg_catalog._jsonb IS 'json binary';
    end if;
END
$$;

DROP FUNCTION IF EXISTS pg_catalog.get_client_info;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7732;
CREATE OR REPLACE FUNCTION pg_catalog.get_client_info(OUT sid bigint, OUT client_info text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED SHIPPABLE ROWS 100
AS $function$get_client_info$function$;
comment on function PG_CATALOG.get_client_info() is 'read current client';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
