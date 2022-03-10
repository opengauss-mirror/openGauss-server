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
