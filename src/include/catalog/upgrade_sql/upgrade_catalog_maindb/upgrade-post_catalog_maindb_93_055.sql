do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans from (select extname from pg_extension where extname='dolphin')
    LOOP
        if ans = false then
            DROP FUNCTION IF EXISTS pg_catalog.to_char(time, text) CASCADE;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9757;
            CREATE FUNCTION pg_catalog.to_char (time, text) RETURNS text LANGUAGE INTERNAL STABLE SHIPPABLE STRICT as 'time_to_char';
            COMMENT ON FUNCTION pg_catalog.to_char(time, text) is 'format time to text';
        end if;
        exit;
    END LOOP;
END$$;

DROP FUNCTION IF EXISTS pg_catalog.to_char(timetz, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9758;
CREATE FUNCTION pg_catalog.to_char (timetz, text) RETURNS text LANGUAGE INTERNAL STABLE SHIPPABLE STRICT as 'timetz_to_char';
COMMENT ON FUNCTION pg_catalog.to_char(timetz, text) is 'format timetz to text';
