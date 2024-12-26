DECLARE
con int;
BEGIN
    select count(*) from pg_proc where proname='json_object' and oid=3400 into con;
    if con = 1 then
        DROP FUNCTION IF EXISTS pg_catalog.json_object() CASCADE;
    end if;
END;
/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3400;
CREATE FUNCTION pg_catalog.json_object()
RETURNS json
LANGUAGE INTERNAL
IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
as 'json_object';