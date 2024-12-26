DECLARE
con int;
BEGIN
    select count(*) from pg_proc where proname='json_object' and oid=3400 into con;
    if con = 1 then
        DROP FUNCTION IF EXISTS pg_catalog.json_object() CASCADE;
    end if;
END;
/