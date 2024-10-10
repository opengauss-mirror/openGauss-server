DROP FUNCTION IF EXISTS pg_catalog.xmltype;
DROP DOMAIN IF EXISTS pg_catalog.xmltype;
do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='xmltype' limit 1)
    LOOP
        if ans = true then
	    DROP FUNCTION IF EXISTS xmltype.createxml;
        end if;
        exit;
    END LOOP;
END$$;
DROP SCHEMA IF EXISTS xmltype cascade;

