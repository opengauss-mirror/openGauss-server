do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='xmltype' limit 1)
    LOOP
        if ans = true then
	    DROP FUNCTION IF EXISTS xmltype.createxml(xmldata varchar2);
        end if;
        exit;
    END LOOP;
END$$;
DROP FUNCTION IF EXISTS pg_catalog.xmltype(xmlvalue text);
DROP SCHEMA IF EXISTS xmltype cascade;
DROP DOMAIN IF EXISTS pg_catalog.xmltype;