--step1 drop the opengauss_version()
DROP FUNCTION IF EXISTS PG_CATALOG.opengauss_version();

--step2 roll back sys function opengauss_version()
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_proc where proname = 'pgxc_version' and pronamespace = 11) into ans;
    if ans = false then
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 90;
        CREATE FUNCTION pg_catalog.pgxc_version() RETURNS text LANGUAGE INTERNAL as 'pgxc_version';
    end if;
END$$;
