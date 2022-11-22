DO $$
BEGIN
    if working_version_num() < 92607 or working_version_num() > 92655 then
        DROP FUNCTION IF EXISTS pg_catalog.gs_is_recycle_obj(Oid, Oid, name);
    end if;
END
$$;
