DO $$
BEGIN
    -- add sys function gs_is_recycle_obj
    if working_version_num() < 92607 or working_version_num() > 92655 then
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4896;
        CREATE FUNCTION pg_catalog.gs_is_recycle_obj(Oid, Oid, name, OUT output_result bool)
        RETURNS bool LANGUAGE INTERNAL as 'gs_is_recycle_obj';
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;
    end if;
END
$$;
