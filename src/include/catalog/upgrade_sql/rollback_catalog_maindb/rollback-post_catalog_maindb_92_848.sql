DO $$
BEGIN
    if working_version_num() < 92507 then
        DROP INDEX IF EXISTS pg_catalog.pg_proc_proname_args_nsp_index;
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2691;
        CREATE UNIQUE INDEX pg_catalog.pg_proc_proname_args_nsp_index on pg_catalog.pg_proc USING BTREE(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops);
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
    elseif working_version_num() < 92609 or working_version_num() > 92655 then
        DROP INDEX IF EXISTS pg_catalog.pg_proc_proname_args_nsp_index;
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2691;
        CREATE INDEX pg_catalog.pg_proc_proname_args_nsp_index on pg_catalog.pg_proc USING BTREE(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops);
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
    end if;
END
$$;
