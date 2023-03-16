DO
$do$
    DECLARE
        index_exists boolean := true;
    BEGIN
        select case when count(*)=1 then true else false end from (select 1 from pg_class where oid = 2691 and relkind = 'i' and relname = 'pg_proc_proname_args_nsp_index') into index_exists;
        IF index_exists = false then
            IF working_version_num() < 92507 then
                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2691;
                CREATE UNIQUE INDEX pg_catalog.pg_proc_proname_args_nsp_index on pg_catalog.pg_proc USING BTREE(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops);
                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
            ELSE
                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2691;
                CREATE INDEX pg_catalog.pg_proc_proname_args_nsp_index on pg_catalog.pg_proc USING BTREE(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops);
                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
            END IF;
        END IF;
    END
$do$;