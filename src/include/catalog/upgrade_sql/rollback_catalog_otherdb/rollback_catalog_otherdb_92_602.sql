declare
    has_version_proc boolean;
    have_column boolean;
begin
    select case when count(*)=1 then true else false end as has_version_proc from (select * from pg_proc where proname = 'working_version_num' limit 1) into has_version_proc;
    if has_version_proc = true then
        select working_version_num >= 92458 as have_column from working_version_num() into have_column;
    end if;

    if have_column = false then
        DROP INDEX IF EXISTS pg_catalog.pg_proc_proname_all_args_nsp_index;
    else
        DROP INDEX IF EXISTS pg_catalog.pg_proc_proname_all_args_nsp_index;
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9666;
        CREATE UNIQUE INDEX pg_catalog.pg_proc_proname_all_args_nsp_index on pg_catalog.pg_proc USING BTREE(proname name_ops, allargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops);
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
        REINDEX INDEX pg_catalog.pg_proc_proname_all_args_nsp_index;
    end if;
end;
DROP INDEX IF EXISTS pg_catalog.gs_uid_relid_index;
DROP TYPE IF EXISTS pg_catalog.gs_uid;
DROP TABLE IF EXISTS pg_catalog.gs_uid;
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_wal_entrytable(int8);
DROP FUNCTION IF EXISTS pg_catalog.gs_walwriter_flush_position();
DROP FUNCTION IF EXISTS pg_catalog.gs_walwriter_flush_stat(int4);
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_undo();--drop system function has_any_privilege(user, privilege)
DROP FUNCTION IF EXISTS pg_catalog.has_any_privilege(name, text);

--drop system view gs_db_privileges
DROP VIEW IF EXISTS pg_catalog.gs_db_privileges;

--drop indexes on system relation gs_db_privilege
DROP INDEX IF EXISTS gs_db_privilege_oid_index;
DROP INDEX IF EXISTS gs_db_privilege_roleid_index;
DROP INDEX IF EXISTS gs_db_privilege_roleid_privilege_type_index;

--drop type gs_db_privilege
DROP TYPE IF EXISTS pg_catalog.gs_db_privilege;

--drop system relation gs_db_privilege
DROP TABLE IF EXISTS pg_catalog.gs_db_privilege;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_record(int8);
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta(int4, int4, int4);
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot(int4, int4);
DROP FUNCTION IF EXISTS pg_catalog.gs_index_verify(oid, oid);
DROP FUNCTION IF EXISTS pg_catalog.gs_index_recycle_queue(oid, oid, oid);DROP FUNCTION IF EXISTS pg_catalog.pg_logical_get_area_changes() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
do $$DECLARE
ans boolean;
func boolean;
user_name text;
query_str text;
BEGIN

    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        select case when count(*)=1 then true else false end as func from (select * from pg_proc where proname='local_double_write_stat' limit 1) into func;
        DROP FUNCTION IF EXISTS pg_catalog.local_double_write_stat();
        DROP FUNCTION IF EXISTS pg_catalog.remote_double_write_stat();
        DROP VIEW IF EXISTS DBE_PERF.global_double_write_status CASCADE;
        if func = true then
            SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4384;
            CREATE FUNCTION pg_catalog.local_double_write_stat
            (
				OUT node_name pg_catalog.text,
				OUT curr_dwn pg_catalog.int8,
				OUT curr_start_page pg_catalog.int8,
				OUT file_trunc_num pg_catalog.int8,
				OUT file_reset_num pg_catalog.int8,
				OUT total_writes pg_catalog.int8,
				OUT low_threshold_writes pg_catalog.int8,
				OUT high_threshold_writes pg_catalog.int8,
				OUT total_pages pg_catalog.int8,
				OUT low_threshold_pages pg_catalog.int8,
				OUT high_threshold_pages pg_catalog.int8
            ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_double_write_stat';

            SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4385;
            CREATE FUNCTION pg_catalog.remote_double_write_stat
            (
				OUT node_name pg_catalog.text,
				OUT curr_dwn pg_catalog.int8,
				OUT curr_start_page pg_catalog.int8,
				OUT file_trunc_num pg_catalog.int8,
				OUT file_reset_num pg_catalog.int8,
				OUT total_writes pg_catalog.int8,
				OUT low_threshold_writes pg_catalog.int8,
				OUT high_threshold_writes pg_catalog.int8,
				OUT total_pages pg_catalog.int8,
				OUT low_threshold_pages pg_catalog.int8,
				OUT high_threshold_pages pg_catalog.int8
            ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_double_write_stat';

            CREATE OR REPLACE VIEW dbe_perf.global_double_write_status AS
    			SELECT node_name, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
           		total_writes, low_threshold_writes, high_threshold_writes,
           		total_pages, low_threshold_pages, high_threshold_pages
    		FROM pg_catalog.local_double_write_stat();

            REVOKE ALL on DBE_PERF.global_double_write_status FROM PUBLIC;

            SELECT SESSION_USER INTO user_name;
            query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.global_double_write_status TO ' || quote_ident(user_name) || ';';
            EXECUTE IMMEDIATE query_str;

            GRANT SELECT ON TABLE DBE_PERF.global_double_write_status TO PUBLIC;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
        end if;
    end if;
END$$;

do $$DECLARE
ans boolean;
func boolean;
user_name text;
query_str text;
has_version_proc boolean;
no_file_id boolean;
BEGIN
    no_file_id = true;
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        select case when count(*)=1 then true else false end as func from (select * from pg_proc where proname='local_double_write_stat' limit 1) into func;
        select case when count(*)=1 then true else false end as has_version_proc from (select * from pg_proc where proname = 'working_version_num' limit 1) into has_version_proc;
        if has_version_proc = true  then
            select working_version_num < 92568 as no_file_id from working_version_num() into no_file_id;
        end if;

        DROP FUNCTION IF EXISTS pg_catalog.local_double_write_stat();
        DROP FUNCTION IF EXISTS pg_catalog.remote_double_write_stat();
        DROP VIEW IF EXISTS DBE_PERF.global_double_write_status CASCADE;
        if func = true then
            if no_file_id = true then
                SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4384;
                CREATE FUNCTION pg_catalog.local_double_write_stat
                (
                    OUT node_name pg_catalog.text,
                    OUT curr_dwn pg_catalog.int8,
                    OUT curr_start_page pg_catalog.int8,
                    OUT file_trunc_num pg_catalog.int8,
                    OUT file_reset_num pg_catalog.int8,
                    OUT total_writes pg_catalog.int8,
                    OUT low_threshold_writes pg_catalog.int8,
                    OUT high_threshold_writes pg_catalog.int8,
                    OUT total_pages pg_catalog.int8,
                    OUT low_threshold_pages pg_catalog.int8,
                    OUT high_threshold_pages pg_catalog.int8
                ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_double_write_stat';

                SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4385;
                CREATE FUNCTION pg_catalog.remote_double_write_stat
                (
                    OUT node_name pg_catalog.text,
                    OUT curr_dwn pg_catalog.int8,
                    OUT curr_start_page pg_catalog.int8,
                    OUT file_trunc_num pg_catalog.int8,
                    OUT file_reset_num pg_catalog.int8,
                    OUT total_writes pg_catalog.int8,
                    OUT low_threshold_writes pg_catalog.int8,
                    OUT high_threshold_writes pg_catalog.int8,
                    OUT total_pages pg_catalog.int8,
                    OUT low_threshold_pages pg_catalog.int8,
                    OUT high_threshold_pages pg_catalog.int8
                ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_double_write_stat';

                CREATE OR REPLACE VIEW dbe_perf.global_double_write_status AS
                    SELECT node_name, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
                    total_writes, low_threshold_writes, high_threshold_writes,
                    total_pages, low_threshold_pages, high_threshold_pages
                FROM pg_catalog.local_double_write_stat();
            else
                SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4384;
                CREATE FUNCTION pg_catalog.local_double_write_stat
                (
                    OUT node_name pg_catalog.text,
                    OUT file_id pg_catalog.int8,
                    OUT curr_dwn pg_catalog.int8,
                    OUT curr_start_page pg_catalog.int8,
                    OUT file_trunc_num pg_catalog.int8,
                    OUT file_reset_num pg_catalog.int8,
                    OUT total_writes pg_catalog.int8,
                    OUT low_threshold_writes pg_catalog.int8,
                    OUT high_threshold_writes pg_catalog.int8,
                    OUT total_pages pg_catalog.int8,
                    OUT low_threshold_pages pg_catalog.int8,
                    OUT high_threshold_pages pg_catalog.int8
                ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_double_write_stat';

                SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4385;
                CREATE FUNCTION pg_catalog.remote_double_write_stat
                (
                    OUT node_name pg_catalog.text,
                    OUT file_id pg_catalog.int8,
                    OUT curr_dwn pg_catalog.int8,
                    OUT curr_start_page pg_catalog.int8,
                    OUT file_trunc_num pg_catalog.int8,
                    OUT file_reset_num pg_catalog.int8,
                    OUT total_writes pg_catalog.int8,
                    OUT low_threshold_writes pg_catalog.int8,
                    OUT high_threshold_writes pg_catalog.int8,
                    OUT total_pages pg_catalog.int8,
                    OUT low_threshold_pages pg_catalog.int8,
                    OUT high_threshold_pages pg_catalog.int8
                ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_double_write_stat';

                CREATE OR REPLACE VIEW dbe_perf.global_double_write_status AS
                    SELECT node_name, file_id, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
                    total_writes, low_threshold_writes, high_threshold_writes,
                    total_pages, low_threshold_pages, high_threshold_pages
                FROM pg_catalog.local_double_write_stat();
            end if;

            REVOKE ALL on DBE_PERF.global_double_write_status FROM PUBLIC;
            SELECT SESSION_USER INTO user_name;
            query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.global_double_write_status TO ' || quote_ident(user_name) || ';';
            EXECUTE IMMEDIATE query_str;

            GRANT SELECT ON TABLE DBE_PERF.global_double_write_status TO PUBLIC;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
        end if;
    end if;
END$$;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_global_double_write_status' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_global_double_write_status
                DROP COLUMN IF EXISTS snap_file_id;
  end if;
END$DO$;SET search_path TO information_schema;

-- element_types is generated by data_type_privileges
DROP VIEW IF EXISTS information_schema.element_types CASCADE;

-- data_type_privileges is generated by columns
DROP VIEW IF EXISTS information_schema.data_type_privileges CASCADE;
-- data_type_privileges is generated by table_privileges
DROP VIEW IF EXISTS information_schema.role_column_grants CASCADE;
-- data_type_privileges is generated by column_privileges
DROP VIEW IF EXISTS information_schema.role_table_grants CASCADE;

-- other views need upgrade for matview
DROP VIEW IF EXISTS information_schema.column_domain_usage CASCADE;
DROP VIEW IF EXISTS information_schema.column_privileges CASCADE;
DROP VIEW IF EXISTS information_schema.column_udt_usage CASCADE;
DROP VIEW IF EXISTS information_schema.columns CASCADE;
DROP VIEW IF EXISTS information_schema.table_privileges CASCADE;
DROP VIEW IF EXISTS information_schema.tables CASCADE;
DROP VIEW IF EXISTS information_schema.view_column_usage CASCADE;
DROP VIEW IF EXISTS information_schema.view_table_usage CASCADE;

CREATE VIEW information_schema.column_domain_usage AS
    SELECT CAST(current_database() AS sql_identifier) AS domain_catalog,
           CAST(nt.nspname AS sql_identifier) AS domain_schema,
           CAST(t.typname AS sql_identifier) AS domain_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_type t, pg_namespace nt, pg_class c, pg_namespace nc,
         pg_attribute a

    WHERE t.typnamespace = nt.oid
          AND c.relnamespace = nc.oid
          AND a.attrelid = c.oid
          AND a.atttypid = t.oid
          AND t.typtype = 'd'
          AND c.relkind IN ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND pg_has_role(t.typowner, 'USAGE');

CREATE VIEW information_schema.column_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(x.relname AS sql_identifier) AS table_name,
           CAST(x.attname AS sql_identifier) AS column_name,
           CAST(x.prtype AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(x.grantee, x.relowner, 'USAGE')
                  OR x.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

    FROM (
           SELECT pr_c.grantor,
                  pr_c.grantee,
                  attname,
                  relname,
                  relnamespace,
                  pr_c.prtype,
                  pr_c.grantable,
                  pr_c.relowner
           FROM (SELECT oid, relname, relnamespace, relowner, (aclexplode(coalesce(relacl, acldefault('r', relowner)))).*
                 FROM pg_class
                 WHERE relkind IN ('r', 'm', 'v', 'f')
                ) pr_c (oid, relname, relnamespace, relowner, grantor, grantee, prtype, grantable),
                pg_attribute a
           WHERE a.attrelid = pr_c.oid
                 AND a.attnum > 0
                 AND NOT a.attisdropped
           UNION
           SELECT pr_a.grantor,
                  pr_a.grantee,
                  attname,
                  relname,
                  relnamespace,
                  pr_a.prtype,
                  pr_a.grantable,
                  c.relowner
           FROM (SELECT attrelid, attname, (aclexplode(coalesce(attacl, acldefault('c', relowner)))).*
                 FROM pg_attribute a JOIN pg_class cc ON (a.attrelid = cc.oid)
                 WHERE attnum > 0
                       AND NOT attisdropped
                ) pr_a (attrelid, attname, grantor, grantee, prtype, grantable),
                pg_class c
           WHERE pr_a.attrelid = c.oid
                 AND relkind IN ('r', 'm', 'v', 'f')
         ) x,
         pg_namespace nc,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE x.relnamespace = nc.oid
          AND x.grantee = grantee.oid
          AND x.grantor = u_grantor.oid
          AND x.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'REFERENCES', 'COMMENT')
          AND (x.relname not like 'mlog_%' AND x.relname not like 'matviewmap_%')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE VIEW information_schema.column_udt_usage AS
    SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
           CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_attribute a, pg_class c, pg_namespace nc,
         (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid))
           LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)

    WHERE a.attrelid = c.oid
          AND a.atttypid = t.oid
          AND nc.oid = c.relnamespace
          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND pg_has_role(coalesce(bt.typowner, t.typowner), 'USAGE');

CREATE VIEW information_schema.columns AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name,
           CAST(a.attnum AS cardinal_number) AS ordinal_position,
           CAST(pg_get_expr(ad.adbin, ad.adrelid) AS character_data) AS column_default,
           CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
             AS yes_or_no)
             AS is_nullable,

           CAST(
             CASE WHEN t.typtype = 'd' THEN
               CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                    WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, null)
                    ELSE 'USER-DEFINED' END
             ELSE
               CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                    WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                    ELSE 'USER-DEFINED' END
             END
             AS character_data)
             AS data_type,

           CAST(
             _pg_char_max_length(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS character_maximum_length,

           CAST(
             _pg_char_octet_length(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS character_octet_length,

           CAST(
             _pg_numeric_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_precision,

           CAST(
             _pg_numeric_precision_radix(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_precision_radix,

           CAST(
             _pg_numeric_scale(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_scale,

           CAST(
             _pg_datetime_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS datetime_precision,

           CAST(
             _pg_interval_type(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS character_data)
             AS interval_type,
           CAST(null AS cardinal_number) AS interval_precision,

           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,

           CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
           CAST(nco.nspname AS sql_identifier) AS collation_schema,
           CAST(co.collname AS sql_identifier) AS collation_name,

           CAST(CASE WHEN t.typtype = 'd' THEN current_database() ELSE null END
             AS sql_identifier) AS domain_catalog,
           CAST(CASE WHEN t.typtype = 'd' THEN nt.nspname ELSE null END
             AS sql_identifier) AS domain_schema,
           CAST(CASE WHEN t.typtype = 'd' THEN t.typname ELSE null END
             AS sql_identifier) AS domain_name,

           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
           CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,

           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,

           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST(a.attnum AS sql_identifier) AS dtd_identifier,
           CAST('NO' AS yes_or_no) AS is_self_referencing,

           CAST('NO' AS yes_or_no) AS is_identity,
           CAST(null AS character_data) AS identity_generation,
           CAST(null AS character_data) AS identity_start,
           CAST(null AS character_data) AS identity_increment,
           CAST(null AS character_data) AS identity_maximum,
           CAST(null AS character_data) AS identity_minimum,
           CAST(null AS yes_or_no) AS identity_cycle,

           CAST('NEVER' AS character_data) AS is_generated,
           CAST(null AS character_data) AS generation_expression,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind = 'v'
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '2' AND is_instead)
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '4' AND is_instead))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_updatable

    FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
         JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
         JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
         LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE (NOT pg_is_other_temp_schema(nc.oid))

          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')

          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')

          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_column_privilege(c.oid, a.attnum,
                                       'SELECT, INSERT, UPDATE, REFERENCES'));

CREATE VIEW information_schema.table_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(c.prtype AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(grantee.oid, c.relowner, 'USAGE')
                  OR c.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable,
           CAST(CASE WHEN c.prtype = 'SELECT' THEN 'YES' ELSE 'NO' END AS yes_or_no) AS with_hierarchy

    FROM (
            SELECT oid, relname, relnamespace, relkind, relowner, (aclexplode(coalesce(relacl, acldefault('r', relowner)))).* FROM pg_class
         ) AS c (oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
         pg_namespace nc,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE c.relnamespace = nc.oid
          AND c.relkind IN ('r', 'm', 'v')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND c.grantee = grantee.oid
          AND c.grantor = u_grantor.oid
          AND (c.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER')
               OR c.prtype IN ('ALTER', 'DROP', 'COMMENT', 'INDEX', 'VACUUM')
          )
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE VIEW information_schema.tables AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,

           CAST(
             CASE WHEN nc.oid = pg_my_temp_schema() THEN 'LOCAL TEMPORARY'
                  WHEN c.relkind = 'r' THEN 'BASE TABLE'
                  WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW'
                  WHEN c.relkind = 'v' THEN 'VIEW'
                  WHEN c.relkind = 'f' THEN 'FOREIGN TABLE'
                  ELSE null END
             AS character_data) AS table_type,

           CAST(null AS sql_identifier) AS self_referencing_column_name,
           CAST(null AS character_data) AS reference_generation,

           CAST(CASE WHEN t.typname IS NOT NULL THEN current_database() ELSE null END AS sql_identifier) AS user_defined_type_catalog,
           CAST(nt.nspname AS sql_identifier) AS user_defined_type_schema,
           CAST(t.typname AS sql_identifier) AS user_defined_type_name,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind = 'v'
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '3' AND is_instead))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_insertable_into,

           CAST(CASE WHEN t.typname IS NOT NULL THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_typed,
           CAST(null AS character_data) AS commit_action

    FROM pg_namespace nc JOIN pg_class c ON (nc.oid = c.relnamespace)
           LEFT JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON (c.reloftype = t.oid)

    WHERE c.relkind IN ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
          AND (NOT pg_is_other_temp_schema(nc.oid))
          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
               OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') );

CREATE VIEW information_schema.view_column_usage AS
    SELECT DISTINCT
           CAST(current_database() AS sql_identifier) AS view_catalog,
           CAST(nv.nspname AS sql_identifier) AS view_schema,
           CAST(v.relname AS sql_identifier) AS view_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nt.nspname AS sql_identifier) AS table_schema,
           CAST(t.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_namespace nv, pg_class v, pg_depend dv,
         pg_depend dt, pg_class t, pg_namespace nt,
         pg_attribute a

    WHERE nv.oid = v.relnamespace
          AND v.relkind = 'v'
          AND v.oid = dv.refobjid
          AND dv.refclassid = 'pg_catalog.pg_class'::regclass
          AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dv.deptype = 'i'
          AND dv.objid = dt.objid
          AND dv.refobjid <> dt.refobjid
          AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dt.refclassid = 'pg_catalog.pg_class'::regclass
          AND dt.refobjid = t.oid
          AND t.relnamespace = nt.oid
          AND t.relkind IN ('r', 'm', 'v', 'f')
          AND (t.relname not like 'mlog_%' AND t.relname not like 'matviewmap_%')
          AND t.oid = a.attrelid
          AND dt.refobjsubid = a.attnum
          AND pg_has_role(t.relowner, 'USAGE');

CREATE VIEW information_schema.view_table_usage AS
    SELECT DISTINCT
           CAST(current_database() AS sql_identifier) AS view_catalog,
           CAST(nv.nspname AS sql_identifier) AS view_schema,
           CAST(v.relname AS sql_identifier) AS view_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nt.nspname AS sql_identifier) AS table_schema,
           CAST(t.relname AS sql_identifier) AS table_name

    FROM pg_namespace nv, pg_class v, pg_depend dv,
         pg_depend dt, pg_class t, pg_namespace nt

    WHERE nv.oid = v.relnamespace
          AND v.relkind = 'v'
          AND v.oid = dv.refobjid
          AND dv.refclassid = 'pg_catalog.pg_class'::regclass
          AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dv.deptype = 'i'
          AND dv.objid = dt.objid
          AND dv.refobjid <> dt.refobjid
          AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dt.refclassid = 'pg_catalog.pg_class'::regclass
          AND dt.refobjid = t.oid
          AND t.relnamespace = nt.oid
          AND t.relkind IN ('r', 'm', 'v', 'f')
          AND (t.relname not like 'mlog_%' AND t.relname not like 'matviewmap_%')
          AND pg_has_role(t.relowner, 'USAGE');

CREATE VIEW information_schema.data_type_privileges AS
    SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(x.objschema AS sql_identifier) AS object_schema,
           CAST(x.objname AS sql_identifier) AS object_name,
           CAST(x.objtype AS character_data) AS object_type,
           CAST(x.objdtdid AS sql_identifier) AS dtd_identifier

    FROM
      (
        SELECT udt_schema, udt_name, 'USER-DEFINED TYPE'::text, dtd_identifier FROM attributes
        UNION ALL
        SELECT table_schema, table_name, 'TABLE'::text, dtd_identifier FROM columns
        UNION ALL
        SELECT domain_schema, domain_name, 'DOMAIN'::text, dtd_identifier FROM domains
        UNION ALL
        SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM parameters
        UNION ALL
        SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM routines
      ) AS x (objschema, objname, objtype, objdtdid);

CREATE VIEW information_schema.role_column_grants AS
    SELECT grantor,
           grantee,
           table_catalog,
           table_schema,
           table_name,
           column_name,
           privilege_type,
           is_grantable
    FROM column_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

CREATE VIEW information_schema.role_table_grants AS
    SELECT grantor,
           grantee,
           table_catalog,
           table_schema,
           table_name,
           privilege_type,
           is_grantable,
           with_hierarchy
    FROM table_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

CREATE VIEW information_schema.element_types AS
    SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(n.nspname AS sql_identifier) AS object_schema,
           CAST(x.objname AS sql_identifier) AS object_name,
           CAST(x.objtype AS character_data) AS object_type,
           CAST(x.objdtdid AS sql_identifier) AS collection_type_identifier,
           CAST(
             CASE WHEN nbt.nspname = 'pg_catalog' THEN format_type(bt.oid, null)
                  ELSE 'USER-DEFINED' END AS character_data) AS data_type,

           CAST(null AS cardinal_number) AS character_maximum_length,
           CAST(null AS cardinal_number) AS character_octet_length,
           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,
           CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
           CAST(nco.nspname AS sql_identifier) AS collation_schema,
           CAST(co.collname AS sql_identifier) AS collation_name,
           CAST(null AS cardinal_number) AS numeric_precision,
           CAST(null AS cardinal_number) AS numeric_precision_radix,
           CAST(null AS cardinal_number) AS numeric_scale,
           CAST(null AS cardinal_number) AS datetime_precision,
           CAST(null AS character_data) AS interval_type,
           CAST(null AS cardinal_number) AS interval_precision,

           CAST(null AS character_data) AS domain_default, -- XXX maybe a bug in the standard

           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(nbt.nspname AS sql_identifier) AS udt_schema,
           CAST(bt.typname AS sql_identifier) AS udt_name,

           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,

           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST('a' || CAST(x.objdtdid AS text) AS sql_identifier) AS dtd_identifier

    FROM pg_namespace n, pg_type at, pg_namespace nbt, pg_type bt,
         (
           /* columns, attributes */
           SELECT c.relnamespace, CAST(c.relname AS sql_identifier),
                  CASE WHEN c.relkind = 'c' THEN 'USER-DEFINED TYPE'::text ELSE 'TABLE'::text END,
                  a.attnum, a.atttypid, a.attcollation
           FROM pg_class c, pg_attribute a
           WHERE c.oid = a.attrelid
                 AND c.relkind IN ('r', 'm', 'v', 'f', 'c')
                 AND (c.relname not like 'mlog_%' AND c.relname not like 'matviewmap_%')
                 AND attnum > 0 AND NOT attisdropped

           UNION ALL

           /* domains */
           SELECT t.typnamespace, CAST(t.typname AS sql_identifier),
                  'DOMAIN'::text, 1, t.typbasetype, t.typcollation
           FROM pg_type t
           WHERE t.typtype = 'd'

           UNION ALL

           /* parameters */
           SELECT pronamespace, CAST(proname || '_' || CAST(oid AS text) AS sql_identifier),
                  'ROUTINE'::text, (ss.x).n, (ss.x).x, 0
           FROM (SELECT p.pronamespace, p.proname, p.oid,
                        _pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS x
                 FROM pg_proc p) AS ss

           UNION ALL

           /* result types */
           SELECT p.pronamespace, CAST(p.proname || '_' || CAST(p.oid AS text) AS sql_identifier),
                  'ROUTINE'::text, 0, p.prorettype, 0
           FROM pg_proc p

         ) AS x (objschema, objname, objtype, objdtdid, objtypeid, objcollation)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON x.objcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE n.oid = x.objschema
          AND at.oid = x.objtypeid
          AND (at.typelem <> 0 AND at.typlen = -1)
          AND at.typelem = bt.oid
          AND nbt.oid = bt.typnamespace

          AND (n.nspname, x.objname, x.objtype, CAST(x.objdtdid AS sql_identifier)) IN
              ( SELECT object_schema, object_name, object_type, dtd_identifier
                    FROM data_type_privileges );

do $$DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.element_types TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.data_type_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.role_column_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.role_table_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.column_domain_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.column_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.column_udt_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.columns TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.table_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.tables TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.view_column_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.view_table_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END$$;

GRANT SELECT ON information_schema.element_types TO PUBLIC;
GRANT SELECT ON information_schema.data_type_privileges TO PUBLIC;
GRANT SELECT ON information_schema.role_column_grants TO PUBLIC;
GRANT SELECT ON information_schema.role_table_grants TO PUBLIC;
GRANT SELECT ON information_schema.column_domain_usage TO PUBLIC;
GRANT SELECT ON information_schema.column_privileges TO PUBLIC;
GRANT SELECT ON information_schema.column_udt_usage TO PUBLIC;
GRANT SELECT ON information_schema.columns TO PUBLIC;
GRANT SELECT ON information_schema.table_privileges TO PUBLIC;
GRANT SELECT ON information_schema.tables TO PUBLIC;
GRANT SELECT ON information_schema.view_column_usage TO PUBLIC;
GRANT SELECT ON information_schema.view_table_usage TO PUBLIC;

RESET search_path;
