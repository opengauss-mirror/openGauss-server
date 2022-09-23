REVOKE SELECT ON pg_catalog.pg_total_user_resource_info_oid FROM public;
REVOKE SELECT ON pg_catalog.pg_total_user_resource_info FROM public;
-- ----------------------------------------------------------------
-- rollback gs_paxos_stat_replication
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.gs_paxos_stat_replication(OUT local_role text, OUT peer_role text, OUT local_dcf_role text, OUT peer_dcf_role text, OUT peer_state text, OUT sender_write_location text, OUT sender_commit_location text, OUT sender_flush_location text, OUT sender_replay_location text, OUT receiver_write_location text, OUT receiver_commit_location text, OUT receiver_flush_location text, OUT receiver_replay_location text, OUT sync_percent text, OUT dcf_run_mode int4, OUT channel text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.connect_by_root(text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.sys_connect_by_path(text, text) cascade;
do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*) = 1 then true else false end as ans from (select typname from pg_type where typname = 'int16' limit 1)
    LOOP
        if ans = true then
            DROP OPERATOR IF EXISTS +  (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS -  (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS *  (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS /  (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS =  (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS <> (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS <  (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS <= (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS >  (int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS >= (int16, int16) CASCADE;

            DROP FUNCTION if EXISTS pg_catalog.int16eq(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16ne(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16lt(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16le(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16gt(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16ge(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16pl(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16mi(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16mul(int16, int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16div(int16, int16) CASCADE;

            DROP CAST IF EXISTS (tinyint AS int16) CASCADE;
            DROP CAST IF EXISTS (int16 AS tinyint) CASCADE;
            DROP CAST IF EXISTS (smallint AS int16) CASCADE;
            DROP CAST IF EXISTS (int16 as smallint) CASCADE;
            DROP CAST IF EXISTS (integer as int16) CASCADE;
            DROP CAST IF EXISTS (int16 as integer) CASCADE;
            DROP CAST IF EXISTS (bigint as int16) CASCADE;
            DROP CAST IF EXISTS (int16 as bigint) CASCADE;
            DROP CAST IF EXISTS (double precision as int16) CASCADE;
            DROP CAST IF EXISTS (int16 as double precision) CASCADE;
            DROP CAST IF EXISTS (real as int16) CASCADE;
            DROP CAST IF EXISTS (int16 as real) CASCADE;
            DROP CAST IF EXISTS (oid as int16) CASCADE;
            DROP CAST IF EXISTS (int16 as oid) CASCADE;
            DROP CAST IF EXISTS (boolean as int16) CASCADE;
            DROP CAST IF EXISTS (int16 as boolean) CASCADE;
            DROP CAST IF EXISTS (numeric as int16) CASCADE;
            DROP CAST IF EXISTS (int16 as numeric) CASCADE;

            DROP FUNCTION if exists pg_catalog.int16(tinyint) CASCADE;
            DROP FUNCTION if exists pg_catalog.i16toi1(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(smallint) CASCADE;
            DROP FUNCTION if exists pg_catalog.int2(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(integer) CASCADE;
            DROP FUNCTION if exists pg_catalog.int4(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(bigint) CASCADE;
            DROP FUNCTION if exists pg_catalog.int8(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(double precision) CASCADE;
            DROP FUNCTION if exists pg_catalog.float8(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(real) CASCADE;
            DROP FUNCTION if exists pg_catalog.float4(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(oid) CASCADE;
            DROP FUNCTION if exists pg_catalog.oid(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(boolean) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16_bool(int16) CASCADE;
            DROP FUNCTION if exists pg_catalog.int16(numeric) CASCADE;
            DROP FUNCTION if exists pg_catalog."numeric"(int16) CASCADE;

            DROP FUNCTION if EXISTS pg_catalog.int16in(cstring) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16out(int16) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16recv(internal) CASCADE;
            DROP FUNCTION if EXISTS pg_catalog.int16send(int16) CASCADE;

            DROP TYPE IF EXISTS pg_catalog.int16 CASCADE;
            DROP TYPE IF EXISTS pg_catalog._int16 CASCADE;
        end if;
        exit;
    END LOOP;
END$$;

DROP VIEW IF EXISTS pg_catalog.pg_gtt_attached_pids CASCADE;

DROP VIEW IF EXISTS pg_catalog.pg_seclabels CASCADE;

DROP VIEW IF EXISTS pg_catalog.pg_statio_sys_sequences;

DROP VIEW IF EXISTS pg_catalog.pg_statio_user_sequences;

DROP VIEW IF EXISTS pg_catalog.pg_statio_all_sequences;
do $$DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP VIEW IF EXISTS DBE_PERF.global_streaming_hadr_rto_and_rpo_stat CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_local_rto_and_rpo_stat();
        DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_remote_rto_and_rpo_stat();
    end if;
END$$;
DROP FUNCTION IF EXISTS pg_catalog.trunc(timestamp with time zone, text);

DROP FUNCTION IF EXISTS pg_catalog.trunc(interval, text);

DROP FUNCTION IF EXISTS pg_catalog.trunc(timestamp without time zone, text);

DROP FUNCTION IF EXISTS pg_catalog.replace(text, text);do $$DECLARE
ans boolean;
func boolean;
user_name text;
query_str text;
BEGIN

    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        select case when count(*)=1 then true else false end as func from (select * from pg_proc where proname='local_single_flush_dw_stat' limit 1) into func;
        DROP FUNCTION IF EXISTS pg_catalog.local_single_flush_dw_stat();
        DROP FUNCTION IF EXISTS pg_catalog.remote_single_flush_dw_stat();
	DROP VIEW IF EXISTS DBE_PERF.global_single_flush_dw_status CASCADE;
        if func = true then
            SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4375;
            CREATE FUNCTION pg_catalog.local_single_flush_dw_stat
            (
                OUT node_name pg_catalog.text,
                OUT curr_dwn pg_catalog.int4,
                OUT curr_start_page pg_catalog.int4,
                OUT total_writes pg_catalog.int8,
                OUT file_trunc_num pg_catalog.int8,
                OUT file_reset_num pg_catalog.int8
            ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_single_flush_dw_stat';

            SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4376;
            CREATE FUNCTION pg_catalog.remote_single_flush_dw_stat
            (  
                OUT node_name pg_catalog.text,
                OUT curr_dwn pg_catalog.int4,
                OUT curr_start_page pg_catalog.int4,
                OUT total_writes pg_catalog.int8,
                OUT file_trunc_num pg_catalog.int8,
                OUT file_reset_num pg_catalog.int8
            ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_single_flush_dw_stat';

            CREATE OR REPLACE VIEW DBE_PERF.global_single_flush_dw_status AS
                SELECT node_name, curr_dwn, curr_start_page, total_writes, file_trunc_num, file_reset_num
                FROM pg_catalog.local_single_flush_dw_stat();

            REVOKE ALL on DBE_PERF.global_single_flush_dw_status FROM PUBLIC;

            SELECT SESSION_USER INTO user_name;
            query_str := 'GRANT ALL ON TABLE DBE_PERF.global_single_flush_dw_status TO ' || quote_ident(user_name) || ';';
            EXECUTE IMMEDIATE query_str;

            GRANT SELECT ON TABLE DBE_PERF.global_single_flush_dw_status TO PUBLIC;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
        end if;	
    end if;
END$$;


-- ----------------------------------------------------------------
-- rollback array interface of pg_catalog
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.array_delete(anyarray) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_exists(anyarray, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_next(anyarray, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_prior(anyarray, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_extendnull(anyarray, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_streaming_dr_in_switchover() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_streaming_dr_get_switchover_barrier() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_streaming_dr_service_truncation_check() cascade;

do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_pldebugger' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS dbe_pldebugger.backtrace(OUT frameno integer, OUT funcname text, OUT lineno integer, OUT query text, OUT funcoid oid);
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1510;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.backtrace(OUT frameno integer, OUT funcname text, OUT lineno integer, OUT query text)
             RETURNS SETOF record
             LANGUAGE internal
             STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
            AS $function$debug_client_backtrace$function$;

            DROP FUNCTION IF EXISTS dbe_pldebugger.disable_breakpoint(integer);

            DROP FUNCTION IF EXISTS dbe_pldebugger.enable_breakpoint(integer);

            DROP FUNCTION IF EXISTS dbe_pldebugger.finish(OUT funcoid oid, OUT funcname text, OUT lineno integer, OUT query text);

            DROP FUNCTION IF EXISTS dbe_pldebugger.info_breakpoints(OUT breakpointno integer, OUT funcoid oid, OUT lineno integer, OUT query text, OUT enable boolean);
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1509;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.info_breakpoints(OUT breakpointno integer, OUT funcoid oid, OUT lineno integer, OUT query text)
             RETURNS SETOF record
             LANGUAGE internal
             STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
            AS $function$debug_client_info_breakpoints$function$;

            DROP FUNCTION IF EXISTS dbe_pldebugger.info_locals(frameno integer, OUT varname text, OUT vartype text, OUT value text, OUT package_name text, OUT isconst boolean);

            DROP FUNCTION IF EXISTS dbe_pldebugger.print_var(var_name text, frameno integer, OUT varname text, OUT vartype text, OUT value text, OUT package_name text, OUT isconst boolean);

            DROP FUNCTION IF EXISTS dbe_pldebugger.set_var(text, text);

            end if;
        exit;
    END LOOP;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP FUNCTION IF EXISTS pg_catalog.array_cat_distinct(anyarray, anyarray) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_intersect(anyarray, anyarray) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_intersect_distinct(anyarray, anyarray) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_except(anyarray, anyarray) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_except_distinct(anyarray, anyarray) cascade;do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*) = 1 then true else false end as ans from (select typname from pg_type where typname = 'int16' limit 1)
    LOOP
        if ans = true then
            DROP CAST IF EXISTS (numeric AS boolean) CASCADE;
            DROP CAST IF EXISTS (boolean AS numeric) CASCADE;
            DROP FUNCTION IF EXISTS pg_catalog.numeric_bool(numeric);
            DROP FUNCTION IF EXISTS pg_catalog.numeric(boolean);

            --drop views that reference pg_sequence_parameters
            DROP VIEW IF EXISTS information_schema.sequences;

            DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_parameters(sequence_oid oid, OUT start_value int16, OUT minimum_value int16, OUT maximum_value int16, OUT increment int16, OUT cycle_option boolean);
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3078;
            CREATE OR REPLACE FUNCTION pg_catalog.pg_sequence_parameters(sequence_oid oid, OUT start_value bigint, OUT minimum_value bigint, OUT maximum_value bigint, OUT increment bigint, OUT cycle_option boolean)
            RETURNS record
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE
            AS $function$pg_sequence_parameters$function$;

            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

            DROP VIEW IF EXISTS dbe_perf.statio_user_sequences;
            DROP VIEW IF EXISTS dbe_perf.statio_sys_sequences;
            DROP VIEW IF EXISTS dbe_perf.statio_all_sequences;
            
            SET search_path = information_schema;
            CREATE OR REPLACE VIEW information_schema.sequences AS
            SELECT CAST(current_database() AS sql_identifier) AS sequence_catalog,
                CAST(nc.nspname AS sql_identifier) AS sequence_schema,
                CAST(c.relname AS sql_identifier) AS sequence_name,
                CAST('bigint' AS character_data) AS data_type,
                CAST(64 AS cardinal_number) AS numeric_precision,
                CAST(2 AS cardinal_number) AS numeric_precision_radix,
                CAST(0 AS cardinal_number) AS numeric_scale,
                -- XXX: The following could be improved if we had LATERAL.
                CAST((pg_sequence_parameters(c.oid)).start_value AS character_data) AS start_value,
                CAST((pg_sequence_parameters(c.oid)).minimum_value AS character_data) AS minimum_value,
                CAST((pg_sequence_parameters(c.oid)).maximum_value AS character_data) AS maximum_value,
                CAST((pg_sequence_parameters(c.oid)).increment AS character_data) AS increment,
                CAST(CASE WHEN (pg_sequence_parameters(c.oid)).cycle_option THEN 'YES' ELSE 'NO' END AS yes_or_no) AS cycle_option
            FROM pg_namespace nc, pg_class c
            WHERE c.relnamespace = nc.oid
                AND c.relkind = 'S'
                AND (NOT pg_is_other_temp_schema(nc.oid))
                AND (pg_has_role(c.relowner, 'USAGE')
                    OR has_sequence_privilege(c.oid, 'SELECT, UPDATE, USAGE') );

            GRANT SELECT ON information_schema.sequences TO PUBLIC;

            CREATE OR REPLACE VIEW information_schema.usage_privileges AS
                /* collations */
                -- Collations have no real privileges, so we represent all collations with implicit usage privilege here.
                SELECT CAST(u.rolname AS sql_identifier) AS grantor,
                    CAST('PUBLIC' AS sql_identifier) AS grantee,
                    CAST(current_database() AS sql_identifier) AS object_catalog,
                    CAST(n.nspname AS sql_identifier) AS object_schema,
                    CAST(c.collname AS sql_identifier) AS object_name,
                    CAST('COLLATION' AS character_data) AS object_type,
                    CAST('USAGE' AS character_data) AS privilege_type,
                    CAST('NO' AS yes_or_no) AS is_grantable

                FROM pg_authid u,
                    pg_namespace n,
                    pg_collation c

                WHERE u.oid = c.collowner
                    AND c.collnamespace = n.oid
                    AND collencoding IN (-1, (SELECT encoding FROM pg_database WHERE datname = current_database()))

                UNION ALL

                /* domains */
                SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
                    CAST(grantee.rolname AS sql_identifier) AS grantee,
                    CAST(current_database() AS sql_identifier) AS object_catalog,
                    CAST(n.nspname AS sql_identifier) AS object_schema,
                    CAST(t.typname AS sql_identifier) AS object_name,
                    CAST('DOMAIN' AS character_data) AS object_type,
                    CAST('USAGE' AS character_data) AS privilege_type,
                    CAST(
                        CASE WHEN
                            -- object owner always has grant options
                            pg_has_role(grantee.oid, t.typowner, 'USAGE')
                            OR t.grantable
                            THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

                FROM (
                        SELECT oid, typname, typnamespace, typtype, typowner, (aclexplode(coalesce(typacl, acldefault('T', typowner)))).* FROM pg_type
                    ) AS t (oid, typname, typnamespace, typtype, typowner, grantor, grantee, prtype, grantable),
                    pg_namespace n,
                    pg_authid u_grantor,
                    (
                    SELECT oid, rolname FROM pg_authid
                    UNION ALL
                    SELECT 0::oid, 'PUBLIC'
                    ) AS grantee (oid, rolname)

                WHERE t.typnamespace = n.oid
                    AND t.typtype = 'd'
                    AND t.grantee = grantee.oid
                    AND t.grantor = u_grantor.oid
                    AND t.prtype IN ('USAGE')
                    AND (pg_has_role(u_grantor.oid, 'USAGE')
                        OR pg_has_role(grantee.oid, 'USAGE')
                        OR grantee.rolname = 'PUBLIC')

                UNION ALL

                /* foreign-data wrappers */
                SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
                    CAST(grantee.rolname AS sql_identifier) AS grantee,
                    CAST(current_database() AS sql_identifier) AS object_catalog,
                    CAST('' AS sql_identifier) AS object_schema,
                    CAST(fdw.fdwname AS sql_identifier) AS object_name,
                    CAST('FOREIGN DATA WRAPPER' AS character_data) AS object_type,
                    CAST('USAGE' AS character_data) AS privilege_type,
                    CAST(
                        CASE WHEN
                            -- object owner always has grant options
                            pg_has_role(grantee.oid, fdw.fdwowner, 'USAGE')
                            OR fdw.grantable
                            THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

                FROM (
                        SELECT fdwname, fdwowner, (aclexplode(coalesce(fdwacl, acldefault('F', fdwowner)))).* FROM pg_foreign_data_wrapper
                    ) AS fdw (fdwname, fdwowner, grantor, grantee, prtype, grantable),
                    pg_authid u_grantor,
                    (
                    SELECT oid, rolname FROM pg_authid
                    UNION ALL
                    SELECT 0::oid, 'PUBLIC'
                    ) AS grantee (oid, rolname)

                WHERE u_grantor.oid = fdw.grantor
                    AND grantee.oid = fdw.grantee
                    AND fdw.prtype IN ('USAGE')
                    AND (pg_has_role(u_grantor.oid, 'USAGE')
                        OR pg_has_role(grantee.oid, 'USAGE')
                        OR grantee.rolname = 'PUBLIC')

                UNION ALL

                /* foreign servers */
                SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
                    CAST(grantee.rolname AS sql_identifier) AS grantee,
                    CAST(current_database() AS sql_identifier) AS object_catalog,
                    CAST('' AS sql_identifier) AS object_schema,
                    CAST(srv.srvname AS sql_identifier) AS object_name,
                    CAST('FOREIGN SERVER' AS character_data) AS object_type,
                    CAST('USAGE' AS character_data) AS privilege_type,
                    CAST(
                        CASE WHEN
                            -- object owner always has grant options
                            pg_has_role(grantee.oid, srv.srvowner, 'USAGE')
                            OR srv.grantable
                            THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

                FROM (
                        SELECT srvname, srvowner, (aclexplode(coalesce(srvacl, acldefault('S', srvowner)))).* FROM pg_foreign_server
                    ) AS srv (srvname, srvowner, grantor, grantee, prtype, grantable),
                    pg_authid u_grantor,
                    (
                    SELECT oid, rolname FROM pg_authid
                    UNION ALL
                    SELECT 0::oid, 'PUBLIC'
                    ) AS grantee (oid, rolname)

                WHERE u_grantor.oid = srv.grantor
                    AND grantee.oid = srv.grantee
                    AND srv.prtype IN ('USAGE')
                    AND (pg_has_role(u_grantor.oid, 'USAGE')
                        OR pg_has_role(grantee.oid, 'USAGE')
                        OR grantee.rolname = 'PUBLIC')

                UNION ALL

                /* sequences */
                SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
                    CAST(grantee.rolname AS sql_identifier) AS grantee,
                    CAST(current_database() AS sql_identifier) AS object_catalog,
                    CAST(n.nspname AS sql_identifier) AS object_schema,
                    CAST(c.relname AS sql_identifier) AS object_name,
                    CAST('SEQUENCE' AS character_data) AS object_type,
                    CAST('USAGE' AS character_data) AS privilege_type,
                    CAST(
                        CASE WHEN
                            -- object owner always has grant options
                            pg_has_role(grantee.oid, c.relowner, 'USAGE')
                            OR c.grantable
                            THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

                FROM (
                        SELECT oid, relname, relnamespace, relkind, relowner, (aclexplode(coalesce(relacl, acldefault('r', relowner)))).* FROM pg_class
                    ) AS c (oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
                    pg_namespace n,
                    pg_authid u_grantor,
                    (
                    SELECT oid, rolname FROM pg_authid
                    UNION ALL
                    SELECT 0::oid, 'PUBLIC'
                    ) AS grantee (oid, rolname)

                WHERE c.relnamespace = n.oid
                    AND c.relkind = 'S'
                    AND c.grantee = grantee.oid
                    AND c.grantor = u_grantor.oid
                    AND c.prtype IN ('USAGE')
                    AND (pg_has_role(u_grantor.oid, 'USAGE')
                        OR pg_has_role(grantee.oid, 'USAGE')
                        OR grantee.rolname = 'PUBLIC');
            GRANT SELECT ON information_schema.usage_privileges TO PUBLIC;

        end if;
        exit;
    END LOOP;
END$$;
RESET search_path;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;-- ----------------------------------------------------------------
-- rollback array interface of pg_catalog
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.array_trim(anyarray, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.copy_summary_create() CASCADE;
do $$DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP VIEW IF EXISTS DBE_PERF.global_candidate_status CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.local_candidate_stat();
	DROP FUNCTION IF EXISTS pg_catalog.remote_candidate_stat();
    end if;
END$$;
-- Ammend sequence related function not properly updated
DROP FUNCTION if EXISTS pg_catalog.currval(regclass);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1575;
CREATE FUNCTION pg_catalog.currval(regclass)
 RETURNS bigint
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$currval_oid$function$;

DROP FUNCTION if EXISTS pg_catalog.lastval();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2559;
CREATE FUNCTION pg_catalog.lastval()
 RETURNS bigint
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$lastval$function$;

DROP FUNCTION IF EXISTS pg_catalog.nextval(regclass);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1574;
CREATE FUNCTION pg_catalog.nextval(regclass)
 RETURNS bigint
 LANGUAGE internal
 STRICT NOT FENCED SHIPPABLE
AS $function$nextval_oid$function$;

DROP FUNCTION IF EXISTS pg_catalog.setval(regclass, numeric);
DROP FUNCTION IF EXISTS pg_catalog.setval(regclass, bigint);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1576;
CREATE FUNCTION pg_catalog.setval(regclass, bigint)
 RETURNS bigint
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$setval_oid$function$;

DROP FUNCTION IF EXISTS pg_catalog.setval(regclass, numeric, boolean);
DROP FUNCTION IF EXISTS pg_catalog.setval(regclass, bigint, boolean);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1765;
CREATE FUNCTION pg_catalog.setval(regclass, bigint, boolean)
 RETURNS bigint
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$setval3_oid$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DO
$do$
DECLARE
v5r1c20_and_later_version boolean;
has_version_proc boolean;
need_upgrade boolean;
BEGIN
  need_upgrade = false;
  select case when count(*)=1 then true else false end as has_version_proc from (select * from pg_proc where proname = 'working_version_num' limit 1) into has_version_proc;
  IF has_version_proc = true  then
    select working_version_num >= 92305 as v5r1c20_and_later_version from working_version_num() into v5r1c20_and_later_version;
    IF v5r1c20_and_later_version = true then
      need_upgrade = true;
    end IF;
  END IF;
  IF need_upgrade = true then

comment on function PG_CATALOG.abbrev(inet) is '';
comment on function PG_CATALOG.abbrev(cidr) is '';
comment on function PG_CATALOG.abs(double precision) is '';
comment on function PG_CATALOG.abs(real) is '';
comment on function PG_CATALOG.abs(bigint) is '';
comment on function PG_CATALOG.abs(integer) is '';
comment on function PG_CATALOG.abs(smallint) is '';
comment on function PG_CATALOG.aclcontains(aclitem[], aclitem) is '';
comment on function PG_CATALOG.acldefault("char", oid) is '';
comment on function PG_CATALOG.aclexplode(acl aclitem[]) is '';
comment on function PG_CATALOG.aclinsert(aclitem[], aclitem) is '';
comment on function PG_CATALOG.aclitemin(cstring) is '';
comment on function PG_CATALOG.aclitemout(aclitem) is '';
comment on function PG_CATALOG.aclremove(aclitem[], aclitem) is '';
comment on function PG_CATALOG.acos(double precision) is '';
comment on function PG_CATALOG.age(xid) is '';
comment on function PG_CATALOG.age(timestamp without time zone) is '';
comment on function PG_CATALOG.age(timestamp with time zone) is '';
comment on function PG_CATALOG.age(timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.any_in(cstring) is '';
comment on function PG_CATALOG.any_out("any") is '';
comment on function PG_CATALOG.anyarray_in(cstring) is '';
comment on function PG_CATALOG.anyarray_out(anyarray) is '';
comment on function PG_CATALOG.anyarray_recv(internal) is '';
comment on function PG_CATALOG.anyarray_send(anyarray) is '';
comment on function PG_CATALOG.anyelement_in(cstring) is '';
comment on function PG_CATALOG.anyelement_out(anyelement) is '';
comment on function PG_CATALOG.anyenum_in(cstring) is '';
comment on function PG_CATALOG.anyenum_out(anyenum) is '';
comment on function PG_CATALOG.anynonarray_in(cstring) is '';
comment on function PG_CATALOG.anynonarray_out(anynonarray) is '';
comment on function PG_CATALOG.anyrange_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.anyrange_out(anyrange) is '';
comment on function PG_CATALOG.area(box) is '';
comment on function PG_CATALOG.area(path) is '';
comment on function PG_CATALOG.area(circle) is '';
comment on function PG_CATALOG.areajoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.areasel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.array_agg(anyelement) is '';
comment on function PG_CATALOG.array_agg_finalfn(internal) is '';
comment on function PG_CATALOG.array_agg_transfn(internal, anyelement) is '';
comment on function PG_CATALOG.array_dims(anyarray) is '';
comment on function PG_CATALOG.array_fill(anyelement, integer[]) is '';
comment on function PG_CATALOG.array_fill(anyelement, integer[], integer[]) is '';
comment on function PG_CATALOG.array_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.array_larger(anyarray, anyarray) is '';
comment on function PG_CATALOG.array_length(anyarray, integer) is '';
comment on function PG_CATALOG.array_lower(anyarray, integer) is '';
comment on function PG_CATALOG.array_ndims(anyarray) is '';
comment on function PG_CATALOG.array_out(anyarray) is '';
comment on function PG_CATALOG.array_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.array_send(anyarray) is '';
comment on function PG_CATALOG.array_smaller(anyarray, anyarray) is '';
comment on function PG_CATALOG.array_to_json(anyarray) is '';
comment on function PG_CATALOG.array_to_json(anyarray, boolean) is '';
comment on function PG_CATALOG.array_to_string(anyarray, text, text) is '';
comment on function PG_CATALOG.array_to_string(anyarray, text) is '';
comment on function PG_CATALOG.array_typanalyze(internal) is '';
comment on function PG_CATALOG.array_upper(anyarray, integer) is '';
comment on function PG_CATALOG.arraycontjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.arraycontsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.ascii(text) is '';
comment on function PG_CATALOG.asin(double precision) is '';
comment on function PG_CATALOG.atan(double precision) is '';
comment on function PG_CATALOG.atan2(double precision, double precision) is '';
comment on function PG_CATALOG.avg(bigint) is '';
comment on function PG_CATALOG.avg(double precision) is '';
comment on function PG_CATALOG.avg(integer) is '';
comment on function PG_CATALOG.avg(interval) is '';
comment on function PG_CATALOG.avg(numeric) is '';
comment on function PG_CATALOG.avg(real) is '';
comment on function PG_CATALOG.avg(smallint) is '';
comment on function PG_CATALOG.avg(tinyint) is '';
comment on function PG_CATALOG.bit(bigint, integer) is '';
comment on function PG_CATALOG.bit(bit, integer, boolean) is '';
comment on function PG_CATALOG.bit(integer, integer ) is '';
comment on function PG_CATALOG.bit_and(bigint) is '';
comment on function PG_CATALOG.bit_and(bit) is '';
comment on function PG_CATALOG.bit_and(integer) is '';
comment on function PG_CATALOG.bit_and(smallint) is '';
comment on function PG_CATALOG.bit_and(tinyint) is '';
comment on function PG_CATALOG.bit_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.bit_length(bit) is '';
comment on function PG_CATALOG.bit_length(bytea) is '';
comment on function PG_CATALOG.bit_length(text) is '';
comment on function PG_CATALOG.bit_or(bigint) is '';
comment on function PG_CATALOG.bit_or(bit) is '';
comment on function PG_CATALOG.bit_or(integer) is '';
comment on function PG_CATALOG.bit_or(smallint) is '';
comment on function PG_CATALOG.bit_or(tinyint) is '';
comment on function PG_CATALOG.bit_out(bit) is '';
comment on function PG_CATALOG.bit_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.bit_send(bit) is '';
comment on function PG_CATALOG.bitcmp(bit, bit) is '';
comment on function PG_CATALOG.bittypmodin(cstring[]) is '';
comment on function PG_CATALOG.bittypmodout(integer) is '';
comment on function PG_CATALOG.bool(integer) is '';
comment on function PG_CATALOG.bool_and(boolean) is '';
comment on function PG_CATALOG.bool_or(boolean) is '';
comment on function PG_CATALOG.booland_statefunc() is '';
comment on function PG_CATALOG.boolin(cstring) is '';
comment on function PG_CATALOG.boolor_statefunc(boolean, boolean) is '';
comment on function PG_CATALOG.boolout(boolean) is '';
comment on function PG_CATALOG.boolrecv(internal) is '';
comment on function PG_CATALOG.boolsend(boolean) is '';
comment on function PG_CATALOG.box(circle) is '';
comment on function PG_CATALOG.box(point, point) is '';
comment on function PG_CATALOG.box(polygon) is '';
comment on function PG_CATALOG.box_center(box) is '';
comment on function PG_CATALOG.box_in(cstring) is '';
comment on function PG_CATALOG.box_out(box) is '';
comment on function PG_CATALOG.box_recv(internal) is '';
comment on function PG_CATALOG.box_send(box) is '';
comment on function PG_CATALOG.bpchar(name) is '';
comment on function PG_CATALOG.bpchar(character, integer, boolean) is '';
comment on function PG_CATALOG.bpchar("char") is '';
comment on function PG_CATALOG.bpchar_larger(character, character) is '';
comment on function PG_CATALOG.bpchar_smaller(character, character) is '';
comment on function PG_CATALOG.bpchar_sortsupport(internal) is '';
comment on function PG_CATALOG.bpcharin(cstring, oid, integer) is '';
comment on function PG_CATALOG.bpcharlike(character, text) is '';
comment on function PG_CATALOG.bpcharnlike(character, text) is '';
comment on function PG_CATALOG.bpcharout(character) is '';
comment on function PG_CATALOG.bpcharrecv(internal, oid, integer) is '';
comment on function PG_CATALOG.bpcharsend(character) is '';
comment on function PG_CATALOG.bpchartypmodin(cstring[]) is '';
comment on function PG_CATALOG.bpchartypmodout(integer) is '';
comment on function PG_CATALOG.broadcast(inet) is '';
comment on function PG_CATALOG.btarraycmp(anyarray, anyarray) is '';
comment on function PG_CATALOG.btboolcmp(boolean, boolean) is '';
comment on function PG_CATALOG.btbpchar_pattern_cmp(character, character) is '';
comment on function PG_CATALOG.btcharcmp("char", "char") is '';
comment on function PG_CATALOG.btfloat48cmp(real, double precision) is '';
comment on function PG_CATALOG.btfloat4cmp(real, real) is '';
comment on function PG_CATALOG.btfloat4sortsupport(internal) is '';
comment on function PG_CATALOG.btfloat84cmp(double precision, real) is '';
comment on function PG_CATALOG.btfloat8cmp(double precision, double precision) is '';
comment on function PG_CATALOG.btfloat8sortsupport(internal) is '';
comment on function PG_CATALOG.btint24cmp(smallint, integer) is '';
comment on function PG_CATALOG.btint28cmp(smallint, bigint) is '';
comment on function PG_CATALOG.btint2cmp(smallint, smallint) is '';
comment on function PG_CATALOG.btint2sortsupport(internal) is '';
comment on function PG_CATALOG.btint42cmp(integer, smallint) is '';
comment on function PG_CATALOG.btint48cmp(integer, bigint) is '';
comment on function PG_CATALOG.btint4cmp(integer, integer) is '';
comment on function PG_CATALOG.btint4sortsupport(internal) is '';
comment on function PG_CATALOG.btint82cmp(bigint, smallint) is '';
comment on function PG_CATALOG.btint84cmp(bigint, integer) is '';
comment on function PG_CATALOG.btint8cmp(bigint, bigint) is '';
comment on function PG_CATALOG.btint8sortsupport(internal) is '';
comment on function PG_CATALOG.btnamecmp(name, name) is '';
comment on function PG_CATALOG.btnamesortsupport(internal) is '';
comment on function PG_CATALOG.btoidcmp(oid, oid) is '';
comment on function PG_CATALOG.btoidsortsupport(internal) is '';
comment on function PG_CATALOG.btoidvectorcmp(oidvector, oidvector) is '';
comment on function PG_CATALOG.btrecordcmp(record, record) is '';
comment on function PG_CATALOG.btrim(text, text) is '';
comment on function PG_CATALOG.btrim(text) is '';
comment on function PG_CATALOG.btrim(bytea, bytea) is '';
comment on function PG_CATALOG.bttext_pattern_cmp(text, text) is '';
comment on function PG_CATALOG.bttextcmp(text, text) is '';
comment on function PG_CATALOG.bttextsortsupport(internal) is '';
comment on function PG_CATALOG.bttidcmp(tid, tid) is '';
comment on function PG_CATALOG.bytea_sortsupport(internal) is '';
comment on function PG_CATALOG.bytea_string_agg_finalfn(internal) is '';
comment on function PG_CATALOG.bytea_string_agg_transfn(internal, bytea, bytea) is '';
comment on function PG_CATALOG.byteacmp(bytea, bytea) is '';
comment on function PG_CATALOG.byteain(cstring) is '';
comment on function PG_CATALOG.bytealike(bytea, bytea) is '';
comment on function PG_CATALOG.byteanlike(bytea, bytea) is '';
comment on function PG_CATALOG.byteaout(bytea) is '';
comment on function PG_CATALOG.cash_cmp(money, money) is '';
comment on function PG_CATALOG.cash_in(cstring) is '';
comment on function PG_CATALOG.cash_out(money) is '';
comment on function PG_CATALOG.cash_recv(internal) is '';
comment on function PG_CATALOG.cash_send(money) is '';
comment on function PG_CATALOG.cash_words(money) is '';
comment on function PG_CATALOG.cashlarger(money, money) is '';
comment on function PG_CATALOG.cashsmaller(money, money) is '';
comment on function PG_CATALOG.cbrt(double precision) is '';
comment on function PG_CATALOG.ceil(double precision) is '';
comment on function PG_CATALOG.ceil(numeric) is '';
comment on function PG_CATALOG.ceiling(double precision) is '';
comment on function PG_CATALOG.ceiling(numeric) is '';
comment on function PG_CATALOG.center(box) is '';
comment on function PG_CATALOG.center(circle) is '';
comment on function PG_CATALOG.char(text) is '';
comment on function PG_CATALOG.char_length(character) is '';
comment on function PG_CATALOG.char_length(text) is '';
comment on function PG_CATALOG.character_length(character) is '';
comment on function PG_CATALOG.character_length(text) is '';
comment on function PG_CATALOG.charin(cstring) is '';
comment on function PG_CATALOG.charout("char") is '';
comment on function PG_CATALOG.charrecv(internal) is '';
comment on function PG_CATALOG.charsend("char") is '';
comment on function PG_CATALOG.checksum(text) is '';
comment on function PG_CATALOG.chr(integer) is '';
comment on function PG_CATALOG.cidin(cstring) is '';
comment on function PG_CATALOG.cidout(cid) is '';
comment on function PG_CATALOG.cidr(inet) is '';
comment on function PG_CATALOG.cidr_in(cstring) is '';
comment on function PG_CATALOG.cidr_out(cidr) is '';
comment on function PG_CATALOG.cidr_recv(internal) is '';
comment on function PG_CATALOG.cidr_send(cidr) is '';
comment on function PG_CATALOG.cidrecv(internal) is '';
comment on function PG_CATALOG.cidsend(cid) is '';
comment on function PG_CATALOG.circle(point, double precision) is '';
comment on function PG_CATALOG.circle(polygon) is '';
comment on function PG_CATALOG.circle(box) is '';
comment on function PG_CATALOG.circle_center(circle) is '';
comment on function PG_CATALOG.circle_in(cstring) is '';
comment on function PG_CATALOG.circle_out(circle) is '';
comment on function PG_CATALOG.circle_recv(internal) is '';
comment on function PG_CATALOG.circle_send(circle) is '';
comment on function PG_CATALOG.clock_timestamp() is '';
comment on function PG_CATALOG.col_description(oid, integer) is '';
comment on function PG_CATALOG.concat(VARIADIC "any") is '';
comment on function PG_CATALOG.concat_ws(text, VARIADIC "any") is '';
comment on function PG_CATALOG.contjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.contsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.convert(bytea, name, name) is '';
comment on function PG_CATALOG.convert_from(bytea, name) is '';
comment on function PG_CATALOG.convert_to(text, name) is '';
comment on function PG_CATALOG.corr(double precision, double precision) is '';
comment on function PG_CATALOG.cos(double precision) is '';
comment on function PG_CATALOG.cot(double precision) is '';
comment on function PG_CATALOG.count() is '';
comment on function PG_CATALOG.count("any") is '';
comment on function PG_CATALOG.covar_pop(double precision, double precision) is '';
comment on function PG_CATALOG.covar_samp(double precision, double precision) is '';
comment on function PG_CATALOG.cstring_in(cstring) is '';
comment on function PG_CATALOG.cstring_out(cstring) is '';
comment on function PG_CATALOG.cstring_recv(internal) is '';
comment on function PG_CATALOG.cstring_send(cstring) is '';
comment on function PG_CATALOG.cume_dist() is '';
comment on function PG_CATALOG.current_database() is '';
comment on function PG_CATALOG.current_query() is '';
comment on function PG_CATALOG.current_schema() is '';
comment on function PG_CATALOG.current_schemas(boolean) is '';
comment on function PG_CATALOG.current_setting(text) is '';
comment on function PG_CATALOG.current_user() is '';
comment on function PG_CATALOG.currtid(oid, tid) is '';
comment on function PG_CATALOG.currtid2(text, tid) is '';
comment on function PG_CATALOG.currval(regclass) is '';
comment on function PG_CATALOG.cursor_to_xml(cursor refcursor, count integer, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.cursor_to_xmlschema(cursor refcursor, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.database_to_xml(nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.database_to_xmlschema(nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.date(timestamp with time zone) is '';
comment on function PG_CATALOG.date(timestamp without time zone) is '';
comment on function PG_CATALOG.date_in(cstring) is '';
comment on function PG_CATALOG.date_part(text, timestamp with time zone) is '';
comment on function PG_CATALOG.date_part(text, interval) is '';
comment on function PG_CATALOG.date_part(text, time with time zone) is '';
comment on function PG_CATALOG.date_part(text, date) is '';
comment on function PG_CATALOG.date_part(text, time without time zone) is '';
comment on function PG_CATALOG.date_part(text, timestamp without time zone) is '';
comment on function PG_CATALOG.date_recv(internal) is '';
comment on function PG_CATALOG.date_sortsupport(internal) is '';
comment on function PG_CATALOG.date_trunc(text, timestamp with time zone) is '';
comment on function PG_CATALOG.date_trunc(text, interval) is '';
comment on function PG_CATALOG.date_trunc(text, timestamp without time zone) is '';
comment on function PG_CATALOG.daterange_canonical(daterange) is '';
comment on function PG_CATALOG.decode(text, text) is '';
comment on function PG_CATALOG.degrees(double precision) is '';
comment on function PG_CATALOG.dense_rank() is '';
comment on function PG_CATALOG.dexp(double precision) is '';
comment on function PG_CATALOG.diagonal(box) is '';
comment on function PG_CATALOG.diameter(circle) is '';
comment on function PG_CATALOG.dispell_init(internal) is '';
comment on function PG_CATALOG.dispell_lexize(internal, internal, internal, internal) is '';
comment on function PG_CATALOG.div(numeric, numeric) is '';
comment on function PG_CATALOG.dlog1(double precision) is '';
comment on function PG_CATALOG.dlog10(double precision) is '';
comment on function PG_CATALOG.domain_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.domain_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.dpow(double precision, double precision) is '';
comment on function PG_CATALOG.dround(double precision) is '';
comment on function PG_CATALOG.dsimple_init(internal) is '';
comment on function PG_CATALOG.dsimple_lexize(internal, internal, internal, internal) is '';
comment on function PG_CATALOG.dsqrt(double precision) is '';
comment on function PG_CATALOG.dsynonym_init(internal) is '';
comment on function PG_CATALOG.dsynonym_lexize(internal, internal, internal, internal) is '';
comment on function PG_CATALOG.dtrunc(double precision) is '';
comment on function PG_CATALOG.encode(bytea, text) is '';
comment on function PG_CATALOG.enum_cmp(anyenum, anyenum) is '';
comment on function PG_CATALOG.enum_first(anyenum) is '';
comment on function PG_CATALOG.enum_in(cstring, oid) is '';
comment on function PG_CATALOG.enum_larger(anyenum, anyenum) is '';
comment on function PG_CATALOG.enum_last(anyenum) is '';
comment on function PG_CATALOG.enum_out(anyenum) is '';
comment on function PG_CATALOG.enum_range(anyenum, anyenum) is '';
comment on function PG_CATALOG.enum_range(anyenum) is '';
comment on function PG_CATALOG.enum_recv(cstring, oid) is '';
comment on function PG_CATALOG.enum_send(anyenum) is '';
comment on function PG_CATALOG.enum_smaller(anyenum, anyenum) is '';
comment on function PG_CATALOG.eqjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.eqsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.every(boolean) is '';
comment on function PG_CATALOG.exp(double precision) is '';
comment on function PG_CATALOG.exp(numeric) is '';
comment on function PG_CATALOG.factorial(bigint) is '';
comment on function PG_CATALOG.family(inet) is '';
comment on function PG_CATALOG.fdw_handler_in(cstring) is '';
comment on function PG_CATALOG.fdw_handler_out(fdw_handler) is '';
comment on function PG_CATALOG.first_value(anyelement) is '';
comment on function PG_CATALOG.float4(smallint) is '';
comment on function PG_CATALOG.float4(double precision) is '';
comment on function PG_CATALOG.float4(integer) is '';
comment on function PG_CATALOG.float4(bigint) is '';
comment on function PG_CATALOG.float4(numeric) is '';
comment on function PG_CATALOG.float4_accum(double precision[], real) is '';
comment on function PG_CATALOG.float4in(cstring) is '';
comment on function PG_CATALOG.float4larger(real, real) is '';
comment on function PG_CATALOG.float4out(real) is '';
comment on function PG_CATALOG.float4recv(internal) is '';
comment on function PG_CATALOG.float4smaller(real, real) is '';
comment on function PG_CATALOG.float4send(real) is '';
comment on function PG_CATALOG.float8(smallint) is '';
comment on function PG_CATALOG.float8(real) is '';
comment on function PG_CATALOG.float8(bigint) is '';
comment on function PG_CATALOG.float8(numeric) is '';
comment on function PG_CATALOG.float8_accum(double precision[], double precision) is '';
comment on function PG_CATALOG.float8_avg(double precision[]) is '';
comment on function PG_CATALOG.float8_corr(double precision[]) is '';
comment on function PG_CATALOG.float8_covar_pop(double precision[]) is '';
comment on function PG_CATALOG.float8_covar_samp(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_accum(double precision[], double precision, double precision) is '';
comment on function PG_CATALOG.float8_regr_avgx(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_avgy(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_intercept(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_r2(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_slope(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_sxx(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_sxy(double precision[]) is '';
comment on function PG_CATALOG.float8_regr_syy(double precision[]) is '';
comment on function PG_CATALOG.float8_stddev_pop(double precision[]) is '';
comment on function PG_CATALOG.float8_stddev_samp(double precision[]) is '';
comment on function PG_CATALOG.float8_var_pop(double precision[]) is '';
comment on function PG_CATALOG.float8_var_samp(double precision[]) is '';
comment on function PG_CATALOG.float8in(cstring) is '';
comment on function PG_CATALOG.float8larger(double precision, double precision) is '';
comment on function PG_CATALOG.float8out(double precision) is '';
comment on function PG_CATALOG.float8recv(internal) is '';
comment on function PG_CATALOG.float8send(double precision) is '';
comment on function PG_CATALOG.float8smaller(double precision, double precision) is '';
comment on function PG_CATALOG.floor(double precision) is '';
comment on function PG_CATALOG.floor(numeric) is '';
comment on function PG_CATALOG.fmgr_c_validator(oid) is '';
comment on function PG_CATALOG.fmgr_internal_validator(oid) is '';
comment on function PG_CATALOG.fmgr_sql_validator(oid) is '';
comment on function PG_CATALOG.format(text) is '';
comment on function PG_CATALOG.format(text, VARIADIC "any") is '';
comment on function PG_CATALOG.format_type(oid, integer) is '';
comment on function PG_CATALOG.generate_series(bigint, bigint) is '';
comment on function PG_CATALOG.generate_series(bigint, bigint, bigint) is '';
comment on function PG_CATALOG.generate_series(timestamp without time zone, timestamp without time zone, interval) is '';
comment on function PG_CATALOG.generate_series(integer, integer, integer) is '';
comment on function PG_CATALOG.generate_series(numeric, numeric) is '';
comment on function PG_CATALOG.generate_series(numeric, numeric, numeric) is '';
comment on function PG_CATALOG.generate_series(timestamp with time zone, timestamp with time zone, interval) is '';
comment on function PG_CATALOG.generate_subscripts(anyarray, integer) is '';
comment on function PG_CATALOG.generate_subscripts(anyarray, integer, boolean) is '';
comment on function PG_CATALOG.get_bit(bit, integer) is '';
comment on function PG_CATALOG.get_bit(bytea, integer) is '';
comment on function PG_CATALOG.get_byte(bytea, integer) is '';
comment on function PG_CATALOG.get_current_ts_config() is '';
comment on function PG_CATALOG.getdatabaseencoding() is '';
comment on function PG_CATALOG.gin_clean_pending_list(regclass) is '';
comment on function PG_CATALOG.gin_cmp_prefix(text, text, smallint, internal) is '';
comment on function PG_CATALOG.gin_cmp_tslexeme(text, text) is '';
comment on function PG_CATALOG.gin_extract_tsquery(tsquery, internal, smallint, internal, internal) is '';
comment on function PG_CATALOG.gin_extract_tsquery(tsquery, internal, smallint, internal, internal, internal, internal) is '';
comment on function PG_CATALOG.gin_extract_tsvector(tsvector, internal) is '';
comment on function PG_CATALOG.gin_extract_tsvector(tsvector, internal, internal) is '';
comment on function PG_CATALOG.gin_tsquery_consistent(internal, smallint, tsquery, integer, internal, internal) is '';
comment on function PG_CATALOG.gin_tsquery_consistent(internal, smallint, tsquery, integer, internal, internal, internal, internal) is '';
comment on function PG_CATALOG.gin_tsquery_triconsistent(internal, smallint, tsquery, integer, internal, internal, internal) is '';
comment on function PG_CATALOG.ginarrayconsistent(internal, smallint, anyarray, integer, internal, internal, internal, internal) is '';
comment on function PG_CATALOG.ginarrayextract(anyarray, internal, internal) is '';
comment on function PG_CATALOG.ginarrayextract(anyarray, internal) is '';
comment on function PG_CATALOG.ginarraytriconsistent(internal, smallint, anyarray, integer, internal, internal, internal) is '';
comment on function PG_CATALOG.ginqueryarrayextract(anyarray, internal, smallint, internal, internal, internal, internal) is '';
comment on function PG_CATALOG.gist_box_consistent(internal, box, integer, oid, internal) is '';
comment on function PG_CATALOG.gist_box_penalty(internal, internal, internal) is '';
comment on function PG_CATALOG.gist_box_picksplit(internal, internal) is '';
comment on function PG_CATALOG.gist_box_same(box, box, internal) is '';
comment on function PG_CATALOG.gist_box_union(internal, internal) is '';
comment on function PG_CATALOG.gist_circle_compress(internal) is '';
comment on function PG_CATALOG.gist_circle_consistent(internal, circle, integer, oid, internal) is '';
comment on function PG_CATALOG.gist_point_compress(internal) is '';
comment on function PG_CATALOG.gist_point_consistent(internal, point, integer, oid, internal) is '';
comment on function PG_CATALOG.gist_point_distance(internal, point, integer, oid) is '';
comment on function PG_CATALOG.gist_poly_compress(internal) is '';
comment on function PG_CATALOG.gist_poly_consistent(internal, polygon, integer, oid, internal) is '';
comment on function PG_CATALOG.gtsquery_compress(internal) is '';
comment on function PG_CATALOG.gtsquery_consistent(internal, internal, integer, oid, internal) is '';
comment on function PG_CATALOG.gtsquery_penalty(internal, internal, internal) is '';
comment on function PG_CATALOG.gtsquery_picksplit(internal, internal) is '';
comment on function PG_CATALOG.gtsquery_same(bigint, bigint, internal) is '';
comment on function PG_CATALOG.gtsquery_union(internal, internal) is '';
comment on function PG_CATALOG.gtsvector_compress(internal) is '';
comment on function PG_CATALOG.gtsvector_consistent(internal, gtsvector, integer, oid, internal) is '';
comment on function PG_CATALOG.gtsvector_decompress(internal) is '';
comment on function PG_CATALOG.gtsvector_penalty(internal, internal, internal) is '';
comment on function PG_CATALOG.gtsvector_picksplit(internal, internal) is '';
comment on function PG_CATALOG.gtsvector_same(gtsvector, gtsvector, internal) is '';
comment on function PG_CATALOG.gtsvector_union(internal, internal) is '';
comment on function PG_CATALOG.gtsvectorin(cstring) is '';
comment on function PG_CATALOG.gtsvectorout(gtsvector) is '';
comment on function PG_CATALOG.has_any_column_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_any_column_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_any_column_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_any_column_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_any_column_privilege(text, text) is '';
comment on function PG_CATALOG.has_any_column_privilege(oid, text) is '';
comment on function PG_CATALOG.has_column_privilege(name, text, text, text) is '';
comment on function PG_CATALOG.has_column_privilege(name, text, smallint, text) is '';
comment on function PG_CATALOG.has_column_privilege(name, oid, text, text) is '';
comment on function PG_CATALOG.has_column_privilege(name, oid, smallint, text) is '';
comment on function PG_CATALOG.has_column_privilege(oid, text, text, text) is '';
comment on function PG_CATALOG.has_column_privilege(oid, text, smallint, text) is '';
comment on function PG_CATALOG.has_column_privilege(oid, oid, text, text) is '';
comment on function PG_CATALOG.has_column_privilege(oid, oid, smallint, text) is '';
comment on function PG_CATALOG.has_column_privilege(text, text, text) is '';
comment on function PG_CATALOG.has_column_privilege(text, smallint, text) is '';
comment on function PG_CATALOG.has_column_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_column_privilege(oid, smallint, text) is '';
comment on function PG_CATALOG.has_database_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_database_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_database_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_database_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_database_privilege(text, text) is '';
comment on function PG_CATALOG.has_database_privilege(oid, text) is '';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(text, text) is '';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(oid, text) is '';
comment on function PG_CATALOG.has_function_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_function_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_function_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_function_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_function_privilege(text, text) is '';
comment on function PG_CATALOG.has_function_privilege(oid, text) is '';
comment on function PG_CATALOG.has_language_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_language_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_language_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_language_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_language_privilege(text, text) is '';
comment on function PG_CATALOG.has_language_privilege(oid, text) is '';
comment on function PG_CATALOG.has_schema_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_schema_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_schema_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_schema_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_schema_privilege(text, text) is '';
comment on function PG_CATALOG.has_schema_privilege(oid, text) is '';
comment on function PG_CATALOG.has_sequence_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_sequence_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_sequence_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_sequence_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_sequence_privilege(text, text) is '';
comment on function PG_CATALOG.has_sequence_privilege(oid, text) is '';
comment on function PG_CATALOG.has_server_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_server_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_server_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_server_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_server_privilege(text, text) is '';
comment on function PG_CATALOG.has_server_privilege(oid, text) is '';
comment on function PG_CATALOG.has_table_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_table_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_table_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_table_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_table_privilege(text, text) is '';
comment on function PG_CATALOG.has_table_privilege(oid, text) is '';
comment on function PG_CATALOG.has_tablespace_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_tablespace_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_tablespace_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_tablespace_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_tablespace_privilege(text, text) is '';
comment on function PG_CATALOG.has_tablespace_privilege(oid, text) is '';
comment on function PG_CATALOG.has_type_privilege(name, text, text) is '';
comment on function PG_CATALOG.has_type_privilege(name, oid, text) is '';
comment on function PG_CATALOG.has_type_privilege(oid, text, text) is '';
comment on function PG_CATALOG.has_type_privilege(oid, oid, text) is '';
comment on function PG_CATALOG.has_type_privilege(text, text) is '';
comment on function PG_CATALOG.has_type_privilege(oid, text) is '';
comment on function PG_CATALOG.hash_aclitem(aclitem) is '';
comment on function PG_CATALOG.hash_array(anyarray) is '';
comment on function PG_CATALOG.hash_numeric(numeric) is '';
comment on function PG_CATALOG.hash_range(anyrange) is '';
comment on function PG_CATALOG.hashbpchar(character) is '';
comment on function PG_CATALOG.hashenum(anyenum) is '';
comment on function PG_CATALOG.hashfloat4(real) is '';
comment on function PG_CATALOG.hashfloat8(double precision) is '';
comment on function PG_CATALOG.hashinet(inet) is '';
comment on function PG_CATALOG.hashint2(smallint) is '';
comment on function PG_CATALOG.hashint4(integer) is '';
comment on function PG_CATALOG.hashint8(bigint) is '';
comment on function PG_CATALOG.hashmacaddr(macaddr) is '';
comment on function PG_CATALOG.hashname(name) is '';
comment on function PG_CATALOG.hashoid(oid) is '';
comment on function PG_CATALOG.hashoidvector(oidvector) is '';
comment on function PG_CATALOG.hashtext(text) is '';
comment on function PG_CATALOG.hashvarlena(internal) is '';
comment on function PG_CATALOG.height(box) is '';
comment on function PG_CATALOG.hll_add_agg(hll_hashval) is '';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer) is '';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer, integer) is '';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer, integer, bigint) is '';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer, integer, bigint, integer) is '';
comment on function PG_CATALOG.hll_union_agg() is '';
comment on function PG_CATALOG.host(inet) is '';
comment on function PG_CATALOG.hostmask() is '';
comment on function PG_CATALOG.iclikejoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.iclikesel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.icnlikejoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.icnlikesel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.icregexeqjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.icregexeqsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.icregexnejoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.icregexnesel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.inet_client_addr() is '';
comment on function PG_CATALOG.inet_client_port() is '';
comment on function PG_CATALOG.inet_in(cstring) is '';
comment on function PG_CATALOG.inet_out(inet) is '';
comment on function PG_CATALOG.inet_recv(internal) is '';
comment on function PG_CATALOG.inet_send(inet) is '';
comment on function PG_CATALOG.inet_server_addr() is '';
comment on function PG_CATALOG.inet_server_port() is '';
comment on function PG_CATALOG.initcap(text) is '';
comment on function PG_CATALOG.int2(real) is '';
comment on function PG_CATALOG.int2(double precision) is '';
comment on function PG_CATALOG.int2(integer) is '';
comment on function PG_CATALOG.int2(bigint) is '';
comment on function PG_CATALOG.int2(numeric) is '';
comment on function PG_CATALOG.int2_accum(numeric[], smallint) is '';
comment on function PG_CATALOG.int2_avg_accum(bigint[], smallint) is '';
comment on function PG_CATALOG.int2_sum(bigint, smallint) is '';
comment on function PG_CATALOG.int2in(cstring) is '';
comment on function PG_CATALOG.int2larger(smallint, smallint) is '';
comment on function PG_CATALOG.int2mod(smallint, smallint) is '';
comment on function PG_CATALOG.int2out(smallint) is '';
comment on function PG_CATALOG.int2recv(internal) is '';
comment on function PG_CATALOG.int2send(smallint) is '';
comment on function PG_CATALOG.int2smaller(smallint, smallint) is '';
comment on function PG_CATALOG.int2vectorin(cstring) is '';
comment on function PG_CATALOG.int2vectorout(int2vector) is '';
comment on function PG_CATALOG.int2vectorrecv(internal) is '';
comment on function PG_CATALOG.int2vectorsend(int2vector) is '';
comment on function PG_CATALOG.int4("char") is '';
comment on function PG_CATALOG.int4(smallint) is '';
comment on function PG_CATALOG.int4(double precision) is '';
comment on function PG_CATALOG.int4(real) is '';
comment on function PG_CATALOG.int4(bigint) is '';
comment on function PG_CATALOG.int4(bit) is '';
comment on function PG_CATALOG.int4(numeric) is '';
comment on function PG_CATALOG.int4(boolean) is '';
comment on function PG_CATALOG.int4_accum(numeric[], integer) is '';
comment on function PG_CATALOG.int4_avg_accum(bigint[], integer) is '';
comment on function PG_CATALOG.int4_sum(bigint, integer) is '';
comment on function PG_CATALOG.int4in(cstring) is '';
comment on function PG_CATALOG.int4inc(integer) is '';
comment on function PG_CATALOG.int4larger(integer, integer) is '';
comment on function PG_CATALOG.int4mod(integer, integer) is '';
comment on function PG_CATALOG.int4out(integer) is '';
comment on function PG_CATALOG.int4range(integer, integer) is '';
comment on function PG_CATALOG.int4range(integer, integer, text) is '';
comment on function PG_CATALOG.int4range_canonical(int4range) is '';
comment on function PG_CATALOG.int4range_subdiff(integer, integer) is '';
comment on function PG_CATALOG.int4recv(internal) is '';
comment on function PG_CATALOG.int4send(integer) is '';
comment on function PG_CATALOG.int4smaller(integer, integer) is '';
comment on function PG_CATALOG.int8(integer) is '';
comment on function PG_CATALOG.int8(real) is '';
comment on function PG_CATALOG.int8(double precision) is '';
comment on function PG_CATALOG.int8(smallint) is '';
comment on function PG_CATALOG.int8(oid) is '';
comment on function PG_CATALOG.int8(numeric) is '';
comment on function PG_CATALOG.int8(bit) is '';
comment on function PG_CATALOG.int8_accum(numeric[], bigint) is '';
comment on function PG_CATALOG.int8_avg(bigint[]) is '';
comment on function PG_CATALOG.int8_avg_collect(bigint[], bigint[]) is '';
comment on function PG_CATALOG.int8_sum(numeric, bigint) is '';
comment on function PG_CATALOG.int8in(cstring) is '';
comment on function PG_CATALOG.int8inc(bigint) is '';
comment on function PG_CATALOG.int8inc_any(bigint, "any") is '';
comment on function PG_CATALOG.int8inc_float8_float8(bigint, double precision, double precision) is '';
comment on function PG_CATALOG.int8larger(bigint, bigint) is '';
comment on function PG_CATALOG.int8mod(bigint, bigint) is '';
comment on function PG_CATALOG.int8out(bigint) is '';
comment on function PG_CATALOG.int8range(bigint, bigint) is '';
comment on function PG_CATALOG.int8range(bigint, bigint, text) is '';
comment on function PG_CATALOG.int8range_canonical(int8range) is '';
comment on function PG_CATALOG.int8range_subdiff(bigint, bigint) is '';
comment on function PG_CATALOG.int8recv(internal) is '';
comment on function PG_CATALOG.int8send(bigint) is '';
comment on function PG_CATALOG.int8smaller(bigint, bigint) is '';
comment on function PG_CATALOG.internal_in(cstring) is '';
comment on function PG_CATALOG.internal_out(internal) is '';
comment on function PG_CATALOG.interval(interval, integer) is '';
comment on function PG_CATALOG.interval(time without time zone) is '';
comment on function PG_CATALOG.interval_accum(interval[], interval) is '';
comment on function PG_CATALOG.interval_avg(interval[]) is '';
comment on function PG_CATALOG.interval_cmp(interval, interval) is '';
comment on function PG_CATALOG.interval_hash(interval) is '';
comment on function PG_CATALOG.interval_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.interval_larger(interval, interval) is '';
comment on function PG_CATALOG.interval_out(interval) is '';
comment on function PG_CATALOG.interval_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.interval_send(interval) is '';
comment on function PG_CATALOG.interval_smaller(interval, interval) is '';
comment on function PG_CATALOG.intervaltypmodin(cstring[]) is '';
comment on function PG_CATALOG.intervaltypmodout(integer) is '';
comment on function PG_CATALOG.isclosed(path) is '';
comment on function PG_CATALOG.isempty(anyrange) is '';
comment on function PG_CATALOG.isfinite(date) is '';
comment on function PG_CATALOG.isfinite(timestamp with time zone) is '';
comment on function PG_CATALOG.isfinite(interval) is '';
comment on function PG_CATALOG.isfinite(timestamp without time zone) is '';
comment on function PG_CATALOG.isopen(path) is '';
comment on function PG_CATALOG.isvertical(lseg) is '';
comment on function PG_CATALOG.json_in(cstring) is '';
comment on function PG_CATALOG.json_out(json) is '';
comment on function PG_CATALOG.json_recv(internal) is '';
comment on function PG_CATALOG.json_send(json) is '';
comment on function PG_CATALOG.justify_days(interval) is '';
comment on function PG_CATALOG.justify_hours(interval) is '';
comment on function PG_CATALOG.justify_interval(interval) is '';
comment on function PG_CATALOG.lag(anyelement) is '';
comment on function PG_CATALOG.lag(anyelement, integer ) is '';
comment on function PG_CATALOG.lag(anyelement, integer, anyelement) is '';
comment on function PG_CATALOG.language_handler_in(cstring) is '';
comment on function PG_CATALOG.language_handler_out(language_handler) is '';
comment on function PG_CATALOG.last_value(anyelement) is '';
comment on function PG_CATALOG.lastval() is '';
comment on function PG_CATALOG.lead(anyelement) is '';
comment on function PG_CATALOG.lead(anyelement, integer) is '';
comment on function PG_CATALOG.lead(anyelement, integer, anyelement) is '';
comment on function PG_CATALOG.left(text, integer) is '';
comment on function PG_CATALOG.length(text) is '';
comment on function PG_CATALOG.length(character) is '';
comment on function PG_CATALOG.length(lseg) is '';
comment on function PG_CATALOG.length(path) is '';
comment on function PG_CATALOG.length(bit) is '';
comment on function PG_CATALOG.length(bytea, name) is '';
comment on function PG_CATALOG.length(bytea) is '';
comment on function PG_CATALOG.length(tsvector) is '';
comment on function PG_CATALOG.lengthb(text) is '';
comment on function PG_CATALOG.like(name, text) is '';
comment on function PG_CATALOG.like(text, text) is '';
comment on function PG_CATALOG.like_escape(bytea, bytea) is '';
comment on function PG_CATALOG.like_escape(text, text) is '';
comment on function PG_CATALOG.likejoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.likesel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.listagg(bigint) is '';
comment on function PG_CATALOG.listagg(bigint, text) is '';
comment on function PG_CATALOG.listagg(date) is '';
comment on function PG_CATALOG.listagg(date, text) is '';
comment on function PG_CATALOG.listagg(double precision) is '';
comment on function PG_CATALOG.listagg(double precision, text) is '';
comment on function PG_CATALOG.listagg(integer) is '';
comment on function PG_CATALOG.listagg(integer, text) is '';
comment on function PG_CATALOG.listagg(interval) is '';
comment on function PG_CATALOG.listagg(interval, text) is '';
comment on function PG_CATALOG.listagg(numeric) is '';
comment on function PG_CATALOG.listagg(numeric, text) is '';
comment on function PG_CATALOG.listagg(real) is '';
comment on function PG_CATALOG.listagg(real, text) is '';
comment on function PG_CATALOG.listagg(smallint) is '';
comment on function PG_CATALOG.listagg(smallint, text) is '';
comment on function PG_CATALOG.listagg(text) is '';
comment on function PG_CATALOG.listagg(text, text) is '';
comment on function PG_CATALOG.listagg(timestamp with time zone) is '';
comment on function PG_CATALOG.listagg(timestamp with time zone, text ) is '';
comment on function PG_CATALOG.listagg(timestamp without time zone) is '';
comment on function PG_CATALOG.listagg(timestamp without time zone, text) is '';
comment on function PG_CATALOG.ln(double precision) is '';
comment on function PG_CATALOG.ln(numeric) is '';
comment on function PG_CATALOG.log(double precision) is '';
comment on function PG_CATALOG.log(numeric, numeric) is '';
comment on function PG_CATALOG.log(numeric) is '';
comment on function PG_CATALOG.lower(text) is '';
comment on function PG_CATALOG.lower(anyrange) is '';
comment on function PG_CATALOG.lower_inc(anyrange) is '';
comment on function PG_CATALOG.lower_inf(anyrange) is '';
comment on function PG_CATALOG.lpad(text, integer) is '';
comment on function PG_CATALOG.lseg(point, point) is '';
comment on function PG_CATALOG.lseg(box) is '';
comment on function PG_CATALOG.lseg_center(lseg) is '';
comment on function PG_CATALOG.lseg_horizontal(lseg) is '';
comment on function PG_CATALOG.lseg_in(cstring) is '';
comment on function PG_CATALOG.lseg_length(lseg) is '';
comment on function PG_CATALOG.lseg_out(lseg) is '';
comment on function PG_CATALOG.lseg_perp(lseg, lseg) is '';
comment on function PG_CATALOG.lseg_recv(internal) is '';
comment on function PG_CATALOG.lseg_send(lseg) is '';
comment on function PG_CATALOG.lseg_vertical(lseg) is '';
comment on function PG_CATALOG.ltrim(text, text) is '';
comment on function PG_CATALOG.ltrim(text) is '';
comment on function PG_CATALOG.macaddr_cmp(macaddr, macaddr ) is '';
comment on function PG_CATALOG.macaddr_in(cstring) is '';
comment on function PG_CATALOG.macaddr_out(macaddr) is '';
comment on function PG_CATALOG.macaddr_recv(internal) is '';
comment on function PG_CATALOG.macaddr_send(macaddr) is '';
comment on function PG_CATALOG.makeaclitem(oid, oid, text, boolean) is '';
comment on function PG_CATALOG.masklen(inet) is '';
comment on function PG_CATALOG.max(abstime) is '';
comment on function PG_CATALOG.max(anyarray) is '';
comment on function PG_CATALOG.max(anyenum) is '';
comment on function PG_CATALOG.max(bigint) is '';
comment on function PG_CATALOG.max(character) is '';
comment on function PG_CATALOG.max(date) is '';
comment on function PG_CATALOG.max(double precision) is '';
comment on function PG_CATALOG.max(integer) is '';
comment on function PG_CATALOG.max(interval) is '';
comment on function PG_CATALOG.max(money) is '';
comment on function PG_CATALOG.max(numeric) is '';
comment on function PG_CATALOG.max(oid) is '';
comment on function PG_CATALOG.max(real) is '';
comment on function PG_CATALOG.max(smalldatetime) is '';
comment on function PG_CATALOG.max(smallint) is '';
comment on function PG_CATALOG.max(text) is '';
comment on function PG_CATALOG.max(tid) is '';
comment on function PG_CATALOG.max(time with time zone) is '';
comment on function PG_CATALOG.max(time without time zone) is '';
comment on function PG_CATALOG.max(timestamp with time zone) is '';
comment on function PG_CATALOG.max(timestamp without time zone) is '';
comment on function PG_CATALOG.max(tinyint) is '';
comment on function PG_CATALOG.md5(bytea) is '';
comment on function PG_CATALOG.md5(text) is '';
comment on function PG_CATALOG.median( double precision) is '';
comment on function PG_CATALOG.median(interval) is '';
comment on function PG_CATALOG.min(abstime) is '';
comment on function PG_CATALOG.min(anyarray) is '';
comment on function PG_CATALOG.min(anyenum) is '';
comment on function PG_CATALOG.min(bigint) is '';
comment on function PG_CATALOG.min(character) is '';
comment on function PG_CATALOG.min(date) is '';
comment on function PG_CATALOG.min(double precision) is '';
comment on function PG_CATALOG.min(integer) is '';
comment on function PG_CATALOG.min(interval) is '';
comment on function PG_CATALOG.min(money) is '';
comment on function PG_CATALOG.min(numeric) is '';
comment on function PG_CATALOG.min(oid) is '';
comment on function PG_CATALOG.min(real) is '';
comment on function PG_CATALOG.min(smalldatetime) is '';
comment on function PG_CATALOG.min(smallint) is '';
comment on function PG_CATALOG.min(text) is '';
comment on function PG_CATALOG.min(tid) is '';
comment on function PG_CATALOG.min(time with time zone) is '';
comment on function PG_CATALOG.min(time without time zone) is '';
comment on function PG_CATALOG.min(timestamp with time zone) is '';
comment on function PG_CATALOG.min(timestamp without time zone) is '';
comment on function PG_CATALOG.mod(numeric, numeric) is '';
comment on function PG_CATALOG.mode() is '';
comment on function PG_CATALOG.mode_final(internal) is '';
comment on function PG_CATALOG.money(integer) is '';
comment on function PG_CATALOG.money(bigint) is '';
comment on function PG_CATALOG.money(numeric) is '';
comment on function PG_CATALOG.name(character) is '';
comment on function PG_CATALOG.name(character varying) is '';
comment on function PG_CATALOG.name(text) is '';
comment on function PG_CATALOG.namein(cstring) is '';
comment on function PG_CATALOG.namelike(name, text) is '';
comment on function PG_CATALOG.namenlike(name, text) is '';
comment on function PG_CATALOG.nameout(name) is '';
comment on function PG_CATALOG.namerecv(internal) is '';
comment on function PG_CATALOG.namesend(name) is '';
comment on function PG_CATALOG.neqjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.neqsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.netmask(inet) is '';
comment on function PG_CATALOG.network(inet) is '';
comment on function PG_CATALOG.network_cmp(inet, inet) is '';
comment on function PG_CATALOG.nextval(regclass) is '';
comment on function PG_CATALOG.nlikejoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.nlikesel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.notlike(name, text) is '';
comment on function PG_CATALOG.now() is '';
comment on function PG_CATALOG.npoints(path) is '';
comment on function PG_CATALOG.npoints(polygon) is '';
comment on function PG_CATALOG.nth_value(anyelement, integer) is '';
comment on function PG_CATALOG.ntile(integer) is '';
comment on function PG_CATALOG.numeric(numeric, integer) is '';
comment on function PG_CATALOG.numeric(integer) is '';
comment on function PG_CATALOG.numeric(real) is '';
comment on function PG_CATALOG.numeric(double precision) is '';
comment on function PG_CATALOG.numeric(bigint) is '';
comment on function PG_CATALOG.numeric(smallint) is '';
comment on function PG_CATALOG.numeric(money) is '';
comment on function PG_CATALOG.numeric_abs(numeric) is '';
comment on function PG_CATALOG.numeric_accum(numeric[], numeric) is '';
comment on function PG_CATALOG.numeric_avg(numeric[]) is '';
comment on function PG_CATALOG.numeric_avg_accum(numeric[], numeric) is '';
comment on function PG_CATALOG.numeric_cmp(numeric, numeric) is '';
comment on function PG_CATALOG.numeric_div_trunc(numeric, numeric) is '';
comment on function PG_CATALOG.numeric_exp(numeric) is '';
comment on function PG_CATALOG.numeric_fac(bigint) is '';
comment on function PG_CATALOG.numeric_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.numeric_inc(numeric) is '';
comment on function PG_CATALOG.numeric_larger(numeric, numeric) is '';
comment on function PG_CATALOG.numeric_ln(numeric) is '';
comment on function PG_CATALOG.numeric_log(numeric, numeric) is '';
comment on function PG_CATALOG.numeric_mod(numeric, numeric) is '';
comment on function PG_CATALOG.numeric_out(numeric) is '';
comment on function PG_CATALOG.numeric_power(numeric, numeric) is '';
comment on function PG_CATALOG.numeric_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.numeric_send(numeric) is '';
comment on function PG_CATALOG.numeric_smaller(numeric, numeric) is '';
comment on function PG_CATALOG.numeric_sortsupport(internal) is '';
comment on function PG_CATALOG.numeric_sqrt(numeric) is '';
comment on function PG_CATALOG.numeric_stddev_pop(numeric[]) is '';
comment on function PG_CATALOG.numeric_stddev_samp(numeric[]) is '';
comment on function PG_CATALOG.numeric_var_pop(numeric[]) is '';
comment on function PG_CATALOG.numeric_var_samp(numeric[]) is '';
comment on function PG_CATALOG.numerictypmodin(cstring[]) is '';
comment on function PG_CATALOG.numerictypmodout(integer) is '';
comment on function PG_CATALOG.numnode(tsquery) is '';
comment on function PG_CATALOG.numrange(numeric, numeric) is '';
comment on function PG_CATALOG.numrange(numeric, numeric, text) is '';
comment on function PG_CATALOG.numrange_subdiff(numeric, numeric) is '';
comment on function PG_CATALOG.obj_description(oid) is '';
comment on function PG_CATALOG.obj_description(oid, name) is '';
comment on function PG_CATALOG.octet_length(bit) is '';
comment on function PG_CATALOG.octet_length(bytea) is '';
comment on function PG_CATALOG.octet_length(character) is '';
comment on function PG_CATALOG.oid(bigint) is '';
comment on function PG_CATALOG.oidin(cstring) is '';
comment on function PG_CATALOG.oidlarger(oid, oid) is '';
comment on function PG_CATALOG.oidout(oid) is '';
comment on function PG_CATALOG.oidrecv(internal) is '';
comment on function PG_CATALOG.oidsend(oid) is '';
comment on function PG_CATALOG.oidsmaller(oid, oid) is '';
comment on function PG_CATALOG.oidvectorin(cstring) is '';
comment on function PG_CATALOG.oidvectorout(oidvector) is '';
comment on function PG_CATALOG.oidvectorrecv(internal) is '';
comment on function PG_CATALOG.oidvectorsend(oidvector) is '';
comment on function PG_CATALOG.oidvectortypes(oidvector) is '';
comment on function PG_CATALOG.ordered_set_transition(internal, "any") is '';
comment on function PG_CATALOG.overlaps(time with time zone, time with time zone, time with time zone, time with time zone) is '';
comment on function PG_CATALOG.overlaps(time without time zone, interval, time without time zone, interval) is '';
comment on function PG_CATALOG.overlaps(time without time zone, interval, time without time zone, time without time zone) is '';
comment on function PG_CATALOG.overlaps(time without time zone, time without time zone, time without time zone, interval) is '';
comment on function PG_CATALOG.overlaps(time without time zone, time without time zone, time without time zone, time without time zone) is '';
comment on function PG_CATALOG.overlaps(timestamp with time zone, interval, timestamp with time zone, interval) is '';
comment on function PG_CATALOG.overlaps(timestamp with time zone, interval, timestamp with time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, interval) is '';
comment on function PG_CATALOG.overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.overlaps(timestamp without time zone, interval, timestamp without time zone, interval) is '';
comment on function PG_CATALOG.overlaps(timestamp without time zone, interval, timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, interval) is '';
comment on function PG_CATALOG.overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.overlay(bytea, bytea, integer, integer) is '';
comment on function PG_CATALOG.overlay(bytea, bytea, integer) is '';
comment on function PG_CATALOG.overlay(text, text, integer, integer) is '';
comment on function PG_CATALOG.overlay(text, text, integer) is '';
comment on function PG_CATALOG.overlay(bit, bit, integer, integer) is '';
comment on function PG_CATALOG.overlay(bit, bit, integer) is '';
comment on function PG_CATALOG.path(polygon) is '';
comment on function PG_CATALOG.path_center(path) is '';
comment on function PG_CATALOG.path_in(cstring) is '';
comment on function PG_CATALOG.path_length(path) is '';
comment on function PG_CATALOG.path_npoints(path) is '';
comment on function PG_CATALOG.path_out(path) is '';
comment on function PG_CATALOG.path_recv(internal) is '';
comment on function PG_CATALOG.path_send(path) is '';
comment on function PG_CATALOG.pclose(path) is '';
comment on function PG_CATALOG.percent_rank() is '';
comment on function PG_CATALOG.percentile_cont(double precision, double precision) is '';
comment on function PG_CATALOG.percentile_cont(double precision, interval) is '';
comment on function PG_CATALOG.percentile_cont_float8_final(internal, double precision, double precision) is '';
comment on function PG_CATALOG.percentile_cont_interval_final(internal, double precision, interval) is '';
comment on function PG_CATALOG.pg_advisory_lock(bigint) is '';
comment on function PG_CATALOG.pg_advisory_lock(integer, integer) is '';
comment on function PG_CATALOG.pg_advisory_lock_shared(bigint) is '';
comment on function PG_CATALOG.pg_advisory_lock_shared(integer, integer) is '';
comment on function PG_CATALOG.pg_advisory_unlock(bigint) is '';
comment on function PG_CATALOG.pg_advisory_unlock(integer, integer) is '';
comment on function PG_CATALOG.pg_advisory_unlock_all() is '';
comment on function PG_CATALOG.pg_advisory_unlock_shared(bigint) is '';
comment on function PG_CATALOG.pg_advisory_unlock_shared(integer, integer ) is '';
comment on function PG_CATALOG.pg_advisory_xact_lock(bigint) is '';
comment on function PG_CATALOG.pg_advisory_xact_lock(integer, integer) is '';
comment on function PG_CATALOG.pg_advisory_xact_lock_shared(bigint) is '';
comment on function PG_CATALOG.pg_advisory_xact_lock_shared(integer, integer) is '';
comment on function PG_CATALOG.pg_available_extension_versions() is '';
comment on function PG_CATALOG.pg_available_extensions() is '';
comment on function PG_CATALOG.pg_backend_pid() is '';
comment on function PG_CATALOG.pg_cancel_backend(bigint) is '';
comment on function PG_CATALOG.pg_char_to_encoding(name) is '';
comment on function PG_CATALOG.pg_client_encoding() is '';
comment on function PG_CATALOG.pg_collation_for("any") is '';
comment on function PG_CATALOG.pg_collation_is_visible(oid) is '';
comment on function PG_CATALOG.pg_column_size("any") is '';
comment on function PG_CATALOG.pg_conf_load_time() is '';
comment on function PG_CATALOG.pg_control_checkpoint() is '';
comment on function PG_CATALOG.pg_control_system() is '';
comment on function PG_CATALOG.pg_conversion_is_visible(oid) is '';
comment on function PG_CATALOG.pg_create_logical_replication_slot(slotname name, plugin name) is '';
comment on function PG_CATALOG.pg_create_physical_replication_slot(slotname name, dummy_standby boolean) is '';
comment on function PG_CATALOG.pg_create_restore_point(text) is '';
comment on function PG_CATALOG.pg_cursor() is '';
comment on function PG_CATALOG.pg_database_size(name) is '';
comment on function PG_CATALOG.pg_database_size(oid) is '';
comment on function PG_CATALOG.pg_describe_object(oid, oid, integer) is '';
comment on function PG_CATALOG.pg_drop_replication_slot(name) is '';
comment on function PG_CATALOG.pg_encoding_max_length(integer) is '';
comment on function PG_CATALOG.pg_encoding_to_char(integer) is '';
comment on function PG_CATALOG.pg_export_snapshot() is '';
comment on function PG_CATALOG.pg_extension_config_dump(regclass, text) is '';
comment on function PG_CATALOG.pg_extension_update_paths(name name) is '';
comment on function PG_CATALOG.pg_filenode_relation(oid, oid) is '';
comment on function PG_CATALOG.pg_function_is_visible(oid) is '';
comment on function PG_CATALOG.pg_get_constraintdef(oid) is '';
comment on function PG_CATALOG.pg_get_constraintdef(oid, boolean) is '';
comment on function PG_CATALOG.pg_get_function_arguments(oid) is '';
comment on function PG_CATALOG.pg_get_function_identity_arguments(oid) is '';
comment on function PG_CATALOG.pg_get_function_result(oid) is '';
comment on function PG_CATALOG.pg_get_functiondef(funcid oid) is '';
comment on function PG_CATALOG.pg_get_indexdef(oid) is '';
comment on function PG_CATALOG.pg_get_indexdef(oid, integer, boolean) is '';
comment on function PG_CATALOG.pg_get_keywords() is '';
comment on function PG_CATALOG.pg_get_replication_slots() is '';
comment on function PG_CATALOG.pg_get_ruledef(oid) is '';
comment on function PG_CATALOG.pg_get_ruledef(oid, boolean) is '';
comment on function PG_CATALOG.pg_get_serial_sequence(text, text) is '';
comment on function PG_CATALOG.pg_get_triggerdef(oid) is '';
comment on function PG_CATALOG.pg_get_triggerdef(oid, boolean) is '';
comment on function PG_CATALOG.pg_get_userbyid(oid) is '';
comment on function PG_CATALOG.pg_get_viewdef(text) is '';
comment on function PG_CATALOG.pg_get_viewdef(oid) is '';
comment on function PG_CATALOG.pg_get_viewdef(text, boolean) is '';
comment on function PG_CATALOG.pg_get_viewdef(oid, boolean) is '';
comment on function PG_CATALOG.pg_get_viewdef(oid, integer) is '';
comment on function PG_CATALOG.pg_has_role(name, name, text) is '';
comment on function PG_CATALOG.pg_has_role(name, oid, text) is '';
comment on function PG_CATALOG.pg_has_role(oid, name, text) is '';
comment on function PG_CATALOG.pg_has_role(oid, oid, text) is '';
comment on function PG_CATALOG.pg_has_role(name, text) is '';
comment on function PG_CATALOG.pg_has_role(oid, text) is '';
comment on function PG_CATALOG.pg_indexes_size(regclass) is '';
comment on function PG_CATALOG.pg_is_in_recovery() is '';
comment on function PG_CATALOG.pg_is_other_temp_schema(oid) is '';
comment on function PG_CATALOG.pg_last_xact_replay_timestamp() is '';
comment on function PG_CATALOG.pg_listening_channels() is '';
comment on function PG_CATALOG.pg_lock_status() is '';
comment on function PG_CATALOG.pg_logical_slot_get_binary_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is '';
comment on function PG_CATALOG.pg_logical_slot_get_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is '';
comment on function PG_CATALOG.pg_logical_slot_peek_binary_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is '';
comment on function PG_CATALOG.pg_logical_slot_peek_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is '';
comment on function PG_CATALOG.pg_ls_dir(text) is '';
comment on function PG_CATALOG.pg_my_temp_schema() is '';
comment on function PG_CATALOG.pg_notify(text, text) is '';
comment on function PG_CATALOG.pg_opclass_is_visible(oid) is '';
comment on function PG_CATALOG.pg_operator_is_visible(oid) is '';
comment on function PG_CATALOG.pg_opfamily_is_visible(oid) is '';
comment on function PG_CATALOG.pg_options_to_table(options_array text[]) is '';
comment on function PG_CATALOG.pg_postmaster_start_time() is '';
comment on function PG_CATALOG.pg_prepared_statement() is '';
comment on function PG_CATALOG.pg_prepared_xact() is '';
comment on function PG_CATALOG.pg_read_binary_file(text) is '';
comment on function PG_CATALOG.pg_read_binary_file(text, bigint, bigint, boolean) is '';
comment on function PG_CATALOG.pg_read_file(text, bigint, bigint) is '';
comment on function PG_CATALOG.pg_read_file(text) is '';
comment on function PG_CATALOG.pg_relation_filenode(regclass) is '';
comment on function PG_CATALOG.pg_relation_filepath(regclass) is '';
comment on function PG_CATALOG.pg_relation_size(regclass, text) is '';
comment on function PG_CATALOG.pg_reload_conf() is '';
comment on function PG_CATALOG.pg_replication_slot_advance(slot_name name, upto_lsn text) is '';
comment on function PG_CATALOG.pg_rotate_logfile() is '';
comment on function PG_CATALOG.pg_sequence_parameters(sequence_oid oid) is '';
comment on function PG_CATALOG.pg_show_all_settings() is '';
comment on function PG_CATALOG.pg_size_pretty(bigint) is '';
comment on function PG_CATALOG.pg_size_pretty(numeric) is '';
comment on function PG_CATALOG.pg_sleep(double precision) is '';
comment on function PG_CATALOG.pg_start_backup(label text, fast boolean) is '';
comment on function PG_CATALOG.pg_stat_clear_snapshot() is '';
comment on function PG_CATALOG.pg_stat_file(filename text) is '';
comment on function PG_CATALOG.pg_stat_get_numscans(oid) is '';
comment on function PG_CATALOG.pg_stat_get_activity(pid bigint) is '';
comment on function PG_CATALOG.pg_stat_get_analyze_count(oid) is '';
comment on function PG_CATALOG.pg_stat_get_autoanalyze_count(oid) is '';
comment on function PG_CATALOG.pg_stat_get_autovacuum_count(oid) is '';
comment on function PG_CATALOG.pg_stat_get_backend_activity(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_activity_start(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_client_addr(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_client_port(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_dbid(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_idset() is '';
comment on function PG_CATALOG.pg_stat_get_backend_pid(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_start(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_userid(integer) is '';
comment on function PG_CATALOG.pg_stat_get_backend_xact_start(integer) is '';
comment on function PG_CATALOG.pg_stat_get_bgwriter_buf_written_checkpoints() is '';
comment on function PG_CATALOG.pg_stat_get_bgwriter_buf_written_clean() is '';
comment on function PG_CATALOG.pg_stat_get_bgwriter_maxwritten_clean() is '';
comment on function PG_CATALOG.pg_stat_get_bgwriter_requested_checkpoints() is '';
comment on function PG_CATALOG.pg_stat_get_bgwriter_stat_reset_time() is '';
comment on function PG_CATALOG.pg_stat_get_bgwriter_timed_checkpoints() is '';
comment on function PG_CATALOG.pg_stat_get_blocks_fetched(oid) is '';
comment on function PG_CATALOG.pg_stat_get_blocks_hit(oid) is '';
comment on function PG_CATALOG.pg_stat_get_buf_alloc() is '';
comment on function PG_CATALOG.pg_stat_get_buf_fsync_backend() is '';
comment on function PG_CATALOG.pg_stat_get_buf_written_backend() is '';
comment on function PG_CATALOG.pg_stat_get_checkpoint_sync_time() is '';
comment on function PG_CATALOG.pg_stat_get_checkpoint_write_time() is '';
comment on function PG_CATALOG.pg_stat_get_db_blk_read_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_blk_write_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_blocks_fetched(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_blocks_hit(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_conflict_all(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_conflict_bufferpin(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_conflict_lock(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_conflict_snapshot(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_conflict_startup_deadlock(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_conflict_tablespace(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_deadlocks(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_numbackends(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_stat_reset_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_temp_bytes(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_temp_files(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_tuples_deleted(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_tuples_fetched(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_tuples_inserted(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_tuples_returned(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_tuples_updated(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_xact_commit(oid) is '';
comment on function PG_CATALOG.pg_stat_get_db_xact_rollback(oid) is '';
comment on function PG_CATALOG.pg_stat_get_dead_tuples(oid) is '';
comment on function PG_CATALOG.pg_stat_get_function_calls(oid) is '';
comment on function PG_CATALOG.pg_stat_get_function_self_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_function_total_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_last_analyze_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_last_autoanalyze_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_last_autovacuum_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_last_data_changed_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_last_vacuum_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_live_tuples(oid) is '';
comment on function PG_CATALOG.pg_stat_get_tuples_deleted(oid) is '';
comment on function PG_CATALOG.pg_stat_get_tuples_fetched(oid) is '';
comment on function PG_CATALOG.pg_stat_get_tuples_hot_updated(oid) is '';
comment on function PG_CATALOG.pg_stat_get_tuples_inserted(oid) is '';
comment on function PG_CATALOG.pg_stat_get_tuples_returned(oid) is '';
comment on function PG_CATALOG.pg_stat_get_tuples_updated(oid) is '';
comment on function PG_CATALOG.pg_stat_get_vacuum_count(oid) is '';
comment on function PG_CATALOG.pg_stat_get_wal_receiver() is '';
comment on function PG_CATALOG.pg_stat_get_wal_senders() is '';
comment on function PG_CATALOG.pg_stat_get_xact_blocks_fetched(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_blocks_hit(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_function_calls(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_function_self_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_function_total_time(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_numscans(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_deleted(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_fetched(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_hot_updated(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_inserted(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_returned(oid) is '';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_updated(oid) is '';
comment on function PG_CATALOG.pg_stat_reset() is '';
comment on function PG_CATALOG.pg_stat_reset_shared(text) is '';
comment on function PG_CATALOG.pg_stat_reset_single_function_counters(oid) is '';
comment on function PG_CATALOG.pg_stat_reset_single_table_counters(oid) is '';
comment on function PG_CATALOG.pg_table_is_visible(oid) is '';
comment on function PG_CATALOG.pg_table_size(regclass) is '';
comment on function PG_CATALOG.pg_tablespace_databases(oid) is '';
comment on function PG_CATALOG.pg_tablespace_location(oid) is '';
comment on function PG_CATALOG.pg_tablespace_size(name) is '';
comment on function PG_CATALOG.pg_tablespace_size(oid) is '';
comment on function PG_CATALOG.pg_terminate_backend(bigint) is '';
comment on function PG_CATALOG.pg_timezone_abbrevs() is '';
comment on function PG_CATALOG.pg_timezone_names() is '';
comment on function PG_CATALOG.pg_total_relation_size(regclass) is '';
comment on function PG_CATALOG.pg_trigger_depth() is '';
comment on function PG_CATALOG.pg_try_advisory_lock(bigint) is '';
comment on function PG_CATALOG.pg_try_advisory_lock(integer, integer) is '';
comment on function PG_CATALOG.pg_try_advisory_lock_shared(bigint) is '';
comment on function PG_CATALOG.pg_try_advisory_lock_shared(integer, integer) is '';
comment on function PG_CATALOG.pg_try_advisory_xact_lock(bigint) is '';
comment on function PG_CATALOG.pg_try_advisory_xact_lock(integer, integer) is '';
comment on function PG_CATALOG.pg_try_advisory_xact_lock_shared(bigint) is '';
comment on function PG_CATALOG.pg_try_advisory_xact_lock_shared(integer, integer) is '';
comment on function PG_CATALOG.pg_ts_config_is_visible(oid) is '';
comment on function PG_CATALOG.pg_ts_dict_is_visible(oid) is '';
comment on function PG_CATALOG.pg_ts_parser_is_visible(oid) is '';
comment on function PG_CATALOG.pg_ts_template_is_visible(oid) is '';
comment on function PG_CATALOG.pg_type_is_visible(oid) is '';
comment on function PG_CATALOG.pg_typeof("any") is '';
comment on function PG_CATALOG.pi() is '';
comment on function PG_CATALOG.plainto_tsquery(regconfig, text) is '';
comment on function PG_CATALOG.plainto_tsquery(text) is '';
comment on function PG_CATALOG.point(box) is '';
comment on function PG_CATALOG.point(double precision, double precision) is '';
comment on function PG_CATALOG.point(circle) is '';
comment on function PG_CATALOG.point(lseg) is '';
comment on function PG_CATALOG.point(path) is '';
comment on function PG_CATALOG.point(polygon) is '';
comment on function PG_CATALOG.point_in(cstring) is '';
comment on function PG_CATALOG.point_out(point) is '';
comment on function PG_CATALOG.point_send(point) is '';
comment on function PG_CATALOG.point_vert(point, point) is '';
comment on function PG_CATALOG.poly_center(polygon) is '';
comment on function PG_CATALOG.poly_in(cstring) is '';
comment on function PG_CATALOG.poly_npoints(polygon) is '';
comment on function PG_CATALOG.poly_out(polygon) is '';
comment on function PG_CATALOG.poly_recv(internal) is '';
comment on function PG_CATALOG.polygon(box) is '';
comment on function PG_CATALOG.polygon(path) is '';
comment on function PG_CATALOG.polygon(integer, circle) is '';
comment on function PG_CATALOG.polygon(circle) is '';
comment on function PG_CATALOG.popen(path) is '';
comment on function PG_CATALOG.position(text, text) is '';
comment on function PG_CATALOG.position(bit, bit) is '';
comment on function PG_CATALOG.position(bytea, bytea) is '';
comment on function PG_CATALOG.positionjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.positionsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.postgresql_fdw_validator(text[], oid) is '';
comment on function PG_CATALOG.pow(double precision, double precision) is '';
comment on function PG_CATALOG.pow(numeric, numeric) is '';
comment on function PG_CATALOG.power(double precision, double precision) is '';
comment on function PG_CATALOG.power(numeric, numeric) is '';
comment on function PG_CATALOG.prsd_end(internal) is '';
comment on function PG_CATALOG.prsd_headline(internal, internal, tsquery) is '';
comment on function PG_CATALOG.prsd_lextype(internal) is '';
comment on function PG_CATALOG.prsd_nexttoken(internal, internal, internal) is '';
comment on function PG_CATALOG.prsd_start(internal, integer, oid) is '';
comment on function PG_CATALOG.query_to_xml(query text, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.query_to_xml_and_xmlschema(query text, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.query_to_xmlschema(query text, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.querytree(tsquery) is '';
comment on function PG_CATALOG.quote_ident(text) is '';
comment on function PG_CATALOG.quote_literal(anyelement) is '';
comment on function PG_CATALOG.quote_literal(text) is '';
comment on function PG_CATALOG.quote_nullable(anyelement) is '';
comment on function PG_CATALOG.quote_nullable(text) is '';
comment on function PG_CATALOG.radians(double precision) is '';
comment on function PG_CATALOG.radius(circle) is '';
comment on function PG_CATALOG.random() is '';
comment on function PG_CATALOG.range_cmp(anyrange, anyrange) is '';
comment on function PG_CATALOG.range_gist_consistent(internal, anyrange, integer, oid, internal) is '';
comment on function PG_CATALOG.range_gist_penalty(internal, internal, internal) is '';
comment on function PG_CATALOG.range_gist_picksplit(internal, internal) is '';
comment on function PG_CATALOG.range_gist_same(anyrange, anyrange, internal) is '';
comment on function PG_CATALOG.range_gist_union(internal, internal) is '';
comment on function PG_CATALOG.range_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.range_out(anyrange) is '';
comment on function PG_CATALOG.range_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.range_send(anyrange) is '';
comment on function PG_CATALOG.range_typanalyze(internal) is '';
comment on function PG_CATALOG.rank() is '';
comment on function PG_CATALOG.rawrecv(internal) is '';
comment on function PG_CATALOG.rawsend(raw) is '';
comment on function PG_CATALOG.record_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.record_out(record) is '';
comment on function PG_CATALOG.record_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.record_send(record) is '';
comment on function PG_CATALOG.regclass(text) is '';
comment on function PG_CATALOG.regclassin(cstring) is '';
comment on function PG_CATALOG.regclassout(regclass) is '';
comment on function PG_CATALOG.regclassrecv(internal) is '';
comment on function PG_CATALOG.regclasssend(regclass) is '';
comment on function PG_CATALOG.regconfigin(cstring) is '';
comment on function PG_CATALOG.regconfigout(regconfig) is '';
comment on function PG_CATALOG.regconfigrecv(internal) is '';
comment on function PG_CATALOG.regconfigsend(regconfig) is '';
comment on function PG_CATALOG.regdictionaryin(cstring) is '';
comment on function PG_CATALOG.regdictionaryout(regdictionary) is '';
comment on function PG_CATALOG.regdictionaryrecv(internal) is '';
comment on function PG_CATALOG.regdictionarysend(regdictionary) is '';
comment on function PG_CATALOG.regexeqjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.regexnejoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.regexnesel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.regexp_matches(text, text) is '';
comment on function PG_CATALOG.regexp_matches(text, text, text) is '';
comment on function PG_CATALOG.regexp_replace(text, text, text) is '';
comment on function PG_CATALOG.regexp_replace(text, text, text, text) is '';
comment on function PG_CATALOG.regexp_split_to_array(text, text) is '';
comment on function PG_CATALOG.regexp_split_to_array(text, text, text) is '';
comment on function PG_CATALOG.regexp_split_to_table(text, text) is '';
comment on function PG_CATALOG.regexp_split_to_table(text, text, text) is '';
comment on function PG_CATALOG.regoperatorin(cstring) is '';
comment on function PG_CATALOG.regoperatorout(regoperator) is '';
comment on function PG_CATALOG.regoperatorrecv(internal) is '';
comment on function PG_CATALOG.regoperatorsend(regoperator) is '';
comment on function PG_CATALOG.regoperin(cstring) is '';
comment on function PG_CATALOG.regoperout(regoper) is '';
comment on function PG_CATALOG.regopersend(regoper) is '';
comment on function PG_CATALOG.regprocedurein(cstring) is '';
comment on function PG_CATALOG.regprocedureout(regprocedure) is '';
comment on function PG_CATALOG.regprocedurerecv(internal) is '';
comment on function PG_CATALOG.regproceduresend(regprocedure) is '';
comment on function PG_CATALOG.regprocin(cstring) is '';
comment on function PG_CATALOG.regprocout(regproc) is '';
comment on function PG_CATALOG.regprocrecv(internal) is '';
comment on function PG_CATALOG.regprocsend(regproc) is '';
comment on function PG_CATALOG.regr_avgx(double precision, double precision) is '';
comment on function PG_CATALOG.regr_avgy(double precision, double precision) is '';
comment on function PG_CATALOG.regr_count(double precision, double precision) is '';
comment on function PG_CATALOG.regr_intercept(double precision, double precision) is '';
comment on function PG_CATALOG.regr_r2(double precision, double precision) is '';
comment on function PG_CATALOG.regr_slope(double precision, double precision) is '';
comment on function PG_CATALOG.regr_sxx(double precision, double precision) is '';
comment on function PG_CATALOG.regr_sxy(double precision, double precision) is '';
comment on function PG_CATALOG.regr_syy(double precision, double precision) is '';
comment on function PG_CATALOG.regtypein(cstring) is '';
comment on function PG_CATALOG.regtypeout(regtype) is '';
comment on function PG_CATALOG.regtyperecv(internal) is '';
comment on function PG_CATALOG.regtypesend(regtype) is '';
comment on function PG_CATALOG.repeat(text, integer) is '';
comment on function PG_CATALOG.replace(text, text, text) is '';
comment on function PG_CATALOG.reverse(text) is '';
comment on function PG_CATALOG.right(text, integer) is '';
comment on function PG_CATALOG.round(numeric, integer) is '';
comment on function PG_CATALOG.round(double precision) is '';
comment on function PG_CATALOG.round(numeric) is '';
comment on function PG_CATALOG.row_number() is '';
comment on function PG_CATALOG.row_to_json(record) is '';
comment on function PG_CATALOG.row_to_json(record, boolean) is '';
comment on function PG_CATALOG.rpad(text, integer) is '';
comment on function PG_CATALOG.rtrim(text) is '';
comment on function PG_CATALOG.rtrim(text, text) is '';
comment on function PG_CATALOG.scalargtjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.scalargtsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.scalarltjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.scalarltsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.session_user() is '';
comment on function PG_CATALOG.set_bit(bit, integer, integer) is '';
comment on function PG_CATALOG.set_bit(bytea, integer, integer) is '';
comment on function PG_CATALOG.set_byte(bytea, integer, integer) is '';
comment on function PG_CATALOG.set_config(text, text, boolean) is '';
comment on function PG_CATALOG.set_masklen(cidr, integer) is '';
comment on function PG_CATALOG.set_masklen(inet, integer) is '';
comment on function PG_CATALOG.setseed(double precision) is '';
comment on function PG_CATALOG.setval(regclass, bigint) is '';
comment on function PG_CATALOG.setval(regclass, bigint, boolean) is '';
comment on function PG_CATALOG.setweight(tsvector, "char") is '';
comment on function PG_CATALOG.shell_in(cstring) is '';
comment on function PG_CATALOG.shell_out(opaque) is '';
comment on function PG_CATALOG.shobj_description(oid, name) is '';
comment on function PG_CATALOG.similar_escape(text, text) is '';
comment on function PG_CATALOG.sin(double precision) is '';
comment on function PG_CATALOG.slope(point, point) is '';
comment on function PG_CATALOG.spg_kd_choose(internal, internal) is '';
comment on function PG_CATALOG.spg_kd_config(internal, internal) is '';
comment on function PG_CATALOG.spg_kd_inner_consistent(internal, internal) is '';
comment on function PG_CATALOG.spg_kd_picksplit(internal, internal) is '';
comment on function PG_CATALOG.spg_quad_choose(internal, internal) is '';
comment on function PG_CATALOG.spg_quad_config(internal, internal) is '';
comment on function PG_CATALOG.spg_quad_inner_consistent(internal, internal) is '';
comment on function PG_CATALOG.spg_quad_leaf_consistent(internal, internal) is '';
comment on function PG_CATALOG.spg_quad_picksplit(internal, internal) is '';
comment on function PG_CATALOG.spg_text_choose(internal, internal) is '';
comment on function PG_CATALOG.spg_text_config(internal, internal) is '';
comment on function PG_CATALOG.spg_text_inner_consistent(internal, internal) is '';
comment on function PG_CATALOG.spg_text_leaf_consistent(internal, internal) is '';
comment on function PG_CATALOG.spg_text_picksplit(internal, internal) is '';
comment on function PG_CATALOG.split_part(text, text, integer) is '';
comment on function PG_CATALOG.sqrt(numeric) is '';
comment on function PG_CATALOG.statement_timestamp() is '';
comment on function PG_CATALOG.stddev(bigint) is '';
comment on function PG_CATALOG.stddev(double precision) is '';
comment on function PG_CATALOG.stddev(integer) is '';
comment on function PG_CATALOG.stddev(numeric) is '';
comment on function PG_CATALOG.stddev(real) is '';
comment on function PG_CATALOG.stddev(smallint) is '';
comment on function PG_CATALOG.stddev_pop(bigint) is '';
comment on function PG_CATALOG.stddev_pop(double precision) is '';
comment on function PG_CATALOG.stddev_pop(integer) is '';
comment on function PG_CATALOG.stddev_pop(numeric) is '';
comment on function PG_CATALOG.stddev_pop(real) is '';
comment on function PG_CATALOG.stddev_pop(smallint) is '';
comment on function PG_CATALOG.stddev_samp(bigint) is '';
comment on function PG_CATALOG.stddev_samp(double precision) is '';
comment on function PG_CATALOG.stddev_samp(integer) is '';
comment on function PG_CATALOG.stddev_samp(numeric) is '';
comment on function PG_CATALOG.stddev_samp(real) is '';
comment on function PG_CATALOG.stddev_samp(smallint) is '';
comment on function PG_CATALOG.string_agg(bytea, bytea) is '';
comment on function PG_CATALOG.string_agg(text, text) is '';
comment on function PG_CATALOG.string_agg_finalfn(internal) is '';
comment on function PG_CATALOG.string_agg_transfn(internal, text, text) is '';
comment on function PG_CATALOG.string_to_array(text, text) is '';
comment on function PG_CATALOG.string_to_array(text, text, text) is '';
comment on function PG_CATALOG.strip(tsvector) is '';
comment on function PG_CATALOG.substring(bit, integer) is '';
comment on function PG_CATALOG.substring(text, text) is '';
comment on function PG_CATALOG.substring(bytea, integer, integer) is '';
comment on function PG_CATALOG.substring(bytea, integer) is '';
comment on function PG_CATALOG.substring(bit, integer, integer) is '';
comment on function PG_CATALOG.substring_inner(text, integer) is '';
comment on function PG_CATALOG.substring_inner(text, integer, integer) is '';
comment on function PG_CATALOG.sum(bigint) is '';
comment on function PG_CATALOG.sum(double precision) is '';
comment on function PG_CATALOG.sum(integer) is '';
comment on function PG_CATALOG.sum(interval) is '';
comment on function PG_CATALOG.sum(money) is '';
comment on function PG_CATALOG.sum(numeric) is '';
comment on function PG_CATALOG.sum(real) is '';
comment on function PG_CATALOG.sum(smallint) is '';
comment on function PG_CATALOG.suppress_redundant_updates_trigger() is '';
comment on function PG_CATALOG.table_to_xml(tbl regclass, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.table_to_xml_and_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.table_to_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text) is '';
comment on function PG_CATALOG.tan(double precision) is '';
comment on function PG_CATALOG.text("char") is '';
comment on function PG_CATALOG.text(name) is '';
comment on function PG_CATALOG.text(inet) is '';
comment on function PG_CATALOG.text(character) is '';
comment on function PG_CATALOG.text(boolean) is '';
comment on function PG_CATALOG.text_larger(text, text) is '';
comment on function PG_CATALOG.text_smaller(text, text) is '';
comment on function PG_CATALOG.textin(cstring) is '';
comment on function PG_CATALOG.textlen(text) is '';
comment on function PG_CATALOG.textlike(text, text) is '';
comment on function PG_CATALOG.textnlike(text, text) is '';
comment on function PG_CATALOG.textout(text) is '';
comment on function PG_CATALOG.textrecv(internal) is '';
comment on function PG_CATALOG.textsend(text) is '';
comment on function PG_CATALOG.thesaurus_init(internal) is '';
comment on function PG_CATALOG.thesaurus_lexize() is '';
comment on function PG_CATALOG.tidin(cstring) is '';
comment on function PG_CATALOG.tidlarger(tid, tid) is '';
comment on function PG_CATALOG.tidout(tid) is '';
comment on function PG_CATALOG.tidrecv(internal) is '';
comment on function PG_CATALOG.tidsend(tid) is '';
comment on function PG_CATALOG.tidsmaller(tid, tid) is '';
comment on function PG_CATALOG.time(interval) is '';
comment on function PG_CATALOG.time(time without time zone, integer) is '';
comment on function PG_CATALOG.time(timestamp without time zone) is '';
comment on function PG_CATALOG.time(timestamp with time zone) is '';
comment on function PG_CATALOG.time(time with time zone) is '';
comment on function PG_CATALOG.time_cmp(time without time zone, time without time zone) is '';
comment on function PG_CATALOG.time_hash(time without time zone) is '';
comment on function PG_CATALOG.time_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.time_larger(time without time zone, time without time zone) is '';
comment on function PG_CATALOG.time_out(time without time zone) is '';
comment on function PG_CATALOG.time_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.time_send(time without time zone) is '';
comment on function PG_CATALOG.time_smaller(time without time zone, time without time zone) is '';
comment on function PG_CATALOG.timeofday() is '';
comment on function PG_CATALOG.timestamp(timestamp without time zone, integer) is '';
comment on function PG_CATALOG.timestamp(timestamp with time zone) is '';
comment on function PG_CATALOG.timestamp_cmp(timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.timestamp_cmp_timestamptz(timestamp without time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.timestamp_hash(timestamp without time zone) is '';
comment on function PG_CATALOG.timestamp_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.timestamp_larger(timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.timestamp_out(timestamp without time zone) is '';
comment on function PG_CATALOG.timestamp_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.timestamp_send(timestamp without time zone) is '';
comment on function PG_CATALOG.timestamp_smaller(timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.timestamp_sortsupport(internal) is '';
comment on function PG_CATALOG.timestamptypmodin(cstring[]) is '';
comment on function PG_CATALOG.timestamptypmodout(integer) is '';
comment on function PG_CATALOG.timestamptz(date) is '';
comment on function PG_CATALOG.timestamptz(timestamp with time zone, integer) is '';
comment on function PG_CATALOG.timestamptz(timestamp without time zone) is '';
comment on function PG_CATALOG.timestamptz_cmp(timestamp with time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.timestamptz_cmp_timestamp(timestamp with time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.timestamptz_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.timestamptz_larger(timestamp with time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.timestamptz_out(timestamp with time zone) is '';
comment on function PG_CATALOG.timestamptz_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.timestamptz_send(timestamp with time zone) is '';
comment on function PG_CATALOG.timestamptz_smaller(timestamp with time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.timestamptztypmodin(cstring[]) is '';
comment on function PG_CATALOG.timestamptztypmodout(integer) is '';
comment on function PG_CATALOG.timetypmodin(cstring[]) is '';
comment on function PG_CATALOG.timetypmodout(integer) is '';
comment on function PG_CATALOG.timetz(time with time zone, integer) is '';
comment on function PG_CATALOG.timetz(time without time zone) is '';
comment on function PG_CATALOG.timetz(timestamp with time zone) is '';
comment on function PG_CATALOG.timetz_cmp(time with time zone, time with time zone) is '';
comment on function PG_CATALOG.timetz_hash(time with time zone) is '';
comment on function PG_CATALOG.timetz_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.timetz_larger(time with time zone, time with time zone) is '';
comment on function PG_CATALOG.timetz_out(time with time zone) is '';
comment on function PG_CATALOG.timetz_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.timetz_send(time with time zone) is '';
comment on function PG_CATALOG.timetz_smaller(time with time zone, time with time zone) is '';
comment on function PG_CATALOG.timetztypmodin(cstring[]) is '';
comment on function PG_CATALOG.timetztypmodout(integer) is '';
comment on function PG_CATALOG.timezone(interval, timestamp with time zone) is '';
comment on function PG_CATALOG.timezone(interval, timestamp without time zone) is '';
comment on function PG_CATALOG.timezone(interval, time with time zone) is '';
comment on function PG_CATALOG.timezone(text, time with time zone ) is '';
comment on function PG_CATALOG.timezone(text, timestamp with time zone) is '';
comment on function PG_CATALOG.timezone(text, timestamp without time zone) is '';
comment on function PG_CATALOG.to_ascii(text) is '';
comment on function PG_CATALOG.to_ascii(text, integer) is '';
comment on function PG_CATALOG.to_ascii(text, name) is '';
comment on function PG_CATALOG.to_char(interval, text) is '';
comment on function PG_CATALOG.to_char(timestamp with time zone, text) is '';
comment on function PG_CATALOG.to_char(numeric, text) is '';
comment on function PG_CATALOG.to_char(integer, text) is '';
comment on function PG_CATALOG.to_char(bigint, text) is '';
comment on function PG_CATALOG.to_char(real, text) is '';
comment on function PG_CATALOG.to_char(double precision, text) is '';
comment on function PG_CATALOG.to_char(timestamp with time zone, text) is '';
comment on function PG_CATALOG.to_date(text, text) is '';
comment on function PG_CATALOG.to_hex(bigint) is '';
comment on function PG_CATALOG.to_hex(integer) is '';
comment on function PG_CATALOG.to_number(text, text) is '';
comment on function PG_CATALOG.to_timestamp(text, text) is '';
comment on function PG_CATALOG.to_tsquery(regconfig, text) is '';
comment on function PG_CATALOG.to_tsquery(text) is '';
comment on function PG_CATALOG.to_tsvector(regconfig, text) is '';
comment on function PG_CATALOG.to_tsvector(text) is '';
comment on function PG_CATALOG.to_tsvector_for_batch(regconfig, text) is '';
comment on function PG_CATALOG.to_tsvector_for_batch(text) is '';
comment on function PG_CATALOG.transaction_timestamp() is '';
comment on function PG_CATALOG.translate(text, text, text) is '';
comment on function PG_CATALOG.trigger_in(cstring) is '';
comment on function PG_CATALOG.trigger_out(trigger) is '';
comment on function PG_CATALOG.trunc(macaddr) is '';
comment on function PG_CATALOG.trunc(numeric, integer) is '';
comment on function PG_CATALOG.trunc(numeric) is '';
comment on function PG_CATALOG.trunc(double precision) is '';
comment on function PG_CATALOG.ts_headline(regconfig, text, tsquery) is '';
comment on function PG_CATALOG.ts_headline(regconfig, text, tsquery, text) is '';
comment on function PG_CATALOG.ts_headline(text, tsquery) is '';
comment on function PG_CATALOG.ts_headline(text, tsquery, text) is '';
comment on function PG_CATALOG.ts_lexize(regdictionary, text) is '';
comment on function PG_CATALOG.ts_parse(parser_name text, txt text) is '';
comment on function PG_CATALOG.ts_parse(parser_oid oid, txt text) is '';
comment on function PG_CATALOG.ts_rank(real[], tsvector, tsquery) is '';
comment on function PG_CATALOG.ts_rank(real[], tsvector, tsquery, integer) is '';
comment on function PG_CATALOG.ts_rank(tsvector, tsquery) is '';
comment on function PG_CATALOG.ts_rank(tsvector, tsquery, integer) is '';
comment on function PG_CATALOG.ts_rank_cd(real[], tsvector, tsquery) is '';
comment on function PG_CATALOG.ts_rank_cd(real[], tsvector, tsquery, integer) is '';
comment on function PG_CATALOG.ts_rank_cd(tsvector, tsquery) is '';
comment on function PG_CATALOG.ts_rank_cd(tsvector, tsquery, integer) is '';
comment on function PG_CATALOG.ts_rewrite(tsquery, text) is '';
comment on function PG_CATALOG.ts_rewrite(tsquery, tsquery, tsquery) is '';
comment on function PG_CATALOG.ts_stat(query text) is '';
comment on function PG_CATALOG.ts_stat(query text, weights text) is '';
comment on function PG_CATALOG.ts_token_type(parser_name text) is '';
comment on function PG_CATALOG.ts_token_type(parser_oid oid) is '';
comment on function PG_CATALOG.ts_typanalyze(internal) is '';
comment on function PG_CATALOG.tsmatchjoinsel(internal, oid, internal, smallint, internal) is '';
comment on function PG_CATALOG.tsmatchsel(internal, oid, internal, integer) is '';
comment on function PG_CATALOG.tsquery_cmp(tsquery, tsquery) is '';
comment on function PG_CATALOG.tsqueryin(cstring) is '';
comment on function PG_CATALOG.tsqueryout(tsquery) is '';
comment on function PG_CATALOG.tsqueryrecv(internal) is '';
comment on function PG_CATALOG.tsquerysend(tsquery) is '';
comment on function PG_CATALOG.tsrange(timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.tsrange(timestamp without time zone, timestamp without time zone, text) is '';
comment on function PG_CATALOG.tsrange_subdiff(timestamp without time zone, timestamp without time zone) is '';
comment on function PG_CATALOG.tstzrange(timestamp with time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.tstzrange(timestamp with time zone, timestamp with time zone, text) is '';
comment on function PG_CATALOG.tstzrange_subdiff(timestamp with time zone, timestamp with time zone) is '';
comment on function PG_CATALOG.tsvector_cmp(tsvector, tsvector) is '';
comment on function PG_CATALOG.tsvector_update_trigger() is '';
comment on function PG_CATALOG.tsvector_update_trigger_column() is '';
comment on function PG_CATALOG.tsvectorin(cstring) is '';
comment on function PG_CATALOG.tsvectorout(tsvector) is '';
comment on function PG_CATALOG.tsvectorrecv(internal) is '';
comment on function PG_CATALOG.tsvectorsend(tsvector) is '';
comment on function PG_CATALOG.unique_key_recheck() is '';
comment on function PG_CATALOG.unknownin(cstring) is '';
comment on function PG_CATALOG.unknownout(unknown) is '';
comment on function PG_CATALOG.unknownrecv(internal) is '';
comment on function PG_CATALOG.unknownsend(unknown) is '';
comment on function PG_CATALOG.unnest(anyarray) is '';
comment on function PG_CATALOG.upper(text) is '';
comment on function PG_CATALOG.upper(anyrange) is '';
comment on function PG_CATALOG.upper_inc(anyrange) is '';
comment on function PG_CATALOG.upper_inf(anyrange) is '';
comment on function PG_CATALOG.uuid_cmp(uuid, uuid) is '';
comment on function PG_CATALOG.uuid_hash(uuid) is '';
comment on function PG_CATALOG.uuid_in(cstring) is '';
comment on function PG_CATALOG.uuid_out(uuid) is '';
comment on function PG_CATALOG.uuid_recv(internal) is '';
comment on function PG_CATALOG.uuid_send(uuid) is '';
comment on function PG_CATALOG.var_pop(bigint) is '';
comment on function PG_CATALOG.var_pop(double precision) is '';
comment on function PG_CATALOG.var_pop(integer) is '';
comment on function PG_CATALOG.var_pop(numeric) is '';
comment on function PG_CATALOG.var_pop(real) is '';
comment on function PG_CATALOG.var_pop(smallint) is '';
comment on function PG_CATALOG.var_samp(bigint) is '';
comment on function PG_CATALOG.var_samp(double precision) is '';
comment on function PG_CATALOG.var_samp(integer) is '';
comment on function PG_CATALOG.var_samp(numeric) is '';
comment on function PG_CATALOG.var_samp(real) is '';
comment on function PG_CATALOG.var_samp(smallint) is '';
comment on function PG_CATALOG.varbit(bit varying, integer, boolean) is '';
comment on function PG_CATALOG.varbit_in(cstring, oid, integer) is '';
comment on function PG_CATALOG.varbit_out(bit varying) is '';
comment on function PG_CATALOG.varbit_recv(internal, oid, integer) is '';
comment on function PG_CATALOG.varbit_send(bit varying) is '';
comment on function PG_CATALOG.varbitcmp(bit varying, bit varying) is '';
comment on function PG_CATALOG.varbittypmodin(cstring[]) is '';
comment on function PG_CATALOG.varbittypmodout(integer) is '';
comment on function PG_CATALOG.varchar(character varying, integer, boolean) is '';
comment on function PG_CATALOG.varchar(name) is '';
comment on function PG_CATALOG.varcharin(cstring, oid, integer) is '';
comment on function PG_CATALOG.varcharout(character varying) is '';
comment on function PG_CATALOG.varcharrecv(internal, oid, integer) is '';
comment on function PG_CATALOG.varcharsend(character varying) is '';
comment on function PG_CATALOG.varchartypmodin(cstring[]) is '';
comment on function PG_CATALOG.varchartypmodout(integer) is '';
comment on function PG_CATALOG.variance(bigint) is '';
comment on function PG_CATALOG.variance(double precision) is '';
comment on function PG_CATALOG.variance(integer) is '';
comment on function PG_CATALOG.variance(numeric) is '';
comment on function PG_CATALOG.variance(real) is '';
comment on function PG_CATALOG.variance(smallint) is '';
comment on function PG_CATALOG.void_in(cstring) is '';
comment on function PG_CATALOG.void_out(void) is '';
comment on function PG_CATALOG.void_recv(internal) is '';
comment on function PG_CATALOG.void_send(void) is '';
comment on function PG_CATALOG.width(box) is '';
comment on function PG_CATALOG.width_bucket(numeric, numeric, numeric, integer) is '';
comment on function PG_CATALOG.xidin(cstring) is '';
comment on function PG_CATALOG.xidout(xid) is '';
comment on function PG_CATALOG.xidrecv(internal) is '';
comment on function PG_CATALOG.xidsend(xid) is '';
  END IF;
END
$do$;
ALTER INDEX pg_proc_proname_args_nsp_index rebuild;

-- ----------------------------------------------------------------
-- rollback array interface of pg_catalog
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.array_deleteidx(anyarray, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_last_value;
-- ----------------------------------------------------------------
-- rollback array interface of pg_catalog
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_exists(anyarray, character varying) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_next(anyarray, character varying) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_prior(anyarray, character varying) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_deleteidx(anyarray, character varying) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_first(anyarray) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_last(anyarray) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_exists(anyarray, character varying) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_next(anyarray, character varying) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_varchar_prior(anyarray, character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7882;
CREATE OR REPLACE FUNCTION pg_catalog.array_varchar_exists(anyarray, character varying)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_exists$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7881;
CREATE OR REPLACE FUNCTION pg_catalog.array_varchar_next(anyarray, character varying)
 RETURNS character varying
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_next$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7883;
CREATE OR REPLACE FUNCTION pg_catalog.array_varchar_prior(anyarray, character varying)
 RETURNS character varying
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_prior$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP FUNCTION IF EXISTS pg_catalog.update_pgjob;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3998;
CREATE OR REPLACE FUNCTION pg_catalog.update_pgjob(bigint, "char", bigint, timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone, smallint)
 RETURNS void
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$syn_update_pg_job$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION IF EXISTS pg_catalog.array_integer_deleteidx(anyarray, integer) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_integer_exists(anyarray, integer) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_integer_first(anyarray) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_integer_last(anyarray) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_integer_next(anyarray, integer) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_integer_prior(anyarray, integer) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_union(anyarray, anyarray) cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_union_distinct(anyarray, anyarray) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8700;
CREATE OR REPLACE FUNCTION pg_catalog.array_cat_distinct(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_cat_distinct$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;-- ----------------------------------------------------------------
-- upgrade pg_comm_status
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.global_comm_get_status() CASCADE;
-- drop old view pg_comm_status
DROP VIEW IF EXISTS pg_catalog.pg_comm_status CASCADE;
-- drop old function pg_comm_status
DROP FUNCTION IF EXISTS pg_catalog.pg_comm_status() CASCADE;


-- create new pg_comm_status function
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1986;
CREATE FUNCTION pg_catalog.pg_comm_status(OUT node_name text, OUT "rxpck_rate" int4, OUT "txpck_rate" int4, OUT "rxkbyte_rate" int8, OUT "txkbyte_rate" int8, OUT buffer int8, OUT "memkbyte_libcomm" int8, OUT "memkbyte_libpq" int8, OUT "used_pm" int4, OUT "used_sflow" int4, OUT "used_rflow" int4, OUT "used_rloop" int4, OUT stream int4) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'pg_comm_status';
-- create new pg_comm_status view
CREATE OR REPLACE VIEW pg_catalog.pg_comm_status AS
    SELECT * FROM pg_catalog.pg_comm_status();

-- create new function for centralized mode
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1990;
CREATE FUNCTION pg_catalog.global_comm_get_status(OUT node_name text, OUT "rxpck_rate" int4, OUT "txpck_rate" int4, OUT "rxkbyte_rate" int8, OUT "txkbyte_rate" int8, OUT buffer int8, OUT "memkbyte_libcomm" int8, OUT "memkbyte_libpq" int8, OUT "used_pm" int4, OUT "used_sflow" int4, OUT "used_rflow" int4, OUT "used_rloop" int4, OUT stream int4) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'global_comm_get_status';

GRANT SELECT ON pg_catalog.pg_comm_status TO public;DROP FUNCTION IF EXISTS pg_catalog.dynamic_func_control() cascade;
DROP FUNCTION IF EXISTS pg_catalog.nlssort(text, text);DROP FUNCTION IF EXISTS pg_catalog.gs_write_term_log() cascade;
DROP FUNCTION IF EXISTS pg_catalog.array_indexby_length(anyarray, integer) cascade;-- deleting system view
DROP VIEW IF EXISTS pg_catalog.pg_publication_tables;
DROP VIEW IF EXISTS pg_catalog.pg_stat_subscription;
DROP VIEW IF EXISTS pg_catalog.pg_replication_origin_status;

-- deleting function pg_replication_origin_create
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_create(IN node_name text, OUT replication_origin_oid oid) CASCADE;

-- deleting function pg_replication_origin_drop
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_drop(IN node_name text, OUT replication_origin_oid oid) CASCADE;

-- deleting function pg_replication_origin_oid
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_oid(IN node_name text, OUT replication_origin_oid oid) CASCADE;

-- deleting function pg_replication_origin_session_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_setup(IN node_name text) CASCADE;

-- deleting function pg_replication_origin_session_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_reset() CASCADE;

-- deleting function pg_replication_origin_session_is_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_is_setup() CASCADE;

-- deleting function pg_replication_origin_session_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_progress(IN flush boolean) CASCADE;

-- deleting function pg_replication_origin_xact_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_setup(IN origin_lsn text, IN origin_timestamp timestamp with time zone) CASCADE;

-- deleting function pg_replication_origin_xact_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_reset() CASCADE;

-- deleting function pg_replication_origin_advance
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_advance(IN node_name text, IN lsn text) CASCADE;

-- deleting function pg_replication_origin_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_progress(IN node_name text, IN flush boolean) CASCADE;

-- deleting function pg_show_replication_origin_status
DROP FUNCTION IF EXISTS pg_catalog.pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn text, OUT local_lsn text) CASCADE;

-- deleting function pg_get_publication_tables
DROP FUNCTION IF EXISTS pg_catalog.pg_get_publication_tables(IN pubname text, OUT relid oid) CASCADE;

-- deleting function pg_stat_get_subscription
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) CASCADE;

-- deleting system table pg_subscription

DROP INDEX IF EXISTS pg_catalog.pg_subscription_oid_index;
DROP INDEX IF EXISTS pg_catalog.pg_subscription_subname_index;
DROP TYPE IF EXISTS pg_catalog.pg_subscription;
DROP TABLE IF EXISTS pg_catalog.pg_subscription;

-- deleting system table pg_replication_origin

DROP INDEX IF EXISTS pg_catalog.pg_replication_origin_roident_index;
DROP INDEX IF EXISTS pg_catalog.pg_replication_origin_roname_index;
DROP TYPE IF EXISTS pg_catalog.pg_replication_origin;
DROP TABLE IF EXISTS pg_catalog.pg_replication_origin;