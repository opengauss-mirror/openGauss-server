-- Rollback pg_lsn type and related I/O functions
-- This rollback script removes the pg_lsn type and its I/O functions

DO $$
DECLARE
ans boolean;
BEGIN
    -- Check if pg_lsn type exists
    select case when count(*)=1 then true else false end as ans 
    from (select * from pg_type where typname = 'pg_lsn' limit 1) into ans;
    
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_send(pg_lsn) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_recv(internal) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_out(pg_lsn) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_in(cstring) CASCADE;
    end if;
END$$;

DROP TYPE IF EXISTS pg_catalog._pg_lsn CASCADE;
DROP TYPE IF EXISTS pg_catalog.pg_lsn CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.timestamp_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INTERVAL)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.interval_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(NUMERIC)
RETURNS NVARCHAR2
AS $$ SELECT CAST(pg_catalog.numeric_out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT2)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.int2out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT4)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.int4out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT8)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.int8out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT4)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.float4out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT8)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.float8out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

-- 1. 回滚 pg_sequence_parameters 至原始 6 参数版本
-- CASCADE 自动删除依赖此函数的 information_schema.sequences 视图
DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_parameters(
    sequence_oid oid,
    OUT start_value int16,
    OUT minimum_value int16,
    OUT maximum_value int16,
    OUT increment int16,
    OUT cycle_option boolean,
    OUT is_global_cache boolean
) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3078;
CREATE OR REPLACE FUNCTION pg_catalog.pg_sequence_parameters(
    sequence_oid oid,
    OUT start_value int16,
    OUT minimum_value int16,
    OUT maximum_value int16,
    OUT increment int16,
    OUT cycle_option boolean
)
RETURNS record LANGUAGE internal STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_sequence_parameters$function$;
COMMENT ON FUNCTION pg_catalog.pg_sequence_parameters(oid) IS 'sequence parameters, for use by information schema';

-- 2. 回滚 pg_sequence_all_parameters 至原始 13 参数版本
DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_all_parameters(
    sequence_name text,
    OUT start_value int16,
    OUT minimum_value int16,
    OUT maximum_value int16,
    OUT increment int16,
    OUT cycle_option boolean,
    OUT cache_size int16,
    OUT last_value int16,
    OUT is_called boolean,
    OUT log_cnt int8,
    OUT uuid int8,
    OUT last_used_value int16,
    OUT is_exhausted boolean,
    OUT is_global_cache boolean
) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8930;
CREATE OR REPLACE FUNCTION pg_catalog.pg_sequence_all_parameters(
    sequence_name text,
    OUT start_value int16,
    OUT minimum_value int16,
    OUT maximum_value int16,
    OUT increment int16,
    OUT cycle_option boolean,
    OUT cache_size int16,
    OUT last_value int16,
    OUT is_called boolean,
    OUT log_cnt int8,
    OUT uuid int8,
    OUT last_used_value int16,
    OUT is_exhausted boolean
)
RETURNS record
LANGUAGE internal
STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_sequence_all_parameters$function$;

COMMENT ON FUNCTION pg_catalog.pg_sequence_all_parameters(
    sequence_name text, OUT start_value int16, OUT minimum_value int16,
    OUT maximum_value int16, OUT increment int16, OUT cycle_option boolean,
    OUT cache_size int16, OUT last_value int16, OUT is_called boolean,
    OUT log_cnt int8, OUT uuid int8, OUT last_used_value int16,
    OUT is_exhausted boolean) IS 'sequence all parameters';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-- 3. 恢复 information_schema.sequences 视图至原始定义（只过滤 'L'、'S'）
SET search_path TO information_schema;
DROP VIEW IF EXISTS information_schema.sequences;
CREATE VIEW information_schema.sequences AS
    SELECT CAST(pg_catalog.current_database() AS sql_identifier) AS sequence_catalog,
           CAST(nc.nspname AS sql_identifier) AS sequence_schema,
           CAST(c.relname AS sql_identifier) AS sequence_name,
           CAST('int16' AS character_data) AS data_type,
           CAST(128 AS cardinal_number) AS numeric_precision,
           CAST(2 AS cardinal_number) AS numeric_precision_radix,
           CAST(0 AS cardinal_number) AS numeric_scale,
           -- XXX: The following could be improved if we had LATERAL.
           CAST((pg_catalog.pg_sequence_parameters(c.oid)).start_value AS character_data) AS start_value,
           CAST((pg_catalog.pg_sequence_parameters(c.oid)).minimum_value AS character_data) AS minimum_value,
           CAST((pg_catalog.pg_sequence_parameters(c.oid)).maximum_value AS character_data) AS maximum_value,
           CAST((pg_catalog.pg_sequence_parameters(c.oid)).increment AS character_data) AS increment,
           CAST(CASE WHEN (pg_catalog.pg_sequence_parameters(c.oid)).cycle_option THEN 'YES' ELSE 'NO' END AS yes_or_no) AS cycle_option
    FROM pg_catalog.pg_namespace nc, pg_catalog.pg_class c
    WHERE c.relnamespace = nc.oid
          AND (c.relkind = 'L' or c.relkind = 'S')
          AND (NOT pg_catalog.pg_is_other_temp_schema(nc.oid))
          AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
               OR pg_catalog.has_sequence_privilege(c.oid, 'SELECT, UPDATE, USAGE') );
GRANT SELECT ON information_schema.sequences TO PUBLIC;
RESET search_path;

-- 4. 恢复 views 更新 pg_catalog.pg_seclabels 视图至原始定义（'S'、'L'）
CREATE OR REPLACE VIEW pg_catalog.pg_seclabels AS
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN rel.relkind = 'r' THEN 'table'::text
		 WHEN rel.relkind = 'v' THEN 'view'::text
		 WHEN rel.relkind = 'm' THEN 'materialized view'::text
		 WHEN rel.relkind = 'S'THEN 'sequence'::text
         WHEN rel.relkind = 'L'THEN 'large sequence'::text
		 WHEN rel.relkind = 'f' THEN 'foreign table'::text END AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_table_is_visible(rel.oid)
	     THEN pg_catalog.quote_ident(rel.relname)
	     ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(rel.relname)
	     END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'column'::text AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_table_is_visible(rel.oid)
	     THEN pg_catalog.quote_ident(rel.relname)
	     ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(rel.relname)
	     END || '.' || att.attname AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_attribute att
	     ON rel.oid = att.attrelid AND l.objsubid = att.attnum
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid != 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN pro.proisagg = true THEN 'aggregate'::text
	     WHEN pro.proisagg = false THEN 'function'::text
	END AS objtype,
	pro.pronamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_function_is_visible(pro.oid)
	     THEN pg_catalog.quote_ident(pro.proname)
	     ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(pro.proname)
	END || '(' || pg_catalog.pg_get_function_arguments(pro.oid) || ')' AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_proc pro ON l.classoid = pro.tableoid AND l.objoid = pro.oid
	JOIN pg_namespace nsp ON pro.pronamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN typ.typtype = 'd' THEN 'domain'::text
	ELSE 'type'::text END AS objtype,
	typ.typnamespace AS objnamespace,
	CASE WHEN pg_catalog.pg_type_is_visible(typ.oid)
	THEN pg_catalog.quote_ident(typ.typname)
	ELSE pg_catalog.quote_ident(nsp.nspname) || '.' || pg_catalog.quote_ident(typ.typname)
	END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_type typ ON l.classoid = typ.tableoid AND l.objoid = typ.oid
	JOIN pg_namespace nsp ON typ.typnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'large object'::text AS objtype,
	NULL::oid AS objnamespace,
	l.objoid::text AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_largeobject_metadata lom ON l.objoid = lom.oid
WHERE
	l.classoid = 'pg_catalog.pg_largeobject'::regclass AND l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'language'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(lan.lanname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_language lan ON l.classoid = lan.tableoid AND l.objoid = lan.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'schema'::text AS objtype,
	nsp.oid AS objnamespace,
    pg_catalog.quote_ident(nsp.nspname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_namespace nsp ON l.classoid = nsp.tableoid AND l.objoid = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
    l.objoid, l.classoid, l.objsubid,
    'event trigger'::text AS objtype,
    NULL::oid AS objnamespace,
    quote_ident(evt.evtname) AS objname,
    l.provider, l.label
FROM
    pg_seclabel l
    JOIN pg_event_trigger evt ON l.classoid = evt.tableoid
        AND l.objoid = evt.oid
WHERE
    l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'database'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(dat.datname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_database dat ON l.classoid = dat.tableoid AND l.objoid = dat.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'tablespace'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(spc.spcname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_tablespace spc ON l.classoid = spc.tableoid AND l.objoid = spc.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'role'::text AS objtype,
	NULL::oid AS objnamespace,
    pg_catalog.quote_ident(rol.rolname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_authid rol ON l.classoid = rol.tableoid AND l.objoid = rol.oid;

-- 5. 恢复 dbe_perf.statio_all_sequences 视图至原始定义（只过滤 'S'、'L'）
CREATE OR REPLACE VIEW dbe_perf.statio_all_sequences AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_catalog.pg_stat_get_blocks_fetched(C.oid) -
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_read,
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_hit
  FROM pg_catalog.pg_class C
       LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S' or C.relkind = 'L';

-- 6. 恢复 pg_catalog.pg_gtt_attached_pids 视图至原始定义（只过滤 'r'、'S'、'L'）
CREATE OR REPLACE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS relid,
    array(select pid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS pids,
    array(select sessionid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS sessionids
 FROM
     pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r', 'S', 'L');

-- 7. 老版本上再创建序列coverage.proc_coverage_coverage_id_seq、表coverage.proc_coverage
DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='proc_coverage_coverage_id_seq' and n.nspname='coverage' and c.relkind ='z') into ans;
    if ans = true THEN
        -- DELETE first
        DROP table IF EXISTS coverage.proc_coverage;
        DROP SEQUENCE IF EXISTS coverage.proc_coverage_coverage_id_seq;
    end if;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4994;
DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='proc_coverage_coverage_id_seq' and n.nspname='coverage') into ans;
    if ans = false THEN
        CREATE SCHEMA IF NOT EXISTS coverage;
        COMMENT ON schema coverage IS 'coverage schema';

        CREATE SEQUENCE IF NOT EXISTS coverage.proc_coverage_coverage_id_seq START 1;
        CREATE unlogged table IF NOT EXISTS coverage.proc_coverage(
            coverage_id bigint NOT NULL DEFAULT nextval('coverage.proc_coverage_coverage_id_seq'::regclass),
            pro_oid oid NOT NULL,
            pro_name text NOT NULL,
            db_name text NOT NULL,
            pro_querys text NOT NULL,
            pro_canbreak bool[] NOT NULL,
            coverage int[] NOT NULL
        ) WITH (orientation=row, compression=no);
        REVOKE ALL on table coverage.proc_coverage FROM public;
    end if;
END$$;

-- 8. 老版本上再创建序列db4ai.snapshot_sequence
DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='snapshot_sequence' and n.nspname='db4ai' and c.relkind ='z') into ans;
    if ans = true THEN
        DROP SEQUENCE IF EXISTS db4ai.snapshot_sequence;
    end if;
END$$;

DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='snapshot_sequence' and n.nspname='db4ai') into ans;
    if ans = false THEN
        CREATE SEQUENCE IF NOT EXISTS db4ai.snapshot_sequence;
        GRANT UPDATE ON db4ai.snapshot_sequence TO PUBLIC;
    end if;
END$$;

--9 恢复 pg_catalog.pg_statio_all_sequences 至原始定义（只过滤 'S'、'L'）
CREATE OR REPLACE VIEW pg_catalog.pg_statio_all_sequences AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_catalog.pg_stat_get_blocks_fetched(C.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_read,
            pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_hit
    FROM pg_catalog.pg_class C
            LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S' or C.relkind = 'L';

--10 恢复 information_schema.usage_privileges 至原始定义（sequences 部分只过滤 'S'、'L'）
SET search_path TO information_schema;
CREATE OR REPLACE VIEW information_schema.usage_privileges AS
    -- collations
    SELECT CAST(u.rolname AS sql_identifier) AS grantor,
           CAST('PUBLIC' AS sql_identifier) AS grantee,
           CAST(pg_catalog.current_database() AS sql_identifier) AS object_catalog,
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
          AND collencoding IN (-1, (SELECT encoding FROM pg_database WHERE datname = pg_catalog.current_database()))
    UNION ALL
    /* domains */
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(pg_catalog.current_database() AS sql_identifier) AS object_catalog,
           CAST(n.nspname AS sql_identifier) AS object_schema,
           CAST(t.typname AS sql_identifier) AS object_name,
           CAST('DOMAIN' AS character_data) AS object_type,
           CAST('USAGE' AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  pg_catalog.pg_has_role(grantee.oid, t.typowner, 'USAGE')
                  OR t.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable
    FROM (
            SELECT oid, typname, typnamespace, typtype, typowner, (pg_catalog.aclexplode(coalesce(typacl, pg_catalog.acldefault('T', typowner)))).* FROM pg_type
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
          AND (pg_catalog.pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_catalog.pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC')
    UNION ALL
    -- foreign-data wrappers
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(pg_catalog.current_database() AS sql_identifier) AS object_catalog,
           CAST('' AS sql_identifier) AS object_schema,
           CAST(fdw.fdwname AS sql_identifier) AS object_name,
           CAST('FOREIGN DATA WRAPPER' AS character_data) AS object_type,
           CAST('USAGE' AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  pg_catalog.pg_has_role(grantee.oid, fdw.fdwowner, 'USAGE')
                  OR fdw.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable
    FROM (
            SELECT fdwname, fdwowner, (pg_catalog.aclexplode(coalesce(fdwacl, pg_catalog.acldefault('F', fdwowner)))).* FROM pg_foreign_data_wrapper
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
          AND (pg_catalog.pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_catalog.pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC')
    UNION ALL
    -- foreign servers
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(pg_catalog.current_database() AS sql_identifier) AS object_catalog,
           CAST('' AS sql_identifier) AS object_schema,
           CAST(srv.srvname AS sql_identifier) AS object_name,
           CAST('FOREIGN SERVER' AS character_data) AS object_type,
           CAST('USAGE' AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  pg_catalog.pg_has_role(grantee.oid, srv.srvowner, 'USAGE')
                  OR srv.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable
    FROM (
            SELECT srvname, srvowner, (pg_catalog.aclexplode(coalesce(srvacl, pg_catalog.acldefault('S', srvowner)))).* FROM pg_foreign_server
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
          AND (pg_catalog.pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_catalog.pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC')
    UNION ALL
    -- sequences
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(pg_catalog.current_database() AS sql_identifier) AS object_catalog,
           CAST(n.nspname AS sql_identifier) AS object_schema,
           CAST(c.relname AS sql_identifier) AS object_name,
           CAST('SEQUENCE' AS character_data) AS object_type,
           CAST('USAGE' AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  pg_catalog.pg_has_role(grantee.oid, c.relowner, 'USAGE')
                  OR c.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable
    FROM (
            SELECT oid, relname, relnamespace, relkind, relowner, (pg_catalog.aclexplode(coalesce(relacl, pg_catalog.acldefault('r', relowner)))).* FROM pg_class
         ) AS c (oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
         pg_namespace n,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)
    WHERE c.relnamespace = n.oid
          AND (c.relkind = 'S' or c.relkind = 'L')
          AND c.grantee = grantee.oid
          AND c.grantor = u_grantor.oid
          AND c.prtype IN ('USAGE')
          AND (pg_catalog.pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_catalog.pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');
GRANT SELECT ON information_schema.usage_privileges TO PUBLIC;
RESET search_path;

-- 11. 升级 db4ai.snapshot_sequence：将旧版 relkind='S' 的序列 DROP 后重建为 'z'
DO $$
DECLARE
    ans boolean;
BEGIN
    SELECT CASE WHEN count(*)=1 THEN true ELSE false END FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
    WHERE c.relname='snapshot_sequence' AND n.nspname='db4ai' AND c.relnamespace=n.oid AND c.relkind='z' INTO ans;
    IF ans = true THEN
        DROP SEQUENCE IF EXISTS db4ai.snapshot_sequence;
        CREATE SEQUENCE IF NOT EXISTS db4ai.snapshot_sequence;
        GRANT UPDATE ON db4ai.snapshot_sequence TO PUBLIC;
    END IF;
END$$;
