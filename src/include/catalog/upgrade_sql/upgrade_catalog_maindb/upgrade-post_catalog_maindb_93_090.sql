DROP TYPE IF EXISTS pg_catalog._pg_lsn CASCADE;
DROP TYPE IF EXISTS pg_catalog.pg_lsn CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3222, 3223, b;
CREATE TYPE pg_catalog.pg_lsn;

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9081;
CREATE FUNCTION pg_catalog.pg_lsn_in (
    cstring
) RETURNS pg_lsn LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_in';
COMMENT ON FUNCTION pg_catalog.pg_lsn_in(cstring) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_out(pg_lsn) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9082;
CREATE FUNCTION pg_catalog.pg_lsn_out (
    pg_lsn
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_out';
COMMENT ON FUNCTION pg_catalog.pg_lsn_out(pg_lsn) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9083;
CREATE FUNCTION pg_catalog.pg_lsn_recv (
    internal
) RETURNS pg_lsn LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_recv';
COMMENT ON FUNCTION pg_catalog.pg_lsn_recv(internal) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_send(pg_lsn) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9084;
CREATE FUNCTION pg_catalog.pg_lsn_send (
    pg_lsn
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_send';
COMMENT ON FUNCTION pg_catalog.pg_lsn_send(pg_lsn) IS 'I/O';

CREATE TYPE pg_catalog.pg_lsn (
    INPUT = pg_lsn_in,
    OUTPUT = pg_lsn_out,
    RECEIVE = pg_lsn_recv,
    SEND = pg_lsn_send,
    INTERNALLENGTH = 8,
    PASSEDBYVALUE,
    ALIGNMENT = double,
    STORAGE = plain,
    CATEGORY = 'U'
);
COMMENT ON TYPE pg_catalog.pg_lsn IS 'PostgreSQL LSN datatype';

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS NVARCHAR2
AS $$  select pg_catalog.nvarchar2in(pg_catalog.timestamp_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INTERVAL)
RETURNS NVARCHAR2
AS $$  select pg_catalog.nvarchar2in(pg_catalog.interval_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(NUMERIC)
RETURNS NVARCHAR2
AS $$ SELECT pg_catalog.nvarchar2in(pg_catalog.numeric_out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT2)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.int2out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT4)
RETURNS NVARCHAR2
AS $$  select pg_catalog.nvarchar2in(pg_catalog.int4out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT8)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.int8out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT4)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.float4out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT8)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.float8out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

-- 1. 更新 pg_sequence_parameters，新增 is_global_cache 输出参数
-- CASCADE 自动删除依赖此函数的 information_schema.sequences 视图
DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_parameters(
    sequence_oid oid,
    OUT start_value int16,
    OUT minimum_value int16,
    OUT maximum_value int16,
    OUT increment int16,
    OUT cycle_option boolean
) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3078;
CREATE OR REPLACE FUNCTION pg_catalog.pg_sequence_parameters(
    sequence_oid oid,
    OUT start_value int16,
    OUT minimum_value int16,
    OUT maximum_value int16,
    OUT increment int16,
    OUT cycle_option boolean,
    OUT is_global_cache boolean
)
RETURNS record LANGUAGE INTERNAL STABLE STRICT NOT FENCED NOT SHIPPABLE AS 'pg_sequence_parameters';
COMMENT ON FUNCTION pg_catalog.pg_sequence_parameters(oid) IS 'sequence parameters, for use by information schema';

-- 2. 更新 pg_sequence_all_parameters，新增 is_global_cache 输出参数
--DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_all_parameters(text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_all_parameters(
    sequence_name text, OUT start_value int16, OUT minimum_value int16, OUT maximum_value int16,
    OUT increment int16, OUT cycle_option boolean, OUT cache_size int16, OUT last_value int16,
    OUT is_called boolean, OUT log_cnt bigint, OUT uuid bigint, OUT last_used_value int16,
    OUT is_exhausted boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8930;
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
    OUT is_exhausted boolean,
    OUT is_global_cache boolean
)
RETURNS record LANGUAGE internal STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_sequence_all_parameters$function$;

COMMENT ON FUNCTION pg_catalog.pg_sequence_all_parameters(
    sequence_name text, OUT start_value int16, OUT minimum_value int16,
    OUT maximum_value int16, OUT increment int16, OUT cycle_option boolean,
    OUT cache_size int16, OUT last_value int16, OUT is_called boolean,
    OUT log_cnt int8, OUT uuid int8, OUT last_used_value int16,
    OUT is_exhausted boolean, OUT is_global_cache boolean) IS 'sequence all parameters';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-- 3. 重建 information_schema.sequences 视图，新增 'z'、'Z' relkind 以支持 GSC 序列
SET search_path TO information_schema;
CREATE OR REPLACE VIEW information_schema.sequences AS
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
           CAST(CASE WHEN (pg_catalog.pg_sequence_parameters(c.oid)).cycle_option THEN 'YES' ELSE 'NO' END AS yes_or_no) AS cycle_option,
           CAST(CASE WHEN (pg_catalog.pg_sequence_parameters(c.oid)).is_global_cache THEN 'YES' ELSE 'NO' END AS yes_or_no) AS global_cache_option
    FROM pg_namespace nc, pg_class c
    WHERE c.relnamespace = nc.oid
          AND (c.relkind = 'L' or c.relkind = 'S' or c.relkind = 'z' or c.relkind = 'Z')
          AND (NOT pg_catalog.pg_is_other_temp_schema(nc.oid))
          AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
               OR pg_catalog.has_sequence_privilege(c.oid, 'SELECT, UPDATE, USAGE') );
GRANT SELECT ON information_schema.sequences TO PUBLIC;
RESET search_path;

-- 4. upgrade views 更新 pg_catalog.pg_seclabels 视图，新增 'z'、'Z' relkind 以支持 GSC 序列
CREATE OR REPLACE VIEW pg_catalog.pg_seclabels AS
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN rel.relkind = 'r' THEN 'table'::text
		 WHEN rel.relkind = 'v' THEN 'view'::text
		 WHEN rel.relkind = 'm' THEN 'materialized view'::text
		 WHEN rel.relkind = 'S' THEN 'sequence'::text
         WHEN rel.relkind = 'L' THEN 'large sequence'::text
         WHEN rel.relkind = 'z' THEN 'sequence'::text
         WHEN rel.relkind = 'Z' THEN 'large sequence'::text
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

-- 5. 更新 dbe_perf.statio_all_sequences 视图，新增 'z'、'Z' relkind 以支持 GSC 序列
CREATE OR REPLACE VIEW dbe_perf.statio_all_sequences AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_catalog.pg_stat_get_blocks_fetched(C.oid) -
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_read,
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_hit
  FROM pg_class C
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S' or C.relkind = 'L' or C.relkind = 'z' or C.relkind = 'Z';

-- 5. 更新 pg_catalog.pg_gtt_attached_pids 视图，新增 'z'、'Z' relkind 以支持 GSC 序列
CREATE OR REPLACE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS relid,
    array(select pid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS pids,
    array(select sessionid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS sessionids
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r', 'S', 'L', 'z', 'Z');

-- 6. 更新 pg_catalog.pg_statio_all_sequences 视图，新增 'z'、'Z' relkind 以支持 GSC 序列
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
    WHERE C.relkind = 'S' or C.relkind = 'L' or C.relkind = 'z' or C.relkind = 'Z';

-- 7. 更新 information_schema.usage_privileges 视图，新增 'z'、'Z' relkind 以支持 GSC 序列
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
    -- domains
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
          AND (c.relkind = 'S' or c.relkind = 'L' or c.relkind = 'z' or c.relkind = 'Z')
          AND c.grantee = grantee.oid
          AND c.grantor = u_grantor.oid
          AND c.prtype IN ('USAGE')
          AND (pg_catalog.pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_catalog.pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');
GRANT SELECT ON information_schema.usage_privileges TO PUBLIC;
RESET search_path;

-- 8. 升级 db4ai.snapshot_sequence：将旧版 relkind='S' 的序列 DROP 后重建为 'z'
DO $$
DECLARE
    ans boolean;
BEGIN
    SELECT CASE WHEN count(*)=1 THEN true ELSE false END FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
    WHERE c.relname='snapshot_sequence' AND n.nspname='db4ai' AND c.relnamespace=n.oid AND c.relkind='S' INTO ans;
    IF ans = true THEN
        DROP SEQUENCE IF EXISTS db4ai.snapshot_sequence;
        CREATE SEQUENCE IF NOT EXISTS db4ai.snapshot_sequence;
        GRANT UPDATE ON db4ai.snapshot_sequence TO PUBLIC;
    END IF;
END$$;
