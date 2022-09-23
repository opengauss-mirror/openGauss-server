GRANT SELECT ON pg_catalog.pg_total_user_resource_info_oid TO public;
GRANT SELECT ON pg_catalog.pg_total_user_resource_info TO public;

-- ----------------------------------------------------------------
-- upgrade gs_paxos_stat_replication
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.gs_paxos_stat_replication(OUT local_role text, OUT peer_role text, OUT local_dcf_role text, OUT peer_dcf_role text, OUT peer_state text, OUT sender_write_location text, OUT sender_commit_location text, OUT sender_flush_location text, OUT sender_replay_location text, OUT receiver_write_location text, OUT receiver_commit_location text, OUT receiver_flush_location text, OUT receiver_replay_location text, OUT sync_percent text, OUT dcf_run_mode int4, OUT channel text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4650;
CREATE FUNCTION pg_catalog.gs_paxos_stat_replication(OUT local_role text, OUT peer_role text, OUT local_dcf_role text, OUT peer_dcf_role text, OUT peer_state text, OUT sender_write_location text, OUT sender_commit_location text, OUT sender_flush_location text, OUT sender_replay_location text, OUT receiver_write_location text, OUT receiver_commit_location text, OUT receiver_flush_location text, OUT receiver_replay_location text, OUT sync_percent text, OUT dcf_run_mode int4, OUT channel text) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 200 as 'gs_paxos_stat_replication';
DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_plancache_clean TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_plancache_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_plancache_clean TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.global_plancache_status TO PUBLIC;
  end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.connect_by_root(text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9351;
CREATE OR REPLACE FUNCTION pg_catalog.connect_by_root(text)
RETURNS text LANGUAGE INTERNAL STABLE STRICT AS 'connect_by_root';

DROP FUNCTION IF EXISTS pg_catalog.sys_connect_by_path(text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9350;
CREATE OR REPLACE FUNCTION pg_catalog.sys_connect_by_path(text, text)
RETURNS text LANGUAGE INTERNAL STABLE STRICT AS 'sys_connect_by_path';
------
-- int16
------
-- drop operators that depends on int16 first.
do $$
BEGIN
    for ans in select case when count(*) = 1 then true else false end as ans from (select typname from pg_type where typname = 'int16' limit 1)
    LOOP
        if ans.ans = true then
            DROP OPERATOR IF EXISTS pg_catalog.=(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.<>(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.<(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.<=(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.>(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.>=(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.+(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.-(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog.*(int16, int16) CASCADE;
            DROP OPERATOR IF EXISTS pg_catalog./(int16, int16) CASCADE;
        end if;
        exit;
    END LOOP;
END$$;

DROP TYPE IF EXISTS pg_catalog.int16 CASCADE;
DROP TYPE IF EXISTS pg_catalog._int16 CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 34, 0, b;
CREATE TYPE pg_catalog.int16;

DROP FUNCTION if EXISTS pg_catalog.int16in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6401;
CREATE OR REPLACE FUNCTION pg_catalog.int16in(cstring)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16in$function$;

DROP FUNCTION if EXISTS pg_catalog.int16out(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6402;
CREATE OR REPLACE FUNCTION pg_catalog.int16out(int16)
 RETURNS cstring
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16out$function$;

DROP FUNCTION if EXISTS pg_catalog.int16recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6403;
CREATE OR REPLACE FUNCTION pg_catalog.int16recv(internal)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16recv$function$;

DROP FUNCTION if EXISTS pg_catalog.int16send(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6404;
CREATE OR REPLACE FUNCTION pg_catalog.int16send(int16)
 RETURNS bytea
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16send$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 34, 1234, b;
create type pg_catalog.int16(
    INPUT=int16in,
    OUTPUT=int16out,
    RECEIVE=int16recv,
    SEND=int16send,
    INTERNALLENGTH=16,
    ALIGNMENT=double,
    CATEGORY=N,
    STORAGE=plain
);

-- int16 casts

DROP FUNCTION if EXISTS pg_catalog.int16(tinyint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6405;
CREATE OR REPLACE FUNCTION pg_catalog.int16(tinyint)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int1_16$function$;

DROP FUNCTION if EXISTS pg_catalog.i16toi1(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6406;
CREATE OR REPLACE FUNCTION pg_catalog.i16toi1(int16)
 RETURNS tinyint
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16_1$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(smallint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6407;
CREATE OR REPLACE FUNCTION pg_catalog.int16(smallint)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int2_16$function$;

DROP FUNCTION if EXISTS pg_catalog.int2(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6408;
CREATE OR REPLACE FUNCTION pg_catalog.int2(int16)
 RETURNS smallint
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16_2$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6409;
CREATE OR REPLACE FUNCTION pg_catalog.int16(integer)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int4_16$function$;

DROP FUNCTION if EXISTS pg_catalog.int4(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6410;
CREATE OR REPLACE FUNCTION pg_catalog.int4(int16)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16_4$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6411;
CREATE OR REPLACE FUNCTION pg_catalog.int16(bigint)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int8_16$function$;

DROP FUNCTION if EXISTS pg_catalog.int8(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6412;
CREATE OR REPLACE FUNCTION pg_catalog.int8(int16)
 RETURNS bigint
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16_8$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(double precision) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6413;
CREATE OR REPLACE FUNCTION pg_catalog.int16(double precision)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$dtoi16$function$;

DROP FUNCTION if EXISTS pg_catalog.float8(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6414;
CREATE OR REPLACE FUNCTION pg_catalog.float8(int16)
 RETURNS double precision
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i16tod$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(real) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6415;
CREATE OR REPLACE FUNCTION pg_catalog.int16(real)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$ftoi16$function$;

DROP FUNCTION if EXISTS pg_catalog.float4(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6416;
CREATE OR REPLACE FUNCTION pg_catalog.float4(int16)
 RETURNS real
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i16tof$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6417;
CREATE OR REPLACE FUNCTION pg_catalog.int16(oid)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$oidtoi16$function$;

DROP FUNCTION if EXISTS pg_catalog.oid(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6418;
CREATE OR REPLACE FUNCTION pg_catalog.oid(int16)
 RETURNS oid
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i16tooid$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6419;
CREATE OR REPLACE FUNCTION pg_catalog.int16(boolean)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bool_int16$function$;

DROP FUNCTION if EXISTS pg_catalog.int16_bool(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6420;
CREATE OR REPLACE FUNCTION pg_catalog.int16_bool(int16)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16_bool$function$;

DROP FUNCTION if EXISTS pg_catalog.int16(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6421;
CREATE OR REPLACE FUNCTION pg_catalog.int16(numeric)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$numeric_int16$function$;

DROP FUNCTION if EXISTS pg_catalog."numeric"(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6422;
CREATE OR REPLACE FUNCTION pg_catalog."numeric"(int16)
 RETURNS numeric
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16_numeric$function$;

DROP CAST IF EXISTS (tinyint AS int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 0;
CREATE CAST (tinyint AS int16)
    WITH FUNCTION pg_catalog.int16(tinyint)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 AS tinyint) CASCADE;
CREATE CAST (int16 AS tinyint)
    WITH FUNCTION pg_catalog.i16toi1(int16)
    AS ASSIGNMENT;

DROP CAST IF EXISTS (smallint AS int16) CASCADE;
CREATE CAST (smallint AS int16)
    WITH FUNCTION pg_catalog.int16(smallint)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as smallint) CASCADE;
CREATE CAST (int16 as smallint)
    WITH FUNCTION pg_catalog.int2(int16)
    AS ASSIGNMENT;

DROP CAST IF EXISTS (integer as int16) CASCADE;
CREATE CAST (integer as int16)
    WITH FUNCTION pg_catalog.int16(integer)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as integer) CASCADE;
CREATE CAST (int16 as integer)
    WITH FUNCTION pg_catalog.int4(int16)
    AS ASSIGNMENT;

DROP CAST IF EXISTS (bigint as int16) CASCADE;
CREATE CAST (bigint as int16)
    WITH FUNCTION pg_catalog.int16(bigint)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as bigint) CASCADE;
CREATE CAST (int16 as bigint)
    WITH FUNCTION pg_catalog.int8(int16)
    AS ASSIGNMENT;

DROP CAST IF EXISTS (double precision as int16) CASCADE;
CREATE CAST (double precision as int16)
    WITH FUNCTION pg_catalog.int16(double precision)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as double precision) CASCADE;
CREATE CAST (int16 as double precision)
    WITH FUNCTION pg_catalog.float8(int16)
    AS IMPLICIT;

DROP CAST IF EXISTS (real as int16) CASCADE;
CREATE CAST (real as int16)
    WITH FUNCTION pg_catalog.int16(real)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as real) CASCADE;
CREATE CAST (int16 as real)
    WITH FUNCTION pg_catalog.float4(int16)
    AS IMPLICIT;

DROP CAST IF EXISTS (oid as int16) CASCADE;
CREATE CAST (oid as int16)
    WITH FUNCTION pg_catalog.int16(oid)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as oid) CASCADE;
CREATE CAST (int16 as oid)
    WITH FUNCTION pg_catalog.oid(int16)
    AS IMPLICIT;

DROP CAST IF EXISTS (boolean as int16) CASCADE;
CREATE CAST (boolean as int16)
    WITH FUNCTION pg_catalog.int16(boolean)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as boolean) CASCADE;
CREATE CAST (int16 as boolean)
    WITH FUNCTION pg_catalog.int16_bool(int16)
    AS IMPLICIT;

DROP CAST IF EXISTS (numeric as int16) CASCADE;
CREATE CAST (numeric as int16)
    WITH FUNCTION pg_catalog.int16(numeric)
    AS IMPLICIT;

DROP CAST IF EXISTS (int16 as numeric) CASCADE;
CREATE CAST (int16 as numeric)
    WITH FUNCTION pg_catalog."numeric"(int16)
    AS IMPLICIT;

-- int16 operator

DROP FUNCTION if EXISTS pg_catalog.int16eq(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6423;
CREATE OR REPLACE FUNCTION pg_catalog.int16eq(int16, int16)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16eq$function$;

DROP FUNCTION if EXISTS pg_catalog.int16ne(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6424;
CREATE OR REPLACE FUNCTION pg_catalog.int16ne(int16, int16)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16ne$function$;

DROP FUNCTION if EXISTS pg_catalog.int16lt(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6425;
CREATE OR REPLACE FUNCTION pg_catalog.int16lt(int16, int16)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16lt$function$;

DROP FUNCTION if EXISTS pg_catalog.int16le(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6426;
CREATE OR REPLACE FUNCTION pg_catalog.int16le(int16, int16)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16le$function$;

DROP FUNCTION if EXISTS pg_catalog.int16gt(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6427;
CREATE OR REPLACE FUNCTION pg_catalog.int16gt(int16, int16)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16gt$function$;

DROP FUNCTION if EXISTS pg_catalog.int16ge(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6428;
CREATE OR REPLACE FUNCTION pg_catalog.int16ge(int16, int16)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16ge$function$;

DROP FUNCTION if EXISTS pg_catalog.int16pl(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6429;
CREATE OR REPLACE FUNCTION pg_catalog.int16pl(int16, int16)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16pl$function$;

DROP FUNCTION if EXISTS pg_catalog.int16mi(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6430;
CREATE OR REPLACE FUNCTION pg_catalog.int16mi(int16, int16)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16mi$function$;

DROP FUNCTION if EXISTS pg_catalog.int16mul(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6431;
CREATE OR REPLACE FUNCTION pg_catalog.int16mul(int16, int16)
 RETURNS int16
 LANGUAGE internal
 IMMUTABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$int16mul$function$;

DROP FUNCTION if EXISTS pg_catalog.int16div(int16, int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6432;
CREATE OR REPLACE FUNCTION pg_catalog.int16div(int16, int16)
 RETURNS double precision
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$int16div$function$;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6000;
CREATE OPERATOR pg_catalog.= (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16eq,
    --COMMUTATOR = =,
    --NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6001;
CREATE OPERATOR pg_catalog.<> (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16ne,
    --COMMUTATOR = <>,
    --NEGATOR = =,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6002;
CREATE OPERATOR pg_catalog.< (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16lt,
    --COMMUTATOR = >,
    --NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6003;
CREATE OPERATOR pg_catalog.<= (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16le,
    --COMMUTATOR = >=,
    --NEGATOR = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6004;
CREATE OPERATOR pg_catalog.> (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16gt,
    --COMMUTATOR = <,
    --NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6005;
CREATE OPERATOR pg_catalog.>= (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16ge,
    --COMMUTATOR = <=,
    --NEGATOR = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6006;
CREATE OPERATOR pg_catalog.+ (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16pl
    --COMMUTATOR = +
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6007;
CREATE OPERATOR pg_catalog.- (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16mi
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6008;
CREATE OPERATOR pg_catalog.* (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16mul
    --COMMUTATOR = *
);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 6009;
CREATE OPERATOR pg_catalog./ (
    LEFTARG = int16,
    RIGHTARG = int16,
    PROCEDURE = int16div
);

UPDATE pg_operator SET oprnegate = 6001 WHERE oid = 6000;
UPDATE pg_operator SET oprnegate = 6000 WHERE oid = 6001;
UPDATE pg_operator SET oprnegate = 6005 WHERE oid = 6002;
UPDATE pg_operator SET oprnegate = 6004 WHERE oid = 6003;
UPDATE pg_operator SET oprnegate = 6003 WHERE oid = 6004;
UPDATE pg_operator SET oprnegate = 6002 WHERE oid = 6005;
UPDATE pg_operator SET oprcom = 6000 WHERE oid = 6000;
UPDATE pg_operator SET oprcom = 6001 WHERE oid = 6001;
UPDATE pg_operator SET oprcom = 6004 WHERE oid = 6002;
UPDATE pg_operator SET oprcom = 6005 WHERE oid = 6003;
UPDATE pg_operator SET oprcom = 6002 WHERE oid = 6004;
UPDATE pg_operator SET oprcom = 6003 WHERE oid = 6005;
UPDATE pg_operator SET oprcom = 6006 WHERE oid = 6006;
UPDATE pg_operator SET oprcom = 6008 WHERE oid = 6008;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

----
-- sys views
----
CREATE OR REPLACE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS relid,
    array(select pid from pg_gtt_attached_pid(c.oid)) AS pids
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r', 'S', 'L');

CREATE OR REPLACE VIEW pg_catalog.pg_seclabels AS
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN rel.relkind = 'r' THEN 'table'::text
		 WHEN rel.relkind = 'v' THEN 'view'::text
		 WHEN rel.relkind = 'm' THEN 'materialized view'::text
		 WHEN rel.relkind = 'S' THEN 'sequence'::text
         WHEN rel.relkind = 'L' THEN 'large sequence'::text
		 WHEN rel.relkind = 'f' THEN 'foreign table'::text END AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_table_is_visible(rel.oid)
	     THEN quote_ident(rel.relname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
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
	CASE WHEN pg_table_is_visible(rel.oid)
	     THEN quote_ident(rel.relname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
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
	CASE WHEN pg_function_is_visible(pro.oid)
	     THEN quote_ident(pro.proname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(pro.proname)
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
	CASE WHEN pg_type_is_visible(typ.oid)
	THEN quote_ident(typ.typname)
	ELSE quote_ident(nsp.nspname) || '.' || quote_ident(typ.typname)
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
	quote_ident(lan.lanname) AS objname,
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
	quote_ident(nsp.nspname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_namespace nsp ON l.classoid = nsp.tableoid AND l.objoid = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'database'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(dat.datname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_database dat ON l.classoid = dat.tableoid AND l.objoid = dat.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'tablespace'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(spc.spcname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_tablespace spc ON l.classoid = spc.tableoid AND l.objoid = spc.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'role'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(rol.rolname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_authid rol ON l.classoid = rol.tableoid AND l.objoid = rol.oid;
GRANT SELECT ON pg_catalog.pg_seclabels TO PUBLIC;

CREATE OR REPLACE VIEW pg_catalog.pg_statio_all_sequences AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_stat_get_blocks_fetched(C.oid) -
                    pg_stat_get_blocks_hit(C.oid) AS blks_read,
            pg_stat_get_blocks_hit(C.oid) AS blks_hit
    FROM pg_class C
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S' or C.relkind = 'L';
GRANT SELECT ON pg_catalog.pg_statio_all_sequences TO PUBLIC;

CREATE OR REPLACE VIEW pg_catalog.pg_statio_sys_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';
GRANT SELECT ON pg_catalog.pg_statio_sys_sequences TO PUBLIC;

CREATE OR REPLACE VIEW pg_catalog.pg_statio_user_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_local_rto_and_rpo_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5077;
GRANT SELECT ON pg_catalog.pg_statio_user_sequences TO PUBLIC;

CREATE FUNCTION pg_catalog.gs_hadr_local_rto_and_rpo_stat
(
OUT hadr_sender_node_name pg_catalog.text,
OUT hadr_receiver_node_name pg_catalog.text,
OUT source_ip pg_catalog.text,
OUT source_port pg_catalog.int4,
OUT dest_ip pg_catalog.text,
OUT dest_port pg_catalog.int4,
OUT current_rto pg_catalog.int8,
OUT target_rto pg_catalog.int8,
OUT current_rpo pg_catalog.int8,
OUT target_rpo pg_catalog.int8,
OUT current_sleep_time pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_hadr_local_rto_and_rpo_stat';

DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_remote_rto_and_rpo_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5078;
CREATE FUNCTION pg_catalog.gs_hadr_remote_rto_and_rpo_stat
(
OUT hadr_sender_node_name pg_catalog.text,
OUT hadr_receiver_node_name pg_catalog.text,
OUT source_ip pg_catalog.text,
OUT source_port pg_catalog.int4,
OUT dest_ip pg_catalog.text,
OUT dest_port pg_catalog.int4,
OUT current_rto pg_catalog.int8,
OUT target_rto pg_catalog.int8,
OUT current_rpo pg_catalog.int8,
OUT target_rpo pg_catalog.int8,
OUT current_sleep_time pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_hadr_remote_rto_and_rpo_stat';

CREATE OR REPLACE VIEW DBE_PERF.global_streaming_hadr_rto_and_rpo_stat AS
    SELECT hadr_sender_node_name, hadr_receiver_node_name, current_rto, target_rto, current_rpo, target_rpo, current_sleep_time
FROM pg_catalog.gs_hadr_local_rto_and_rpo_stat();

REVOKE ALL on DBE_PERF.global_streaming_hadr_rto_and_rpo_stat FROM PUBLIC;
DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_streaming_hadr_rto_and_rpo_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/
GRANT SELECT ON TABLE DBE_PERF.global_streaming_hadr_rto_and_rpo_stat TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION IF EXISTS pg_catalog.trunc(timestamp with time zone, text) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1751;

CREATE OR REPLACE FUNCTION pg_catalog.trunc(timestamp with time zone, text)
 RETURNS timestamp with time zone
 LANGUAGE internal
 STABLE STRICT NOT FENCED SHIPPABLE
AS $function$timestamptz_trunc_alias$function$;


DROP FUNCTION IF EXISTS pg_catalog.trunc(interval, text) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1752;

CREATE OR REPLACE FUNCTION pg_catalog.trunc(interval, text)
 RETURNS interval
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$interval_trunc_alias$function$;


DROP FUNCTION IF EXISTS pg_catalog.trunc(timestamp without time zone, text) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1753;

CREATE OR REPLACE FUNCTION pg_catalog.trunc(timestamp without time zone, text)
 RETURNS timestamp without time zone
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED SHIPPABLE
AS $function$timestamp_trunc_alias$function$;


DROP FUNCTION IF EXISTS pg_catalog.replace(text, text) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2187;

CREATE OR REPLACE FUNCTION pg_catalog.replace(text, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$replace_text_with_two_args$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;DROP FUNCTION IF EXISTS pg_catalog.local_single_flush_dw_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4375;
CREATE FUNCTION pg_catalog.local_single_flush_dw_stat
(
OUT node_name pg_catalog.text,
OUT curr_dwn pg_catalog.text,
OUT curr_start_page pg_catalog.text,
OUT total_writes pg_catalog.text,
OUT file_trunc_num pg_catalog.text,
OUT file_reset_num pg_catalog.text
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_single_flush_dw_stat';

DROP FUNCTION IF EXISTS pg_catalog.remote_single_flush_dw_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4376;
CREATE FUNCTION pg_catalog.remote_single_flush_dw_stat
(
OUT node_name pg_catalog.text,
OUT curr_dwn pg_catalog.text,
OUT curr_start_page pg_catalog.text,
OUT total_writes pg_catalog.text,
OUT file_trunc_num pg_catalog.text,
OUT file_reset_num pg_catalog.text
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_single_flush_dw_stat';

DROP VIEW IF EXISTS DBE_PERF.global_single_flush_dw_status CASCADE;
CREATE OR REPLACE VIEW DBE_PERF.global_single_flush_dw_status AS
    SELECT node_name, curr_dwn, curr_start_page, total_writes, file_trunc_num, file_reset_num
    FROM pg_catalog.local_single_flush_dw_stat();

REVOKE ALL on DBE_PERF.global_single_flush_dw_status FROM PUBLIC;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_single_flush_dw_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

GRANT SELECT ON TABLE DBE_PERF.global_single_flush_dw_status TO PUBLIC;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
-- ----------------------------------------------------------------
-- upgrade array interface of pg_catalog
-- ----------------------------------------------------------------

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6012;
CREATE OR REPLACE FUNCTION pg_catalog.array_delete(anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_delete$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6009;
CREATE OR REPLACE FUNCTION pg_catalog.array_exists(anyarray, integer)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_exists$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6010;
CREATE OR REPLACE FUNCTION pg_catalog.array_next(anyarray, integer)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_next$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6011;
CREATE OR REPLACE FUNCTION pg_catalog.array_prior(anyarray, integer)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_prior$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6014;
CREATE OR REPLACE FUNCTION pg_catalog.array_extendnull(anyarray, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_extend$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP FUNCTION IF EXISTS pg_catalog.gs_streaming_dr_get_switchover_barrier() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9139;
CREATE OR REPLACE FUNCTION pg_catalog.gs_streaming_dr_get_switchover_barrier
(  OUT get_switchover_barrier boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_streaming_dr_get_switchover_barrier';

DROP FUNCTION IF EXISTS pg_catalog.gs_streaming_dr_in_switchover() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9140;
CREATE OR REPLACE FUNCTION pg_catalog.gs_streaming_dr_in_switchover
(  OUT is_in_switchover boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_streaming_dr_in_switchover';

DROP FUNCTION IF EXISTS pg_catalog.gs_streaming_dr_service_truncation_check() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9141;
CREATE OR REPLACE FUNCTION pg_catalog.gs_streaming_dr_service_truncation_check
(  OUT complete_truncation boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_streaming_dr_service_truncation_check';


DROP FUNCTION IF EXISTS dbe_pldebugger.backtrace(OUT frameno integer, OUT funcname text, OUT lineno integer, OUT query text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1510;
CREATE OR REPLACE FUNCTION dbe_pldebugger.backtrace(OUT frameno integer, OUT funcname text, OUT lineno integer, OUT query text, OUT funcoid oid)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$debug_client_backtrace$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1517;
CREATE OR REPLACE FUNCTION dbe_pldebugger.disable_breakpoint(integer)
 RETURNS boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$debug_client_disable_breakpoint$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1516;
CREATE OR REPLACE FUNCTION dbe_pldebugger.enable_breakpoint(integer)
 RETURNS boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$debug_client_enable_breakpoint$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1518;
CREATE OR REPLACE FUNCTION dbe_pldebugger.finish(OUT funcoid oid, OUT funcname text, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_client_finish$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.info_breakpoints(OUT breakpointno integer, OUT funcoid oid, OUT lineno integer, OUT query text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1509;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_breakpoints(OUT breakpointno integer, OUT funcoid oid, OUT lineno integer, OUT query text, OUT enable boolean)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$debug_client_info_breakpoints$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1520;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_locals(frameno integer, OUT varname text, OUT vartype text, OUT value text, OUT package_name text, OUT isconst boolean)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$debug_client_local_variables_frame$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1521;
CREATE OR REPLACE FUNCTION dbe_pldebugger.print_var(var_name text, frameno integer, OUT varname text, OUT vartype text, OUT value text, OUT package_name text, OUT isconst boolean)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_client_print_variables_frame$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1519;
CREATE OR REPLACE FUNCTION dbe_pldebugger.set_var(text, text)
 RETURNS SETOF boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_client_set_variable$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8700;
CREATE OR REPLACE FUNCTION pg_catalog.array_cat_distinct(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_cat_distinct$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8701;
CREATE OR REPLACE FUNCTION pg_catalog.array_intersect(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_intersect$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8702;
CREATE OR REPLACE FUNCTION pg_catalog.array_intersect_distinct(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_intersect_distinct$function$;


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8703;
CREATE OR REPLACE FUNCTION pg_catalog.array_except(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_except$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8704;
CREATE OR REPLACE FUNCTION pg_catalog.array_except_distinct(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_except_distinct$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP FUNCTION IF EXISTS pg_catalog.numeric(boolean);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6433;
CREATE OR REPLACE FUNCTION pg_catalog.numeric(boolean)
 RETURNS numeric
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bool_numeric$function$;

DROP FUNCTION IF EXISTS pg_catalog.numeric_bool(numeric);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6434;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_bool(numeric)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$numeric_bool$function$;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 0;

DROP CAST IF EXISTS (boolean AS numeric) CASCADE;
CREATE CAST (boolean AS numeric)
    WITH FUNCTION pg_catalog.numeric(boolean)
    AS IMPLICIT;

DROP CAST IF EXISTS (numeric AS boolean) CASCADE;
CREATE CAST (numeric AS boolean)
    WITH FUNCTION pg_catalog.numeric_bool(numeric)
    AS IMPLICIT;

--drop views that reference pg_sequence_parameters
DROP VIEW IF EXISTS information_schema.sequences;

DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_parameters(sequence_oid oid, OUT start_value bigint, OUT minimum_value bigint, OUT maximum_value bigint, OUT increment bigint, OUT cycle_option boolean);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3078;
CREATE OR REPLACE FUNCTION pg_catalog.pg_sequence_parameters(sequence_oid oid, OUT start_value int16, OUT minimum_value int16, OUT maximum_value int16, OUT increment int16, OUT cycle_option boolean)
 RETURNS record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_sequence_parameters$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

SET search_path = dbe_perf;
CREATE OR REPLACE VIEW dbe_perf.statio_all_sequences AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_stat_get_blocks_fetched(C.oid) -
    pg_stat_get_blocks_hit(C.oid) AS blks_read,
    pg_stat_get_blocks_hit(C.oid) AS blks_hit
  FROM pg_class C
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S' or C.relkind = 'L';

SET search_path = information_schema;
CREATE OR REPLACE VIEW information_schema.sequences AS
    SELECT CAST(current_database() AS sql_identifier) AS sequence_catalog,
           CAST(nc.nspname AS sql_identifier) AS sequence_schema,
           CAST(c.relname AS sql_identifier) AS sequence_name,
           CAST('int16' AS character_data) AS data_type,
           CAST(128 AS cardinal_number) AS numeric_precision,
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
          AND (c.relkind = 'L' or c.relkind = 'S')
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
          AND (c.relkind = 'S' or c.relkind = 'L')
          AND c.grantee = grantee.oid
          AND c.grantor = u_grantor.oid
          AND c.prtype IN ('USAGE')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

GRANT SELECT ON information_schema.usage_privileges TO PUBLIC;
RESET search_path;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP TYPE IF EXISTS pg_catalog.bulk_exception;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
CREATE TYPE pg_catalog.bulk_exception as (error_index integer, error_code integer, error_message text);-- ----------------------------------------------------------------
-- upgrade array interface of pg_catalog
-- ----------------------------------------------------------------

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6013;
CREATE OR REPLACE FUNCTION pg_catalog.array_trim(anyarray, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_trim$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DROP FUNCTION IF EXISTS pg_catalog.copy_summary_create() CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.copy_summary_create()
RETURNS bool
AS $$
DECLARE
	BEGIN
        EXECUTE 'CREATE TABLE public.gs_copy_summary
                (relname varchar, begintime timestamptz, endtime timestamptz, 
                id bigint, pid bigint, readrows bigint, skiprows bigint, loadrows bigint, errorrows bigint, whenrows bigint, allnullrows bigint, detail text);';

        EXECUTE 'CREATE INDEX gs_copy_summary_idx on public.gs_copy_summary (id);';

		return true;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION IF EXISTS pg_catalog.local_candidate_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4377;
CREATE FUNCTION pg_catalog.local_candidate_stat
(
OUT node_name pg_catalog.text,
OUT candidate_slots pg_catalog.int4,
OUT get_buf_from_list pg_catalog.int8,
OUT get_buf_clock_sweep pg_catalog.int8,
OUT seg_candidate_slots pg_catalog.int4,
OUT seg_get_buf_from_list pg_catalog.int8,
OUT seg_get_buf_clock_sweep pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_candidate_stat';

DROP FUNCTION IF EXISTS pg_catalog.remote_candidate_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4386;
CREATE FUNCTION pg_catalog.remote_candidate_stat
(
OUT node_name pg_catalog.text,
OUT candidate_slots pg_catalog.int4,
OUT get_buf_from_list pg_catalog.int8,
OUT get_buf_clock_sweep pg_catalog.int8,
OUT seg_candidate_slots pg_catalog.int4,
OUT seg_get_buf_from_list pg_catalog.int8,
OUT seg_get_buf_clock_sweep pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_candidate_stat';

CREATE OR REPLACE VIEW DBE_PERF.global_candidate_status AS
    SELECT node_name, candidate_slots, get_buf_from_list, get_buf_clock_sweep, seg_candidate_slots, seg_get_buf_from_list, seg_get_buf_clock_sweep
    FROM pg_catalog.local_candidate_stat();

REVOKE ALL on DBE_PERF.global_candidate_status FROM PUBLIC;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_candidate_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

GRANT SELECT ON TABLE DBE_PERF.global_candidate_status TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
-- Ammend sequence related function not properly updated
DROP FUNCTION if EXISTS pg_catalog.currval(regclass);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1575;
CREATE FUNCTION pg_catalog.currval(regclass)
 RETURNS numeric
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$currval_oid$function$;

DROP FUNCTION if EXISTS pg_catalog.lastval();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2559;
CREATE FUNCTION pg_catalog.lastval()
 RETURNS numeric
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$lastval$function$;

DROP FUNCTION IF EXISTS pg_catalog.nextval(regclass);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1574;
CREATE FUNCTION pg_catalog.nextval(regclass)
 RETURNS numeric
 LANGUAGE internal
 STRICT NOT FENCED SHIPPABLE
AS $function$nextval_oid$function$;

DROP FUNCTION IF EXISTS pg_catalog.setval(regclass, bigint);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1576;
CREATE FUNCTION pg_catalog.setval(regclass, numeric)
 RETURNS numeric
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$setval_oid$function$;

DROP FUNCTION IF EXISTS pg_catalog.setval(regclass, bigint, boolean);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1765;
CREATE FUNCTION pg_catalog.setval(regclass, numeric, boolean)
 RETURNS numeric
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

comment on function PG_CATALOG.abbrev(inet) is 'abbreviated display of inet value';
comment on function PG_CATALOG.abbrev(cidr) is 'abbreviated display of cidr value';
comment on function PG_CATALOG.abs(double precision) is 'absolute value';
comment on function PG_CATALOG.abs(real) is 'absolute value';
comment on function PG_CATALOG.abs(bigint) is 'absolute value';
comment on function PG_CATALOG.abs(integer) is 'absolute value';
comment on function PG_CATALOG.abs(smallint) is 'absolute value';
comment on function PG_CATALOG.aclcontains(aclitem[], aclitem) is 'contains';
comment on function PG_CATALOG.acldefault("char", oid) is 'show hardwired default privileges, primarily for use by the information schema';
comment on function PG_CATALOG.aclexplode(acl aclitem[]) is 'convert ACL item array to table, primarily for use by information schema';
comment on function PG_CATALOG.aclinsert(aclitem[], aclitem) is 'I/O';
comment on function PG_CATALOG.aclitemin(cstring) is 'I/O';
comment on function PG_CATALOG.aclitemout(aclitem) is 'I/O';
comment on function PG_CATALOG.aclremove(aclitem[], aclitem) is 'remove ACL item';
comment on function PG_CATALOG.acos(double precision) is 'arccosine';
comment on function PG_CATALOG.age(xid) is 'age of a transaction ID, in transactions before current transaction';
comment on function PG_CATALOG.age(timestamp without time zone) is 'date difference from today preserving months and years';
comment on function PG_CATALOG.age(timestamp with time zone) is 'date difference from today preserving months and years';
comment on function PG_CATALOG.age(timestamp without time zone, timestamp without time zone) is 'date difference preserving months and years';
comment on function PG_CATALOG.any_in(cstring) is 'I/O';
comment on function PG_CATALOG.any_out("any") is 'I/O';
comment on function PG_CATALOG.anyarray_in(cstring) is 'I/O';
comment on function PG_CATALOG.anyarray_out(anyarray) is 'I/O';
comment on function PG_CATALOG.anyarray_recv(internal) is 'I/O';
comment on function PG_CATALOG.anyarray_send(anyarray) is 'I/O';
comment on function PG_CATALOG.anyelement_in(cstring) is 'I/O';
comment on function PG_CATALOG.anyelement_out(anyelement) is 'I/O';
comment on function PG_CATALOG.anyenum_in(cstring) is 'I/O';
comment on function PG_CATALOG.anyenum_out(anyenum) is 'I/O';
comment on function PG_CATALOG.anynonarray_in(cstring) is 'I/O';
comment on function PG_CATALOG.anynonarray_out(anynonarray) is 'I/O';
comment on function PG_CATALOG.anyrange_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.anyrange_out(anyrange) is 'I/O';
comment on function PG_CATALOG.area(box) is 'box area';
comment on function PG_CATALOG.area(path) is 'area of a closed path';
comment on function PG_CATALOG.area(circle) is 'area of circle';
comment on function PG_CATALOG.areajoinsel(internal, oid, internal, smallint, internal) is 'join selectivity for area-comparison operators';
comment on function PG_CATALOG.areasel(internal, oid, internal, integer) is 'restriction selectivity for area-comparison operators';
comment on function PG_CATALOG.array_agg(anyelement) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.array_agg_finalfn(internal) is 'aggregate final function';
comment on function PG_CATALOG.array_agg_transfn(internal, anyelement) is 'aggregate transition function';
comment on function PG_CATALOG.array_dims(anyarray) is 'array dimensions';
comment on function PG_CATALOG.array_fill(anyelement, integer[]) is 'array constructor with value';
comment on function PG_CATALOG.array_fill(anyelement, integer[], integer[]) is 'array constructor with value';
comment on function PG_CATALOG.array_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.array_larger(anyarray, anyarray) is 'larger of two';
comment on function PG_CATALOG.array_length(anyarray, integer) is 'array length';
comment on function PG_CATALOG.array_lower(anyarray, integer) is 'array lower dimension';
comment on function PG_CATALOG.array_ndims(anyarray) is 'number of array dimensions';
comment on function PG_CATALOG.array_out(anyarray) is 'I/O';
comment on function PG_CATALOG.array_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.array_send(anyarray) is 'I/O';
comment on function PG_CATALOG.array_smaller(anyarray, anyarray) is 'smaller of two';
comment on function PG_CATALOG.array_to_json(anyarray) is 'map array to json';
comment on function PG_CATALOG.array_to_json(anyarray, boolean) is 'map array to json with optional pretty printing';
comment on function PG_CATALOG.array_to_string(anyarray, text, text) is 'concatenate array elements, using delimiter and null string, into text';
comment on function PG_CATALOG.array_to_string(anyarray, text) is 'concatenate array elements, using delimiter, into text';
comment on function PG_CATALOG.array_typanalyze(internal) is 'array typanalyze';
comment on function PG_CATALOG.array_upper(anyarray, integer) is 'array upper dimension';
comment on function PG_CATALOG.arraycontjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity for array-containment operators';
comment on function PG_CATALOG.arraycontsel(internal, oid, internal, integer) is 'restriction selectivity for array-containment operators';
comment on function PG_CATALOG.ascii(text) is 'convert first char to int4';
comment on function PG_CATALOG.asin(double precision) is 'arcsine';
comment on function PG_CATALOG.atan(double precision) is 'arctangent';
comment on function PG_CATALOG.atan2(double precision, double precision) is 'arctangent, two arguments';
comment on function PG_CATALOG.avg(bigint) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.avg(double precision) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.avg(integer) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.avg(interval) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.avg(numeric) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.avg(real) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.avg(smallint) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.avg(tinyint) is 'concatenate aggregate input into an array';
comment on function PG_CATALOG.bit(bigint, integer) is 'convert int8 to bitstring';
comment on function PG_CATALOG.bit(bit, integer, boolean) is 'adjust bit() to typmod length';
comment on function PG_CATALOG.bit(integer, integer ) is 'convert int4 to bitstring';
comment on function PG_CATALOG.bit_and(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_and(bit) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_and(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_and(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_and(tinyint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.bit_length(bit) is 'length in bits';
comment on function PG_CATALOG.bit_length(bytea) is 'length in bits';
comment on function PG_CATALOG.bit_length(text) is 'length in bits';
comment on function PG_CATALOG.bit_or(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_or(bit) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_or(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_or(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_or(tinyint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bit_out(bit) is 'I/O';
comment on function PG_CATALOG.bit_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.bit_send(bit) is 'I/O';
comment on function PG_CATALOG.bitcmp(bit, bit) is 'less-equal-greater';
comment on function PG_CATALOG.bittypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.bittypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.bool(integer) is 'convert int4 to boolean';
comment on function PG_CATALOG.bool_and(boolean) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.bool_or(boolean) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.booland_statefunc() is 'aggregate transition function';
comment on function PG_CATALOG.boolin(cstring) is 'I/O';
comment on function PG_CATALOG.boolor_statefunc(boolean, boolean) is 'aggregate transition function';
comment on function PG_CATALOG.boolout(boolean) is 'I/O';
comment on function PG_CATALOG.boolrecv(internal) is 'I/O';
comment on function PG_CATALOG.boolsend(boolean) is 'I/O';
comment on function PG_CATALOG.box(circle) is 'convert circle to box';
comment on function PG_CATALOG.box(point, point) is 'convert points to box';
comment on function PG_CATALOG.box(polygon) is 'convert polygon to bounding box';
comment on function PG_CATALOG.box_center(box) is 'center of';
comment on function PG_CATALOG.box_in(cstring) is 'I/O';
comment on function PG_CATALOG.box_out(box) is 'I/O';
comment on function PG_CATALOG.box_recv(internal) is 'I/O';
comment on function PG_CATALOG.box_send(box) is 'I/O';
comment on function PG_CATALOG.bpchar(name) is 'convert name to char(n)';
comment on function PG_CATALOG.bpchar(character, integer, boolean) is 'adjust char() to typmod length';
comment on function PG_CATALOG.bpchar("char") is 'convert char to char(n)';
comment on function PG_CATALOG.bpchar_larger(character, character) is 'larger of two';
comment on function PG_CATALOG.bpchar_smaller(character, character) is 'smaller of two';
comment on function PG_CATALOG.bpchar_sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.bpcharin(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.bpcharlike(character, text) is 'matches LIKE expression';
comment on function PG_CATALOG.bpcharnlike(character, text) is 'does not match LIKE expression';
comment on function PG_CATALOG.bpcharout(character) is 'I/O';
comment on function PG_CATALOG.bpcharrecv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.bpcharsend(character) is 'I/O';
comment on function PG_CATALOG.bpchartypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.bpchartypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.broadcast(inet) is 'broadcast address of network';
comment on function PG_CATALOG.btarraycmp(anyarray, anyarray) is 'less-equal-greater';
comment on function PG_CATALOG.btboolcmp(boolean, boolean) is 'less-equal-greater';
comment on function PG_CATALOG.btbpchar_pattern_cmp(character, character) is 'less-equal-greater';
comment on function PG_CATALOG.btcharcmp("char", "char") is 'less-equal-greater';
comment on function PG_CATALOG.btfloat48cmp(real, double precision) is 'less-equal-greater';
comment on function PG_CATALOG.btfloat4cmp(real, real) is 'less-equal-greater';
comment on function PG_CATALOG.btfloat4sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.btfloat84cmp(double precision, real) is 'less-equal-greater';
comment on function PG_CATALOG.btfloat8cmp(double precision, double precision) is 'less-equal-greater';
comment on function PG_CATALOG.btfloat8sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.btint24cmp(smallint, integer) is 'less-equal-greater';
comment on function PG_CATALOG.btint28cmp(smallint, bigint) is 'less-equal-greater';
comment on function PG_CATALOG.btint2cmp(smallint, smallint) is 'less-equal-greater';
comment on function PG_CATALOG.btint2sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.btint42cmp(integer, smallint) is 'less-equal-greater';
comment on function PG_CATALOG.btint48cmp(integer, bigint) is 'less-equal-greater';
comment on function PG_CATALOG.btint4cmp(integer, integer) is 'less-equal-greater';
comment on function PG_CATALOG.btint4sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.btint82cmp(bigint, smallint) is 'less-equal-greater';
comment on function PG_CATALOG.btint84cmp(bigint, integer) is 'less-equal-greater';
comment on function PG_CATALOG.btint8cmp(bigint, bigint) is 'less-equal-greater';
comment on function PG_CATALOG.btint8sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.btnamecmp(name, name) is 'less-equal-greater';
comment on function PG_CATALOG.btnamesortsupport(internal) is 'sort support';
comment on function PG_CATALOG.btoidcmp(oid, oid) is 'less-equal-greater';
comment on function PG_CATALOG.btoidsortsupport(internal) is 'sort support';
comment on function PG_CATALOG.btoidvectorcmp(oidvector, oidvector) is 'less-equal-greater';
comment on function PG_CATALOG.btrecordcmp(record, record) is 'less-equal-greater';
comment on function PG_CATALOG.btrim(text, text) is 'trim selected characters from both ends of string';
comment on function PG_CATALOG.btrim(text) is 'trim spaces from both ends of string';
comment on function PG_CATALOG.btrim(bytea, bytea) is 'trim both ends of string';
comment on function PG_CATALOG.bttext_pattern_cmp(text, text) is 'less-equal-greater';
comment on function PG_CATALOG.bttextcmp(text, text) is 'less-equal-greater';
comment on function PG_CATALOG.bttextsortsupport(internal) is 'sort support';
comment on function PG_CATALOG.bttidcmp(tid, tid) is 'less-equal-greater';
comment on function PG_CATALOG.bytea_sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.bytea_string_agg_finalfn(internal) is 'concatenate aggregate input into a bytea';
comment on function PG_CATALOG.bytea_string_agg_transfn(internal, bytea, bytea) is 'aggregate transition function';
comment on function PG_CATALOG.byteacmp(bytea, bytea) is 'less-equal-greater';
comment on function PG_CATALOG.byteain(cstring) is 'I/O';
comment on function PG_CATALOG.bytealike(bytea, bytea) is 'matches LIKE expression';
comment on function PG_CATALOG.byteanlike(bytea, bytea) is 'does not match LIKE expression';
comment on function PG_CATALOG.byteaout(bytea) is 'I/O';
comment on function PG_CATALOG.cash_cmp(money, money) is 'less-equal-greater';
comment on function PG_CATALOG.cash_in(cstring) is 'I/O';
comment on function PG_CATALOG.cash_out(money) is 'I/O';
comment on function PG_CATALOG.cash_recv(internal) is 'I/O';
comment on function PG_CATALOG.cash_send(money) is 'I/O';
comment on function PG_CATALOG.cash_words(money) is 'output money amount as words';
comment on function PG_CATALOG.cashlarger(money, money) is 'larger of two';
comment on function PG_CATALOG.cashsmaller(money, money) is 'smaller of two';
comment on function PG_CATALOG.cbrt(double precision) is 'cube root';
comment on function PG_CATALOG.ceil(double precision) is 'nearest integer >= value';
comment on function PG_CATALOG.ceil(numeric) is 'nearest integer >= value';
comment on function PG_CATALOG.ceiling(double precision) is 'nearest integer >= value';
comment on function PG_CATALOG.ceiling(numeric) is 'nearest integer >= value';
comment on function PG_CATALOG.center(box) is 'center of';
comment on function PG_CATALOG.center(circle) is 'center of';
comment on function PG_CATALOG.char(text) is 'convert text to char';
comment on function PG_CATALOG.char_length(character) is 'character length';
comment on function PG_CATALOG.char_length(text) is 'length';
comment on function PG_CATALOG.character_length(character) is 'character length';
comment on function PG_CATALOG.character_length(text) is 'length';
comment on function PG_CATALOG.charin(cstring) is 'I/O';
comment on function PG_CATALOG.charout("char") is 'I/O';
comment on function PG_CATALOG.charrecv(internal) is 'I/O';
comment on function PG_CATALOG.charsend("char") is 'I/O';
comment on function PG_CATALOG.checksum(text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.chr(integer) is 'convert int4 to char';
comment on function PG_CATALOG.cidin(cstring) is 'I/O';
comment on function PG_CATALOG.cidout(cid) is 'I/O';
comment on function PG_CATALOG.cidr(inet) is 'convert inet to cidr';
comment on function PG_CATALOG.cidr_in(cstring) is 'I/O';
comment on function PG_CATALOG.cidr_out(cidr) is 'I/O';
comment on function PG_CATALOG.cidr_recv(internal) is 'I/O';
comment on function PG_CATALOG.cidr_send(cidr) is 'I/O';
comment on function PG_CATALOG.cidrecv(internal) is 'I/O';
comment on function PG_CATALOG.cidsend(cid) is 'I/O';
comment on function PG_CATALOG.circle(point, double precision) is 'convert point and radius to circle';
comment on function PG_CATALOG.circle(polygon) is 'convert polygon to circle';
comment on function PG_CATALOG.circle(box) is 'convert box to circle';
comment on function PG_CATALOG.circle_center(circle) is 'center of';
comment on function PG_CATALOG.circle_in(cstring) is 'I/O';
comment on function PG_CATALOG.circle_out(circle) is 'I/O';
comment on function PG_CATALOG.circle_recv(internal) is 'I/O';
comment on function PG_CATALOG.circle_send(circle) is 'I/O';
comment on function PG_CATALOG.clock_timestamp() is 'current clock time';
comment on function PG_CATALOG.col_description(oid, integer) is 'get description for table column';
comment on function PG_CATALOG.concat(VARIADIC "any") is 'concatenate values';
comment on function PG_CATALOG.concat_ws(text, VARIADIC "any") is 'concatenate values with separators';
comment on function PG_CATALOG.contjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity for containment comparison operators';
comment on function PG_CATALOG.contsel(internal, oid, internal, integer) is 'restriction selectivity for containment comparison operators';
comment on function PG_CATALOG.convert(bytea, name, name) is 'convert string with specified encoding names';
comment on function PG_CATALOG.convert_from(bytea, name) is 'convert string with specified source encoding name';
comment on function PG_CATALOG.convert_to(text, name) is 'convert string with specified destination encoding name';
comment on function PG_CATALOG.corr(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.cos(double precision) is 'cosine';
comment on function PG_CATALOG.cot(double precision) is 'cotangent';
comment on function PG_CATALOG.count() is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.count("any") is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.covar_pop(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.covar_samp(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.cstring_in(cstring) is '"I/O';
comment on function PG_CATALOG.cstring_out(cstring) is '"I/O';
comment on function PG_CATALOG.cstring_recv(internal) is '"I/O';
comment on function PG_CATALOG.cstring_send(cstring) is '"I/O';
comment on function PG_CATALOG.cume_dist() is 'fractional row number within partition';
comment on function PG_CATALOG.current_database() is 'name of the current database';
comment on function PG_CATALOG.current_query() is 'get the currently executing query';
comment on function PG_CATALOG.current_schema() is 'current schema name';
comment on function PG_CATALOG.current_schemas(boolean) is 'current schema search list';
comment on function PG_CATALOG.current_setting(text) is 'SHOW X as a function';
comment on function PG_CATALOG.current_user() is 'current user name';
comment on function PG_CATALOG.currtid(oid, tid) is 'latest tid of a tuple';
comment on function PG_CATALOG.currtid2(text, tid) is 'latest tid of a tuple';
comment on function PG_CATALOG.currval(regclass) is 'sequence current value';
comment on function PG_CATALOG.cursor_to_xml(cursor refcursor, count integer, nulls boolean, tableforest boolean, targetns text) is 'map rows from cursor to XML';
comment on function PG_CATALOG.cursor_to_xmlschema(cursor refcursor, nulls boolean, tableforest boolean, targetns text) is 'map cursor structure to XML Schema';
comment on function PG_CATALOG.database_to_xml(nulls boolean, tableforest boolean, targetns text) is 'map database contents to XML';
comment on function PG_CATALOG.database_to_xml_and_xmlschema(nulls boolean, tableforest boolean, targetns text) is 'map database contents and structure to XML and XML Schema';
comment on function PG_CATALOG.database_to_xmlschema(nulls boolean, tableforest boolean, targetns text) is 'map database structure to XML Schema';
comment on function PG_CATALOG.date(timestamp with time zone) is 'convert timestamp with time zone to date';
comment on function PG_CATALOG.date(timestamp without time zone) is 'convert timestamp to date';
comment on function PG_CATALOG.date_in(cstring) is 'I/O';
comment on function PG_CATALOG.date_part(text, timestamp with time zone) is 'extract field from timestamp with time zone';
comment on function PG_CATALOG.date_part(text, interval) is 'extract field from interval';
comment on function PG_CATALOG.date_part(text, time with time zone) is 'extract field from time with time zone';
comment on function PG_CATALOG.date_part(text, date) is 'extract field from date';
comment on function PG_CATALOG.date_part(text, time without time zone) is 'extract field from time';
comment on function PG_CATALOG.date_part(text, timestamp without time zone) is 'extract field from timestamp';
comment on function PG_CATALOG.date_recv(internal) is 'I/O';
comment on function PG_CATALOG.date_sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.date_trunc(text, timestamp with time zone) is 'truncate timestamp with time zone to specified units';
comment on function PG_CATALOG.date_trunc(text, interval) is 'truncate interval to specified units';
comment on function PG_CATALOG.date_trunc(text, timestamp without time zone) is 'truncate timestamp to specified units';
comment on function PG_CATALOG.daterange_canonical(daterange) is 'convert a date range to canonical form';
comment on function PG_CATALOG.decode(text, text) is 'convert ascii-encoded text string into bytea value';
comment on function PG_CATALOG.degrees(double precision) is 'radians to degrees';
comment on function PG_CATALOG.dense_rank() is 'integer rank without gaps';
comment on function PG_CATALOG.dexp(double precision) is 'natural exponential (e^x)';
comment on function PG_CATALOG.diagonal(box) is 'box diagonal';
comment on function PG_CATALOG.diameter(circle) is 'diameter of circle';
comment on function PG_CATALOG.dispell_init(internal) is '(internal)';
comment on function PG_CATALOG.dispell_lexize(internal, internal, internal, internal) is '(internal)';
comment on function PG_CATALOG.div(numeric, numeric) is 'trunc(x/y)';
comment on function PG_CATALOG.dlog1(double precision) is 'natural logarithm';
comment on function PG_CATALOG.dlog10(double precision) is 'base 10 logarithm';
comment on function PG_CATALOG.domain_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.domain_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.dpow(double precision, double precision) is 'exponentiation';
comment on function PG_CATALOG.dround(double precision) is 'round to nearest integer';
comment on function PG_CATALOG.dsimple_init(internal) is '(internal)';
comment on function PG_CATALOG.dsimple_lexize(internal, internal, internal, internal) is '(internal)';
comment on function PG_CATALOG.dsqrt(double precision) is 'square root';
comment on function PG_CATALOG.dsynonym_init(internal) is '(internal)';
comment on function PG_CATALOG.dsynonym_lexize(internal, internal, internal, internal) is '(internal)';
comment on function PG_CATALOG.dtrunc(double precision) is 'truncate to integer';
comment on function PG_CATALOG.encode(bytea, text) is 'convert bytea value into some ascii-only text string';
comment on function PG_CATALOG.enum_cmp(anyenum, anyenum) is 'less-equal-greater';
comment on function PG_CATALOG.enum_first(anyenum) is 'first value of the input enum type';
comment on function PG_CATALOG.enum_in(cstring, oid) is 'I/O';
comment on function PG_CATALOG.enum_larger(anyenum, anyenum) is 'larger of two';
comment on function PG_CATALOG.enum_last(anyenum) is 'last value of the input enum type';
comment on function PG_CATALOG.enum_out(anyenum) is 'I/O';
comment on function PG_CATALOG.enum_range(anyenum, anyenum) is 'range between the two given enum values, as an ordered array';
comment on function PG_CATALOG.enum_range(anyenum) is 'range of the given enum type, as an ordered array';
comment on function PG_CATALOG.enum_recv(cstring, oid) is 'I/O';
comment on function PG_CATALOG.enum_send(anyenum) is 'I/O';
comment on function PG_CATALOG.enum_smaller(anyenum, anyenum) is 'smaller of two';
comment on function PG_CATALOG.eqjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of = and related operators';
comment on function PG_CATALOG.eqsel(internal, oid, internal, integer) is 'restriction selectivity of = and related operators';
comment on function PG_CATALOG.every(boolean) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.exp(double precision) is 'natural exponential (e^x)';
comment on function PG_CATALOG.exp(numeric) is 'natural exponential (e^x)';
comment on function PG_CATALOG.factorial(bigint) is 'implementation of deprecated ! and !! factorial operators';
comment on function PG_CATALOG.family(inet) is 'address family (4 for IPv4, 6 for IPv6)';
comment on function PG_CATALOG.fdw_handler_in(cstring) is 'I/O';
comment on function PG_CATALOG.fdw_handler_out(fdw_handler) is 'I/O';
comment on function PG_CATALOG.first_value(anyelement) is 'fetch the first row value';
comment on function PG_CATALOG.float4(smallint) is 'convert int2 to float4';
comment on function PG_CATALOG.float4(double precision) is 'convert double to float4';
comment on function PG_CATALOG.float4(integer) is 'convert int4 to float4';
comment on function PG_CATALOG.float4(bigint) is 'convert int8 to float4';
comment on function PG_CATALOG.float4(numeric) is 'convert numeric to float4';
comment on function PG_CATALOG.float4_accum(double precision[], real) is 'aggregate transition function';
comment on function PG_CATALOG.float4in(cstring) is 'I/O';
comment on function PG_CATALOG.float4larger(real, real) is 'larger of two';
comment on function PG_CATALOG.float4out(real) is 'I/O';
comment on function PG_CATALOG.float4recv(internal) is 'I/O';
comment on function PG_CATALOG.float4smaller(real, real) is 'smaller of two';
comment on function PG_CATALOG.float4send(real) is 'I/O';
comment on function PG_CATALOG.float8(smallint) is 'convert int2 to float8';
comment on function PG_CATALOG.float8(real) is 'convert float4 to float8';
comment on function PG_CATALOG.float8(bigint) is 'convert int8 to float8';
comment on function PG_CATALOG.float8(numeric) is 'convert numeric to float8';
comment on function PG_CATALOG.float8_accum(double precision[], double precision) is 'aggregate transition function';
comment on function PG_CATALOG.float8_avg(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_corr(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_covar_pop(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_covar_samp(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_accum(double precision[], double precision, double precision) is 'aggregate transition function';
comment on function PG_CATALOG.float8_regr_avgx(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_avgy(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_intercept(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_r2(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_slope(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_sxx(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_sxy(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_regr_syy(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_stddev_pop(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_stddev_samp(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_var_pop(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8_var_samp(double precision[]) is 'aggregate final function';
comment on function PG_CATALOG.float8in(cstring) is 'I/O';
comment on function PG_CATALOG.float8larger(double precision, double precision) is 'larger of two';
comment on function PG_CATALOG.float8out(double precision) is 'I/O';
comment on function PG_CATALOG.float8recv(internal) is 'I/O';
comment on function PG_CATALOG.float8send(double precision) is 'I/O';
comment on function PG_CATALOG.float8smaller(double precision, double precision) is 'smaller of two';
comment on function PG_CATALOG.floor(double precision) is 'nearest integer <= value';
comment on function PG_CATALOG.floor(numeric) is 'nearest integer <= value';
comment on function PG_CATALOG.fmgr_c_validator(oid) is '(internal)';
comment on function PG_CATALOG.fmgr_internal_validator(oid) is '(internal)';
comment on function PG_CATALOG.fmgr_sql_validator(oid) is '(internal)';
comment on function PG_CATALOG.format(text) is 'format text message';
comment on function PG_CATALOG.format(text, VARIADIC "any") is 'format text message';
comment on function PG_CATALOG.format_type(oid, integer) is 'format a type oid and atttypmod to canonical SQL';
comment on function PG_CATALOG.generate_series(bigint, bigint) is 'non-persistent series generator';
comment on function PG_CATALOG.generate_series(bigint, bigint, bigint) is 'non-persistent series generator';
comment on function PG_CATALOG.generate_series(timestamp without time zone, timestamp without time zone, interval) is 'non-persistent series generator';
comment on function PG_CATALOG.generate_series(integer, integer, integer) is 'non-persistent series generator';
comment on function PG_CATALOG.generate_series(numeric, numeric) is 'non-persistent series generator';
comment on function PG_CATALOG.generate_series(numeric, numeric, numeric) is 'non-persistent series generator';
comment on function PG_CATALOG.generate_series(timestamp with time zone, timestamp with time zone, interval) is 'non-persistent series generator';
comment on function PG_CATALOG.generate_subscripts(anyarray, integer) is 'array subscripts generato';
comment on function PG_CATALOG.generate_subscripts(anyarray, integer, boolean) is 'array subscripts generato';
comment on function PG_CATALOG.get_bit(bit, integer) is 'get bit';
comment on function PG_CATALOG.get_bit(bytea, integer) is 'get bit';
comment on function PG_CATALOG.get_byte(bytea, integer) is 'get byte';
comment on function PG_CATALOG.get_current_ts_config() is 'get current tsearch configuration';
comment on function PG_CATALOG.getdatabaseencoding() is 'encoding name of current database';
comment on function PG_CATALOG.gin_clean_pending_list(regclass) is 'clean up GIN pending list';
comment on function PG_CATALOG.gin_cmp_prefix(text, text, smallint, internal) is 'GIN tsvector support';
comment on function PG_CATALOG.gin_cmp_tslexeme(text, text) is 'GIN tsvector support';
comment on function PG_CATALOG.gin_extract_tsquery(tsquery, internal, smallint, internal, internal) is 'GIN tsvector support (obsolete)';
comment on function PG_CATALOG.gin_extract_tsquery(tsquery, internal, smallint, internal, internal, internal, internal) is 'GIN tsvector support';
comment on function PG_CATALOG.gin_extract_tsvector(tsvector, internal) is 'GIN tsvector support (obsolete)';
comment on function PG_CATALOG.gin_extract_tsvector(tsvector, internal, internal) is 'GIN tsvector support';
comment on function PG_CATALOG.gin_tsquery_consistent(internal, smallint, tsquery, integer, internal, internal) is 'GIN tsvector support (obsolete)';
comment on function PG_CATALOG.gin_tsquery_consistent(internal, smallint, tsquery, integer, internal, internal, internal, internal) is 'GIN tsvector support';
comment on function PG_CATALOG.gin_tsquery_triconsistent(internal, smallint, tsquery, integer, internal, internal, internal) is 'GIN tsvector support';
comment on function PG_CATALOG.ginarrayconsistent(internal, smallint, anyarray, integer, internal, internal, internal, internal) is 'GIN array support';
comment on function PG_CATALOG.ginarrayextract(anyarray, internal, internal) is 'GIN array support';
comment on function PG_CATALOG.ginarrayextract(anyarray, internal) is 'GIN array support (obsolete)';
comment on function PG_CATALOG.ginarraytriconsistent(internal, smallint, anyarray, integer, internal, internal, internal) is 'GIN array support';
comment on function PG_CATALOG.ginqueryarrayextract(anyarray, internal, smallint, internal, internal, internal, internal) is 'GIN array support';
comment on function PG_CATALOG.gist_box_consistent(internal, box, integer, oid, internal) is 'GiST support';
comment on function PG_CATALOG.gist_box_penalty(internal, internal, internal) is 'GiST support';
comment on function PG_CATALOG.gist_box_picksplit(internal, internal) is 'GiST support';
comment on function PG_CATALOG.gist_box_same(box, box, internal) is 'GiST support';
comment on function PG_CATALOG.gist_box_union(internal, internal) is 'GiST support';
comment on function PG_CATALOG.gist_circle_compress(internal) is 'GiST support';
comment on function PG_CATALOG.gist_circle_consistent(internal, circle, integer, oid, internal) is 'GiST support';
comment on function PG_CATALOG.gist_point_compress(internal) is 'GiST support';
comment on function PG_CATALOG.gist_point_consistent(internal, point, integer, oid, internal) is 'GiST support';
comment on function PG_CATALOG.gist_point_distance(internal, point, integer, oid) is 'GiST support';
comment on function PG_CATALOG.gist_poly_compress(internal) is 'GiST support';
comment on function PG_CATALOG.gist_poly_consistent(internal, polygon, integer, oid, internal) is 'GiST support';
comment on function PG_CATALOG.gtsquery_compress(internal) is 'GiST tsquery support';
comment on function PG_CATALOG.gtsquery_consistent(internal, internal, integer, oid, internal) is 'GiST tsquery support';
comment on function PG_CATALOG.gtsquery_penalty(internal, internal, internal) is 'GiST tsquery support';
comment on function PG_CATALOG.gtsquery_picksplit(internal, internal) is 'GiST tsquery support';
comment on function PG_CATALOG.gtsquery_same(bigint, bigint, internal) is 'GiST tsquery support';
comment on function PG_CATALOG.gtsquery_union(internal, internal) is 'GiST tsquery support';
comment on function PG_CATALOG.gtsvector_compress(internal) is 'GiST tsvector support';
comment on function PG_CATALOG.gtsvector_consistent(internal, gtsvector, integer, oid, internal) is 'GiST tsvector support';
comment on function PG_CATALOG.gtsvector_decompress(internal) is 'GiST tsvector support';
comment on function PG_CATALOG.gtsvector_penalty(internal, internal, internal) is 'GiST tsvector support';
comment on function PG_CATALOG.gtsvector_picksplit(internal, internal) is 'GiST tsvector support';
comment on function PG_CATALOG.gtsvector_same(gtsvector, gtsvector, internal) is 'GiST tsvector support';
comment on function PG_CATALOG.gtsvector_union(internal, internal) is 'GiST tsvector support';
comment on function PG_CATALOG.gtsvectorin(cstring) is 'I/O';
comment on function PG_CATALOG.gtsvectorout(gtsvector) is 'I/O';
comment on function PG_CATALOG.has_any_column_privilege(name, text, text) is 'user privilege on any column by username, rel name';
comment on function PG_CATALOG.has_any_column_privilege(name, oid, text) is 'user privilege on any column by username, rel oid';
comment on function PG_CATALOG.has_any_column_privilege(oid, text, text) is 'user privilege on any column by user oid, rel name';
comment on function PG_CATALOG.has_any_column_privilege(oid, oid, text) is 'user privilege on any column by user oid, rel oid';
comment on function PG_CATALOG.has_any_column_privilege(text, text) is 'current user privilege on any column by rel name';
comment on function PG_CATALOG.has_any_column_privilege(oid, text) is 'current user privilege on any column by rel oid';
comment on function PG_CATALOG.has_column_privilege(name, text, text, text) is 'user privilege on column by username, rel name, col name';
comment on function PG_CATALOG.has_column_privilege(name, text, smallint, text) is 'user privilege on column by username, rel name, col attnum';
comment on function PG_CATALOG.has_column_privilege(name, oid, text, text) is 'user privilege on column by username, rel oid, col name';
comment on function PG_CATALOG.has_column_privilege(name, oid, smallint, text) is 'user privilege on column by username, rel oid, col attnum';
comment on function PG_CATALOG.has_column_privilege(oid, text, text, text) is 'user privilege on column by user oid, rel name, col name';
comment on function PG_CATALOG.has_column_privilege(oid, text, smallint, text) is 'user privilege on column by user oid, rel name, col attnum';
comment on function PG_CATALOG.has_column_privilege(oid, oid, text, text) is 'user privilege on column by user oid, rel oid, col name';
comment on function PG_CATALOG.has_column_privilege(oid, oid, smallint, text) is 'user privilege on column by user oid, rel oid, col attnum';
comment on function PG_CATALOG.has_column_privilege(text, text, text) is 'current user privilege on column by rel name, col name';
comment on function PG_CATALOG.has_column_privilege(text, smallint, text) is 'current user privilege on column by rel name, col attnum';
comment on function PG_CATALOG.has_column_privilege(oid, text, text) is 'current user privilege on column by rel oid, col name';
comment on function PG_CATALOG.has_column_privilege(oid, smallint, text) is 'current user privilege on column by rel oid, col attnum';
comment on function PG_CATALOG.has_database_privilege(name, text, text) is 'user privilege on database by username, database name';
comment on function PG_CATALOG.has_database_privilege(name, oid, text) is 'user privilege on database by username, database oid';
comment on function PG_CATALOG.has_database_privilege(oid, text, text) is 'user privilege on database by user oid, database name';
comment on function PG_CATALOG.has_database_privilege(oid, oid, text) is 'user privilege on database by user oid, database oid';
comment on function PG_CATALOG.has_database_privilege(text, text) is 'current user privilege on database by database name';
comment on function PG_CATALOG.has_database_privilege(oid, text) is 'current user privilege on database by database oid';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(name, text, text) is 'user privilege on foreign data wrapper by username, foreign data wrapper name';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(name, oid, text) is 'user privilege on foreign data wrapper by username, foreign data wrapper oid';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(oid, text, text) is 'user privilege on foreign data wrapper by user oid, foreign data wrapper name';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(oid, oid, text) is 'user privilege on foreign data wrapper by user oid, foreign data wrapper oid';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(text, text) is 'current user privilege on foreign data wrapper by foreign data wrapper name';
comment on function PG_CATALOG.has_foreign_data_wrapper_privilege(oid, text) is 'current user privilege on foreign data wrapper by foreign data wrapper oid';
comment on function PG_CATALOG.has_function_privilege(name, text, text) is 'user privilege on function by username, function name';
comment on function PG_CATALOG.has_function_privilege(name, oid, text) is 'user privilege on function by username, function oid';
comment on function PG_CATALOG.has_function_privilege(oid, text, text) is 'user privilege on function by user oid, function name';
comment on function PG_CATALOG.has_function_privilege(oid, oid, text) is 'user privilege on function by user oid, function oid';
comment on function PG_CATALOG.has_function_privilege(text, text) is 'current user privilege on function by function name';
comment on function PG_CATALOG.has_function_privilege(oid, text) is 'current user privilege on function by function oid';
comment on function PG_CATALOG.has_language_privilege(name, text, text) is 'user privilege on language by username, language name';
comment on function PG_CATALOG.has_language_privilege(name, oid, text) is 'user privilege on language by username, language oid';
comment on function PG_CATALOG.has_language_privilege(oid, text, text) is 'user privilege on language by user oid, language name';
comment on function PG_CATALOG.has_language_privilege(oid, oid, text) is 'user privilege on language by user oid, language oid';
comment on function PG_CATALOG.has_language_privilege(text, text) is 'current user privilege on language by language name';
comment on function PG_CATALOG.has_language_privilege(oid, text) is 'current user privilege on language by language oid';
comment on function PG_CATALOG.has_schema_privilege(name, text, text) is 'user privilege on schema by username, schema name';
comment on function PG_CATALOG.has_schema_privilege(name, oid, text) is 'user privilege on schema by username, schema oid';
comment on function PG_CATALOG.has_schema_privilege(oid, text, text) is 'user privilege on schema by user oid, schema name';
comment on function PG_CATALOG.has_schema_privilege(oid, oid, text) is 'user privilege on schema by user oid, schema oid';
comment on function PG_CATALOG.has_schema_privilege(text, text) is 'current user privilege on schema by schema name';
comment on function PG_CATALOG.has_schema_privilege(oid, text) is 'current user privilege on schema by schema oid';
comment on function PG_CATALOG.has_sequence_privilege(name, text, text) is 'user privilege on sequence by username, seq name';
comment on function PG_CATALOG.has_sequence_privilege(name, oid, text) is 'user privilege on sequence by username, seq oid';
comment on function PG_CATALOG.has_sequence_privilege(oid, text, text) is 'user privilege on sequence by user oid, seq name';
comment on function PG_CATALOG.has_sequence_privilege(oid, oid, text) is 'user privilege on sequence by user oid, seq oid';
comment on function PG_CATALOG.has_sequence_privilege(text, text) is 'current user privilege on sequence by seq name';
comment on function PG_CATALOG.has_sequence_privilege(oid, text) is 'current user privilege on sequence by seq oid';
comment on function PG_CATALOG.has_server_privilege(name, text, text) is 'user privilege on server by username, server name';
comment on function PG_CATALOG.has_server_privilege(name, oid, text) is 'user privilege on server by username, server oid';
comment on function PG_CATALOG.has_server_privilege(oid, text, text) is 'user privilege on server by user oid, server name';
comment on function PG_CATALOG.has_server_privilege(oid, oid, text) is 'user privilege on server by user oid, server oid';
comment on function PG_CATALOG.has_server_privilege(text, text) is 'current user privilege on server by server name';
comment on function PG_CATALOG.has_server_privilege(oid, text) is 'current user privilege on server by server oid';
comment on function PG_CATALOG.has_table_privilege(name, text, text) is 'user privilege on relation by username, rel name';
comment on function PG_CATALOG.has_table_privilege(name, oid, text) is 'user privilege on relation by username, rel oid';
comment on function PG_CATALOG.has_table_privilege(oid, text, text) is 'user privilege on relation by user oid, rel name';
comment on function PG_CATALOG.has_table_privilege(oid, oid, text) is 'user privilege on relation by user oid, rel oid';
comment on function PG_CATALOG.has_table_privilege(text, text) is 'current user privilege on relation by rel name';
comment on function PG_CATALOG.has_table_privilege(oid, text) is 'current user privilege on relation by rel oid';
comment on function PG_CATALOG.has_tablespace_privilege(name, text, text) is 'user privilege on tablespace by username, tablespace name';
comment on function PG_CATALOG.has_tablespace_privilege(name, oid, text) is 'user privilege on tablespace by username, tablespace oid';
comment on function PG_CATALOG.has_tablespace_privilege(oid, text, text) is 'user privilege on tablespace by user oid, tablespace name';
comment on function PG_CATALOG.has_tablespace_privilege(oid, oid, text) is 'user privilege on tablespace by user oid, tablespace oid';
comment on function PG_CATALOG.has_tablespace_privilege(text, text) is 'current user privilege on tablespace by tablespace name';
comment on function PG_CATALOG.has_tablespace_privilege(oid, text) is 'current user privilege on tablespace by tablespace oid';
comment on function PG_CATALOG.has_type_privilege(name, text, text) is 'user privilege on type by username, type name';
comment on function PG_CATALOG.has_type_privilege(name, oid, text) is 'user privilege on type by username, type oid';
comment on function PG_CATALOG.has_type_privilege(oid, text, text) is 'user privilege on type by user oid, type name';
comment on function PG_CATALOG.has_type_privilege(oid, oid, text) is 'user privilege on type by user oid, type oid';
comment on function PG_CATALOG.has_type_privilege(text, text) is 'current user privilege on type by type name';
comment on function PG_CATALOG.has_type_privilege(oid, text) is 'current user privilege on type by type oid';
comment on function PG_CATALOG.hash_aclitem(aclitem) is 'hash';
comment on function PG_CATALOG.hash_array(anyarray) is 'hash';
comment on function PG_CATALOG.hash_numeric(numeric) is 'hash';
comment on function PG_CATALOG.hash_range(anyrange) is 'hash a range';
comment on function PG_CATALOG.hashbpchar(character) is 'hash';
comment on function PG_CATALOG.hashenum(anyenum) is 'hash';
comment on function PG_CATALOG.hashfloat4(real) is 'hash';
comment on function PG_CATALOG.hashfloat8(double precision) is 'hash';
comment on function PG_CATALOG.hashinet(inet) is 'hash';
comment on function PG_CATALOG.hashint2(smallint) is 'hash';
comment on function PG_CATALOG.hashint4(integer) is 'hash';
comment on function PG_CATALOG.hashint8(bigint) is 'hash';
comment on function PG_CATALOG.hashmacaddr(macaddr) is 'hash';
comment on function PG_CATALOG.hashname(name) is 'hash';
comment on function PG_CATALOG.hashoid(oid) is 'hash';
comment on function PG_CATALOG.hashoidvector(oidvector) is 'hash';
comment on function PG_CATALOG.hashtext(text) is 'hash';
comment on function PG_CATALOG.hashvarlena(internal) is 'hash';
comment on function PG_CATALOG.height(box) is 'box height';
comment on function PG_CATALOG.hll_add_agg(hll_hashval) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer, integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer, integer, bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.hll_add_agg(hll_hashval, integer, integer, bigint, integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.hll_union_agg() is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.host(inet) is 'show address octets only';
comment on function PG_CATALOG.hostmask() is 'hostmask of address';
comment on function PG_CATALOG.iclikejoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of ILIKE';
comment on function PG_CATALOG.iclikesel(internal, oid, internal, integer) is 'restriction selectivity of ILIKE';
comment on function PG_CATALOG.icnlikejoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of NOT ILIKE';
comment on function PG_CATALOG.icnlikesel(internal, oid, internal, integer) is 'restriction selectivity of NOT ILIKE';
comment on function PG_CATALOG.icregexeqjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of case-insensitive regex match';
comment on function PG_CATALOG.icregexeqsel(internal, oid, internal, integer) is 'restriction selectivity of case-insensitive regex match';
comment on function PG_CATALOG.icregexnejoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of case-insensitive regex non-match';
comment on function PG_CATALOG.icregexnesel(internal, oid, internal, integer) is 'restriction selectivity of case-insensitive regex non-match';
comment on function PG_CATALOG.inet_client_addr() is 'inet address of the client';
comment on function PG_CATALOG.inet_client_port() is 'client\''s port number for this connection';
comment on function PG_CATALOG.inet_in(cstring) is 'I/O';
comment on function PG_CATALOG.inet_out(inet) is 'I/O';
comment on function PG_CATALOG.inet_recv(internal) is 'I/O';
comment on function PG_CATALOG.inet_send(inet) is 'I/O';
comment on function PG_CATALOG.inet_server_addr() is 'inet address of the server';
comment on function PG_CATALOG.inet_server_port() is 'server\''s port number for this connection';
comment on function PG_CATALOG.initcap(text) is 'capitalize each word';
comment on function PG_CATALOG.int2(real) is 'convert float4 to int2';
comment on function PG_CATALOG.int2(double precision) is 'convert float8 to int2';
comment on function PG_CATALOG.int2(integer) is 'convert int4 to int2';
comment on function PG_CATALOG.int2(bigint) is 'convert int8 to int2';
comment on function PG_CATALOG.int2(numeric) is 'convert numeric to int2';
comment on function PG_CATALOG.int2_accum(numeric[], smallint) is 'aggregate transition function';
comment on function PG_CATALOG.int2_avg_accum(bigint[], smallint) is 'aggregate transition function';
comment on function PG_CATALOG.int2_sum(bigint, smallint) is 'aggregate transition function';
comment on function PG_CATALOG.int2in(cstring) is 'I/O';
comment on function PG_CATALOG.int2larger(smallint, smallint) is 'larger of two';
comment on function PG_CATALOG.int2mod(smallint, smallint) is 'modulus';
comment on function PG_CATALOG.int2out(smallint) is 'I/O';
comment on function PG_CATALOG.int2recv(internal) is 'I/O';
comment on function PG_CATALOG.int2send(smallint) is 'I/O';
comment on function PG_CATALOG.int2smaller(smallint, smallint) is 'smaller of two';
comment on function PG_CATALOG.int2vectorin(cstring) is 'I/O';
comment on function PG_CATALOG.int2vectorout(int2vector) is 'I/O';
comment on function PG_CATALOG.int2vectorrecv(internal) is 'I/O';
comment on function PG_CATALOG.int2vectorsend(int2vector) is 'I/O';
comment on function PG_CATALOG.int4("char") is 'convert char to int4';
comment on function PG_CATALOG.int4(smallint) is 'convert int2 to int4';
comment on function PG_CATALOG.int4(double precision) is 'convert double to int4';
comment on function PG_CATALOG.int4(real) is 'convert float to int4';
comment on function PG_CATALOG.int4(bigint) is 'convert int8 to int4';
comment on function PG_CATALOG.int4(bit) is 'convert bit to int4';
comment on function PG_CATALOG.int4(numeric) is 'convert numeric to int4';
comment on function PG_CATALOG.int4(boolean) is 'convert bool to int4';
comment on function PG_CATALOG.int4_accum(numeric[], integer) is 'aggregate transition function';
comment on function PG_CATALOG.int4_avg_accum(bigint[], integer) is 'aggregate transition function';
comment on function PG_CATALOG.int4_sum(bigint, integer) is 'aggregate transition function';
comment on function PG_CATALOG.int4in(cstring) is 'I/O';
comment on function PG_CATALOG.int4inc(integer) is 'increment';
comment on function PG_CATALOG.int4larger(integer, integer) is 'larger of two';
comment on function PG_CATALOG.int4mod(integer, integer) is 'modulus';
comment on function PG_CATALOG.int4out(integer) is 'I/O';
comment on function PG_CATALOG.int4range(integer, integer) is 'int4range constructor';
comment on function PG_CATALOG.int4range(integer, integer, text) is 'int4range constructor';
comment on function PG_CATALOG.int4range_canonical(int4range) is 'convert an int4 range to canonical form';
comment on function PG_CATALOG.int4range_subdiff(integer, integer) is 'float8 difference of two int4 values';
comment on function PG_CATALOG.int4recv(internal) is 'I/O';
comment on function PG_CATALOG.int4send(integer) is 'I/O';
comment on function PG_CATALOG.int4smaller(integer, integer) is 'smaller of two';
comment on function PG_CATALOG.int8(integer) is 'convert int4 to int8';
comment on function PG_CATALOG.int8(real) is 'convert float4 to int8';
comment on function PG_CATALOG.int8(double precision) is 'convert float8 to int8';
comment on function PG_CATALOG.int8(smallint) is 'convert int2 to int8';
comment on function PG_CATALOG.int8(oid) is 'convert oid to int8';
comment on function PG_CATALOG.int8(numeric) is 'convert numeric to int8';
comment on function PG_CATALOG.int8(bit) is 'convert bitstring to int8';
comment on function PG_CATALOG.int8_accum(numeric[], bigint) is 'aggregate transition function';
comment on function PG_CATALOG.int8_avg(bigint[]) is 'aggregate final function';
comment on function PG_CATALOG.int8_avg_collect(bigint[], bigint[]) is 'aggregate transition function';
comment on function PG_CATALOG.int8_sum(numeric, bigint) is 'aggregate transition function';
comment on function PG_CATALOG.int8in(cstring) is 'I/O';
comment on function PG_CATALOG.int8inc(bigint) is 'increment';
comment on function PG_CATALOG.int8inc_any(bigint, "any") is 'increment, ignores second argument';
comment on function PG_CATALOG.int8inc_float8_float8(bigint, double precision, double precision) is 'aggregate transition function';
comment on function PG_CATALOG.int8larger(bigint, bigint) is 'larger of two';
comment on function PG_CATALOG.int8mod(bigint, bigint) is 'modulus';
comment on function PG_CATALOG.int8out(bigint) is 'I/O';
comment on function PG_CATALOG.int8range(bigint, bigint) is 'int8range constructo';
comment on function PG_CATALOG.int8range(bigint, bigint, text) is 'int8range constructo';
comment on function PG_CATALOG.int8range_canonical(int8range) is 'convert an int8 range to canonical form';
comment on function PG_CATALOG.int8range_subdiff(bigint, bigint) is 'float8 difference of two int8 values';
comment on function PG_CATALOG.int8recv(internal) is 'I/O';
comment on function PG_CATALOG.int8send(bigint) is 'I/O';
comment on function PG_CATALOG.int8smaller(bigint, bigint) is 'smaller of two';
comment on function PG_CATALOG.internal_in(cstring) is 'I/O';
comment on function PG_CATALOG.internal_out(internal) is 'I/O';
comment on function PG_CATALOG.interval(interval, integer) is 'adjust interval precision';
comment on function PG_CATALOG.interval(time without time zone) is 'convert time to interval';
comment on function PG_CATALOG.interval_accum(interval[], interval) is 'aggregate transition function';
comment on function PG_CATALOG.interval_avg(interval[]) is 'aggregate final function';
comment on function PG_CATALOG.interval_cmp(interval, interval) is 'less-equal-greater';
comment on function PG_CATALOG.interval_hash(interval) is 'hash';
comment on function PG_CATALOG.interval_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.interval_larger(interval, interval) is 'larger of two';
comment on function PG_CATALOG.interval_out(interval) is 'I/O';
comment on function PG_CATALOG.interval_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.interval_send(interval) is 'I/O';
comment on function PG_CATALOG.interval_smaller(interval, interval) is 'smaller of two';
comment on function PG_CATALOG.intervaltypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.intervaltypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.isclosed(path) is 'path closed?';
comment on function PG_CATALOG.isempty(anyrange) is 'is the range empty?';
comment on function PG_CATALOG.isfinite(date) is 'finite date?';
comment on function PG_CATALOG.isfinite(timestamp with time zone) is 'finite timestamp?';
comment on function PG_CATALOG.isfinite(interval) is 'finite interval?';
comment on function PG_CATALOG.isfinite(timestamp without time zone) is 'finite timestamp?';
comment on function PG_CATALOG.isopen(path) is 'path open?';
comment on function PG_CATALOG.isvertical(lseg) is 'vertical';
comment on function PG_CATALOG.json_in(cstring) is 'I/O';
comment on function PG_CATALOG.json_out(json) is 'I/O';
comment on function PG_CATALOG.json_recv(internal) is 'I/O';
comment on function PG_CATALOG.json_send(json) is 'I/O';
comment on function PG_CATALOG.justify_days(interval) is 'promote groups of 30 days to numbers of months';
comment on function PG_CATALOG.justify_hours(interval) is 'promote groups of 24 hours to numbers of days';
comment on function PG_CATALOG.justify_interval(interval) is 'promote groups of 24 hours to numbers of days and promote groups of 30 days to numbers of months';
comment on function PG_CATALOG.lag(anyelement) is 'fetch the preceding row value';
comment on function PG_CATALOG.lag(anyelement, integer ) is 'fetch the Nth preceding row value';
comment on function PG_CATALOG.lag(anyelement, integer, anyelement) is 'fetch the Nth preceding row value with default';
comment on function PG_CATALOG.language_handler_in(cstring) is 'I/O';
comment on function PG_CATALOG.language_handler_out(language_handler) is 'I/O';
comment on function PG_CATALOG.last_value(anyelement) is 'fetch the last row value';
comment on function PG_CATALOG.lastval() is 'current value from last used sequence';
comment on function PG_CATALOG.lead(anyelement) is 'fetch the following row value';
comment on function PG_CATALOG.lead(anyelement, integer) is 'fetch the Nth following row value';
comment on function PG_CATALOG.lead(anyelement, integer, anyelement) is 'fetch the Nth following row value with default';
comment on function PG_CATALOG.left(text, integer) is 'extract the first n characters';
comment on function PG_CATALOG.length(text) is 'character length';
comment on function PG_CATALOG.length(character) is 'character length';
comment on function PG_CATALOG.length(lseg) is 'distance between endpoints';
comment on function PG_CATALOG.length(path) is 'sum of path segments';
comment on function PG_CATALOG.length(bit) is 'bitstring length';
comment on function PG_CATALOG.length(bytea, name) is 'length of string in specified encoding';
comment on function PG_CATALOG.length(bytea) is 'octet length';
comment on function PG_CATALOG.length(tsvector) is 'number of lexemes';
comment on function PG_CATALOG.lengthb(text) is 'octet length';
comment on function PG_CATALOG.like(name, text) is 'matches LIKE expression';
comment on function PG_CATALOG.like(text, text) is 'matches LIKE expression';
comment on function PG_CATALOG.like_escape(bytea, bytea) is 'convert LIKE pattern to use backslash escapes';
comment on function PG_CATALOG.like_escape(text, text) is 'convert LIKE pattern to use backslash escapes';
comment on function PG_CATALOG.likejoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of LIKE';
comment on function PG_CATALOG.likesel(internal, oid, internal, integer) is 'restriction selectivity of LIKE';
comment on function PG_CATALOG.listagg(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(bigint, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(date) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(date, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(double precision, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(integer, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(interval) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(interval, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(numeric, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(real, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(smallint, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(text, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(timestamp with time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(timestamp with time zone, text ) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(timestamp without time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.listagg(timestamp without time zone, text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.ln(double precision) is 'natural logarithm';
comment on function PG_CATALOG.ln(numeric) is 'natural logarithm';
comment on function PG_CATALOG.log(double precision) is 'base 10 logarithm';
comment on function PG_CATALOG.log(numeric, numeric) is 'logarithm base m of n';
comment on function PG_CATALOG.log(numeric) is 'base 10 logarithm';
comment on function PG_CATALOG.lower(text) is 'lowercase';
comment on function PG_CATALOG.lower(anyrange) is 'lower bound of range';
comment on function PG_CATALOG.lower_inc(anyrange) is 'is the range\''s lower bound inclusive?';
comment on function PG_CATALOG.lower_inf(anyrange) is 'is the range\''s lower bound infinite?';
comment on function PG_CATALOG.lpad(text, integer) is 'left-pad string to length';
comment on function PG_CATALOG.lseg(point, point) is 'convert points to line segment';
comment on function PG_CATALOG.lseg(box) is 'box diagonal';
comment on function PG_CATALOG.lseg_center(lseg) is 'center of';
comment on function PG_CATALOG.lseg_horizontal(lseg) is 'horizontal';
comment on function PG_CATALOG.lseg_in(cstring) is 'I/O';
comment on function PG_CATALOG.lseg_length(lseg) is 'distance between endpoints';
comment on function PG_CATALOG.lseg_out(lseg) is 'I/O';
comment on function PG_CATALOG.lseg_perp(lseg, lseg) is 'perpendicular';
comment on function PG_CATALOG.lseg_recv(internal) is 'I/O';
comment on function PG_CATALOG.lseg_send(lseg) is 'I/O';
comment on function PG_CATALOG.lseg_vertical(lseg) is 'vertical';
comment on function PG_CATALOG.ltrim(text, text) is 'trim selected characters from left end of string';
comment on function PG_CATALOG.ltrim(text) is 'trim spaces from left end of string';
comment on function PG_CATALOG.macaddr_cmp(macaddr, macaddr ) is 'less-equal-greater';
comment on function PG_CATALOG.macaddr_in(cstring) is 'I/O';
comment on function PG_CATALOG.macaddr_out(macaddr) is 'I/O';
comment on function PG_CATALOG.macaddr_recv(internal) is 'I/O';
comment on function PG_CATALOG.macaddr_send(macaddr) is 'I/O';
comment on function PG_CATALOG.makeaclitem(oid, oid, text, boolean) is 'make ACL item';
comment on function PG_CATALOG.masklen(inet) is 'netmask length';
comment on function PG_CATALOG.max(abstime) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(anyarray) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(anyenum) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(character) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(date) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(interval) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(money) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(oid) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(smalldatetime) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(tid) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(time with time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(time without time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(timestamp with time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(timestamp without time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.max(tinyint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.md5(bytea) is 'MD5 hash';
comment on function PG_CATALOG.md5(text) is 'MD5 hash';
comment on function PG_CATALOG.median( double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.median(interval) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(abstime) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(anyarray) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(anyenum) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(character) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(date) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(interval) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(money) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(oid) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(smalldatetime) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(text) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(tid) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(time with time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(time without time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(timestamp with time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.min(timestamp without time zone) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.mod(numeric, numeric) is 'modulus';
comment on function PG_CATALOG.mode() is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.mode_final(internal) is 'aggregate final function';
comment on function PG_CATALOG.money(integer) is 'convert int4 to money';
comment on function PG_CATALOG.money(bigint) is 'convert int8 to money';
comment on function PG_CATALOG.money(numeric) is 'convert numeric to money';
comment on function PG_CATALOG.name(character) is 'convert char(n) to name';
comment on function PG_CATALOG.name(character varying) is 'convert varchar to name';
comment on function PG_CATALOG.name(text) is 'convert varchar to name';
comment on function PG_CATALOG.namein(cstring) is 'I/O';
comment on function PG_CATALOG.namelike(name, text) is 'matches LIKE expression';
comment on function PG_CATALOG.namenlike(name, text) is 'does not match LIKE expression';
comment on function PG_CATALOG.nameout(name) is 'I/O';
comment on function PG_CATALOG.namerecv(internal) is 'I/O';
comment on function PG_CATALOG.namesend(name) is 'I/O';
comment on function PG_CATALOG.neqjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of <> and related operators';
comment on function PG_CATALOG.neqsel(internal, oid, internal, integer) is 'restriction selectivity of <> and related operators';
comment on function PG_CATALOG.netmask(inet) is 'netmask of address';
comment on function PG_CATALOG.network(inet) is 'network part of address';
comment on function PG_CATALOG.network_cmp(inet, inet) is 'less-equal-greater';
comment on function PG_CATALOG.nextval(regclass) is 'sequence next value';
comment on function PG_CATALOG.nlikejoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of NOT LIKE';
comment on function PG_CATALOG.nlikesel(internal, oid, internal, integer) is 'restriction selectivity of NOT ILIKE';
comment on function PG_CATALOG.notlike(name, text) is 'does not match LIKE expression';
comment on function PG_CATALOG.now() is 'current transaction time';
comment on function PG_CATALOG.npoints(path) is 'number of points';
comment on function PG_CATALOG.npoints(polygon) is 'number of points';
comment on function PG_CATALOG.nth_value(anyelement, integer) is 'fetch the Nth row value';
comment on function PG_CATALOG.ntile(integer) is 'split rows into N groups';
comment on function PG_CATALOG.numeric(numeric, integer) is 'djust numeric to typmod precision/scale';
comment on function PG_CATALOG.numeric(integer) is 'convert int4 to numeric';
comment on function PG_CATALOG.numeric(real) is 'convert float4 to numeric';
comment on function PG_CATALOG.numeric(double precision) is 'convert float8 to numeric';
comment on function PG_CATALOG.numeric(bigint) is 'convert int8 to numeric';
comment on function PG_CATALOG.numeric(smallint) is 'convert int2 to numeric';
comment on function PG_CATALOG.numeric(money) is 'convert money to numeric';
comment on function PG_CATALOG.numeric_abs(numeric) is 'sign of value';
comment on function PG_CATALOG.numeric_accum(numeric[], numeric) is 'aggregate transition function';
comment on function PG_CATALOG.numeric_avg(numeric[]) is 'aggregate final function';
comment on function PG_CATALOG.numeric_avg_accum(numeric[], numeric) is 'aggregate transition function';
comment on function PG_CATALOG.numeric_cmp(numeric, numeric) is 'less-equal-greater';
comment on function PG_CATALOG.numeric_div_trunc(numeric, numeric) is 'trunc(x/y)';
comment on function PG_CATALOG.numeric_exp(numeric) is 'natural exponential (e^x)';
comment on function PG_CATALOG.numeric_fac(bigint) is 'factorial';
comment on function PG_CATALOG.numeric_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.numeric_inc(numeric) is 'increment by one';
comment on function PG_CATALOG.numeric_larger(numeric, numeric) is 'larger of two';
comment on function PG_CATALOG.numeric_ln(numeric) is 'natural logarithm';
comment on function PG_CATALOG.numeric_log(numeric, numeric) is 'logarithm base m of n';
comment on function PG_CATALOG.numeric_mod(numeric, numeric) is 'modulus';
comment on function PG_CATALOG.numeric_out(numeric) is 'I/O';
comment on function PG_CATALOG.numeric_power(numeric, numeric) is 'exponentiation';
comment on function PG_CATALOG.numeric_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.numeric_send(numeric) is 'I/O';
comment on function PG_CATALOG.numeric_smaller(numeric, numeric) is 'smaller of two';
comment on function PG_CATALOG.numeric_sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.numeric_sqrt(numeric) is 'square root';
comment on function PG_CATALOG.numeric_stddev_pop(numeric[]) is 'aggregate final function';
comment on function PG_CATALOG.numeric_stddev_samp(numeric[]) is 'aggregate final function';
comment on function PG_CATALOG.numeric_var_pop(numeric[]) is 'aggregate final function';
comment on function PG_CATALOG.numeric_var_samp(numeric[]) is 'aggregate final function';
comment on function PG_CATALOG.numerictypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.numerictypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.numnode(tsquery) is 'number of nodes';
comment on function PG_CATALOG.numrange(numeric, numeric) is 'numrange constructor';
comment on function PG_CATALOG.numrange(numeric, numeric, text) is 'numrange constructor';
comment on function PG_CATALOG.numrange_subdiff(numeric, numeric) is 'float8 difference of two numeric values';
comment on function PG_CATALOG.obj_description(oid) is 'deprecated, use two-argument form instead';
comment on function PG_CATALOG.obj_description(oid, name) is 'get description for object id and catalog name';
comment on function PG_CATALOG.octet_length(bit) is 'octet length';
comment on function PG_CATALOG.octet_length(bytea) is 'octet length';
comment on function PG_CATALOG.octet_length(character) is 'octet length';
comment on function PG_CATALOG.oid(bigint) is 'convert int8 to oid';
comment on function PG_CATALOG.oidin(cstring) is 'I/O';
comment on function PG_CATALOG.oidlarger(oid, oid) is 'larger of two';
comment on function PG_CATALOG.oidout(oid) is 'I/O';
comment on function PG_CATALOG.oidrecv(internal) is 'I/O';
comment on function PG_CATALOG.oidsend(oid) is 'I/O';
comment on function PG_CATALOG.oidsmaller(oid, oid) is 'smaller of two';
comment on function PG_CATALOG.oidvectorin(cstring) is 'I/O';
comment on function PG_CATALOG.oidvectorout(oidvector) is 'I/O';
comment on function PG_CATALOG.oidvectorrecv(internal) is 'I/O';
comment on function PG_CATALOG.oidvectorsend(oidvector) is 'I/O';
comment on function PG_CATALOG.oidvectortypes(oidvector) is 'print type names of oidvector field';
comment on function PG_CATALOG.ordered_set_transition(internal, "any") is 'aggregate transition function';
comment on function PG_CATALOG.overlaps(time with time zone, time with time zone, time with time zone, time with time zone) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(time without time zone, interval, time without time zone, interval) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(time without time zone, interval, time without time zone, time without time zone) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(time without time zone, time without time zone, time without time zone, interval) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(time without time zone, time without time zone, time without time zone, time without time zone) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp with time zone, interval, timestamp with time zone, interval) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp with time zone, interval, timestamp with time zone, timestamp with time zone) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, interval) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp with time zone, timestamp with time zone, timestamp with time zone, timestamp with time zone) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp without time zone, interval, timestamp without time zone, interval) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp without time zone, interval, timestamp without time zone, timestamp without time zone) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, interval) is 'intervals overlap?';
comment on function PG_CATALOG.overlaps(timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone) is 'intervals overlap?';
comment on function PG_CATALOG.overlay(bytea, bytea, integer, integer) is 'substitute portion of string';
comment on function PG_CATALOG.overlay(bytea, bytea, integer) is 'substitute portion of string';
comment on function PG_CATALOG.overlay(text, text, integer, integer) is 'substitute portion of string';
comment on function PG_CATALOG.overlay(text, text, integer) is 'substitute portion of string';
comment on function PG_CATALOG.overlay(bit, bit, integer, integer) is 'substitute portion of bitstring';
comment on function PG_CATALOG.overlay(bit, bit, integer) is 'substitute portion of bitstring';
comment on function PG_CATALOG.path(polygon) is 'convert polygon to path';
comment on function PG_CATALOG.path_center(path) is 'center of';
comment on function PG_CATALOG.path_in(cstring) is 'I/O';
comment on function PG_CATALOG.path_length(path) is 'sum of path segments';
comment on function PG_CATALOG.path_npoints(path) is 'number of points';
comment on function PG_CATALOG.path_out(path) is 'I/O';
comment on function PG_CATALOG.path_recv(internal) is 'I/O';
comment on function PG_CATALOG.path_send(path) is 'I/O';
comment on function PG_CATALOG.pclose(path) is 'close path';
comment on function PG_CATALOG.percent_rank() is 'fractional rank within partition';
comment on function PG_CATALOG.percentile_cont(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.percentile_cont(double precision, interval) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.percentile_cont_float8_final(internal, double precision, double precision) is 'aggregate final function';
comment on function PG_CATALOG.percentile_cont_interval_final(internal, double precision, interval) is 'aggregate final function';
comment on function PG_CATALOG.pg_advisory_lock(bigint) is 'obtain exclusive advisory lock';
comment on function PG_CATALOG.pg_advisory_lock(integer, integer) is 'obtain exclusive advisory lock';
comment on function PG_CATALOG.pg_advisory_lock_shared(bigint) is 'obtain shared advisory lock';
comment on function PG_CATALOG.pg_advisory_lock_shared(integer, integer) is 'obtain shared advisory lock';
comment on function PG_CATALOG.pg_advisory_unlock(bigint) is 'release exclusive advisory lock';
comment on function PG_CATALOG.pg_advisory_unlock(integer, integer) is 'release exclusive advisory lock';
comment on function PG_CATALOG.pg_advisory_unlock_all() is 'release all advisory locks';
comment on function PG_CATALOG.pg_advisory_unlock_shared(bigint) is 'release shared advisory lock';
comment on function PG_CATALOG.pg_advisory_unlock_shared(integer, integer ) is 'release shared advisory lock';
comment on function PG_CATALOG.pg_advisory_xact_lock(bigint) is 'obtain exclusive advisory lock';
comment on function PG_CATALOG.pg_advisory_xact_lock(integer, integer) is 'obtain exclusive advisory lock';
comment on function PG_CATALOG.pg_advisory_xact_lock_shared(bigint) is 'obtain shared advisory lock';
comment on function PG_CATALOG.pg_advisory_xact_lock_shared(integer, integer) is 'obtain shared advisory lock';
comment on function PG_CATALOG.pg_available_extension_versions() is 'list available extension versions';
comment on function PG_CATALOG.pg_available_extensions() is 'list available extensions';
comment on function PG_CATALOG.pg_backend_pid() is 'statistics: current backend PID';
comment on function PG_CATALOG.pg_cancel_backend(bigint) is 'cancel a server process\'' current query';
comment on function PG_CATALOG.pg_char_to_encoding(name) is 'convert encoding name to encoding id';
comment on function PG_CATALOG.pg_client_encoding() is 'encoding name of current database';
comment on function PG_CATALOG.pg_collation_for("any") is 'collation of the argument; implementation of the COLLATION FOR expression';
comment on function PG_CATALOG.pg_collation_is_visible(oid) is 'is collation visible in search path?';
comment on function PG_CATALOG.pg_column_size("any") is 'bytes required to store the value, perhaps with compression';
comment on function PG_CATALOG.pg_conf_load_time() is 'configuration load time';
comment on function PG_CATALOG.pg_control_checkpoint() is 'pg_controldata checkpoint state information as a function';
comment on function PG_CATALOG.pg_control_system() is 'pg_controldata general state information as a function';
comment on function PG_CATALOG.pg_conversion_is_visible(oid) is 'is conversion visible in search path?';
comment on function PG_CATALOG.pg_create_logical_replication_slot(slotname name, plugin name) is 'set up a logical replication slot';
comment on function PG_CATALOG.pg_create_physical_replication_slot(slotname name, dummy_standby boolean) is 'create a physical replication slot';
comment on function PG_CATALOG.pg_create_restore_point(text) is 'create a named restore point';
comment on function PG_CATALOG.pg_cursor() is 'get the open cursors for this session';
comment on function PG_CATALOG.pg_database_size(name) is 'total disk space usage for the specified database';
comment on function PG_CATALOG.pg_database_size(oid) is 'total disk space usage for the specified database';
comment on function PG_CATALOG.pg_describe_object(oid, oid, integer) is 'get identification of SQL object';
comment on function PG_CATALOG.pg_drop_replication_slot(name) is 'drop a replication slot';
comment on function PG_CATALOG.pg_encoding_max_length(integer) is 'maximum octet length of a character in given encoding';
comment on function PG_CATALOG.pg_encoding_to_char(integer) is 'convert encoding id to encoding name';
comment on function PG_CATALOG.pg_export_snapshot() is 'export a snapshot';
comment on function PG_CATALOG.pg_extension_config_dump(regclass, text) is 'flag an extension\''s table contents to be emitted by pg_dump';
comment on function PG_CATALOG.pg_extension_update_paths(name name) is 'list an extension\''s version update paths';
comment on function PG_CATALOG.pg_filenode_relation(oid, oid) is 'relation OID for filenode and tablespace';
comment on function PG_CATALOG.pg_function_is_visible(oid) is 'is function visible in search path?';
comment on function PG_CATALOG.pg_get_constraintdef(oid) is 'constraint description';
comment on function PG_CATALOG.pg_get_constraintdef(oid, boolean) is 'constraint description with pretty-print option';
comment on function PG_CATALOG.pg_get_function_arguments(oid) is 'argument list of a function';
comment on function PG_CATALOG.pg_get_function_identity_arguments(oid) is 'identity argument list of a function';
comment on function PG_CATALOG.pg_get_function_result(oid) is 'result type of a function';
comment on function PG_CATALOG.pg_get_functiondef(funcid oid) is 'definition of a function';
comment on function PG_CATALOG.pg_get_indexdef(oid) is 'index description';
comment on function PG_CATALOG.pg_get_indexdef(oid, integer, boolean) is 'index description (full create statement or single expression) with pretty-print option';
comment on function PG_CATALOG.pg_get_keywords() is 'list of SQL keywords';
comment on function PG_CATALOG.pg_get_replication_slots() is 'information about replication slots currently in use';
comment on function PG_CATALOG.pg_get_ruledef(oid) is 'source text of a rule';
comment on function PG_CATALOG.pg_get_ruledef(oid, boolean) is 'source text of a rule with pretty-print option';
comment on function PG_CATALOG.pg_get_serial_sequence(text, text) is 'name of sequence for a serial column';
comment on function PG_CATALOG.pg_get_triggerdef(oid) is 'trigger description';
comment on function PG_CATALOG.pg_get_triggerdef(oid, boolean) is 'trigger description with pretty-print option';
comment on function PG_CATALOG.pg_get_userbyid(oid) is 'role name by OID (with fallback)';
comment on function PG_CATALOG.pg_get_viewdef(text) is 'select statement of a view';
comment on function PG_CATALOG.pg_get_viewdef(oid) is 'select statement of a view';
comment on function PG_CATALOG.pg_get_viewdef(text, boolean) is 'select statement of a view';
comment on function PG_CATALOG.pg_get_viewdef(oid, boolean) is 'select statement of a view';
comment on function PG_CATALOG.pg_get_viewdef(oid, integer) is 'select statement of a view';
comment on function PG_CATALOG.pg_has_role(name, name, text) is 'user privilege on role by username, role name';
comment on function PG_CATALOG.pg_has_role(name, oid, text) is 'user privilege on role by username, role oid';
comment on function PG_CATALOG.pg_has_role(oid, name, text) is 'user privilege on role by user oid, role name';
comment on function PG_CATALOG.pg_has_role(oid, oid, text) is 'user privilege on role by user oid, role oid';
comment on function PG_CATALOG.pg_has_role(name, text) is 'current user privilege on role by role name';
comment on function PG_CATALOG.pg_has_role(oid, text) is 'current user privilege on role by role oid';
comment on function PG_CATALOG.pg_indexes_size(regclass) is 'disk space usage for all indexes attached to the specified table';
comment on function PG_CATALOG.pg_is_in_recovery() is 'true if server is in recovery';
comment on function PG_CATALOG.pg_is_other_temp_schema(oid) is 'is schema another session\''s temp schema?';
comment on function PG_CATALOG.pg_last_xact_replay_timestamp() is 'timestamp of last replay xact';
comment on function PG_CATALOG.pg_listening_channels() is 'get the channels that the current backend listens to';
comment on function PG_CATALOG.pg_lock_status() is 'view system lock information';
comment on function PG_CATALOG.pg_logical_slot_get_binary_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is 'get binary changes from replication slot';
comment on function PG_CATALOG.pg_logical_slot_get_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is 'get changes from replication slot';
comment on function PG_CATALOG.pg_logical_slot_peek_binary_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is 'peek at binary changes from replication slot';
comment on function PG_CATALOG.pg_logical_slot_peek_changes(slotname name, upto_lsn text, upto_nchanges integer, VARIADIC options text[]) is 'peek at changes from replication slot';
comment on function PG_CATALOG.pg_ls_dir(text) is 'list all files in a directory';
comment on function PG_CATALOG.pg_my_temp_schema() is 'get OID of current session\''s temp schema, if any';
comment on function PG_CATALOG.pg_notify(text, text) is 'send a notification event';
comment on function PG_CATALOG.pg_opclass_is_visible(oid) is 'is opclass visible in search path?';
comment on function PG_CATALOG.pg_operator_is_visible(oid) is 'is operator visible in search path?';
comment on function PG_CATALOG.pg_opfamily_is_visible(oid) is 'is opfamily visible in search path?';
comment on function PG_CATALOG.pg_options_to_table(options_array text[]) is 'convert generic options array to name/value table';
comment on function PG_CATALOG.pg_postmaster_start_time() is 'postmaster start time';
comment on function PG_CATALOG.pg_prepared_statement() is 'get the prepared statements for this session';
comment on function PG_CATALOG.pg_prepared_xact() is 'view two-phase transactions';
comment on function PG_CATALOG.pg_read_binary_file(text) is 'read bytea from a file';
comment on function PG_CATALOG.pg_read_binary_file(text, bigint, bigint, boolean) is 'read bytea from a file';
comment on function PG_CATALOG.pg_read_file(text, bigint, bigint) is 'read text from a file - old version for adminpack 1.0';
comment on function PG_CATALOG.pg_read_file(text) is 'read text from a file';
comment on function PG_CATALOG.pg_relation_filenode(regclass) is 'filenode identifier of relation';
comment on function PG_CATALOG.pg_relation_filepath(regclass) is 'file path of relation';
comment on function PG_CATALOG.pg_relation_size(regclass, text) is 'disk space usage for the specified fork of a table or index';
comment on function PG_CATALOG.pg_reload_conf() is 'reload configuration files';
comment on function PG_CATALOG.pg_replication_slot_advance(slot_name name, upto_lsn text) is 'advance logical replication slot';
comment on function PG_CATALOG.pg_rotate_logfile() is 'rotate log file - old version for adminpack 1.0';
comment on function PG_CATALOG.pg_sequence_parameters(sequence_oid oid) is 'sequence parameters, for use by information schema';
comment on function PG_CATALOG.pg_show_all_settings() is 'SHOW ALL as a function';
comment on function PG_CATALOG.pg_size_pretty(bigint) is 'convert a long int to a human readable text using size units';
comment on function PG_CATALOG.pg_size_pretty(numeric) is 'convert a numeric to a human readable text using size units';
comment on function PG_CATALOG.pg_sleep(double precision) is 'sleep for the specified time in seconds';
comment on function PG_CATALOG.pg_start_backup(label text, fast boolean) is 'prepare for taking an online backup';
comment on function PG_CATALOG.pg_stat_clear_snapshot() is 'statistics: discard current transaction\''s statistics snapshot';
comment on function PG_CATALOG.pg_stat_file(filename text) is 'get information about file';
comment on function PG_CATALOG.pg_stat_get_numscans(oid) is 'statistics: number of scans done for table/index';
comment on function PG_CATALOG.pg_stat_get_activity(pid bigint) is 'statistics: information about currently active backends';
comment on function PG_CATALOG.pg_stat_get_analyze_count(oid) is 'statistics: number of manual analyzes for a table';
comment on function PG_CATALOG.pg_stat_get_autoanalyze_count(oid) is 'statistics: number of auto analyzes for a table';
comment on function PG_CATALOG.pg_stat_get_autovacuum_count(oid) is 'statistics: number of auto vacuums for a table';
comment on function PG_CATALOG.pg_stat_get_backend_activity(integer) is 'statistics: current query of backend';
comment on function PG_CATALOG.pg_stat_get_backend_activity_start(integer) is 'statistics: start time for current query of backend';
comment on function PG_CATALOG.pg_stat_get_backend_client_addr(integer) is 'statistics: address of client connected to backend';
comment on function PG_CATALOG.pg_stat_get_backend_client_port(integer) is 'statistics: port number of client connected to backend';
comment on function PG_CATALOG.pg_stat_get_backend_dbid(integer) is 'statistics: database ID of backend';
comment on function PG_CATALOG.pg_stat_get_backend_idset() is 'statistics: currently active backend IDs';
comment on function PG_CATALOG.pg_stat_get_backend_pid(integer) is 'statistics: PID of backend';
comment on function PG_CATALOG.pg_stat_get_backend_start(integer) is 'statistics: start time for current backend session';
comment on function PG_CATALOG.pg_stat_get_backend_userid(integer) is 'statistics: user ID of backend';
comment on function PG_CATALOG.pg_stat_get_backend_xact_start(integer) is 'statistics: start time for backend\''s current transaction';
comment on function PG_CATALOG.pg_stat_get_bgwriter_buf_written_checkpoints() is 'statistics: number of buffers written by the bgwriter during checkpoints';
comment on function PG_CATALOG.pg_stat_get_bgwriter_buf_written_clean() is 'statistics: number of buffers written by the bgwriter for cleaning dirty buffers';
comment on function PG_CATALOG.pg_stat_get_bgwriter_maxwritten_clean() is 'statistics: number of times the bgwriter stopped processing when it had written too many buffers while cleaning';
comment on function PG_CATALOG.pg_stat_get_bgwriter_requested_checkpoints() is 'statistics: number of backend requested checkpoints started by the bgwriter';
comment on function PG_CATALOG.pg_stat_get_bgwriter_stat_reset_time() is 'statistics: last reset for the bgwriter';
comment on function PG_CATALOG.pg_stat_get_bgwriter_timed_checkpoints() is 'statistics: number of timed checkpoints started by the bgwriter';
comment on function PG_CATALOG.pg_stat_get_blocks_fetched(oid) is 'statistics: number of blocks fetched';
comment on function PG_CATALOG.pg_stat_get_blocks_hit(oid) is 'statistics: number of blocks found in cache';
comment on function PG_CATALOG.pg_stat_get_buf_alloc() is 'statistics: number of buffer allocations';
comment on function PG_CATALOG.pg_stat_get_buf_fsync_backend() is 'statistics: number of backend buffer writes that did their own fsync';
comment on function PG_CATALOG.pg_stat_get_buf_written_backend() is 'statistics: number of buffers written by backends';
comment on function PG_CATALOG.pg_stat_get_checkpoint_sync_time() is 'statistics: checkpoint time spent synchronizing buffers to disk, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_checkpoint_write_time() is 'statistics: checkpoint time spent writing buffers to disk, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_db_blk_read_time(oid) is 'statistics: block read time, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_db_blk_write_time(oid) is 'statistics: block write time, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_db_blocks_fetched(oid) is 'statistics: blocks fetched for database';
comment on function PG_CATALOG.pg_stat_get_db_blocks_hit(oid) is 'statistics: blocks found in cache for database';
comment on function PG_CATALOG.pg_stat_get_db_conflict_all(oid) is 'statistics: recovery conflicts in database';
comment on function PG_CATALOG.pg_stat_get_db_conflict_bufferpin(oid) is 'statistics: recovery conflicts in database caused by shared buffer pin';
comment on function PG_CATALOG.pg_stat_get_db_conflict_lock(oid) is 'statistics: recovery conflicts in database caused by relation lock';
comment on function PG_CATALOG.pg_stat_get_db_conflict_snapshot(oid) is 'statistics: recovery conflicts in database caused by snapshot expiry';
comment on function PG_CATALOG.pg_stat_get_db_conflict_startup_deadlock(oid) is 'statistics: recovery conflicts in database caused by buffer deadlock';
comment on function PG_CATALOG.pg_stat_get_db_conflict_tablespace(oid) is 'statistics: recovery conflicts in database caused by drop tablespace';
comment on function PG_CATALOG.pg_stat_get_db_deadlocks(oid) is 'statistics: deadlocks detected in database';
comment on function PG_CATALOG.pg_stat_get_db_numbackends(oid) is 'statistics: number of backends in database';
comment on function PG_CATALOG.pg_stat_get_db_stat_reset_time(oid) is 'statistics: last reset for a database';
comment on function PG_CATALOG.pg_stat_get_db_temp_bytes(oid) is 'statistics: number of bytes in temporary files written';
comment on function PG_CATALOG.pg_stat_get_db_temp_files(oid) is 'statistics: number of temporary files written';
comment on function PG_CATALOG.pg_stat_get_db_tuples_deleted(oid) is 'statistics: tuples deleted in database';
comment on function PG_CATALOG.pg_stat_get_db_tuples_fetched(oid) is 'statistics: tuples fetched for database';
comment on function PG_CATALOG.pg_stat_get_db_tuples_inserted(oid) is 'statistics: tuples inserted in database';
comment on function PG_CATALOG.pg_stat_get_db_tuples_returned(oid) is 'statistics: tuples returned for database';
comment on function PG_CATALOG.pg_stat_get_db_tuples_updated(oid) is 'statistics: tuples updated in database';
comment on function PG_CATALOG.pg_stat_get_db_xact_commit(oid) is 'statistics: transactions committed';
comment on function PG_CATALOG.pg_stat_get_db_xact_rollback(oid) is 'statistics: transactions rolled back';
comment on function PG_CATALOG.pg_stat_get_dead_tuples(oid) is 'statistics: number of dead tuples';
comment on function PG_CATALOG.pg_stat_get_function_calls(oid) is 'statistics: number of function calls';
comment on function PG_CATALOG.pg_stat_get_function_self_time(oid) is 'statistics: self execution time of function, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_function_total_time(oid) is 'statistics: total execution time of function, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_last_analyze_time(oid) is 'statistics: last manual analyze time for a table';
comment on function PG_CATALOG.pg_stat_get_last_autoanalyze_time(oid) is 'statistics: last auto analyze time for a table';
comment on function PG_CATALOG.pg_stat_get_last_autovacuum_time(oid) is 'statistics: last auto vacuum time for a table';
comment on function PG_CATALOG.pg_stat_get_last_data_changed_time(oid) is 'statistics: last manual vacuum time for a table';
comment on function PG_CATALOG.pg_stat_get_last_vacuum_time(oid) is 'statistics: last manual vacuum time for a table';
comment on function PG_CATALOG.pg_stat_get_live_tuples(oid) is 'statistics: number of live tuples';
comment on function PG_CATALOG.pg_stat_get_tuples_deleted(oid) is 'statistics: number of tuples deleted';
comment on function PG_CATALOG.pg_stat_get_tuples_fetched(oid) is 'statistics: number of tuples fetched by idxscan';
comment on function PG_CATALOG.pg_stat_get_tuples_hot_updated(oid) is 'statistics: number of tuples hot updated';
comment on function PG_CATALOG.pg_stat_get_tuples_inserted(oid) is 'statistics: number of tuples inserted';
comment on function PG_CATALOG.pg_stat_get_tuples_returned(oid) is 'statistics: number of tuples read by seqscan';
comment on function PG_CATALOG.pg_stat_get_tuples_updated(oid) is 'statistics: number of tuples updated';
comment on function PG_CATALOG.pg_stat_get_vacuum_count(oid) is 'statistics: number of manual vacuums for a table';
comment on function PG_CATALOG.pg_stat_get_wal_receiver() is 'statistics: information about WAL receiver';
comment on function PG_CATALOG.pg_stat_get_wal_senders() is 'statistics: information about currently active replication';
comment on function PG_CATALOG.pg_stat_get_xact_blocks_fetched(oid) is 'statistics: number of blocks fetched in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_blocks_hit(oid) is 'statistics: number of blocks found in cache in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_function_calls(oid) is 'statistics: number of function calls in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_function_self_time(oid) is 'statistics: self execution time of function in current transaction, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_xact_function_total_time(oid) is 'statistics: total execution time of function in current transaction, in milliseconds';
comment on function PG_CATALOG.pg_stat_get_xact_numscans(oid) is 'statistics: number of scans done for table/index in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_deleted(oid) is 'statistics: number of tuples deleted in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_fetched(oid) is 'statistics: number of tuples fetched by idxscan in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_hot_updated(oid) is 'statistics: number of tuples hot updated in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_inserted(oid) is 'statistics: number of tuples inserted in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_returned(oid) is 'statistics: number of tuples read by seqscan in current transaction';
comment on function PG_CATALOG.pg_stat_get_xact_tuples_updated(oid) is 'statistics: number of tuples updated in current transaction';
comment on function PG_CATALOG.pg_stat_reset() is 'statistics: reset collected statistics for current database';
comment on function PG_CATALOG.pg_stat_reset_shared(text) is 'statistics: reset collected statistics shared across the cluster';
comment on function PG_CATALOG.pg_stat_reset_single_function_counters(oid) is 'statistics: reset collected statistics for a single function in the current database';
comment on function PG_CATALOG.pg_stat_reset_single_table_counters(oid) is 'statistics: reset collected statistics for a single table or index in the current database';
comment on function PG_CATALOG.pg_table_is_visible(oid) is 'is table visible in search path?';
comment on function PG_CATALOG.pg_table_size(regclass) is 'disk space usage for the specified table, including TOAST, free space and visibility map';
comment on function PG_CATALOG.pg_tablespace_databases(oid) is 'get OIDs of databases in a tablespace';
comment on function PG_CATALOG.pg_tablespace_location(oid) is 'tablespace location';
comment on function PG_CATALOG.pg_tablespace_size(name) is 'total disk space usage for the specified tablespace';
comment on function PG_CATALOG.pg_tablespace_size(oid) is 'total disk space usage for the specified tablespace';
comment on function PG_CATALOG.pg_terminate_backend(bigint) is 'terminate a server process';
comment on function PG_CATALOG.pg_timezone_abbrevs() is 'get the available time zone abbreviations';
comment on function PG_CATALOG.pg_timezone_names() is 'get the available time zone names';
comment on function PG_CATALOG.pg_total_relation_size(regclass) is 'total disk space usage for the specified table and associated indexes';
comment on function PG_CATALOG.pg_trigger_depth() is 'current trigger depth';
comment on function PG_CATALOG.pg_try_advisory_lock(bigint) is 'obtain exclusive advisory lock if available';
comment on function PG_CATALOG.pg_try_advisory_lock(integer, integer) is 'obtain exclusive advisory lock if available';
comment on function PG_CATALOG.pg_try_advisory_lock_shared(bigint) is 'obtain shared advisory lock if available';
comment on function PG_CATALOG.pg_try_advisory_lock_shared(integer, integer) is 'obtain shared advisory lock if available';
comment on function PG_CATALOG.pg_try_advisory_xact_lock(bigint) is 'obtain exclusive advisory lock if available';
comment on function PG_CATALOG.pg_try_advisory_xact_lock(integer, integer) is 'obtain exclusive advisory lock if available';
comment on function PG_CATALOG.pg_try_advisory_xact_lock_shared(bigint) is 'obtain shared advisory lock if available';
comment on function PG_CATALOG.pg_try_advisory_xact_lock_shared(integer, integer) is 'obtain shared advisory lock if available';
comment on function PG_CATALOG.pg_ts_config_is_visible(oid) is 'is text search configuration visible in search path?';
comment on function PG_CATALOG.pg_ts_dict_is_visible(oid) is 'is text search dictionary visible in search path?';
comment on function PG_CATALOG.pg_ts_parser_is_visible(oid) is 'is text search parser visible in search path?';
comment on function PG_CATALOG.pg_ts_template_is_visible(oid) is 'is text search template visible in search path?';
comment on function PG_CATALOG.pg_type_is_visible(oid) is 'is type visible in search path?';
comment on function PG_CATALOG.pg_typeof("any") is 'type of the argument';
comment on function PG_CATALOG.pi() is 'PI';
comment on function PG_CATALOG.plainto_tsquery(regconfig, text) is 'transform to tsquery';
comment on function PG_CATALOG.plainto_tsquery(text) is 'transform to tsquery';
comment on function PG_CATALOG.point(box) is 'center of';
comment on function PG_CATALOG.point(double precision, double precision) is 'convert x, y to point';
comment on function PG_CATALOG.point(circle) is 'center of';
comment on function PG_CATALOG.point(lseg) is 'center of';
comment on function PG_CATALOG.point(path) is 'center of';
comment on function PG_CATALOG.point(polygon) is 'center of';
comment on function PG_CATALOG.point_in(cstring) is 'I/O';
comment on function PG_CATALOG.point_out(point) is 'I/O';
comment on function PG_CATALOG.point_send(point) is 'I/O';
comment on function PG_CATALOG.point_vert(point, point) is 'vertically aligned';
comment on function PG_CATALOG.poly_center(polygon) is 'center of';
comment on function PG_CATALOG.poly_in(cstring) is 'I/O';
comment on function PG_CATALOG.poly_npoints(polygon) is 'number of points';
comment on function PG_CATALOG.poly_out(polygon) is 'I/O';
comment on function PG_CATALOG.poly_recv(internal) is 'I/O';
comment on function PG_CATALOG.polygon(box) is 'convert box to polygon';
comment on function PG_CATALOG.polygon(path) is 'convert path to polygon';
comment on function PG_CATALOG.polygon(integer, circle) is 'convert vertex count and circle to polygon';
comment on function PG_CATALOG.polygon(circle) is 'convert circle to 12-vertex polygon';
comment on function PG_CATALOG.popen(path) is 'open path';
comment on function PG_CATALOG.position(text, text) is 'position of substring';
comment on function PG_CATALOG.position(bit, bit) is 'position of sub-bitstring';
comment on function PG_CATALOG.position(bytea, bytea) is 'position of substring';
comment on function PG_CATALOG.positionjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity for position-comparison operators';
comment on function PG_CATALOG.positionsel(internal, oid, internal, integer) is 'restriction selectivity for position-comparison operators';
comment on function PG_CATALOG.postgresql_fdw_validator(text[], oid) is '(internal)';
comment on function PG_CATALOG.pow(double precision, double precision) is 'exponentiation';
comment on function PG_CATALOG.pow(numeric, numeric) is 'exponentiation';
comment on function PG_CATALOG.power(double precision, double precision) is 'exponentiation';
comment on function PG_CATALOG.power(numeric, numeric) is 'exponentiation';
comment on function PG_CATALOG.prsd_end(internal) is '(internal)';
comment on function PG_CATALOG.prsd_headline(internal, internal, tsquery) is '(internal)';
comment on function PG_CATALOG.prsd_lextype(internal) is '(internal)';
comment on function PG_CATALOG.prsd_nexttoken(internal, internal, internal) is '(internal)';
comment on function PG_CATALOG.prsd_start(internal, integer, oid) is '(internal)';
comment on function PG_CATALOG.query_to_xml(query text, nulls boolean, tableforest boolean, targetns text) is 'map query result to XML';
comment on function PG_CATALOG.query_to_xml_and_xmlschema(query text, nulls boolean, tableforest boolean, targetns text) is 'map query result and structure to XML and XML Schema';
comment on function PG_CATALOG.query_to_xmlschema(query text, nulls boolean, tableforest boolean, targetns text) is 'map query result structure to XML Schema';
comment on function PG_CATALOG.querytree(tsquery) is 'show real useful query for GiST index';
comment on function PG_CATALOG.quote_ident(text) is 'quote an identifier for usage in a querystring';
comment on function PG_CATALOG.quote_literal(anyelement) is 'quote a data value for usage in a querystring';
comment on function PG_CATALOG.quote_literal(text) is 'quote a literal for usage in a querystring';
comment on function PG_CATALOG.quote_nullable(anyelement) is 'quote a possibly-null data value for usage in a querystring';
comment on function PG_CATALOG.quote_nullable(text) is 'quote a possibly-null literal for usage in a querystring';
comment on function PG_CATALOG.radians(double precision) is 'degrees to radians';
comment on function PG_CATALOG.radius(circle) is 'radius of circle';
comment on function PG_CATALOG.random() is 'random value';
comment on function PG_CATALOG.range_cmp(anyrange, anyrange) is 'less-equal-greater';
comment on function PG_CATALOG.range_gist_consistent(internal, anyrange, integer, oid, internal) is 'GiST support';
comment on function PG_CATALOG.range_gist_penalty(internal, internal, internal) is 'GiST support';
comment on function PG_CATALOG.range_gist_picksplit(internal, internal) is 'GiST support';
comment on function PG_CATALOG.range_gist_same(anyrange, anyrange, internal) is 'GiST support';
comment on function PG_CATALOG.range_gist_union(internal, internal) is 'GiST support';
comment on function PG_CATALOG.range_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.range_out(anyrange) is 'I/O';
comment on function PG_CATALOG.range_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.range_send(anyrange) is 'I/O';
comment on function PG_CATALOG.range_typanalyze(internal) is 'range typanalyze';
comment on function PG_CATALOG.rank() is 'integer rank with gaps';
comment on function PG_CATALOG.rawrecv(internal) is 'I/O';
comment on function PG_CATALOG.rawsend(raw) is 'I/O';
comment on function PG_CATALOG.record_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.record_out(record) is 'I/O';
comment on function PG_CATALOG.record_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.record_send(record) is 'I/O';
comment on function PG_CATALOG.regclass(text) is 'convert text to regclass';
comment on function PG_CATALOG.regclassin(cstring) is 'I/O';
comment on function PG_CATALOG.regclassout(regclass) is 'I/O';
comment on function PG_CATALOG.regclassrecv(internal) is 'I/O';
comment on function PG_CATALOG.regclasssend(regclass) is 'I/O';
comment on function PG_CATALOG.regconfigin(cstring) is 'I/O';
comment on function PG_CATALOG.regconfigout(regconfig) is 'I/O';
comment on function PG_CATALOG.regconfigrecv(internal) is 'I/O';
comment on function PG_CATALOG.regconfigsend(regconfig) is 'I/O';
comment on function PG_CATALOG.regdictionaryin(cstring) is 'I/O';
comment on function PG_CATALOG.regdictionaryout(regdictionary) is 'I/O';
comment on function PG_CATALOG.regdictionaryrecv(internal) is 'I/O';
comment on function PG_CATALOG.regdictionarysend(regdictionary) is 'I/O';
comment on function PG_CATALOG.regexeqjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of regex match';
comment on function PG_CATALOG.regexnejoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of regex non-match';
comment on function PG_CATALOG.regexnesel(internal, oid, internal, integer) is 'restriction selectivity of regex non-match';
comment on function PG_CATALOG.regexp_matches(text, text) is 'find match(es) for regexp';
comment on function PG_CATALOG.regexp_matches(text, text, text) is 'find match(es) for regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text, text) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_split_to_array(text, text) is 'split string by pattern';
comment on function PG_CATALOG.regexp_split_to_array(text, text, text) is 'split string by pattern';
comment on function PG_CATALOG.regexp_split_to_table(text, text) is 'split string by pattern';
comment on function PG_CATALOG.regexp_split_to_table(text, text, text) is 'split string by pattern';
comment on function PG_CATALOG.regoperatorin(cstring) is 'I/O';
comment on function PG_CATALOG.regoperatorout(regoperator) is 'I/O';
comment on function PG_CATALOG.regoperatorrecv(internal) is 'I/O';
comment on function PG_CATALOG.regoperatorsend(regoperator) is 'I/O';
comment on function PG_CATALOG.regoperin(cstring) is 'I/O';
comment on function PG_CATALOG.regoperout(regoper) is 'I/O';
comment on function PG_CATALOG.regopersend(regoper) is 'I/O';
comment on function PG_CATALOG.regprocedurein(cstring) is 'I/O';
comment on function PG_CATALOG.regprocedureout(regprocedure) is 'I/O';
comment on function PG_CATALOG.regprocedurerecv(internal) is 'I/O';
comment on function PG_CATALOG.regproceduresend(regprocedure) is 'I/O';
comment on function PG_CATALOG.regprocin(cstring) is 'I/O';
comment on function PG_CATALOG.regprocout(regproc) is 'I/O';
comment on function PG_CATALOG.regprocrecv(internal) is 'I/O';
comment on function PG_CATALOG.regprocsend(regproc) is 'I/O';
comment on function PG_CATALOG.regr_avgx(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_avgy(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_count(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_intercept(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_r2(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_slope(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_sxx(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_sxy(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regr_syy(double precision, double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.regtypein(cstring) is 'I/O';
comment on function PG_CATALOG.regtypeout(regtype) is 'I/O';
comment on function PG_CATALOG.regtyperecv(internal) is 'I/O';
comment on function PG_CATALOG.regtypesend(regtype) is 'I/O';
comment on function PG_CATALOG.repeat(text, integer) is 'replicate string n times';
comment on function PG_CATALOG.replace(text, text, text) is 'replace all occurrences in string of old_substr with new_substr';
comment on function PG_CATALOG.reverse(text) is 'reverse text';
comment on function PG_CATALOG.right(text, integer) is 'extract the last n characters';
comment on function PG_CATALOG.round(numeric, integer) is 'value rounded to \''scale\''';
comment on function PG_CATALOG.round(double precision) is 'round to nearest integer';
comment on function PG_CATALOG.round(numeric) is 'value rounded to \''scale\'' of zero';
comment on function PG_CATALOG.row_number() is 'row number within partition';
comment on function PG_CATALOG.row_to_json(record) is 'map row to json';
comment on function PG_CATALOG.row_to_json(record, boolean) is 'map row to json with optional pretty printing';
comment on function PG_CATALOG.rpad(text, integer) is 'right-pad string to length';
comment on function PG_CATALOG.rtrim(text) is 'trim spaces from right end of string';
comment on function PG_CATALOG.rtrim(text, text) is 'trim selected characters from right end of string';
comment on function PG_CATALOG.scalargtjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of > and related operators on scalar datatypes';
comment on function PG_CATALOG.scalargtsel(internal, oid, internal, integer) is 'restriction selectivity of > and related operators on scalar datatypes';
comment on function PG_CATALOG.scalarltjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of < and related operators on scalar datatypes';
comment on function PG_CATALOG.scalarltsel(internal, oid, internal, integer) is 'restriction selectivity of < and related operators on scalar datatypes';
comment on function PG_CATALOG.schema_to_xml(schema name, nulls boolean, tableforest boolean, targetns text) is 'map schema contents to XML';
comment on function PG_CATALOG.schema_to_xml_and_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text) is 'map schema contents and structure to XML and XML Schema';
comment on function PG_CATALOG.schema_to_xmlschema(schema name, nulls boolean, tableforest boolean, targetns text) is 'map schema structure to XML Schema';
comment on function PG_CATALOG.session_user() is 'session user name';
comment on function PG_CATALOG.set_bit(bit, integer, integer) is 'set bit';
comment on function PG_CATALOG.set_bit(bytea, integer, integer) is 'set bit';
comment on function PG_CATALOG.set_byte(bytea, integer, integer) is 'set byte';
comment on function PG_CATALOG.set_config(text, text, boolean) is 'SET X as a function';
comment on function PG_CATALOG.set_masklen(cidr, integer) is 'change netmask of cidr';
comment on function PG_CATALOG.set_masklen(inet, integer) is 'change netmask of inet';
comment on function PG_CATALOG.setseed(double precision) is 'set random seed';
comment on function PG_CATALOG.setval(regclass, numeric) is 'set sequence value';
comment on function PG_CATALOG.setval(regclass, numeric, boolean) is 'set sequence value and is_called status';
comment on function PG_CATALOG.setweight(tsvector, "char") is 'set given weight for whole tsvector';
comment on function PG_CATALOG.shell_in(cstring) is 'I/O';
comment on function PG_CATALOG.shell_out(opaque) is 'I/O';
comment on function PG_CATALOG.shobj_description(oid, name) is 'get description for object id and shared catalog name';
comment on function PG_CATALOG.similar_escape(text, text) is 'convert SQL regexp pattern to POSIX style';
comment on function PG_CATALOG.sin(double precision) is 'sine';
comment on function PG_CATALOG.slope(point, point) is 'slope between points';
comment on function PG_CATALOG.spg_kd_choose(internal, internal) is 'SP-GiST support for k-d tree over point';
comment on function PG_CATALOG.spg_kd_config(internal, internal) is 'SP-GiST support for k-d tree over point';
comment on function PG_CATALOG.spg_kd_inner_consistent(internal, internal) is 'SP-GiST support for k-d tree over point';
comment on function PG_CATALOG.spg_kd_picksplit(internal, internal) is 'SP-GiST support for k-d tree over point';
comment on function PG_CATALOG.spg_quad_choose(internal, internal) is 'SP-GiST support for quad tree over point';
comment on function PG_CATALOG.spg_quad_config(internal, internal) is 'SP-GiST support for quad tree over point';
comment on function PG_CATALOG.spg_quad_inner_consistent(internal, internal) is 'SP-GiST support for quad tree over point';
comment on function PG_CATALOG.spg_quad_leaf_consistent(internal, internal) is 'SP-GiST support for quad tree and k-d tree over point';
comment on function PG_CATALOG.spg_quad_picksplit(internal, internal) is 'SP-GiST support for quad tree over point';
comment on function PG_CATALOG.spg_text_choose(internal, internal) is 'SP-GiST support for radix tree over text';
comment on function PG_CATALOG.spg_text_config(internal, internal) is 'SP-GiST support for radix tree over text';
comment on function PG_CATALOG.spg_text_inner_consistent(internal, internal) is 'SP-GiST support for radix tree over text';
comment on function PG_CATALOG.spg_text_leaf_consistent(internal, internal) is 'SP-GiST support for radix tree over text';
comment on function PG_CATALOG.spg_text_picksplit(internal, internal) is 'SP-GiST support for radix tree over text';
comment on function PG_CATALOG.split_part(text, text, integer) is 'split string by field_sep and return field_num';
comment on function PG_CATALOG.sqrt(numeric) is 'square root';
comment on function PG_CATALOG.statement_timestamp() is 'current statement time';
comment on function PG_CATALOG.stddev(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_pop(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_pop(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_pop(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_pop(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_pop(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_pop(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_samp(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_samp(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_samp(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_samp(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_samp(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.stddev_samp(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.string_agg(bytea, bytea) is 'concatenate aggregate input into a string';
comment on function PG_CATALOG.string_agg(text, text) is 'concatenate aggregate input into a string';
comment on function PG_CATALOG.string_agg_finalfn(internal) is 'aggregate final function';
comment on function PG_CATALOG.string_agg_transfn(internal, text, text) is 'aggregate transition function';
comment on function PG_CATALOG.string_to_array(text, text) is 'split delimited text into text[]';
comment on function PG_CATALOG.string_to_array(text, text, text) is 'concatenate array elements, using delimiter, into text';
comment on function PG_CATALOG.strip(tsvector) is 'strip position information';
comment on function PG_CATALOG.substring(bit, integer) is 'extract portion of bitstring';
comment on function PG_CATALOG.substring(text, text) is 'extract text matching regular expression';
comment on function PG_CATALOG.substring(bytea, integer, integer) is 'extract portion of string';
comment on function PG_CATALOG.substring(bytea, integer) is 'extract portion of string';
comment on function PG_CATALOG.substring(bit, integer, integer) is 'extract portion of bitstring';
comment on function PG_CATALOG.substring_inner(text, integer) is 'extract portion of string';
comment on function PG_CATALOG.substring_inner(text, integer, integer) is 'extract portion of string';
comment on function PG_CATALOG.sum(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.sum(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.sum(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.sum(interval) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.sum(money) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.sum(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.sum(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.sum(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.suppress_redundant_updates_trigger() is 'trigger to suppress updates when new and old records match';
comment on function PG_CATALOG.table_to_xml(tbl regclass, nulls boolean, tableforest boolean, targetns text) is 'map table contents to XML';
comment on function PG_CATALOG.table_to_xml_and_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text) is 'map table contents and structure to XML and XML Schema';
comment on function PG_CATALOG.table_to_xmlschema(tbl regclass, nulls boolean, tableforest boolean, targetns text) is 'map table structure to XML Schema';
comment on function PG_CATALOG.tan(double precision) is 'tangent';
comment on function PG_CATALOG.text("char") is 'convert char to text';
comment on function PG_CATALOG.text(name) is 'convert name to text';
comment on function PG_CATALOG.text(inet) is 'show all parts of inet/cidr value';
comment on function PG_CATALOG.text(character) is 'convert char(n) to text';
comment on function PG_CATALOG.text(boolean) is 'convert boolean to text';
comment on function PG_CATALOG.text_larger(text, text) is 'larger of two';
comment on function PG_CATALOG.text_smaller(text, text) is 'smaller of two';
comment on function PG_CATALOG.textin(cstring) is 'I/O';
comment on function PG_CATALOG.textlen(text) is 'length';
comment on function PG_CATALOG.textlike(text, text) is 'matches LIKE expression';
comment on function PG_CATALOG.textnlike(text, text) is 'does not match LIKE expression';
comment on function PG_CATALOG.textout(text) is 'I/O';
comment on function PG_CATALOG.textrecv(internal) is 'I/O';
comment on function PG_CATALOG.textsend(text) is 'I/O';
comment on function PG_CATALOG.thesaurus_init(internal) is '(internal)';
comment on function PG_CATALOG.thesaurus_lexize() is '(internal)';
comment on function PG_CATALOG.tidin(cstring) is 'I/O';
comment on function PG_CATALOG.tidlarger(tid, tid) is 'larger of two';
comment on function PG_CATALOG.tidout(tid) is 'I/O';
comment on function PG_CATALOG.tidrecv(internal) is 'I/O';
comment on function PG_CATALOG.tidsend(tid) is 'I/O';
comment on function PG_CATALOG.tidsmaller(tid, tid) is 'smaller of two';
comment on function PG_CATALOG.time(interval) is 'convert interval to time';
comment on function PG_CATALOG.time(time without time zone, integer) is 'adjust time precision';
comment on function PG_CATALOG.time(timestamp without time zone) is 'convert timestamp to time';
comment on function PG_CATALOG.time(timestamp with time zone) is 'convert timestamp with time zone to time';
comment on function PG_CATALOG.time(time with time zone) is 'convert time with time zone to time';
comment on function PG_CATALOG.time_cmp(time without time zone, time without time zone) is 'less-equal-greater';
comment on function PG_CATALOG.time_hash(time without time zone) is 'hash';
comment on function PG_CATALOG.time_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.time_larger(time without time zone, time without time zone) is 'larger of two';
comment on function PG_CATALOG.time_out(time without time zone) is 'I/O';
comment on function PG_CATALOG.time_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.time_send(time without time zone) is 'I/O';
comment on function PG_CATALOG.time_smaller(time without time zone, time without time zone) is 'smaller of two';
comment on function PG_CATALOG.timeofday() is 'current date and time - increments during transactions';
comment on function PG_CATALOG.timestamp(timestamp without time zone, integer) is 'adjust timestamp precision';
comment on function PG_CATALOG.timestamp(timestamp with time zone) is 'convert timestamp with time zone to timestamp';
comment on function PG_CATALOG.timestamp_cmp(timestamp without time zone, timestamp without time zone) is 'less-equal-greater';
comment on function PG_CATALOG.timestamp_cmp_timestamptz(timestamp without time zone, timestamp with time zone) is 'less-equal-greater';
comment on function PG_CATALOG.timestamp_hash(timestamp without time zone) is 'hash';
comment on function PG_CATALOG.timestamp_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.timestamp_larger(timestamp without time zone, timestamp without time zone) is 'larger of two';
comment on function PG_CATALOG.timestamp_out(timestamp without time zone) is 'I/O';
comment on function PG_CATALOG.timestamp_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.timestamp_send(timestamp without time zone) is 'I/O';
comment on function PG_CATALOG.timestamp_smaller(timestamp without time zone, timestamp without time zone) is 'smaller of two';
comment on function PG_CATALOG.timestamp_sortsupport(internal) is 'sort support';
comment on function PG_CATALOG.timestamptypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.timestamptypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.timestamptz(date) is 'convert date to timestamp with time zone';
comment on function PG_CATALOG.timestamptz(timestamp with time zone, integer) is 'adjust timestamptz precision';
comment on function PG_CATALOG.timestamptz(timestamp without time zone) is 'convert timestamp to timestamp with time zone';
comment on function PG_CATALOG.timestamptz_cmp(timestamp with time zone, timestamp with time zone) is 'less-equal-greater';
comment on function PG_CATALOG.timestamptz_cmp_timestamp(timestamp with time zone, timestamp without time zone) is 'less-equal-greater';
comment on function PG_CATALOG.timestamptz_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.timestamptz_larger(timestamp with time zone, timestamp with time zone) is 'larger of two';
comment on function PG_CATALOG.timestamptz_out(timestamp with time zone) is 'I/O';
comment on function PG_CATALOG.timestamptz_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.timestamptz_send(timestamp with time zone) is 'I/O';
comment on function PG_CATALOG.timestamptz_smaller(timestamp with time zone, timestamp with time zone) is 'smaller of two';
comment on function PG_CATALOG.timestamptztypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.timestamptztypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.timetypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.timetypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.timetz(time with time zone, integer) is 'adjust time with time zone precision';
comment on function PG_CATALOG.timetz(time without time zone) is 'convert timestamp with time zone to time with time zone';
comment on function PG_CATALOG.timetz(timestamp with time zone) is 'convert time to time with time zone';
comment on function PG_CATALOG.timetz_cmp(time with time zone, time with time zone) is 'less-equal-greater';
comment on function PG_CATALOG.timetz_hash(time with time zone) is 'hash';
comment on function PG_CATALOG.timetz_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.timetz_larger(time with time zone, time with time zone) is 'larger of two';
comment on function PG_CATALOG.timetz_out(time with time zone) is 'I/O';
comment on function PG_CATALOG.timetz_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.timetz_send(time with time zone) is 'I/O';
comment on function PG_CATALOG.timetz_smaller(time with time zone, time with time zone) is 'smaller of two';
comment on function PG_CATALOG.timetztypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.timetztypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.timezone(interval, timestamp with time zone) is 'adjust timestamp to new time zone';
comment on function PG_CATALOG.timezone(interval, timestamp without time zone) is 'adjust timestamp to new time zone';
comment on function PG_CATALOG.timezone(interval, time with time zone) is 'adjust time with time zone to new zone';
comment on function PG_CATALOG.timezone(text, time with time zone ) is 'adjust time with time zone to new zone';
comment on function PG_CATALOG.timezone(text, timestamp with time zone) is 'adjust timestamp to new time zone';
comment on function PG_CATALOG.timezone(text, timestamp without time zone) is 'adjust timestamp to new time zone';
comment on function PG_CATALOG.to_ascii(text) is 'encode text from DB encoding to ASCII text';
comment on function PG_CATALOG.to_ascii(text, integer) is 'encode text from encoding to ASCII text';
comment on function PG_CATALOG.to_ascii(text, name) is 'encode text from encoding to ASCII text';
comment on function PG_CATALOG.to_char(interval, text) is 'format interval to text';
comment on function PG_CATALOG.to_char(timestamp with time zone, text) is 'format timestamp with time zone to text';
comment on function PG_CATALOG.to_char(numeric, text) is 'format numeric to text';
comment on function PG_CATALOG.to_char(integer, text) is 'format int4 to text';
comment on function PG_CATALOG.to_char(bigint, text) is 'format int8 to text';
comment on function PG_CATALOG.to_char(real, text) is 'format float4 to text';
comment on function PG_CATALOG.to_char(double precision, text) is 'format float8 to text';
comment on function PG_CATALOG.to_char(timestamp with time zone, text) is 'format timestamp to text';
comment on function PG_CATALOG.to_date(text, text) is 'convert text to timestamp with time zone';
comment on function PG_CATALOG.to_hex(bigint) is 'convert int8 number to hex';
comment on function PG_CATALOG.to_hex(integer) is 'convert int4 number to hex';
comment on function PG_CATALOG.to_number(text, text) is 'convert text to numeric';
comment on function PG_CATALOG.to_timestamp(text, text) is 'convert text to timestamp with time zone';
comment on function PG_CATALOG.to_tsquery(regconfig, text) is 'make tsquery';
comment on function PG_CATALOG.to_tsquery(text) is 'make tsquery';
comment on function PG_CATALOG.to_tsvector(regconfig, text) is 'transform to tsvector';
comment on function PG_CATALOG.to_tsvector(text) is 'transform to tsvector';
comment on function PG_CATALOG.to_tsvector_for_batch(regconfig, text) is 'transform to tsvector';
comment on function PG_CATALOG.to_tsvector_for_batch(text) is 'transform to tsvector';
comment on function PG_CATALOG.transaction_timestamp() is 'current transaction time';
comment on function PG_CATALOG.translate(text, text, text) is 'map a set of characters appearing in string';
comment on function PG_CATALOG.trigger_in(cstring) is '"I/O';
comment on function PG_CATALOG.trigger_out(trigger) is '"I/O';
comment on function PG_CATALOG.trunc(macaddr) is 'MACADDR manufacturer fields';
comment on function PG_CATALOG.trunc(numeric, integer) is 'value truncated to \''scale\''';
comment on function PG_CATALOG.trunc(numeric) is 'value truncated to \''scale\'' of zero';
comment on function PG_CATALOG.trunc(double precision) is 'truncate to integer';
comment on function PG_CATALOG.ts_headline(regconfig, text, tsquery) is 'generate headline';
comment on function PG_CATALOG.ts_headline(regconfig, text, tsquery, text) is 'generate headline';
comment on function PG_CATALOG.ts_headline(text, tsquery) is 'generate headline';
comment on function PG_CATALOG.ts_headline(text, tsquery, text) is 'generate headline';
comment on function PG_CATALOG.ts_lexize(regdictionary, text) is 'normalize one word by dictionary';
comment on function PG_CATALOG.ts_parse(parser_name text, txt text) is 'parse text to tokens';
comment on function PG_CATALOG.ts_parse(parser_oid oid, txt text) is 'parse text to tokens';
comment on function PG_CATALOG.ts_rank(real[], tsvector, tsquery) is 'relevance';
comment on function PG_CATALOG.ts_rank(real[], tsvector, tsquery, integer) is 'relevance';
comment on function PG_CATALOG.ts_rank(tsvector, tsquery) is 'relevance';
comment on function PG_CATALOG.ts_rank(tsvector, tsquery, integer) is 'relevance';
comment on function PG_CATALOG.ts_rank_cd(real[], tsvector, tsquery) is 'relevance';
comment on function PG_CATALOG.ts_rank_cd(real[], tsvector, tsquery, integer) is 'relevance';
comment on function PG_CATALOG.ts_rank_cd(tsvector, tsquery) is 'relevance';
comment on function PG_CATALOG.ts_rank_cd(tsvector, tsquery, integer) is 'relevance';
comment on function PG_CATALOG.ts_rewrite(tsquery, text) is 'rewrite tsquery';
comment on function PG_CATALOG.ts_rewrite(tsquery, tsquery, tsquery) is 'rewrite tsquery';
comment on function PG_CATALOG.ts_stat(query text) is 'statistics of tsvector column';
comment on function PG_CATALOG.ts_stat(query text, weights text) is 'statistics of tsvector column';
comment on function PG_CATALOG.ts_token_type(parser_name text) is 'get parser\''s token types';
comment on function PG_CATALOG.ts_token_type(parser_oid oid) is 'get parser\''s token types';
comment on function PG_CATALOG.ts_typanalyze(internal) is 'tsvector typanalyze';
comment on function PG_CATALOG.tsmatchjoinsel(internal, oid, internal, smallint, internal) is 'join selectivity of tsvector @@ tsquery';
comment on function PG_CATALOG.tsmatchsel(internal, oid, internal, integer) is 'restriction selectivity of tsvector @@ tsquery';
comment on function PG_CATALOG.tsquery_cmp(tsquery, tsquery) is 'less-equal-greater';
comment on function PG_CATALOG.tsqueryin(cstring) is 'I/O';
comment on function PG_CATALOG.tsqueryout(tsquery) is 'I/O';
comment on function PG_CATALOG.tsqueryrecv(internal) is 'I/O';
comment on function PG_CATALOG.tsquerysend(tsquery) is 'I/O';
comment on function PG_CATALOG.tsrange(timestamp without time zone, timestamp without time zone) is 'tsrange constructor';
comment on function PG_CATALOG.tsrange(timestamp without time zone, timestamp without time zone, text) is 'tsrange constructor';
comment on function PG_CATALOG.tsrange_subdiff(timestamp without time zone, timestamp without time zone) is 'float8 difference of two timestamp values';
comment on function PG_CATALOG.tstzrange(timestamp with time zone, timestamp with time zone) is 'tstzrange constructor';
comment on function PG_CATALOG.tstzrange(timestamp with time zone, timestamp with time zone, text) is 'tstzrange constructor';
comment on function PG_CATALOG.tstzrange_subdiff(timestamp with time zone, timestamp with time zone) is 'float8 difference of two timestamp with time zone values';
comment on function PG_CATALOG.tsvector_cmp(tsvector, tsvector) is 'less-equal-greater';
comment on function PG_CATALOG.tsvector_update_trigger() is 'trigger for automatic update of tsvector column';
comment on function PG_CATALOG.tsvector_update_trigger_column() is 'trigger for automatic update of tsvector column';
comment on function PG_CATALOG.tsvectorin(cstring) is 'I/O';
comment on function PG_CATALOG.tsvectorout(tsvector) is 'I/O';
comment on function PG_CATALOG.tsvectorrecv(internal) is 'I/O';
comment on function PG_CATALOG.tsvectorsend(tsvector) is 'I/O';
comment on function PG_CATALOG.unique_key_recheck() is 'deferred UNIQUE constraint check';
comment on function PG_CATALOG.unknownin(cstring) is 'I/O';
comment on function PG_CATALOG.unknownout(unknown) is 'I/O';
comment on function PG_CATALOG.unknownrecv(internal) is 'I/O';
comment on function PG_CATALOG.unknownsend(unknown) is 'I/O';
comment on function PG_CATALOG.unnest(anyarray) is 'expand array to set of rows';
comment on function PG_CATALOG.upper(text) is 'uppercase';
comment on function PG_CATALOG.upper(anyrange) is 'upper bound of range';
comment on function PG_CATALOG.upper_inc(anyrange) is 'is the range\''s upper bound inclusive?';
comment on function PG_CATALOG.upper_inf(anyrange) is 'is the range\''s upper bound infinite?';
comment on function PG_CATALOG.uuid_cmp(uuid, uuid) is 'less-equal-greater';
comment on function PG_CATALOG.uuid_hash(uuid) is 'hash';
comment on function PG_CATALOG.uuid_in(cstring) is 'I/O';
comment on function PG_CATALOG.uuid_out(uuid) is 'I/O';
comment on function PG_CATALOG.uuid_recv(internal) is 'I/O';
comment on function PG_CATALOG.uuid_send(uuid) is 'I/O';
comment on function PG_CATALOG.var_pop(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_pop(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_pop(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_pop(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_pop(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_pop(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_samp(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_samp(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_samp(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_samp(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_samp(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.var_samp(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.varbit(bit varying, integer, boolean) is 'adjust varbit() to typmod length';
comment on function PG_CATALOG.varbit_in(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.varbit_out(bit varying) is 'I/O';
comment on function PG_CATALOG.varbit_recv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.varbit_send(bit varying) is 'I/O';
comment on function PG_CATALOG.varbitcmp(bit varying, bit varying) is 'less-equal-greater';
comment on function PG_CATALOG.varbittypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.varbittypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.varchar(character varying, integer, boolean) is 'adjust varchar() to typmod length';
comment on function PG_CATALOG.varchar(name) is 'convert name to varchar';
comment on function PG_CATALOG.varcharin(cstring, oid, integer) is 'I/O';
comment on function PG_CATALOG.varcharout(character varying) is 'I/O';
comment on function PG_CATALOG.varcharrecv(internal, oid, integer) is 'I/O';
comment on function PG_CATALOG.varcharsend(character varying) is 'I/O';
comment on function PG_CATALOG.varchartypmodin(cstring[]) is 'I/O typmod';
comment on function PG_CATALOG.varchartypmodout(integer) is 'I/O typmod';
comment on function PG_CATALOG.variance(bigint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.variance(double precision) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.variance(integer) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.variance(numeric) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.variance(real) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.variance(smallint) is 'the average (arithmetic mean) as numeric of all bigint values';
comment on function PG_CATALOG.void_in(cstring) is 'I/O';
comment on function PG_CATALOG.void_out(void) is 'I/O';
comment on function PG_CATALOG.void_recv(internal) is 'I/O';
comment on function PG_CATALOG.void_send(void) is 'I/O';
comment on function PG_CATALOG.width(box) is 'box width';
comment on function PG_CATALOG.width_bucket(numeric, numeric, numeric, integer) is 'bucket number of operand in equal-width histogram';
comment on function PG_CATALOG.xidin(cstring) is 'I/O';
comment on function PG_CATALOG.xidout(xid) is 'I/O';
comment on function PG_CATALOG.xidrecv(internal) is 'I/O';
comment on function PG_CATALOG.xidsend(xid) is 'I/O';
  END IF;
END
$do$;
create or replace procedure resetallargtypes()
is
    ansvec oidvector;
    aa int;
    xx int;
    tmp text;
begin
    for aa in (select oid from pg_proc where allargtypes is null) loop
        tmp := null;
        for xx in (select unnest(proallargtypes)   from pg_proc where oid =aa) loop
           tmp := tmp || ' '||xx;
        end loop;
        tmp := substr(tmp,2);
        ansvec := tmp::oidvector;
        RAISE INFO ' %' ,ansvec;
        update pg_proc set allargtypes = coalesce(ansvec, proargtypes) where oid=aa;
   end loop;
end;
/
call resetallargtypes();
DROP PROCEDURE IF EXISTS resetallargtypes;

ALTER INDEX pg_proc_proname_args_nsp_index unusable;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9666;
CREATE INDEX pg_catalog.pg_proc_proname_all_args_nsp_index on pg_catalog.pg_proc USING BTREE(proname name_ops, allargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops);
-- ----------------------------------------------------------------
-- upgrade array interface of pg_catalog
-- ----------------------------------------------------------------

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6015;
CREATE OR REPLACE FUNCTION pg_catalog.array_deleteidx(anyarray, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_deleteidx$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DROP FUNCTION IF EXISTS pg_catalog.pg_sequence_last_value;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3080;
CREATE OR REPLACE FUNCTION pg_catalog.pg_sequence_last_value(sequence_oid oid, OUT cache_value int16, OUT last_value int16)
 RETURNS record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_sequence_last_value$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
-- ----------------------------------------------------------------
-- upgrade array interface of pg_catalog
-- ----------------------------------------------------------------

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
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7886;
CREATE OR REPLACE FUNCTION pg_catalog.array_varchar_deleteidx(anyarray, character varying)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_deleteidx$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7884;
CREATE OR REPLACE FUNCTION pg_catalog.array_varchar_first(anyarray)
 RETURNS character varying
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_first$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7885;
CREATE OR REPLACE FUNCTION pg_catalog.array_varchar_last(anyarray)
 RETURNS character varying
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_last$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;-- ----------------------------------------------------------------
-- upgrade array interface of pg_catalog
-- ----------------------------------------------------------------
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
CREATE OR REPLACE FUNCTION pg_catalog.update_pgjob(bigint, "char", bigint, timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone, timestamp without time zone, smallint, text)
 RETURNS void
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$syn_update_pg_job$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION IF EXISTS pg_catalog.array_cat_distinct(anyarray, anyarray) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7887;
CREATE OR REPLACE FUNCTION pg_catalog.array_integer_deleteidx(anyarray, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_deleteidx$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7888;
CREATE OR REPLACE FUNCTION pg_catalog.array_integer_exists(anyarray, integer)
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_exists$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7889;
CREATE OR REPLACE FUNCTION pg_catalog.array_integer_first(anyarray)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_first$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7890;
CREATE OR REPLACE FUNCTION pg_catalog.array_integer_last(anyarray)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_last$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7891;
CREATE OR REPLACE FUNCTION pg_catalog.array_integer_next(anyarray, integer)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_next$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7892;
CREATE OR REPLACE FUNCTION pg_catalog.array_integer_prior(anyarray, integer)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_prior$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7894;
CREATE OR REPLACE FUNCTION pg_catalog.array_union(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_union$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8700;
CREATE OR REPLACE FUNCTION pg_catalog.array_union_distinct(anyarray, anyarray)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_union_distinct$function$;

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
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5733;
CREATE OR REPLACE FUNCTION pg_catalog.dynamic_func_control(scope text, function_name text, action text, "{params}" text[], OUT node_name text, OUT result text) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'dynamic_func_control';
declare
    ansvec oidvector;
    tmpstr1 text;
    tmpstr2 text;
    md5_len int;
    tmpchar text;
    i int;
    j int;
begin
    -- FUNC_MAX_ARGS_INROW = 666
    -- only in param
    update pg_proc set allargtypesext = proargtypesext 
        where proallargtypes is null and pronargs > 666;

    -- include out param
    update pg_proc set allargtypesext = proallargtypes 
        where proallargtypes is not null and array_length(proallargtypes, 1) > 666;

    -- update allargtypes
    for i in (select oid from pg_proc where allargtypesext is not null) loop
        tmpstr1 := null;
        for j in (select unnest(proallargtypes) from pg_proc where oid = i) loop
            tmpstr1 := tmpstr1 || ' ' ||j;
        end loop;
        tmpstr1 := substr(tmpstr1,2);
        tmpstr1 := md5(tmpstr1);

        md5_len := length(tmpstr1);
        tmpstr2 := null;
        for k in 1 .. md5_len loop
            tmpchar := substr(tmpstr1,k,1); 
            if (lower(tmpchar) in ('a', 'b', 'c', 'd', 'e', 'f')) then
                tmpchar := ascii(tmpchar) - ascii('a') + 10;
            end if;
            tmpstr2 := tmpstr2 || ' ' ||tmpchar;
        end loop;
        tmpstr2 := substr(tmpstr2,2);
        ansvec := tmpstr2::oidvector;
        update pg_proc set allargtypes = ansvec where oid = i;
    end loop;
end;
DROP FUNCTION IF EXISTS pg_catalog.nlssort(text, text) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1849;

CREATE OR REPLACE FUNCTION pg_catalog.nlssort(text, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$nlssort$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP FUNCTION IF EXISTS pg_catalog.gs_write_term_log() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9376;
CREATE OR REPLACE FUNCTION pg_catalog.gs_write_term_log(OUT setTermDone boolean)
 RETURNS boolean
 LANGUAGE internal
AS $function$gs_write_term_log$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7895;

CREATE OR REPLACE FUNCTION pg_catalog.array_indexby_length(anyarray, integer)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_indexby_length$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;-- adding system table pg_subscription

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 6126, 6128, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_subscription
(
    subdbid oid NOCOMPRESS NOT NULL,
    subname name NOCOMPRESS,
    subowner oid NOCOMPRESS,
    subenabled bool NOCOMPRESS,
    subconninfo text NOCOMPRESS,
    subslotname name NOCOMPRESS,
    subsynccommit text NOCOMPRESS,
    subpublications text[] NOCOMPRESS
) WITH OIDS TABLESPACE pg_global;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6124;
CREATE UNIQUE INDEX pg_subscription_oid_index ON pg_catalog.pg_subscription USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6125;
CREATE UNIQUE INDEX pg_subscription_subname_index ON pg_catalog.pg_subscription USING BTREE(subdbid, subname);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_subscription TO PUBLIC;

-- adding system table pg_replication_origin

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 6134, 6143, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_replication_origin
(
    roident oid NOCOMPRESS NOT NULL,
    roname text NOCOMPRESS
) TABLESPACE pg_global;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6136;
CREATE UNIQUE INDEX pg_replication_origin_roident_index ON pg_catalog.pg_replication_origin USING BTREE(roident OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6137;
CREATE UNIQUE INDEX pg_replication_origin_roname_index ON pg_catalog.pg_replication_origin USING BTREE(roname TEXT_PATTERN_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_replication_origin TO PUBLIC;

-- adding function pg_replication_origin_create
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_create(IN node_name text, OUT replication_origin_oid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2635;
CREATE FUNCTION pg_catalog.pg_replication_origin_create(IN node_name text, OUT replication_origin_oid oid) RETURNS oid LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_create';

-- adding function pg_replication_origin_drop
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_drop(IN node_name text, OUT replication_origin_oid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2636;
CREATE FUNCTION pg_catalog.pg_replication_origin_drop(IN node_name text, OUT replication_origin_oid oid) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_drop';

-- adding function pg_replication_origin_oid
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_oid(IN node_name text, OUT replication_origin_oid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2637;
CREATE FUNCTION pg_catalog.pg_replication_origin_oid(IN node_name text, OUT replication_origin_oid oid) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_oid';

-- adding function pg_replication_origin_session_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_setup(IN node_name text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2751;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_setup(IN node_name text) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_setup';

-- adding function pg_replication_origin_session_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_reset() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2750;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_reset() RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_reset';

-- adding function pg_replication_origin_session_is_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_is_setup() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2639;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_is_setup() RETURNS boolean LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_is_setup';

-- adding function pg_replication_origin_session_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_progress(IN flush boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2640;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_progress(IN flush boolean) RETURNS record LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_progress';

-- adding function pg_replication_origin_xact_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_setup(IN origin_lsn text, IN origin_timestamp timestamp with time zone) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2799;
CREATE FUNCTION pg_catalog.pg_replication_origin_xact_setup(IN origin_lsn text, IN origin_timestamp timestamp with time zone) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_xact_setup';

-- adding function pg_replication_origin_xact_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_reset() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2752;
CREATE FUNCTION pg_catalog.pg_replication_origin_xact_reset() RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_xact_reset';

-- adding function pg_replication_origin_advance
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_advance(IN node_name text, IN lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2634;
CREATE FUNCTION pg_catalog.pg_replication_origin_advance(IN node_name text, IN lsn text) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_advance';

-- adding function pg_replication_origin_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_progress(IN node_name text, IN flush boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2638;
CREATE FUNCTION pg_catalog.pg_replication_origin_progress(IN node_name text, IN flush boolean) RETURNS record LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_progress';

-- adding function pg_show_replication_origin_status
DROP FUNCTION IF EXISTS pg_catalog.pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn text, OUT local_lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2800;
CREATE FUNCTION pg_catalog.pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn text, OUT local_lsn text) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 AS 'pg_show_replication_origin_status';

-- adding function pg_get_publication_tables
DROP FUNCTION IF EXISTS pg_catalog.pg_get_publication_tables(IN pubname text, OUT relid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2801;
CREATE FUNCTION pg_catalog.pg_get_publication_tables(IN pubname text, OUT relid oid) RETURNS SETOF oid LANGUAGE INTERNAL STABLE STRICT AS 'pg_get_publication_tables';

-- adding function pg_stat_get_subscription
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2802;
CREATE FUNCTION pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) RETURNS record LANGUAGE INTERNAL STABLE AS 'pg_stat_get_subscription';

-- adding system view
DROP VIEW IF EXISTS pg_catalog.pg_publication_tables CASCADE;
CREATE VIEW pg_catalog.pg_publication_tables AS
    SELECT
        P.pubname AS pubname,
        N.nspname AS schemaname,
        C.relname AS tablename
    FROM pg_publication P, pg_class C
         JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.oid IN (SELECT relid FROM pg_get_publication_tables(P.pubname));

DROP VIEW IF EXISTS pg_catalog.pg_stat_subscription CASCADE;
CREATE VIEW pg_catalog.pg_stat_subscription AS
    SELECT
            su.oid AS subid,
            su.subname,
            st.pid,
            st.received_lsn,
            st.last_msg_send_time,
            st.last_msg_receipt_time,
            st.latest_end_lsn,
            st.latest_end_time
    FROM pg_subscription su
            LEFT JOIN pg_stat_get_subscription(NULL) st
                      ON (st.subid = su.oid);

DROP VIEW IF EXISTS pg_catalog.pg_replication_origin_status CASCADE;
CREATE VIEW pg_catalog.pg_replication_origin_status AS
    SELECT *
    FROM pg_show_replication_origin_status();

REVOKE ALL ON pg_catalog.pg_replication_origin_status FROM public;
