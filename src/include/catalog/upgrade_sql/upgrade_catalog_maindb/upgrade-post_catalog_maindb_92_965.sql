SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1158;
CREATE OR REPLACE FUNCTION pg_catalog.to_timestamp(float8) RETURNS timestamptz LANGUAGE INTERNAL IMMUTABLE STRICT AS 'float8_timestamptz';

DROP FUNCTION IF EXISTS pg_catalog.age(xid);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1181;
CREATE OR REPLACE FUNCTION pg_catalog.age(xid)
RETURNS xid LANGUAGE internal STRICT STABLE NOT FENCED as 'xid_age';
COMMENT ON FUNCTION pg_catalog.age(xid) IS 'age of a transaction ID, in transactions before current transaction';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5435;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_sum(internal)
RETURNS numeric
LANGUAGE internal IMMUTABLE
AS 'numeric_sum';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5439;
CREATE OR REPLACE FUNCTION pg_catalog.int8_avg_accum_numeric(internal, int8)
RETURNS internal
LANGUAGE internal IMMUTABLE
AS 'int8_avg_accum_numeric';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5440;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_accum_numeric(internal, numeric)
RETURNS internal
LANGUAGE internal IMMUTABLE
AS 'numeric_accum_numeric';
COMMENT ON FUNCTION pg_catalog.numeric_accum_numeric(internal, numeric) IS 'aggregate transition function';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5441;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_avg_numeric(internal)
RETURNS numeric
LANGUAGE internal IMMUTABLE
AS 'numeric_avg_numeric';
COMMENT ON FUNCTION pg_catalog.numeric_avg_numeric(internal) IS 'aggregate final function';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5442;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_avg_accum_numeric(internal, numeric)
RETURNS _numeric
LANGUAGE internal IMMUTABLE
AS 'numeric_avg_accum_numeric';
COMMENT ON FUNCTION pg_catalog.numeric_avg_accum_numeric(internal, numeric) IS 'aggregate transition function';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5443;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_stddev_pop_numeric(internal)
RETURNS numeric
LANGUAGE internal IMMUTABLE
AS 'numeric_stddev_pop_numeric';
COMMENT ON FUNCTION pg_catalog.numeric_stddev_pop_numeric(internal) IS 'aggregate final function';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5444;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_stddev_samp_numeric(internal)
RETURNS numeric
LANGUAGE internal IMMUTABLE
AS 'numeric_stddev_samp_numeric';
COMMENT ON FUNCTION pg_catalog.numeric_stddev_samp_numeric(internal) IS 'aggregate final function';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5445;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_var_pop_numeric(internal)
RETURNS numeric
LANGUAGE internal IMMUTABLE
AS 'numeric_var_pop_numeric';
COMMENT ON FUNCTION pg_catalog.numeric_var_pop_numeric(internal) IS 'aggregate final function';

DROP FUNCTION IF EXISTS pg_catalog.numeric_var_samp_numeric(numeric);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5446;
CREATE OR REPLACE FUNCTION pg_catalog.numeric_var_samp_numeric(internal)
RETURNS numeric
LANGUAGE internal IMMUTABLE
AS 'numeric_var_samp_numeric';
COMMENT ON FUNCTION pg_catalog.numeric_var_samp_numeric(internal) IS 'aggregate final function';

DROP FUNCTION IF EXISTS pg_catalog.pgsysconf() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2371;
CREATE OR REPLACE FUNCTION
pg_catalog.pgsysconf(OUT os_page_size   bigint,
          OUT os_pages_free  bigint,
          OUT os_total_pages bigint)
RETURNS record LANGUAGE INTERNAL VOLATILE COST 1
AS 'pgsysconf';
COMMENT ON FUNCTION pg_catalog.pgsysconf()
IS 'Get system configuration information at run time, man 3 sysconf for details';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise(regclass, text, int) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2372;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise(IN relname regclass, IN fork text, IN action int,
		  OUT relpath text,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT os_pages_free bigint)
RETURNS setof record LANGUAGE INTERNAL VOLATILE ROWS 1 COST 1
AS 'pgfadvise';
COMMENT ON FUNCTION pg_catalog.pgfadvise(regclass, text, int)
IS 'Predeclare an access pattern for file data';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_loader(regclass, text, char, text, int, bool, bool, varbit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2373;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise_loader(IN relname regclass, IN fork text, IN reltype "char", IN partition_name text, IN segment int, IN load bool, IN unload bool, IN databit varbit,
				 OUT relpath text,
				 OUT os_page_size bigint,
				 OUT os_pages_free bigint,
				 OUT pages_loaded bigint,
				 OUT pages_unloaded bigint)
RETURNS setof record LANGUAGE INTERNAL VOLATILE ROWS 1 COST 1
AS 'pgfadvise_loader';
COMMENT ON FUNCTION pg_catalog.pgfadvise_loader(regclass, text, "char", text, int, bool, bool, varbit)
IS 'Restore cache from the snapshot, options to load/unload each block to/from cache';

DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass, text, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2374;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfincore(IN relname regclass, IN fork text, IN getdatabit bool,
		  OUT relpath text,
		  OUT segment int,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT pages_mem bigint,
		  OUT group_mem bigint,
		  OUT os_pages_free bigint,
		  OUT databit      varbit,
		  OUT pages_dirty bigint,
		  OUT group_dirty bigint)
RETURNS setof record LANGUAGE INTERNAL VOLATILE ROWS 1 COST 1
AS 'pgfincore';
COMMENT ON FUNCTION pg_catalog.pgfincore(regclass, text, bool)
IS 'Utility to inspect and get a snapshot of the system cache';

DROP FUNCTION IF EXISTS pg_catalog.pgsysconf_pretty() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2534;
CREATE OR REPLACE FUNCTION
pg_catalog.pgsysconf_pretty(OUT os_page_size   text,
                 OUT os_pages_free  text,
                 OUT os_total_pages text)
RETURNS record LANGUAGE SQL VOLATILE COST 1
AS 'select pg_catalog.pg_size_pretty(os_page_size) as os_page_size, pg_catalog.pg_size_pretty(os_pages_free * os_page_size)  as os_pages_free, pg_catalog.pg_size_pretty(os_total_pages * os_page_size) as os_total_pages from pg_catalog.pgsysconf()';
COMMENT ON FUNCTION pg_catalog.pgsysconf_pretty()
IS 'Pgsysconf() with human readable output';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_willneed(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2535;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise_willneed(IN relname regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 10)';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_dontneed(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2536;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise_dontneed(IN relname regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 20)';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_normal(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2537;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise_normal(IN relname regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 30)';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_sequential(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2538;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise_sequential(IN relname regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 40)';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_loader(regclass, char, text, int, bool, bool, varbit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2539;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise_loader(IN relname regclass, IN reltype "char", IN partition_name text, IN segment int, IN load bool, IN unload bool, IN databit varbit,
				 OUT relpath text,
				 OUT os_page_size bigint,
				 OUT os_pages_free bigint,
				 OUT pages_loaded bigint,
				 OUT pages_unloaded bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT pg_catalog.pgfadvise_loader($1, ''main'', $2, $3, $4, $5, $6, $7)';

DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_random(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2542;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise_random(IN relname regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 50)';

DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2541;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfincore(IN relname regclass,
		  OUT relpath text,
		  OUT segment int,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT pages_mem bigint,
		  OUT group_mem bigint,
		  OUT os_pages_free bigint,
		  OUT databit      varbit,
		  OUT pages_dirty bigint,
		  OUT group_dirty bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT * from pg_catalog.pgfincore($1, ''main'', false)';

DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2540;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfincore(IN relname regclass, IN getdatabit bool,
		  OUT relpath text,
		  OUT segment int,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT pages_mem bigint,
		  OUT group_mem bigint,
		  OUT os_pages_free bigint,
		  OUT databit      varbit,
		  OUT pages_dirty bigint,
		  OUT group_dirty bigint)
RETURNS setof record LANGUAGE SQL VOLATILE ROWS 1 COST 1
AS 'SELECT * from pg_catalog.pgfincore($1, ''main'', $2)';

DROP FUNCTION IF EXISTS pg_catalog.pgfincore_drawer(varbit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2543;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfincore_drawer(IN input varbit,
		  OUT drawer cstring)
RETURNS cstring LANGUAGE INTERNAL IMMUTABLE
AS 'pgfincore_drawer';
COMMENT ON FUNCTION pg_catalog.pgfincore_drawer(varbit)
IS 'A naive drawing function to visualize page cache per object';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4099;
CREATE OR REPLACE FUNCTION pg_catalog.group_concat_transfn(internal, text, VARIADIC "any")
 RETURNS internal
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE AS 'group_concat_transfn';
COMMENT ON FUNCTION pg_catalog.group_concat_transfn(internal, text, VARIADIC "any") IS 'aggregate transition function';

DROP AGGREGATE IF EXISTS pg_catalog.group_concat(text, "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4097;
CREATE AGGREGATE pg_catalog.group_concat(text, "any") (SFUNC=group_concat_transfn, STYPE=internal, FINALFUNC=group_concat_finalfn);
UPDATE pg_catalog.pg_proc SET provariadic=2276, proallargtypes=ARRAY[25,2276], proargmodes=ARRAY['i','v'] WHERE oid=4097;
comment on function pg_catalog.group_concat(text, "any") is 'concatenate values in each grouping with separators';
comment on function pg_catalog.group_concat_finalfn(internal) is 'aggregate final function';

DROP FUNCTION IF EXISTS pg_catalog.dss_io_stat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6990;
CREATE FUNCTION pg_catalog.dss_io_stat
(
    IN duration int4,
    OUT read_kilobyte_per_sec   int8,
    OUT write_kilobyte_per_sec   int8,
    OUT io_times  int4
)
RETURNS record LANGUAGE INTERNAL STRICT IMMUTABLE as 'dss_io_stat';

DROP FUNCTION IF EXISTS pg_catalog.query_page_distribution_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2866;
CREATE FUNCTION pg_catalog.query_page_distribution_info
(
    IN relname 		text,
    IN fork 		int4,
    IN blockno 		int4,
    OUT instance_id int4,
    OUT is_master   boolean,
    OUT is_owner    boolean,
    OUT is_copy     boolean,
    OUT lock_mode   text,
    OUT mem_lsn     oid,
    OUT disk_lsn    oid,
    OUT is_dirty    boolean
)
RETURNS SETOF record LANGUAGE INTERNAL STRICT ROWS 64 as 'query_page_distribution_info';
COMMENT ON FUNCTION pg_catalog.query_page_distribution_info() IS 'statistics: query page distribution information ';

DO
$do$
    DECLARE
        index_exists boolean := true;
    BEGIN
        select case when count(*)=1 then true else false end from (select 1 from pg_class where oid = 3480 and relkind = 'i' and relname = 'pg_partition_tblspc_relfilenode_index') into index_exists;
        IF index_exists = false then
            -- ADD INDEX pg_partition_tblspc_relfilenode_index for pg_partition(reltablespace, relfilenode)
            -- set GUC for pg_partition_tblspc_relfilenode_index
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3480;

            -- create index
            CREATE INDEX pg_partition_tblspc_relfilenode_index ON pg_catalog.pg_partition
                USING BTREE(reltablespace oid_ops, relfilenode oid_ops);
        END IF;
    END
$do$;

CREATE OR REPLACE FUNCTION pg_catalog.update_pg_description(IN colloid integer, IN colldesc text)
RETURNS void
AS $$
DECLARE
row_name record;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_description where objoid = colloid and classoid = 3456;
    insert into pg_catalog.pg_description values(colloid, 3456, 0, colldesc);
  END LOOP;
  return;
END;
$$ LANGUAGE 'plpgsql';

select pg_catalog.update_pg_description(1553, 'utf8_bin collation');
select pg_catalog.update_pg_description(1552, 'utf8_unicode_ci collation');
select pg_catalog.update_pg_description(1551, 'utf8_general_ci collation');

DROP FUNCTION pg_catalog.update_pg_description;

comment on function PG_CATALOG.regexp_count(text, text) is 'find match(es) count for regexp';
comment on function PG_CATALOG.regexp_count(text, text, integer) is 'find match(es) count for regexp';
comment on function PG_CATALOG.regexp_count(text, text, integer, text) is 'find match(es) count for regexp';
comment on function PG_CATALOG.regexp_instr(text, text) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer, integer) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer, integer, text) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_replace(text, text) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer, integer) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer, integer, text) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_substr(text, text, integer) is 'extract text matching regular expression';
comment on function PG_CATALOG.regexp_substr(text, text, integer, integer) is 'extract text matching regular expression';
comment on function PG_CATALOG.regexp_substr(text, text, integer, integer, text) is 'extract text matching regular expression';

DO $upgrade$
BEGIN
IF working_version_num() < 92780 then
DROP INDEX IF EXISTS pg_partition_tblspc_relfilenode_index CASCADE;
END IF;
END $upgrade$;