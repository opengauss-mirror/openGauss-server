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
    WHERE C.relkind = 'S';

CREATE OR REPLACE VIEW pg_catalog.pg_statio_sys_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE VIEW pg_catalog.pg_statio_user_sequences AS
    SELECT * FROM pg_statio_all_sequences
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*) = 1 then true else false end as ans from (select proname from pg_proc where proname = 'pg_gtt_attached_pid' and pronargs = 1 limit 1)
    LOOP
        if ans = true then -- base version is after 92-255, create older view
            CREATE OR REPLACE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
            SELECT n.nspname AS schemaname,
                c.relname AS tablename,
                c.oid AS relid,
                array(select pid from pg_gtt_attached_pid(c.oid)) AS pids
            FROM
                pg_class c
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relpersistence='g' AND c.relkind in('r', 'S');
        end if;
        exit;
    END LOOP;
END$$;

CREATE OR REPLACE VIEW pg_catalog.pg_seclabels AS
SELECT
    l.objoid, l.classoid, l.objsubid,
    CASE WHEN rel.relkind = 'r' THEN 'table'::text
         WHEN rel.relkind = 'v' THEN 'view'::text
         WHEN rel.relkind = 'm' THEN 'materialized view'::text
         WHEN rel.relkind = 'S' THEN 'sequence'::text
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
    JOIN pg_authid rol ON l.classoid = rol.tableoid AND l.objoid = rol.oid;DROP INDEX IF EXISTS pg_catalog.gs_job_attribute_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_job_attribute_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_job_attribute;
DROP TABLE IF EXISTS pg_catalog.gs_job_attribute;

DROP INDEX IF EXISTS pg_catalog.gs_job_argument_position_index;
DROP INDEX IF EXISTS pg_catalog.gs_job_argument_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_job_argument_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_job_argument;
DROP TABLE IF EXISTS pg_catalog.gs_job_argument;do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*) = 1 then true else false end as ans from (select typname from pg_type where typname = 'int16' limit 1)
    LOOP
        if ans = true then
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
                WHERE C.relkind = 'S';
            
            CREATE OR REPLACE VIEW DBE_PERF.statio_user_sequences AS
            SELECT * FROM DBE_PERF.statio_all_sequences
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
                    schemaname !~ '^pg_toast';

            CREATE OR REPLACE VIEW DBE_PERF.statio_sys_sequences AS
            SELECT * FROM DBE_PERF.statio_all_sequences
                WHERE schemaname IN ('pg_catalog', 'information_schema') OR
                    schemaname ~ '^pg_toast';
        end if;
        exit;
    END LOOP;
END$$;
RESET search_path;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_pldeveloper' limit 1)
    LOOP
        if ans = true then
            DROP TABLE IF EXISTS dbe_pldeveloper.gs_source;
            DROP TABLE IF EXISTS dbe_pldeveloper.gs_errors;
        end if;
        exit;
    END LOOP;
END$$;

DROP SCHEMA IF EXISTS dbe_pldeveloper;
DROP TYPE IF EXISTS pg_catalog.bulk_exception CASCADE;
-- ----------------------------------------------------------------
-- rollback pg_catalog.pg_conversion 
-- ----------------------------------------------------------------
UPDATE pg_catalog.pg_conversion SET conforencoding=39 WHERE conname like 'gb18030_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=39 WHERE conname like '%_to_gb18030';

UPDATE pg_catalog.pg_conversion SET conforencoding=36 WHERE conname like 'sjis_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=36 WHERE conname like '%_to_sjis';

UPDATE pg_catalog.pg_conversion SET conforencoding=37 WHERE conname like 'big5_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=37 WHERE conname like '%_to_big5';

UPDATE pg_catalog.pg_conversion SET conforencoding=38 WHERE conname like 'uhc_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=38 WHERE conname like '%_to_uhc';

-- deleting system table pg_publication

DROP INDEX IF EXISTS pg_catalog.pg_publication_oid_index;
DROP INDEX IF EXISTS pg_catalog.pg_publication_pubname_index;
DROP TYPE IF EXISTS pg_catalog.pg_publication;
DROP TABLE IF EXISTS pg_catalog.pg_publication;

-- deleting system table pg_publication_rel

DROP INDEX IF EXISTS pg_catalog.pg_publication_rel_oid_index;
DROP INDEX IF EXISTS pg_catalog.pg_publication_rel_map_index;
DROP TYPE IF EXISTS pg_catalog.pg_publication_rel;
DROP TABLE IF EXISTS pg_catalog.pg_publication_rel;

