--rollback VIEWS
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
--rollback TABLE
DROP INDEX IF EXISTS pg_event_trigger_evtname_index;
DROP INDEX IF EXISTS pg_event_trigger_oid_index;
DROP TYPE IF EXISTS pg_catalog.pg_event_trigger;
DROP TABLE IF EXISTS pg_catalog.pg_event_trigger;