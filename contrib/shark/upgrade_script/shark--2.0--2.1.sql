SET LOCAL d_format_behavior_compat_options = '';

-- PR #9961: apply the Shark 2.1 view corrections, matching the 6.0 branch.
-- Older 6.0 packages may already contain these column renames in Shark 2.0.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = 'sys.databases'::regclass
               AND attname = 'is_read_onliy' AND NOT attisdropped) THEN
        ALTER TABLE sys.databases RENAME COLUMN is_read_onliy TO is_read_only;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = 'sys.databases'::regclass
               AND attname = 'is_vorder_enable' AND NOT attisdropped) THEN
        ALTER TABLE sys.databases RENAME COLUMN is_vorder_enable TO is_vorder_enabled;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = 'sys.sysdatabases'::regclass
               AND attname = 'cmplevel' AND NOT attisdropped) THEN
        ALTER TABLE sys.sysdatabases RENAME COLUMN cmplevel TO cmptlevel;
    END IF;
END$$;

DROP VIEW information_schema_tsql.columns RESTRICT;
CREATE VIEW information_schema_tsql.columns AS
SELECT
    CAST(current_database() AS nvarchar(128)) AS table_catalog,
    CAST(nc.nspname AS nvarchar(128)) AS table_schema,
    CAST(c.relname AS nvarchar(128)) AS table_name,
    CAST(a.attname AS nvarchar(128)) AS column_name,
	CAST(
        CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE a.atttypid END
        AS int
    ) AS ordinal_position,
	CAST(pg_get_expr(ad.adbin, ad.adrelid) AS nvarchar(4000)) AS column_default,
	CAST(
        CASE
            WHEN a.attnotnull THEN 'NO'
            ELSE 'YES'
        END AS varchar(3)
    ) AS is_nullable,
      CAST(pg_catalog.format_type(a.atttypid, a.atttypmod) AS varchar(128)) AS data_type,
	CAST(
		 information_schema_tsql._pg_char_max_length(t.typname, a.atttypmod)
		 AS int)
		 AS character_maximum_length,
	CAST(
		 information_schema_tsql._pg_char_octet_length(t.typname, a.atttypmod)
		 AS int)
		 AS character_octet_length,
	CAST(information_schema_tsql._pgtsql_numeric_precision(t.typname, a.atttypid, a.atttypmod) AS tinyint) AS numeric_precision,
	CAST(information_schema_tsql._pgtsql_numeric_precision_radix(t.typname, a.atttypid, a.atttypmod) AS smallint) AS numeric_precision_radix,
	CAST(information_schema_tsql._pgtsql_numeric_scale(t.typname, a.atttypid, a.atttypmod) AS int) AS numeric_scale,
	CAST(information_schema_tsql._pgtsql_datetime_precision(t.typname, a.atttypmod) AS smallint) AS datetime_precision,
  CAST(null AS nvarchar(128)) AS character_set_catalog,
  CAST(null AS nvarchar(128)) AS character_set_schema,
	CAST(pg_encoding_to_char(co.collencoding) AS nvarchar(128)) AS character_set_name,
	CAST(null as nvarchar(128)) as collation_catalog,
	CAST(null as nvarchar(128)) as collation_schema,
	CAST(co.collname AS nvarchar(128)) AS collation_name,
	CAST(CASE WHEN t.typtype = 'd' AND nc.nspname <> 'pg_catalog' AND nc.nspname <> 'sys' THEN pg_catalog.current_database() ELSE null END
		AS nvarchar(128)) AS domain_catalog,
	CAST(CASE WHEN t.typtype = 'd' AND nc.nspname <> 'pg_catalog' AND nc.nspname <> 'sys' THEN nc.nspname ELSE null END
		AS nvarchar(128)) AS domain_schema,
	CAST(CASE WHEN t.typtype = 'd' AND nc.nspname <> 'pg_catalog' AND nc.nspname <> 'sys' THEN t.typname ELSE null END
		AS nvarchar(128)) AS domain_name
FROM
    pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace nc ON c.relnamespace = nc.oid
    JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
    LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid, a.attnum) = (ad.adrelid, ad.adnum)
    LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
    LEFT JOIN pg_catalog.pg_collation co ON t.typcollation = co.oid
WHERE information_schema_tsql.is_d_format_schema(nc.oid, nc.nspname)
    AND c.relkind IN ('r', 'v', 'm', 'f')  -- 表/视图/物化视图/外表
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND (pg_has_role(c.relowner, 'USAGE')
			OR has_column_privilege(c.oid, a.attnum,
			'SELECT, INSERT, UPDATE, REFERENCES'));

-- dateadd: remove STRICT so that a null datepart raises an error,
-- while a null number/date still returns null (handled in datefuncs.cpp).
CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,date)
RETURNS timestamp without time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateadddate$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp without time zone)
RETURNS timestamp without time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamp$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,timestamp with time zone)
RETURNS timestamp with time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimestamptz$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time without time zone)
RETURNS timestamp without time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtime$function$;

CREATE OR REPLACE FUNCTION sys.dateadd(cstring,integer,time with time zone)
RETURNS timestamp with time zone
language c
immutable NOT FENCED NOT SHIPPABLE
AS '$libdir/shark', $function$dateaddtimetz$function$;
