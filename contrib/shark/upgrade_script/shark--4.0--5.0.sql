-- Shark 4.0 to 5.0 version transition.
ALTER TABLE sys.databases RENAME COLUMN is_read_onliy TO is_read_only;
ALTER TABLE sys.databases RENAME COLUMN is_vorder_enable TO is_vorder_enabled;
ALTER TABLE sys.server_principals RENAME COLUMN is_disbaled TO is_disabled;
ALTER TABLE sys.server_principals RENAME COLUMN creadential_id TO credential_id;
ALTER TABLE sys.sysdatabases RENAME COLUMN cmplevel TO cmptlevel;

-- Recreate the view to change is_nullable from a domain to varchar(3).
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

-- Store function-local options without square-bracket identifiers. Use the
-- configured session default: earlier install scripts temporarily SET LOCAL
-- this option to an empty string, which must not become the function default.
CREATE FUNCTION pg_temp.shark_remove_sbr_identifier(target REGPROCEDURE)
RETURNS void
LANGUAGE plpgsql
AS $body$
DECLARE
    compat_options TEXT;
    filtered_options TEXT;
BEGIN
    SELECT reset_val INTO compat_options
      FROM pg_catalog.pg_settings
     WHERE name = 'd_format_behavior_compat_options';

    IF pg_catalog.upper(compat_options) = 'ALL' THEN
        compat_options := 'ALL,-enable_sbr_identifier';
    END IF;

    SELECT COALESCE(pg_catalog.string_agg(option, ','), '')
      INTO filtered_options
      FROM (
          SELECT pg_catalog.lower(pg_catalog.btrim(value, E' \t\n\r"')) AS option
            FROM pg_catalog.regexp_split_to_table(compat_options, ',') AS value
      ) AS options
     WHERE option <> '' AND option <> 'enable_sbr_identifier';

    EXECUTE pg_catalog.format(
        'ALTER FUNCTION %s SET d_format_behavior_compat_options TO %L',
        target, filtered_options);
END;
$body$;

SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_get_lang_metadata_json(TEXT)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_get_timeunit_from_string(TEXT, TEXT)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_conv_string_to_datetime_v2(TEXT, TEXT, NUMERIC)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_conv_string_to_datetimeoffset(TEXT, TEXT, NUMERIC)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(SMALLINT, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(TINYINT, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(INTEGER, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(BIGINT, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(NUMERIC, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(TEXT, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(VARCHAR, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(BPCHAR, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(NVARCHAR2, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(REAL, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(DOUBLE PRECISION, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(BOOLEAN, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(TIME, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(TIMETZ, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(TIMESTAMP, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(TIMESTAMPTZ, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(DATE, ANYELEMENT, INTEGER)'::REGPROCEDURE);
SELECT pg_temp.shark_remove_sbr_identifier('sys.shark_try_cast_to_any(SMALLDATETIME, ANYELEMENT, INTEGER)'::REGPROCEDURE);

DROP FUNCTION pg_temp.shark_remove_sbr_identifier(REGPROCEDURE);
