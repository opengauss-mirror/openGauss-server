SET search_path TO information_schema;

BEGIN
IF working_version_num() > 92923 THEN
UPDATE pg_class
set reloptions = (CASE WHEN array_length(array_remove(reloptions, 'segment=on'), 1) = 0
                     then NULL
                     else array_remove(reloptions, 'segment=on')
                     END
                )
WHERE relkind = 'v';
END IF;
END;

-- drop and rebuild COLUMNS which depend on _pg_char_max_length. COLUMNS has new attribute(25)
-- of pg_attribute after upgrade, so auto-rebuilding it during recreate _pg_char_max_length may
-- cause error, because historic version don't have the new attribute.
drop view information_schema.element_types;
drop view information_schema.data_type_privileges;
drop view information_schema.columns;

CREATE OR REPLACE FUNCTION information_schema._pg_char_max_length(typid oid, typmod int4) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    NOT FENCED
    RETURNS NULL ON NULL INPUT
    AS
$$SELECT
  CASE WHEN $2 = -1 /* default typmod */
       THEN null::integer
       WHEN $1 IN (1042, 1043, 3969) /* char, varchar ,nvarchar2*/
       THEN $2 - 4
       WHEN $1 IN (1560, 1562) /* bit, varbit */
       THEN $2
       ELSE null::integer
  END$$;

CREATE OR REPLACE VIEW columns AS
    SELECT CAST(pg_catalog.current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name,
           CAST(a.attnum AS cardinal_number) AS ordinal_position,
           CAST(CASE WHEN ad.adgencol <> 's' THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS column_default,
           CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
             AS yes_or_no)
             AS is_nullable,

           CAST(
             CASE WHEN t.typtype = 'd' THEN
               CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                    WHEN nbt.nspname = 'pg_catalog' THEN pg_catalog.format_type(t.typbasetype, null)
                    ELSE 'USER-DEFINED' END
             ELSE
               CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                    WHEN nt.nspname = 'pg_catalog' THEN pg_catalog.format_type(a.atttypid, null)
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

           CAST(CASE WHEN nco.nspname IS NOT NULL THEN pg_catalog.current_database() END AS sql_identifier) AS collation_catalog,
           CAST(nco.nspname AS sql_identifier) AS collation_schema,
           CAST(co.collname AS sql_identifier) AS collation_name,

           CAST(CASE WHEN t.typtype = 'd' THEN pg_catalog.current_database() ELSE null END
             AS sql_identifier) AS domain_catalog,
           CAST(CASE WHEN t.typtype = 'd' THEN nt.nspname ELSE null END
             AS sql_identifier) AS domain_schema,
           CAST(CASE WHEN t.typtype = 'd' THEN t.typname ELSE null END
             AS sql_identifier) AS domain_name,

           CAST(pg_catalog.current_database() AS sql_identifier) AS udt_catalog,
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

           CAST(CASE WHEN ad.adgencol = 's' THEN 'ALWAYS' ELSE 'NEVER' END AS character_data) AS is_generated,
           CAST(CASE WHEN ad.adgencol = 's' THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS generation_expression,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind in ('v', 'f') AND pg_column_is_updatable(c.oid, a.attnum, false))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_updatable

    FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
         JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
         JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
         LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE (NOT pg_catalog.pg_is_other_temp_schema(nc.oid))

          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')

          AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')

          AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
               OR pg_catalog.has_column_privilege(c.oid, a.attnum,
                                       'SELECT, INSERT, UPDATE, REFERENCES'));

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
                 AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')
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
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.columns TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END$$;

GRANT SELECT ON information_schema.element_types TO PUBLIC;
GRANT SELECT ON information_schema.data_type_privileges TO PUBLIC;
GRANT SELECT ON information_schema.columns TO PUBLIC;

RESET search_path;
