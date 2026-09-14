-- Report AUTO_INCREMENT metadata using the pre-upgrade uppercase value.
SET search_path TO information_schema;

DO $$
DECLARE
    column_key_function_exists BOOLEAN;
    sequence_function_exists BOOLEAN;
    attidentity_exists BOOLEAN;
    lowercase_view_installed BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pg_catalog'
          AND p.proname = 'pg_get_index_type'
          AND p.proargtypes = '26 21'::oidvector
    ) INTO column_key_function_exists;

    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pg_catalog'
          AND p.proname = 'pg_sequence_parameters'
          AND p.proargtypes = '26'::oidvector
    ) INTO sequence_function_exists;

    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = 'pg_catalog.pg_attribute'::regclass
          AND a.attname = 'attidentity'
          AND a.attnum > 0
          AND NOT a.attisdropped
    ) INTO attidentity_exists;

    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'information_schema'
          AND c.relname = 'columns'
          AND c.relkind = 'v'
          AND pg_catalog.strpos(pg_catalog.pg_get_viewdef(c.oid), '''auto_increment''') > 0
    ) INTO lowercase_view_installed;

    IF column_key_function_exists AND sequence_function_exists
       AND attidentity_exists AND lowercase_view_installed THEN
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

           CAST(CASE WHEN a.attidentity IN ('a', 'd') THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_identity,
           CAST(CASE a.attidentity WHEN 'a' THEN 'ALWAYS' WHEN 'd' THEN 'BY DEFAULT' END AS character_data) AS identity_generation,
           CAST(CASE WHEN seq.oid IS NOT NULL THEN (pg_catalog.pg_sequence_parameters(seq.oid)).start_value ELSE NULL END
                AS character_data) AS identity_start,
           CAST(CASE WHEN seq.oid IS NOT NULL THEN (pg_catalog.pg_sequence_parameters(seq.oid)).increment ELSE NULL END
                AS character_data) AS identity_increment,
           CAST(CASE WHEN seq.oid IS NOT NULL THEN (pg_catalog.pg_sequence_parameters(seq.oid)).maximum_value ELSE NULL END
                AS character_data) AS identity_maximum,
           CAST(CASE WHEN seq.oid IS NOT NULL THEN (pg_catalog.pg_sequence_parameters(seq.oid)).minimum_value ELSE NULL END
                AS character_data) AS identity_minimum,
           CAST(CASE WHEN seq.oid IS NOT NULL THEN
                     CASE WHEN (pg_catalog.pg_sequence_parameters(seq.oid)).cycle_option
                     THEN 'YES' ELSE 'NO' END
                ELSE NULL END AS yes_or_no) AS identity_cycle,

           CAST(CASE WHEN ad.adgencol = 's' THEN 'ALWAYS' ELSE 'NEVER' END AS character_data) AS is_generated,
           CAST(CASE WHEN ad.adgencol = 's' THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS generation_expression,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind in ('v', 'f') AND pg_column_is_updatable(c.oid, a.attnum, false))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_updatable,
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
             AS COLUMN_TYPE,
            CAST(d.description AS information_schema.character_data) AS COLUMN_COMMENT,
            CAST(
               CASE WHEN ad.adsrc = 'AUTO_INCREMENT' THEN 'AUTO_INCREMENT'
               ELSE
                  CASE WHEN ad.adsrc_on_update is not null THEN CONCAT('DEFAULT_GENERATED on update ', pg_catalog.quote_literal(ad.adsrc_on_update))
                  ELSE null
                  END
               END
               AS character_data) AS EXTRA,
            CAST(array_to_string(ARRAY[
                CASE WHEN has_column_privilege(c.oid, a.attnum, 'SELECT') THEN 'select' END,
                CASE WHEN has_column_privilege(c.oid, a.attnum, 'INSERT') THEN 'insert' END,
                CASE WHEN has_column_privilege(c.oid, a.attnum, 'UPDATE') THEN 'update' END,
                CASE WHEN has_column_privilege(c.oid, a.attnum, 'REFERENCES') THEN 'references' END
                ], ',') AS varchar(154)) AS privileges,
            CAST(pg_get_index_type(c.oid, a.attnum) AS varchar(3)) AS column_key,
            CAST(null AS int) AS srs_id

    FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
         JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
         JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
         LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')
         LEFT JOIN (pg_depend dep JOIN pg_class seq ON (dep.classid = 'pg_class'::regclass AND dep.objid = seq.oid AND dep.deptype = 'i' AND seq.relkind in ('s', 'S', 'z', 'Z')))
           ON (dep.refclassid = 'pg_class'::regclass AND dep.refobjid = seq.oid AND dep.refobjsubid = a.attnum)
         LEFT JOIN pg_description d on d.objoid = a.attrelid and d.objsubid = a.attnum

    WHERE (NOT pg_catalog.pg_is_other_temp_schema(nc.oid))

          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')

          AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')

          AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
               OR pg_catalog.has_column_privilege(c.oid, a.attnum,
                                       'SELECT, INSERT, UPDATE, REFERENCES'));

GRANT SELECT ON columns TO PUBLIC;

    END IF;
END $$;

RESET search_path;
