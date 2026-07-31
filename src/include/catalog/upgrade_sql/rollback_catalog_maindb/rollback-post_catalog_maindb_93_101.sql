DROP FUNCTION IF EXISTS pg_catalog.gs_catalog_attribute_records(
        oid, OUT oid, OUT name, OUT oid, OUT integer, OUT smallint, OUT smallint, OUT integer, OUT integer, OUT integer,
        OUT boolean, OUT "char", OUT "char", OUT boolean, OUT boolean, OUT boolean, OUT boolean, OUT tinyint, OUT integer,
        OUT oid, OUT aclitem[], OUT text[], OUT text[], OUT bytea, OUT tinyint, OUT name, OUT "char"
);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8010;
CREATE FUNCTION pg_catalog.gs_catalog_attribute_records(
        IN relid oid,
        OUT attrelid oid,
        OUT attname name,
        OUT atttypid oid,
        OUT attstattarget integer,
        OUT attlen smallint,
        OUT attnum smallint,
        OUT attndims integer,
        OUT attcacheoff integer,
        OUT atttypmod integer,
        OUT attbyval boolean,
        OUT attstorage "char",
        OUT attalign "char",
        OUT attnotnull boolean,
        OUT atthasdef boolean,
        OUT attisdropped boolean,
        OUT attislocal boolean,
        OUT attcmprmode tinyint,
        OUT attinhcount integer,
        OUT attcollation oid,
        OUT attacl aclitem [],
        OUT attoptions text [],
        OUT attfdwoptions text [],
        OUT attinitdefval bytea,
        OUT attkvtype tinyint,
        OUT attdroppedname name
    ) RETURNS SETOF RECORD STRICT STABLE ROWS 1000 LANGUAGE INTERNAL NOT FENCED NOT SHIPPABLE AS 'gs_catalog_attribute_records';

COMMENT ON FUNCTION pg_catalog.gs_catalog_attribute_records(
        oid, OUT oid, OUT name, OUT oid, OUT integer, OUT smallint, OUT smallint, OUT integer, OUT integer, OUT integer,
        OUT boolean, OUT "char", OUT "char", OUT boolean, OUT boolean, OUT boolean, OUT boolean, OUT tinyint, OUT integer,
        OUT oid, OUT aclitem[], OUT text[], OUT text[], OUT bytea, OUT tinyint, OUT name
) IS 'attribute description for catalog relation';

-- Restore information_schema.columns and information_schema.sequences views to their
-- pre-attidentity definitions: identity columns revert to constant NULL/'NO', the
-- pg_depend/seq JOIN is removed from columns, and the NOT EXISTS filter is removed
-- from sequences. The output column structure is unchanged, so CREATE OR REPLACE VIEW
-- suffices without cascading dependent views.
SET search_path TO information_schema;

SET skip_new_column_for_ruledef = true;

DO $$
DECLARE
    function_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pg_catalog'
          AND p.proname = 'pg_get_index_type'
          AND p.proargtypes = '26 21'::oidvector
    ) INTO function_exists;

    -- During rollback verification, rollback-post can run before the historical
    -- upgrade-post script that creates pg_get_index_type. In that case the
    -- identity-aware columns view has not been installed either, so keep the
    -- existing view instead of referencing a catalog function that is absent.
    IF function_exists THEN
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
                        THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_updatable,
                  CAST(
                    CASE WHEN t.typtype = 'd' THEN
                      CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                            WHEN nbt.nspname = 'pg_catalog' THEN pg_catalog.format_type(t.typbasetype, null)
                            ELSE 'USER-DEFINED' END
                    ELSE
                      CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                            When nt.nspname = 'pg_catalog' THEN pg_catalog.format_type(a.atttypid, null)
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
                      AS character_data)
                    AS EXTRA,
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
                LEFT JOIN pg_description d on d.objoid = a.attrelid  and d.objsubid = a.attnum

            WHERE (NOT pg_catalog.pg_is_other_temp_schema(nc.oid))

                  AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')

                  AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')

                  AND (pg_catalog.pg_has_role(c.relowner, 'USAGE')
                      OR pg_catalog.has_column_privilege(c.oid, a.attnum,
                                              'SELECT, INSERT, UPDATE, REFERENCES'));
        GRANT SELECT ON columns TO PUBLIC;
    END IF;
END $$;

DO $$
DECLARE
    function_has_global_cache BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pg_catalog'
          AND p.proname = 'pg_sequence_parameters'
          AND p.proargtypes = '26'::oidvector
          AND p.proargnames @> ARRAY['is_global_cache']
    ) INTO function_has_global_cache;

    -- Keep the view definition aligned with the function signature. Some
    -- databases do not have the is_global_cache OUT parameter because the
    -- historical 93_090 change only updated maindb catalogs.
    IF function_has_global_cache THEN
        CREATE OR REPLACE VIEW sequences AS
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
        GRANT SELECT ON sequences TO PUBLIC;
    END IF;
END $$;


RESET skip_new_column_for_ruledef;

RESET search_path;
