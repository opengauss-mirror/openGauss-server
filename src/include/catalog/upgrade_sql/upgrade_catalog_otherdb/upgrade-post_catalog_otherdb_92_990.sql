do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans from (select extname from pg_extension where extname='dolphin')
    LOOP
        if ans = true then
            ALTER EXTENSION dolphin UPDATE TO '4.5';
        end if;
        exit;
    END LOOP;
END$$;

DROP FUNCTION IF EXISTS pg_catalog.gs_get_preparse_location() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2874;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_preparse_location(
    OUT preparse_start_location text,
    OUT preparse_end_location text,
    OUT last_valid_record text
) RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE ROWS 10
AS $function$gs_get_preparse_location$function$;

comment on function pg_catalog.gs_get_preparse_location() is 'statistics: information about WAL locations';

DROP FUNCTION IF EXISTS pg_catalog.pg_prepared_statement() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_prepared_statement(bigint) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2510;
CREATE OR REPLACE FUNCTION pg_catalog.pg_prepared_statement(
    OUT name text,
    OUT statement text,
    OUT prepare_time timestamp with time zone,
    OUT parameter_types regtype[],
    OUT from_sql boolean
) RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_prepared_statement$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3702;
CREATE OR REPLACE FUNCTION pg_catalog.pg_prepared_statement(
    in_sessionid bigint,
    OUT sessionid bigint,
    OUT username text, OUT name text,
    OUT statement text,
    OUT prepare_time timestamp with time zone,
    OUT parameter_types regtype[],
    OUT from_sql boolean
) RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_prepared_statement_global$function$;

comment on function pg_catalog.pg_prepared_statement() is 'get the prepared statements for this session';
comment on function pg_catalog.pg_prepared_statement(bigint) is 'get the prepared statements for specified session';