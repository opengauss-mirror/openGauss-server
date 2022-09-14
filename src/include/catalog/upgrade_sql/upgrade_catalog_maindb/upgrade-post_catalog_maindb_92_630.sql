DROP VIEW IF EXISTS pg_catalog.pg_gtt_attached_pids;
DROP FUNCTION IF EXISTS pg_catalog.pg_gtt_attached_pid(relid oid, OUT relid oid, OUT pid bigint);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3598;
CREATE OR REPLACE FUNCTION pg_catalog.pg_gtt_attached_pid(relid oid, OUT relid oid, OUT pid bigint, OUT sessionid bigint)
 RETURNS SETOF record
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE ROWS 10
AS $function$pg_gtt_attached_pid$function$;

CREATE OR REPLACE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS relid,
    array(select pid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS pids,
    array(select sessionid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS sessionids
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r', 'S', 'L');
GRANT SELECT ON pg_catalog.pg_gtt_attached_pids TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
