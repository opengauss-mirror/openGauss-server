set local inplace_upgrade_next_system_object_oids = IUO_PROC, 3148;
CREATE OR REPLACE FUNCTION pg_catalog.pg_terminate_active_session_socket
(IN threadid BIGINT,
  IN sessionid BIGINT)
RETURNS bool LANGUAGE INTERNAL NOT FENCED as 'pg_terminate_active_session_socket' STRICT;

DROP FUNCTION IF EXISTS pg_catalog.pg_get_replica_identity_index(regclass);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6120;
CREATE OR REPLACE FUNCTION pg_catalog.pg_get_replica_identity_index(regclass)
returns regclass
LANGUAGE internal
STABLE STRICT NOT FENCED COST 10
AS $function$pg_get_replica_identity_index$function$;

CREATE OR REPLACE FUNCTION pg_catalog.pg_relation_is_updatable(oid, bool)
RETURNS int4 LANGUAGE INTERNAL STABLE STRICT AS 'pg_relation_is_updatable';

CREATE OR REPLACE FUNCTION pg_catalog.pg_column_is_updatable(oid, int2, bool)
RETURNS bool LANGUAGE INTERNAL STABLE STRICT AS 'pg_column_is_updatable';

COMMENT ON FUNCTION PG_CATALOG.pg_get_replica_identity_index(regclass) IS 'oid of replica identity index if any';
COMMENT ON FUNCTION PG_CATALOG.pg_stat_get_subscription(oid) is 'statistics: information about subscription';
COMMENT ON FUNCTION PG_CATALOG.boolum(bool) is 'implementation of - operator';
