DROP FUNCTION IF EXISTS pg_catalog.cume_dist_final(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4465;
CREATE FUNCTION pg_catalog.cume_dist_final(
internal, VARIADIC "any"
) RETURNS float8 LANGUAGE INTERNAL IMMUTABLE as 'hypothetical_cume_dist_final';

DROP FUNCTION IF EXISTS pg_catalog.rank_final(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3249;
CREATE FUNCTION pg_catalog.rank_final(
internal, VARIADIC "any"
) RETURNS int8 LANGUAGE INTERNAL IMMUTABLE as 'hypothetical_rank_final';

DROP FUNCTION IF EXISTS pg_catalog.dense_rank_final(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4084;
CREATE FUNCTION pg_catalog.dense_rank_final(
internal, VARIADIC "any"
) RETURNS int8 LANGUAGE INTERNAL IMMUTABLE as 'hypothetical_dense_rank_final';

DROP FUNCTION IF EXISTS pg_catalog.percent_rank_final(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4085;
CREATE FUNCTION pg_catalog.percent_rank_final(
internal, VARIADIC "any"
) RETURNS int8 LANGUAGE INTERNAL IMMUTABLE as 'hypothetical_percent_rank_final';

DROP FUNCTION IF EXISTS  pg_catalog.ordered_set_transition_multi(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4464;
CREATE FUNCTION pg_catalog.ordered_set_transition_multi(
internal, VARIADIC "any"
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE as 'ordered_set_transition_multi';

DROP AGGREGATE IF EXISTS pg_catalog.cume_dist("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3149;
CREATE AGGREGATE pg_catalog.cume_dist("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = cume_dist_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'] WHERE oid=3149;

DROP AGGREGATE IF EXISTS pg_catalog.rank("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3248;
CREATE AGGREGATE pg_catalog.rank("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = rank_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'] WHERE oid=3248;

DROP AGGREGATE IF EXISTS pg_catalog.dense_rank("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4082;
CREATE AGGREGATE pg_catalog.dense_rank("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = dense_rank_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'] WHERE oid=4082;

DROP AGGREGATE IF EXISTS pg_catalog.percent_rank("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4083;
CREATE AGGREGATE pg_catalog.percent_rank("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = percent_rank_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'] WHERE oid=4083;
