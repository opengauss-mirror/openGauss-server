DROP FUNCTION IF EXISTS pg_catalog.cume_dist_final(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4465;
CREATE FUNCTION pg_catalog.cume_dist_final(
internal, VARIADIC "any"
) RETURNS float8 LANGUAGE INTERNAL IMMUTABLE as 'hypothetical_cume_dist_final';

DROP FUNCTION IF EXISTS pg_catalog.rank_final(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4775;
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
) RETURNS float8 LANGUAGE INTERNAL IMMUTABLE as 'hypothetical_percent_rank_final';

DROP FUNCTION IF EXISTS  pg_catalog.ordered_set_transition_multi(internal, VARIADIC "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4464;
CREATE FUNCTION pg_catalog.ordered_set_transition_multi(
internal, VARIADIC "any"
) RETURNS internal LANGUAGE INTERNAL IMMUTABLE as 'ordered_set_transition_multi';

DROP AGGREGATE IF EXISTS pg_catalog.cume_dist("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4776;
CREATE AGGREGATE pg_catalog.cume_dist("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = cume_dist_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'] WHERE oid=4776;

DROP AGGREGATE IF EXISTS pg_catalog.rank("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4774;
CREATE AGGREGATE pg_catalog.rank("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = rank_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'] WHERE oid=4774;

DROP AGGREGATE IF EXISTS pg_catalog.dense_rank("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4082;
CREATE AGGREGATE pg_catalog.dense_rank("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = dense_rank_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'] WHERE oid=4082;

DROP AGGREGATE IF EXISTS pg_catalog.percent_rank("any");
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4083;
CREATE AGGREGATE pg_catalog.percent_rank("any") (SFUNC=ordered_set_transition_multi, STYPE= internal, finalfunc = percent_rank_final, hypothetical);
UPDATE pg_catalog.pg_proc SET provariadic = 2276, proallargtypes=ARRAY[2276], proargmodes=ARRAY['v'], prorettype = 701 WHERE oid=4083;

COMMENT ON FUNCTION pg_catalog.cume_dist("any") is 'cumulative distribution of hypothetical row';
COMMENT ON FUNCTION pg_catalog.cume_dist_final(internal, VARIADIC "any") is 'aggregate final function';
COMMENT ON FUNCTION pg_catalog.dense_rank("any") is 'dense rank of hypothetical row';
COMMENT ON FUNCTION pg_catalog.dense_rank_final(internal, VARIADIC "any") is 'aggregate final function';
COMMENT ON FUNCTION pg_catalog.ordered_set_transition_multi(internal, VARIADIC "any") is 'aggregate transition function';
COMMENT ON FUNCTION pg_catalog.percent_rank("any") is 'percent rank of hypothetical row';
COMMENT ON FUNCTION pg_catalog.percent_rank_final(internal, VARIADIC "any") is 'aggregate final function';
COMMENT ON FUNCTION pg_catalog.rank("any") is 'rank of hypothetical row';
COMMENT ON FUNCTION pg_catalog.rank_final(internal, VARIADIC "any") is 'aggregate final function';