
-- function gist_point_distance
DROP FUNCTION IF EXISTS pg_catalog.gist_point_distance;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3064;
CREATE FUNCTION pg_catalog.gist_point_distance(internal, point, int4, oid, internal) RETURNS pg_catalog.float8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'gist_point_distance';
COMMENT ON FUNCTIOn pg_catalog.gist_point_distance(internal, point, int4, oid, internal) IS 'GiST support';

-- function dist_cpoint
DROP FUNCTION IF EXISTS pg_catalog.dist_cpoint(circle, point);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3291;
CREATE FUNCTION pg_catalog.dist_cpoint(circle,point) RETURNS pg_catalog.float8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'dist_cpoint';
COMMENT ON FUNCTIOn pg_catalog.dist_cpoint(circle,point) IS 'implementation of <-> operator';

-- function gist_circle_distance
DROP FUNCTION IF EXISTS pg_catalog.gist_circle_distance(internal, point, int4, oid, internal);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3280;
CREATE FUNCTION pg_catalog.gist_circle_distance(internal, point, int4, oid, internal) RETURNS pg_catalog.float8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'gist_circle_distance';
COMMENT ON FUNCTIOn pg_catalog.gist_circle_distance(internal, point, int4, oid, internal) IS 'GiST support';

-- operator circle <-> point
DROP OPERATOR IF EXISTS pg_catalog.<->(circle, point) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 3291;
CREATE OPERATOR pg_catalog.<->(
leftarg =  circle, rightarg = point, procedure = dist_cpoint, COMMUTATOR = <->
);
COMMENT ON OPERATOR pg_catalog.<->(circle, point) IS 'distance between';

-- update circle_ops
ALTER OPERATOR FAMILY circle_ops USING gist ADD OPERATOR 15 <->(circle, point) FOR ORDER BY pg_catalog.float_ops;
ALTER OPERATOR FAMILY circle_ops USING gist ADD FUNCTION 8 (circle, circle) gist_circle_distance(internal, point, int4, oid, internal);
