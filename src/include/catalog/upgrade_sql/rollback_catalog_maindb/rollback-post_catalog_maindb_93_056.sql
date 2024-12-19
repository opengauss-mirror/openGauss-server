
DO
$do$
DECLARE
    havefunc8 boolean;
    haveoper15 boolean;
BEGIN
    select count(*)>0 into havefunc8 from pg_amproc where amprocfamily = 2595 and amprocnum = 8;
    select count(*)>0 into haveoper15 from pg_amop where amopfamily = 2595 and amopstrategy = 15;

    IF havefunc8 = true then
        ALTER OPERATOR FAMILY circle_ops USING gist DROP FUNCTION 8 (circle, circle);
    END IF;

    IF haveoper15 = true then
        ALTER OPERATOR FAMILY circle_ops USING gist DROP OPERATOR 15 (circle, point);
    END IF;
END
$do$;


DROP OPERATOR IF EXISTS pg_catalog.<->(circle, point) cascade;
DROP FUNCTION IF EXISTS pg_catalog.gist_circle_distance(internal, point, int4, oid, internal);
DROP FUNCTION IF EXISTS pg_catalog.dist_cpoint(circle, point);

-- function gist_point_distance
DROP FUNCTION IF EXISTS pg_catalog.gist_point_distance;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3064;
CREATE FUNCTION pg_catalog.gist_point_distance(internal, point, int4, oid) RETURNS pg_catalog.float8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'gist_point_distance';
COMMENT ON FUNCTIOn pg_catalog.gist_point_distance(internal, point, int4, oid) IS 'GiST support';
