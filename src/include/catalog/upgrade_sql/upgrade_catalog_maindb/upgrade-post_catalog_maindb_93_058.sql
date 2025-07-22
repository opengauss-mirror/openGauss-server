DROP FUNCTION IF EXISTS pg_catalog.diskannbuild(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8538;
CREATE FUNCTION pg_catalog.diskannbuild(internal, internal, internal)
    RETURNS internal
AS 'diskannbuild'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannbuildempty(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8539;
CREATE FUNCTION pg_catalog.diskannbuildempty(internal)
    RETURNS void
AS 'diskannbuildempty'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskanninsert(internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8533;
CREATE FUNCTION pg_catalog.diskanninsert(internal, internal, internal, internal, internal, internal)
    RETURNS boolean
AS 'diskanninsert'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannbulkdelete(internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8540;
CREATE FUNCTION pg_catalog.diskannbulkdelete(internal, internal, internal, internal)
    RETURNS internal
AS 'diskannbulkdelete'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannvacuumcleanup(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8541;
CREATE FUNCTION pg_catalog.diskannvacuumcleanup(internal, internal)
    RETURNS internal
AS 'diskannvacuumcleanup'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskanncostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8542;
CREATE FUNCTION pg_catalog.diskanncostestimate(internal, internal, internal, internal, internal, internal, internal)
    RETURNS void
AS 'diskanncostestimate'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannoptions(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8543;
CREATE FUNCTION pg_catalog.diskannoptions(internal, internal)
    RETURNS internal
AS 'diskannoptions'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannvalidate(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8531;
CREATE FUNCTION pg_catalog.diskannvalidate(internal)
    RETURNS boolean
AS 'diskannvalidate'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannbeginscan(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8534;
CREATE FUNCTION pg_catalog.diskannbeginscan(internal, internal, internal)
    RETURNS internal
AS 'diskannbeginscan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannrescan(internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8536;
CREATE FUNCTION pg_catalog.diskannrescan(internal, internal, internal, internal, internal)
    RETURNS void
AS 'diskannrescan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskanngettuple(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8535;
CREATE FUNCTION pg_catalog.diskanngettuple(internal, internal)
    RETURNS boolean
AS 'diskanngettuple'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannendscan(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8537;
CREATE FUNCTION pg_catalog.diskannendscan(internal)
    RETURNS void
AS 'diskannendscan'
LANGUAGE INTERNAL
STRICT;

DROP FUNCTION IF EXISTS pg_catalog.diskannhandler(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8532;
CREATE FUNCTION pg_catalog.diskannhandler(internal)
    RETURNS internal
AS 'diskannhandler'
LANGUAGE INTERNAL
STRICT;

COMMENT ON FUNCTION pg_catalog.diskannbuild(internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannbuildempty(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskanninsert(internal, internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannbulkdelete(internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannvacuumcleanup(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskanncostestimate(internal, internal, internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannoptions(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannvalidate(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannbeginscan(internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannrescan(internal, internal, internal, internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskanngettuple(internal, internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannendscan(internal) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.diskannhandler(internal) IS 'NULL';

DROP ACCESS METHOD IF EXISTS diskann CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8303;
CREATE ACCESS METHOD diskann TYPE INDEX HANDLER diskannhandler;

SET search_path = 'pg_catalog';

DROP OPERATOR FAMILY IF EXISTS pg_catalog.vector_l2_ops USING diskann CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8400;
CREATE OPERATOR FAMILY pg_catalog.vector_l2_ops USING diskann;

DROP OPERATOR CLASS IF EXISTS pg_catalog.vector_l2_ops USING diskann CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8917;
CREATE OPERATOR CLASS pg_catalog.vector_l2_ops
	FOR TYPE vector USING diskann AS
	OPERATOR 1 pg_catalog.<->(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_l2_squared_distance(vector, vector);

DROP OPERATOR FAMILY IF EXISTS pg_catalog.vector_ip_ops USING diskann CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8401;
CREATE OPERATOR FAMILY pg_catalog.vector_ip_ops USING diskann;

DROP OPERATOR CLASS IF EXISTS pg_catalog.vector_ip_ops USING diskann CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8918;
CREATE OPERATOR CLASS pg_catalog.vector_ip_ops
	FOR TYPE vector USING diskann AS
	OPERATOR 1 pg_catalog.<#>(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_negative_inner_product(vector, vector),
	FUNCTION 4 pg_catalog.vector_norm(vector);

DROP OPERATOR FAMILY IF EXISTS pg_catalog.vector_cosine_ops USING diskann CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8402;
CREATE OPERATOR FAMILY pg_catalog.vector_cosine_ops USING diskann;

DROP OPERATOR CLASS IF EXISTS pg_catalog.vector_cosine_ops USING diskann CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8919;
CREATE OPERATOR CLASS pg_catalog.vector_cosine_ops
	FOR TYPE vector USING diskann AS
	OPERATOR 1 pg_catalog.<=>(vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 pg_catalog.vector_negative_inner_product(vector, vector),
	FUNCTION 2 pg_catalog.vector_norm(vector),
	FUNCTION 4 pg_catalog.vector_norm(vector);
