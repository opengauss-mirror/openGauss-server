DROP TYPE IF EXISTS pg_catalog.halfvec CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_TYPE, 8306, 8309, b;
CREATE TYPE pg_catalog.halfvec;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_in(cstring, oid, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8486;
CREATE FUNCTION pg_catalog.halfvec_in(cstring, oid, int4)
RETURNS halfvec
AS 'halfvec_in'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_out(halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8488;
CREATE FUNCTION pg_catalog.halfvec_out(halfvec)
RETURNS cstring
AS 'halfvec_out'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_typmod_in(_cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8490;
CREATE FUNCTION pg_catalog.halfvec_typmod_in(_cstring)
RETURNS int4
AS 'halfvec_typmod_in'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_recv(internal, oid, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8491;
CREATE FUNCTION pg_catalog.halfvec_recv(internal, oid, int4)
RETURNS halfvec
AS 'halfvec_recv'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_send(halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8492;
CREATE FUNCTION pg_catalog.halfvec_send(halfvec)
RETURNS bytea
AS 'halfvec_send'
LANGUAGE INTERNAL
STABLE STRICT;

CREATE TYPE pg_catalog.halfvec (
	INPUT     = halfvec_in,
	OUTPUT    = halfvec_out,
	TYPMOD_IN = halfvec_typmod_in,
	RECEIVE   = halfvec_recv,
	SEND      = halfvec_send,
	STORAGE   = external
);

DROP FUNCTION IF EXISTS pg_catalog.l2_distance(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8430;
CREATE FUNCTION pg_catalog.l2_distance(halfvec, halfvec)
RETURNS float8
AS 'halfvec_l2_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.inner_product(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8485;
CREATE FUNCTION pg_catalog.inner_product(halfvec, halfvec)
RETURNS float8
AS 'halfvec_inner_product'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.cosine_distance(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8544;
CREATE FUNCTION pg_catalog.cosine_distance(halfvec, halfvec)
RETURNS float8
AS 'halfvec_cosine_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l1_distance(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8484;
CREATE FUNCTION pg_catalog.l1_distance(halfvec, halfvec)
RETURNS float8
AS 'halfvec_l1_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l2_norm(halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8503;
CREATE FUNCTION pg_catalog.l2_norm(halfvec)
RETURNS float8
AS 'halfvec_l2_norm'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.l2_normalize(halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8504;
CREATE FUNCTION pg_catalog.l2_normalize(halfvec)
RETURNS halfvec
AS 'halfvec_l2_normalize'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_lt(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8496;
CREATE FUNCTION pg_catalog.halfvec_lt(halfvec, halfvec)
RETURNS bool
AS 'halfvec_lt'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_le(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8497;
CREATE FUNCTION pg_catalog.halfvec_le(halfvec, halfvec)
RETURNS bool
AS 'halfvec_le'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_eq(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8498;
CREATE FUNCTION pg_catalog.halfvec_eq(halfvec, halfvec)
RETURNS bool
AS 'halfvec_eq'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_ne(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8499;
CREATE FUNCTION pg_catalog.halfvec_ne(halfvec, halfvec)
RETURNS bool
AS 'halfvec_ne'
LANGUAGE INTERNAL
STABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_ge(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8500;
CREATE FUNCTION pg_catalog.halfvec_ge(halfvec, halfvec)
RETURNS bool
AS 'halfvec_ge'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_gt(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8501;
CREATE FUNCTION pg_catalog.halfvec_gt(halfvec, halfvec)
RETURNS bool
AS 'halfvec_gt'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_cmp(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8494;
CREATE FUNCTION pg_catalog.halfvec_cmp(halfvec, halfvec)
RETURNS int4
AS 'halfvec_cmp'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_l2_squared_distance(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8644;
CREATE FUNCTION pg_catalog.halfvec_l2_squared_distance(halfvec, halfvec)
RETURNS float8
AS 'halfvec_l2_squared_distance'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_negative_inner_product(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8493;
CREATE FUNCTION pg_catalog.halfvec_negative_inner_product(halfvec, halfvec)
RETURNS float8
AS 'halfvec_negative_inner_product'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec(halfvec, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8510;
CREATE FUNCTION pg_catalog.halfvec(halfvec, int4, boolean)
RETURNS halfvec
AS 'halfvec'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.vector_to_halfvec(vector, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8505;
CREATE FUNCTION pg_catalog.vector_to_halfvec(vector, int4, boolean)
RETURNS halfvec
AS 'vector_to_halfvec'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.halfvec_to_vector(halfvec, int4, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8506;
CREATE FUNCTION pg_catalog.halfvec_to_vector(halfvec, int4, boolean)
RETURNS vector
AS 'halfvec_to_vector'
LANGUAGE INTERNAL
IMMUTABLE STRICT;

COMMENT ON FUNCTION pg_catalog.halfvec_in(cstring, oid, int4) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.halfvec_out(halfvec) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.halfvec_typmod_in(_cstring) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_recv(internal, oid, int4) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.halfvec_send(halfvec) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.l2_distance(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.inner_product(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.cosine_distance(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l1_distance(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l2_norm(halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.l2_normalize(halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_lt(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_le(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_eq(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_ne(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_ge(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_gt(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_cmp(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_l2_squared_distance(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_negative_inner_product(halfvec, halfvec) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec(halfvec, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.vector_to_halfvec(vector, int4, boolean) IS 'NULL';
COMMENT ON FUNCTION pg_catalog.halfvec_to_vector(halfvec, int4, boolean) IS 'NULL';

DROP CAST IF EXISTS (halfvec AS halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8507;
CREATE CAST (halfvec AS halfvec)
	WITH FUNCTION halfvec(halfvec, int4, boolean) AS IMPLICIT;

DROP CAST IF EXISTS (halfvec AS vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8509;
CREATE CAST (halfvec AS vector)
	WITH FUNCTION halfvec_to_vector(halfvec, int4, boolean) AS ASSIGNMENT;

DROP CAST IF EXISTS (vector AS halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8508;
CREATE CAST (vector AS halfvec)
	WITH FUNCTION vector_to_halfvec(vector, int4, boolean) AS IMPLICIT;

DROP OPERATOR IF EXISTS pg_catalog.<->(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8980;
CREATE OPERATOR pg_catalog.<->(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.l2_distance,
	COMMUTATOR = '<->'
);

DROP OPERATOR IF EXISTS pg_catalog.<#>(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8981;
CREATE OPERATOR pg_catalog.<#>(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.halfvec_negative_inner_product,
	COMMUTATOR = '<#>'
);

DROP OPERATOR IF EXISTS pg_catalog.<=>(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8982;
CREATE OPERATOR pg_catalog.<=>(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.cosine_distance,
	COMMUTATOR = '<=>'
);

DROP OPERATOR IF EXISTS pg_catalog.<+>(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8983;
CREATE OPERATOR pg_catalog.<+>(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.l1_distance,
	COMMUTATOR = '<+>'
);

DROP OPERATOR IF EXISTS pg_catalog.<(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8343;
CREATE OPERATOR pg_catalog.<(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.halfvec_lt,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.<=(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8344;
CREATE OPERATOR pg_catalog.<=(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.halfvec_le,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.=(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8347;
CREATE OPERATOR pg_catalog.=(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.halfvec_eq,
	COMMUTATOR = '=' ,
	RESTRICT = eqsel, JOIN = eqjoinsel, HASHES
);

DROP OPERATOR IF EXISTS pg_catalog.<>(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8348;
CREATE OPERATOR pg_catalog.<>(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.halfvec_ne,
	COMMUTATOR = '<>' , NEGATOR = '=' ,
	RESTRICT = neqsel, JOIN = neqjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.>=(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8346;
CREATE OPERATOR pg_catalog.>=(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.halfvec_ge,
	COMMUTATOR = '<=' , NEGATOR = '<' ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

DROP OPERATOR IF EXISTS pg_catalog.>(halfvec, halfvec) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8345;
CREATE OPERATOR pg_catalog.>(
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = pg_catalog.halfvec_gt,
	COMMUTATOR = '<' , NEGATOR = '<=' ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

COMMENT ON OPERATOR pg_catalog.<->(halfvec,halfvec) IS 'halfvec_l2_distance';
COMMENT ON OPERATOR pg_catalog.<#>(halfvec,halfvec) IS 'halfvec_negative_inner_product';
COMMENT ON OPERATOR pg_catalog.<=>(halfvec,halfvec) IS 'halfvec_cosine_distance';
COMMENT ON OPERATOR pg_catalog.<+>(halfvec,halfvec) IS 'halfvec_l1_distance';
COMMENT ON OPERATOR pg_catalog.<(halfvec,halfvec) IS 'halfvec less than';
COMMENT ON OPERATOR pg_catalog.<=(halfvec,halfvec) IS 'halfvec less than or equal';
COMMENT ON OPERATOR pg_catalog.>(halfvec,halfvec) IS 'halfvec greater than';
COMMENT ON OPERATOR pg_catalog.>=(halfvec,halfvec) IS 'halfvec greater than or equal';
COMMENT ON OPERATOR pg_catalog.=(halfvec,halfvec) IS 'halfvec equal';
COMMENT ON OPERATOR pg_catalog.<>(halfvec,halfvec) IS 'halfvec unequal';

DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ops USING btree CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8520;
CREATE OPERATOR FAMILY pg_catalog.halfvec_ops USING btree;

DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ops USING btree CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8519;
CREATE OPERATOR CLASS pg_catalog.halfvec_ops
	DEFAULT FOR TYPE halfvec USING btree AS
	OPERATOR 1 pg_catalog.<(halfvec, halfvec),
	OPERATOR 2 pg_catalog.<=(halfvec, halfvec),
	OPERATOR 3 pg_catalog.=(halfvec, halfvec),
	OPERATOR 4 pg_catalog.>=(halfvec, halfvec),
	OPERATOR 5 pg_catalog.>(halfvec, halfvec),
	FUNCTION 1 pg_catalog.halfvec_cmp(halfvec, halfvec);

DROP OPERATOR FAMILY IF EXISTS pg_catalog.halfvec_ops USING ubtree CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8582;
CREATE OPERATOR FAMILY pg_catalog.halfvec_ops USING ubtree;

DROP OPERATOR CLASS IF EXISTS pg_catalog.halfvec_ops USING ubtree CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8521;
CREATE OPERATOR CLASS pg_catalog.halfvec_ops DEFAULT
	FOR TYPE halfvec USING ubtree AS
	OPERATOR 1 pg_catalog.<(halfvec, halfvec),
	OPERATOR 2 pg_catalog.<=(halfvec, halfvec),
	OPERATOR 3 pg_catalog.=(halfvec, halfvec),
	OPERATOR 4 pg_catalog.>=(halfvec, halfvec),
	OPERATOR 5 pg_catalog.>(halfvec, halfvec),
	FUNCTION 1 halfvec_cmp(halfvec, halfvec);