create function binary_coercible_2(oid, oid) returns bool as $$
SELECT ($1 = $2) OR
 EXISTS(select 1 from pg_catalog.pg_cast where
        castsource = $1 and casttarget = $2 and
        castmethod = 'b' and castcontext = 'i') OR
 ($2 = 'pg_catalog.anyarray'::pg_catalog.regtype AND
  EXISTS(select 1 from pg_catalog.pg_type where
         oid = $1 and typelem != 0 and typlen = -1))
$$ language sql strict stable;

create function binary_coercible_3(oid, oid) returns bool as $$
SELECT ($1 = $2) OR
 EXISTS(select 1 from pg_catalog.pg_cast where
        castsource = $1 and casttarget = $2 and
        castmethod = 'b' and castcontext = 'i') OR
 ($2 = 'pg_catalog.any'::pg_catalog.regtype) OR
 ($2 = 'pg_catalog.anyarray'::pg_catalog.regtype AND
  EXISTS(select 1 from pg_catalog.pg_type where
         oid = $1 and typelem != 0 and typlen = -1)) OR
 ($2 = 'pg_catalog.anyrange'::pg_catalog.regtype AND
  (select typtype from pg_catalog.pg_type where oid = $1) = 'r')
$$ language sql strict stable;

create function physically_coercible_2(oid, oid) returns bool as $$
SELECT ($1 = $2) OR
 EXISTS(select 1 from pg_catalog.pg_cast where
        castsource = $1 and casttarget = $2 and
        castmethod = 'b') OR
 ($2 = 'pg_catalog.any'::pg_catalog.regtype) OR
 ($2 = 'pg_catalog.anyarray'::pg_catalog.regtype AND
  EXISTS(select 1 from pg_catalog.pg_type where
         oid = $1 and typelem != 0 and typlen = -1)) OR
 ($2 = 'pg_catalog.anyrange'::pg_catalog.regtype AND
  (select typtype from pg_catalog.pg_type where oid = $1) = 'r')
$$ language sql strict stable;

-- **************** pg_operator ****************

-- Look for illegal values in pg_operator fields.

SELECT p1.oid, p1.oprname
FROM pg_operator as p1
WHERE (p1.oprkind != 'b' AND p1.oprkind != 'l' AND p1.oprkind != 'r') OR
    p1.oprresult = 0 OR p1.oprcode = 0;

-- Look for missing or unwanted operand types

SELECT p1.oid, p1.oprname
FROM pg_operator as p1
WHERE (p1.oprleft = 0 and p1.oprkind != 'l') OR
    (p1.oprleft != 0 and p1.oprkind = 'l') OR
    (p1.oprright = 0 and p1.oprkind != 'r') OR
    (p1.oprright != 0 and p1.oprkind = 'r');

-- Look for conflicting operator definitions (same names and input datatypes).

SELECT p1.oid, p1.oprcode, p2.oid, p2.oprcode
FROM pg_operator AS p1, pg_operator AS p2
WHERE p1.oid != p2.oid AND
    p1.oprname = p2.oprname AND
    p1.oprkind = p2.oprkind AND
    p1.oprleft = p2.oprleft AND
    p1.oprright = p2.oprright;

-- Look for commutative operators that don't commute.
-- DEFINITIONAL NOTE: If A.oprcom = B, then x A y has the same result as y B x.
-- We expect that B will always say that B.oprcom = A as well; that's not
-- inherently essential, but it would be inefficient not to mark it so.

SELECT p1.oid, p1.oprcode, p2.oid, p2.oprcode
FROM pg_operator AS p1, pg_operator AS p2
WHERE p1.oprcom = p2.oid AND
    (p1.oprkind != 'b' OR
     p1.oprleft != p2.oprright OR
     p1.oprright != p2.oprleft OR
     p1.oprresult != p2.oprresult OR
     p1.oid != p2.oprcom);

-- Look for negatory operators that don't agree.
-- DEFINITIONAL NOTE: If A.oprnegate = B, then both A and B must yield
-- boolean results, and (x A y) == ! (x B y), or the equivalent for
-- single-operand operators.
-- We expect that B will always say that B.oprnegate = A as well; that's not
-- inherently essential, but it would be inefficient not to mark it so.
-- Also, A and B had better not be the same operator.

SELECT p1.oid, p1.oprcode, p2.oid, p2.oprcode
FROM pg_operator AS p1, pg_operator AS p2
WHERE p1.oprnegate = p2.oid AND
    (p1.oprkind != p2.oprkind OR
     p1.oprleft != p2.oprleft OR
     p1.oprright != p2.oprright OR
     p1.oprresult != 'bool'::regtype OR
     p2.oprresult != 'bool'::regtype OR
     p1.oid != p2.oprnegate OR
     p1.oid = p2.oid);

-- A mergejoinable or hashjoinable operator must be binary, must return
-- boolean, and must have a commutator (itself, unless it's a cross-type
-- operator).

SELECT p1.oid, p1.oprname FROM pg_operator AS p1
WHERE (p1.oprcanmerge OR p1.oprcanhash) AND NOT
    (p1.oprkind = 'b' AND p1.oprresult = 'bool'::regtype AND p1.oprcom != 0);

-- What's more, the commutator had better be mergejoinable/hashjoinable too.

SELECT p1.oid, p1.oprname, p2.oid, p2.oprname
FROM pg_operator AS p1, pg_operator AS p2
WHERE p1.oprcom = p2.oid AND
    (p1.oprcanmerge != p2.oprcanmerge OR
     p1.oprcanhash != p2.oprcanhash);

-- Mergejoinable operators should appear as equality members of btree index
-- opfamilies.

SELECT p1.oid, p1.oprname
FROM pg_operator AS p1
WHERE p1.oprcanmerge AND NOT EXISTS
  (SELECT 1 FROM pg_amop
   WHERE amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
         amopopr = p1.oid AND amopstrategy = 3);

-- And the converse.

SELECT p1.oid, p1.oprname, p.amopfamily
FROM pg_operator AS p1, pg_amop p
WHERE amopopr = p1.oid
  AND amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree')
  AND amopstrategy = 3
  AND NOT p1.oprcanmerge;

-- Hashable operators should appear as members of hash index opfamilies.

SELECT p1.oid, p1.oprname
FROM pg_operator AS p1
WHERE p1.oprcanhash AND NOT EXISTS
  (SELECT 1 FROM pg_amop
   WHERE amopmethod = (SELECT oid FROM pg_am WHERE amname = 'hash') AND
         amopopr = p1.oid AND amopstrategy = 1);

-- And the converse.

SELECT p1.oid, p1.oprname, p.amopfamily
FROM pg_operator AS p1, pg_amop p
WHERE amopopr = p1.oid
  AND amopmethod = (SELECT oid FROM pg_am WHERE amname = 'hash')
  AND NOT p1.oprcanhash;

-- Check that each operator defined in pg_operator matches its oprcode entry
-- in pg_proc.  Easiest to do this separately for each oprkind.

SELECT p1.oid, p1.oprname, p2.oid, p2.proname
FROM pg_operator AS p1, pg_proc AS p2
WHERE p1.oprcode = p2.oid AND
    p1.oprkind = 'b' AND
    (p2.pronargs != 2
     OR NOT binary_coercible_2(p2.prorettype, p1.oprresult)
     OR NOT binary_coercible_2(p1.oprleft, p2.proargtypes[0])
     OR NOT binary_coercible_2(p1.oprright, p2.proargtypes[1]));

SELECT p1.oid, p1.oprname, p2.oid, p2.proname
FROM pg_operator AS p1, pg_proc AS p2
WHERE p1.oprcode = p2.oid AND
    p1.oprkind = 'l' AND
    (p2.pronargs != 1
     OR NOT binary_coercible_2(p2.prorettype, p1.oprresult)
     OR NOT binary_coercible_2(p1.oprright, p2.proargtypes[0])
     OR p1.oprleft != 0);

SELECT p1.oid, p1.oprname, p2.oid, p2.proname
FROM pg_operator AS p1, pg_proc AS p2
WHERE p1.oprcode = p2.oid AND
    p1.oprkind = 'r' AND
    (p2.pronargs != 1
     OR NOT binary_coercible_2(p2.prorettype, p1.oprresult)
     OR NOT binary_coercible_2(p1.oprleft, p2.proargtypes[0])
     OR p1.oprright != 0);

-- If the operator is mergejoinable or hashjoinable, its underlying function
-- should not be volatile.

SELECT p1.oid, p1.oprname, p2.oid, p2.proname
FROM pg_operator AS p1, pg_proc AS p2
WHERE p1.oprcode = p2.oid AND
    (p1.oprcanmerge OR p1.oprcanhash) AND
    p2.provolatile = 'v';

-- If oprrest is set, the operator must return boolean,
-- and it must link to a proc with the right signature
-- to be a restriction selectivity estimator.
-- The proc signature we want is: float8 proc(internal, oid, internal, int4)

SELECT p1.oid, p1.oprname, p2.oid, p2.proname
FROM pg_operator AS p1, pg_proc AS p2
WHERE p1.oprrest = p2.oid AND
    (p1.oprresult != 'bool'::regtype OR
     p2.prorettype != 'float8'::regtype OR p2.proretset OR
     p2.pronargs != 4 OR
     p2.proargtypes[0] != 'internal'::regtype OR
     p2.proargtypes[1] != 'oid'::regtype OR
     p2.proargtypes[2] != 'internal'::regtype OR
     p2.proargtypes[3] != 'int4'::regtype);

-- If oprjoin is set, the operator must be a binary boolean op,
-- and it must link to a proc with the right signature
-- to be a join selectivity estimator.
-- The proc signature we want is: float8 proc(internal, oid, internal, int2, internal)
-- (Note: the old signature with only 4 args is still allowed, but no core
-- estimator should be using it.)

SELECT p1.oid, p1.oprname, p2.oid, p2.proname
FROM pg_operator AS p1, pg_proc AS p2
WHERE p1.oprjoin = p2.oid AND
    (p1.oprkind != 'b' OR p1.oprresult != 'bool'::regtype OR
     p2.prorettype != 'float8'::regtype OR p2.proretset OR
     p2.pronargs != 5 OR
     p2.proargtypes[0] != 'internal'::regtype OR
     p2.proargtypes[1] != 'oid'::regtype OR
     p2.proargtypes[2] != 'internal'::regtype OR
     p2.proargtypes[3] != 'int2'::regtype OR
     p2.proargtypes[4] != 'internal'::regtype);

-- Insist that all built-in pg_operator entries have descriptions
SELECT p1.oid, p1.oprname
FROM pg_operator as p1 LEFT JOIN pg_description as d
     ON p1.tableoid = d.classoid and p1.oid = d.objoid and d.objsubid = 0
WHERE d.classoid IS NULL AND p1.oid <= 9999;

-- Check that operators' underlying functions have suitable comments,
-- namely 'implementation of XXX operator'.  In some cases involving legacy
-- names for operators, there are multiple operators referencing the same
-- pg_proc entry, so ignore operators whose comments say they are deprecated.
-- We also have a few functions that are both operator support and meant to
-- be called directly; those should have comments matching their operator.
WITH funcdescs AS (
  SELECT p.oid as p_oid, proname, o.oid as o_oid,
    obj_description(p.oid, 'pg_proc') as prodesc,
    'implementation of ' || oprname || ' operator' as expecteddesc,
    obj_description(o.oid, 'pg_operator') as oprdesc
  FROM pg_proc p JOIN pg_operator o ON oprcode = p.oid
  WHERE o.oid <= 9999
)
SELECT * FROM funcdescs
  WHERE prodesc IS DISTINCT FROM expecteddesc
    AND oprdesc NOT LIKE 'deprecated%'
    AND prodesc IS DISTINCT FROM oprdesc;


-- **************** pg_aggregate ****************

-- Look for illegal values in pg_aggregate fields.

SELECT ctid, aggfnoid::oid
FROM pg_aggregate as p1
WHERE aggfnoid = 0 OR aggtransfn = 0 OR aggtranstype = 0;

-- Make sure the matching pg_proc entry is sensible, too.

SELECT a.aggfnoid::oid, p.proname
FROM pg_aggregate as a, pg_proc as p
WHERE a.aggfnoid = p.oid AND
    (NOT p.proisagg OR p.proretset);

-- Make sure there are no proisagg pg_proc entries without matches.

SELECT oid, proname
FROM pg_proc as p
WHERE p.proisagg AND
    NOT EXISTS (SELECT 1 FROM pg_aggregate a WHERE a.aggfnoid = p.oid);

-- If there is no finalfn then the output type must be the transtype.

SELECT a.aggfnoid::oid, p.proname
FROM pg_aggregate as a, pg_proc as p
WHERE a.aggfnoid = p.oid AND
    a.aggfinalfn = 0 AND p.prorettype != a.aggtranstype;

-- Cross-check transfn against its entry in pg_proc.
-- NOTE: use physically_coercible_2 here, not binary_coercible_2, because
-- max and min on abstime are implemented using int4larger/int4smaller.
SELECT a.aggfnoid::oid, p.proname, ptr.oid, ptr.proname
FROM pg_aggregate AS a, pg_proc AS p, pg_proc AS ptr
WHERE a.aggfnoid = p.oid AND
    a.aggtransfn = ptr.oid AND
    (ptr.proretset
     OR NOT (ptr.pronargs = 
             CASE WHEN a.aggkind = 'n' THEN p.pronargs + 1
             ELSE greatest(p.pronargs - a.aggnumdirectargs, 1) + 1 END)
     OR NOT physically_coercible_2(ptr.prorettype, a.aggtranstype)
     OR NOT physically_coercible_2(a.aggtranstype, ptr.proargtypes[0])
     OR (p.pronargs > 0 AND
         NOT physically_coercible_2(p.proargtypes[0], ptr.proargtypes[1]))
     OR (p.pronargs > 1 AND
         NOT physically_coercible_2(p.proargtypes[1], ptr.proargtypes[2]))
     OR (p.pronargs > 2 AND
         NOT physically_coercible_2(p.proargtypes[2], ptr.proargtypes[3]))
     -- we could carry the check further, but that's enough for now
    );

-- Cross-check finalfn (if present) against its entry in pg_proc.

SELECT a.aggfnoid::oid, p.proname, pfn.oid, pfn.proname
FROM pg_aggregate AS a, pg_proc AS p, pg_proc AS pfn
WHERE a.aggfnoid = p.oid AND
    a.aggfinalfn = pfn.oid AND
    (pfn.proretset OR
     NOT binary_coercible_3(pfn.prorettype, p.prorettype) OR
     NOT binary_coercible_3(a.aggtranstype, pfn.proargtypes[0]) 
     OR (pfn.pronargs > 1 AND
         NOT binary_coercible_3(p.proargtypes[0], pfn.proargtypes[1]))
     OR (pfn.pronargs > 2 AND
         NOT binary_coercible_3(p.proargtypes[1], pfn.proargtypes[2]))
     OR (pfn.pronargs > 3 AND
         NOT binary_coercible_3(p.proargtypes[2], pfn.proargtypes[3]))
     -- we could carry the check further, but 3 args is enough for now
    );

-- If transfn is strict then either initval should be non-NULL, or
-- input type should match transtype so that the first non-null input
-- can be assigned as the state value.

SELECT a.aggfnoid::oid, p.proname, ptr.oid, ptr.proname
FROM pg_aggregate AS a, pg_proc AS p, pg_proc AS ptr
WHERE a.aggfnoid = p.oid AND
    a.aggtransfn = ptr.oid AND ptr.proisstrict AND
    a.agginitval IS NULL AND
    NOT binary_coercible_2(p.proargtypes[0], a.aggtranstype);

-- Cross-check aggsortop (if present) against pg_operator.
-- We expect to find entries for bool_and, bool_or, every, max, and min.

SELECT DISTINCT proname, oprname
FROM pg_operator AS o, pg_aggregate AS a, pg_proc AS p
WHERE a.aggfnoid = p.oid AND a.aggsortop = o.oid
ORDER BY 1, 2;

-- Check datatypes match

SELECT a.aggfnoid::oid, o.oid
FROM pg_operator AS o, pg_aggregate AS a, pg_proc AS p
WHERE a.aggfnoid = p.oid AND a.aggsortop = o.oid AND
    (oprkind != 'b' OR oprresult != 'boolean'::regtype
     OR oprleft != p.proargtypes[0] OR oprright != p.proargtypes[0]);

-- Check operator is a suitable btree opfamily member

SELECT a.aggfnoid::oid, o.oid
FROM pg_operator AS o, pg_aggregate AS a, pg_proc AS p
WHERE a.aggfnoid = p.oid AND a.aggsortop = o.oid AND
    NOT EXISTS(SELECT 1 FROM pg_amop
               WHERE amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree')
                     AND amopopr = o.oid
                     AND amoplefttype = o.oprleft
                     AND amoprighttype = o.oprright);

-- Check correspondence of btree strategies and names

SELECT DISTINCT proname, oprname, amopstrategy
FROM pg_operator AS o, pg_aggregate AS a, pg_proc AS p,
     pg_amop as ao
WHERE a.aggfnoid = p.oid AND a.aggsortop = o.oid AND
    amopopr = o.oid AND
    amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree')
ORDER BY 1, 2;

-- Check that there are not aggregates with the same name and different
-- numbers of arguments.  While not technically wrong, we have a project policy
-- to avoid this because it opens the door for confusion in connection with
-- ORDER BY: novices frequently put the ORDER BY in the wrong place.
-- See the fate of the single-argument form of string_agg() for history.
-- The only aggregates that should show up here are count(x) and count(*).

SELECT p1.oid::regprocedure, p2.oid::regprocedure
FROM pg_proc AS p1, pg_proc AS p2
WHERE p1.oid < p2.oid AND p1.proname = p2.proname AND
    p1.proisagg AND p2.proisagg AND
    array_dims(p1.proargtypes) != array_dims(p2.proargtypes) AND
    p1.proname != 'listagg'
ORDER BY 1;

-- For the same reason, aggregates with default arguments are no good.

SELECT oid, proname
FROM pg_proc AS p
WHERE proisagg AND proargdefaults IS NOT NULL;
