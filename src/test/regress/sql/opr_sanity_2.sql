create function binary_coercible_1(oid, oid) returns bool as $$
SELECT ($1 = $2) OR
 EXISTS(select 1 from pg_catalog.pg_cast where
        castsource = $1 and casttarget = $2 and
        castmethod = 'b' and castcontext = 'i') OR
 ($2 = 'pg_catalog.anyarray'::pg_catalog.regtype AND
  EXISTS(select 1 from pg_catalog.pg_type where
         oid = $1 and typelem != 0 and typlen = -1))
$$ language sql strict stable;

create function physically_coercible_1(oid, oid) returns bool as $$
SELECT ($1 = $2) OR
 EXISTS(select 1 from pg_catalog.pg_cast where
        castsource = $1 and casttarget = $2 and
        castmethod = 'b') OR
 ($2 = 'pg_catalog.anyarray'::pg_catalog.regtype AND
  EXISTS(select 1 from pg_catalog.pg_type where
         oid = $1 and typelem != 0 and typlen = -1))
$$ language sql strict stable;

-- **************** pg_opfamily ****************

-- Look for illegal values in pg_opfamily fields

SELECT p1.oid
FROM pg_opfamily as p1
WHERE p1.opfmethod = 0 OR p1.opfnamespace = 0;

-- **************** pg_opclass ****************

-- Look for illegal values in pg_opclass fields

SELECT p1.oid
FROM pg_opclass AS p1
WHERE p1.opcmethod = 0 OR p1.opcnamespace = 0 OR p1.opcfamily = 0
    OR p1.opcintype = 0;

-- opcmethod must match owning opfamily's opfmethod

SELECT p1.oid, p2.oid
FROM pg_opclass AS p1, pg_opfamily AS p2
WHERE p1.opcfamily = p2.oid AND p1.opcmethod != p2.opfmethod;

-- There should not be multiple entries in pg_opclass with opcdefault true
-- and the same opcmethod/opcintype combination.

SELECT p1.oid, p2.oid
FROM pg_opclass AS p1, pg_opclass AS p2
WHERE p1.oid != p2.oid AND
    p1.opcmethod = p2.opcmethod AND p1.opcintype = p2.opcintype AND
    p1.opcdefault AND p2.opcdefault;

-- **************** pg_amop ****************

-- Look for illegal values in pg_amop fields

SELECT p1.amopfamily, p1.amopstrategy
FROM pg_amop as p1
WHERE p1.amopfamily = 0 OR p1.amoplefttype = 0 OR p1.amoprighttype = 0
    OR p1.amopopr = 0 OR p1.amopmethod = 0 OR p1.amopstrategy < 1;

SELECT p1.amopfamily, p1.amopstrategy
FROM pg_amop as p1
WHERE NOT ((p1.amoppurpose = 's' AND p1.amopsortfamily = 0) OR
           (p1.amoppurpose = 'o' AND p1.amopsortfamily <> 0));

-- amoplefttype/amoprighttype must match the operator

SELECT p1.oid, p2.oid
FROM pg_amop AS p1, pg_operator AS p2
WHERE p1.amopopr = p2.oid AND NOT
    (p1.amoplefttype = p2.oprleft AND p1.amoprighttype = p2.oprright);

-- amopmethod must match owning opfamily's opfmethod

SELECT p1.oid, p2.oid
FROM pg_amop AS p1, pg_opfamily AS p2
WHERE p1.amopfamily = p2.oid AND p1.amopmethod != p2.opfmethod;

-- amopsortfamily, if present, must reference a btree family

SELECT p1.amopfamily, p1.amopstrategy
FROM pg_amop AS p1
WHERE p1.amopsortfamily <> 0 AND NOT EXISTS
    (SELECT 1 from pg_opfamily op WHERE op.oid = p1.amopsortfamily
     AND op.opfmethod = (SELECT oid FROM pg_am WHERE amname = 'btree'));

-- check for ordering operators not supported by parent AM

SELECT p1.amopfamily, p1.amopopr, p2.oid, p2.amname
FROM pg_amop AS p1, pg_am AS p2
WHERE p1.amopmethod = p2.oid AND
    p1.amoppurpose = 'o' AND NOT p2.amcanorderbyop;

-- Cross-check amopstrategy index against parent AM

SELECT p1.amopfamily, p1.amopopr, p2.oid, p2.amname
FROM pg_amop AS p1, pg_am AS p2
WHERE p1.amopmethod = p2.oid AND
    p1.amopstrategy > p2.amstrategies AND p2.amstrategies <> 0;

-- Detect missing pg_amop entries: should have as many strategy operators
-- as AM expects for each datatype combination supported by the opfamily.
-- We can't check this for AMs with variable strategy sets.

SELECT p1.amname, p2.amoplefttype, p2.amoprighttype
FROM pg_am AS p1, pg_amop AS p2
WHERE p2.amopmethod = p1.oid AND
    p1.amstrategies <> 0 AND
    p1.amstrategies != (SELECT count(*) FROM pg_amop AS p3
                        WHERE p3.amopfamily = p2.amopfamily AND
                              p3.amoplefttype = p2.amoplefttype AND
                              p3.amoprighttype = p2.amoprighttype AND
                              p3.amoppurpose = 's');

-- Currently, none of the AMs with fixed strategy sets support ordering ops.

SELECT p1.amname, p2.amopfamily, p2.amopstrategy
FROM pg_am AS p1, pg_amop AS p2
WHERE p2.amopmethod = p1.oid AND
    p1.amstrategies <> 0 AND p2.amoppurpose <> 's';

-- Check that amopopr points at a reasonable-looking operator, ie a binary
-- operator.  If it's a search operator it had better yield boolean,
-- otherwise an input type of its sort opfamily.

SELECT p1.amopfamily, p1.amopopr, p2.oid, p2.oprname
FROM pg_amop AS p1, pg_operator AS p2
WHERE p1.amopopr = p2.oid AND
    p2.oprkind != 'b';

SELECT p1.amopfamily, p1.amopopr, p2.oid, p2.oprname
FROM pg_amop AS p1, pg_operator AS p2
WHERE p1.amopopr = p2.oid AND p1.amoppurpose = 's' AND
    p2.oprresult != 'bool'::regtype;

SELECT p1.amopfamily, p1.amopopr, p2.oid, p2.oprname
FROM pg_amop AS p1, pg_operator AS p2
WHERE p1.amopopr = p2.oid AND p1.amoppurpose = 'o' AND NOT EXISTS
    (SELECT 1 FROM pg_opclass op
     WHERE opcfamily = p1.amopsortfamily AND opcintype = p2.oprresult);

-- Make a list of all the distinct operator names being used in particular
-- strategy slots.  This is a bit hokey, since the list might need to change
-- in future releases, but it's an effective way of spotting mistakes such as
-- swapping two operators within a family.

SELECT DISTINCT amopmethod, amopstrategy, oprname
FROM pg_amop p1 LEFT JOIN pg_operator p2 ON amopopr = p2.oid
ORDER BY 1, 2, 3;

-- Check that all opclass search operators have selectivity estimators.
-- This is not absolutely required, but it seems a reasonable thing
-- to insist on for all standard datatypes.

SELECT p1.amopfamily, p1.amopopr, p2.oid, p2.oprname
FROM pg_amop AS p1, pg_operator AS p2
WHERE p1.amopopr = p2.oid AND p1.amoppurpose = 's' AND
    (p2.oprrest = 0 OR p2.oprjoin = 0);

-- Check that each opclass in an opfamily has associated operators, that is
-- ones whose oprleft matches opcintype (possibly by coercion).

SELECT p1.opcname, p1.opcfamily
FROM pg_opclass AS p1
WHERE NOT EXISTS(SELECT 1 FROM pg_amop AS p2
                 WHERE p2.amopfamily = p1.opcfamily
                   AND binary_coercible_1(p1.opcintype, p2.amoplefttype));

-- Check that each operator listed in pg_amop has an associated opclass,
-- that is one whose opcintype matches oprleft (possibly by coercion).
-- Otherwise the operator is useless because it cannot be matched to an index.
-- (In principle it could be useful to list such operators in multiple-datatype
-- btree opfamilies, but in practice you'd expect there to be an opclass for
-- every datatype the family knows about.)

SELECT p1.amopfamily, p1.amopstrategy, p1.amopopr
FROM pg_amop AS p1
WHERE NOT EXISTS(SELECT 1 FROM pg_opclass AS p2
                 WHERE p2.opcfamily = p1.amopfamily
                   AND binary_coercible_1(p2.opcintype, p1.amoplefttype));

-- Operators that are primary members of opclasses must be immutable (else
-- it suggests that the index ordering isn't fixed).  Operators that are
-- cross-type members need only be stable, since they are just shorthands
-- for index probe queries.

SELECT p1.amopfamily, p1.amopopr, p2.oprname, p3.prosrc
FROM pg_amop AS p1, pg_operator AS p2, pg_proc AS p3
WHERE p1.amopopr = p2.oid AND p2.oprcode = p3.oid AND
    p1.amoplefttype = p1.amoprighttype AND
    p3.provolatile != 'i';

SELECT p1.amopfamily, p1.amopopr, p2.oprname, p3.prosrc
FROM pg_amop AS p1, pg_operator AS p2, pg_proc AS p3
WHERE p1.amopopr = p2.oid AND p2.oprcode = p3.oid AND
    p1.amoplefttype != p1.amoprighttype AND
    p3.provolatile = 'v';

-- Multiple-datatype btree opfamilies should provide closed sets of equality
-- operators; that is if you provide int2 = int4 and int4 = int8 then you
-- should also provide int2 = int8 (and commutators of all these).  This is
-- important because the planner tries to deduce additional qual clauses from
-- transitivity of mergejoinable operators.  If there are clauses
-- int2var = int4var and int4var = int8var, the planner will want to deduce
-- int2var = int8var ... so there should be a way to represent that.  While
-- a missing cross-type operator is now only an efficiency loss rather than
-- an error condition, it still seems reasonable to insist that all built-in
-- opfamilies be complete.

-- check commutative closure
SELECT p1.amoplefttype, p1.amoprighttype
FROM pg_amop AS p1
WHERE p1.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
    p1.amopstrategy = 3 AND
    p1.amoplefttype != p1.amoprighttype AND
    NOT EXISTS(SELECT 1 FROM pg_amop p2 WHERE
                 p2.amopfamily = p1.amopfamily AND
                 p2.amoplefttype = p1.amoprighttype AND
                 p2.amoprighttype = p1.amoplefttype AND
                 p2.amopstrategy = 3);

-- check transitive closure
SELECT p1.amoplefttype, p1.amoprighttype, p2.amoprighttype
FROM pg_amop AS p1, pg_amop AS p2
WHERE p1.amopfamily = p2.amopfamily AND
    p1.amoprighttype = p2.amoplefttype AND
    p1.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
    p2.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
    p1.amopstrategy = 3 AND p2.amopstrategy = 3 AND
    p1.amoplefttype != p1.amoprighttype AND
    p2.amoplefttype != p2.amoprighttype AND
    NOT EXISTS(SELECT 1 FROM pg_amop p3 WHERE
                 p3.amopfamily = p1.amopfamily AND
                 p3.amoplefttype = p1.amoplefttype AND
                 p3.amoprighttype = p2.amoprighttype AND
                 p3.amopstrategy = 3);

-- We also expect that built-in multiple-datatype hash opfamilies provide
-- complete sets of cross-type operators.  Again, this isn't required, but
-- it is reasonable to expect it for built-in opfamilies.

-- if same family has x=x and y=y, it should have x=y
SELECT p1.amoplefttype, p2.amoplefttype
FROM pg_amop AS p1, pg_amop AS p2
WHERE p1.amopfamily = p2.amopfamily AND
    p1.amoplefttype = p1.amoprighttype AND
    p2.amoplefttype = p2.amoprighttype AND
    p1.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'hash') AND
    p2.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'hash') AND
    p1.amopstrategy = 1 AND p2.amopstrategy = 1 AND
    p1.amoplefttype != p2.amoplefttype AND
    NOT EXISTS(SELECT 1 FROM pg_amop p3 WHERE
                 p3.amopfamily = p1.amopfamily AND
                 p3.amoplefttype = p1.amoplefttype AND
                 p3.amoprighttype = p2.amoplefttype AND
                 p3.amopstrategy = 1);


-- **************** pg_amproc ****************

-- Look for illegal values in pg_amproc fields

SELECT p1.amprocfamily, p1.amprocnum
FROM pg_amproc as p1
WHERE p1.amprocfamily = 0 OR p1.amproclefttype = 0 OR p1.amprocrighttype = 0
    OR p1.amprocnum < 1 OR p1.amproc = 0;

-- Cross-check amprocnum index against parent AM

SELECT p1.amprocfamily, p1.amprocnum, p2.oid, p2.amname
FROM pg_amproc AS p1, pg_am AS p2, pg_opfamily AS p3
WHERE p1.amprocfamily = p3.oid AND p3.opfmethod = p2.oid AND
    p1.amprocnum > p2.amsupport;

-- Detect missing pg_amproc entries: should have as many support functions
-- as AM expects for each datatype combination supported by the opfamily.
-- btree/GiST/GIN each allow one optional support function, though.

SELECT p1.amname, p2.opfname, p3.amproclefttype, p3.amprocrighttype
FROM pg_am AS p1, pg_opfamily AS p2, pg_amproc AS p3
WHERE p2.opfmethod = p1.oid AND p3.amprocfamily = p2.oid AND
    (SELECT count(*) FROM pg_amproc AS p4
     WHERE p4.amprocfamily = p2.oid AND
           p4.amproclefttype = p3.amproclefttype AND
           p4.amprocrighttype = p3.amprocrighttype)
    NOT BETWEEN
      (CASE WHEN p1.amname IN ('gist', 'gin') THEN p1.amsupport - 1
            WHEN p1.amname IN ('btree', 'ubtree') THEN p1.amsupport - 2
            WHEN p1.amname IN ('ivfflat', 'hnsw') THEN p1.amsupport - 3
            ELSE p1.amsupport END)
      AND p1.amsupport;

-- Also, check if there are any pg_opclass entries that don't seem to have
-- pg_amproc support.  Again, opclasses with an optional support proc have
-- to be checked specially.

SELECT amname, opcname, count(*)
FROM pg_am am JOIN pg_opclass op ON opcmethod = am.oid
     LEFT JOIN pg_amproc p ON amprocfamily = opcfamily AND
         amproclefttype = amprocrighttype AND amproclefttype = opcintype
WHERE am.amname <> 'btree' AND am.amname <> 'gist' AND am.amname <> 'gin' AND am.amname <> 'ubtree'
    AND am.amname <> 'hnsw' AND am.amname <> 'ivfflat'
GROUP BY amname, amsupport, opcname, amprocfamily
HAVING count(*) != amsupport OR amprocfamily IS NULL;

SELECT amname, opcname, count(*)
FROM pg_am am JOIN pg_opclass op ON opcmethod = am.oid
     LEFT JOIN pg_amproc p ON amprocfamily = opcfamily AND
         amproclefttype = amprocrighttype AND amproclefttype = opcintype
WHERE am.amname = 'gist' OR am.amname = 'gin'
GROUP BY amname, amsupport, opcname, amprocfamily
HAVING (count(*) != amsupport AND count(*) != amsupport - 1)
    OR amprocfamily IS NULL;

SELECT amname, opcname, count(*)
FROM pg_am am JOIN pg_opclass op ON opcmethod = am.oid
     LEFT JOIN pg_amproc p ON amprocfamily = opcfamily AND
         amproclefttype = amprocrighttype AND amproclefttype = opcintype
WHERE am.amname = 'btree' OR am.amname = 'ubtree'
GROUP BY amname, amsupport, opcname, amprocfamily
HAVING (count(*) != amsupport AND count(*) != amsupport - 1 and count(*) != amsupport - 2)
    OR amprocfamily IS NULL;

-- Unfortunately, we can't check the amproc link very well because the
-- signature of the function may be different for different support routines
-- or different base data types.
-- We can check that all the referenced instances of the same support
-- routine number take the same number of parameters, but that's about it
-- for a general check...

SELECT p1.amprocfamily, p1.amprocnum,
	p2.oid, p2.proname,
	p3.opfname,
	p4.amprocfamily, p4.amprocnum,
	p5.oid, p5.proname,
	p6.opfname
FROM pg_amproc AS p1, pg_proc AS p2, pg_opfamily AS p3,
     pg_amproc AS p4, pg_proc AS p5, pg_opfamily AS p6
WHERE p1.amprocfamily = p3.oid AND p4.amprocfamily = p6.oid AND
    p3.opfmethod = p6.opfmethod AND p1.amprocnum = p4.amprocnum AND
    p1.amproc = p2.oid AND p4.amproc = p5.oid AND
    (p2.proretset OR p5.proretset OR p2.pronargs != p5.pronargs);

-- For btree, though, we can do better since we know the support routines
-- must be of the form cmp(lefttype, righttype) returns int4
-- or sortsupport(internal) returns void.

SELECT p1.amprocfamily, p1.amprocnum,
	p2.oid, p2.proname,
	p3.opfname
FROM pg_amproc AS p1, pg_proc AS p2, pg_opfamily AS p3
WHERE p3.opfmethod = (SELECT oid FROM pg_am WHERE amname = 'btree')
    AND p1.amprocfamily = p3.oid AND p1.amproc = p2.oid AND
    (CASE WHEN amprocnum = 1
          THEN prorettype != 'int4'::regtype OR proretset OR pronargs != 2
               OR proargtypes[0] != amproclefttype
               OR proargtypes[1] != amprocrighttype
          WHEN amprocnum = 2
          THEN prorettype != 'void'::regtype OR proretset OR pronargs != 1
               OR proargtypes[0] != 'internal'::regtype
          WHEN amprocnum = 3
          THEN prorettype != 'boolean'::regtype OR proretset OR pronargs != 1
               OR amproclefttype != amprocrighttype
          ELSE true END);

-- For hash we can also do a little better: the support routines must be
-- of the form hash(lefttype) returns int4.  There are several cases where
-- we cheat and use a hash function that is physically compatible with the
-- datatype even though there's no cast, so this check does find a small
-- number of entries.

SELECT p1.amprocfamily, p1.amprocnum, p2.proname, p3.opfname
FROM pg_amproc AS p1, pg_proc AS p2, pg_opfamily AS p3
WHERE p3.opfmethod = (SELECT oid FROM pg_am WHERE amname = 'hash')
    AND p1.amprocfamily = p3.oid AND p1.amproc = p2.oid AND
    (amprocnum != 1
     OR proretset
     OR prorettype != 'int4'::regtype
     OR pronargs != 1
     OR NOT physically_coercible_1(amproclefttype, proargtypes[0])
     OR amproclefttype != amprocrighttype)
ORDER BY 1;

-- We can also check SP-GiST carefully, since the support routine signatures
-- are independent of the datatype being indexed.

SELECT p1.amprocfamily, p1.amprocnum,
	p2.oid, p2.proname,
	p3.opfname
FROM pg_amproc AS p1, pg_proc AS p2, pg_opfamily AS p3
WHERE p3.opfmethod = (SELECT oid FROM pg_am WHERE amname = 'spgist')
    AND p1.amprocfamily = p3.oid AND p1.amproc = p2.oid AND
    (CASE WHEN amprocnum = 1 OR amprocnum = 2 OR amprocnum = 3 OR amprocnum = 4
          THEN prorettype != 'void'::regtype OR proretset OR pronargs != 2
               OR proargtypes[0] != 'internal'::regtype
               OR proargtypes[1] != 'internal'::regtype
          WHEN amprocnum = 5
          THEN prorettype != 'bool'::regtype OR proretset OR pronargs != 2
               OR proargtypes[0] != 'internal'::regtype
               OR proargtypes[1] != 'internal'::regtype
          ELSE true END);

-- Support routines that are primary members of opfamilies must be immutable
-- (else it suggests that the index ordering isn't fixed).  But cross-type
-- members need only be stable, since they are just shorthands
-- for index probe queries.

SELECT p1.amprocfamily, p1.amproc, p2.prosrc
FROM pg_amproc AS p1, pg_proc AS p2
WHERE p1.amproc = p2.oid AND
    p1.amproclefttype = p1.amprocrighttype AND
    p2.provolatile != 'i';

SELECT p1.amprocfamily, p1.amproc, p2.prosrc
FROM pg_amproc AS p1, pg_proc AS p2
WHERE p1.amproc = p2.oid AND
    p1.amproclefttype != p1.amprocrighttype AND
    p2.provolatile = 'v';
