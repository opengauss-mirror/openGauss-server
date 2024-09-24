-- ======================================================================================================================================
-- README: 
-- "--nspt " means the feature that openGauss not supported. We don't remove it directly, leave it for incremental adaptation.
-- 
-- The following test cases are available:
--     postgres_fdw           : This test case is modified from the commit:164d174bbf9a3aba719c845497863cd3c49a3ad0 of PG14.4.
--     postgres_fdw_cstore    : Test column orientation table.
--     postgres_fdw_partition : This is a beta feature in openGauss, open it by "set sql_beta_feature='partition_fdw_on'"
-- ======================================================================================================================================
create database postgresfdw_test_db;
\c postgresfdw_test_db
set show_fdw_remote_plan = on;

-- ======================================================================================================================================
-- TEST-MODULE: create FDW objects
-- --------------------------------------
-- ======================================================================================================================================
CREATE EXTENSION postgres_fdw;

CREATE SERVER testserver1 FOREIGN DATA WRAPPER postgres_fdw;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
        EXECUTE $$CREATE SERVER loopback2 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
        EXECUTE $$CREATE SERVER loopback3 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

CREATE USER MAPPING FOR public SERVER testserver1
    OPTIONS (user 'value', password 'value');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback2;
CREATE USER MAPPING FOR public SERVER loopback3;
-- ======================================================================================================================================
-- TEST-MODULE: create objects used through FDW loopback server
-- --------------------------------------
-- ======================================================================================================================================
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
CREATE SCHEMA "S 1";
CREATE TABLE "S 1"."T 1" (
    "C 1" int NOT NULL,
    c2 int NOT NULL,
    c3 text,
    c4 timestamptz,
    c5 timestamp,
    c6 varchar(10),
    c7 char(10),
    c8 user_enum,
    CONSTRAINT t1_pkey PRIMARY KEY ("C 1")
);
CREATE TABLE "S 1"."T 2" (
    c1 int NOT NULL,
    c2 text,
    CONSTRAINT t2_pkey PRIMARY KEY (c1)
);
CREATE TABLE "S 1"."T 3" (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text,
    CONSTRAINT t3_pkey PRIMARY KEY (c1)
);
CREATE TABLE "S 1"."T 4" (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text,
    CONSTRAINT t4_pkey PRIMARY KEY (c1)
);

-- Disable autovacuum for these tables to avoid unexpected effects of that
ALTER TABLE "S 1"."T 1" SET (autovacuum_enabled = 'false');
ALTER TABLE "S 1"."T 2" SET (autovacuum_enabled = 'false');
ALTER TABLE "S 1"."T 3" SET (autovacuum_enabled = 'false');
ALTER TABLE "S 1"."T 4" SET (autovacuum_enabled = 'false');

INSERT INTO "S 1"."T 1"
    SELECT id,
           id % 10,
           to_char(id, 'FM00000'),
           '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
           '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
           id % 10,
           id % 10,
           'foo'::user_enum
    FROM generate_series(1, 1000) id;
INSERT INTO "S 1"."T 2"
    SELECT id,
           'AAA' || to_char(id, 'FM000')
    FROM generate_series(1, 100) id;
INSERT INTO "S 1"."T 3"
    SELECT id,
           id + 1,
           'AAA' || to_char(id, 'FM000')
    FROM generate_series(1, 100) id;
DELETE FROM "S 1"."T 3" WHERE c1 % 2 != 0;    -- delete for outer join tests
INSERT INTO "S 1"."T 4"
    SELECT id,
           id + 1,
           'AAA' || to_char(id, 'FM000')
    FROM generate_series(1, 100) id;
DELETE FROM "S 1"."T 4" WHERE c1 % 3 != 0;    -- delete for outer join tests

ANALYZE "S 1"."T 1";
ANALYZE "S 1"."T 2";
ANALYZE "S 1"."T 3";
ANALYZE "S 1"."T 4";
-- ======================================================================================================================================
-- TEST-MODULE: create foreign tables
-- --------------------------------------
-- ======================================================================================================================================
CREATE FOREIGN TABLE ft1 (
    c0 int,
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text,
    c4 timestamptz,
    c5 timestamp,
    c6 varchar(10),
    c7 char(10) default 'ft1',
    c8 user_enum
) SERVER loopback;
ALTER FOREIGN TABLE ft1 DROP COLUMN c0;

CREATE FOREIGN TABLE ft2 (
    c1 int NOT NULL,
    c2 int NOT NULL,
    cx int,
    c3 text,
    c4 timestamptz,
    c5 timestamp,
    c6 varchar(10),
    c7 char(10) default 'ft2',
    c8 user_enum
) SERVER loopback;
ALTER FOREIGN TABLE ft2 DROP COLUMN cx;

CREATE FOREIGN TABLE ft4 (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text
) SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 3');

CREATE FOREIGN TABLE ft5 (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text
) SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 4');

CREATE FOREIGN TABLE ft6 (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text
) SERVER loopback2 OPTIONS (schema_name 'S 1', table_name 'T 4');

CREATE FOREIGN TABLE ft7 (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text
) SERVER loopback3 OPTIONS (schema_name 'S 1', table_name 'T 4');
-- ======================================================================================================================================
-- TEST-MODULE: tests for validator
-- --------------------------------------
-- ======================================================================================================================================
-- requiressl and some other parameters are omitted because
-- valid values for them depend on configure options
ALTER SERVER testserver1 OPTIONS (
    use_remote_estimate 'false',
    updatable 'true',
    fdw_startup_cost '123.456',
    fdw_tuple_cost '0.123',
    service 'value',
    connect_timeout 'value',
    dbname 'value',
    host 'value',
    hostaddr 'value',
    port 'value',
    --client_encoding 'value',
    application_name 'value',
    --fallback_application_name 'value',
    keepalives 'value',
    keepalives_idle 'value',
    keepalives_interval 'value',
    --nspt tcp_user_timeout 'value',
    -- requiressl 'value',
    sslcompression 'value',
    sslmode 'value',
    sslcert 'value',
    sslkey 'value',
    sslrootcert 'value',
    sslcrl 'value',
    --requirepeer 'value',
    krbsrvname 'value'
    --nspt gsslib 'value'
    --replication 'value'
);

-- Error, invalid list syntax
ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');  --nspt

-- OK but gets a warning
ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');  --nspt
ALTER SERVER testserver1 OPTIONS (DROP extensions);            --nspt

ALTER USER MAPPING FOR public SERVER testserver1 OPTIONS (DROP user, DROP password);

-- Attempt to add a valid option that's not allowed in a user mapping
ALTER USER MAPPING FOR public SERVER testserver1           --nspt
    OPTIONS (ADD sslmode 'require');                       --nspt

-- But we can add valid ones fine
ALTER USER MAPPING FOR public SERVER testserver1           --nspt
    OPTIONS (ADD sslpassword 'dummy');                     --nspt

-- Ensure valid options we haven't used in a user mapping yet are
-- permitted to check validation.
ALTER USER MAPPING FOR public SERVER testserver1           --nspt
    OPTIONS (ADD sslkey 'value', ADD sslcert 'value');     --nspt

ALTER FOREIGN TABLE ft1 OPTIONS (schema_name 'S 1', table_name 'T 1');
ALTER FOREIGN TABLE ft2 OPTIONS (schema_name 'S 1', table_name 'T 1');
ALTER FOREIGN TABLE ft1 ALTER COLUMN c1 OPTIONS (column_name 'C 1');
ALTER FOREIGN TABLE ft2 ALTER COLUMN c1 OPTIONS (column_name 'C 1');
\det+

-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
\set VERBOSITY terse
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
ALTER SERVER loopback OPTIONS (SET dbname 'no such database');
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER loopback
            OPTIONS (SET dbname '$$||current_database()||$$')$$;
    END;
$d$;
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again

-- Test that alteration of user mapping options causes reconnection
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (ADD user 'no such user');
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (DROP user);
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
\set VERBOSITY default

-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
ANALYZE ft1;
ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');
-- ======================================================================================================================================
-- TEST-MODULE: simple queries
-- --------------------------------------
-- 
-- ======================================================================================================================================
-- single table without alias
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--nspt single table with alias - also test that tableoid sort is not pushed to remote side
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
-- with FOR UPDATE/SHARE
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- aggregate
SELECT COUNT(*) FROM ft1 t1;
-- subquery
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
SET enable_hashjoin TO false;
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
RESET enable_hashjoin;
RESET enable_nestloop;

-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
CREATE TABLE loct_empty (c1 int NOT NULL, c2 text);
CREATE FOREIGN TABLE ft_empty (c1 int NOT NULL, c2 text)
  SERVER loopback OPTIONS (table_name 'loct_empty');
INSERT INTO loct_empty
  SELECT id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;
DELETE FROM loct_empty;
ANALYZE ft_empty;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

-- test restriction on non_system foreign tables.
set restrict_nonsystem_relation_kind to 'foreign-table';
select * from ft1 where c1 < 0; --Error
insert into ft1 (c1) values(1); --Error
delete from ft1 where c1 = 1; --Error
reset restrict_nonsystem_relation_kind;

-- ======================================================================================================================================
-- TEST-MODULE: WHERE with remotely-executable conditions
-- --------------------------------------
-- ======================================================================================================================================
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T 1" a, ft2 b WHERE a."C 1" = 47 AND b.c1 = a.c2;
SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));
-- we should not push order by clause with volatile expressions or unsafe
-- collations
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT * FROM ft2 ORDER BY ft2.c1, random();
EXPLAIN (VERBOSE, COSTS OFF)
    SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function
CREATE FUNCTION postgres_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...
ALTER EXTENSION postgres_fdw ADD FUNCTION postgres_fdw_abs(int);
ALTER EXTENSION postgres_fdw ADD OPERATOR === (int, int);
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw'); --nspt

-- ... now they can be shipped
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can be shipped
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- case when expr
explain (verbose, costs off) select * from ft1 where case c1 when 1 then 0 end = 0;
select * from ft1 where case c1 when 1 then 0 end = 0;

-- ======================================================================================================================================
-- TEST-MODULE: JOIN queries
-- --------------------------------------
-- ======================================================================================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
ANALYZE ft4;
ANALYZE ft5;

-- join two tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
            WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
            WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60)  t2 ON (TRUE) OFFSET 10 LIMIT 2;
SELECT count(*) FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60)  t2 ON (TRUE) OFFSET 10 LIMIT 2;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + right outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + full outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + left outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + full outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SET enable_memoize TO off; --nspt
-- right outer join + left outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
RESET enable_memoize; --nspt
-- left outer join + right outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE postgres_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
ALTER SERVER loopback OPTIONS (DROP extensions); --nspt
-- full outer join + WHERE clause with shippable extensions not set
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE postgres_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw'); --nspt
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference, currently, does not support system column
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN can be pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
EXPLAIN (VERBOSE, COSTS OFF)       -- openGauss not support LATERAL
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
UPDATE ft5 SET c3 = null where c1 % 9 = 0;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
CREATE TABLE local_tbl (c1 int NOT NULL, c2 int NOT NULL, c3 text, CONSTRAINT local_tbl_pkey PRIMARY KEY (c1));
INSERT INTO local_tbl SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
ANALYZE local_tbl;
SET enable_nestloop TO false;
SET enable_hashjoin TO false;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
RESET enable_nestloop;
RESET enable_hashjoin;
DROP TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved
CREATE ROLE regress_view_owner sysadmin password 'QWERT@12345';
CREATE USER MAPPING FOR regress_view_owner SERVER loopback;
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

CREATE VIEW v4 AS SELECT * FROM ft4;
CREATE VIEW v5 AS SELECT * FROM ft5;
ALTER VIEW v5 OWNER TO regress_view_owner;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO regress_view_owner;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO regress_view_owner;

-- PlaceHolder
explain(verbose, costs off) select * from ft1 t1 left join (select c1, case when c2 is not null then 1 else 0 end as cc from ft2) t2 on t1.c1 = t2.c1;

-- cleanup
DROP OWNED BY regress_view_owner;
DROP ROLE regress_view_owner;

-- ======================================================================================================================================
-- TEST-MODULE: Aggregate and grouping queries
-- --------------------------------------
-- openGauss not support filter
-- ======================================================================================================================================

-- Simple aggregates
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
select exists(select 1 from pg_enum), sum(c1) from ft1;

explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--nspt FILTER within aggregate
-- openGauss not support FILTER within aggregate, leve a err case, make others run as normal
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
explain (verbose, costs off)
select sum(c1)/* filter (where c1 < 100 and c2 > 5)*/ from ft1 group by c2 order by 1 nulls last;
select sum(c1)/* filter (where c1 < 100 and c2 > 5)*/ from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3)/* filter (where c1%3 < 2)*/, c2 from ft1 where c2 = 6 group by c2;
select sum(c1%3), sum(distinct c1%3 order by c1%3)/* filter (where c1%3 < 2)*/, c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
explain (verbose, costs off)
select distinct (select count(*) /*filter (where t2.c2 = 6 and t2.c1 < 10)*/ from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
select distinct (select count(*) /*filter (where t2.c2 = 6 and t2.c1 < 10)*/ from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
explain (verbose, costs off)
select distinct (select count(t1.c1) /*filter (where t2.c2 = 6 and t2.c1 < 10)*/ from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
select distinct (select count(t1.c1) /*filter (where t2.c2 = 6 and t2.c1 < 10)*/ from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
explain (verbose, costs off) select sum(c1) /*filter (where (c1 / c1) * random() <= 1)*/ from ft1 group by c2 order by 1;
explain (verbose, costs off) select sum(c2) /*filter (where c2 in (select c2 from ft1 where c2 < 5))*/ from ft1;

--nspt Ordered-sets within aggregate
--nspt Using multiple arguments within aggregates

-- User defined function for user defined aggregate, VARIADIC
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
alter extension postgres_fdw add function least_accum(anyelement, variadic anyarray);
alter extension postgres_fdw add aggregate least_agg(variadic items anyarray);
alter server loopback options (set extensions 'postgres_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
alter extension postgres_fdw drop function least_accum(anyelement, variadic anyarray);
alter extension postgres_fdw drop aggregate least_agg(variadic items anyarray);
alter server loopback options (set extensions 'postgres_fdw');

-- Not pushed down as we have dropped objects from extension.
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
reset enable_hashagg;
drop aggregate least_agg(variadic items anyarray);
drop function least_accum(anyelement, variadic anyarray);


--nspt Testing USING OPERATOR() in ORDER BY within aggregate.
--nspt For this, we need user defined operators along with operator family and
--nspt operator class.  Create those and then add them in extension.  Note that
--nspt user defined objects are considered unshippable unless they are part of
--nspt the extension.
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

create operator family my_op_family using btree;

create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- NOTICES: 
-- openGauss not support create operator class.  remove the case. If you want to restore the test case later, refer to README in the file header.

-- This will not be pushed as sort operator is now removed from the extension.
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
drop operator class my_op_class using btree;
drop function my_op_cmp(a int, b int);
drop operator family my_op_family using btree;
drop operator public.>^(int, int);
drop operator public.=^(int, int);
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

--nspt LATERAL join, with parameterization
-- NOTICES: 
-- openGauss not support create operator class.  remove the case. If you want to restore the test case later, refer to README in the file header.

-- Check with placeHolderVars
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);


-- Not supported cases
-- Grouping sets
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;

-- Same group by
explain verbose select c1,c1,c1,count(*) from ft1 group by 1,1,1;
set enable_hashagg = off;
explain verbose select c1,c1,c1,count(*) from ft1 group by 1,1,1;
set enable_hashagg = on;

-- ======================================================================================================================================
-- TEST-MODULE: parameterized queries
-- --------------------------------------
-- ======================================================================================================================================
-- simple join
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
EXECUTE st1(1, 1);
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND "date"(c4) = '1970-01-17'::date) ORDER BY c1;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
EXECUTE st2(10, 20);
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND "date"(c5) = '1970-01-17'::date) ORDER BY c1;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
EXECUTE st3(10, 20);
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
PREPARE st5(user_enum,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
PREPARE st7 AS INSERT INTO ft1 (c1,c2,c3) VALUES (1001,101,'foo');
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
ALTER TABLE "S 1"."T 1" RENAME TO "T 0";
ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T 0');
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
EXECUTE st6;   --nspt
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
ALTER TABLE "S 1"."T 0" RENAME TO "T 1";
ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T 1');

PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
ALTER SERVER loopback OPTIONS (DROP extensions);
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
EXECUTE st8;
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- cleanup
DEALLOCATE st1;
DEALLOCATE st2;
DEALLOCATE st3;
DEALLOCATE st4;
DEALLOCATE st5;
DEALLOCATE st6;
DEALLOCATE st7;
DEALLOCATE st8;

-- System columns, except ctid and oid, should not be sent to remote
-- openGauss NOT SUPPROT select system columns in ftable.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1'::regclass LIMIT 1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
SELECT ctid, * FROM ft1 t1 LIMIT 1;
-- ======================================================================================================================================
-- TEST-MODULE: used in PL/pgSQL function
-- --------------------------------------
-- ======================================================================================================================================

CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
    v_c1 int;
BEGIN
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
SELECT f_test(100);
DROP FUNCTION f_test(int);
-- ======================================================================================================================================
-- TEST-MODULE: REINDEX
-- --------------------------------------
-- ======================================================================================================================================
-- remote table is not created here
CREATE FOREIGN TABLE reindex_foreign (c1 int, c2 int)
  SERVER loopback2 OPTIONS (table_name 'reindex_local');
REINDEX TABLE reindex_foreign; -- error
REINDEX TABLE CONCURRENTLY reindex_foreign; -- error --nspt
DROP FOREIGN TABLE reindex_foreign;
-- partitions and foreign tables
CREATE TABLE reind_fdw_parent (c1 int) PARTITION BY RANGE (c1) (
    partition reind_fdw_0_10 values less than(11),
    partition reind_fdw_10_20 values less than(21)
);
CREATE FOREIGN TABLE reind_fdw_10_20_fp(c1 int)
  SERVER loopback OPTIONS (table_name 'reind_fdw_parent');
CREATE FOREIGN TABLE reind_fdw_10_20_f1(c1 int)
  SERVER loopback OPTIONS (table_name 'reind_fdw_parent partition(reind_fdw_10_20)');
REINDEX TABLE reind_fdw_10_20_fp; -- error
REINDEX TABLE reind_fdw_10_20_f1; -- error
REINDEX TABLE reind_fdw_parent; -- ok
REINDEX TABLE CONCURRENTLY reind_fdw_parent; -- ok --nspt
DROP TABLE reind_fdw_parent;
DROP FOREIGN TABLE reind_fdw_10_20_fp;
DROP FOREIGN TABLE reind_fdw_10_20_f1;

-- ======================================================================================================================================
-- TEST-MODULE: conversion error
-- --------------------------------------
-- ======================================================================================================================================
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE int;
SELECT * FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8) WHERE x1 = 1;  -- ERROR
SELECT ftx.x1, ft2.c2, ftx.x8 FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
SELECT ftx.x1, ft2.c2, ftx FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
ANALYZE ft1; -- ERROR
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE user_enum;
-- ======================================================================================================================================
-- TEST-MODULE: subtransaction
--                 + local/remote error doesn't break cursor
-- --------------------------------------
-- ======================================================================================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
FETCH c;
SAVEPOINT s;
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
FETCH c;
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;
-- ======================================================================================================================================
-- TEST-MODULE: test handling of collations
-- --------------------------------------
-- ======================================================================================================================================
create table loct3 (f1 text collate "C" unique, f2 text, f3 varchar(10) unique);
create foreign table ft3 (f1 text collate "C", f2 text, f3 varchar(10))
  server loopback options (table_name 'loct3', use_remote_estimate 'true');

-- can be sent to remote
explain (verbose, costs off) select * from ft3 where f1 = 'foo';
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
explain (verbose, costs off) select * from ft3 where f2 = 'foo';
explain (verbose, costs off) select * from ft3 where f3 = 'foo';
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';
-- ======================================================================================================================================
-- TEST-MODULE: test writable foreign table stuff
-- --------------------------------------
-- ======================================================================================================================================
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
INSERT INTO ft2 (c1,c2,c3)
  VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc') RETURNING *;
INSERT INTO ft2 (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7 RETURNING *;  -- can be pushed down
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7 RETURNING *;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;
EXPLAIN (verbose, costs off)
  DELETE FROM ft2 WHERE c1 % 10 = 5 RETURNING c1, c4;                               -- can be pushed down
DELETE FROM ft2 WHERE c1 % 10 = 5 RETURNING c1, c4;
EXPLAIN (verbose, costs off)
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;

--nspt openGauss not have tableoid, also not support select system columns.
EXPLAIN (verbose, costs off)
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo') RETURNING tableoid::regclass;
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo') RETURNING tableoid::regclass;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200 RETURNING tableoid::regclass;             -- can be pushed down
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200 RETURNING tableoid::regclass;
EXPLAIN (verbose, costs off)
DELETE FROM ft2 WHERE c1 = 1200 RETURNING tableoid::regclass;                       -- can be pushed down
DELETE FROM ft2 WHERE c1 = 1200 RETURNING tableoid::regclass;

-- Test UPDATE/DELETE with RETURNING on a three-table join
INSERT INTO ft2 (c1,c2,c3)
  SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1
  RETURNING ft2, ft2.*, ft4, ft4.*;       -- can be pushed down
UPDATE ft2 SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1
  RETURNING ft2, ft2.*, ft4, ft4.*;
EXPLAIN (verbose, costs off)
DELETE FROM ft2
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1
  RETURNING 100;                          -- can be pushed down
DELETE FROM ft2
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1
  RETURNING 100;
DELETE FROM ft2 WHERE ft2.c1 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)
EXPLAIN (verbose, costs off)
UPDATE ft2 AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;
UPDATE ft2 AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

UPDATE ft2 AS target SET (c2) = (
    SELECT c2 / 10
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

-- Test UPDATE involving a join that can be pushed down,
-- but a SET clause that can't be
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE ft2 d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;
UPDATE ft2 d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
ALTER SERVER loopback OPTIONS (DROP extensions);
INSERT INTO ft2 (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'bar' WHERE postgres_fdw_abs(c1) > 2000 RETURNING *;            -- can't be pushed down
UPDATE ft2 SET c3 = 'bar' WHERE postgres_fdw_abs(c1) > 2000 RETURNING *;
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1
  RETURNING ft2.*, ft4.*, ft5.*;                                                    -- can't be pushed down
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1
  RETURNING ft2.*, ft4.*, ft5.*;
EXPLAIN (verbose, costs off)
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1
  RETURNING ft2.c1, ft2.c2, ft2.c3;       -- can't be pushed down
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1
  RETURNING ft2.c1, ft2.c2, ft2.c3;
DELETE FROM ft2 WHERE ft2.c1 > 2000;
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- Test that trigger on remote table works as expected
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON "S 1"."T 1" FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

INSERT INTO ft2 (c1,c2,c3) VALUES (1208, 818, 'fff') RETURNING *;
INSERT INTO ft2 (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;') RETURNING *;
UPDATE ft2 SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200 RETURNING *;

-- Test errors thrown on remote side during update
ALTER TABLE "S 1"."T 1" ADD CONSTRAINT c2positive CHECK (c2 >= 0);

INSERT INTO ft1(c1, c2) VALUES(11, 12);  -- duplicate key
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON DUPLICATE KEY UPDATE NOTHING; -- works
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON DUPLICATE KEY UPDATE c3 = 'ffg'; -- unsupported
INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive

-- Test savepoint/rollback behavior
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
begin;
update ft2 set c2 = 42 where c2 = 0;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s1;
update ft2 set c2 = 44 where c2 = 4;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s1;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s2;
update ft2 set c2 = 46 where c2 = 6;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
rollback to savepoint s2;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s2;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s3;
update ft2 set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
rollback to savepoint s3;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s3;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- none of the above is committed yet remotely
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
commit;
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;

VACUUM ANALYZE "S 1"."T 1";

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

-- ======================================================================================================================================
-- TEST-MODULE: check constraints
-- --------------------------------------
--     openGauss not support to "ALTER FOREIGN TABLE ft1 ADD CONSTRAINT", so this test module
--     is unuseful.
-- ======================================================================================================================================
-- Consistent check constraints provide consistent results
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK (c2 >= 0);
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
SELECT count(*) FROM ft1 WHERE c2 < 0;
SET constraint_exclusion = 'on';
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
SELECT count(*) FROM ft1 WHERE c2 < 0;
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally
INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2positive;

-- But inconsistent check constraints provide inconsistent results
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK (c2 < 0);
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
SELECT count(*) FROM ft1 WHERE c2 >= 0;
SET constraint_exclusion = 'on';
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
SELECT count(*) FROM ft1 WHERE c2 >= 0;
RESET constraint_exclusion;
-- local check constraint is not actually enforced
INSERT INTO ft1(c1, c2) VALUES(1111, 2);
UPDATE ft1 SET c2 = c2 + 1 WHERE c1 = 1;
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2negative;

-- ======================================================================================================================================
-- TEST-MODULE: IUD VIEW and WITH CHECK OPTION constraints
-- --------------------------------------
-- ======================================================================================================================================
CREATE TABLE base_tbl (id int, a int, b int);
ALTER TABLE base_tbl SET (autovacuum_enabled = 'false');
CREATE FOREIGN TABLE foreign_tbl (id int, a int, b int) SERVER loopback OPTIONS (table_name 'base_tbl');
CREATE VIEW rw_view1 AS SELECT * FROM foreign_tbl WHERE a < b and id = 1;
CREATE VIEW rw_view2 AS SELECT * FROM foreign_tbl WHERE a < b and id = 2 WITH CHECK OPTION;
\d+ rw_view1
\d+ rw_view2
CREATE OR REPLACE FUNCTION checkdata(out tbname text, out id int, out a int, out b int) RETURNS SETOF record as $$ 
  select * from (
     (select 'rw_view1', * from rw_view1 where id = 1) union all 
     (select 'rw_view2', * from rw_view2 where id = 2) union all 
     (select 'base_tbl', * from base_tbl) union all 
     (select 'foreign_tbl', * from foreign_tbl)
  ) data(tbname, id, a, b)
  order by 1,2,3,4;
$$ language sql;

-- simple case
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view1 VALUES (1, 0, 5);  -- should success
INSERT INTO rw_view1 VALUES (1, 0, 5); 
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view1 VALUES (1, 10, 5); -- should success
INSERT INTO rw_view1 VALUES (1, 10, 5);
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view2 VALUES (2, 0, 5);  -- should success
INSERT INTO rw_view2 VALUES (2, 0, 5);
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view2 VALUES (2, 10, 5); -- should failed
INSERT INTO rw_view2 VALUES (2, 10, 5);
select * from checkdata();

EXPLAIN (VERBOSE, COSTS OFF) UPDATE rw_view1 SET a = a + 20 where id = 1;  -- should success
UPDATE rw_view1 SET a = a + 20 where id = 1;
EXPLAIN (VERBOSE, COSTS OFF) UPDATE rw_view2 SET a = a + 20 where id = 2;  -- should failed
UPDATE rw_view2 SET a = a + 20 where id = 2;
select * from checkdata();

insert into base_tbl values(1,100,99),(2,100,99);
EXPLAIN (VERBOSE, COSTS OFF) delete from rw_view1 where id = 1;
delete from rw_view1 where id = 1;
EXPLAIN (VERBOSE, COSTS OFF) delete from rw_view2 where id = 2;
delete from rw_view2 where id = 2;
select * from checkdata();

delete from base_tbl;

-- with trigger
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON base_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view1 VALUES (1, 0, 5);  -- should success
INSERT INTO rw_view1 VALUES (1, 0, 5);
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view1 VALUES (1, 0, 16); -- should success
INSERT INTO rw_view1 VALUES (1, 0, 16);
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view2 VALUES (2, 0, 5);  -- should failed
INSERT INTO rw_view2 VALUES (2, 0, 5);
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO rw_view2 VALUES (2, 0, 16); -- should success
INSERT INTO rw_view2 VALUES (2, 0, 16);
select * from checkdata();


EXPLAIN (VERBOSE, COSTS OFF) UPDATE rw_view1 SET a = a - 1 where id = 1;
UPDATE rw_view1 SET a = a - 1 where id = 1;
EXPLAIN (VERBOSE, COSTS OFF) UPDATE rw_view1 SET a = a + 1 where id = 1;
UPDATE rw_view1 SET a = a + 1 where id = 1;
EXPLAIN (VERBOSE, COSTS OFF) UPDATE rw_view2 SET a = a - 10 where id = 2;
UPDATE rw_view2 SET a = a - 10 where id = 2;
EXPLAIN (VERBOSE, COSTS OFF) UPDATE rw_view2 SET a = a + 10 where id = 2;
UPDATE rw_view2 SET a = a + 10 where id = 2;
select * from checkdata();


DROP TRIGGER row_before_insupd_trigger ON base_tbl;
DROP FUNCTION row_before_insupd_trigfunc;
DROP FUNCTION checkdata;
DROP VIEW rw_view1 cascade;
DROP VIEW rw_view2 cascade;
DROP FOREIGN TABLE foreign_tbl CASCADE;
DROP TABLE base_tbl CASCADE;


-- ======================================================================================================================================
-- TEST-MODULE: test serial columns (ie, sequence-based defaults)
-- --------------------------------------
-- ======================================================================================================================================
create table loc1 (f1 serial, f2 text);
alter table loc1 set (autovacuum_enabled = 'false');
create foreign table rem1 (f1 serial, f2 text)
  server loopback options(table_name 'loc1');
select pg_catalog.setval('rem1_f1_seq', 10, false);
insert into loc1(f2) values('hi');
insert into rem1(f2) values('hi remote');
insert into loc1(f2) values('bye');
insert into rem1(f2) values('bye remote');
select * from loc1;
select * from rem1;

-- ======================================================================================================================================
-- TEST-MODULE: test generated columns
-- --------------------------------------
-- CURRENTLY NOT SUPPORT
-- ======================================================================================================================================
create table gloc1 (
  a int,
  b int generated always as (a * 2) stored);
alter table gloc1 set (autovacuum_enabled = 'false');
create foreign table grem1 (
  a int,
  b int generated always as (a * 2) stored)
  server loopback options(table_name 'gloc1');
explain (verbose, costs off)
insert into grem1 (a) values (1), (2);  --nspt
insert into grem1 (a) values (1), (2);  --nspt
explain (verbose, costs off)
update grem1 set a = 22 where a = 2;
update grem1 set a = 22 where a = 2;
select * from gloc1;
select * from grem1;
delete from grem1;

-- test copy from
copy grem1 from stdin;
1
2
\.
select * from gloc1;
select * from grem1;
delete from grem1;

-- test batch insert
alter server loopback options (add batch_size '10');
explain (verbose, costs off)
insert into grem1 (a) values (1), (2);
insert into grem1 (a) values (1), (2);
select * from gloc1;
select * from grem1;
delete from grem1;
alter server loopback options (drop batch_size);

-- ======================================================================================================================================
-- TEST-MODULE: test local triggers
-- --------------------------------------
--     openGauss not support create trigger on foreign table, so this module is unuseful.
--     we adapt the test to create trigger on base table of foreign table.
-- ======================================================================================================================================
create table tglog(id serial, context text);
create table previd(a int);
insert into previd values(0);

create or replace function showtrigger(id out int, context out text) returns setof record LANGUAGE plpgsql as
$$
DECLARE
    r RECORD;
    prev int;
BEGIN
    select max(a) from previd into prev;
    update previd set a = (select max(id) from tglog);

    FOR r IN SELECT * FROM tglog where id > prev ORDER BY id
    LOOP
        id := r.id;
        context := r.context;
        return next;
    END LOOP;
END;$$;
    


-- Trigger functions "borrowed" from triggers regress test.
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    insert into tglog(context) values(
        format('trigger_func(%s) called: action = %s, when = %s, level = %s',
               TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL));
    RETURN NULL;
END;$$;

-- error, openGauss not support create trigger on foreign table
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1
    FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1
    FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

-- success
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON loc1
    FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON loc1
    FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

CREATE OR REPLACE FUNCTION trigger_data()  RETURNS trigger
LANGUAGE plpgsql AS $$

declare
    oldnew text[];
    relid text;
    argstr text;
begin

    relid := TG_relid::regclass;
    argstr := '';
    for i in 0 .. TG_nargs - 1 loop
        if i > 0 then
            argstr := argstr || ', ';
        end if;
        argstr := argstr || TG_argv[i];
    end loop;

    insert into tglog(context) values(
        format('%s(%s) %s %s %s ON %s',
               tg_name, argstr, TG_when, TG_level, TG_OP, relid));
    oldnew := '{}'::text[];
    if TG_OP != 'INSERT' then
        oldnew := array_append(oldnew, format('OLD: %s', OLD));
    end if;

    if TG_OP != 'DELETE' then
        oldnew := array_append(oldnew, format('NEW: %s', NEW));
    end if;

    insert into tglog(context) values(
        format('%s',
               array_to_string(oldnew, ',')));

    if TG_OP = 'DELETE' then
        return OLD;
    else
        return NEW;
    end if;
end;
$$;

-- Test basic functionality
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

delete from rem1;
select * from showtrigger();
insert into rem1 values(1,'insert');
select * from showtrigger();
update rem1 set f2  = 'update' where f1 = 1;
select * from showtrigger();
update rem1 set f2 = f2 || f2;
select * from showtrigger();

-- cleanup
DROP TRIGGER trig_row_before ON loc1;
DROP TRIGGER trig_row_after ON loc1;
DROP TRIGGER trig_stmt_before ON loc1;
DROP TRIGGER trig_stmt_after ON loc1;

DELETE from rem1;
select * from showtrigger();

-- Test multiple AFTER ROW triggers on a foreign table
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

insert into rem1 values(1,'insert');
select * from showtrigger();
update rem1 set f2  = 'update' where f1 = 1;
select * from showtrigger();
update rem1 set f2 = f2 || f2;
select * from showtrigger();
delete from rem1;
select * from showtrigger();

-- cleanup
DROP TRIGGER trig_row_after1 ON loc1;
DROP TRIGGER trig_row_after2 ON loc1;

-- Test WHEN conditions

CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON loc1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON loc1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
INSERT INTO rem1 values(1, 'insert');
select * from showtrigger();
UPDATE rem1 set f2 = 'test';
select * from showtrigger();

-- Insert or update matching: triggers are fired
INSERT INTO rem1 values(2, 'update');
select * from showtrigger();
UPDATE rem1 set f2 = 'update update' where f1 = '2';
select * from showtrigger();

CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON loc1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON loc1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
DELETE FROM rem1;
select * from showtrigger();

-- cleanup
DROP TRIGGER trig_row_before_insupd ON loc1;
DROP TRIGGER trig_row_after_insupd ON loc1;
DROP TRIGGER trig_row_before_delete ON loc1;
DROP TRIGGER trig_row_after_delete ON loc1;


-- Test various RETURN statements in BEFORE triggers.

CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
INSERT INTO rem1 values(1, 'insert');
select * from showtrigger();
SELECT * from loc1;
INSERT INTO rem1 values(2, 'insert') RETURNING f2;
select * from showtrigger();
SELECT * from loc1;
UPDATE rem1 set f2 = '';
select * from showtrigger();
SELECT * from loc1;
UPDATE rem1 set f2 = 'skidoo' RETURNING f2;
select * from showtrigger();
SELECT * from loc1;

EXPLAIN (verbose, costs off)
UPDATE rem1 set f1 = 10;          -- all columns should be transmitted
select * from showtrigger();
UPDATE rem1 set f1 = 10;
select * from showtrigger();
SELECT * from loc1;

DELETE FROM rem1;
select * from showtrigger();

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

INSERT INTO rem1 values(1, 'insert');
select * from showtrigger();
SELECT * from loc1;
INSERT INTO rem1 values(2, 'insert') RETURNING f2;
select * from showtrigger();
SELECT * from loc1;
UPDATE rem1 set f2 = '';
select * from showtrigger();
SELECT * from loc1;
UPDATE rem1 set f2 = 'skidoo' RETURNING f2;
select * from showtrigger();
SELECT * from loc1;

DROP TRIGGER trig_row_before_insupd ON loc1;
DROP TRIGGER trig_row_before_insupd2 ON loc1;

DELETE from rem1;
select * from showtrigger();
INSERT INTO rem1 VALUES (1, 'test');
select * from showtrigger();
-- Test with a trigger returning NULL
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
INSERT INTO rem1 VALUES (2, 'test2');
select * from showtrigger();
SELECT * from loc1;

UPDATE rem1 SET f2 = 'test2';
select * from showtrigger();
SELECT * from loc1;

DELETE from rem1;
select * from showtrigger();
SELECT * from loc1;

DROP TRIGGER trig_null ON loc1;
DELETE from rem1;
select * from showtrigger();
-- Test a combination of local and remote triggers
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

CREATE TRIGGER trig_local_before BEFORE INSERT OR UPDATE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

INSERT INTO rem1(f2) VALUES ('test');
select * from showtrigger();
UPDATE rem1 SET f2 = 'testo';
select * from showtrigger();

-- Test returning a system attribute
INSERT INTO rem1(f2) VALUES ('test') RETURNING ctid;
select * from showtrigger();
-- cleanup
DROP TRIGGER trig_row_before ON loc1;
DROP TRIGGER trig_row_after ON loc1;
DROP TRIGGER trig_local_before ON loc1;


-- Test direct foreign table modification functionality
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1 WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers
CREATE TRIGGER trig_stmt_before
    BEFORE DELETE OR INSERT OR UPDATE ON loc1
    FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_stmt_before ON loc1;

CREATE TRIGGER trig_stmt_after
    AFTER DELETE OR INSERT OR UPDATE ON loc1
    FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_stmt_after ON loc1;

-- Test with row-level ON INSERT triggers
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_before_insert ON loc1;

CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_after_insert ON loc1;

-- Test with row-level ON UPDATE triggers
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_before_update ON loc1;

CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
DROP TRIGGER trig_row_after_update ON loc1;

-- Test with row-level ON DELETE triggers
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
DROP TRIGGER trig_row_before_delete ON loc1;

CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON loc1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can be pushed down
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
DROP TRIGGER trig_row_after_delete ON loc1;

-- ======================================================================================================================================
-- TEST-MODULE: test inheritance features
-- --------------------------------------
--     openGauss not support this feature. and cannot fix, remove all test case.
-- ======================================================================================================================================

-- ======================================================================================================================================
-- TEST-MODULE: test tuple routing for foreign-table partitions
-- --------------------------------------
--   partition table is different between openGauss and pg, and cannot fix these test cases, remove all.
-- ======================================================================================================================================


-- ======================================================================================================================================
-- TEST-MODULE: test COPY FROM
-- --------------------------------------
-- ======================================================================================================================================
create table loc2 (f1 int, f2 text);
alter table loc2 set (autovacuum_enabled = 'false');
create foreign table rem2 (f1 int, f2 text) server loopback options(table_name 'loc2');

-- Test basic functionality
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

delete from rem2;

-- Test check constraints
alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
--nspt alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally
copy rem2 from stdin;
1	foo
2	bar
\.
copy rem2 from stdin; -- ERROR
-1	xyzzy
\.
select * from rem2;

--nspt alter foreign table rem2 drop constraint rem2_f1positive;
alter table loc2 drop constraint loc2_f1positive;

delete from rem2;

-- Test local triggers
create trigger trig_stmt_before before insert on loc2
    for each statement execute procedure trigger_func();
create trigger trig_stmt_after after insert on loc2
    for each statement execute procedure trigger_func();
create trigger trig_row_before before insert on loc2
    for each row execute procedure trigger_data(23,'skidoo');
create trigger trig_row_after after insert on loc2
    for each row execute procedure trigger_data(23,'skidoo');

copy rem2 from stdin;
1	foo
2	bar
\.
select * from showtrigger();
select * from rem2;

drop trigger trig_row_before on loc2;
drop trigger trig_row_after on loc2;
drop trigger trig_stmt_before on loc2;
drop trigger trig_stmt_after on loc2;

delete from rem2;
select * from showtrigger();

create trigger trig_row_before_insert before insert on loc2
    for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2 from stdin;
1	foo
2	bar
\.
select * from showtrigger();
select * from rem2;

drop trigger trig_row_before_insert on loc2;

delete from rem2;

create trigger trig_null before insert on loc2
    for each row execute procedure trig_null();

-- Nothing happens
copy rem2 from stdin;
1	foo
2	bar
\.
select * from showtrigger();
select * from rem2;

drop trigger trig_null on loc2;

delete from rem2;

-- Test remote triggers
create trigger trig_row_before_insert before insert on loc2
    for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2 from stdin;
1	foo
2	bar
\.
select * from showtrigger();
select * from rem2;

drop trigger trig_row_before_insert on loc2;

delete from rem2;

create trigger trig_null before insert on loc2
    for each row execute procedure trig_null();

-- Nothing happens
copy rem2 from stdin;
1	foo
2	bar
\.
select * from showtrigger();
select * from rem2;

drop trigger trig_null on loc2;

delete from rem2;

-- Test a combination of local and remote triggers
create trigger rem2_trig_row_before before insert on loc2
    for each row execute procedure trigger_data(23,'skidoo');
create trigger rem2_trig_row_after after insert on loc2
    for each row execute procedure trigger_data(23,'skidoo');
create trigger loc2_trig_row_before_insert before insert on loc2
    for each row execute procedure trig_row_before_insupdate();

copy rem2 from stdin;
1	foo
2	bar
\.
select * from showtrigger();
select * from rem2;

drop trigger rem2_trig_row_before on loc2;
drop trigger rem2_trig_row_after on loc2;
drop trigger loc2_trig_row_before_insert on loc2;

delete from rem2;

-- test COPY FROM with foreign table created in the same transaction
create table loc3 (f1 int, f2 text);
begin;
create foreign table rem3 (f1 int, f2 text)
    server loopback options(table_name 'loc3');
copy rem3 from stdin;
1	foo
2	bar
\.
select * from showtrigger();
commit;
select * from rem3;
drop foreign table rem3;
drop table loc3;

-- ======================================================================================================================================
-- TEST-MODULE: test for TRUNCATE
-- --------------------------------------
--    openGauss not support this feature. However, some cases are still left for maintenance.
-- ======================================================================================================================================
CREATE TABLE tru_rtable0 (id int primary key);
CREATE FOREIGN TABLE tru_ftable (id int)
       SERVER loopback OPTIONS (table_name 'tru_rtable0');
INSERT INTO tru_rtable0 (SELECT x FROM generate_series(1,10) x);

CREATE TABLE tru_ptable (id int) PARTITION BY HASH(id)(
    partition tru_ptable__p0,
    partition tru_ptable__p1
);
CREATE FOREIGN TABLE tru_p_ftable (id int)
       SERVER loopback OPTIONS (table_name 'tru_ptable');
INSERT INTO tru_ptable (SELECT x FROM generate_series(11,20) x);

CREATE TABLE tru_pk_table(id int primary key);
CREATE TABLE tru_fk_table(fkey int references tru_pk_table(id));
INSERT INTO tru_pk_table (SELECT x FROM generate_series(1,10) x);
INSERT INTO tru_fk_table (SELECT x % 10 + 1 FROM generate_series(5,25) x);
CREATE FOREIGN TABLE tru_pk_ftable (id int)
       SERVER loopback OPTIONS (table_name 'tru_pk_table');

-- normal truncate
SELECT sum(id) FROM tru_ftable;         -- 55
TRUNCATE tru_ftable;                    -- nspt
SELECT count(*) FROM tru_rtable0;        -- 10
SELECT count(*) FROM tru_ftable;        -- 10


-- partitioned table with both local and foreign tables as partitions
SELECT sum(id) FROM tru_p_ftable;        -- 155
TRUNCATE tru_p_ftable;
SELECT count(*) FROM tru_p_ftable;        -- 0
SELECT count(*) FROM tru_p_ftable partition(tru_ptable__p0);    -- 0
SELECT count(*) FROM tru_ptable   partition(tru_ftable__p1);    -- 0
SELECT count(*) FROM tru_ptable;        -- 0

-- 'CASCADE' option
SELECT sum(id) FROM tru_pk_ftable;      -- 55
TRUNCATE tru_pk_ftable;                    -- nspt
TRUNCATE tru_pk_ftable CASCADE;
SELECT count(*) FROM tru_pk_ftable;     -- 10
SELECT count(*) FROM tru_fk_table;        -- 21

-- truncate two tables at a command
INSERT INTO tru_ftable (SELECT x FROM generate_series(1,8) x);
INSERT INTO tru_pk_ftable (SELECT x FROM generate_series(3,10) x);
SELECT count(*) from tru_ftable; -- 8
SELECT count(*) from tru_pk_ftable; -- 8
TRUNCATE tru_ftable, tru_pk_ftable CASCADE;
SELECT count(*) from tru_ftable; -- 0
SELECT count(*) from tru_pk_ftable; -- 0

-- cleanup
DROP FOREIGN TABLE tru_pk_ftable,tru_ftable__p1,tru_ftable;
DROP TABLE tru_rtable0, tru_ptable, tru_pk_table, tru_fk_table;

-- ======================================================================================================================================
-- TEST-MODULE: test IMPORT FOREIGN SCHEMA
-- --------------------------------------
--     openGauss not support this feature. However, some cases are still left for maintenance.
-- ======================================================================================================================================
CREATE SCHEMA import_source;
CREATE TABLE import_source.t1 (c1 int, c2 varchar NOT NULL);

CREATE SCHEMA import_dest1;
IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest1;
\det+ import_dest1.*
\d import_dest1.*

-- ======================================================================================================================================
-- TEST-MODULE: test partitionwise joins
-- TEST-MODULE: test partitionwise aggregates
-- --------------------------------------
--     partition table is different between openGauss and pg, and cannot fix these test cases, remove all.
-- ======================================================================================================================================
-- nspt

-- ======================================================================================================================================
-- TEST-MODULE: access rights and superuser
-- --------------------------------------
--     some privileges is different between og and pg 
-- ======================================================================================================================================
-- Non-superuser cannot create a FDW without a password in the connstr
CREATE ROLE regress_nosuper with password 'qwer@1234';

GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO regress_nosuper;
grant all on schema public to regress_nosuper;

SET SESSION AUTHORIZATION regress_nosuper PASSWORD 'qwer@1234';

SHOW is_superuser;  --nspt

-- This will be OK, we can create the FDW
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback_nopw FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

-- But creation of user mappings for non-superusers should fail
CREATE USER MAPPING FOR public SERVER loopback_nopw;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

CREATE FOREIGN TABLE ft1_nopw (
    c1 int NOT NULL,
    c2 int NOT NULL,
    c3 text,
    c4 timestamptz,
    c5 timestamp,
    c6 varchar(10),
    c7 char(10) default 'ft1',
    c8 text
) SERVER loopback_nopw OPTIONS (schema_name 'public', table_name 'ft1');

SELECT 1 FROM ft1_nopw LIMIT 1;

-- If we add a password to the connstr it'll fail, because we don't allow passwords
-- in connstrs only in user mappings.

DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER loopback_nopw OPTIONS (ADD password  'qwer@1234')$$;
    END;
$d$;

-- If we add a password for our user mapping instead, we should get a different
-- error because the password wasn't actually *used* when we run with trust auth.
--
-- This won't work with installcheck, but neither will most of the FDW checks.

ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password 'qwer@1234');

SELECT 1 FROM ft1_nopw LIMIT 1;

-- Unpriv user cannot make the mapping passwordless
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password_required 'false');


SELECT 1 FROM ft1_nopw LIMIT 1;

RESET ROLE;

-- But the superuser can
ALTER USER MAPPING FOR regress_nosuper SERVER loopback_nopw OPTIONS (ADD password_required 'false');

SET ROLE regress_nosuper;

-- Should finally work now
SELECT 1 FROM ft1_nopw LIMIT 1;

-- unpriv user also cannot set sslcert / sslkey on the user mapping
-- first set password_required so we see the right error messages
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (SET password_required 'true');
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslcert 'foo.crt');
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslkey 'foo.key');

-- We're done with the role named after a specific user and need to check the
-- changes to the public mapping.
DROP USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- This will fail again as it'll resolve the user mapping for public, which
-- lacks password_required=false
SELECT 1 FROM ft1_nopw LIMIT 1;

\c

-- The user mapping for public is passwordless and lacks the password_required=false
-- mapping option, but will work because the current user is a superuser.
SELECT 1 FROM ft1_nopw LIMIT 1;

-- cleanup
DROP USER MAPPING FOR public SERVER loopback_nopw;
DROP OWNED BY regress_nosuper;
DROP ROLE regress_nosuper;

-- Clean-up
RESET enable_partitionwise_aggregate;

-- Two-phase transactions are not supported.
BEGIN;
SELECT count(*) FROM ft1;
-- error here
PREPARE TRANSACTION 'fdw_tpc';
ROLLBACK;

-- ======================================================================================================================================
-- TEST-MODULE: reestablish new connection
-- TEST-MODULE: test connection invalidation cases and postgres_fdw_get_connections function
-- TEST-MODULE: test postgres_fdw_disconnect and postgres_fdw_disconnect_all functions
-- TEST-MODULE: test case for having multiple cached connections for a foreign server
-- TEST-MODULE: Test foreign server level option keep_connections
-- --------------------------------------
--     openGauss not have the functions following:
--         postgres_fdw_abs
--         postgres_fdw_disconnect
--         postgres_fdw_disconnect_all
--         postgres_fdw_get_connections
--     Therefore, these test modules are temporarily unavailable.
-- ======================================================================================================================================
\df postgres_fdw_abs
\df postgres_fdw_disconnect
\df postgres_fdw_disconnect_all
\df postgres_fdw_get_connections
\df postgres_fdw_handler
\df postgres_fdw_validator

-- ======================================================================================================================================
-- TEST-MODULE: batch insert
-- --------------------------------------
--     openGauss not support this feature, it will run as normal
-- ======================================================================================================================================

BEGIN;

CREATE SERVER batch10 FOREIGN DATA WRAPPER postgres_fdw OPTIONS( batch_size '10' );

SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=20'];

CREATE FOREIGN TABLE table30 ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );

SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30'::regclass
AND ftoptions @> array['batch_size=30'];

ALTER FOREIGN TABLE table30 OPTIONS ( SET batch_size '40');

SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30'::regclass
AND ftoptions @> array['batch_size=30'];

SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30'::regclass
AND ftoptions @> array['batch_size=40'];

ROLLBACK;

CREATE TABLE batch_table ( x int );

CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table'/*, batch_size '10' */);
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
INSERT INTO ftable SELECT * FROM generate_series(11, 31) i;
INSERT INTO ftable VALUES (32);
INSERT INTO ftable VALUES (33), (34);
SELECT COUNT(*) FROM ftable;
TRUNCATE batch_table;
DROP FOREIGN TABLE ftable;

-- try if large batches exceed max number of bind parameters
CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table'/*, batch_size '100000'*/ );
INSERT INTO ftable SELECT * FROM generate_series(1, 70000) i;
SELECT COUNT(*) FROM ftable;
TRUNCATE batch_table;
DROP FOREIGN TABLE ftable;

-- Disable batch insert
CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table'/*, batch_size '1'*/ );
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (1), (2);
INSERT INTO ftable VALUES (1), (2);
SELECT COUNT(*) FROM ftable;

-- Disable batch inserting into foreign tables with BEFORE ROW INSERT triggers
-- even if the batch_size option is enabled.
ALTER FOREIGN TABLE ftable OPTIONS ( SET batch_size '10' );
CREATE TRIGGER trig_row_before BEFORE INSERT ON ftable
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (3), (4);
INSERT INTO ftable VALUES (3), (4);
SELECT COUNT(*) FROM ftable;

-- Clean up
DROP TRIGGER trig_row_before ON ftable;
DROP FOREIGN TABLE ftable;
DROP TABLE batch_table;

-- Use partitioning
-- nspt remove it all

ALTER SERVER loopback OPTIONS (DROP batch_size);
-- ======================================================================================================================================
-- TEST-MODULE: test asynchronous execution
-- --------------------------------------
--     openGauss not support this feature, therefore, some cases that can run properly after modification are retained, 
--     and some cases that cannot be supported are directly deleted.
--     If you want to restore the test case later, refer to README in the file header.
-- ======================================================================================================================================
ALTER SERVER loopback OPTIONS (DROP extensions);              --nspt
ALTER SERVER loopback OPTIONS (ADD async_capable 'true');     --nspt
ALTER SERVER loopback2 OPTIONS (ADD async_capable 'true');    --nspt


-- ======================================================================================================================================
-- TEST-MODULE: test invalid server and foreign table options
-- --------------------------------------
-- ======================================================================================================================================
-- Invalid fdw_startup_cost option
CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS(fdw_startup_cost '100$%$#$#');
-- Invalid fdw_tuple_cost option
CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS(fdw_tuple_cost '100$%$#$#');
-- Invalid fetch_size option
CREATE FOREIGN TABLE inv_fsz (c1 int )
    SERVER loopback OPTIONS (fetch_size '100$%$#$#');
-- Invalid batch_size option
CREATE FOREIGN TABLE inv_bsz (c1 int )
    SERVER loopback OPTIONS (batch_size '100$%$#$#');

-- ======================================================================================================================================
-- TEST-MODULE: clean up all the test data
-- --------------------------------------
--  heihei!
-- ======================================================================================================================================
\c regression
drop database postgresfdw_test_db;