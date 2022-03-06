create schema cte_inline;
set current_schema = cte_inline;

-- Set up some simple test tables
CREATE TABLE test (
  f1 integer,
  f2 integer,
  f3 float
);

INSERT INTO test VALUES (1, 2, 3);
INSERT INTO test VALUES (2, 3, 4);
INSERT INTO test VALUES (3, 4, 5);
INSERT INTO test VALUES (1, 1, 1);
INSERT INTO test VALUES (2, 2, 2);
INSERT INTO test VALUES (3, 3, 3);
INSERT INTO test VALUES (6, 7, 8);
INSERT INTO test VALUES (8, 9, NULL);

CREATE TABLE test_1 (like test);

--
-- Tests for CTE inlining behavior
--

-- Basic subquery that can be inlined
explain (verbose, costs off)
with x as (select * from (select f1 from test) ss)
select * from x where f1 = 1;

-- Deep deep subquery
explain (verbose, costs off)
with a as (
    with b as (
        with c as (
            with d as (select * from (
                with z as (
                  with y as (
                    with x as (select f1 from test) 
                    select * from x)
                  select * from y)
                select * from z)
            ) select * from d)
        select * from c)
    select * from b)
select * from a where f1 = 1;

-- Explicitly request materialization
explain (verbose, costs off)
with x as materialized (select * from (select f1 from test) ss)
select * from x where f1 = 1;

-- Stable functions are safe to inline
explain (verbose, costs off)
with x as (select * from (select f1, now() from test) ss)
select * from x where f1 = 1;

-- Volatile functions prevent inlining
explain (verbose, costs off)
with x as (select * from (select f1, random() from test) ss)
select * from x where f1 = 1;

-- SELECT FOR UPDATE/SHARE cannot be inlined
explain (verbose, costs off)
with x as (select * from (select f1 from test for update) ss)
select * from x where f1 = 1;

explain (verbose, costs off)
with x as not materialized (select * from (select f1 from test for share) ss)
select * from x where f1 = 1;

-- IUDs cannot be inlined
explain (verbose, costs off)
with x as not materialized (insert into test_1 values(1,2,4) returning *)
select * from x;

explain (verbose, costs off)
with x as not materialized (update test_1 set f3 = 3 where f1 = 1 returning *)
select * from x;

explain (verbose, costs off)
with x as not materialized (delete from test_1 returning *)
select * from x;

-- Multiply-referenced CTEs are inlined only when requested
explain (verbose, costs off)
with x as (select * from (select f1, now() as n from test) ss)
select * from x, x x2 where x.n = x2.n;

explain (verbose, costs off)
with x as not materialized (select * from (select f1, now() as n from test) ss)
select * from x, x x2 where x.n = x2.n;

-- Check handling of outer references
explain (verbose, costs off)
with x as (select * from test)
select * from (with y as (select * from x) select * from y) ss;

explain (verbose, costs off)
with x as materialized (select * from test)
select * from (with y as (select * from x) select * from y) ss;

-- Ensure that we inline the currect CTE when there are
-- multiple CTEs with the same name
explain (verbose, costs off)
with x as (select 1 as y)
select * from (with x as (select 2 as y) select * from x) ss;

-- Row marks are not pushed into CTEs (opengauss not supported)
explain (verbose, costs off)
with x as (select * from test)
select * from x for update;

-- For CTEs in subquery
explain (verbose, costs off)
select * from (with x as (select * from test_1) select x.f1 from x) tmp where tmp.f1 = 1;

explain (verbose, costs off)
select * from (with x as materialized (select * from test_1) select x.f1 from x) tmp where tmp.f1 = 1;

-- cte within in/any/some sublink are handled correctly
explain (verbose, costs off)
select * from test where test.f1 in 
(with x as (select * from test_1) select x.f1 from x);

explain (verbose, costs off)
select * from test where test.f1 in 
(with x as materialized (select * from test_1) select x.f1 from x);

explain (verbose, costs off)
select * from test where test.f1 = any
(with x as (select * from test_1) select x.f1 from x);

explain (verbose, costs off)
select * from test where test.f1 = any
(with x as materialized (select * from test_1) select x.f1 from x);

-- not expanded subquery
explain (verbose, costs off)
select * from test where test.f1 = any
(with x as (select * from test_1) select x.f1 from x);

explain (verbose, costs off)
select * from test where test.f1 = any
(with x as materialized (select * from test_1) select /*+ no_expand */ x.f1 from x);

explain (verbose, costs off)
select * from test where exists 
(with x as (select * from test_1) select /*+ no_expand */ x.f1 from x where test.f1 = x.f1);

-- intargetlist rewrite
explain (verbose, costs off)
select * from test where test.f1 = (with x as (select * from test_1) select x.f2 from x where x.f2 = test.f2 and x.f2 < 10 order by 1 limit 1) and test.f2 < 50 order by 1,2,3;

explain (verbose, costs off)
select * from test where test.f1 = (with x as materialized (select * from test_1) select x.f2 from x where x.f2 = test.f2 and x.f2 < 10 order by 1 limit 1) and test.f2 < 50 order by 1,2,3;

-- not referenced cte contains DML
explain (verbose, costs off)
with x as (select f1 from test),
y as (insert into test_1 default values)
select * from x;

explain (verbose, costs off)
with a as( with z as (insert into test default values) select 1)
select 1;

-- cte with subquery and referenced in grouping function will not be inlined
explain (verbose, costs off)
WITH cte AS not materialized (
  SELECT 
    (
      CASE WHEN (
        NOT EXISTS (
          select 
            * 
          from 
            test
        )
      ) THEN ('P') END
    ) col 
  FROM 
    test_1
) 
SELECT 
  col, GROUPING(col)
FROM 
  cte
GROUP BY 
  GROUPING SETS(col);

drop schema cte_inline cascade;