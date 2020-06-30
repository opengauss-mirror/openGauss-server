/*
 * This file is used to test pull down of subplan expressions
 */
create schema col_distribute_subplan_extend;
set current_schema = col_distribute_subplan_extend;

-- Set up some simple test tables
CREATE TABLE INT4_TBL(f1 int4) with (orientation=column);

INSERT INTO INT4_TBL(f1) VALUES ('   0  ');
INSERT INTO INT4_TBL(f1) VALUES ('123456     ');
INSERT INTO INT4_TBL(f1) VALUES ('    -123456');
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');
INSERT INTO INT4_TBL(f1) VALUES (NULL);

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	ten			int4,
	hundred		int4,
	thousand	int4
) WITH (orientation=column);
insert into tenk1 select unique1, unique2, ten, hundred, thousand from public.tenk1;

CREATE TABLE SUBSELECT_TBL (
  f1 integer,
  f2 integer,
  f3 float
) with (orientation=column) distribute by hash(f1, f2, f3);

INSERT INTO SUBSELECT_TBL VALUES (1, 2, 3);
INSERT INTO SUBSELECT_TBL VALUES (2, 3, 4);
INSERT INTO SUBSELECT_TBL VALUES (3, 4, 5);
INSERT INTO SUBSELECT_TBL VALUES (1, 1, 1);
INSERT INTO SUBSELECT_TBL VALUES (2, 2, 2);
INSERT INTO SUBSELECT_TBL VALUES (3, 3, 3);
INSERT INTO SUBSELECT_TBL VALUES (6, 7, 8);
INSERT INTO SUBSELECT_TBL VALUES (8, 9, NULL);

SELECT '' AS eight, * FROM SUBSELECT_TBL ORDER BY f1, f2, f3;

-- Uncorrelated subselects

explain (costs off, nodes off)
SELECT '' AS two, f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1) ORDER BY 2;
SELECT '' AS two, f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1) ORDER BY 2;

explain (costs off, nodes off)
SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL) 
  ORDER BY 2;
SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL) 
  ORDER BY 2;

explain (costs off, nodes off)
SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE
    f2 IN (SELECT f1 FROM SUBSELECT_TBL)) 
    ORDER BY 2;
SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE
    f2 IN (SELECT f1 FROM SUBSELECT_TBL)) 
    ORDER BY 2;

explain (costs off, nodes off)
SELECT '' AS three, f1, f2
  FROM SUBSELECT_TBL
  WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                         WHERE f3 IS NOT NULL) 
                         ORDER BY f1, f2;
SELECT '' AS three, f1, f2
  FROM SUBSELECT_TBL
  WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                         WHERE f3 IS NOT NULL) 
                         ORDER BY f1, f2;
-- Correlated subselects

explain (costs off, nodes off)
SELECT '' AS six, f1 AS "Correlated Field", f2 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1) 
  ORDER BY f1, f2;
SELECT '' AS six, f1 AS "Correlated Field", f2 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1) 
  ORDER BY f1, f2;

explain (costs off, nodes off)
SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN
    (SELECT f2 FROM SUBSELECT_TBL WHERE CAST(upper.f2 AS float) = f3)
    ORDER BY 2, 3;
SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN
    (SELECT f2 FROM SUBSELECT_TBL WHERE CAST(upper.f2 AS float) = f3)
    ORDER BY 2, 3;

explain (costs off, nodes off)
SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f3 IN (SELECT upper.f1 + f2 FROM SUBSELECT_TBL
               WHERE f2 = CAST(f3 AS integer)) 
               ORDER BY 2, 3;
SELECT '' AS six, f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f3 IN (SELECT upper.f1 + f2 FROM SUBSELECT_TBL
               WHERE f2 = CAST(f3 AS integer)) 
               ORDER BY 2, 3;

explain (costs off, nodes off)
SELECT '' AS five, f1 AS "Correlated Field"
  FROM SUBSELECT_TBL
  WHERE (f1, f2) IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                     WHERE f3 IS NOT NULL) 
                     ORDER BY 2;
SELECT '' AS five, f1 AS "Correlated Field"
  FROM SUBSELECT_TBL
  WHERE (f1, f2) IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                     WHERE f3 IS NOT NULL) 
                     ORDER BY 2;

--
-- Use some existing tables in the regression test
--
explain (costs off, nodes off)
SELECT '' AS eight, ss.f1 AS "Correlated Field", ss.f3 AS "Second Field"
  FROM SUBSELECT_TBL ss
  WHERE f1 NOT IN (SELECT f1+1 FROM INT4_TBL
                   WHERE f1 != ss.f1 AND f1 < 2147483647) 
                   ORDER BY 2, 3;
SELECT '' AS eight, ss.f1 AS "Correlated Field", ss.f3 AS "Second Field"
  FROM SUBSELECT_TBL ss
  WHERE f1 NOT IN (SELECT f1+1 FROM INT4_TBL
                   WHERE f1 != ss.f1 AND f1 < 2147483647) 
                   ORDER BY 2, 3;

explain (costs off, nodes off)
select f1, float8(count(*)) / (select count(*) from int4_tbl)
from int4_tbl group by f1 order by f1;
select f1, float8(count(*)) / (select count(*) from int4_tbl)
from int4_tbl group by f1 order by f1;

--
-- Test cases to catch unpleasant interactions between IN-join processing
-- and subquery pullup.
--

-- delelte this sentence, because the output is related with number of nodes
--select count(*) from
--  (select 1 from tenk1 a
--   where unique1 IN (select hundred from tenk1 b)) ss;
explain (costs off, nodes off)
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
explain (costs off, nodes off)
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
explain (costs off, nodes off)
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;

--
-- Test cases to check for overenthusiastic optimization of
-- "IN (SELECT DISTINCT ...)" and related cases.  Per example from
-- Luca Pireddu and Michael Fuhr.
--

-- Enforce use of COMMIT instead of 2PC for temporary objects
SET enforce_two_phase_commit TO off;

-- CREATE TEMP TABLE subselect_foo (id integer);
CREATE  TABLE subselect_foo (id integer) with (orientation = column);
-- CREATE TEMP TABLE bar (id1 integer, id2 integer);
CREATE  TABLE subselect_bar (id1 integer, id2 integer) with (orientation = column);

INSERT INTO subselect_foo VALUES (1);

INSERT INTO subselect_bar VALUES (1, 1);
INSERT INTO subselect_bar VALUES (2, 2);
INSERT INTO subselect_bar VALUES (3, 1);

-- These cases require an extra level of distinct-ing above subquery s
explain (costs off, nodes off)
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT id1, id2 FROM subselect_bar) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT id1, id2 FROM subselect_bar) AS s);
explain (costs off, nodes off)
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id1,id2 FROM subselect_bar GROUP BY id1,id2) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id1,id2 FROM subselect_bar GROUP BY id1,id2) AS s);
explain (costs off, nodes off)
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id1, id2 FROM subselect_bar UNION
                      SELECT id1, id2 FROM subselect_bar) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id1, id2 FROM subselect_bar UNION
                      SELECT id1, id2 FROM subselect_bar) AS s);

-- These cases do not
explain (costs off, nodes off)
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT ON (id2) id1, id2 FROM subselect_bar) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT ON (id2) id1, id2 FROM subselect_bar) AS s);
explain (costs off, nodes off)
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM subselect_bar GROUP BY id2) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM subselect_bar GROUP BY id2) AS s);
explain (costs off, nodes off)
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM subselect_bar UNION
                      SELECT id2 FROM subselect_bar) AS s);
SELECT * FROM subselect_foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM subselect_bar UNION
                      SELECT id2 FROM subselect_bar) AS s);

--
-- Test case to catch problems with multiply nested sub-SELECTs not getting
-- recalculated properly.  Per bug report from Didier Moens.
--

CREATE TABLE orderstest (
    approver_ref integer,
    po_ref integer,
    ordercanceled boolean
) with (orientation = column);

INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 5, false);
INSERT INTO orderstest VALUES (66, 6, false);
INSERT INTO orderstest VALUES (66, 7, false);
INSERT INTO orderstest VALUES (66, 1, true);
INSERT INTO orderstest VALUES (66, 8, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (77, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);

CREATE VIEW orders_view AS
SELECT *,
(SELECT CASE
   WHEN ord.approver_ref=1 THEN '---' ELSE 'Approved'
 END) AS "Approved",
(SELECT CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (SELECT CASE
		WHEN ord.po_ref=1
		THEN
		 (SELECT CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status",
(CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (CASE
		WHEN ord.po_ref=1
		THEN
		 (CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status_OK"
FROM orderstest ord;

explain (costs off, nodes off)
SELECT * FROM orders_view 
ORDER BY approver_ref, po_ref, ordercanceled;

SELECT * FROM orders_view 
ORDER BY approver_ref, po_ref, ordercanceled;

DROP TABLE orderstest cascade;

explain (costs off, nodes off)
select f1, ss1 as relabel from
    (select *, (select sum(f1) from int4_tbl b where f1 >= a.f1) as ss1
     from int4_tbl a) ss 
     ORDER BY f1, relabel;

select f1, ss1 as relabel from
    (select *, (select sum(f1) from int4_tbl b where f1 >= a.f1) as ss1
     from int4_tbl a) ss 
     ORDER BY f1, relabel;

--
-- Test cases involving PARAM_EXEC parameters and min/max index optimizations.
-- Per bug report from David Sanchez i Gregori.
--

explain (costs off, nodes off)
select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;

select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;

explain (costs off, nodes off)
select * from (
  select min(unique1) from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss;

select * from (
  select min(unique1) from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss;

--
-- Test case for bug #4290: bogus calculation of subplan param sets
--

create  table ta (id int, val int) with (orientation=column);

insert into ta values(1,1);
insert into ta values(2,2);

create  table tb (id int, aval int) with (orientation=column);

insert into tb values(1,1);
insert into tb values(2,1);
insert into tb values(3,2);
insert into tb values(4,2);

create  table tc (id int, aid int) with (orientation=column);

insert into tc values(1,1);
insert into tc values(2,2);

explain (costs off, nodes off)
select
  ( select min(tb.id) from tb
    where tb.aval = (select ta.val from ta where ta.id = tc.aid) ) as min_tb_id
from tc 
ORDER BY min_tb_id;

select
  ( select min(tb.id) from tb
    where tb.aval = (select ta.val from ta where ta.id = tc.aid) ) as min_tb_id
from tc 
ORDER BY min_tb_id;

--
-- Test case for 8.3 "failed to locate grouping columns" bug
--

-- create temp table t1 (f1 numeric(14,0), f2 varchar(30));
create  table subselect_t1 (f1 numeric(14,0), f2 varchar(30)) with (orientation=column);

explain (costs off, nodes off)
select * from
  (select distinct f1, f2, (select f2 from subselect_t1 x where x.f1 = up.f1) as fs
   from subselect_t1 up) ss
group by f1,f2,fs;

select * from
  (select distinct f1, f2, (select f2 from subselect_t1 x where x.f1 = up.f1) as fs
   from subselect_t1 up) ss
group by f1,f2,fs;

--
-- Check that whole-row Vars reading the result of a subselect don't include
-- any junk columns therein
--
explain (costs off, nodes off)
select q from (select max(f1) from int4_tbl group by f1 order by f1) q;

select q from (select max(f1) from int4_tbl group by f1 order by f1) q;

explain (costs off, nodes off)
with q as (select max(f1) from int4_tbl group by f1 order by f1)
  select q from q;

with q as (select max(f1) from int4_tbl group by f1 order by f1)
  select q from q;

--
-- Test case for sublinks pushed down into subselects via join alias expansion
--

explain (costs off, nodes off)
select
  (select sq1) as qq1
from
  (select exists(select 1 from int4_tbl where f1 = f1) as sq1, 42 as dummy
   from int4_tbl) sq0
  join
  int4_tbl i4 on dummy = i4.f1;

select
  (select sq1) as qq1
from
  (select exists(select 1 from int4_tbl where f1 = f1) as sq1, 42 as dummy
   from int4_tbl) sq0
  join
  int4_tbl i4 on dummy = i4.f1;

--
-- Test case for cross-type partial matching in hashed subplan (bug #7597)
--

-- create temp table outer_7597 (f1 int4, f2 int4);
create  table outer_7597 (f1 int4, f2 int4) with (orientation=column);
insert into outer_7597 values (0, 0);
insert into outer_7597 values (1, 0);
insert into outer_7597 values (0, null);
insert into outer_7597 values (1, null);

-- create temp table inner_7597(c1 int8, c2 int8);
create  table inner_7597(c1 int8, c2 int8) with (orientation=column);
insert into inner_7597 values(0, null);

explain (costs off, nodes off)
select * from outer_7597 where (f1, f2) not in (select * from inner_7597);
select * from outer_7597 where (f1, f2) not in (select * from inner_7597) order by 1 ,2;

--
-- Test case for planner bug with nested EXISTS handling
--
explain (costs off, nodes off)
select a.thousand from tenk1 a, tenk1 b
where a.thousand = b.thousand
  and exists ( select 1 from tenk1 c where b.hundred = c.hundred
                   and not exists ( select 1 from tenk1 d
                                    where a.thousand = d.thousand ) );

select a.thousand from tenk1 a, tenk1 b
where a.thousand = b.thousand
  and exists ( select 1 from tenk1 c where b.hundred = c.hundred
                   and not exists ( select 1 from tenk1 d
                                    where a.thousand = d.thousand ) );

---add llt case
CREATE TABLE t_subplan08
(
   col_num	numeric(5, 0)
  ,col_int	int
  ,col_timestamptz	timestamptz
  ,col_varchar	varchar
  ,col_char	char(2)
  ,col_interval	interval
  ,col_timetz	timetz
  ,col_tinterval	tinterval
) with(orientation=column);

COPY t_subplan08(col_num, col_int, col_timestamptz, col_varchar, col_char, col_interval, col_timetz, col_tinterval) FROM stdin;
123	5	2017-09-09 19:45:37	2017-09-09 19:45:37	a	2 day 13:34:56	1984-2-6 01:00:30+8	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]
234	6	2017-10-09 19:45:37	2017-10-09 19:45:37	c	1 day 18:34:56	1986-2-6 03:00:30+8	["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
345	7	2017-11-09 19:45:37	2017-11-09 19:45:37	d	1 day 13:34:56	1987-2-6 08:00:30+8	["epoch" "Mon May 1 00:30:30 1995"]
456	8	2017-12-09 19:45:37	2017-12-09 19:45:37	h	18 day 14:34:56	1989-2-6 06:00:30+8	["Feb 15 1990 12:15:03" "2001-09-23 11:12:13"]
567	9	2018-01-09 19:45:37	2018-01-09 19:45:37	m	18 day 15:34:56	1990-2-6 12:00:30+8	\N
678	10	2018-02-09 19:45:37	2018-02-09 19:45:37	\N	7 day 16:34:56	2002-2-6 00:00:30+8	["-infinity" "infinity"]
789	11	2018-03-09 19:45:37	2018-03-09 19:45:37	g	22 day 13:34:56	1984-2-6 00:00:30+8	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
147	12	2018-04-09 19:45:37	2018-04-09 19:45:37	l	\N	1984-2-7 00:00:30+8	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
369	13	2018-05-09 19:45:37	2018-05-09 19:45:37	a	21 day 13:34:56	\N	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
\.

CREATE TABLE t_subplan09
(
   col_num	numeric(5, 0)
  ,col_int	int
  ,col_timestamptz	timestamptz
  ,col_varchar	varchar
  ,col_char	char(2)
  ,col_interval	interval
  ,col_timetz	timetz
  ,col_tinterval	tinterval
) with(orientation=column);

insert into t_subplan09 select * from t_subplan08;
insert into t_subplan09 values (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

CREATE TABLE t_subplan10
(
   col_num	numeric(5, 0)
  ,col_int	int
  ,col_timestamptz	timestamptz
  ,col_varchar	varchar
  ,col_char	char(2)
  ,col_interval	interval
  ,col_timetz	timetz
  ,col_tinterval	tinterval
) with(orientation=column);

insert into t_subplan08 values (1,4,'2014-01-09 19:35:37','2014-11-09 19:35:37','j','8 day 13:34:56','1988-2-6 01:00:30+8', NULL);

explain (costs off, nodes off, verbose on) 
select count(*) from t_subplan08 group by col_interval  having(min(col_interval)  = any(select col_interval  from t_subplan09));
select count(*) from t_subplan08 group by col_interval  having(min(col_interval)  = any(select col_interval  from t_subplan09));

explain (costs off, nodes off, verbose on) 
select count(*) from t_subplan08 group by col_interval  having(min(col_interval)  = any(select col_interval  from t_subplan10));
select count(*) from t_subplan08 group by col_interval  having(min(col_interval)  = any(select col_interval  from t_subplan10));

explain (costs off, nodes off, verbose on)
select count(*) from t_subplan08 group by col_timetz  having(min(col_timetz)  = any(select col_timetz  from t_subplan09)) order by 1;
select count(*) from t_subplan08 group by col_timetz  having(min(col_timetz)  = any(select col_timetz  from t_subplan09)) order by 1;


explain (costs off, nodes off, verbose on)
select count(*) from t_subplan08 group by col_char  having(min(col_char)  = any(select col_char  from t_subplan09)) order by 1;
select count(*) from t_subplan08 group by col_char  having(min(col_char)  = any(select col_char  from t_subplan09)) order by 1;

--added for dts
explain (costs off, nodes off, verbose on)
select count(*) from t_subplan08 group by col_interval , col_int  having(min(col_interval)  > (select col_interval  from t_subplan09 where col_int = t_subplan08.col_int));
select count(*) from t_subplan08 group by col_interval , col_int  having(min(col_interval)  > (select col_interval  from t_subplan09 where col_int = t_subplan08.col_int));
select count(*) from t_subplan08 group by col_interval , col_int  having(min(col_interval)  = (select col_interval  from t_subplan09 where col_int = t_subplan08.col_int));
select count(*) from t_subplan08  where ctid  = (select ctid  from t_subplan09 where col_int = t_subplan08.col_int);

drop schema col_distribute_subplan_extend cascade;
