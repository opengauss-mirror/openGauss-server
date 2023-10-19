--
-- LIMIT
-- Check the LIMIT/OFFSET feature of SELECT
--

SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		ORDER BY unique1 LIMIT 2;
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60
		ORDER BY unique1 LIMIT 5;
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60 AND unique1 < 63
		ORDER BY unique1 LIMIT 5;
SELECT ''::text AS three, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 100
		ORDER BY unique1 LIMIT 3 OFFSET 20;
SELECT ''::text AS zero, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
SELECT ''::text AS eleven, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
SELECT ''::text AS ten, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990;
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990 LIMIT 5;
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 LIMIT 5 OFFSET 900;

-- Stress test for variable LIMIT in conjunction with bounded-heap sorting

SELECT
  (SELECT n
     FROM (VALUES (1)) AS x,
          (SELECT n FROM generate_series(1,10) AS n
             ORDER BY n LIMIT 1 OFFSET s-1) AS y) AS z
  FROM generate_series(1,10) AS s;


CREATE SCHEMA test_limit_broadcast;
SET CURRENT_SCHEMA = test_limit_broadcast;


--
-- test limit for Gather->BroadCast issues
-- frankly speaking, this will not happen now, we keep subquery scan upon broadcast
--

--we have to test both row and column store
create table trow( a int , b int);
create table tcol( a int , b int) with (orientation=column);

--add a broadcast upon limit, need to remove
select * from ( select * from trow limit 1);
select * from ( select * from tcol limit 1);

--only one column is sorted, need to remove
select * from ( select * from trow order by a limit 1);
select * from ( select * from tcol order by a limit 1);

--broadcast not added, so no need to remove
select * from ( select * from trow order by a,b limit 1);
select * from ( select * from tcol order by a,b limit 1);

--more complicated cases: join against system table.
--the broadcast node is hide in the plan
select * from (select * from trow limit 1) t1, pg_class t2 where t1.a=t2.oid;
select * from (select * from tcol limit 1) t1, pg_class t2 where t1.a=t2.oid;

--union
select * from (select * from trow limit 1) t1, pg_class t2 where t1.a=t2.oid union select * from (select * from trow limit 1) t1, pg_class t2 where t1.a=t2.oid;
select * from (select * from tcol limit 1) t1, pg_class t2 where t1.a=t2.oid union select * from (select * from tcol limit 1) t1, pg_class t2 where t1.a=t2.oid;

--subplan
select reltype,relnamespace from pg_class where exists   ( select * from (select * from trow limit 1) t1, pg_class t2 where t1.a=t2.oid );
select reltype,relnamespace from pg_class where exists   ( select * from (select * from tcol limit 1) t1, pg_class t2 where t1.a=t2.oid );


DROP SCHEMA test_limit_broadcast CASCADE;
