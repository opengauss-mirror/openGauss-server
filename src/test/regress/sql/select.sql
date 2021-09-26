--
-- SELECT
--

-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1d -2 +0nr -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using <, unique1 using >;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1dr -2 +0n -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using >, unique1 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0nr -1 +1d -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >, string4 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0n -1 +1dr -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using <, string4 using >;

--
-- test partial btree indexes
--
-- As of 7.2, planner probably won't pick an indexscan without stats,
-- so ANALYZE first.  Also, we want to prevent it from picking a bitmapscan
-- followed by sort, because that could hide index ordering problems.
--
ANALYZE onek2;

SET enable_seqscan TO off;
SET enable_bitmapscan TO off;
SET enable_sort TO off;

--
-- awk '{if($1<10){print $0;}else{next;}}' onek.data | sort +0n -1
--
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10 ORDER BY unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980 
   ORDER BY unique1 using <;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;

SELECT two, stringu1, ten, string4
  INTO TABLE tmp
   FROM onek;
--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=2){print $4,$5;}else{print;}}' - stud_emp.data
--
-- SELECT name, age FROM person*; ??? check if different
SELECT p.name, p.age FROM person* p 
    ORDER BY p.name, p.age;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $4,$5;}else{print;}}' - stud_emp.data |
-- sort +1nr -2
--
SELECT p.name, p.age FROM person* p ORDER BY age using >, name;

--
-- Test some cases involving whole-row Var referencing a subquery
--
select foo from (select 1) as foo;
select foo from (select null) as foo;
select foo from (select 'xyzzy',1,null) as foo;

--
-- Test VALUES lists
--
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j 
    ORDER BY unique1;

-- a more complex case
-- looks like we're coding lisp :-)
select * from onek,
  (values ((select i from
    (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
    order by i asc limit 1))) bar (i)
  where onek.unique1 = bar.i 
  ORDER BY unique1;
-- try VALUES in a subquery
select * from onek
    where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
    order by unique1;

-- VALUES is also legal as a standalone query or a set-operation member
VALUES (1,2), (3,4+4), (7,77.7);
VALUES (1,2), (3,4+4), (7,77.7)
UNION ALL
SELECT 2+2, 57
UNION ALL
TABLE int8_tbl 
ORDER BY column1,column2;
--
-- Test ORDER BY options
--

-- Enforce use of COMMIT instead of 2PC for temporary objects

CREATE TEMP TABLE foo (f1 int);

INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 ASC;	-- same thing
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- check if indexscans do the right things
CREATE INDEX fooi ON foo (f1);
SET enable_sort = false;

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--
-- Test alias : VALUE, NAME, TYPE
--
CREATE TEMP TABLE fooAlias (f1 int);
INSERT INTO fooAlias VALUES (42);
SELECT f1 VALUE FROM fooAlias;
SELECT f1 NAME FROM fooAlias;
SELECT f1 TYPE FROM fooAlias;
--
-- Test some corner cases that have been known to confuse the planner
--

-- ORDER BY on a constant doesn't really need any sorting
SELECT 1 AS x ORDER BY x;

-- But ORDER BY on a set-valued expression does
create function sillysrf(int) returns setof int as
  'values (1),(10),(2),($1)' language sql immutable;

select sillysrf(42) order by 1;
select sillysrf(-1) order by 1;

drop function sillysrf(int);

-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
select * from (values (2),(null),(1)) v(k) where k = k order by k;
select * from (values (2),(null),(1)) v(k) where k = k order by k desc;
SELECT (NULL)IN(1,2,3)AS RESULT3 FROM DUAL; 
SELECT 'TRUE'iN(1 IN (1),2 IN (1))AS RESULT;

drop table if exists row_rep_tb_toast;
create table row_rep_tb_toast(a int, b float, c varchar(10240), d text) ;

drop function if exists test_toast;

create or replace function test_toast()
returns table(chunk_id oid, chunk_seq integer, chunk_data bytea) as
$BODY$	
BEGIN
RETURN QUERY EXECUTE format('select * from pg_toast.%I', (select 'pg_toast_' || (select oid from pg_class where relname = 'row_rep_tb_toast')));
END;
$BODY$
LANGUAGE plpgsql;

select * from test_toast();

drop function test_toast;
drop table row_rep_tb_toast;

-- test current_date for FQS
create table test_current_date(a int);
explain verbose select current_date from test_current_date;
drop table test_current_date;

-- system column of foreign table

create schema syscolofforeign;
set current_schema=syscolofforeign;

create table t (a int, b int, c int, d int);
create foreign table ft1 (a int, b int, c int, d int)
SERVER gsmpp_server OPTIONS (delimiter '|', encoding 'utf8', format 'text', location 'gsfs://127.0.0.1:12345/test.dat', mode 'Normal');

select xmin from t;
select xmin from ft1;
insert into ft1(xmin) values (666);
select * from ft1 tt1, ft1 tt2 where tt1.xmin=tt2.xmin;
select * from ft1 tt1 left join ft1 tt2 on tt1.xmin=tt2.xmin;
select * from ft1 tt1 left join ft1 tt2 using(xmin);
select xmin, sum(a) from ft1 group by xmin;
select tt1.xmin from ft1 tt1, ft1 tt2 where tt1.a=tt2.a;
select * from (select xmin, a from ft1 where b=2 and c=5) tt1, ft1 tt2 where tt1.a=tt2.a;
select * from (select xmin, a from ft1 where b=2 and c=5) tt1 left join ft1 tt2 on tt1.a=tt2.a;
select (j.*) is null from (ft1 tt1 join ft1 tt2 using (xmin)) j;
select ft1.xmin from t, ft1 where t.a=ft1.a;
select t.xmin from t, ft1 where t.a=ft1.a;
select xmin from t, ft1 where t.a=ft1.a;

drop schema syscolofforeign cascade;

-- test ^=
select 1 ^= 2;