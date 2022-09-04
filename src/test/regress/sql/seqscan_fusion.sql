create schema lazyagg;
set current_schema=lazyagg;
set rewrite_rule=lazyagg;

------------------------------------------------------------------------------------
-- create table
------------------------------------------------------------------------------------

create table t (a int, b int, c int, d int);
create table t1 (a int, b int, c int, d int);
create table t2 (a int, b int, c int, d int);
create table t3 (a int, b int, c int, d int);

insert into t values 
 (1, 1, 1, 1)
,(1, 1, 1, 2)
,(1, 1, 2, 1)
,(1, 1, 2, 2)
,(1, 2, 1, 1)
,(1, 2, 1, 2)
,(1, 2, 2, 1)
,(1, 2, 2, 2)
,(1, 3, 3, 3)
,(2, 5, 5, 5)
,(2, NULL, 6, 6)
,(2, 6, NULL, 6)
,(2, 6, 6, NULL)
,(2, NULL, NULL, 7)
,(2, NULL, 7, NULL)
,(2, 7, NULL, NULL)
,(3, NULL, NULL, NULL)
;

insert into t1 select * from t;
insert into t2 select * from t;
insert into t3 select * from t;
------------------------------------------------------------------------------------
-- basic single sub-query
------------------------------------------------------------------------------------

-- valid for lazy agg
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- invalid for lazy agg
explain verbose select t.b, sum(cc) from (select b, min(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, min(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, avg(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, avg(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- basic setop
------------------------------------------------------------------------------------

-- same functions
explain verbose select t.b, min(cc) from (select b, min(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- different functions: f(f union all g) or f(g union all f)
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, count(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, min(cc) from (select b, sum(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- different functions: f(g union all g)
explain verbose select t.b, sum(cc) from (select b, max(c) as cc from t1 group by b union all select b, max(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b union all select b, count(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- different functions: f(g union all h)
explain verbose select t.b, sum(cc) from (select b, min(c) as cc from t1 group by b union all select b, max(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- other set op
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union select b, sum(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b intersect all select b, sum(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- complex setop (single super agg)
------------------------------------------------------------------------------------

-- uncomplete agg in setop branch
explain verbose select t.b, sum(cc) from (select b, c as cc from t1 union all select b, sum(c) as cc from t2 group by b union all select b, sum(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- over-complete agg in setop branch, agg number not match
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc, sum(d) as dd from t1 group by b union all select b, sum(c) as cc, count(d) as dd from t2 group by b union all select b, sum(c) as cc, count(*) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, all setop are union all, item(s) have same function
explain verbose select t.b, min(cc) from (select b, min(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b union all select b, min(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b union all select b, count(c) as cc from t2 group by b union all select b, count(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, all setop are union all, item(s) have different function, non-delay rewrite
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, sum(c) as cc from t2 group by b union all select b, max(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, all setop are union all, item(s) have different function, delay rewrite
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, sum(c) as cc from t2 group by b union all select b, count(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(*) as cc from t1 group by b union all select b, sum(c) as cc from t2 group by b union all select b, sum(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, count(*) as cc from t2 group by b union all select b, count(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, not all setop(s) are union all, item(s) have same function
explain verbose select t.b, min(cc) from (select b, min(c) as cc from t1 group by b intersect (select b, min(c) as cc from t2 group by b union all select b, min(c) as cc from t3 group by b)) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- parent-query check
------------------------------------------------------------------------------------

-- no agg func OR no group by
explain verbose select t.b from (select b, max(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1;

explain verbose select t.b from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1;

explain verbose select t.b from (select b, sum(c) as cc from t1 group by b) s1, (select b, count(c) as cc from t1 group by b) s2, t where s1.b=t.b and s2.b=t.b group by t.b order by 1;

explain verbose select b from (select b from t1 group by b) group by b;

explain verbose select sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b order by 1;

explain verbose select sum(cc) from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b order by 1;

explain verbose select sum(b) from (select b from t1 group by b);

-- agg param is not in subquery, in subquery and super table, OR in multiple subquery
explain verbose select t.b, sum(t.d) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc), sum(t.d) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(t.d) from (select b, sum(c) as cc, a, sum(d) as dd from t1 group by b,a) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(s1.cc) from (select b, sum(c) as cc from t1 group by b) s1, (select b, sum(c) as cc from t2 group by b) s2, t where s1.b=t.b and s2.b=t.b group by t.b order by 1,2;

explain verbose select t.b, max(s1.cc), max(s2.cc) from (select b, max(c) as cc from t1 group by b) s1, (select b, max(c) as cc from t2 group by b) s2, t where s1.b=t.b and s2.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, min(cc), min(t.d) from (select b, min(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

-- expr on agg param
explain verbose select t.b, sum(cc+cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc*random()) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc)*2.3 from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- volatile
explain verbose select t.b, sum(cc)*random() from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- no subquery
explain verbose select t.b, sum(t1.c) from t1, t where t1.b=t.b group by t.b order by 1,2;


------------------------------------------------------------------------------------
-- child-query check
------------------------------------------------------------------------------------

-- no setop no agg
explain verbose select t.b, sum(cc) from (select b, c as cc from t1) s1, t where s1.b=t.b group by t.b order by 1,2;


-- has agg, but no group by
explain verbose select t.b from (select max(b) as b, max(c) as cc from t1) s1, t where s1.b=t.b group by t.b order by 1;

-- no agg function, but have group by
explain verbose select count(*) from (select b,c from t1 group by b,c) s1, t where s1.b=t.b group by s1.c order by 1;

-- expr out of agg function
explain verbose select t.b, sum(cc) from (select b, sum(c)*2 as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c)+sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, cast(sum(c) as int8) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c+d) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- normal function in child-query's target list
explain verbose select aa from (select a, PG_CLIENT_ENCODING() aa from t group by 1) group by 1 order by 1;

------------------------------------------------------------------------------------
-- joint check
------------------------------------------------------------------------------------

-- agg func column in super query join/group by/where
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.cc>t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b and s1.cc>9000 group by t.b order by 1,2;

explain verbose select t.b, cc, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b, s1.cc order by 1,2,3;


-- join type, normal
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1 left join t on s1.b=t.b group by t.b order by 1,2;

explain verbose select s1.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1 where s1.b not in (select t.b from t) group by s1.b order by 1,2;


-- join type, delay rewrite
explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b) s1 left join t on s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from t right join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;


-- join type, multiple join, with SUM(COUNT)
explain verbose select s1.b, sum(cc2) from (select b, sum(c) as cc1 from t1 group by b) s1 left join (select b, sum(c) as cc2 from t2 group by b) s2 on s1.b=s2.b left join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc2) from (select b, sum(c) as cc1 from t1 group by b) s1 left join (select b, count(c) as cc2 from t2 group by b) s2 on s1.b=s2.b left join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc3) from (select b, sum(c) as cc1 from t1 group by b) s1 left join ((select b, sum(c) as cc2 from t2 group by b) s2 right join (select b, count(c) as cc3 from t3 group by b) s3 on s2.b=s3.b) on s1.b=s2.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc1) from ((select b, count(c) as cc1 from t1 group by b) s1 left join (select b, sum(c) as cc2 from t2 group by b) s2 on s1.b=s2.b) right join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc3) from (select b, sum(c) as cc1 from t1 group by b) s1 right join (select b, sum(c) as cc2 from t2 group by b) s2 on s1.b=s2.b left join (select b, count(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc2) from (select b, sum(c) as cc1 from t1 group by b) s1 right join (select b, count(c) as cc2 from t2 group by b) s2 on s1.b=s2.b right join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;


-- super agg func param come from non-agg func column of sub-query
explain verbose select t.b, sum(s1.d) from (select b, d, sum(c) as cc from t1 group by b, d) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- multi-agg functions
------------------------------------------------------------------------------------

-- single subquery, different order of super-agg and sub-agg
explain verbose select t.b, sum(cc), max(dd) from (select b, sum(c) as cc, max(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(dd), sum(cc) from (select b, sum(c) as cc, count(*) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select max(dd), t.b, sum(cc) from (select b, sum(c) as cc, max(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, min(cc), sum(dd) from (select b, min(c) as cc, count(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, count(c) as cc, count(*) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- simple setop
explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, sum(d) as dd from t1 group by b union all select b, sum(c) as cc, sum(d) as dd from t2 group by b union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), max(dd) from (select b, count(c) as cc, max(d) as dd from t1 group by b union all select b, count(c) as cc, max(d) as dd from t2 group by b union all select b, count(c) as cc, max(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- complex setop, one is valid and another is invalid
explain verbose select t.b, sum(cc), min(dd) from (select b, sum(c) as cc, min(d) as dd from t1 group by b union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, sum(d) as dd from t1 group by b union all select b, sum(c) as cc, count(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, count(c) as cc, sum(d) as dd from t1 group by b union all select b, count(c) as cc, count(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, count(c) as cc, count(d) as dd from t1 group by b union all select b, count(c) as cc, count(*) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- complex setop: agg func number not match in setop branch
explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, d as dd from t1 group by b,d union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, d as dd from t1 group by b,d union all select b, c as cc, sum(d) as dd from t2 group by b,c union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- 1--n
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc, min(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc, avg(d)+1 as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc, sum(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc, avg(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;


-- n--1
explain verbose select t.b, sum(cc), sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(cc), sum(cc), sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3,4,5;


-- n--1, expr
explain verbose select t.b, sum(cc) + sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) + sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;


------------------------------------------------------------------------------------
-- CTE
------------------------------------------------------------------------------------

-- basic with as
explain verbose with s as (select b, sum(c) as cc from t1 group by b) select t.b, sum(cc) from s, t where s.b=t.b group by t.b order by 1,2;


-- multi-usage
explain verbose with s1(b, cc) as (select b, sum(c) from t1 group by b) select count(*) from s1 where cc > (select sum(cc)/100 from s1) order by 1;

-- pull up
explain verbose with s as (select b, sum(c) as cc from t1 group by b) select a, b, c, d from t where b not in (select sum(cc) from s group by b) order by 1,2,3,4;

-- not pull up
explain verbose with s as (select b, sum(c) as cc from t1 group by b) select 1 from t where exists (select b, sum(cc) from s where s.b=t.b group by b);


-- multi-cte
explain verbose 
with s as
(select a,b from t1 group by a,b)
select max(b) from
(
	select b from
	(
		select max(b) b from
		(
			with s as
			(select a+b a, a-b b from t2 group by 1,2)
			select max(b) b from
			(select a*b b from t where a in (select b from s group by b))
		)
	)
) order by 1;

------------------------------------------------------------------------------------
-- In, Exists
------------------------------------------------------------------------------------
explain verbose select t.b from t where exists (select b, sum(c) as cc from t1 where b > 100 group by b) group by t.b order by 1;

explain verbose select b from t where b in (select b from t1 group by b) group by b order by 1;

explain verbose 
select a,b,c,d from t2 where exists
(select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2) order by 1,2,3,4;

------------------------------------------------------------------------------------
-- multi-layer lazy agg
------------------------------------------------------------------------------------

-- a chain

explain verbose select * from (select s1.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1 group by s1.b) order by 1,2;
select * from (select s1.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1 group by s1.b) order by 1,2;

explain verbose 
select b, sum(ccc) as cccc from(
select b, sum(cc) as ccc from (
select b, sum(c) as cc from t1 group by b
) s1 group by b
) s2 group by b order by 1,2;

-- with setop
explain verbose 
select b, sum(ccc) as cccc from(
select b, sum(cc) as ccc from (
select b, sum(c) as cc from t1 group by b
) s11 group by b
union all
select b, count(cc) as ccc from (
select b, sum(c) as cc from t2 group by b
) s12 group by b
) s2 group by b order by 1,2;

explain verbose 
select b, sum(ccc) as cccc from(
select b, sum(cc) as ccc from (
select b, count(c) as cc from t1 group by b
) s11 group by b
union all
select b, count(cc) as ccc from (
select b, sum(c) as cc from t2 group by b
) s12 group by b
) s2 group by b order by 1,2;

explain verbose 
(select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2)
union all
(select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2) order by 1,2;

------------------------------------------------------------------------------------
-- empty table
------------------------------------------------------------------------------------

create table t_empty (a int, b int, c int, d int);

explain verbose select t_empty.b, sum(cc) from (select b, count(c) as cc from t_empty group by b) s, t_empty where s.b=t_empty.b group by t_empty.b;

explain verbose select sum(cc) from (select b, count(c) as cc from t_empty group by b) s, t_empty where s.b=t_empty.b;

explain verbose select t_empty.b, sum(cc) from (select b, count(*) as cc from t_empty group by b) s, t_empty where s.b=t_empty.b group by t_empty.b;

explain verbose select sum(cc) from (select b, count(*) as cc from t_empty group by b) s, t_empty where s.b=t_empty.b;

explain verbose select t_empty.b, sum(cc), sum(dd) from (select b, count(c) as cc, sum(d) as dd from t_empty group by b) s, t_empty where s.b=t_empty.b group by t_empty.b;

explain verbose select sum(cc), sum(dd) from (select b, count(c) as cc, sum(d) as dd from t_empty group by b) s, t_empty where s.b=t_empty.b;

------------------------------------------------------------------------------------
-- data type convert
------------------------------------------------------------------------------------

create table t_type (a int2, b int8, c varchar(10), d char(5), e text, f date, g timestamp, h interval);

insert into t_type values (1, 2, 'CCC', 'DDD', 'EEE', date '5-4-2017', '4-17-2017', interval '3' day);

explain verbose select t_type.c, sum(dd) from (select c, count(d) as dd from t_type group by c) s, t_type where s.c=t_type.c group by t_type.c;

explain verbose select t_type.c, sum(dd) from (select c, count(d) as dd from t_type group by c union all select c, count(d) as dd from t_type group by c) s, t_type where s.c=t_type.c group by t_type.c order by 1,2;

explain verbose select sum(a1), sum(a2), sum(a3) from (select a, sum(b) a1, count(c) a2, count(f) a3 from t_type group by a union all select a, sum(a), count(e), count(g) from t_type group by a) s, t_type where s.a=t_type.a group by t_type.a;

explain verbose select sum(a1), sum(a2), sum(a3) from (select a, sum(b) a1, count(c) a2, count(*) a3 from t_type group by a union all select a, sum(a), count(e), count(*) from t_type group by a) s, t_type where s.a=t_type.a group by t_type.a;

create view v as select * from t;

explain verbose  
select t1.a, sum(cc)
from t1 full join v using(a) full join (select a, sum(c) as cc from t2 group by a) using(a)
where t1.a>5
group by t1.a
order by 1;

create view v1 as select a from t1 group by a;

explain verbose 
select t2.b
from t2 full join v1 using(a) full join t3 using(a)
group by t2.b order by 1;

------------------------------------------------------------------------------------
-- Unused column
------------------------------------------------------------------------------------

explain verbose 
select t.b from (select b, max(c) as cc from t1 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, count(c) as cc from t1 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, count(c) as cc, count(d) as dd from t1 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, count(c) as cc from t1 group by b) s1 left join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, count(*) as cc from t1 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, count(*) as cc from t1 group by b) s1 left join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, avg(c) as cc from t1 group by b) s1 full join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, count(c) as cc from t1 group by b) s1 inner join t on s1.b=t.b inner join (select b, count(d) as dd from t2 group by b) s2 on s2.b=t.b group by t.b order by 1;

explain verbose 
select t.b, sum(dd) from (select b, count(c) as cc, count(d) as dd from t1 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1,2;

explain verbose 
select t.b from (select b, count(c) as cc from t1 group by b union all select b, sum(c) as cc from t2 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, c as cc from t1 group by b,c union all select b, count(c) as cc from t2 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1;

explain verbose 
select t.b from (select b, avg(c) as cc from t1 group by b union all select b, avg(c) as cc from t2 group by b) s1 inner join t on s1.b=t.b group by t.b order by 1;

set enable_seqscan_fusion = on;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- invalid for lazy agg
explain verbose select t.b, sum(cc) from (select b, min(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from (select b, min(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, avg(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;
select t.b, avg(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- basic setop
------------------------------------------------------------------------------------

-- same functions
explain verbose select t.b, min(cc) from (select b, min(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- different functions: f(f union all g) or f(g union all f)
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, count(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, min(cc) from (select b, sum(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- different functions: f(g union all g)
explain verbose select t.b, sum(cc) from (select b, max(c) as cc from t1 group by b union all select b, max(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b union all select b, count(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- different functions: f(g union all h)
explain verbose select t.b, sum(cc) from (select b, min(c) as cc from t1 group by b union all select b, max(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- other set op
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union select b, sum(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b intersect all select b, sum(c) as cc from t2 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- complex setop (single super agg)
------------------------------------------------------------------------------------

-- uncomplete agg in setop branch
explain verbose select t.b, sum(cc) from (select b, c as cc from t1 union all select b, sum(c) as cc from t2 group by b union all select b, sum(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- over-complete agg in setop branch, agg number not match
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc, sum(d) as dd from t1 group by b union all select b, sum(c) as cc, count(d) as dd from t2 group by b union all select b, sum(c) as cc, count(*) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, all setop are union all, item(s) have same function
explain verbose select t.b, min(cc) from (select b, min(c) as cc from t1 group by b union all select b, min(c) as cc from t2 group by b union all select b, min(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b union all select b, count(c) as cc from t2 group by b union all select b, count(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, all setop are union all, item(s) have different function, non-delay rewrite
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, sum(c) as cc from t2 group by b union all select b, max(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, all setop are union all, item(s) have different function, delay rewrite
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, sum(c) as cc from t2 group by b union all select b, count(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(*) as cc from t1 group by b union all select b, sum(c) as cc from t2 group by b union all select b, sum(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b union all select b, count(*) as cc from t2 group by b union all select b, count(c) as cc from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- three items, not all setop(s) are union all, item(s) have same function
explain verbose select t.b, min(cc) from (select b, min(c) as cc from t1 group by b intersect (select b, min(c) as cc from t2 group by b union all select b, min(c) as cc from t3 group by b)) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- parent-query check
------------------------------------------------------------------------------------

-- no agg func OR no group by
explain verbose select t.b from (select b, max(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1;

explain verbose select t.b from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1;

explain verbose select t.b from (select b, sum(c) as cc from t1 group by b) s1, (select b, count(c) as cc from t1 group by b) s2, t where s1.b=t.b and s2.b=t.b group by t.b order by 1;

explain verbose select b from (select b from t1 group by b) group by b;

explain verbose select sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b order by 1;

explain verbose select sum(cc) from (select b, count(*) as cc from t1 group by b) s1, t where s1.b=t.b order by 1;

explain verbose select sum(b) from (select b from t1 group by b);

-- agg param is not in subquery, in subquery and super table, OR in multiple subquery
explain verbose select t.b, sum(t.d) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc), sum(t.d) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(t.d) from (select b, sum(c) as cc, a, sum(d) as dd from t1 group by b,a) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(s1.cc) from (select b, sum(c) as cc from t1 group by b) s1, (select b, sum(c) as cc from t2 group by b) s2, t where s1.b=t.b and s2.b=t.b group by t.b order by 1,2;

explain verbose select t.b, max(s1.cc), max(s2.cc) from (select b, max(c) as cc from t1 group by b) s1, (select b, max(c) as cc from t2 group by b) s2, t where s1.b=t.b and s2.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, min(cc), min(t.d) from (select b, min(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

-- expr on agg param
explain verbose select t.b, sum(cc+cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc*random()) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc)*2.3 from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- volatile
explain verbose select t.b, sum(cc)*random() from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- no subquery
explain verbose select t.b, sum(t1.c) from t1, t where t1.b=t.b group by t.b order by 1,2;


------------------------------------------------------------------------------------
-- child-query check
------------------------------------------------------------------------------------

-- no setop no agg
explain verbose select t.b, sum(cc) from (select b, c as cc from t1) s1, t where s1.b=t.b group by t.b order by 1,2;


-- has agg, but no group by
explain verbose select t.b from (select max(b) as b, max(c) as cc from t1) s1, t where s1.b=t.b group by t.b order by 1;

-- no agg function, but have group by
explain verbose select count(*) from (select b,c from t1 group by b,c) s1, t where s1.b=t.b group by s1.c order by 1;

-- expr out of agg function
explain verbose select t.b, sum(cc) from (select b, sum(c)*2 as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c)+sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, cast(sum(c) as int8) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c+d) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

-- normal function in child-query's target list
explain verbose select aa from (select a, PG_CLIENT_ENCODING() aa from t group by 1) group by 1 order by 1;

------------------------------------------------------------------------------------
-- joint check
------------------------------------------------------------------------------------

-- agg func column in super query join/group by/where
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.cc>t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b and s1.cc>9000 group by t.b order by 1,2;

explain verbose select t.b, cc, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b, s1.cc order by 1,2,3;


-- join type, normal
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1 left join t on s1.b=t.b group by t.b order by 1,2;

explain verbose select s1.b, sum(cc) from (select b, sum(c) as cc from t1 group by b) s1 where s1.b not in (select t.b from t) group by s1.b order by 1,2;


-- join type, delay rewrite
explain verbose select t.b, sum(cc) from (select b, count(c) as cc from t1 group by b) s1 left join t on s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from t right join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;


-- join type, multiple join, with SUM(COUNT)
explain verbose select s1.b, sum(cc2) from (select b, sum(c) as cc1 from t1 group by b) s1 left join (select b, sum(c) as cc2 from t2 group by b) s2 on s1.b=s2.b left join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc2) from (select b, sum(c) as cc1 from t1 group by b) s1 left join (select b, count(c) as cc2 from t2 group by b) s2 on s1.b=s2.b left join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc3) from (select b, sum(c) as cc1 from t1 group by b) s1 left join ((select b, sum(c) as cc2 from t2 group by b) s2 right join (select b, count(c) as cc3 from t3 group by b) s3 on s2.b=s3.b) on s1.b=s2.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc1) from ((select b, count(c) as cc1 from t1 group by b) s1 left join (select b, sum(c) as cc2 from t2 group by b) s2 on s1.b=s2.b) right join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc3) from (select b, sum(c) as cc1 from t1 group by b) s1 right join (select b, sum(c) as cc2 from t2 group by b) s2 on s1.b=s2.b left join (select b, count(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;

explain verbose select s1.b, sum(cc2) from (select b, sum(c) as cc1 from t1 group by b) s1 right join (select b, count(c) as cc2 from t2 group by b) s2 on s1.b=s2.b right join (select b, sum(c) as cc3 from t3 group by b) s3 on s2.b=s3.b group by s1.b order by 1,2;


-- super agg func param come from non-agg func column of sub-query
explain verbose select t.b, sum(s1.d) from (select b, d, sum(c) as cc from t1 group by b, d) s1, t where s1.b=t.b group by t.b order by 1,2;

------------------------------------------------------------------------------------
-- multi-agg functions
------------------------------------------------------------------------------------

-- single subquery, different order of super-agg and sub-agg
explain verbose select t.b, sum(cc), max(dd) from (select b, sum(c) as cc, max(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(dd), sum(cc) from (select b, sum(c) as cc, count(*) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select max(dd), t.b, sum(cc) from (select b, sum(c) as cc, max(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, min(cc), sum(dd) from (select b, min(c) as cc, count(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, count(c) as cc, count(*) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- simple setop
explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, sum(d) as dd from t1 group by b union all select b, sum(c) as cc, sum(d) as dd from t2 group by b union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), max(dd) from (select b, count(c) as cc, max(d) as dd from t1 group by b union all select b, count(c) as cc, max(d) as dd from t2 group by b union all select b, count(c) as cc, max(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- complex setop, one is valid and another is invalid
explain verbose select t.b, sum(cc), min(dd) from (select b, sum(c) as cc, min(d) as dd from t1 group by b union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, sum(d) as dd from t1 group by b union all select b, sum(c) as cc, count(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, count(c) as cc, sum(d) as dd from t1 group by b union all select b, count(c) as cc, count(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, count(c) as cc, count(d) as dd from t1 group by b union all select b, count(c) as cc, count(*) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- complex setop: agg func number not match in setop branch
explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, d as dd from t1 group by b,d union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(dd) from (select b, sum(c) as cc, d as dd from t1 group by b,d union all select b, c as cc, sum(d) as dd from t2 group by b,c union all select b, sum(c) as cc, sum(d) as dd from t3 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;


-- 1--n
explain verbose select t.b, sum(cc) from (select b, sum(c) as cc, min(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, sum(c) as cc, avg(d)+1 as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc, sum(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) from (select b, count(c) as cc, avg(d) as dd from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;


-- n--1
explain verbose select t.b, sum(cc), sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3;

explain verbose select t.b, sum(cc), sum(cc), sum(cc), sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2,3,4,5;


-- n--1, expr
explain verbose select t.b, sum(cc) + sum(cc) from (select b, sum(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;

explain verbose select t.b, sum(cc) + sum(cc) from (select b, count(c) as cc from t1 group by b) s1, t where s1.b=t.b group by t.b order by 1,2;


------------------------------------------------------------------------------------
-- CTE
------------------------------------------------------------------------------------

-- basic with as
explain verbose with s as (select b, sum(c) as cc from t1 group by b) select t.b, sum(cc) from s, t where s.b=t.b group by t.b order by 1,2;


-- multi-usage
explain verbose with s1(b, cc) as (select b, sum(c) from t1 group by b) select count(*) from s1 where cc > (select sum(cc)/100 from s1) order by 1;

-- pull up
explain verbose with s as (select b, sum(c) as cc from t1 group by b) select a, b, c, d from t where b not in (select sum(cc) from s group by b) order by 1,2,3,4;

-- not pull up
explain verbose with s as (select b, sum(c) as cc from t1 group by b) select 1 from t where exists (select b, sum(cc) from s where s.b=t.b group by b);

set enable_seqscan_fusion = off;

-- delete data and shema
------------------------------------------------------------------------------------

drop schema lazyagg cascade;
