create schema inlist2join;
set current_schema=inlist2join;
set qrw_inlist2join_optmode=1;
create table t1(c1 int, c2 int, c3 int) distribute by hash(c1);
insert into t1 select v,v,v from generate_series(1,12) as v;
create table t2(c1 int, c2 int, c3 int) distribute by hash(c1);
insert into t2 select v,v,v from generate_series(1,10) as v;

explain (costs off) select ctid,  xmin, cmin, xmax, cmax, tableoid, xc_node_id from t1 where t1.c1 in (1,2,3);

explain (costs off) select c1,c2 from t1 where t1.c2 in (3,4,7);
select c1,c2 from t1 where t1.c2 in (3,4,7) order by 1;

explain (costs off) select c1,c3 from t1 where t1.c2 in (3,4,7);
select c1,c3 from t1 where t1.c3 in (3,4,7) order by 1;

explain (costs off) select c2,c3 from t1 where t1.c2 in (3,4,7);
select c2,c3 from t1 where t1.c3 in (3,4,7) order by 1;

explain (costs off) select t1.c1 as c1, t2.c2 as c2
from t1,t2
where t1.c1 = t2.c1 and t1.c1 in (3,4,5,6)
order by 1,2;

select t1.c1 as c1, t2.c2 as c2
from t1,t2
where t1.c1 = t2.c1 and t1.c1 in (3,4,5,6)
order by 1,2;

explain (costs off) select * from
(
    select t1.c1 as c1, t2.c2 as c2, count(*) as sum
    from t1,t2
    where t1.c1 = t2.c1
    group by 1,2
) as dt where dt.c1 in (3,4,5,6)
order by 1,2,3;

select * from
(
    select t1.c1 as c1, t2.c2 as c2, count(*) as sum
    from t1,t2
    where t1.c1 = t2.c1
    group by 1,2
) as dt where dt.c1 in (3,4,5,6)
order by 1,2,3;

explain (costs off) select t1.c2, count(*) from t1 where t1.c1 in (1,2,3) group by t1.c2 having t1.c2 in (1,3) order by 1;
select t1.c2, count(*) from t1 where t1.c1 in (1,2,3) group by t1.c2 having t1.c2 in (1,3) order by 1;

explain (costs off) select t1.c3, t1.c2 from t1 where t1.c1 > 1 AND t1.c2 in (select t2.c2 from t2 where t2.c1 IN (3,4,5,6,7)) order by 1,2;
select t1.c3, t1.c2 from t1 where t1.c1 > 1 AND t1.c2 in (select t2.c2 from t2 where t2.c1 IN (3,4,5,6,7)) order by 1,2;

select t1.c2, sum(t1.c3) from t1 where t1.c1 in (1,2,3) group by t1.c2 order by t1.c2 limit 2;
select max(t1.c2) from t1 where t1.c1 < 4 and t1.c1 in (2,3,4);
select t1.c2, t2.c3, count(*) from t1,t2 where t1.c1 = t2.c2 and t1.c1 in (1,2,3) group by t1.c2,t2.c3 order by t1.c2;
select t1.c2, t2.c3, sum(t1.c3) from t1,t2 where t1.c1 = t2.c2 and t1.c1 in (1,2,3) group by t1.c2,t2.c3 order by t1.c2 limit 2;

select * from (select * from t1 where t1.c1 in (1,4,11) union all select * from t2) as dt order by 1;
select * from (select * from t1 union all select * from t2 where t2.c1 in (1,4,11)) as dt order by 1;
select * from (select * from t1 where t1.c1 in (1,4,11) union all select * from t2 where t2.c1 in (2,3)) as dt order by 1;

--测试expression inlist场景
-- allow for inlist2join
explain (costs off) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + 2) in (1,2,3,4,5) order by 1;
select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + 2) in (1,2,3,4,5) order by 1;

explain (costs off) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c2 * 2) in (1,2,3,4,5) order by 1;
select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c2 *2) in (1,2,3,4,5) order by 1;

-- disallow for inlistjoin
explain (costs off) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + t1.c2) in (1,2,3,4,5) order by 1;
select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + t2.c2) in (1,2,3,4,5) order by 1;

--不支持的场景INLIST对CTE,VALUES,UPDATE,FUNC的列做过滤条件
-- ValueScan not supported to inlist2join
explain (costs off) select * from (values(1),(2),(3)) as v (col1) where v.col1 in (1,2,3,4);
select * from (values(1),(2),(3)) as v (col1) where v.col1 in (1,2,3,4);

-- CTE not support to inlist2join
explain (costs off) with q1(x) as (select random() from generate_series(1, 5))
select * from q1 where q1.x in (1,2,3);
with q1(x) as (select random() from generate_series(1, 5))
select * from q1 where q1.x in (1,2,3);

-- FUNC not supported to inlist2join
explain (costs off) select * from unnest(ARRAY[1,2,3,4,5,6,7]) as t1(c1) where t1.c1 in (1,2,3,4);
select * from unnest(ARRAY[1,2,3,4,5,6,7]) as t1(c1) where t1.c1 in (1,2,3,4);

--测试用例用来看护ERROR:  could not find attribute 2 in subquery targetlist(SubLink pullup)
explain  (costs off) select t1.c1 from t1, t2 where t1.c1=t2.c1 and t1.c1 in (1,2,3,4,5,6,7) and t1.c2 =1;
select t1.c1 from t1, t2 where t1.c1=t2.c1 and t1.c1 in (1,2,3,4,5,6,7) and t1.c2 =1;
explain  (costs off) select t1.c1 from t2, t1 where t1.c1=t2.c1 and t1.c1 in (1,2,3,4,5,6,7) and t1.c2 =1;
select t1.c1 from t2, t1 where t1.c1=t2.c1 and t1.c1 in (1,2,3,4,5,6,7) and t1.c2 =1;

-- UPDATE not support to inlist2join
explain (costs off) update t1 set c2 = c2 * 10 where t1.c1 in (select t2.c2 from t2 where t2.c1 in (1,2,3));
update t1 set c2 = c2 * 10 where t1.c1 in (select t2.c2 from t2 where t2.c1 in (1,2,3));
select * from t1 order by 1,2,3;

-- confirm =ALL will not apply inlist2join conversion
explain  (costs off) select c1 from t1 where c1= all(array[1,2]);
select c1 from t1 where c1= all(array[1,2]);

-- test extended rule
set qrw_inlist2join_optmode=4;
explain  (costs off) select c1 from t1 where c1= any(array[1,2]);
select c1 from t1 where c1= all(array[1,2]);

explain (costs off) select c1,c3 from t1 where t1.c2 in (3,4,7);
select c1,c3 from t1 where t1.c3 in (3,4,7) order by 1;

set qrw_inlist2join_optmode=2;
explain  (costs off) select c1 from t1 where c1= any(array[1,2]);
select c1 from t1 where c1= all(array[1,2]);

explain (costs off) select c1,c3 from t1 where t1.c2 in (3,4,7);
select c1,c3 from t1 where t1.c3 in (3,4,7) order by 1;
explain (costs off)  select c1,c3 from t1 where t1.c3 in (3,4,7) order by 1;

--
-- forbid inlist2join query rewrite if there's subplan
set qrw_inlist2join_optmode=1;
create table t3(c1 int, c2 int, c3 int) distribute by hash(c1);
insert into t3 select v,v,v from generate_series(1,10) as v;

-- the sublink can be pull up, so there is no subpaln, do inlist2join
explain (costs off) select * from t1 where t1.c1 in (select c1 from t2 where t2.c2 > 2) AND t1.c2 in (1,2,3);
explain (costs off) select * from t1 where exists (select c1 from t2 where t2.c2 = t1.c1) and t1.c1 in (1,2,3);
explain (costs off) select * from t1 join t2 on t1.c1=t2.c1 and t1.c1 not in (select c2 from t3 where t3.c2>2) and t1.c3 in (1,2,3);

-- the sublink can not be pull up, so there is subpaln, do not inlist2join
explain (costs off) select * from t1 where t1.c1 in (select c1 from t2 where t2.c2 = t1.c2) AND t1.c2 in (1,2,3);
explain (costs off) select * from t1 where exists (select c1 from t2 where t2.c2 > 2) and t1.c1 in (1,2,3);
explain (costs off) select * from t1 join t2 on t1.c1=t2.c1 and t1.c1 not in (select c2 from t3 where t3.c2=t1.c2) and t1.c3 in (1,2,3);
--

---subquery when the qual cannot be push down, except/lilimit/window function
explain (costs off) select c1 from(
select t1.c1,t1.c2 from t1
except
select t2.c1,t2.c2 from t2
) where c1 in (1,2,3) order by 1;
select c1 from(
select t1.c1,t1.c2 from t1
except
select t2.c1,t2.c2 from t2
) where c1 in (1,2,3) order by 1;

explain (costs off)select c1 from(
select t1.c1,t1.c2 from t1
except
select t2.c1,t2.c2 from t2 where t2.c2 in (3,4,5)
) where c1 in (1,2,3) order by 1;
select c1 from(
select t1.c1,t1.c2 from t1
except
select t2.c1,t2.c2 from t2 where t2.c2 in (3,4,5)
) where c1 in (1,2,3) order by 1;

explain (costs off)select c1 from(
select t1.c1,t1.c2 from t1
intersect
select t2.c1,t2.c2 from t2 where t2.c2 in (3,4,5) order by 1,2 limit 2
) where c1 in (1,2,3) order by 1;
select c1 from(
select t1.c1,t1.c2 from t1
intersect
select t2.c1,t2.c2 from t2 where t2.c2 in (3,4,5) order by 1,2 limit 2
) where c1 in (1,2,3) order by 1;

explain (costs off)select * from (
select * from t1 where t1.c1 in (1,4,11) union all select * from t2 order by 1,2,3 limit 3
) as dt where dt.c2 in (1,2,3) order by 1;
select * from (
select * from t1 where t1.c1 in (1,4,11) union all select * from t2 order by 1,2,3 limit 3
) as dt where dt.c2 in (1,2,3) order by 1;

explain (costs off)select * from (
select * from t1 union all select * from t2 where t2.c1 in (1,4,11) order by 1,2,3 limit 4
) as dt where dt.c3 in (1,2,3) order by 1;
select * from (
select * from t1 union all select * from t2 where t2.c1 in (1,4,11) order by 1,2,3 limit 4
) as dt where dt.c3 in (1,2,3) order by 1;


CREATE TABLE empsalary (
    depname varchar,
    empno bigint,
    salary int,
    enroll_date date
);

INSERT INTO empsalary VALUES
('develop', 10, 5200, '2007-08-01'),
('sales', 1, 5000, '2006-10-01'),
('personnel', 5, 3500, '2007-12-10'),
('sales', 4, 4800, '2007-08-08'),
('personnel', 2, 3900, '2006-12-23'),
('develop', 7, 4200, '2008-01-01'),
('develop', 9, 4500, '2008-01-01'),
('sales', 3, 4800, '2007-08-01'),
('develop', 8, 6000, '2006-10-01'),
('develop', 11, 5200, '2007-08-15');
explain (costs off)SELECT depname from (
	SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary where empno in (1,2,3) WINDOW w AS (PARTITION BY depname) ORDER BY empno,salary
) where empno in (1,3) order by 1;
SELECT depname from (
	SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary where empno in (1,2,3) WINDOW w AS (PARTITION BY depname) ORDER BY empno,salary
) where empno in (1,3) order by 1;
drop table empsalary;
drop table t1;
drop table t2;
drop table t3;

create table t1 (a int2, b int4, c int8) distribute by hash(a);
create table t2 (a int2, b int4, c int8) distribute by hash(a);
create table t3 (a int2, b int4, c int8) distribute by hash(a);
select * from t1 inner join t2 on t1.b = 2*t2.b inner join t3 on t2.b = 2*t3.b where t1.b in (8, 84, 80, 24, 48) order by 1, 2, 3, 4, 5, 6, 7, 8, 9;
drop table t1;
drop table t2;
drop table t3;

/*其他问题*/
--1.
create table t_pruning_datatype_int32(c1 int,c2 int,c3 int,c4 text) distribute by hash(c1)
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));

insert into t_pruning_datatype_int32 values(-100,20,20,'a'),
                     (100,300,300,'bb'),
                     (150,75,500,NULL),
                     (200,500,50,'ccc'),
                     (230,50,50,'111'),
                     (330,700,125,'222'),
                     (450,35,150,'dddd');

select * from t_pruning_datatype_int32 where t_pruning_datatype_int32.c1 IN (230,330,350) order by 1,2,3;
drop table t_pruning_datatype_int32;

--2. targetlist中包含func，遗漏fix-vars
select hashname(node_name) = node_id from pgxc_node WHERE node_name IN ('coordinator1',  'datanode');
--3. 解决 no relation entry for relid 1问题. 根因 alternative_rel里面对restrictinfo是浅拷贝导致后续在
-- rebuild_subquery时修正restrictinfo varno -> 1是错误的修改到base_rel的varno
select * from
(
    SELECT c.relname
    FROM pg_class c JOIN pg_namespace nc ON c.relnamespace = nc.oid
    WHERE NOT nc.oid = 1327 AND c.relkind = ANY (ARRAY['r'::"char", 'v'::"char"])
) as dt order by dt.relname limit 1;

create table t(col_bigint bigint) distribute by hash(col_bigint);
insert into t values(256);
insert into t values(-128);
insert into t values(5879);
insert into t values(1000);
insert into t values(2000);
select col_bigint from t where col_bigint = any(array[-128, 256, 5879]) order by 1;
drop table t;

--看护两个以上的inlist其中存在不能强转的场景
create table t10(c1 int, c2 int, c3 varchar, c4 nvarchar2) distribute by hash (c1);
insert into t10 select v,v,v,v from generate_series(1,10) as v;

explain (costs off) select * from t10 where cast(c1 as numeric) in (1,2,3,4,5) and c2 in (5,6,7,8) order by 1;
select * from t10 where cast(c1 as numeric) in (1,2,3,4,5) and c2 in (5,6,7,8) order by 1;

explain (costs off) select * from t10 where (c1,c2) in ((1,1), (2,2),(3,3)) order by 1,2;
select * from t10 where (c1,c2) in ((1,1), (2,2),(3,3)) order by 1,2;

-- 测试存在隐式类型转换的场景
explain (costs off) select * from t10 where c4 in ('3','4','5','6','7') order by 1;
select * from t10 where c4 in ('3','4','5','6','7') order by 1;

drop table t10;

-- column in ANY, args0 is const, args1 is a var
SELECT p1.oid, p1.proname FROM pg_proc as p1 WHERE p1.prorettype = 'internal'::regtype AND 'internal'::regtype = ANY (p1.proargtypes) limit 3;
-- no var in scalararray
SELECT p1.oid, p1.proname FROM pg_proc as p1 WHERE p1.prorettype = 'internal'::regtype AND 12 = ANY (array[2275,1245,2588]);

-- sort pathkey not found issue
set enable_hashjoin=off;
set enable_nestloop=off;

SELECT c.relname
FROM pg_catalog.pg_class c,
	 pg_catalog.pg_index i,
	 pg_catalog.pg_constraint con
WHERE c.oid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x') AND c.oid = '29438';

-- the subquery is dummy rel --
create  node group group_11 with (datanode1,datanode2);
create table associate_benefit_expense
(
    period_end_dt date not null ,
    associate_expns_type_cd varchar(50) not null ,
    associate_party_id integer not null ,
    benefit_hours_qty decimal(18,11) ,
    benefit_cost_amt number(18,7)
)  distribute by replication
partition by range (associate_expns_type_cd )
(
partition associate_benefit_expense_1 values less than ('B'),
partition associate_benefit_expense_2 values less than ('E'),
partition associate_benefit_expense_3 values less than ('G'),
partition associate_benefit_expense_4 values less than ('I'),
partition associate_benefit_expense_5 values less than ('L'),
partition associate_benefit_expense_6 values less than ('N'),
partition associate_benefit_expense_7 values less than ('P'),
partition associate_benefit_expense_8 values less than ('Q'),
partition associate_benefit_expense_9 values less than ('R'),
partition associate_benefit_expense_10 values less than ('T'),
partition associate_benefit_expense_11 values less than ('U'),
partition associate_benefit_expense_12 values less than ('V'),
partition associate_benefit_expense_13 values less than (maxvalue)
);
set qrw_inlist2join_optmode=rule_base;
set constraint_exclusion=on;
SELECT *
    FROM
         (SELECT
                 Table_012.Column_027 Column_035
            FROM associate_benefit_expense Table_004
           INNER JOIN (SELECT Table_004.associate_party_id Column_026,
                             Table_004.benefit_cost_amt  Column_027
                        FROM associate_benefit_expense Table_004
                        ) Table_012
              ON (Table_012.Column_026) IS NULL) Table_013
   WHERE (Table_013.Column_035) IN (212, 4, 103);
explain (costs off)
SELECT *
    FROM
         (SELECT
                 Table_012.Column_027 Column_035
            FROM associate_benefit_expense Table_004
           INNER JOIN (SELECT Table_004.associate_party_id Column_026,
                             Table_004.benefit_cost_amt Column_027
                        FROM associate_benefit_expense Table_004
                        ) Table_012
              ON (Table_012.Column_026) IS NULL) Table_013
   WHERE (Table_013.Column_035) IN (212, 4, 103);
drop table associate_benefit_expense;
drop node group group_11;

-- test for replication table --

create table t1(c1 int, c2 int, c3 int) with (orientation=row)
distribute by replication;
insert into t1 select v,v,v from generate_series(1,12) as v;
create table t2(c1 int, c2 int, c3 int) with (orientation=column)
distribute by hash(c3);
insert into t2 select v,v,v from generate_series(1,10) as v;

SELECT t1.c1
FROM t1 left outer join t2
on t1.c3 = t2.c3
WHERE t1.c1 IN (1,3,5)
order by 1;
explain (costs off) SELECT t1.c1
FROM t1 left outer join t2
on t1.c3 = t2.c3
WHERE t1.c1 IN (1,3,5)
order by 1;
drop table t1;
drop table t2;

drop schema inlist2join cascade;
