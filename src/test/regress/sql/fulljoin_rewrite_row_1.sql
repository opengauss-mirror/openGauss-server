create schema full_join_on_true_row_1;
set current_schema='full_join_on_true_row_1';

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;

create table t1(a1 int , b1 int, c1 int, samename int);
create table t2(a2 int , b2 int, c2 int, samename int);
create table t3(a3 int , b3 int, c3 int, samename int);
create table t4(a4 int , b4 int, c4 int, samename int);

--------------------------------
-- When the condition is 1=1, GaussDB can noly create Nestloop plan
--------------------------------

-- Both sides are empty
select *from t1 full join t2 on 1=1;
select *from t1 full join t2 on 1=1 full join t3 on 1=1;
select *from t1 full join t2 on 1=1 full join t3 on 1=1 full join t4 on 1=1;
explain (costs off) select *from t1 full join t2 on 1=1;
explain (costs off) select *from t1 full join t2 on 1=1 full join t3 on 1=1;
explain (costs off) select *from t1 full join t2 on 1=1 full join t3 on 1=1 full join t4 on 1=1;
explain (verbose,costs off) select *from t1 full join t2 on 1=1;
explain (verbose,costs off) select *from t1 full join t2 on 1=1 full join t3 on 1=1;
explain (verbose,costs off) select *from t1 full join t2 on 1=1 full join t3 on 1=1 full join t4 on 1=1;

-- One side is empty
insert into t1 values(1,1,1,1);
insert into t1 values(11,11,11,11);
insert into t1 values(11,11,11,11);
insert into t1 values(111,111,111,111);
insert into t1 values(111,111,111,111);
insert into t1 values(111,111,111,111);
insert into t1 values(NULL,1111,1111,1111);

select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 order by 1,2,3,4,5,6;
select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6,7,8,9;
select a1,b1,c1,a2,b2,c2,a3,b3,c3,a4,b4,c4 from t1 full join t2 on 1=1 full join t3 on 1=1 full join t4 on 1=1 order by 1,2,3,4,5,6,7,8,9,10,11,12;

-- Neither side is empty

insert into t2 values(2,2,2,2);
insert into t2 values(22,22,22,22);
insert into t2 values(22,22,22,22);
insert into t2 values(NULL,222,222,222);

select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 order by 1,2,3,4,5,6;
select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6,7,8,9;
select a1,b1,c1,a2,b2,c2,a3,b3,c3,a4,b4,c4 from t1 full join t2 on 1=1 full join t3 on 1=1 full join t4 on 1=1 order by 1,2,3,4,5,6,7,8,9,10,11,12;

-- Quals on base table
select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where t1.a1=11 order by 1,2,3,4,5,6;
select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where t1.a1=11 or t1.a1 is null order by 1,2,3,4,5,6;
select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where (t1.a1=11 or t1.a1 is null) and (t2.a2>1 or t2.a2 is null) order by 1,2,3,4,5,6;
explain (costs off) select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where t1.a1=11 order by 1,2,3,4,5,6;
explain (costs off) select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where t1.a1=11 or t1.a1 is null order by 1,2,3,4,5,6;
explain (costs off) select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where (t1.a1=11 or t1.a1 is null) and (t2.a2>1 or t2.a2 is null) order by 1,2,3,4,5,6;
explain (verbose,costs off) select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where t1.a1=11 order by 1,2,3,4,5,6;
explain (verbose,costs off) select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where t1.a1=11 or t1.a1 is null order by 1,2,3,4,5,6;
explain (verbose,costs off) select a1,b1,c1,a2,b2,c2 from t1 full join t2 on 1=1 where (t1.a1=11 or t1.a1 is null) and (t2.a2>1 or t2.a2 is null) order by 1,2,3,4,5,6;

explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3  from t1, t2 full join t3 on 1=1 where t1.a1=11 order by 1,2,3,4,5,6,7,8,9;
explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3  from t1, t2 full join t3 on 1=1 where t1.a1=11 and t2.a2=22 order by 1,2,3,4,5,6,7,8,9;
explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3  from t1, t2 full join t3 on 1=1 where t1.a1=11 and (t2.a2=22 or t2.a2 is null) order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3  from t1, t2 full join t3 on 1=1 where t1.a1=11 order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3  from t1, t2 full join t3 on 1=1 where t1.a1=11 and t2.a2=22 order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3  from t1, t2 full join t3 on 1=1 where t1.a1=11 and (t2.a2=22 or t2.a2 is null) order by 1,2,3,4,5,6,7,8,9;

-- Quals on JoinExpr

select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and sub.a2=22 order by 1,2,3,4,5,6,7,8,9;
select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and (sub.a2=22 or sub.a2 is null) order by 1,2,3,4,5,6,7,8,9;
select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and (sub.a2>t1.a1 or sub.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;
select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and ( t1.a1>sub.a2 and sub.a2>t1.a1 or sub.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;
select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, t2 full join t3 on 1=1  where t1.a1=11 and (t2.a2>t1.a1 or t2.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;

explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and sub.a2=22 order by 1,2,3,4,5,6,7,8,9;
explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and (sub.a2=22 or sub.a2 is null) order by 1,2,3,4,5,6,7,8,9;
explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and (sub.a2>t1.a1 or sub.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;
explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and ( t1.a1>sub.a2 and sub.a2>t1.a1 or sub.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;
explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, t2 full join t3 on 1=1  where t1.a1=11 and (t2.a2>t1.a1 or t2.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;

explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and sub.a2=22 order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and (sub.a2=22 or sub.a2 is null) order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and (sub.a2>t1.a1 or sub.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (t2 full join t3 on 1=1) sub where t1.a1=11 and ( t1.a1>sub.a2 and sub.a2>t1.a1 or sub.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, t2 full join t3 on 1=1  where t1.a1=11 and (t2.a2>t1.a1 or t2.a2 is null or t1.a1 is null) order by 1,2,3,4,5,6,7,8,9;

-- Subquery  on 1=1
select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null order by 1,2,3,4,5,6,7,8,9;
select a1,b1,c1 from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) order by 1,2,3;
explain  (costs off ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null order by 1,2,3,4,5,6,7,8,9;
explain  (costs off ) select a1,b1,c1 from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) order by 1,2,3;
explain  (costs off, verbose ) select a1,b1,c1,a2,b2,c2,a3,b3,c3 from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null order by 1,2,3,4,5,6,7,8,9;
explain  (costs off, verbose ) select a1,b1,c1 from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) order by 1,2,3;

-- group by

select a1,sum(a2) from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null group by a1 order by 1,2;
select sum(a1) from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) group by a1 order by 1;
select a1,sum(a2) suma2 from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null group by a1 order by a1,suma2;
select sum(a1) suma1 from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) group by a1 order by suma1;

explain  (costs off ) select a1,sum(a2) from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null group by a1 order by 1,2;
explain  (costs off ) select sum(a1) from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) group by a1 order by 1;
explain  (costs off ) select a1,sum(a2) suma2 from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null group by a1 order by a1,suma2;
explain  (costs off ) select sum(a1) suma2 from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) group by a1 order by a1,suma2;


explain  (costs off, verbose ) select a1,sum(a2) from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null group by a1 order by 1,2;
explain  (costs off, verbose ) select sum(a1) from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) group by a1 order by 1;
explain  (costs off, verbose ) select a1,sum(a2) suma2 from t1, (select *from t2 full join t3 on 1=1 where (t2.a2>1 or t2.a2 is null)) sub where t1.a1>sub.a2 or sub.a2 is null group by a1 order by a1,suma2;
explain  (costs off, verbose ) select sum(a1) suma2 from t1 where exists (select *from t2 full join t3 on 1=1 where (t2.a2>t1.a1 or t2.a2 is null)) group by a1 order by a1,suma2;

-- Type not distributable

drop table if exists cidr1;
drop table if exists cidr2;

create table cidr1(a1 cidr, b1 cidr, d int);
create table cidr2(a2 cidr, b2 cidr, d int);
insert into cidr2 values('192.168.0.0/16', '192.168.0.0/16', 1);
insert into cidr2 values('100.100.0.0/16', '100.100.0.0/16', 1);
insert into cidr2 values('100.100.0.0/16', '100.100.0.0/16', 1);
insert into cidr2 values('2.2.0.0/16', '2.2.0.0/16', 1);
-- Nestloop
set enable_nestloop=on;
set enable_hashjoin=off;
set enable_mergejoin=off;
select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

explain (verbose,costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (verbose,costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (verbose,costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

-- Hashjoin
set enable_nestloop=off;
set enable_hashjoin=on;
set enable_mergejoin=off;
select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

explain (verbose,costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (verbose,costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (verbose,costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

-- Merge join
set enable_nestloop=off;
set enable_hashjoin=off;
set enable_mergejoin=on;
select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

explain (verbose,costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (verbose,costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (verbose,costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

insert into cidr1 values('192.168.0.0/16', '192.168.0.0/16', 1);
insert into cidr1 values('100.100.0.0/16', '100.100.0.0/16', 1);
insert into cidr1 values('100.100.0.0/16', '100.100.0.0/16', 1);
insert into cidr1 values('1.1.0.0/16', '1.1.0.0/16', 1);

set enable_nestloop=on;
set enable_hashjoin=off;
set enable_mergejoin=off;
select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

explain (verbose,costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (verbose,costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (verbose,costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

set enable_nestloop=off;
set enable_hashjoin=on;
set enable_mergejoin=off;
select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (verbose,costs off) select *from cidr1 full join cidr2 on a1=a2;
explain (verbose,costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (verbose,costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;

set enable_nestloop=off;
set enable_hashjoin=off;
set enable_mergejoin=on;
select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;
explain (verbose,costs off) select a1,b1,a2,b2 from cidr1 full join cidr2 on a1=a2 order by 1,2,3,4;
explain (verbose,costs off) select a1, sum(cidr2.d) from cidr1 full join cidr2 on a1=a2 group by a1 order by 1,2;
explain (verbose,costs off) select a1, sum(cidr2.d) sumd from cidr1 full join cidr2 on a1=a2 group by a1 order by a1,sumd;


drop table if exists t1;
drop table if exists t;


create table t(a float, b float, c float, d int);
create table t1(a float, b float, c float, d int);

insert into t values (-1.1, -1.1, -1.1, -1);
insert into t values (-2.2, -2.2, -2.2, -2);
insert into t values (-2.2, -2.2, -2.2, -2);
insert into t values (1.1, 1.1, 1.1, 1);
insert into t values (2.2, 2.2, 2.2, 2);
insert into t values (2.2, 2.2, 2.2, 2);

insert into t1 values (-10.1, -10.1, -10.1, -10);
insert into t1 values (-20.2, -20.2, -20.2, -20);
insert into t1 values (-20.2, -20.2, -20.2, -20);
insert into t1 values (1.1, 1.1, 1.1, 1);
insert into t1 values (2.2, 2.2, 2.2, 2);
insert into t1 values (2.2, 2.2, 2.2, 2);

set enable_nestloop=on;
set enable_hashjoin=off;
set enable_mergejoin=off;

explain (verbose,costs off) select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;


set enable_nestloop=off;
set enable_hashjoin=on;
set enable_mergejoin=off;

explain (verbose,costs off) select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;

set enable_nestloop=off;
set enable_hashjoin=off;
set enable_mergejoin=on;

explain (verbose,costs off) select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;
select t.b, sum(cc) from t full join (select b, count(c) as cc from t1 group by b) s1 on s1.b=t.b group by t.b order by 1,2;


drop schema full_join_on_true_row_1 CASCADE;