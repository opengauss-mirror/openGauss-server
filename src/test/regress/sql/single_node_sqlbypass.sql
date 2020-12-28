set enable_seqscan=off;
set enable_bitmapscan=off;
set enable_material=off;
set enable_beta_opfusion=on;
drop table if exists t1;
create table t1 (c1 int, c2 numeric, c3 numeric, c4 int, colreal real);
create table t2 (c1 int, c2 numeric, c3 numeric, c4 int, colreal real);
create index idx1 on t1(c2);
create index idx2 on t1(c3);
insert into t1 select generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000);
insert into t1 select generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000);
insert into t1 select generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000);
insert into t1 values (1,2,3,5,0),(1,2,3,6,0),(1,3,2,7,0),(1,3,2,8,0);
insert into t2 select * from t1;
create index on t2(c2);
analyze t1;
analyze t2;
explain (verbose on, costs off) select sum(c1) from t1 group by c2;
explain (verbose on, costs off) select count(c1) from t1 where c2=1;
explain (verbose on, costs off) select sum(colreal) from t1 where c2=1;
explain (verbose on, costs off) select sum(c1) as result from t1 where c2=1 having result !=10;
explain (verbose on, costs off) select sum(c1), sum(c2) from t1 where c3 = 1;
explain (verbose on, costs off) select sum(c1)+1 from t1 where c2=1;
explain (verbose on, costs off) select sum(c1+1) from t1 where c2=1;
explain (verbose on, costs off) select sum(c1) from t1 where c2=1 limit 1;

-- agg fusion
drop index idx2;
-- index t1(c2): indexonlyscan
explain (verbose on, costs off) select sum(c2) from t1 where c2=3;
select sum(c2) from t1 where c2=3;

-- index t1(c2): indexscan
explain (verbose on, costs off) select sum(c3) from t1 where c2=3;
select sum(c3) from t1 where c2=3;

-- index t1(c3, c2): indexonlyscan
drop index idx1;
create index idx3 on t1(c3, c2);
explain (verbose on, costs off) select sum(c3) from t1 where c3=3;
explain (verbose on, costs off) select sum(c2) from t1 where c3=3;
explain (verbose on, costs off) select sum(c3) from t1 where c2=3;
explain (verbose on, costs off) select sum(c2) from t1 where c2=3;

select sum(c3) from t1 where c3=3;
select sum(c2) from t1 where c3=3;
select sum(c3) from t1 where c2=3;
select sum(c2) from t1 where c2=3;

-- sort fusion
explain (verbose on, costs off) select c3 from t1 where c3 < 10 order by c2;
select c3 from t1 where c3 < 10 order by c2;

-- nestloop fusion
drop table if exists tn1, tn2;
create table tn1(c1 int, c2 int, c3 int);
create table tn2(c1 int, c2 int, c3 int);
insert into tn1 select generate_series(20, 100000), generate_series(20, 100000), generate_series(20, 100000);
insert into tn2 select * from tn1;
insert into tn1 values (1,2,3),(4,5,6),(7,8,9);
insert into tn2 values (11,12,13),(14,15,16),(17,18,19);
create index on tn1(c2);
create index on tn2(c2);
explain (verbose on, costs off) select tn1.c3, tn2.c3 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;
explain (verbose on, costs off) select tn2.c3, tn1.c3 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;
explain (verbose on, costs off) select tn1.c1, tn2.c1 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;
explain (verbose on, costs off) select tn2.c1, tn1.c1 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;
select tn1.c3, tn2.c3 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;
select tn2.c3, tn1.c3 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;
select tn1.c1, tn2.c1 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;
select tn2.c1, tn1.c1 from tn1,tn2 where tn1.c2 <20 and tn2.c2 <20;

drop table if exists t1, t2;
drop table if exists tn1, tn2;
reset enable_seqscan;
reset enable_bitmapscan;
reset enable_material;
reset enable_beta_opfusion;
