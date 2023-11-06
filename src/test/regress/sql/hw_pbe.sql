--
-- HW_PBE
--

-- Testset 1 dynamic datanode reduction for single table
create table t1_xc_fqs(id1 int, id2 int, num int);

-- only params
prepare s as select * from t1_xc_fqs where id1=$1 and id2=$2;
prepare i as insert into t1_xc_fqs values ($1, $2, $3);
prepare u as update t1_xc_fqs set num=0 where id1=$1 and id2=$2;
prepare d as delete from t1_xc_fqs where id1=$1 and id2=$2;

set enable_pbe_optimization to false;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1,1);
explain (costs off, verbose on) execute i (6,6,6);
explain (costs off, verbose on) execute u (2,2);
explain (costs off, verbose on) execute d (3,3);
execute s (1,1);
execute i (6,6,6);
select * from t1_xc_fqs order by id1;
execute u (2,2);
select * from t1_xc_fqs order by id1;
execute d (3,3);
select * from t1_xc_fqs order by id1;

set enable_pbe_optimization to true;
truncate t1_xc_fqs;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1,1);
explain (costs off, verbose on) execute i (6,6,6);
explain (costs off, verbose on) execute u (2,2);
explain (costs off, verbose on) execute d (3,3);
execute s (1,1);
execute i (6,6,6);
select * from t1_xc_fqs order by id1;
execute u (2,2);
select * from t1_xc_fqs order by id1;
execute d (3,3);
select * from t1_xc_fqs order by id1;

deallocate s;
deallocate i;
deallocate u;
deallocate d;

-- const and params
prepare s as select * from t1_xc_fqs where id1=$1 and id2=2;
prepare i as insert into t1_xc_fqs values ($1, 2, 3);
prepare u as update t1_xc_fqs set num=1 where id1=$1 and id2=2;
prepare d as delete from t1_xc_fqs where id1=$1 and id2=2;

set enable_pbe_optimization to false;
truncate t1_xc_fqs;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1);
explain (costs off, verbose on) execute i (6);
explain (costs off, verbose on) execute u (2);
explain (costs off, verbose on) execute d (3);
execute s (1);
execute i (6);
select * from t1_xc_fqs order by id1;
execute u (2);
select * from t1_xc_fqs order by id1;
execute d (3);
select * from t1_xc_fqs order by id1, id2;

set enable_pbe_optimization to true;
truncate t1_xc_fqs;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1);
explain (costs off, verbose on) execute i (6);
explain (costs off, verbose on) execute u (2);
explain (costs off, verbose on) execute d (3);
execute s (1);
execute i (6);
select * from t1_xc_fqs order by id1;
execute u (2);
select * from t1_xc_fqs order by id1;
execute d (3);
select * from t1_xc_fqs order by id1, id2;

deallocate s;
deallocate i;
deallocate u;
deallocate d;
drop table t1_xc_fqs;


-- Testset 2 dynamic datanode reduction for multi-table join
create table t1_xc_fqs(id1 int, id2 int, num int);
insert into t1_xc_fqs values (1,1,11), (2,2,21), (3,3,31), (4,4,41), (5,5,51);
create table t2_xc_fqs(id1 int, id2 int, num int);
insert into t2_xc_fqs values (1,2,12), (2,3,22), (3,4,32), (4,5,42), (5,6,52);
create table t3_xc_fqs(id11 int, id22 int, num int);
insert into t3_xc_fqs values (1,13,13), (2,23,23), (3,33,33), (4,43,43), (5,53,53);

-- implicit join
prepare s0 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=$1 and t2.id1=$2;
prepare s1 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=$1 and t2.id1=$2 and t3.id11=$3;
prepare s2 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=t2.id1 and t1.id1=$1;
prepare s3 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=t2.id1 and t3.id11=$1;
prepare s4 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=t2.id1 and t1.id1=t3.id11 and t3.id11=$1;
prepare s5 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=t2.id2 and t2.id1=$1 and t2.id2=$2;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

deallocate s0;
deallocate s1;
deallocate s2;
deallocate s3;
deallocate s4;
deallocate s5;

-- explicit join
prepare s0 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=$1 and t2.id1=$2;
prepare s1 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=$1 and t2.id1=$2 join t3_xc_fqs t3 on t3.id11=$3;
prepare s2 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1;
prepare s3 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 join t3_xc_fqs t3 on t3.id11=$1;
prepare s4 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 join t3_xc_fqs t3 on t1.id1=t3.id11 and t3.id11=$1;
prepare s5 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id2 where t2.id1=$1 and t2.id2=$2;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

deallocate s0;
deallocate s1;
deallocate s2;
deallocate s3;
deallocate s4;
deallocate s5;

-- outer join
prepare s0 as select * from t1_xc_fqs t1 left join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1 order by t1.id1;
prepare s1 as select * from t1_xc_fqs t1 right join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1 order by t2.id1;
prepare s2 as select * from t1_xc_fqs t1 full join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1 order by t1.id1, t2.id1;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1);
explain (costs off, verbose on) execute s1 (1);
explain (costs off, verbose on) execute s2 (1);
execute s0 (1);
execute s1 (1);
execute s2 (1);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1);
explain (costs off, verbose on) execute s1 (1);
explain (costs off, verbose on) execute s2 (1);
execute s0 (1);
execute s1 (1);
execute s2 (1);

deallocate s0;
deallocate s1;
deallocate s2;

prepare s0 as select * from t1_xc_fqs t1 left join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1 order by t1.id1;
prepare s1 as select * from t1_xc_fqs t1 right join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1 order by t2.id1;
prepare s2 as select * from t1_xc_fqs t1 full join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1 order by t1.id1, t2.id1;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1);
explain (costs off, verbose on) execute s1 (1);
explain (costs off, verbose on) execute s2 (1);
execute s0 (1);
execute s1 (1);
execute s2 (1);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1);
explain (costs off, verbose on) execute s1 (1);
explain (costs off, verbose on) execute s2 (1);
execute s0 (1);
execute s1 (1);
execute s2 (1);

deallocate s0;
deallocate s1;
deallocate s2;
drop table t1_xc_fqs;
drop table t2_xc_fqs;
drop table t3_xc_fqs;


-- Testset 3 dynamic datanode reduction for single table (column)
create table t1_xc_fqs(id1 int, id2 int, num int) with (orientation = column);

-- only params
prepare s as select * from t1_xc_fqs where id1=$1 and id2=$2;
prepare i as insert into t1_xc_fqs values ($1, $2, $3);
prepare u as update t1_xc_fqs set num=0 where id1=$1 and id2=$2;
prepare d as delete from t1_xc_fqs where id1=$1 and id2=$2;

set enable_pbe_optimization to false;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1,1);
explain (costs off, verbose on) execute i (6,6,6);
explain (costs off, verbose on) execute u (2,2);
explain (costs off, verbose on) execute d (3,3);
execute s (1,1);
execute i (6,6,6);
select * from t1_xc_fqs order by id1;
execute u (2,2);
select * from t1_xc_fqs order by id1;
execute d (3,3);
select * from t1_xc_fqs order by id1;

set enable_pbe_optimization to true;
truncate t1_xc_fqs;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1,1);
explain (costs off, verbose on) execute i (6,6,6);
explain (costs off, verbose on) execute u (2,2);
explain (costs off, verbose on) execute d (3,3);
execute s (1,1);
execute i (6,6,6);
select * from t1_xc_fqs order by id1;
execute u (2,2);
select * from t1_xc_fqs order by id1;
execute d (3,3);
select * from t1_xc_fqs order by id1;

deallocate s;
deallocate i;
deallocate u;
deallocate d;

-- const and params
prepare s as select * from t1_xc_fqs where id1=$1 and id2=2;
prepare i as insert into t1_xc_fqs values ($1, 2, 3);
prepare u as update t1_xc_fqs set num=1 where id1=$1 and id2=2;
prepare d as delete from t1_xc_fqs where id1=$1 and id2=2;

set enable_pbe_optimization to false;
truncate t1_xc_fqs;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1);
explain (costs off, verbose on) execute i (6);
explain (costs off, verbose on) execute u (2);
explain (costs off, verbose on) execute d (3);
execute s (1);
execute i (6);
select * from t1_xc_fqs order by id1, id2;
execute u (2);
select * from t1_xc_fqs order by id1, id2;
execute d (3);
select * from t1_xc_fqs order by id1, id2;

set enable_pbe_optimization to true;
truncate t1_xc_fqs;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1);
explain (costs off, verbose on) execute i (6);
explain (costs off, verbose on) execute u (2);
explain (costs off, verbose on) execute d (3);
execute s (1);
execute i (6);
select * from t1_xc_fqs order by id1, id2;
execute u (2);
select * from t1_xc_fqs order by id1, id2;
execute d (3);
select * from t1_xc_fqs order by id1, id2;

deallocate s;
deallocate i;
deallocate u;
deallocate d;
drop table t1_xc_fqs;


-- Testset 4 dynamic datanode reduction for multi-table join (column and row)
create table t1_xc_fqs(id1 int, id2 int, num int) with (orientation = column);
insert into t1_xc_fqs values (1,1,11), (2,2,21), (3,3,31), (4,4,41), (5,5,51);
create table t2_xc_fqs(id1 int, id2 int, num int);
insert into t2_xc_fqs values (1,2,12), (2,3,22), (3,4,32), (4,5,42), (5,6,52);
create table t3_xc_fqs(id11 int, id22 int, num int);
insert into t3_xc_fqs values (1,13,13), (2,23,23), (3,33,33), (4,43,43), (5,53,53);

-- implicit join
prepare s0 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=$1 and t2.id1=$2;
prepare s1 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=$1 and t2.id1=$2 and t3.id11=$3;
prepare s2 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=t2.id1 and t1.id1=$1;
prepare s3 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=t2.id1 and t3.id11=$1;
prepare s4 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=t2.id1 and t1.id1=t3.id11 and t3.id11=$1;
prepare s5 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=t2.id2 and t2.id1=$1 and t2.id2=$2;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

deallocate s0;
deallocate s1;
deallocate s2;
deallocate s3;
deallocate s4;
deallocate s5;

-- explicit join
prepare s0 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=$1 and t2.id1=$2;
prepare s1 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=$1 and t2.id1=$2 join t3_xc_fqs t3 on t3.id11=$3;
prepare s2 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1;
prepare s3 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 join t3_xc_fqs t3 on t3.id11=$1;
prepare s4 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 join t3_xc_fqs t3 on t1.id1=t3.id11 and t3.id11=$1;
prepare s5 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id2 where t2.id1=$1 and t2.id2=$2;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

deallocate s0;
deallocate s1;
deallocate s2;
deallocate s3;
deallocate s4;
deallocate s5;

drop table t1_xc_fqs;
drop table t2_xc_fqs;
drop table t3_xc_fqs;


-- Testset 5 dynamic datanode reduction for single table (replication)
create table t1_xc_fqs(id1 int, id2 int, num int);

prepare s as select * from t1_xc_fqs where id1=$1 and id2=2;
prepare i as insert into t1_xc_fqs values ($1, 2, 3);
prepare u as update t1_xc_fqs set num=0 where id1=$1 and id2=2;
prepare d as delete from t1_xc_fqs where id1=$1 and id2=2;

set enable_pbe_optimization to false;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1);
explain (costs off, verbose on) execute i (6);
explain (costs off, verbose on) execute u (2);
explain (costs off, verbose on) execute d (3);
execute s (1);
execute i (6);
select * from t1_xc_fqs order by id1;
execute u (2);
select * from t1_xc_fqs order by id1;
execute d (3);
select * from t1_xc_fqs order by id1;

set enable_pbe_optimization to true;
truncate t1_xc_fqs;
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
explain (costs off, verbose on) execute s (1);
explain (costs off, verbose on) execute i (6);
explain (costs off, verbose on) execute u (2);
explain (costs off, verbose on) execute d (3);
execute s (1);
execute i (6);
select * from t1_xc_fqs order by id1;
execute u (2);
select * from t1_xc_fqs order by id1;
execute d (3);
select * from t1_xc_fqs order by id1;

deallocate s;
deallocate i;
deallocate u;
deallocate d;
drop table t1_xc_fqs;


-- Testset 6 dynamic datanode reduction for multi-table join (replication and distribute)
create table t1_xc_fqs(id1 int, id2 int, num int);
insert into t1_xc_fqs values (1,1,11), (2,2,21), (3,3,31), (4,4,41), (5,5,51);
create table t2_xc_fqs(id1 int, id2 int, num int);
insert into t2_xc_fqs values (1,2,12), (2,3,22), (3,4,32), (4,5,42), (5,6,52);
create table t3_xc_fqs(id11 int, id22 int, num int);
insert into t3_xc_fqs values (1,13,13), (2,23,23), (3,33,33), (4,43,43), (5,53,53);

-- implicit join
prepare s0 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=$1 and t2.id1=$2;
prepare s1 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=$1 and t2.id1=$2 and t3.id11=$3;
prepare s2 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=t2.id1 and t1.id1=$1;
prepare s3 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=t2.id1 and t3.id11=$1;
prepare s4 as select id11 from t1_xc_fqs t1,t2_xc_fqs t2,t3_xc_fqs t3 where t1.id1=t2.id1 and t1.id1=t3.id11 and t3.id11=$1;
prepare s5 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=t2.id2 and t2.id1=$1 and t2.id2=$2;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

deallocate s0;
deallocate s1;
deallocate s2;
deallocate s3;
deallocate s4;
deallocate s5;

-- explicit join
prepare s0 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=$1 and t2.id1=$2;
prepare s1 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=$1 and t2.id1=$2 join t3_xc_fqs t3 on t3.id11=$3;
prepare s2 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 and t2.id1=$1;
prepare s3 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 join t3_xc_fqs t3 on t3.id11=$1;
prepare s4 as select id11 from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id1 join t3_xc_fqs t3 on t1.id1=t3.id11 and t3.id11=$1;
prepare s5 as select * from t1_xc_fqs t1 join t2_xc_fqs t2 on t1.id1=t2.id2 where t2.id1=$1 and t2.id2=$2;

set enable_pbe_optimization to false;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

set enable_pbe_optimization to true;
explain (costs off, verbose on) execute s0 (1,1);
explain (costs off, verbose on) execute s0 (1,3);
explain (costs off, verbose on) execute s1 (2,2,2);
explain (costs off, verbose on) execute s2 (3);
explain (costs off, verbose on) execute s3 (4);
explain (costs off, verbose on) execute s4 (5);
explain (costs off, verbose on) execute s5 (4,5);
execute s0 (1,1);
execute s0 (1,3);
execute s1 (2,2,2);
execute s2 (3);
execute s3 (4);
execute s4 (5);
execute s5 (4,5);

deallocate s0;
deallocate s1;
deallocate s2;
deallocate s3;
deallocate s4;
deallocate s5;

drop table t1_xc_fqs;
drop table t2_xc_fqs;
drop table t3_xc_fqs;


--Optimization
create table t1_xc_fqs(id1 int, id2 int, num int);
insert into t1_xc_fqs values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5);
prepare a as select num from t1_xc_fqs where id1=$1 order by id2;
set enable_pbe_optimization to false;
explain verbose execute a (1);
execute a (1);
set enable_pbe_optimization to true;
explain verbose execute a (1);
execute a (1);
deallocate a;
drop table t1_xc_fqs;


-- Testset bug fixing
create table distribute_by_int (int1 int,numeric1 numeric(7,3));
prepare a as insert into distribute_by_int values ($1,$2);
explain verbose execute a(51,1111);
execute a(51,1111);
explain verbose execute a(51,1111);
deallocate a;
drop table distribute_by_int;

create table distribute_by_date (col_date date, c_ts_without timestamp  without time zone, c_ts_with  timestamp with time zone);
insert into distribute_by_date values ('2014-09-12 00:00:00','2014-09-12 00:00:00');
prepare a as select * from distribute_by_date where col_date=$1;
explain verbose execute a('2014-09-12 00:00:00');
deallocate a;
drop table distribute_by_date;

create table distribute_by_varchar (char1 char(4),varchar1 varchar(50));
insert into distribute_by_varchar values ('10','10');
prepare a as delete from distribute_by_varchar where varchar1=$1;
explain verbose execute a('10');
deallocate a;
drop table distribute_by_varchar;

CREATE TABLE pbe_prunning_000(id INT,part INT, c_bigint bigint, c_text text);
insert into pbe_prunning_000 values (10,10,10,'abcdefghijklmn0010');
CREATE TABLE pbe_prunning_tmp (id INT,part INT, c_bigint bigint, c_text text);
SET enable_pbe_optimization to false;
PREPARE pa AS INSERT into pbe_prunning_tmp(id,part,c_bigint, c_text) SELECT id,part,c_bigint, c_text FROM pbe_prunning_000 pp0 WHERE pp0.c_bigint=$1 AND pp0.c_text=$2;
EXECUTE pa(10,'abcdefghijklmn0010');
SELECT * FROM pbe_prunning_tmp pp1 WHERE pp1.c_bigint=10 AND pp1.c_text='abcdefghijklmn0010';
DEALLOCATE PREPARE pa;
PREPARE pa AS UPDATE pbe_prunning_tmp pp1 SET id=999 WHERE pp1.c_bigint=$1 AND pp1.c_text=$2;
EXECUTE pa(10,'abcdefghijklmn0010');
SELECT * FROM pbe_prunning_tmp pp1 WHERE pp1.c_bigint=10 AND pp1.c_text='abcdefghijklmn0010';
DEALLOCATE PREPARE pa;
drop table pbe_prunning_tmp;
drop table pbe_prunning_000;

SET enable_pbe_optimization to false;
create table t_TESTTABLE (id int,name text);
insert into t_TESTTABLE values (1,'a'),(2,'b'),(3,'c'), (4,'d'),(5,'e');
prepare a as select * from t_TESTTABLE where id=$1;
execute a (1);
execute a (2);
execute a (3);
execute a (4);
execute a (5);
execute a (1);
deallocate a;
drop table t_TESTTABLE;

-- Bugfix for FQS multi-table join
create table t1(id1 int, id2 int, num int);
insert into t1 values (1,11,11), (2,21,21), (3,31,31), (4,41,41), (5,51,51);
create table t2(id1 int, id2 int, num int);
insert into t2 values (1,12,12), (2,22,22), (3,32,32), (4,42,42), (5,52,52);
create table t3(id11 int, id22 int, num int);
insert into t3 values (1,13,13), (2,23,23), (3,33,33), (4,43,43), (5,53,53);
prepare a as select id11 from t1, t2, t3 where t1.id1=$1 and t2.id1=$2 and t3.id11=$3;
prepare b as select id11 from t1, t2, t3 where t1.id1=t2.id1 and t3.id11=$1;
prepare c as select id11 from t1,t2,t3 where t1.id1=t2.id1 and t1.id1=t3.id11 and t1.id1=$1;
SET enable_pbe_optimization to false;
execute a (1,1,1);
execute b (1);
execute c (1);
SET enable_pbe_optimization to true;
execute a (1,1,1);
execute b (1);
execute c (1);
drop table t1;
drop table t2;
drop table t3;

CREATE TABLE pbe_prunning_001 (id INT, c_int INT, c_numeric NUMERIC(7,3));
CREATE TABLE pbe_prunning_002 (id INT, c_int INT, c_numeric NUMERIC(7,3));
INSERT INTO pbe_prunning_001 values (1, 1, 1);
INSERT INTO pbe_prunning_002 values (1, 1, 1);
PREPARE pa AS SELECT * FROM pbe_prunning_001 pp1 RIGHT OUTER JOIN pbe_prunning_002 pp2 ON pp1.c_int=pp2.c_int WHERE (pp1.c_int=$1 AND pp1.c_numeric=$2 AND pp2.c_int=$3 AND pp2.c_numeric=$4) or pp1.id=$5;
EXPLAIN(COSTS FALSE) EXECUTE pa(10,10,10,10,11);
DEALLOCATE PREPARE pa;
DROP TABLE pbe_prunning_001;
DROP TABLE pbe_prunning_002;

create table pbe_prunning_000(id INT,part INT, c_int INT, c_numeric NUMERIC(7,3));
insert into pbe_prunning_000 values(1,1,10,10);
CREATE TABLE pbe_prunning_tmp (id INT,part INT, c_int INT, c_numeric NUMERIC(7,3));
set enable_pbe_optimization to false;
PREPARE pa AS INSERT INTO pbe_prunning_tmp (id, part, c_int, c_numeric) SELECT id, part, c_int, c_numeric FROM pbe_prunning_000 pp0 WHERE pp0.c_int=$1 AND pp0.c_numeric=$2;
explain(costs off)execute pa(10,10);
EXECUTE pa(10,10);
deallocate pa;
PREPARE pa AS INSERT INTO pbe_prunning_tmp (id, part, c_int, c_numeric) SELECT id, part, c_int, c_numeric FROM pbe_prunning_000 pp0 WHERE pp0.c_int=$1;
explain(costs off)execute pa(10);
EXECUTE pa(10);
select * from pbe_prunning_tmp;
deallocate pa;
drop table pbe_prunning_000;
drop table pbe_prunning_tmp;

CREATE TABLE pbe_prunning_001 (id INT,part INT, c_int INT, c_numeric NUMERIC(7,3));
insert into pbe_prunning_001 values(1937,1937,1937.000);
insert into pbe_prunning_001 values(1938,1938,1938.000);
CREATE TABLE pbe_prunning_002 (id INT,part INT, c_int INT, c_numeric NUMERIC(7,3));
set enable_pbe_optimization to false;
PREPARE pa AS SELECT * FROM pbe_prunning_001 pp1 CROSS JOIN pbe_prunning_002 pp2 WHERE pp1.c_int=$1 OR pp1.c_numeric=$2 OR pp2.c_int=$3 OR pp2.c_numeric=$4 order by 1,2,3,4,5,6;
explain(costs off) execute pa(10,10,10,10);
EXECUTE pa(10,10,10,10);
deallocate pa;
PREPARE pa AS SELECT * FROM pbe_prunning_001 pp1 CROSS JOIN pbe_prunning_002 pp2 WHERE pp1.c_int=$1 OR pp1.c_numeric=$2 OR pp2.c_int=$3 order by 1,2,3,4,5,6;
explain(costs off) execute pa(10,10,10);
EXECUTE pa(10,10,10);
deallocate pa;
drop table pbe_prunning_001;
drop table pbe_prunning_002;

--mix options: custom, generic, stream
create table t1_xc_fqs(id1 int, id2 int, num int);
insert into t1_xc_fqs values (1,11,11), (2,21,21);
create table t2_xc_fqs(id1 int, id2 int, num int);
insert into t2_xc_fqs values (1,12,12), (1,22,22), (2,22,22);
prepare s0 as select * from t1_xc_fqs t1,t2_xc_fqs t2 where t1.id1=$1 and t2.id1=$2 and t2.id2=$3;

set enable_pbe_optimization to false;
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);

set enable_pbe_optimization to true;
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);

set enable_pbe_optimization to false;
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);
explain(costs off, verbose on) execute s0 (1,1,12);
execute s0 (1,1,12);
explain(costs off, verbose on) execute s0 (1,1,22);
execute s0 (1,1,22);

deallocate s0;
drop table t1_xc_fqs;
drop table t2_xc_fqs;

set enable_pbe_optimization to true;
create table TESTTABLE_t1 (id int, num int);
insert into TESTTABLE_t1 values (1,1),(20,20);
explain analyze create table TESTTABLE_t2 as select * from TESTTABLE_t1;
drop table TESTTABLE_t2;
drop table TESTTABLE_t1;

create table cs_his(segment1 varchar(15), period_name varchar(15), currency_code varchar(15), frequency_code varchar(15), segment3 varchar(15), segment3_desc varchar(200), end_balance_dr numeric, end_balance_cr numeric) with (orientation=column);
create table bs_his(row_desc varchar(800),row_id numeric,amount1 numeric, amount2 numeric, period_name varchar(80), frequency_code varchar(80), currency_code varchar(80), segment1 varchar(80)) with (orientation=column);
insert into cs_his values('11','20190227','22','33','44','55',1.3,3.5);
insert into cs_his values('11','20181231','22','33','44','55',1.3,3.5);
CREATE OR REPLACE FUNCTION test_func(v_date character varying, OUT ret integer)
 RETURNS integer
 LANGUAGE plpgsql
 NOT FENCED
AS $$DECLARE 
V_DT             VARCHAR2(10);
V_LY_END         VARCHAR2(10) := '20181231';
BEGIN
insert into bs_his
    (row_desc,
     row_id,
     amount1,
     amount2,
     period_name,
     frequency_code,
     currency_code,
     segment1)
    select '资  产：',
           '1',
           '' qc,
           '' qm,
           b.period_name,
           b.frequency_code,
           b.currency_code,
           b.segment1
      from cs_his b
     where b.period_name in (v_date, V_LY_END);
END $$;
select test_func(20190227);
select test_func(20190227);
select test_func(20190227);
select test_func(20190227);
select test_func(20190227);
select test_func(20190227);
select test_func(20190227);
--14 rows
select * from bs_his order by period_name;
drop FUNCTION test_func;
drop table bs_his;
drop table cs_his;

-- pbe choose_adaptive_gplan
create table tab_1103983(c1 int,c2 varchar,c3 text) ;
insert into tab_1103983  values(generate_series(1, 500),generate_series(1, 500),generate_series(1, 100));
insert into tab_1103983  values(generate_series(1, 50),generate_series(2, 30),generate_series(2, 30));
insert into tab_1103983  values(generate_series(2, 30),generate_series(2, 50),generate_series(2, 30));
insert into tab_1103983  values(generate_series(2, 30),generate_series(2, 30),generate_series(1, 50));
insert into tab_1103983  values(generate_series(1, 100),null,null);
insert into tab_1103983  values(generate_series(1, 500),generate_series(1, 500),generate_series(2, 100));
create index on tab_1103983(c2,c3);

analyze tab_1103983;

prepare pbe_cagp as select  /*+ choose_adaptive_gplan */ t1.c1,max(t2.c3) from tab_1103983 t1 
    join (select  /*+ choose_adaptive_gplan */ * from  tab_1103983 where c1=$1 and c2 = $2) t2 
    on t1.c1 = t2.c1 
    where t2.c2 = (select max(c2) from tab_1103983 where c2 = $1 ) 
    and t1.c3 = $2
    group by 1 
    order by 1,2;

execute pbe_cagp(1,1);
execute pbe_cagp(3,3);
execute pbe_cagp(4,4);
execute pbe_cagp(5,1);
execute pbe_cagp(7,8);
execute pbe_cagp(10,10);

deallocate pbe_cagp;
drop table tab_1103983;

drop table if exists tab_1109433;
create table tab_1109433(c1 int,c2 varchar,c3 text,c4 date) ;

insert into tab_1109433 values(generate_series(0, 60),generate_series(1, 30),generate_series(1, 10),date'1999-01-01'+generate_series(1, 60));

insert into tab_1109433 values(generate_series(1, 50),generate_series(0, 30),generate_series(2, 10),date'2000-01-01'+generate_series(1, 50));

insert into tab_1109433 values(generate_series(2, 30),generate_series(2, 30),generate_series(0, 10),date'2001-01-01'+generate_series(1, 50));

insert into tab_1109433 values(generate_series(2, 30),generate_series(2, 30),generate_series(1, 10),date'2002-01-01'+generate_series(1, 50));

insert into tab_1109433 values(generate_series(1, 60),null,null,null);

create index on tab_1109433(c2,c3);

analyze tab_1109433;

drop table if exists tab_1109433_1;
create table tab_1109433_1(c1 int,c2 varchar2(20),c3 varchar2(20),c4 int,c5 date);
insert into tab_1109433_1 values(generate_series(1, 50),'li','adjani',10,'2001-02-02');
insert into tab_1109433_1 values(generate_series(2, 50),'li','adjani',20,'2001-01-12');
insert into tab_1109433_1 values(generate_series(3, 50),'li','adjani',50,'2000-01-01');

drop table if exists tab_1109433_2;
create table tab_1109433_2(c1 int,c2 varchar2(20),c3 varchar2(20),c4 int,c5 date);
insert into tab_1109433_2 values(1,'li','adjani',10,'2001-02-02');
insert into tab_1109433_2 values(2,'li','adjani',20,'2001-01-12');
insert into tab_1109433_2 values(3,'li','adjani',50,'2000-01-01');
insert into tab_1109433_2 values(10,'li','adj',10,'2001-02-02');
insert into tab_1109433_2 values(20,'li','adj',20,'2001-01-12');
insert into tab_1109433_2 values(30,'li','adj',50,'2000-01-01');

drop table if exists tab_1109433_3;
create table tab_1109433_3(c1 int,c2 varchar2(20),c3 varchar2(20),c4 int,c5 date);
insert into tab_1109433_3 values(1,'li','adjani',10,'2001-02-02');
insert into tab_1109433_3 values(2,'li','adjani',20,'2001-01-12');
insert into tab_1109433_3 values(3,'li','adjani',50,'2000-01-01');
insert into tab_1109433_3 values(11,'li','ani',11,'2001-01-02');
insert into tab_1109433_3 values(12,'li','ani',12,'2001-01-12');
insert into tab_1109433_3 values(13,'li','ani',15,'2000-01-01');

prepare tab_1109433_pbe as SELECT /*+ choose_adaptive_gplan */ T.c2
,count(case when POSITION(T.c3 IN '1,2') > 0 and T.c2 = $1 then 1 end) AS LG_PERSON_NUM
,count(case when POSITION(T.c3 IN '1,2') > 0 and T.c2 = $2 then 1 end) AS LG_ORG_NUM
,count(case when POSITION(T.c3 IN '1,2') > 0 then 1 end) AS LG_NUM
,count(case when T.c3 = '4' and T.c2 = $1 then 1 end) AS VTM_PERSON_NUM
,count(case when T.c3 = '4' and T.c2 = $2 then 1 end) AS VTM_ORG_NUM
,count(case when T.c3 = '4' then 1 end) AS VTM_NUM
,count(case when T.c3 = $1 and T.c2 = $1 then 1 end) AS REMOTE_PERSON_NUM
,count(case when T.c3 = $1 and T.c2 = $2 then 1 end) AS REMOTE_ORG_NUM
,count(case when T.c3 = $1 then 1 end) AS REMOTE_NUM
,count(case when POSITION(T.c3 IN '1,2') > 0 and T.c2 = $1 and B.c1 = $1 then 1 end) AS LG_REJECT_PERSON_NUM
,count(case when POSITION(T.c3 IN '1,2') > 0 and T.c2 = $2 and B.c1 = $1 then 1 end) AS LG_REJECT_ORG_NUM
,count(case when POSITION(T.c3 IN '1,2') > 0 and B.c1 = $1 then 1 end) AS LG_REJECT_NUM
,count(case when T.c3 = $1 and B.c1 = $1 then 1 end) AS REMOTE_REJECT_NUM
,count(case when T.c3 = '4' and B.c1 = $1 then 1 end) AS VTM_REJECT_NUM
FROM tab_1109433 T left JOIN tab_1109433_1 B
ON T.c1 = B.c1
WHERE 1 = 1
AND T.c2 = $4
AND T.c4 >= TO_DATE($3)
AND T.c4 < TO_DATE('20220922') + 1
GROUP BY T.c2
order by 1,2,3,4,5,6,7,8;

execute tab_1109433_pbe(0,1,'19900401',13);
execute tab_1109433_pbe(1,1,'19910401',14);

explain (costs off) execute tab_1109433_pbe(0,1,'19900401',13);
explain (costs off) execute tab_1109433_pbe(1,1,'19910401',14);

deallocate tab_1109433_pbe;
drop table tab_1109433;
drop table tab_1109433_1;
drop table tab_1109433_2;
drop table tab_1109433_3;
