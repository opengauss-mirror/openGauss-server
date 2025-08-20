create schema spm_adaptive_gplan;
set search_path = 'spm_adaptive_gplan';

-------------------------------------------------------------------
---adaptive select the right index accroding given constraint values
create table t1(c1 int, c2 int, c3 int, c4 varchar(32), c5 text);
Create index t1_idx2 on t1(c1,c2,c3,c4);
Create index t1_idx1 on t1(c1,c2,c3);
insert into t1( c1, c2, c3, c4, c5) SELECT (random()*(2*10^9))::integer , (random()*(2*10^9))::integer,  (random()*(2*10^9))::integer, (random()*(2*10^9))::integer,  repeat('abc', i%10) ::text from generate_series(1,10000) i;
insert into t1( c1, c2, c3, c4, c5) SELECT (random()*1)::integer, (random()*1)::integer, (random()*1)::integer, (random()*(2*10^9))::integer, repeat('abc', i%10) ::text from generate_series(1,10000) i;
analyze;

---cplan
explain(costs off) select * from t1 where c1=1 and c2=1 and c3=1 and c4='1';
explain(costs off) select * from t1 where c1=1 and c2=1 and c3=11 and c4='11';

prepare multi_indices_test as select * from t1 where c1=$1 and c2=$2 and c3=$3 and c4=$4;
explain(costs off) execute multi_indices_test(1,1,1,'1');
explain(costs off) execute multi_indices_test(11,11,11, '11');
deallocate multi_indices_test;

prepare multi_indices_test as select /*+ use_cplan */ * from t1 where c1=$1 and c2=$2 and c3=$3 and c4=$4;
explain(costs off) execute multi_indices_test(1,1,1,'1');
explain(costs off) execute multi_indices_test(11,11,11, '11');
deallocate multi_indices_test;


---adpative gplan selection
prepare multi_indices_test as select /*+ choose_adaptive_gplan */ * from t1 where c1=$1 and c2=$2 and c3=$3 and c4=$4;
explain(costs off) execute multi_indices_test(1,1,1,'1');
explain(costs off) execute multi_indices_test(1,1,1, '11');
explain(costs off) execute multi_indices_test(1,1,11, '11');
explain(costs off) execute multi_indices_test(1,1,20, '11');
explain(costs off) execute multi_indices_test(1,1,10,'1');
deallocate multi_indices_test;

prepare multi_indices_test as select /*+ choose_adaptive_gplan */ c1, c2, c3 from t1 where c1=$1 and c2=$2 and c3=$3;
explain(costs off) execute multi_indices_test(1,1,1);
explain(costs off) execute multi_indices_test(11,11,11);
deallocate multi_indices_test;

prepare multi_indices_test as select /*+ choose_adaptive_gplan */ c1, c2, c3 from t1 where c1=$1 and c2=$2;
explain(costs off) execute multi_indices_test(1,1);
explain(costs off) execute multi_indices_test(11,11);


-------------------------------------------------------------------
---enable gplan using partial index
create table t0(c1 int, c2 text);
create index t0_partial_index on t0(c1) where c1 < 3;
insert into t0( c1, c2) SELECT (random()*100)::integer, repeat('abc', i%10) ::text from generate_series(1,100000) i;

---cplan
explain select * from t0 where c1 = 1;
explain select * from t0 where c1 = 10;

---adpative gplan selection
prepare partidx_test as select /*+ choose_adaptive_gplan */ * from t0 where c1 = $1;
explain(costs off) execute partidx_test(1);
explain(costs off) execute partidx_test(10);
explain(costs off) execute partidx_test(2);
explain(costs off) execute partidx_test(11);

deallocate partidx_test;

--------------------
---adaptive select the right join operator according to given limit offset values
create table tt1(c1 int, c2 int);
create table tt2(c1 int, c2 int);
insert into tt1 values(generate_series(1, 10), generate_series(1, 1000));
insert into tt2 values(generate_series(1, 10), generate_series(1, 1000));

---cplan
explain(costs off) select * from tt1, tt2 where tt1.c1 = tt2.c1 limit 1, 1;
explain(costs off) select * from tt1, tt2 where tt1.c1 = tt2.c1 limit 1000, 1;

---adpative gplan selection
prepare offset_test as select /*+ choose_adaptive_gplan */ * from tt1, tt2 where tt1.c1 = tt2.c1 limit $1, 1;
explain(costs off) execute offset_test(1);
explain(costs off) execute offset_test(1000);
explain(costs off) execute offset_test(1);
explain(costs off) execute offset_test(1000);

deallocate offset_test;

---generate adaptive gplan then choose cplan
set enable_pbe_optimization = off;
create table t_adapt(c1 int,c2 varchar,c3 text) ;
insert into t_adapt values(generate_series(1, 1000),generate_series(1, 1000),generate_series(1, 100));
create index ind_adapt on t_adapt(c2,c3);
analyze t_adapt;

prepare s_adapt as select /*+ choose_adaptive_gplan */ t1.c1,max(t2.c3) from t_adapt t1 join (select /*+ choose_adaptive_gplan */ * from t_adapt where c1=$1 and c2 = $2) t2 on t1.c1 = t2.c1 where t2.c2 in (select c2 from t_adapt t3 where t3.c2 = $1 and t1.c1 = t3.c1 ) and t1.c3 = $2 group by 1 order by 1,2;
--execute with index 5 times
execute s_adapt(1,1);
execute s_adapt(1,1);
execute s_adapt(1,1);
execute s_adapt(1,1);
execute s_adapt(1,1);

--drop index
drop index ind_adapt;

--sixth execute, generate adaptive plan but costs higher than average cplan costs
execute s_adapt(1,1);

deallocate s_adapt;
set enable_pbe_optimization = default;

--------------------------------------------------------------------
---cleaning up
drop table t0;
drop table t1;
drop table tt1;
drop table tt2;
drop table t_adapt;

drop schema spm_adaptive_gplan cascade;
reset current_schema;

