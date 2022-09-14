create schema full_join_on_true_row_2;
set current_schema='full_join_on_true_row_2';
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;



create table t1(acct_id float, d int) ;
create table t2(acct_id float, d int) ;
create table t3(acct_id float, d int) ;
					 
insert into t1 values(1.1);
insert into t1 values(1.1);
insert into t1 values(121.1);
insert into t1 values(122.1);
insert into t1 values(122.1);
insert into t1 values(131.1);
insert into t1 values(132.1);
insert into t1 values(132.1);
insert into t1 values(888.1);
insert into t1 values(999.1);
insert into t1 values(999.1);

insert into t2 values(2.1);
insert into t2 values(2.1);
insert into t2 values(121.1);
insert into t2 values(122.1);
insert into t2 values(122.1);
insert into t2 values(231.1);
insert into t2 values(232.1);
insert into t2 values(232.1);
insert into t2 values(888.1);
insert into t2 values(999.1);
insert into t2 values(999.1);

insert into t3 values(3.1);
insert into t3 values(3.1);
insert into t3 values(231.1);
insert into t3 values(232.1);
insert into t3 values(232.1);
insert into t3 values(131.1);
insert into t3 values(132.1);
insert into t3 values(132.1);
insert into t3 values(888.1);
insert into t3 values(999.1);
insert into t3 values(999.1);

set enable_nestloop=on;
set enable_hashjoin=off;
set enable_mergejoin=off;	
				 
explain (verbose, costs off) select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;		
select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;


explain (verbose, costs off) select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join (select atan2(t3.acct_id,t2.acct_id)as acct_id from  t2 full outer join t3 on t3.acct_id=t2.acct_id )  t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;
select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join (select atan2(t3.acct_id,t2.acct_id)as acct_id from  t2 full outer join t3 on t3.acct_id=t2.acct_id )  t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;

explain select /*+  leading((t1 t2)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
select /*+  leading((t1 t2)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
explain select /*+  leading((t2 t1)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
select /*+  leading((t2 t1)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;

set enable_nestloop=off;
set enable_hashjoin=on;
set enable_mergejoin=off;

explain (verbose, costs off) select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;
select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;

explain (verbose, costs off) select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join (select atan2(t3.acct_id,t2.acct_id)as acct_id from  t2 full outer join t3 on t3.acct_id=t2.acct_id )  t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;
select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join (select atan2(t3.acct_id,t2.acct_id)as acct_id from  t2 full outer join t3 on t3.acct_id=t2.acct_id )  t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;

explain select /*+  leading((t1 t2)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
select /*+  leading((t1 t2)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
explain select /*+  leading((t2 t1)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
select /*+  leading((t2 t1)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;


set enable_nestloop=off;
set enable_hashjoin=off;
set enable_mergejoin=on;
					 
explain (verbose, costs off) select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;
select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;


explain (verbose, costs off) select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join (select atan2(t3.acct_id,t2.acct_id)as acct_id from  t2 full outer join t3 on t3.acct_id=t2.acct_id )  t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;
select * from (select atan2(t1.acct_id,t2.acct_id)as acct_id from  t1 full outer join t2 on t1.acct_id=t2.acct_id ) A full join (select atan2(t3.acct_id,t2.acct_id)as acct_id from  t2 full outer join t3 on t3.acct_id=t2.acct_id )  t3 on a.acct_id=t3.acct_id full join t1 on atan2(a.acct_id,t3.acct_id)=t1.acct_id order by 1,2,3,4;


explain select /*+  leading((t1 t2)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
select /*+  leading((t1 t2)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
explain select /*+  leading((t2 t1)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;
select /*+  leading((t2 t1)) */ *from t1 full join t2 on t1.acct_id=t2.acct_id order by 1,2,3,4;


drop schema full_join_on_true_row_2 CASCADE;