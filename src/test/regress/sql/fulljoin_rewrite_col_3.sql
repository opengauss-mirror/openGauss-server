create schema full_join_on_true_col_3;
set current_schema='full_join_on_true_col_3';

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1(a int, b int) WITH (ORIENTATION = COLUMN);
create table t2(j int, k int) WITH (ORIENTATION = COLUMN);
create table t3(x int, y int) WITH (ORIENTATION = COLUMN);

insert into t1 values(1,1);
insert into t1 values(1,1);
insert into t1 values(100,100);
insert into t1 values(200,200);
insert into t1 values(200,200);
insert into t1 values(121,121);
insert into t1 values(122,122);
insert into t1 values(122,122);
insert into t1 values(131,131);
insert into t1 values(132,132);
insert into t1 values(132,132);

insert into t2 values(2,2);
insert into t2 values(2,2);
insert into t2 values(100,100);
insert into t2 values(200,200);
insert into t2 values(200,200);
insert into t2 values(121,121);
insert into t2 values(122,122);
insert into t2 values(122,122);
insert into t2 values(231,231);
insert into t2 values(232,232);
insert into t2 values(232,232);

insert into t3 values(3,3);
insert into t3 values(3,3);
insert into t3 values(100,100);
insert into t3 values(200,200);
insert into t3 values(200,200);
insert into t3 values(232,232);
insert into t3 values(232,232);
insert into t3 values(131,131);
insert into t3 values(132,132);
insert into t3 values(132,132);

--- PART 1  Nestloop

set enable_nestloop=on;
set enable_hashjoin=off;
set enable_mergejoin=off;	

select * from t1 inner join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;

select * from t1 inner join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;

select * from t1 inner join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 inner join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 left join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

select * from t1 right join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 right join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

select * from t1 full join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 right join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 full join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

-- PART 2 hashjoin copied from PART 1
set enable_nestloop=off;
set enable_hashjoin=on;
set enable_mergejoin=off;	

select * from t1 inner join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;

select * from t1 inner join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;

select * from t1 inner join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 inner join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 left join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

select * from t1 right join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 right join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

select * from t1 full join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 right join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 full join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

-- PART 3 mergejoin copied from PART 1
set enable_nestloop=off;
set enable_hashjoin=off;
set enable_mergejoin=on;	

select * from t1 inner join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;

select * from t1 inner join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;

select * from t1 inner join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 inner join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 inner join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 left join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 left join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

select * from t1 right join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 right join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 right join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

select * from t1 full join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
select * from t1 full join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
select * from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 full join t3 on 1=1 on t1.a=t2.j order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on j=x on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 full join t3 on j=x on t1.a=t2.j order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 inner join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 inner join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 left join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 right join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 right join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

explain(verbose, costs off) select * from t1 full join t2 on a=j full join t3 on 1=1 order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 on a=j full join t3 on k=y order by 1,2,3,4,5,6;
explain(verbose, costs off) select * from t1 full join t2 on 1=1 full join t3 on 1=1 order by 1,2,3,4,5,6;

drop table if exists full_join_t1;
create table full_join_t1 (a int, b int, c int) WITH (ORIENTATION = COLUMN) distribute by hash(a) ;
drop table if exists full_join_t2;
create table full_join_t2 (a int, b float, c int) WITH (ORIENTATION = COLUMN) distribute by hash(a) ;

insert into full_join_t1 values(1,1,2);
insert into full_join_t2 values(1,1,1);

insert into full_join_t1 values(3,3,3);
insert into full_join_t2 values(3,3,3);

insert into full_join_t1 values(0,0,0);
insert into full_join_t2 values(4,4,4);

select * from full_join_t1 t1 full join full_join_t2 t2 on t1.a =t2.b and t1.a in (select distinct a from full_join_t2 ) and 1=1 order by 1,2,3,4,5,6;
select * from full_join_t1 t1 full join full_join_t2 t2 on t1.a =t2.b and t1.a in (select distinct a from full_join_t2 where t1.c>t2.c) and 1=1 order by 1,2,3,4,5,6;

explain(costs off, verbose) select * from full_join_t1 t1 full join full_join_t2 t2 on t1.a =t2.b and t1.a in (select distinct a from full_join_t2 ) and 1=1 order by 1,2,3,4,5,6;
explain(costs off, verbose) select * from full_join_t1 t1 full join full_join_t2 t2 on t1.a =t2.b and t1.a in (select distinct a from full_join_t2 where t1.c>t2.c) and 1=1 order by 1,2,3,4,5,6;

drop schema full_join_on_true_col_3 CASCADE;

