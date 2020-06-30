create schema distribute_joinplan;
set current_schema = distribute_joinplan;

-- add code coverage for hashjoin
set best_agg_plan=3;
create table t1(a int, b int, c int, d int, e int);
create table t(a int, b int, c int, d int, e int);
insert into t1 select generate_series(1,10);
insert into t select generate_series(1,100000), generate_series(1,100000), generate_series(1,100000), generate_series(1,100000);
analyze t1;
analyze t;
update pg_class set reltuples=1000000 where relname='t1' and relnamespace=(select oid from pg_namespace where nspname='distribute_joinplan');
update pg_class set reltuples=100 where relname='t' and relnamespace=(select oid from pg_namespace where nspname='distribute_joinplan');
set explain_perf_mode=summary;
explain performance select * from t1 join (select * from t union all select * from t union all select * from t union all select * from t) t2 on t1.b=t2.a;
explain performance select a, b, c, d, e from (select a,b,c,d,e from t union all select a+100001,b+100001,c+100001,d+100001,e+100001 from t union all
select a+200001,b+200001,c+200001,d+200001,e+200001 from t union all select a+300001,b+300001,c+300001,d+300001,e+300001 from t union all
select a,b,c,d,e from t union all select a+100001,b+100001,c+100001,d+100001,e+100001 from t union all
select a+200001,b+200001,c+200001,d+200001,e+200001 from t union all select a+300001,b+300001,c+300001,d+300001,e+300001 from t)
group by 1,2,3,4,5 order by 1,2,3,4,5 limit 5 offset 400000;

create table t1_col(a int, b int, c int, d int, e int) with (orientation=column);
create table t_col(a int, b int, c int, d int, e int) with (orientation=column);
insert into t1_col select * from t1;
insert into t_col select * from t;
analyze t1_col;
analyze t_col;
update pg_class set reltuples=1000000 where relname='t1_col' and relnamespace=(select oid from pg_namespace where nspname='distribute_joinplan');
update pg_class set reltuples=100 where relname='t_col' and relnamespace=(select oid from pg_namespace where nspname='distribute_joinplan');
set explain_perf_mode=summary;
explain performance select * from t1_col join (select * from t_col union all select * from t_col union all select * from t_col union all select * from t_col) t2 on t1_col.b=t2.a;
explain performance select a, b, c, d, e from (select a,b,c,d,e from t_col union all select a+100001,b+100001,c+100001,d+100001,e+100001 from t_col union all
select a+200001,b+200001,c+200001,d+200001,e+200001 from t_col union all select a+300001,b+300001,c+300001,d+300001,e+300001 from t_col union all
select a,b,c,d,e from t_col union all select a+100001,b+100001,c+100001,d+100001,e+100001 from t_col union all
select a+200001,b+200001,c+200001,d+200001,e+200001 from t_col union all select a+300001,b+300001,c+300001,d+300001,e+300001 from t_col) 
group by 1,2,3,4,5 order by 1,2,3,4,5 limit 5 offset 400000;

drop schema  distribute_joinplan cascade;
