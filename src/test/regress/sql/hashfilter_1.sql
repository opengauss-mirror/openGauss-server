create schema hashfilter_1;
set current_schema = hashfilter_1;

--one-time filter is false in one of branch of setop
START TRANSACTION;
create table t1 (a int, d int);
create table t2 (a int8, d int);
create view v1 as select * from t1 union all select * from t1;
create view v2 as select * from t2 union all select * from t2;
create view v as 
select * from v1 where d > 6 and d < 9
union all
select * from v2 where d > 9 and d < 11;

DROP table t1 cascade;
DROP table t2 cascade;

create table t1 (col_int integer,col_numeric numeric,col_varchar varchar,col_date date) with(orientation=column);
create table t2 (col_int integer,col_numeric numeric,col_varchar varchar,col_date date) with(orientation=column);
insert into t1 values(1,1,'aaa','2010-10-01');
insert into t1 values(2,2,'bbb','2010-11-01');
insert into t1 values(3,3,'ccc','2010-12-01');
insert into t2 select * from t1;

--hashfilter expr select other qual
explain (costs off) select t1.* from t1 right join t2 on t2.col_int = trunc(t1.col_numeric) and t1.col_varchar like 'a%' where t2.col_date is not null and t2.col_int < 750 order by t1.col_int;
select t1.* from t1 right join t2 on t2.col_int = trunc(t1.col_numeric) and t1.col_varchar like 'a%' where t2.col_date is not null and t2.col_int < 750 order by t1.col_int;

COMMIT;

--drop schema
reset plan_mode_seed;

--hashfilter for muti-nodegroup


-- clean up
reset current_schema;
drop schema hashfilter_1 cascade;
