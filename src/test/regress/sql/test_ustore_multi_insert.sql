create table t1(c1 int, c2 int) with (storage_type=USTORE);
insert into t1 values(1, 2),(2, 3),(3, 4);

create table t2(c1 int, c2 int) with (storage_type=USTORE);
insert into t2 select * from t1;

create table t3 as select * from t1;

select * into t4 from t1;

-- with ustore_attr = "enable_default_ustore_table=on"

set enable_default_ustore_table = on;

create table t5(c1 int, c2 int);
create table t6(c1 int, c2 int) with (orientation = row);

create table t7 as select * from t1;

select * into t8 from t1;

\d+ t[1-8]

select * from t1 order by c1;
select * from t2 order by c1;
select * from t3 order by c1;
select * from t4 order by c1;
select * from t7 order by c1;
select * from t8 order by c1;

drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t6;
drop table t7;
drop table t8;
