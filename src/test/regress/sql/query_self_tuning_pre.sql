\c postgres
create schema sql_self_tuning;
set current_schema='sql_self_tuning';


create table t1 ( c1 integer[], c2 int) with(autovacuum_enabled = off);
create table t3(c1 text, c2 text, c3 int) with(autovacuum_enabled = off);

create table t5(c1 int, c2 int, c3 int, c4 int) with(autovacuum_enabled = off);
insert into t5 select v % 5,v,v, 0 from generate_series(1,1024) as v;
create table t4 with(autovacuum_enabled = off) as select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
insert into t5 select * from t5;
create table t13 with(autovacuum_enabled = off) as select * from t5;
insert into t13 select * from t13;
insert into t13 select * from t13;
create table t14(c1 int, c2 int, c3 int, c4 int) with(autovacuum_enabled = off);
create table t15(c1 int, c2 int, c3 int, c4 int) with(autovacuum_enabled = off);
insert into t14 select * from t5;
insert into t15 select * from t5;
analyze t5;
analyze t13;
analyze t14;
analyze t15;

create table t6(c1 bytea, c2 bytea,c3 int) with(autovacuum_enabled = off);

create table t16(c1 int, c2 int, c3 int, c4 int) with(autovacuum_enabled = off);
insert into t16 select * from t5;
create index idx_t16 on t16(c4);
create table ct16(c1 int, c2 int, c3 int, c4 int) with(autovacuum_enabled = off, ORIENTATION = column);
insert into ct16 select * from t5;
create index idx_ct16 on ct16(c1);

\c regression

