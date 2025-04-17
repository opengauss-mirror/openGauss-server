set enable_default_ustore_table = on;


-- test: build index
drop table if exists t1;
create table t1(c1 int) with (storage_type=ustore);
insert into t1 values(generate_series(1,400));
create index t1_idx1 on t1(c1) with (index_type=pcr);
\di+
explain analyze select /*+ indexonlyscan(t1 t1_idx1)*/ count(*) from t1;
select /*+ indexonlyscan(t1 t1_idx1)*/ count(*) from t1;
select count(*) from t1;


-- test: build empty
drop table if exists t1;
create table t1(c1 int) with (storage_type=ustore);
create index t1_idx1 on t1(c1) with (index_type=pcr);
insert into t1 values(generate_series(1,400));
select /*+ indexonlyscan(t1 t1_idx1)*/ count(*) from t1;
select count(*) from t1;

-- test: build with table
drop table if exists t1;
create table t1(c1 int primary key) with (storage_type=ustore, index_type=pcr);
\d+
\di+
insert into t1 values(generate_series(1,400));
\di+
select /*+ indexonlyscan(t1 t1_pkey)*/ count(*) from t1;
select count(*) from t1;


-- test: split
drop table if exists t1;
create table t1(c1 int) with (storage_type=ustore);
insert into t1 values(generate_series(1,400));
create index t1_idx1 on t1(c1) with (index_type=pcr);
delete t1 where c1 > 200;
select /*+ indexonlyscan(t1 t1_idx1)*/ count(*) from t1;
select count(*) from t1;
insert into t1 values(generate_series(201,800));
select /*+ indexonlyscan(t1 t1_idx1)*/ count(*) from t1;
select count(*) from t1;


-- test：extend td
drop table if exists t1;
create table t1(c1 int) with (storage_type=ustore);
create index t1_idx1 on t1(c1) with (index_type=pcr);
\parallel on
insert into t1 values(generate_series(1,50));
insert into t1 values(generate_series(51,100));
insert into t1 values(generate_series(101,150));
insert into t1 values(generate_series(151,200));
insert into t1 values(generate_series(201,250));
\parallel off
select /*+ indexonlyscan(t1 t1_idx1)*/ count(*) from t1;
select count(*) from t1;