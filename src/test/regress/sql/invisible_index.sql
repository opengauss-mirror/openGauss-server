/* unsupported */
create schema invisible_index;
set search_path to 'invisible_index';

create table t1 (a int, b int, constraint key_a primary key(a) visible); --error

create table t1 (a int, b int, constraint key_a primary key(a));
alter table t1 alter index key_a invisible; --error
alter table t1 add constraint key_b unique (b) visible; --error

reset search_path;
drop schema invisible_index cascade;

create database invisible_index_db dbcompatibility 'B';
\c invisible_index_db

-- create table with index
create table t1 (a int, b int, constraint key_a primary key(a) visible);
create index key_b on t1 (b) invisible;
alter table t1 alter index key_b visible, alter index key_b invisible;

select indkey, indisvisible from pg_index where indrelid = 't1'::regclass order by indkey;

insert into t1 values (generate_series(1, 100), generate_series(1, 100));
analyze t1;

set enable_seqscan = off;

-- error
alter table key_b alter index key_b invisible;
create view v1 as select * from t1;
alter table v1 alter index key_b invisible;

-- modify invisible status
explain (costs off) select * from t1 where a < 10;
alter table t1 alter index key_a invisible;
explain (costs off) select * from t1 where a < 10;

explain (costs off) select * from t1 where b < 10;
alter table t1 alter index key_b visible;
explain (costs off) select * from t1 where b < 10;

-- test alter table add constraint with visible
drop index key_b;
alter table t1 add constraint key_b unique (b) visible;
explain (costs off) select * from t1 where b < 10;
alter table t1 alter index key_b invisible;
explain (costs off) select * from t1 where b < 10;

-- hybrid test unusable and invisible
alter index key_b unusable;
alter table t1 alter index key_b visible;
explain (costs off) select * from t1 where b < 10;

alter index key_b rebuild;
explain (costs off) select * from t1 where b < 10;

-- test partitioned table
CREATE TABLE t_partition(a int, b int)
PARTITION BY RANGE (a)
(
    PARTITION P1 VALUES LESS THAN(50),
    PARTITION P2 VALUES LESS THAN(100),
    PARTITION P3 VALUES LESS THAN(150)
);
insert into t_partition values (generate_series(1, 100), generate_series(1, 100));
analyze t_partition;

create index global_idx on t_partition(b) global;

explain (costs off) select * from t_partition where b < 10;
alter table t_partition alter index global_idx invisible;
explain (costs off) select * from t_partition where b < 10;

drop index global_idx;

create index local_idx on t_partition(b) local;

explain (costs off) select * from t_partition partition (p1) where b < 10;
alter table t_partition alter index local_idx invisible;
explain (costs off) select * from t_partition partition (p1) where b < 10;

-- test oriented-column table
create table c1 (a int, b int, constraint ckey_a primary key(a) visible) with (orientation = column);
insert into c1 values (generate_series(1, 100), generate_series(1, 100));
analyze c1;

set enable_seqscan = off;

-- modify invisible status
explain (costs off) select * from c1 where a < 10;
alter table c1 alter index ckey_a invisible;
explain (costs off) select * from c1 where a < 10;

-- test other index type
alter table t1 drop constraint key_b;
create index key_b on t1 using hash (b) visible;
explain (costs off) select * from t1 where b = 10;
alter table t1 alter index key_b invisible;
explain (costs off) select * from t1 where b = 10;

create table test_cgin_t1(id int, name varchar(1000)) with (orientation=column);
insert into test_cgin_t1 values(1, 'apple');
insert into test_cgin_t1 values(2, 'pear');
insert into test_cgin_t1 values(3, 'apple pear');
create index cgin_idx_1 on test_cgin_t1 using gin(to_tsvector('ngram',name)) with (fastupdate = on);

explain (costs off) select count(*) from test_cgin_t1 where to_tsvector('ngram',name)@@to_tsquery('ngram','pear');
alter table test_cgin_t1 alter index cgin_idx_1 invisible;
explain (costs off) select count(*) from test_cgin_t1 where to_tsvector('ngram',name)@@to_tsquery('ngram','pear');

\c regression
drop database invisible_index_db;