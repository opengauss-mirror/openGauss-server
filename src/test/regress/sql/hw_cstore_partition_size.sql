
---- test function for cstore partition size:
-- 1. pg_table_size
-- 2. pg_relation_size
-- 3. pg_indexes_size
-- 4. pg_total_relation_size
-- 5. pg_partition_size
-- 5. pg_partition_indexes_size


---- 1. pg_table_size
create table test_cstore_pg_table_size (a int)
with(orientation = column)
partition by range (a)
(
partition test_cstore_pg_table_size_p1 values less than (2),
partition test_cstore_pg_table_size_p2 values less than (4)
);

insert into test_cstore_pg_table_size values (1), (3);

select pg_table_size('test_cstore_pg_table_size') > 0;

-- b. test table has index

create index test_cstore_pg_table_size_index on test_cstore_pg_table_size(a) local;

insert into test_cstore_pg_table_size values (1), (3);

select pg_table_size('test_cstore_pg_table_size_index') > 0;

---- 2. pg_relation_size

select pg_relation_size('test_cstore_pg_table_size') > 0;

select pg_relation_size('test_cstore_pg_table_size_index') > 0;

drop table test_cstore_pg_table_size;


---- 3. pg_indexes_size
create table test_cstore_pg_indexes_size(a int, b int)
with(orientation = column)
partition by range (a, b)
(
partition test_cstore_pg_indexes_size_p1 values less than (2, 2),
partition test_cstore_pg_indexes_size_p2 values less than (4, 4)
);

create index test_cstore_pg_indexes_size_index_a on test_cstore_pg_indexes_size(a) local;
create index test_cstore_pg_indexes_size_index_b on test_cstore_pg_indexes_size (b) local;

insert into test_cstore_pg_indexes_size values (1, 1), (3, 3);

select pg_indexes_size('test_cstore_pg_indexes_size') > 0;

---- 4. pg_total_relation_size
select pg_total_relation_size('test_cstore_pg_indexes_size') = pg_table_size('test_cstore_pg_indexes_size') + pg_indexes_size('test_cstore_pg_indexes_size');

drop table test_cstore_pg_indexes_size;

-- 5. pg_partition_size
create table test_cstore_pg_partition_size (a int)
with(orientation = column)
partition by range (a)
(
	partition test_cstore_pg_partition_size_p1 values less than (4)
);

insert into test_cstore_pg_partition_size values (1);

select pg_partition_size('test_cstore_pg_partition_size', 'test_cstore_pg_partition_size_p1')>0;
select pg_partition_size(a.oid, b.oid)>0 from pg_class a, pg_partition b where a.oid=b.parentid and a.relname='test_cstore_pg_partition_size' and b.relname='test_cstore_pg_partition_size_p1';
--ERROR
select pg_partition_size('test_cstore_pg_partition_size', 19000)>0;
--ERROR
select pg_partition_size('test_cstore_pg_partition_size', 'test_cstore_pg_partition_size_p2')>0;

drop table test_cstore_pg_partition_size;

-- 6. pg_partition_indexes_size
create table test_cstore_pg_partition_indexes_size (a int)
with(orientation = column)
partition by range (a)
(
	partition test_cstore_pg_partition_indexes_size_p1 values less than (4)
);
create index test_cstore_pg_partition_indexes_size_index on test_cstore_pg_partition_indexes_size (a) local;

insert into test_cstore_pg_partition_indexes_size values (1);

select pg_partition_indexes_size('test_cstore_pg_partition_indexes_size', 'test_cstore_pg_partition_indexes_size_p1')>0;
select pg_partition_indexes_size(a.oid, b.oid)>0 from pg_class a, pg_partition b where a.oid=b.parentid and a.relname='test_cstore_pg_partition_indexes_size' and b.relname='test_cstore_pg_partition_indexes_size_p1';
--ERROR
select pg_partition_indexes_size('test_cstore_pg_partition_indexes_size', 19000)>0;
--ERROR
select pg_partition_indexes_size('test_cstore_pg_partition_indexes_size', 'test_cstore_pg_partition_indexes_size_p2')>0;

drop table test_cstore_pg_partition_indexes_size;
