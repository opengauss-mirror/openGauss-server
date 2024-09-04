SET client_min_messages = warning;
-- test varchar semantic btree index and hash index
drop table if exists test_varchar_index;
create table test_varchar_index (id int, city varchar (10 char));
create unique index test_varchar_index_btree on test_varchar_index using btree(city);
create index test_varchar_index_hash on test_varchar_index using hash(city);
insert into test_varchar_index values(1, 'beijing');
insert into test_varchar_index values(2, 'shanghai');
insert into test_varchar_index values(3, 'tianjin');
insert into test_varchar_index values(4, 'guangzhou');
explain (verbose, costs off) select * from test_varchar_index where city = 'beijing';
explain (verbose, costs off) select * from test_varchar_index where city > 'beijing';
drop table test_varchar_index;

--test varchar semantic ustore using ubtree index
drop table if exists test_varchar_ubtree;
create table test_varchar_ubtree (id int, city varchar (10 char)) with (storage_type=ustore);
create unique index test_varchar_ubtree_btree_index on test_varchar_ubtree using ubtree(city);
insert into test_varchar_ubtree values(1, 'beijing');
insert into test_varchar_ubtree values(2, 'shanghai');
insert into test_varchar_ubtree values(3, 'tianjin');
insert into test_varchar_ubtree values(4, 'guangzhou');
explain (verbose, costs off) select * from test_varchar_ubtree where city = 'beijing';
explain (verbose, costs off) select * from test_varchar_ubtree where city > 'beijing';
drop table test_varchar_ubtree;

-- test varchar semantic cstore using cbtree index
drop table if exists test_varchar_cbtree;
create table test_varchar_cbtree (id int, city varchar (10 char)) with (orientation=column);
create unique index test_varchar_cbtree_btree_index on test_varchar_cbtree using btree(city);
insert into test_varchar_cbtree values(1, 'beijing');
insert into test_varchar_cbtree values(2, 'shanghai');
insert into test_varchar_cbtree values(3, 'tianjin');
insert into test_varchar_cbtree values(4, 'guangzhou');
explain (verbose, costs off) select city from test_varchar_cbtree where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_cbtree where city > 'beijing';
drop table test_varchar_cbtree;

-- test varchar semantic partition table using btree index
drop table if exists test_varchar_partition;
create table test_varchar_partition (id int, city varchar (10 char)) partition by range (city) ( partition p1 values less than ('dalian'), partition p2 values less than (MAXVALUE ));
create index test_varchar_partition_btree_index on test_varchar_partition using btree(city) local;
insert into test_varchar_partition values(1, 'beijing');
insert into test_varchar_partition values(2, 'shanghai');
insert into test_varchar_partition values(3, 'tianjin');
insert into test_varchar_partition values(4, 'guangzhou');
explain (verbose, costs off) select city from test_varchar_partition where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_partition where city > 'beijing';
drop index test_varchar_partition_btree_index;
create index test_varchar_partition_btree_index on test_varchar_partition using btree(city) global;
explain (verbose, costs off) select city from test_varchar_partition where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_partition where city > 'beijing';
create index test_varchar_partition_hash_index on test_varchar_partition using hash(city) local;
explain (verbose, costs off) select city from test_varchar_partition where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_partition where city > 'beijing';
drop table test_varchar_partition;

-- test byte semantic btree index and hash index
drop table if exists test_varchar_byte_index;
create table test_varchar_byte_index (id int, city varchar (10 byte));
create unique index test_varchar_byte_index_btree on test_varchar_byte_index using btree(city);
create index test_varchar_byte_index_hash on test_varchar_byte_index using hash(city);
insert into test_varchar_byte_index values(1, 'beijing');
insert into test_varchar_byte_index values(2, 'shanghai');
insert into test_varchar_byte_index values(3, 'tianjin');
insert into test_varchar_byte_index values(4, 'guangzhou');
explain (verbose, costs off) select * from test_varchar_byte_index where city = 'beijing';
explain (verbose, costs off) select * from test_varchar_byte_index where city > 'beijing';
drop table test_varchar_byte_index;

--test byte semantic ustore using ubtree index
drop table if exists test_varchar_byte_ubtree;
create table test_varchar_byte_ubtree (id int, city varchar (10 byte)) with (storage_type=ustore);
create unique index test_varchar_byte_ubtree_btree_index on test_varchar_byte_ubtree using ubtree(city);
insert into test_varchar_byte_ubtree values(1, 'beijing');
insert into test_varchar_byte_ubtree values(2, 'shanghai');
insert into test_varchar_byte_ubtree values(3, 'tianjin');
insert into test_varchar_byte_ubtree values(4, 'guangzhou');
explain (verbose, costs off) select * from test_varchar_byte_ubtree where city = 'beijing';
explain (verbose, costs off) select * from test_varchar_byte_ubtree where city > 'beijing';
drop table test_varchar_byte_ubtree;

-- test byte semantic cstore using cbtree index
drop table if exists test_varchar_byte_cbtree;
create table test_varchar_byte_cbtree (id int, city varchar(10 byte)) with (orientation=column);
create unique index test_varchar_byte_cbtree_btree_index on test_varchar_byte_cbtree using btree(city);
insert into test_varchar_byte_cbtree values(1, 'beijing');
insert into test_varchar_byte_cbtree values(2, 'shanghai');
insert into test_varchar_byte_cbtree values(3, 'tianjin');
insert into test_varchar_byte_cbtree values(4, 'guangzhou');
explain (verbose, costs off) select city from test_varchar_byte_cbtree where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_byte_cbtree where city > 'beijing';
drop table test_varchar_byte_cbtree;

-- test byte semantic partition table using btree index
drop table if exists test_varchar_byte_partition;
create table test_varchar_byte_partition (id int, city varchar (10 char)) partition by range (city) ( partition p1 values less than ('dalian'), partition p2 values less than (MAXVALUE ));
create index test_varchar_byte_partition_btree_index on test_varchar_byte_partition using btree(city) local;
insert into test_varchar_byte_partition values(1, 'beijing');
insert into test_varchar_byte_partition values(2, 'shanghai');
insert into test_varchar_byte_partition values(3, 'tianjin');
insert into test_varchar_byte_partition values(4, 'guangzhou');
explain (verbose, costs off) select city from test_varchar_byte_partition where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_byte_partition where city > 'beijing';
drop index test_varchar_byte_partition_btree_index;
create index test_varchar_byte_partition_btree_index on test_varchar_byte_partition using btree(city) global;
explain (verbose, costs off) select city from test_varchar_byte_partition where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_byte_partition where city > 'beijing';
create index test_varchar_byte_partition_hash_index on test_varchar_byte_partition using hash(city) local;
explain (verbose, costs off) select city from test_varchar_byte_partition where city = 'beijing';
explain (verbose, costs off) select city from test_varchar_byte_partition where city > 'beijing';
drop table test_varchar_byte_partition;