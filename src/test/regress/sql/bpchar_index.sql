SET client_min_messages = warning;
-- test char semantic btree index and hash index
drop table if exists test_char_index;
create table test_char_index (id int, city char (10 char));
create unique index test_char_index_btree on test_char_index using btree(city);
create index test_char_index_hash on test_char_index using hash(city);
insert into test_char_index values(1, 'beijing');
insert into test_char_index values(2, 'shanghai');
insert into test_char_index values(3, 'tianjin');
insert into test_char_index values(4, 'guangzhou');
explain (verbose, costs off) select * from test_char_index where city = 'beijing';
explain (verbose, costs off) select * from test_char_index where city > 'beijing';
explain (verbose, costs off) select * from test_char_index where city > 'beijing' order by city limit 1;
explain (verbose, costs off) select count(city) from test_char_index where city > 'beijing';
drop table test_char_index;

--test char semantic ustore using ubtree index
drop table if exists test_char_ubtree;
create table test_char_ubtree (id int, city char (10 char)) with (storage_type=ustore);
create unique index test_char_ubtree_btree_index on test_char_ubtree using ubtree(city);
insert into test_char_ubtree values(1, 'beijing');
insert into test_char_ubtree values(2, 'shanghai');
insert into test_char_ubtree values(3, 'tianjin');
insert into test_char_ubtree values(4, 'guangzhou');
explain (verbose, costs off) select * from test_char_ubtree where city = 'beijing';
explain (verbose, costs off) select * from test_char_ubtree where city > 'beijing';
drop table test_char_ubtree;

-- test char semantic cstore using cbtree index
drop table if exists test_char_cbtree;
create table test_char_cbtree (id int, city char (10 char)) with (orientation=column);
create unique index test_char_cbtree_btree_index on test_char_cbtree using btree(city);
insert into test_char_cbtree values(1, 'beijing');
insert into test_char_cbtree values(2, 'shanghai');
insert into test_char_cbtree values(3, 'tianjin');
insert into test_char_cbtree values(4, 'guangzhou');
explain (verbose, costs off) select city from test_char_cbtree where city = 'beijing';
explain (verbose, costs off) select city from test_char_cbtree where city > 'beijing';
drop table test_char_cbtree;

-- test char semantic partition table using btree index
drop table if exists test_char_partition;
create table test_char_partition (id int, city char (10 char)) partition by range (city) ( partition p1 values less than ('dalian'), partition p2 values less than (MAXVALUE ));
create index test_char_partition_btree_index on test_char_partition using btree(city) local;
insert into test_char_partition values(1, 'beijing');
insert into test_char_partition values(2, 'shanghai');
insert into test_char_partition values(3, 'tianjin');
insert into test_char_partition values(4, 'guangzhou');
explain (verbose, costs off) select city from test_char_partition where city = 'beijing';
explain (verbose, costs off) select city from test_char_partition where city > 'beijing';
drop index test_char_partition_btree_index;
create index test_char_partition_btree_index on test_char_partition using btree(city) global;
explain (verbose, costs off) select city from test_char_partition where city = 'beijing';
explain (verbose, costs off) select city from test_char_partition where city > 'beijing';
create index test_char_partition_hash_index on test_char_partition using hash(city) local;
explain (verbose, costs off) select city from test_char_partition where city = 'beijing';
explain (verbose, costs off) select city from test_char_partition where city > 'beijing';
drop table test_char_partition;

-- test byte semantic btree index and hash index
drop table if exists test_byte_index;
create table test_byte_index (id int, city char (10 byte));
create unique index test_byte_index_btree on test_byte_index using btree(city);
create index test_byte_index_hash on test_byte_index using hash(city);
insert into test_byte_index values(1, 'beijing');
insert into test_byte_index values(2, 'shanghai');
insert into test_byte_index values(3, 'tianjin');
insert into test_byte_index values(4, 'guangzhou');
explain (verbose, costs off) select * from test_byte_index where city = 'beijing';
explain (verbose, costs off) select * from test_byte_index where city > 'beijing';
explain (verbose, costs off) select * from test_byte_index where city > 'beijing' order by city limit 1;
explain (verbose, costs off) select count(city) from test_byte_index where city > 'beijing';
drop table test_byte_index;

--test byte semantic ustore using ubtree index
drop table if exists test_byte_ubtree;
create table test_byte_ubtree (id int, city char (10 byte)) with (storage_type=ustore);
create unique index test_byte_ubtree_btree_index on test_byte_ubtree using ubtree(city);
insert into test_byte_ubtree values(1, 'beijing');
insert into test_byte_ubtree values(2, 'shanghai');
insert into test_byte_ubtree values(3, 'tianjin');
insert into test_byte_ubtree values(4, 'guangzhou');
explain (verbose, costs off) select * from test_byte_ubtree where city = 'beijing';
explain (verbose, costs off) select * from test_byte_ubtree where city > 'beijing';
drop table test_byte_ubtree;

-- test byte semantic cstore using cbtree index
drop table if exists test_byte_cbtree;
create table test_byte_cbtree (id int, city char(10 byte)) with (orientation=column);
create unique index test_byte_cbtree_btree_index on test_byte_cbtree using btree(city);
insert into test_byte_cbtree values(1, 'beijing');
insert into test_byte_cbtree values(2, 'shanghai');
insert into test_byte_cbtree values(3, 'tianjin');
insert into test_byte_cbtree values(4, 'guangzhou');
explain (verbose, costs off) select city from test_byte_cbtree where city = 'beijing';
explain (verbose, costs off) select city from test_byte_cbtree where city > 'beijing';
drop table test_byte_cbtree;

-- test byte semantic partition table using btree index
drop table if exists test_byte_partition;
create table test_byte_partition (id int, city char (10 byte)) partition by range (city) ( partition p1 values less than ('dalian'), partition p2 values less than (MAXVALUE ));
create index test_byte_partition_btree_index on test_byte_partition using btree(city) local;
insert into test_byte_partition values(1, 'beijing');
insert into test_byte_partition values(2, 'shanghai');
insert into test_byte_partition values(3, 'tianjin');
insert into test_byte_partition values(4, 'guangzhou');
explain (verbose, costs off) select city from test_byte_partition where city = 'beijing';
explain (verbose, costs off) select city from test_byte_partition where city > 'beijing';
drop index test_byte_partition_btree_index;
create index test_byte_partition_btree_index on test_byte_partition using btree(city) global;
explain (verbose, costs off) select city from test_byte_partition where city = 'beijing';
explain (verbose, costs off) select city from test_byte_partition where city > 'beijing';
create index test_byte_partition_hash_index on test_byte_partition using hash(city) local;
explain (verbose, costs off) select city from test_byte_partition where city = 'beijing';
explain (verbose, costs off) select city from test_byte_partition where city > 'beijing';
drop table test_byte_partition;

set enable_seqscan=off;
set enable_heap_prefetch=on;
drop table if exists test_char_index;
create table test_char_index (id int, city char (10 char));
create unique index test_char_index_btree on test_char_index using btree(city);
insert into test_char_index values(1, 'beijing');
insert into test_char_index values(2, 'shanghai');
insert into test_char_index values(3, 'tianjin');
insert into test_char_index values(4, 'guangzhou');
select * from test_char_index where city > 'beijing' limit 2;
select * from test_char_index limit 3;
select * from test_char_index where city > 'beijing';
drop table test_char_index;
set enable_heap_prefetch=off;
set enable_seqscan=on;
