CREATE SCHEMA IF NOT EXISTS test_ddl;
SET current_schema = test_ddl;

drop table if exists online_ddl_test_error;
create table online_ddl_test_error (id int, value1 varchar, value2 int);
-- case 1: alter column type in transaction block
begin;
alter table concurrently online_ddl_test_error alter column value1 type int; --error
rollback;

-- case 2: alter set column not null in transaction block
begin;
alter table concurrently online_ddl_test_error alter column value2 set not null; --error
rollback;

-- case 3: alter column type in subtransaction block
begin;
savepoint sp1;
alter table concurrently online_ddl_test_error alter column value1 type int; --error
rollback;

-- case 4: alter set column not null in subtransaction block
begin;
savepoint sp1;
alter table concurrently online_ddl_test_error alter column value2 set not null; --error
rollback;

-- case 5: alter command with no concurrently gram
alter table concurrently online_ddl_test_error rename column value1 to value_1; --error
alter table concurrently online_ddl_test_error rename column value_1 to value1; --error

drop table if exists online_ddl_test_error;

-- case 6: alter column type with invalid value
drop table if exists online_ddl_test_invalid;

-- case 7: alter column set not null with null value
create table online_ddl_test_not_null (id int, value1 varchar, value2 int);
insert into online_ddl_test_not_null select generate_series(1, 10), 'test', 111;
insert into online_ddl_test_not_null values (11, 'test_null', null);
alter table concurrently online_ddl_test_not_null alter column value2 set not null; --error
drop table if exists online_ddl_test_not_null;

-- case 8: alter column set range constraint with out of range value
create table online_ddl_test_range (id int, value1 varchar, value2 int);
insert into online_ddl_test_range select generate_series(1, 10), 'test', 9;
insert into online_ddl_test_range values (11, 'test_out_of_range', 11);
alter table concurrently online_ddl_test_range
add constraint ck_online_ddl_test_range check (value2 > 1 and value2 < 10); --error
drop table if exists online_ddl_test_range;

drop table if exists online_ddl_test_skip;
create table online_ddl_test_skip (id int, value1 varchar, value2 int);
-- case 9: alter command no need to rewrite table
-- add column and drop column
alter table concurrently online_ddl_test_skip alter column id set default 100; --skip
alter table concurrently online_ddl_test_skip add column value3 int; --skip
alter table concurrently online_ddl_test_skip drop column value3; --skip
alter table concurrently online_ddl_test_skip add column value3 int, add column value4 varchar(20); --skip
alter table concurrently online_ddl_test_skip drop column value3, drop column value4; --skip
-- add column with default and not null
alter table concurrently online_ddl_test_skip add column register_time int not null default 10; --skip
alter table concurrently online_ddl_test_skip drop column register_time; --skip
-- rename column
alter table concurrently online_ddl_test_skip rename column value1 to value_1; --error
alter table concurrently online_ddl_test_skip rename column value_1 to value1; --error
-- add constraint unique
alter table concurrently online_ddl_test_skip add constraint uq_online_ddl_test_skip unique (value2); --skip
alter table concurrently online_ddl_test_skip drop constraint uq_online_ddl_test_skip; --skip
-- add primary key
alter table concurrently online_ddl_test_skip add constraint pk_online_ddl_test_skip primary key (id); --skip
alter table concurrently online_ddl_test_skip drop constraint pk_online_ddl_test_skip; --skip

-- case 10: alter column add constraint with valid data
create table online_ddl_test_valid (id int, value1 varchar, value2 int);
insert into online_ddl_test_valid select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_valid  alter column value2 set not null; --success
\d+ online_ddl_test_valid
select * from online_ddl_test_valid order by id;
drop table if exists online_ddl_test_valid;

create table online_ddl_test_valid (id int, value1 varchar, value2 int);
insert into online_ddl_test_valid select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_valid
add constraint ck_online_ddl_test_valid check (value2 > 1 and value2 < 10); --success
\d+ online_ddl_test_valid
select * from online_ddl_test_valid order by id;
drop table if exists online_ddl_test_valid;

-- case 11: alter table concurrently set row compression
-- no need to rewrite table
create table online_ddl_test_compress (id int, value1 varchar, value2 int);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress set (compress_level=0); --skip
\d+ online_ddl_test_compress
drop table if exists online_ddl_test_compress;

create table online_ddl_test_compress (id int, value1 varchar, value2 int) with (compresstype = 2, compress_level = 15);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress set (compresstype=2, compress_level = 15); --skip
\d+ online_ddl_test_compresss
drop table if exists online_ddl_test_compress;

-- no compress to compress
create table online_ddl_test_compress (id int, value1 varchar, value2 int);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress set (compresstype=2, compress_level = 15); --success
\d+ online_ddl_test_compress
drop table if exists online_ddl_test_compress;

-- indexed table no compress to compress
create table online_ddl_test_compress (id int, value1 varchar, value2 int);
create index idx_online_ddl_test_compress_value1 on online_ddl_test_compress using btree (value1);
create index idx_online_ddl_test_compress_value2 on online_ddl_test_compress using hash (value2);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress set (compresstype=2, compress_level = 15); --success
\d+ online_ddl_test_compress
drop table if exists online_ddl_test_compress;

-- compress to no compress
create table online_ddl_test_compress (id int, value1 varchar, value2 int) with (compresstype = 2, compress_level = 15);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress reset (compresstype, compress_level); --success
\d+ online_ddl_test_compress
drop table if exists online_ddl_test_compress;

--index table compress to no compress
create table online_ddl_test_compress (id int, value1 varchar, value2 int) with (compresstype = 2, compress_level = 15);
create index idx_online_ddl_test_compress_value1 on online_ddl_test_compress using btree (value1);
create index idx_online_ddl_test_compress_value2 on online_ddl_test_compress using hash (value2);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress reset (compresstype, compress_level); --success
\d+ online_ddl_test_compress    
drop table if exists online_ddl_test_compress;

-- alter compress level
create table online_ddl_test_compress (id int, value1 varchar, value2 int) with (compresstype = 2, compress_level = 10);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress set (compress_level = 5); --success
\d+ online_ddl_test_compress
drop table if exists online_ddl_test_compress;

--indexed table alter compress level
create table online_ddl_test_compress (id int, value1 varchar, value2 int) with (compresstype = 2, compress_level = 10);
create index idx_online_ddl_test_compress_value1 on online_ddl_test_compress using btree (value1);
create index idx_online_ddl_test_compress_value2 on online_ddl_test_compress using hash (value2);
insert into online_ddl_test_compress select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_compress set (compress_level = 5); --success
\d+ online_ddl_test_compress
drop table if exists online_ddl_test_compress;

-- case 12: alter table concurrently alter column type with valid data
-- int to varchar and varchar to int
create table online_ddl_test_alter_type (id int, value1 varchar, value2 int);
insert into online_ddl_test_alter_type select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_alter_type alter column value2 type varchar; --success
\d+ online_ddl_test_alter_type
select * from online_ddl_test_alter_type order by id;
drop table if exists online_ddl_test_alter_type;

create table online_ddl_test_alter_type (id int, value1 varchar, value2 varchar);
insert into online_ddl_test_alter_type select generate_series(1, 10), 'test', '1000';
alter table concurrently online_ddl_test_alter_type alter column value2 type int; --success
\d+ online_ddl_test_alter_type
select * from online_ddl_test_alter_type order by id;
drop table if exists online_ddl_test_alter_type;

-- indexed table varchar to int
create table online_ddl_test_alter_type (id int, value1 varchar, value2 varchar(10));
create index idx_online_ddl_test_alter_type_value2 on online_ddl_test_alter_type using btree (value1);
create index idx_online_ddl_test_alter_type_value2_hash on online_ddl_test_alter_type using hash (value1);
insert into online_ddl_test_alter_type select generate_series(1, 10), 'test', '1000';
alter table concurrently online_ddl_test_alter_type alter column value2 type int; --success
\d+ online_ddl_test_alter_type
drop table if exists online_ddl_test_alter_type;

-- int to bigint and bigint to int
create table online_ddl_test_alter_type (id int, value1 varchar, value2 int);
insert into online_ddl_test_alter_type select generate_series(1, 10), 'test', 5;
alter table concurrently online_ddl_test_alter_type alter column value2 type bigint; --success
\d+ online_ddl_test_alter_type
drop table if exists online_ddl_test_alter_type;

create table online_ddl_test_alter_type (id int, value1 varchar, value2 bigint);
insert into online_ddl_test_alter_type select generate_series(1, 10), 'test', 1000;
alter table concurrently online_ddl_test_alter_type alter column value2 type int; --success
\d+ online_ddl_test_alter_type
drop table if exists online_ddl_test_alter_type;

-- indexed table bigint to int
create table online_ddl_test_alter_type (id int, value1 varchar, value2 bigint);
create index idx_online_ddl_test_alter_type_value2 on online_ddl_test_alter_type using btree (value1);
create index idx_online_ddl_test_alter_type_value2_hash on online_ddl_test_alter_type using hash (value1);
insert into online_ddl_test_alter_type select generate_series(1, 10), 'test', 1000;
alter table concurrently online_ddl_test_alter_type alter column value2 type int; --success
\d+ online_ddl_test_alter_type
drop table if exists online_ddl_test_alter_type;

-- indexed table huge number of rows
create table online_ddl_test_alter_type (id int, value1 varchar, value2 varchar(10));
create index idx_online_ddl_test_alter_type_value2 on online_ddl_test_alter_type using btree (value1);
create index idx_online_ddl_test_alter_type_value2_hash on online_ddl_test_alter_type using hash (value1);
insert into online_ddl_test_alter_type select generate_series(1, 3000), 'testtesttesttestesttesttesttest', '1000';
alter table concurrently online_ddl_test_alter_type alter column value2 type int; --success
\d+ online_ddl_test_alter_type
select count(*) from online_ddl_test_alter_type;
drop table if exists online_ddl_test_alter_type;

-- indexed table with toast data
create table online_ddl_test_alter_type_toast (id int, value1 varchar, value2 int, value3 varchar);
create index idx_online_ddl_test_alter_type_toast_value2 on online_ddl_test_alter_type_toast using btree (value2);
create index idx_online_ddl_test_alter_type_toast_value2_hash on online_ddl_test_alter_type_toast using hash (value2);
alter table online_ddl_test_alter_type_toast alter value3 set storage external;
insert into online_ddl_test_alter_type_toast select generate_series(1, 10), '112', 1234, 'aaaaaasfsfsfsfsdfsfsfsfsfsdfsd
sfsfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffssssssssssssssssssssssssssssfffffffffffffffffffffffffffff
fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffssssssssssssssssssssssssssssfffffffffffffffffffff
fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffssssssssssssssssssssssssssssfffffffffffff
fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffssssssssssssssssssssssssssssfffff
fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffsssssssssssssssssssssssss
sssffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffsssssssssssssssss
sssssssssssffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffsssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
ssssssssssssssssssssssssssssssssssssssssffffffffffffffffffffsfsfsfsssssssssssss';
select count(*) from pg_class where oid = (select reltoastrelid from pg_class where
relname = 'online_ddl_test_alter_type_toast');
alter table concurrently online_ddl_test_alter_type_toast alter column value1 type int; --success
\d+ online_ddl_test_alter_type_toast
drop table if exists online_ddl_test_alter_type_toast;

-- case 13: alter table concurrently alter column type with invalid data
create table online_ddl_test_alter_type (id int, value1 varchar, value2 varchar);
insert into online_ddl_test_alter_type select generate_series(1, 10), 'test', 'not number';
alter table concurrently online_ddl_test_alter_type alter column value2 type int; --error
\d+ online_ddl_test_alter_type
drop table if exists online_ddl_test_alter_type;

-- case 14: vacuum full concurrently for ordinary table
set client_min_messages = warning;
create table online_ddl_vacuum_t (id int, grp int, note text);
create index online_ddl_vacuum_t_idx on online_ddl_vacuum_t (id);
insert into online_ddl_vacuum_t select generate_series(1, 20), 1, 'before vacuum';
update online_ddl_vacuum_t set note = 'updated before vacuum' where id <= 5;
delete from online_ddl_vacuum_t where id between 6 and 10;
vacuum full concurrently online_ddl_vacuum_t;
select count(*) as row_count, min(id) as min_id, max(id) as max_id from online_ddl_vacuum_t;
select count(*) as index_count from pg_indexes where schemaname = 'test_ddl' and tablename = 'online_ddl_vacuum_t';
select count(*) from online_ddl_status();
drop table if exists online_ddl_vacuum_t;

-- case 15: cluster concurrently for ordinary table
create table online_ddl_cluster_t (id int, note text);
create index online_ddl_cluster_t_idx on online_ddl_cluster_t (id);
insert into online_ddl_cluster_t select generate_series(20, 1, -1), 'before cluster';
cluster concurrently online_ddl_cluster_t using online_ddl_cluster_t_idx;
select count(*) as row_count, min(id) as min_id, max(id) as max_id, sum(id) as sum_id from online_ddl_cluster_t;
select indisclustered from pg_index where indexrelid = 'online_ddl_cluster_t_idx'::regclass;
drop table if exists online_ddl_cluster_t;

-- case 16: vacuum full concurrently for partitioned table
create table online_ddl_vacuum_part (
    id int not null,
    grp int,
    note text
) partition by range (id)
(
    partition p1 values less than (10),
    partition p2 values less than (20),
    partition p3 values less than (maxvalue)
);
create index online_ddl_vacuum_part_idx on online_ddl_vacuum_part (id) local;
insert into online_ddl_vacuum_part select generate_series(1, 25), 1, 'before partition vacuum';
delete from online_ddl_vacuum_part where id in (2, 12, 22);
vacuum full concurrently online_ddl_vacuum_part;
select count(*) as row_count, min(id) as min_id, max(id) as max_id from online_ddl_vacuum_part;
drop table if exists online_ddl_vacuum_part;

-- case 17: cluster concurrently for partitioned table
create table online_ddl_cluster_part (
    id int not null,
    grp int,
    note text
) partition by range (id)
(
    partition p1 values less than (10),
    partition p2 values less than (20),
    partition p3 values less than (maxvalue)
);
create index online_ddl_cluster_part_idx on online_ddl_cluster_part (id) local;
insert into online_ddl_cluster_part select generate_series(18, 1, -1), 1, 'before partition cluster';
cluster concurrently online_ddl_cluster_part using online_ddl_cluster_part_idx;
select count(*) as row_count, min(id) as min_id, max(id) as max_id, sum(id) as sum_id from online_ddl_cluster_part;
drop table if exists online_ddl_cluster_part;
set client_min_messages = notice;

DROP SCHEMA IF EXISTS test_ddl CASCADE;
