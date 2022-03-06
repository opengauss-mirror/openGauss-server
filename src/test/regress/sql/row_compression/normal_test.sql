create schema normal_test;
CREATE TABLE normal_test.tbl_pc(id int, c1 text) WITH(compresstype=1);
\d+ normal_test.tbl_pc
INSERT INTO normal_test.tbl_pc SELECT id, id::text FROM generate_series(1,1000) id;
select count(*) from normal_test.tbl_pc;
select count(*) from normal_test.tbl_pc where id < 100;
checkpoint;
vacuum normal_test.tbl_pc;
select count(*) from normal_test.tbl_pc;
select count(*) from normal_test.tbl_pc where id < 100;

-- normal index
create index on normal_test.tbl_pc(id) WITH (compresstype=2,compress_chunk_size=1024);
alter index normal_test.tbl_pc_id_idx set (compresstype=1); --failed
alter index normal_test.tbl_pc_id_idx set (compress_chunk_size=2048); --failed
alter index normal_test.tbl_pc_id_idx set (compress_prealloc_chunks=2); --success
alter index normal_test.tbl_pc_id_idx set (compress_level=2); --success

set enable_seqscan = off;
set enable_bitmapscan = off;
select count(*) from normal_test.tbl_pc;
CREATE TABLE normal_test.tbl_partition(id int) WITH(compresstype=2,compress_chunk_size=1024) partition by range(id)
(
    partition p0 values less than(5000),
    partition p1 values less than(10000),
    partition p2 values less than(20000),
    partition p3 values less than(30000),
    partition p4 values less than(40000),
    partition p5 values less than(50000),
    partition p6 values less than(60000),
    partition p7 values less than(70000)
);
insert into normal_test.tbl_partition select generate_series(1,65000);
select count(*) from normal_test.tbl_partition;
checkpoint;
vacuum normal_test.tbl_partition;
select count(*) from normal_test.tbl_partition;

-- exchange
select relname, reloptions from pg_partition where parentid in (Select relfilenode from pg_class where relname like 'tbl_partition') order by relname;
create table normal_test.exchange_table(id int) WITH(compresstype=2,compress_chunk_size=1024);
ALTER TABLE normal_test.tbl_partition EXCHANGE PARTITION FOR(2500) WITH TABLE normal_test.exchange_table;
select count(*) from normal_test.tbl_partition;

-- spilit
ALTER TABLE normal_test.tbl_partition SPLIT PARTITION p1 AT (7500) INTO (PARTITION p10, PARTITION p11);
select relname, reloptions from pg_partition where parentid in (Select relfilenode from pg_class where relname like 'tbl_partition') order by relname;

create index on normal_test.tbl_partition(id) local WITH (compresstype=2,compress_chunk_size=1024);
\d+ normal_test.tbl_partition
select relname, reloptions from pg_partition where parentid in (Select relfilenode from pg_class where relname like 'tbl_partition_id_idx') order by relname;


-- unsupport
alter index normal_test.tbl_partition_id_idx set (compresstype=1);
alter index normal_test.tbl_partition_id_idx set (compress_chunk_size=2048);
alter index normal_test.tbl_partition_id_idx set (compress_prealloc_chunks=2);
create index rolcompress_index on normal_test.tbl_pc(id) with (compress_chunk_size=4096);
create table rolcompress_table_001(a int) with (compresstype=2, compress_prealloc_chunks=3);
-- support
alter table normal_test.tbl_pc set (compress_prealloc_chunks=1);

-- new testcase
set search_path=normal_test;
\d+
reset search_path;
CREATE TABLE normal_test.pre_handle(id int) WITH(compresstype=2, compress_chunk_size=512, compress_byte_convert=true, compress_diff_convert=true);
insert into normal_test.pre_handle select generate_series(1,1000);
checkpoint;
select count(*) from normal_test.pre_handle;
drop schema normal_test cascade;
