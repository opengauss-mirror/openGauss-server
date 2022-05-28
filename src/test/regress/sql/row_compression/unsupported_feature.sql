create schema unspported_feature;
-- unspport compressType: 3
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compresstype=3, compress_chunk_size=1024);
-- unspport compress_chunk_size: 2000
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_chunk_size=2000);
-- unspport compress_prealloc_chunks: -1
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_prealloc_chunks=-1);
-- unspport compress_prealloc_chunks: 8
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_prealloc_chunks=8);
-- unspport compress_level: 128
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_level=128);
-- compresstype cant be used with column table
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(ORIENTATION = 'column', compresstype=2);
-- compresstype cant be used with temp table
CREATE TEMP TABLE compressed_temp_table_1024(id int) WITH(compresstype=2);
-- compresstype cant be used with unlogged table
CREATE unlogged TABLE compressed_unlogged_table_1024(id int) WITH(compresstype=2);
-- use compress_prealloc_chunks\compress_chunk_size\compress_level without compresstype
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compress_prealloc_chunks=5);
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compress_chunk_size=1024);
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compress_byte_convert=true);
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compress_diff_convert=true);
CREATE TABLE unspported_feature.compressed_table_1024(id int) WITH(compress_level=5);
-- unspport exchange
CREATE TABLE unspported_feature.exchange_table(id int) WITH(compresstype=2);
CREATE TABLE unspported_feature.alter_table(id int) partition by range(id)
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
ALTER TABLE unspported_feature.alter_table EXCHANGE PARTITION FOR(2500) WITH TABLE unspported_feature.exchange_table;
-- unspport alter compress_chunk_size
create TABLE unspported_feature.alter_table_option(id int) WITH(compresstype=2);
\d+ unspported_feature.alter_table_option
ALTER TABLE unspported_feature.alter_table_option SET(compresstype=0); -- fail
ALTER TABLE unspported_feature.alter_table_option SET(compress_chunk_size=2048); -- fail
ALTER TABLE unspported_feature.alter_table_option SET(compress_level=2, compress_prealloc_chunks=0);
-- alter compress_byte_convert\compress_diff_convert
create table unspported_feature.rolcompress_table_001(a int) with (compresstype=2, compress_diff_convert=true); -- fail

create table unspported_feature.t_rowcompress_0007(cid int, name varchar2) with (compresstype=1);
alter table unspported_feature.t_rowcompress_0007 set (compress_diff_convert=true); --fail
alter table unspported_feature.t_rowcompress_0007 set (compress_byte_convert=true, compress_diff_convert=true); --success
alter table unspported_feature.t_rowcompress_0007 set (compress_level=31); --failed
create table unspported_feature.t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2); -- failed
create table unspported_feature.t_rowcompress_pglz_compresslevel(id int) with (compresstype=2,compress_level=2); -- success

CREATE TABLE unspported_feature.index_test(id int, c1 text);
-- ustore
CREATE TABLE unspported_feature.ustore_table(id int, c1 text) WITH(compresstype=2, storage_type=ustore); --failed
CREATE INDEX tbl_pc_idx1 on unspported_feature.index_test(c1) WITH(compresstype=2, storage_type=ustore); --failed
-- segment
CREATE TABLE unspported_feature.segment_table(id int, c1 text) WITH(compresstype=2, segment=on);  --failed
CREATE INDEX on unspported_feature.index_test(c1) WITH(compresstype=2, segment=on); --failed
-- set compress_diff_convert
create table unspported_feature.compress_byte_test(id int) with (compresstype=2, compress_byte_convert=false, compress_diff_convert = true); -- failed