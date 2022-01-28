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
