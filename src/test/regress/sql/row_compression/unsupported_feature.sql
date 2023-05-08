create schema unsupported_feature;
-- unspport compressType: 4
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compresstype=4, compress_chunk_size=1024);
-- unspport compress_chunk_size: 2000
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_chunk_size=2000);
-- unspport compress_prealloc_chunks: -1
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_prealloc_chunks=-1);
-- unspport compress_prealloc_chunks: 8
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_prealloc_chunks=8);
-- unspport compress_level: 128
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compresstype=2, compress_level=128);
-- compresstype cant be used with column table
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(ORIENTATION = 'column', compresstype=2);
-- compresstype cant be used with temp table
CREATE TEMP TABLE compressed_temp_table_1024(id int) WITH(compresstype=2);
-- compresstype cant be used with unlogged table
CREATE unlogged TABLE compressed_unlogged_table_1024(id int) WITH(compresstype=2);
-- use compress_prealloc_chunks\compress_chunk_size\compress_level without compresstype
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compress_prealloc_chunks=5);
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compress_chunk_size=1024);
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compress_byte_convert=true);
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compress_diff_convert=true);
CREATE TABLE unsupported_feature.compressed_table_1024(id int) WITH(compress_level=5);
-- unspport exchange
CREATE TABLE unsupported_feature.exchange_table(id int) WITH(compresstype=2);
CREATE TABLE unsupported_feature.alter_table(id int) partition by range(id)
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
ALTER TABLE unsupported_feature.alter_table EXCHANGE PARTITION FOR(2500) WITH TABLE unsupported_feature.exchange_table;
-- unspport alter compress_chunk_size
create TABLE unsupported_feature.alter_table_option(id int) WITH(compresstype=2);
\d+ unsupported_feature.alter_table_option
ALTER TABLE unsupported_feature.alter_table_option SET(compresstype=0); -- success
ALTER TABLE unsupported_feature.alter_table_option SET(compress_chunk_size=2048); -- failed
ALTER TABLE unsupported_feature.alter_table_option SET(compresstype=2, compress_level=2, compress_prealloc_chunks=0); -- success
-- alter compress_byte_convert\compress_diff_convert
create table unsupported_feature.rolcompress_table_001(a int) with (compresstype=2, compress_diff_convert=true); -- failed

create table unsupported_feature.t_rowcompress_0007(cid int, name varchar2) with (compresstype=1);
alter table unsupported_feature.t_rowcompress_0007 set (compress_diff_convert=true); --fail
alter table unsupported_feature.t_rowcompress_0007 set (compress_byte_convert=true, compress_diff_convert=true); --success
alter table unsupported_feature.t_rowcompress_0007 set (compress_level=31); --failed
create table unsupported_feature.t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2); -- failed
create table unsupported_feature.t_rowcompress_pglz_compresslevel(id int) with (compresstype=2,compress_level=2); -- success

CREATE TABLE unsupported_feature.index_test(id int, c1 text);
-- segment
CREATE TABLE unsupported_feature.segment_table(id int, c1 text) WITH(compresstype=2, segment=on);  --failed
CREATE INDEX on unsupported_feature.index_test(c1) WITH(compresstype=2, segment=on); --failed
-- set compress_diff_convert
create table unsupported_feature.compress_byte_test(id int) with (compresstype=2, compress_byte_convert=false, compress_diff_convert = true); -- failed

create table unsupported_feature.test(id int) with (compresstype=2); -- success
alter table unsupported_feature.test set(Compresstype=1); -- success
alter table unsupported_feature.test set(Compress_level=3); -- failed

create table lm_rcp_4 (c1 int,c2 varchar2,c3 number,c4 money,c5 CHAR(20),c6 CLOB,c7 blob,c8 DATE,c9 BOOLEAN,c10 TIMESTAMP,c11 point,columns12 cidr) with(Compresstype=2,Compress_chunk_size=512)
    partition by list(c1) subpartition by range(c3)(
    partition ts1 values(1,2,3,4,5)(subpartition ts11 values less than(500),subpartition ts12 values less than(5000),subpartition ts13 values less than(MAXVALUE)),
    partition ts2 values(6,7,8,9,10),
    partition ts3 values(11,12,13,14,15)(subpartition ts31 values less than(5000),subpartition ts32 values less than(10000),subpartition ts33 values less than(MAXVALUE)),
    partition ts4 values(default));
create unique index indexg_lm_rcp_4 on lm_rcp_4(c1 NULLS first,c2,c3) global
with(FILLFACTOR=80,Compresstype=2,Compress_chunk_size=512,compress_byte_convert=1,compress_diff_convert=1);
--s3.
alter index indexg_lm_rcp_4 rename to indexg_lm_rcp_4_newname;
--s4.修改压缩类型
alter index indexg_lm_rcp_4_newname set(Compresstype=1);
--s5.修改Compress_level
alter index indexg_lm_rcp_4_newname set(Compress_level=3);
