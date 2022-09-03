------------
--compressed table with different parameters
------------
drop schema if exists compress_normal_user;
create schema compress_normal_user;
--testcase 1
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 2
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(compresstype=2, compress_chunk_size=512, compress_byte_convert=true, compress_diff_convert=true, compress_prealloc_chunks=1);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 3
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(compresstype=2, compress_chunk_size=512);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 4
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(compresstype=2, compress_chunk_size=2048, compress_prealloc_chunks=1);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 5
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(compresstype=1, compress_chunk_size=2048, compress_prealloc_chunks=1);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 6
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(compresstype=2, compress_chunk_size=2048, compress_prealloc_chunks=1);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 7
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(compresstype=1, compress_chunk_size=512, compress_prealloc_chunks=2);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','aaaaaaaabbbbbbbbbbbcccccccc', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 8
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) WITH(compresstype=2, compress_chunk_size=512, compress_prealloc_chunks=5);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','trgtrh', 12365);
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 9
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) WITH(compresstype=1, compress_chunk_size=512, compress_prealloc_chunks=1);
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','trgtrh', 12365);
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 10
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (compresstype=2, compress_chunk_size = 512, compress_level = 1, compress_prealloc_chunks=1,compress_diff_convert = true, compress_byte_convert=true) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE));
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','trgtrh', 12365);
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 11
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (storage_type = ustore, compresstype=2, compress_chunk_size = 512, compress_level = 1, compress_prealloc_chunks=1,compress_diff_convert = true, compress_byte_convert=true) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE));
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','trgtrh', 12365);
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(storage_type = ustore,compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 12
--12.1: create table
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
create table compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)) WITH(storage_type = ustore, compresstype=2, compress_chunk_size = 512, compress_level = 1, compress_prealloc_chunks=1,compress_diff_convert = true, compress_byte_convert=true);
drop table if exists compress_normal_user.row_noncompression_test_tbl1 cascade;
create table compress_normal_user.row_noncompression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50));

--12.2: insert many data
insert into compress_normal_user.row_compression_test_tbl1 values(0,'ZAINSERT1','aaaaaaaaaaaaaaaaaaaaaaaa', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
insert into compress_normal_user.row_compression_test_tbl1 values(1,'ZAINSERT1','aaaaaaaaaaaaaaaa', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
insert into compress_normal_user.row_compression_test_tbl1 values(2,'ZAINSERT1','aaaaaaaa', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(3, 1000000),'ZAINSERT1','aaaaaaaabbbbbbbbbbbccccccccaaaaaaaaaaaaaaaa', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');  -- for overload the buffer
checkpoint;
select * from compress_normal_user.row_compression_test_tbl1 order by COLUMNONE limit 3; -- seq scan
select count(*) from compress_normal_user.row_compression_test_tbl1; -- seq scan
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTHREE) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;
select * from compress_normal_user.row_compression_test_tbl1 order by COLUMNONE limit 3; -- index scan
select count(*) from compress_normal_user.row_compression_test_tbl1; -- index scan
insert into compress_normal_user.row_compression_test_tbl1 values(1000001,'ZAINSERT1','bbbbbbbbbbbbbbbbbbbbbbbb', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
insert into compress_normal_user.row_compression_test_tbl1 values(1000002,'ZAINSERT1','bbbbbbbbbbbbbbbb', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
insert into compress_normal_user.row_compression_test_tbl1 values(1000003,'ZAINSERT1','bbbbbbbb', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
checkpoint;
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(3, 10000),'ZAINSERT1','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');
checkpoint;
insert into compress_normal_user.row_noncompression_test_tbl1 values(generate_series(0, 1000000),'ZAINSERT1','aaaaaaaabbbbbbbbbbbcccccccc', 2, 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc', 'aaaaaaaabbbbbbbbbbbcccccccc');  -- for overload the buffer all
checkpoint;
select * from compress_normal_user.row_compression_test_tbl1 order by COLUMNONE desc limit 3; -- index scan
select count(*) from compress_normal_user.row_compression_test_tbl1; -- index scan

--12.3: do update operations
update compress_normal_user.row_noncompression_test_tbl1 set columntwo='ZAUPDATE' where columnone>900000; 
select count(*) from compress_normal_user.row_noncompression_test_tbl1;
update compress_normal_user.row_noncompression_test_tbl1 set columntwo='ZAUPDATE' where columnone<100000; 
select count(*) from compress_normal_user.row_noncompression_test_tbl1;

--12.4: do delete operations
delete from compress_normal_user.row_noncompression_test_tbl1 where columnone>700000;
select count(*) from compress_normal_user.row_noncompression_test_tbl1;

--12.5: do vacuum operations
vacuum full compress_normal_user.row_noncompression_test_tbl1;
select count(*) from compress_normal_user.row_noncompression_test_tbl1;
vacuum compress_normal_user.row_noncompression_test_tbl1;
select count(*) from compress_normal_user.row_noncompression_test_tbl1;

drop table if exists compress_normal_user.row_noncompression_test_tbl1 cascade;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;

--testcase 13
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (storage_type = astore, compresstype=3, compress_chunk_size = 512, compress_level = 1, compress_prealloc_chunks=1,compress_diff_convert = true, compress_byte_convert=true) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE)); --error
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (storage_type = astore, compresstype=3, compress_chunk_size = 512, compress_prealloc_chunks=1,compress_diff_convert = true, compress_byte_convert=true) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE)); --error
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (storage_type = astore, compresstype=3, compress_chunk_size = 512, compress_prealloc_chunks=1,compress_byte_convert=true) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE)); --error
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (storage_type = astore, compresstype=3, compress_chunk_size = 512, compress_prealloc_chunks=1,compress_diff_convert=true) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE)); --error
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (storage_type = ustore, compresstype=3, compress_chunk_size = 512, compress_prealloc_chunks=1) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE)); --error
CREATE TABLE compress_normal_user.row_compression_test_tbl1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER) with (storage_type = astore, compresstype=3, compress_chunk_size = 512, compress_prealloc_chunks=1) PARTITION BY RANGE(COLUMNONE)(PARTITION P1 VALUES LESS THAN(1000),PARTITION P2 VALUES LESS THAN(2000),PARTITION P3 VALUES LESS THAN(3000),PARTITION P4 VALUES LESS THAN(4000),PARTITION P5 VALUES LESS THAN(5000),PARTITION P6 VALUES LESS THAN(MAXVALUE)); --ok
insert into compress_normal_user.row_compression_test_tbl1 values(generate_series(0, 1600),'ZAINSERT1','trgtrh', 12365);
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(storage_type = astore,compresstype=3, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true); --error
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(storage_type = astore,compresstype=3, compress_chunk_size=1024, compress_byte_convert=true); --error
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(storage_type = astore,compresstype=3, compress_chunk_size=1024, compress_diff_convert=true); --error
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(storage_type = ustore,compresstype=3, compress_chunk_size=1024); --error
create index compress_normal_user.row_compression_test_tbl1_idx on compress_normal_user.row_compression_test_tbl1(COLUMNONE, COLUMNTWO) WITH(storage_type = astore,compresstype=3, compress_chunk_size=1024); --ok
checkpoint;
drop table if exists compress_normal_user.row_compression_test_tbl1 cascade;
drop schema if exists compress_normal_user;

------------
--compression funcs test
------------
CREATE OR REPLACE FUNCTION compress_func_findpath(character varying)
  RETURNS character varying
  LANGUAGE plpgsql
AS
$BODY$
declare
  relpath character varying;
begin
  relpath = (select pg_relation_filepath(relname::regclass) from pg_class where relname =  $1);
  return relpath;
end;
$BODY$;
CREATE OR REPLACE FUNCTION compress_func_findoid(character varying)
  RETURNS integer
  LANGUAGE plpgsql
AS
$BODY$
declare
  ret integer;
begin
  ret = (select relfilenode from pg_class where relname =  $1);
  return ret;
end;
$BODY$;

--non compression table
drop table if exists row_noncompression_test_tbl1_kkk cascade;
create table row_noncompression_test_tbl1_kkk(a int);
select * from compress_ratio_info(compress_func_findpath('row_noncompression_test_tbl1_kkk'));
select * from compress_statistic_info(compress_func_findpath('row_noncompression_test_tbl1_kkk'), 1);
select * from compress_address_header(compress_func_findoid('row_noncompression_test_tbl1_kkk'), 0);
select * from compress_address_details(compress_func_findoid('row_noncompression_test_tbl1_kkk'), 0);
drop table if exists row_noncompression_test_tbl1_kkk cascade;

--compression table
drop table if exists row_compression_test_tbl1_kkk cascade;
create table row_compression_test_tbl1_kkk(COLUMNONE INTEGER ,COLUMNTWO CHAR(50)) WITH(compresstype=2, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true);
insert into row_compression_test_tbl1_kkk values(generate_series(0, 500),'ZAINSERT1');
create index row_compression_test_tbl1_kkk_idx on row_compression_test_tbl1_kkk(COLUMNONE, COLUMNTWO) WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
checkpoint;

select compress_func_findpath('row_compression_test_tbl1_kkk');  -- need ignore
select compress_func_findoid('row_compression_test_tbl1_kkk');  -- need ignore

select * from compress_buffer_stat_info();  -- need ignore
select count(pg_relation_filepath(relname::regclass)) from pg_class where relname = 'row_compression_test_tbl1_kkk';
select count(compress_func_findpath('row_compression_test_tbl1_kkk'));
select count(compress_func_findoid('row_compression_test_tbl1_kkk'));
select is_compress, file_count, logic_size, physic_size, compress_ratio from compress_ratio_info(compress_func_findpath('row_compression_test_tbl1_kkk'));
select extent_count, dispersion_count, void_count from compress_statistic_info(compress_func_findpath('row_compression_test_tbl1_kkk'), 1);
select * from compress_address_header(compress_func_findoid('row_compression_test_tbl1_kkk'), 0);
select extent, extent_block_number, block_number, alocated_chunks, nchunks from compress_address_details(compress_func_findoid('row_compression_test_tbl1_kkk'), 0);
select * from pg_table_size(compress_func_findoid('row_compression_test_tbl1_kkk'));

select count(pg_relation_filepath(relname::regclass)) from pg_class where relname = 'row_compression_test_tbl1_kkk_idx';
select count(compress_func_findpath('row_compression_test_tbl1_kkk_idx'));
select count(compress_func_findoid('row_compression_test_tbl1_kkk_idx'));
select is_compress, file_count, logic_size, physic_size, compress_ratio from compress_ratio_info(compress_func_findpath('row_compression_test_tbl1_kkk_idx'));
select extent_count, dispersion_count, void_count from compress_statistic_info(compress_func_findpath('row_compression_test_tbl1_kkk_idx'), 1);
select * from compress_address_header(compress_func_findoid('row_compression_test_tbl1_kkk_idx'), 0);
select extent, extent_block_number, block_number, alocated_chunks, nchunks from compress_address_details(compress_func_findoid('row_compression_test_tbl1_kkk_idx'), 0);
select * from pg_table_size(compress_func_findoid('row_compression_test_tbl1_kkk_idx'));

-- test shrink chrunks
shrink table row_compression_test_tbl1_kkk;
shrink table row_compression_test_tbl1_kkk nowait;
shrink table row_compression_test_tbl1_kkk_idx;
shrink table row_compression_test_tbl1_kkk_idx nowait;

-- test truncate
truncate table row_compression_test_tbl1_kkk;
checkpoint;

insert into row_compression_test_tbl1_kkk values(generate_series(0, 500),'ZAINSERT1');
checkpoint;

-- pg_read_binary_file_blocks func test
drop table if exists t_compression_test_1 cascade;
create table t_compression_test_1(COLUMNONE INTEGER ,COLUMNTWO CHAR(50)) WITH(compresstype=2, compress_chunk_size=512, compress_byte_convert=true, compress_diff_convert=true);
insert into t_compression_test_1 values(generate_series(0, 500),'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
checkpoint;
select * from t_compression_test_1 order by COLUMNONE limit 10;
select blocknum, len, algorithm, chunk_size from pg_read_binary_file_blocks(compress_func_findpath('t_compression_test_1') || '_compress', 1, 6);
select blocknum, len, algorithm, chunk_size from pg_read_binary_file_blocks(compress_func_findpath('t_compression_test_1') || '_compress', 1, 5);
select blocknum, len, algorithm, chunk_size from pg_read_binary_file_blocks(compress_func_findpath('t_compression_test_1') || '_compress', 3, 2);
drop table if exists t_compression_test_1 cascade;

drop FUNCTION compress_func_findpath(character varying);
drop FUNCTION compress_func_findoid(character varying);
drop table if exists row_compression_test_tbl1_kkk cascade;

-- 
drop schema if exists compress_normal_user;
create schema compress_normal_user;

drop table if exists compress_normal_user.nocompress_0039 cascade;
create table compress_normal_user.nocompress_0039(
COLUMNONE INTEGER ,COLUMNTWO CHAR(50) ,COLUMNTHREE VARCHAR(50) ,COLUMNFOUR INTEGER ,COLUMNFIVE CHAR(50) ,COLUMNSIX VARCHAR(50) ,COLUMNSEVEN CHAR(50) ,COLUMNEIGHT CHAR(50) ,COLUMNNINE VARCHAR(50) ,COLUMNTEN VARCHAR(50) ,COLUMNELEVEN CHAR(50) ,COLUMNTWELVE CHAR(50) ,COLUMNTHIRTEEN VARCHAR(50) ,COLUMNFOURTEEN CHAR(50) ,COLUMNFIFTEEM VARCHAR(50)
)WITH(compresstype=2, compress_chunk_size=1024, compress_byte_convert=true, compress_diff_convert=true);
insert into compress_normal_user.nocompress_0039 values(generate_series(0,160000 * 1), 'ZAINSERT1','abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx');
checkpoint;
select count(*) from compress_normal_user.nocompress_0039;
select * from compress_normal_user.nocompress_0039 order by COLUMNONE asc limit 5;
drop table if exists compress_normal_user.nocompress_0039 cascade;

-- 
drop table  if exists compress_normal_user.t_part_csf11 cascade;
create table compress_normal_user.t_part_csf11(id int, name varchar2(100))
WITH(compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY list(id)
(
partition p1 values (1),
partition p2 values (2),
partition p3 values (3)
);
alter table compress_normal_user.t_part_csf11 add partition p6 values(4);
alter table compress_normal_user.t_part_csf11 add partition p7 values(5);
insert into compress_normal_user.t_part_csf11 values(1, 'dscdscds'),(2, 'dscdscds'),(3, 'dscdscds'),(4, 'dscdscds'),(5, 'dscdscds');
checkpoint;
select * from compress_normal_user.t_part_csf11 order by id asc;
drop table  if exists compress_normal_user.t_part_csf11 cascade;

-- 
drop table  if exists compress_normal_user.t_part_csf22 cascade;
create table compress_normal_user.t_part_csf22(id int, name varchar2(100))
WITH(compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY range(id)
(
partition p1 VALUES LESS THAN (1) ,
partition p2 VALUES LESS THAN (2),
partition p3 VALUES LESS THAN (3)
) ;
alter table compress_normal_user.t_part_csf22 add partition p6 VALUES LESS THAN(4) ;
alter table compress_normal_user.t_part_csf22 add partition p7 VALUES LESS THAN(5);
insert into compress_normal_user.t_part_csf22 values(1, 'dscdscds'),(2, 'dscdscds'),(3, 'dscdscds');
checkpoint;
select * from compress_normal_user.t_part_csf22 order by id asc;
drop table  if exists compress_normal_user.t_part_csf22 cascade;

--
drop table  if exists compress_normal_user.t_part_csf44 cascade;
create table compress_normal_user.t_part_csf44(id int, name varchar2(100))
WITH(compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY hash(id)
(
partition p1,
partition p2,
partition p3
);
insert into compress_normal_user.t_part_csf44 values(1, 'dscdscds'),(2, 'dscdscds'),(3, 'dscdscds'); 
checkpoint;
select * from compress_normal_user.t_part_csf44 order by id asc;
drop table  if exists compress_normal_user.t_part_csf44 cascade;

-- 
drop table if exists compress_normal_user.list_list cascade;
CREATE TABLE compress_normal_user.list_list
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
    PARTITION p_201901 VALUES ( '201902' )
    (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
    ),
    PARTITION p_201902 VALUES ( '201903' )
    (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
    )
);
insert into compress_normal_user.list_list values('201902', '1', '1', 1),('201902', '2', '1', 1),('201902', '1', '1', 1),('201903', '2', '1', 1),('201903', '1', '1', 1),('201903', '2', '1', 1);
checkpoint;
select * from compress_normal_user.list_list order by month_code, dept_code;
drop table if exists compress_normal_user.list_list cascade;

-- 
drop table if exists compress_normal_user.list_hash cascade;
CREATE TABLE compress_normal_user.list_hash
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY LIST (month_code) SUBPARTITION BY HASH (dept_code)
(
    PARTITION p_201901 VALUES ( '201902' )
    (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
    ),
    PARTITION p_201902 VALUES ( '201903' )
    (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
    )
);
insert into compress_normal_user.list_hash values('201902', '1', '1', 1),('201902', '2', '1', 1),('201902', '3', '1', 1),('201903', '4', '1', 1),('201903', '5', '1', 1),('201903', '6', '1', 1);
checkpoint;
select * from compress_normal_user.list_hash order by month_code, dept_code;
drop table if exists compress_normal_user.list_hash cascade;

-- 
drop table if exists compress_normal_user.list_range cascade;
CREATE TABLE compress_normal_user.list_range
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY LIST (month_code) SUBPARTITION BY RANGE (dept_code)
(
    PARTITION p_201901 VALUES ( '201902' )
    (
    SUBPARTITION p_201901_a values less than ('4'),
    SUBPARTITION p_201901_b values less than ('6')
    ),
    PARTITION p_201902 VALUES ( '201903' )
    (
    SUBPARTITION p_201902_a values less than ('3'),
    SUBPARTITION p_201902_b values less than ('6')
    )
);
insert into compress_normal_user.list_range values('201902', '1', '1', 1),('201902', '2', '1', 1),('201902', '3', '1', 1),('201903', '4', '1', 1),('201903', '5', '1', 1);
checkpoint;
select * from compress_normal_user.list_range order by month_code, dept_code;
drop table if exists compress_normal_user.list_range cascade;

-- 
drop table if exists compress_normal_user.range_list cascade;
CREATE TABLE compress_normal_user.range_list
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
    PARTITION p_201901 VALUES LESS THAN( '201903' )
    (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
    ),
    PARTITION p_201902 VALUES LESS THAN( '201904' )
    (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
    )
);
insert into compress_normal_user.range_list values('201902', '1', '1', 1),('201902', '2', '1', 1),('201902', '1', '1', 1),('201903', '2', '1', 1),('201903', '1', '1', 1),('201903', '2', '1', 1);
checkpoint;
select * from compress_normal_user.range_list order by month_code, dept_code;
drop table if exists compress_normal_user.range_list cascade;

--
drop table if exists compress_normal_user.range_hash cascade;
CREATE TABLE compress_normal_user.range_hash
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY RANGE (month_code) SUBPARTITION BY HASH (dept_code)
(
    PARTITION p_201901 VALUES LESS THAN( '201903' )
    (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
    ),
    PARTITION p_201902 VALUES LESS THAN( '201904' )
    (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
    )
);
insert into compress_normal_user.range_hash values('201902', '1', '1', 1),('201902', '2', '1', 1),('201902', '1', '1', 1),('201903', '2', '1', 1),('201903', '1', '1', 1),('201903', '2', '1', 1);
checkpoint;
select * from compress_normal_user.range_hash order by month_code, dept_code;
drop table if exists compress_normal_user.range_hash cascade;

--
drop table if exists compress_normal_user.range_range cascade;
CREATE TABLE compress_normal_user.range_range
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
    PARTITION p_201901 VALUES LESS THAN( '201903' )
    (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
    ),
    PARTITION p_201902 VALUES LESS THAN( '201904' )
    (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
    )
);
insert into compress_normal_user.range_range values('201902', '1', '1', 1),('201902', '2', '1', 1),('201902', '1', '1', 1),('201903', '2', '1', 1),('201903', '1', '1', 1),('201903', '2', '1', 1);
checkpoint;
select * from compress_normal_user.range_range order by month_code, dept_code;
drop table if exists compress_normal_user.range_range cascade;

-- 
drop table if exists compress_normal_user.hash_list cascade;
CREATE TABLE compress_normal_user.hash_list
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY hash (month_code) SUBPARTITION BY LIST (dept_code)
(
    PARTITION p_201901
    (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
    ),
    PARTITION p_201902
    (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
    )
);
insert into compress_normal_user.hash_list values('201901', '1', '1', 1),('201901', '2', '1', 1),('201901', '1', '1', 1),('201903', '2', '1', 1),('201903', '1', '1', 1),('201903', '2', '1', 1);
checkpoint;
select * from compress_normal_user.hash_list order by month_code, dept_code;
drop table if exists compress_normal_user.hash_list cascade;

--
drop table if exists compress_normal_user.hash_hash cascade;
CREATE TABLE compress_normal_user.hash_hash
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY hash (month_code) SUBPARTITION BY hash (dept_code)
(
    PARTITION p_201901
    (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
    ),
    PARTITION p_201902
    (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
    )
);
insert into compress_normal_user.hash_hash values('201901', '1', '1', 1),('201901', '2', '1', 1),('201901', '1', '1', 1),('201903', '2', '1', 1),('201903', '1', '1', 1),('201903', '2', '1', 1);
checkpoint;
select * from compress_normal_user.hash_hash order by month_code, dept_code;
drop table if exists compress_normal_user.hash_hash cascade;

-- 
drop table if exists compress_normal_user.hash_range cascade;
CREATE TABLE compress_normal_user.hash_range
(
month_code VARCHAR2 ( 30 ) NOT NULL ,
dept_code VARCHAR2 ( 30 ) NOT NULL ,
user_no VARCHAR2 ( 30 ) NOT NULL ,
sales_amt int
) WITH (compresstype=2, compress_level=1, compress_chunk_size=512, compress_prealloc_chunks=1, compress_byte_convert=true, compress_diff_convert=true)
PARTITION BY hash (month_code) SUBPARTITION BY range (dept_code)
(
    PARTITION p_201901
    (
    SUBPARTITION p_201901_a VALUES LESS THAN ( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN ( '3' )
    ),
    PARTITION p_201902
    (
    SUBPARTITION p_201902_a VALUES LESS THAN ( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN ( '3' )
    )
);
insert into compress_normal_user.hash_range values('201901', '1', '1', 1),('201901', '2', '1', 1),('201901', '1', '1', 1),('201903', '2', '1', 1),('201903', '1', '1', 1),('201903', '2', '1', 1);
checkpoint;
select * from compress_normal_user.hash_range order by month_code, dept_code;
drop table if exists compress_normal_user.hash_range cascade;

drop schema if exists compress_normal_user cascade;
