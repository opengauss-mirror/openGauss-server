-- parameter validation exception during altering compression parameters
CREATE SCHEMA alter_compress_params_schema;

-- set compression parameters with compresstype = 0
CREATE TABLE alter_compress_params_schema.uncompress_astore_to_cl_30 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_cl_30 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_cl_30 SET (compress_level = 30); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_cl_30;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_ccs_512 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_ccs_512 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_ccs_512 SET (compress_chunk_size = 512); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_ccs_512;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_ccs_512_cpc_7 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_ccs_512_cpc_7 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_ccs_512_cpc_7 SET (compress_chunk_size = 512, compress_prealloc_chunks = 7); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_ccs_512_cpc_7;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_cbc_1 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_cbc_1 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_cbc_1 SET (compress_byte_convert = true); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_cbc_1;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_cbc_1_cdc_1 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_cbc_1_cdc_1 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_cbc_1_cdc_1 SET (compress_byte_convert = true, compress_diff_convert = true); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_cbc_1_cdc_1;

-- the new compression parameters is out of the value range
CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_5 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_5 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_5 SET (compresstype = 5); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_5;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype__1 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype__1 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype__1 SET (compresstype = -1); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype__1;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl_32 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl_32 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl_32 SET (compresstype = 2, compress_level = 32); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl_32;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl__32 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl__32 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl__32 SET (compresstype = 2, compress_level = -32); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_2_cl__32;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_511 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_511 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_511 SET (compresstype = 1, compress_chunk_size = 511); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_511;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4097 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4097 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4097 SET (compresstype = 1, compress_chunk_size = 4097); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4097;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_1023 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_1023 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_1023 SET (compresstype = 1, compress_chunk_size = 1023); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_1023;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc_8 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc_8 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc_8 SET (compresstype = 1, compress_chunk_size = 512, compress_prealloc_chunks = 8); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc_8;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc__1 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc__1 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc__1 SET (compresstype = 1, compress_chunk_size = 512, compress_prealloc_chunks = -1); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_512_cpc__1;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_2 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_2 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_2 SET (compresstype = 1, compress_chunk_size = 512, compress_byte_convert = 2); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_2;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_1_cdc_2 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_1_cdc_2 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_1_cdc_2 SET (compresstype = 1, compress_byte_convert = true, compress_diff_convert = 2); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_cbc_1_cdc_2;

CREATE TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4096_cpc_2 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4096_cpc_2 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4096_cpc_2 SET (compresstype = 1, compress_chunk_size = 4096, compress_prealloc_chunks = 2); -- fail
DROP TABLE alter_compress_params_schema.uncompress_astore_to_compresstype_1_ccs_4096_cpc_2;

CREATE TABLE alter_compress_params_schema.uncompress_to_compresstype_3 (id int, value varchar);
INSERT INTO alter_compress_params_schema.uncompress_to_compresstype_3 SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.uncompress_to_compresstype_3 SET (compresstype = 3); -- fail
DROP TABLE alter_compress_params_schema.uncompress_to_compresstype_3;
-- set compressed options of column table
CREATE TABLE alter_compress_params_schema.alter_column_table_compressed_options (id int, value varchar) WITH (ORIENTATION = column);
INSERT INTO alter_compress_params_schema.alter_column_table_compressed_options SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.alter_column_table_compressed_options SET (compresstype = 1); -- fail
DROP TABLE alter_compress_params_schema.alter_column_table_compressed_options;

-- set compressed options of segment table
CREATE TABLE alter_compress_params_schema.alter_segment_table_compressed_options (id int, value varchar) WITH (segment = on);
INSERT INTO alter_compress_params_schema.alter_segment_table_compressed_options SELECT generate_series(1,5), 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
ALTER TABLE alter_compress_params_schema.alter_segment_table_compressed_options SET (compresstype = 1); -- fail
DROP TABLE alter_compress_params_schema.alter_segment_table_compressed_options;

-- set compressed options of index
CREATE TABLE alter_compress_params_schema.segment_table (id int, c1 text) WITH( segment=on);
CREATE INDEX alter_compress_params_schema.uncompressed_index_test ON alter_compress_params_schema.segment_table(c1);
ALTER INDEX alter_compress_params_schema.uncompressed_index_test SET (compresstype = 1); -- failed
DROP INDEX alter_compress_params_schema.uncompressed_index_test;
DROP TABLE alter_compress_params_schema.segment_table;

CREATE TABLE alter_compress_params_schema.row_table (id int, c1 text);
CREATE INDEX alter_compress_params_schema.compressed_index_test ON alter_compress_params_schema.row_table(c1) WITH (compresstype = 1);
ALTER INDEX alter_compress_params_schema.compressed_index_test SET (compresstype = 2, compress_level = 15); -- failed
ALTER INDEX alter_compress_params_schema.compressed_index_test SET (compresstype = 0); -- failed
DROP INDEX alter_compress_params_schema.compressed_index_test;
DROP TABLE alter_compress_params_schema.row_table;

-- set compressed options of system catalog
ALTER TABLE pg_class SET (compresstype = 1);

-- set uncompressed table to compressed table
CREATE TABLE alter_compress_params_schema.uncompressed_table_compresstype_1 (id int, c1 text);
INSERT INTO alter_compress_params_schema.uncompressed_table_compresstype_1 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_table_compresstype_1 SET (compresstype = 1);
\d+ alter_compress_params_schema.uncompressed_table_compresstype_1
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_table_compresstype_1;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_table_compresstype_1', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_table_compresstype_1', 0);
DROP TABLE alter_compress_params_schema.uncompressed_table_compresstype_1;

CREATE TABLE alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30 (id int, c1 text);
INSERT INTO alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30 SET (compresstype = 2, compress_level = 30);
\d+ alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30', 0);
DROP TABLE alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30;

CREATE TABLE alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3 (id int, c1 text);
INSERT INTO alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3 SET (compresstype = 2, compress_level = 30, compress_chunk_size = 2048, compress_prealloc_chunks = 3);
\d+ alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3', 0);
DROP TABLE alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3;

CREATE TABLE alter_compress_params_schema.uncompressed_table_all_options (id int, c1 text);
INSERT INTO alter_compress_params_schema.uncompressed_table_all_options SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_table_all_options SET (compresstype = 2, compress_level = 30, compress_chunk_size = 512, compress_prealloc_chunks = 6, compress_byte_convert = true, compress_diff_convert=true);
\d+ alter_compress_params_schema.uncompressed_table_all_options
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_table_all_options;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_table_all_options', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_table_all_options', 0);
DROP TABLE alter_compress_params_schema.uncompressed_table_all_options;

-- set uncompressed segment table to compressed segment table
CREATE TABLE alter_compress_params_schema.uncompressed_table_compresstype_1 (id int, c1 text) with (segment=on);
INSERT INTO alter_compress_params_schema.uncompressed_table_compresstype_1 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_table_compresstype_1 SET (compresstype = 1, segment=on);
\d+ alter_compress_params_schema.uncompressed_table_compresstype_1
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_table_compresstype_1;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_table_compresstype_1', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_table_compresstype_1', 0);
DROP TABLE alter_compress_params_schema.uncompressed_table_compresstype_1;

CREATE TABLE alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30 (id int, c1 text) with (segment=on);
INSERT INTO alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30 SET (compresstype = 2, compress_level = 30, segment=on);
\d+ alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30', 0);
DROP TABLE alter_compress_params_schema.uncompressed_table_compresstype_2_cl_30;

CREATE TABLE alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3 (id int, c1 text) with (segment=on);
INSERT INTO alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3 SET (compresstype = 2, compress_level = 30, compress_chunk_size = 2048, compress_prealloc_chunks = 3, segment=on);
\d+ alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3', 0);
DROP TABLE alter_compress_params_schema.uncompressed_compresstype_2_cl_30_ccs_2048_cpc_3;

CREATE TABLE alter_compress_params_schema.uncompressed_table_all_options (id int, c1 text) with (segment=on);
INSERT INTO alter_compress_params_schema.uncompressed_table_all_options SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.uncompressed_table_all_options SET (compresstype = 2, compress_level = 30, compress_chunk_size = 512, compress_prealloc_chunks = 6, compress_byte_convert = true, compress_diff_convert=true, segment=on);
\d+ alter_compress_params_schema.uncompressed_table_all_options
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.uncompressed_table_all_options;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.uncompressed_table_all_options', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.uncompressed_table_all_options', 0);
DROP TABLE alter_compress_params_schema.uncompressed_table_all_options;

-- set uncompressed partitioned table to compressed table
CREATE TABLE alter_compress_params_schema.uncompressed_partitioned_compresstype_1
(
	order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store	 CHAR(20)
)
PARTITION BY RANGE(sales_date)
(
	PARTITION uncompressed_partitioned_compresstype_1_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION uncompressed_partitioned_compresstype_1_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION uncompressed_partitioned_compresstype_1_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION uncompressed_partitioned_compresstype_1_season4 VALUES LESS THAN(MAXVALUE)
);
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_compresstype_1 SELECT generate_series(1, 10), 'session1 item', '2021-02-01 00:00:00', 1000, '711';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_compresstype_1 SELECT generate_series(1, 10), 'session2 item', '2021-05-01 00:00:00', 1000, '722';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_compresstype_1 SELECT generate_series(1, 10), 'session3 item', '2021-08-01 00:00:00', 1000, '733';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_compresstype_1 SELECT generate_series(1, 10), 'session4 item', '2021-11-01 00:00:00', 1000, '744';
ALTER TABLE alter_compress_params_schema.uncompressed_partitioned_compresstype_1 SET (compresstype = 1);
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_compresstype_1_season1';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_compresstype_1_season2';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_compresstype_1_season3';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_compresstype_1_season4';
\d+ alter_compress_params_schema.uncompressed_partitioned_compresstype_1
SELECT count(*) FROM alter_compress_params_schema.uncompressed_partitioned_compresstype_1;
DROP TABLE alter_compress_params_schema.uncompressed_partitioned_compresstype_1;

CREATE TABLE alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3
(
	order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store	 CHAR(20)
)
PARTITION BY RANGE(sales_date)
(
	PARTITION uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season4 VALUES LESS THAN(MAXVALUE)
);
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3 SELECT generate_series(1, 10), 'session1 item', '2021-02-01 00:00:00', 1000, '711';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3 SELECT generate_series(1, 10), 'session2 item', '2021-05-01 00:00:00', 1000, '722';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3 SELECT generate_series(1, 10), 'session3 item', '2021-08-01 00:00:00', 1000, '733';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3 SELECT generate_series(1, 10), 'session4 item', '2021-11-01 00:00:00', 1000, '744';
ALTER TABLE alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3 SET (compresstype  = 2, compress_level = 30, compress_chunk_size = 2048, compress_prealloc_chunks = 3);
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season1';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season2';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season3';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_cl_10_ccs_2048_cpc_3_season4';
\d+ alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3
SELECT count(*) FROM alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3;
DROP TABLE alter_compress_params_schema.uncompressed_partitioned_cl_10_ccs_2048_cpc_3;

CREATE TABLE alter_compress_params_schema.uncompressed_partitioned_all_options
(
	order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store	 CHAR(20)
)
PARTITION BY RANGE(sales_date)
(
	PARTITION uncompressed_partitioned_all_options_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION uncompressed_partitioned_all_options_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION uncompressed_partitioned_all_options_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION uncompressed_partitioned_all_options_season4 VALUES LESS THAN(MAXVALUE)
);
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_all_options SELECT generate_series(1, 10), 'session1 item', '2021-02-01 00:00:00', 1000, '711';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_all_options SELECT generate_series(1, 10), 'session2 item', '2021-05-01 00:00:00', 1000, '722';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_all_options SELECT generate_series(1, 10), 'session3 item', '2021-08-01 00:00:00', 1000, '733';
INSERT INTO alter_compress_params_schema.uncompressed_partitioned_all_options SELECT generate_series(1, 10), 'session4 item', '2021-11-01 00:00:00', 1000, '744';
ALTER TABLE alter_compress_params_schema.uncompressed_partitioned_all_options SET (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true);
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_all_options_season1';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_all_options_season2';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_all_options_season3';
select relname, reloptions from pg_partition where relname = 'uncompressed_partitioned_all_options_season4';
\d+ alter_compress_params_schema.uncompressed_partitioned_all_options
SELECT count(*) FROM alter_compress_params_schema.uncompressed_partitioned_all_options;
DROP TABLE alter_compress_params_schema.uncompressed_partitioned_all_options;

-- set uncompressed subpartitioned table to compressed table
CREATE TABLE alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    id         int NOT NULL
)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION uncompressed_subpartitioned_compresstype_1_201901 VALUES ( '201902' )
  (
    SUBPARTITION uncompressed_subpartitioned_compresstype_1_201901_a VALUES ( '1' ),
    SUBPARTITION uncompressed_subpartitioned_compresstype_1_201901_b VALUES ( '2' )
  ),
  PARTITION uncompressed_subpartitioned_compresstype_1_201902 VALUES ( '201903' )
  (
    SUBPARTITION uncompressed_subpartitioned_compresstype_1_201902_a VALUES ( '1' ),
    SUBPARTITION uncompressed_subpartitioned_compresstype_1_201902_b VALUES ( '2' )
  )
);
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1 values ('201902', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1 values ('201902', '2',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1 values ('201903', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1 values ('201903', '2',  generate_series(1, 10));
ALTER TABLE alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1 SET (compresstype = 1);
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_compresstype_1_201901_a';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_compresstype_1_201901_b';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_compresstype_1_201902_a';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_compresstype_1_201902_b';
\d+ alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1
SELECT count(*) FROM alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1;
DROP TABLE alter_compress_params_schema.uncompressed_subpartitioned_compresstype_1;

CREATE TABLE alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    id         int NOT NULL
)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201901 VALUES ( '201902' )
  (
    SUBPARTITION uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201901_a VALUES ( '1' ),
    SUBPARTITION uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201901_b VALUES ( '2' )
  ),
  PARTITION uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201902 VALUES ( '201903' )
  (
    SUBPARTITION uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201902_a VALUES ( '1' ),
    SUBPARTITION uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201902_b VALUES ( '2' )
  )
);
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3 values ('201902', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3 values ('201902', '2',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3 values ('201903', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3 values ('201903', '2',  generate_series(1, 10));
ALTER TABLE alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3 SET (compresstype = 2, compress_level = 10, compress_chunk_size = 2048, compress_prealloc_chunks = 3);
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201901_a';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201901_b';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201902_a';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3_201902_b';
\d+ alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3
SELECT count(*) FROM alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3;
DROP TABLE alter_compress_params_schema.uncompressed_subpartitioned_cl_10_ccs_2048_cpc_3;

CREATE TABLE alter_compress_params_schema.uncompressed_subpartitioned_all_options
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    id         int NOT NULL
)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION uncompressed_subpartitioned_all_options_201901 VALUES ( '201902' )
  (
    SUBPARTITION uncompressed_subpartitioned_all_options_201901_a VALUES ( '1' ),
    SUBPARTITION uncompressed_subpartitioned_all_options_201901_b VALUES ( '2' )
  ),
  PARTITION uncompressed_subpartitioned_all_options_201902 VALUES ( '201903' )
  (
    SUBPARTITION uncompressed_subpartitioned_all_options_201902_a VALUES ( '1' ),
    SUBPARTITION uncompressed_subpartitioned_all_options_201902_b VALUES ( '2' )
  )
);
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_all_options values ('201902', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_all_options values ('201902', '2',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_all_options values ('201903', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.uncompressed_subpartitioned_all_options values ('201903', '2',  generate_series(1, 10));
ALTER TABLE alter_compress_params_schema.uncompressed_subpartitioned_all_options SET (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true);
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_all_options_201901_a';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_all_options_201901_b';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_all_options_201902_a';
select relname, reloptions from pg_partition where relname = 'uncompressed_subpartitioned_all_options_201902_b';
\d+ alter_compress_params_schema.uncompressed_subpartitioned_all_options
SELECT count(*) FROM alter_compress_params_schema.uncompressed_subpartitioned_all_options;
DROP TABLE alter_compress_params_schema.uncompressed_subpartitioned_all_options;


-- set compressed options of compressed table 
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
CREATE TABLE alter_compress_params_schema.compressed_table_compresstype_2_cl_30 (id int, c1 text) with (compresstype  = 1);
INSERT INTO alter_compress_params_schema.compressed_table_compresstype_2_cl_30 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.compressed_table_compresstype_2_cl_30 SET (compresstype = 2, compress_level = 30);
\d+ alter_compress_params_schema.compressed_table_compresstype_2_cl_30
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.compressed_table_compresstype_2_cl_30;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.compressed_table_compresstype_2_cl_30', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.compressed_table_compresstype_2_cl_30', 0);
DROP TABLE alter_compress_params_schema.compressed_table_compresstype_2_cl_30;

CREATE TABLE alter_compress_params_schema.all_options_table_compresstype_1_cpc_1 (id int, c1 text) with (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true);
INSERT INTO alter_compress_params_schema.all_options_table_compresstype_1_cpc_1 SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.all_options_table_compresstype_1_cpc_1 SET (compresstype = 1, compress_level = 0, compress_chunk_size = 4096, compress_prealloc_chunks = 1, compress_byte_convert = false, compress_diff_convert = false);
\d+ alter_compress_params_schema.all_options_table_compresstype_1_cpc_1
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.all_options_table_compresstype_1_cpc_1;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.all_options_table_compresstype_1_cpc_1', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.all_options_table_compresstype_1_cpc_1', 0);
SELECT count(*) FROM compress_ratio_info(compress_func_findpath('alter_compress_params_schema.all_options_table_compresstype_1_cpc_1'));
SELECT count(*) FROM compress_statistic_info(compress_func_findpath('alter_compress_params_schema.all_options_table_compresstype_1_cpc_1'), 1);
DROP TABLE alter_compress_params_schema.all_options_table_compresstype_1_cpc_1;

CREATE TABLE alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512
(
	order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store	 CHAR(20)
)
WITH (compresstype  = 1)
PARTITION BY RANGE(sales_date)
(
	PARTITION compressed_partitioned_compresstype_2_cl_30_ccs_512_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION compressed_partitioned_compresstype_2_cl_30_ccs_512_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION compressed_partitioned_compresstype_2_cl_30_ccs_512_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION compressed_partitioned_compresstype_2_cl_30_ccs_512_season4 VALUES LESS THAN(MAXVALUE)
);
INSERT INTO alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512 SELECT generate_series(1, 10), 'session1 item', '2021-02-01 00:00:00', 1000, '711';
INSERT INTO alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512 SELECT generate_series(1, 10), 'session2 item', '2021-05-01 00:00:00', 1000, '722';
INSERT INTO alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512 SELECT generate_series(1, 10), 'session3 item', '2021-08-01 00:00:00', 1000, '733';
INSERT INTO alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512 SELECT generate_series(1, 10), 'session4 item', '2021-11-01 00:00:00', 1000, '744';
ALTER TABLE alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512 SET (compresstype  = 2, compress_level = 30, compress_chunk_size = 512);
CHECKPOINT;
select relname, reloptions from pg_partition where relname = 'compressed_partitioned_compresstype_2_cl_30_ccs_512_season1';
select relname, reloptions from pg_partition where relname = 'compressed_partitioned_compresstype_2_cl_30_ccs_512_season2';
select relname, reloptions from pg_partition where relname = 'compressed_partitioned_compresstype_2_cl_30_ccs_512_season3';
select relname, reloptions from pg_partition where relname = 'compressed_partitioned_compresstype_2_cl_30_ccs_512_season4';
\d+ alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512
SELECT count(*) FROM alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512;
SELECT count(*) FROM compress_ratio_info(compress_func_findpath('alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512'));
SELECT count(*) FROM compress_statistic_info(compress_func_findpath('alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512'), 1);
DROP TABLE alter_compress_params_schema.compressed_partitioned_compresstype_2_cl_30_ccs_512;

CREATE TABLE alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1
(
	order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store	 CHAR(20)
)
WITH (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true)
PARTITION BY RANGE(sales_date)
(
	PARTITION all_options_partitioned_compresstype_1_cpc_1_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION all_options_partitioned_compresstype_1_cpc_1_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION all_options_partitioned_compresstype_1_cpc_1_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION all_options_partitioned_compresstype_1_cpc_1_season4 VALUES LESS THAN(MAXVALUE)
);
INSERT INTO alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1 SELECT generate_series(1, 10), 'session1 item', '2021-02-01 00:00:00', 1000, '711';
INSERT INTO alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1 SELECT generate_series(1, 10), 'session2 item', '2021-05-01 00:00:00', 1000, '722';
INSERT INTO alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1 SELECT generate_series(1, 10), 'session3 item', '2021-08-01 00:00:00', 1000, '733';
INSERT INTO alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1 SELECT generate_series(1, 10), 'session4 item', '2021-11-01 00:00:00', 1000, '744';
ALTER TABLE alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1 SET (compresstype = 1, compress_level = 0, compress_chunk_size = 4096, compress_prealloc_chunks = 1, compress_byte_convert = false, compress_diff_convert = false);
CHECKPOINT;
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_compresstype_1_cpc_1_season1';
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_compresstype_1_cpc_1_season2';
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_compresstype_1_cpc_1_season3';
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_compresstype_1_cpc_1_season4';
\d+ alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1
SELECT count(*) FROM alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1;
DROP TABLE alter_compress_params_schema.all_options_partitioned_compresstype_1_cpc_1;

CREATE TABLE alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    id         int NOT NULL
)
WITH (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION all_options_subpartitioned_compresstype_1_cpc_1_201901 VALUES ( '201902' )
  (
    SUBPARTITION all_options_subpartitioned_compresstype_1_cpc_1_201901_a VALUES ( '1' ),
    SUBPARTITION all_options_subpartitioned_compresstype_1_cpc_1_201901_b VALUES ( '2' )
  ),
  PARTITION all_options_subpartitioned_compresstype_1_cpc_1_201902 VALUES ( '201903' )
  (
    SUBPARTITION all_options_subpartitioned_compresstype_1_cpc_1_201902_a VALUES ( '1' ),
    SUBPARTITION all_options_subpartitioned_compresstype_1_cpc_1_201902_b VALUES ( '2' )
  )
);
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1 values ('201902', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1 values ('201902', '2',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1 values ('201903', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1 values ('201903', '2',  generate_series(1, 10));
ALTER TABLE alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1 SET (compresstype = 1, compress_level = 0, compress_chunk_size = 4096, compress_prealloc_chunks = 1, compress_byte_convert = false, compress_diff_convert = false);
CHECKPOINT;
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_compresstype_1_cpc_1_201901_a';
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_compresstype_1_cpc_1_201901_b';
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_compresstype_1_cpc_1_201902_a';
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_compresstype_1_cpc_1_201902_b';
\d+ alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1
SELECT count(*) FROM alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1;
SELECT count(*) FROM compress_ratio_info(compress_func_findpath('alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1'));
SELECT count(*) FROM compress_statistic_info(compress_func_findpath('alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1'), 1);
DROP TABLE alter_compress_params_schema.all_options_subpartitioned_compresstype_1_cpc_1;

-- set compressed table to uncompressed table
CREATE TABLE alter_compress_params_schema.all_options_table_uncompressed (id int, c1 text) with (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true);
INSERT INTO alter_compress_params_schema.all_options_table_uncompressed SELECT generate_series(1, 10), 'fsfsfsfsfsfsfsfsfsfsfsfssfsf';
ALTER TABLE alter_compress_params_schema.all_options_table_uncompressed SET (compresstype = 0, compress_level = 0, compress_chunk_size = 4096, compress_prealloc_chunks = 0, compress_byte_convert = false, compress_diff_convert = false);
\d+ alter_compress_params_schema.all_options_table_uncompressed
CHECKPOINT;
SELECT * FROM alter_compress_params_schema.all_options_table_uncompressed;
SELECT chunk_size, algorithm FROM pg_catalog.compress_address_header('alter_compress_params_schema.all_options_table_uncompressed', 0);
SELECT nchunks, chunknos FROM pg_catalog.compress_address_details('alter_compress_params_schema.all_options_table_uncompressed', 0);
DROP TABLE alter_compress_params_schema.all_options_table_uncompressed;


CREATE TABLE alter_compress_params_schema.all_options_partitioned_uncompressed
(
	order_no INTEGER NOT NULL,
    goods_name CHAR(20) NOT NULL,
    sales_date DATE NOT NULL,
    sales_volume INTEGER,
    sales_store	 CHAR(20)
)
WITH (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true)
PARTITION BY RANGE(sales_date)
(
	PARTITION all_options_partitioned_uncompressed_season1 VALUES LESS THAN('2021-04-01 00:00:00'),
    PARTITION all_options_partitioned_uncompressed_season2 VALUES LESS THAN('2021-07-01 00:00:00'),
    PARTITION all_options_partitioned_uncompressed_season3 VALUES LESS THAN('2021-10-01 00:00:00'),
    PARTITION all_options_partitioned_uncompressed_season4 VALUES LESS THAN(MAXVALUE)
);
INSERT INTO alter_compress_params_schema.all_options_partitioned_uncompressed SELECT generate_series(1, 10), 'session1 item', '2021-02-01 00:00:00', 1000, '711';
INSERT INTO alter_compress_params_schema.all_options_partitioned_uncompressed SELECT generate_series(1, 10), 'session2 item', '2021-05-01 00:00:00', 1000, '722';
INSERT INTO alter_compress_params_schema.all_options_partitioned_uncompressed SELECT generate_series(1, 10), 'session3 item', '2021-08-01 00:00:00', 1000, '733';
INSERT INTO alter_compress_params_schema.all_options_partitioned_uncompressed SELECT generate_series(1, 10), 'session4 item', '2021-11-01 00:00:00', 1000, '744';
ALTER TABLE alter_compress_params_schema.all_options_partitioned_uncompressed SET (compresstype = 0, compress_level = 0, compress_chunk_size = 4096, compress_prealloc_chunks = 0, compress_byte_convert = false, compress_diff_convert = false);
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_uncompressed_season1';
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_uncompressed_season2';
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_uncompressed_season3';
select relname, reloptions from pg_partition where relname = 'all_options_partitioned_uncompressed_season4';
\d+ alter_compress_params_schema.all_options_partitioned_uncompressed
SELECT count(*) FROM alter_compress_params_schema.all_options_partitioned_uncompressed;
DROP TABLE alter_compress_params_schema.all_options_partitioned_uncompressed;

CREATE TABLE alter_compress_params_schema.all_options_subpartitioned_uncompressed
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    id         int NOT NULL
)
WITH (compresstype = 2, compress_level = 30, compress_chunk_size = 512,  compress_prealloc_chunks = 7, compress_byte_convert = true, compress_diff_convert = true)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION all_options_subpartitioned_uncompressed_201901 VALUES ( '201902' )
  (
    SUBPARTITION all_options_subpartitioned_uncompressed_201901_a VALUES ( '1' ),
    SUBPARTITION all_options_subpartitioned_uncompressed_201901_b VALUES ( '2' )
  ),
  PARTITION all_options_subpartitioned_uncompressed_201902 VALUES ( '201903' )
  (
    SUBPARTITION all_options_subpartitioned_uncompressed_201902_a VALUES ( '1' ),
    SUBPARTITION all_options_subpartitioned_uncompressed_201902_b VALUES ( '2' )
  )
);
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_uncompressed values ('201902', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_uncompressed values ('201902', '2',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_uncompressed values ('201903', '1',  generate_series(1, 10));
INSERT INTO alter_compress_params_schema.all_options_subpartitioned_uncompressed values ('201903', '2',  generate_series(1, 10));
ALTER TABLE alter_compress_params_schema.all_options_subpartitioned_uncompressed SET (compresstype = 0, compress_level = 0, compress_chunk_size = 4096, compress_prealloc_chunks = 0, compress_byte_convert = false, compress_diff_convert = false);
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_uncompressed_201901_a';
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_uncompressed_201901_b';
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_uncompressed_201902_a';
select relname, reloptions from pg_partition where relname = 'all_options_subpartitioned_uncompressed_201902_b';
\d+ alter_compress_params_schema.all_options_subpartitioned_uncompressed
SELECT count(*) FROM alter_compress_params_schema.all_options_subpartitioned_uncompressed;
DROP TABLE alter_compress_params_schema.all_options_subpartitioned_uncompressed;

--Test relfilenode reuse for compressed segment table index.
--1.create compressed segment table
CREATE TABLE alter_compress_params_schema.seg_tbl_with_index(id int, c1 text) WITH(compresstype=1, segment=on);
CREATE TABLE alter_compress_params_schema.seg_tbl_with_20index(id int, c1 text) WITH(compresstype=1, segment=on);
INSERT INTO alter_compress_params_schema.seg_tbl_with_index SELECT id, id::text FROM generate_series(1,1000) id;
INSERT INTO alter_compress_params_schema.seg_tbl_with_20index SELECT id, id::text FROM generate_series(1,1000) id;
select count(*) from alter_compress_params_schema.seg_tbl_with_index;
select count(*) from alter_compress_params_schema.seg_tbl_with_index where id < 100;

select count(*) from alter_compress_params_schema.seg_tbl_with_20index;
select count(*) from alter_compress_params_schema.seg_tbl_with_20index where id < 100;
checkpoint;
vacuum alter_compress_params_schema.seg_tbl_with_index;
vacuum alter_compress_params_schema.seg_tbl_with_20index;
select count(*) from alter_compress_params_schema.seg_tbl_with_index;
select count(*) from alter_compress_params_schema.seg_tbl_with_index where id < 100;
select count(*) from alter_compress_params_schema.seg_tbl_with_20index;
select count(*) from alter_compress_params_schema.seg_tbl_with_20index where id < 100;

--2.create compressed segment index
create index on alter_compress_params_schema.seg_tbl_with_index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);
create index on alter_compress_params_schema.seg_tbl_with_20index(id) WITH (compresstype=2,compress_chunk_size=1024);

--3.alter the compressed segment index, after this the filenode in index metadata and in page buffers tag will by different
--since the index is not rebuilt so the buffer tag remains the same but the filenode opt for index is changed.
alter index alter_compress_params_schema.seg_tbl_with_index_id_idx set (compresstype=1); --failed
alter index alter_compress_params_schema.seg_tbl_with_index_id_idx set (compress_chunk_size=2048); --failed
alter index alter_compress_params_schema.seg_tbl_with_index_id_idx set (compress_prealloc_chunks=2); --success
alter index alter_compress_params_schema.seg_tbl_with_index_id_idx set (compress_level=2); --success
select count(*) from alter_compress_params_schema.seg_tbl_with_index;
select count(*) from alter_compress_params_schema.seg_tbl_with_index where id < 100;

alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx1 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx2 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx3 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx4 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx5 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx6 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx7 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx8 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx9 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx10 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx11 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx12 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx13 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx14 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx15 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx16 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx17 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx18 set (compress_level=2); --success
alter index alter_compress_params_schema.seg_tbl_with_20index_id_idx19 set (compress_level=2); --success


--4.drop table and it will trigger index drop, which will contain DropRelFileNodeAllBuffersUsingScan to clean buffers.
--Though the relfilenode differs in opt, we should ignore it and find the right buffers to clean.
drop table alter_compress_params_schema.seg_tbl_with_index;
drop table alter_compress_params_schema.seg_tbl_with_20index;

--5.create normal segment tables, if we didn't clean out of date buffers in step 4, here we will find unexpected buffers
--in memory, which should be on disk and not loaded till now, and it will lead to crash.
CREATE TABLE alter_compress_params_schema.seg_tbl_normal1 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal2 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal3 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal4 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal5 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal6 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal7 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal8 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal9 (id int, value varchar) WITH (segment = on);
CREATE TABLE alter_compress_params_schema.seg_tbl_normal10 (id int, value varchar) WITH (segment = on);

drop table alter_compress_params_schema.seg_tbl_normal1;
drop table alter_compress_params_schema.seg_tbl_normal2;
drop table alter_compress_params_schema.seg_tbl_normal3;
drop table alter_compress_params_schema.seg_tbl_normal4;
drop table alter_compress_params_schema.seg_tbl_normal5;
drop table alter_compress_params_schema.seg_tbl_normal6;
drop table alter_compress_params_schema.seg_tbl_normal7;
drop table alter_compress_params_schema.seg_tbl_normal8;
drop table alter_compress_params_schema.seg_tbl_normal9;
drop table alter_compress_params_schema.seg_tbl_normal10;
DROP SCHEMA alter_compress_params_schema;