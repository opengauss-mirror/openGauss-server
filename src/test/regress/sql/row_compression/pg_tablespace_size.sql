CREATE TABLESPACE normal_tablespace RELATIVE LOCATION 'normal_tablespace';
SELECT pg_tablespace_size('normal_tablespace');
CREATE TABLE normal_table(id int) TABLESPACE normal_tablespace;
SELECT pg_tablespace_size('normal_tablespace');

CREATE TABLESPACE compress_tablespace RELATIVE LOCATION 'compress_tablespace';
SELECT pg_tablespace_size('compress_tablespace');
CREATE TABLE compressed_table_1024(id int) WITH(compresstype=2, compress_chunk_size=1024) TABLESPACE compress_tablespace;
SELECT pg_tablespace_size('compress_tablespace');
DROP TABLE normal_table;
DROP TABLESPACE normal_tablespace;
DROP TABLE compressed_table_1024;
DROP TABLESPACE compress_tablespace;

