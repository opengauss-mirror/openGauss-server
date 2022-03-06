CREATE TABLE tbl_pc(id int, c1 text) WITH(compresstype=2, compress_chunk_size=512);
create index on tbl_pc(id) WITH (compresstype=2,compress_chunk_size=1024);
INSERT INTO tbl_pc SELECT id, id::text FROM generate_series(1,1000) id;
checkpoint;
