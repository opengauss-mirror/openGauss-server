create table tx(c1 int, c2 int, c3 int);
create schema data_redis;
CREATE TABLE data_redis.data_redis_tmp_12345 (LIKE tx INCLUDING STORAGE INCLUDING RELOPTIONS INCLUDING DISTRIBUTION, nodeid int,tupleblocknum bigint,tupleoffset int);
create unlogged table data_redis.pg_delete_delta_12345
(
	xcnodeid int,
	dnrelid int,
	block_number bigint,
	block_offset int
)
distribute by hash(xcnodeid,dnrelid,block_number,block_offset);
ALTER TABLE tx SET (append_mode=on,rel_cn_oid=12345);
