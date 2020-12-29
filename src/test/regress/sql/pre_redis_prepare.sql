create table ptx (c1 int, c2 int, c3 int) 
distribute by hash (c1) 
PARTITION BY RANGE (c3)
(
	partition p0 values less than (10),
	partition p1 values less than (20),
	partition p2 values less than (maxvalue)
)
;
create schema data_redis;
CREATE TABLE data_redis.data_redis_tmp_12345 (LIKE ptx INCLUDING STORAGE INCLUDING RELOPTIONS INCLUDING DISTRIBUTION INCLUDING PARTITION, nodeid int,partitionoid int,tupleblocknum bigint,tupleoffset int);
create unlogged table data_redis.pg_delete_delta_12345
(
	xcnodeid int,
	dnrelid int,
	block_number bigint,
	block_offset int
)
distribute by hash(xcnodeid,dnrelid,block_number,block_offset);
insert into ptx (select x,x,x from generate_series(1,30) as x);
create table cmpts(c1 int, ts timestamp);
ALTER TABLE ptx SET (append_mode=on,rel_cn_oid=12345);
