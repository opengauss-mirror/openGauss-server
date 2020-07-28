
--create table
DROP TABLE IF EXISTS partition_unsable_index_1;
create table partition_unsable_index_1
(
	c1 int,
	c2 int,
	c3 date not null
)
partition by range (c3)
INTERVAL ('1 month') 
(
	PARTITION partition_unsable_index_1_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION partition_unsable_index_1_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION partition_unsable_index_1_p2 VALUES LESS THAN ('2020-05-01')
);

-- create 3 indexes, and specify it's partition name 
CREATE INDEX idx1_partition_unsable_index_1 on partition_unsable_index_1(c3) local 
(
	partition idx1_partition_unsable_index_1_p1,
	partition idx1_partition_unsable_index_1_p2,
	partition idx1_partition_unsable_index_1_p3
);
CREATE INDEX idx2_partition_unsable_index_1 on partition_unsable_index_1(c2, c3) local
(
	partition idx2_partition_unsable_index_1_p1,
	partition idx2_partition_unsable_index_1_p2,
	partition idx2_partition_unsable_index_1_p3
);
CREATE INDEX idx3_partition_unsable_index_1 on partition_unsable_index_1(c1, c2, c3) local
(
	partition idx3_partition_unsable_index_1_p1,
	partition idx3_partition_unsable_index_1_p2,
	partition idx3_partition_unsable_index_1_p3
);

--insert data
insert into partition_unsable_index_1 values(7,2,'2020-03-01');
insert into partition_unsable_index_1 values(3,1,'2020-04-01');
insert into partition_unsable_index_1 values(5,3,'2020-05-01');
insert into partition_unsable_index_1 values(7,5,'2020-06-01');
insert into partition_unsable_index_1 values(1,4,'2020-07-01');

-- query all index partitions
select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by relname;

-- 1. alter index, modify one of it's partition to unusable state
ALTER INDEX idx1_partition_unsable_index_1 MODIFY PARTITION idx1_partition_unsable_index_1_p1 UNUSABLE;

-- check indunusable info
select relname, parttype, partstrategy, boundaries, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;

-- rebuild index partition
ALTER INDEX idx1_partition_unsable_index_1 REBUILD PARTITION idx1_partition_unsable_index_1_p1;

-- check indunusable info
select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;

--2.ALTER INDEX unusable
select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;

ALTER INDEX idx1_partition_unsable_index_1 UNUSABLE;

select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;

ALTER INDEX idx1_partition_unsable_index_1 REBUILD;

select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;

--test for reindex partition
ALTER INDEX idx1_partition_unsable_index_1 UNUSABLE;
select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;

REINDEX INDEX idx1_partition_unsable_index_1 PARTITION idx1_partition_unsable_index_1_p1;
select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;

ALTER INDEX idx1_partition_unsable_index_1 REBUILD;
select relname, parttype, partstrategy, indisusable from pg_partition
	where parentid = (select oid from pg_class where relname = 'idx1_partition_unsable_index_1')
	order by 1;
		
-- 3. alter table, modify one of it's partition's all indexes to unusable state
ALTER TABLE partition_unsable_index_1 MODIFY PARTITION partition_unsable_index_1_p0 UNUSABLE LOCAL INDEXES;

-- check indunusable info
select relname, indisusable from pg_partition 
	where  relname = 'idx1_partition_unsable_index_1_p1' 
		or relname = 'idx2_partition_unsable_index_1_p1' 
		or relname = 'idx3_partition_unsable_index_1_p1'
		or relname = 'p1_partition_unsable_index_1'
		order by 1;

-- rebuild
ALTER TABLE partition_unsable_index_1 MODIFY PARTITION partition_unsable_index_1_p0 REBUILD UNUSABLE LOCAL INDEXES;

-- check again
select relname, indisusable from pg_partition 
	where  relname = 'idx1_partition_unsable_index_1_p1' 
		or relname = 'idx2_partition_unsable_index_1_p1' 
		or relname = 'idx3_partition_unsable_index_1_p1'
		or relname = 'p1_partition_unsable_index_1'
		order by 1;

ALTER INDEX idx1_partition_unsable_index_1 MODIFY PARTITION idx1_partition_unsable_index_1_p1 UNUSABLE;
-- idx1_partition_unsable_index_1_p1 is unusable
select part.relname, part.indisusable
		from pg_class class , pg_partition part , pg_index ind 
		where class.relname = 'partition_unsable_index_1' 
		and ind.indrelid = class.oid 
		and part.parentid = ind.indexrelid 
		order by 1;
-- can not cluster partition bacause of unusable local index
CLUSTER partition_unsable_index_1 PARTITION (partition_unsable_index_1_p0) USING idx1_partition_unsable_index_1;
CLUSTER partition_unsable_index_1 USING idx1_partition_unsable_index_1;

-- indisclustered is false
select class.relname, ind.indisclustered
		from pg_class class ,  pg_index ind 
		where class.relname = 'partition_unsable_index_1' 
		and ind.indrelid = class.oid 
		order by 2;

-- cluster ok
ALTER INDEX idx1_partition_unsable_index_1 REBUILD;
CLUSTER partition_unsable_index_1 USING idx1_partition_unsable_index_1;

-- idx1_partition_unsable_index_1_p1 is usable
select part.relname, part.indisusable
		from pg_class class , pg_partition part , pg_index ind 
		where class.relname = 'partition_unsable_index_1' 
		and ind.indrelid = class.oid 
		and part.parentid = ind.indexrelid 
		order by 1;

-- indisclustered is true
select class.relname, ind.indisclustered
		from pg_class class ,  pg_index ind 
		where class.relname = 'partition_unsable_index_1' 
		and ind.indrelid = class.oid 
		order by 2;

-- cluster other index
ALTER INDEX idx2_partition_unsable_index_1 MODIFY PARTITION idx2_partition_unsable_index_1_p1 UNUSABLE;
ALTER INDEX idx2_partition_unsable_index_1 REBUILD;
CLUSTER partition_unsable_index_1 USING idx2_partition_unsable_index_1;
select class.relname, ind.indisclustered
		from pg_class class ,  pg_index ind 
		where class.relname = 'partition_unsable_index_1' 
		and ind.indrelid = class.oid 
		order by 2;

-- cluster a partition
ALTER INDEX idx1_partition_unsable_index_1 MODIFY PARTITION idx1_partition_unsable_index_1_p1 UNUSABLE;
ALTER INDEX idx1_partition_unsable_index_1 REBUILD PARTITION idx1_partition_unsable_index_1_p1;
CLUSTER partition_unsable_index_1 PARTITION (partition_unsable_index_1_p0) USING idx1_partition_unsable_index_1;
CLUSTER partition_unsable_index_1 USING idx1_partition_unsable_index_1;
select class.relname, ind.indisclustered
		from pg_class class ,  pg_index ind 
		where class.relname = 'partition_unsable_index_1' 
		and ind.indrelid = class.oid 
		order by 2;

--5.2 merge (not support yet, keep the case hear for support in the future)
-- merge failed due to unusable index partition
ALTER INDEX idx1_partition_unsable_index_1 MODIFY PARTITION idx1_partition_unsable_index_1_p1 UNUSABLE;
ALTER TABLE partition_unsable_index_1 MERGE PARTITIONS partition_unsable_index_1_p0, p2_partition_unsable_index_3 
	INTO PARTITION px_partition_unsable_index_3;
--rebuild unusable index partition
ALTER INDEX idx1_partition_unsable_index_1 REBUILD PARTITION idx1_partition_unsable_index_1_p1;

--5.3 exchange
-- create plain table and index
CREATE TABLE table_unusable_index_exchange (c1 int, c2 int, c3 date not null);
CREATE INDEX idx1_table_unusable_index_exchange on table_unusable_index_exchange(c3);
CREATE INDEX idx2_table_unusable_index_exchange on table_unusable_index_exchange(c2, c3);
CREATE INDEX idx3_table_unusable_index_exchange on table_unusable_index_exchange(c1, c2, c3);

--- unusable non-partitioned-index
ALTER INDEX idx1_table_unusable_index_exchange UNUSABLE;
ALTER TABLE partition_unsable_index_1 EXCHANGE PARTITION (partition_unsable_index_1_p0)
	WITH TABLE table_unusable_index_exchange;
ALTER INDEX idx1_table_unusable_index_exchange REBUILD;

-- unusable partitioned-index
ALTER INDEX idx1_partition_unsable_index_1 UNUSABLE;
ALTER TABLE partition_unsable_index_1 EXCHANGE PARTITION (partition_unsable_index_1_p0)
	WITH TABLE table_unusable_index_exchange;
ALTER INDEX idx1_partition_unsable_index_1 REBUILD;

-- modify one index partition unusable
-- exchange failed due to unusable index partition
ALTER INDEX idx1_partition_unsable_index_1 MODIFY PARTITION idx1_partition_unsable_index_1_p1 UNUSABLE;
ALTER TABLE partition_unsable_index_1 EXCHANGE PARTITION (partition_unsable_index_1_p0)
	WITH TABLE table_unusable_index_exchange;
-- exchange ok
ALTER INDEX idx1_partition_unsable_index_1 REBUILD PARTITION idx1_partition_unsable_index_1_p1;
ALTER TABLE partition_unsable_index_1 EXCHANGE PARTITION (partition_unsable_index_1_p0)
	WITH TABLE table_unusable_index_exchange;

-- clean table_unusable_index_exchange
DROP TABLE table_unusable_index_exchange;
DROP TABLE partition_unsable_index_1;

--6. index and unique index check
create table partition_unsable_index_1
(
	c1 int,
	c2 int,
	c3 date not null
)
partition by range (c3)
INTERVAL ('1 month') 
(
	PARTITION partition_unsable_index_1_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION partition_unsable_index_1_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION partition_unsable_index_1_p2 VALUES LESS THAN ('2020-05-01')
);

-- create a unique index
CREATE UNIQUE INDEX idx_unique_partition_unsable_index_1 on partition_unsable_index_1(c2, c3) LOCAL
(
	partition idx_unique_partition_unsable_index_1_p1,
	partition idx_unique_partition_unsable_index_1_p2,
	partition idx_unique_partition_unsable_index_1_p3
);
--insert duplicated rows should report error
insert into partition_unsable_index_1 values(3,3,'2020-02-01');
insert into partition_unsable_index_1 values(5,3,'2020-02-01'); -- fail
-- set local index unusable
ALTER INDEX idx_unique_partition_unsable_index_1 MODIFY PARTITION idx_unique_partition_unsable_index_1_p1 UNUSABLE;
-- bypass the unique index check
insert into partition_unsable_index_1 values(5,3,'2020-02-01'); -- success
--but report unique check error here
ALTER INDEX idx_unique_partition_unsable_index_1 REBUILD PARTITION idx_unique_partition_unsable_index_1_p1;
-- cleanup
DROP INDEX idx_unique_partition_unsable_index_1;
insert into partition_unsable_index_1 values(5,3,'2020-02-01'); -- success
--cleanup
DROP TABLE partition_unsable_index_1;