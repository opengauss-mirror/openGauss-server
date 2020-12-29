drop schema columnar_storage cascade;
create schema columnar_storage;

--CREATE TABLE: partitioned relation
start transaction;
create table if not exists columnar_storage.create_columnar_table_152 ( c_smallint smallint not null,c_text text not null) 
distribute by hash(c_smallint)
partition by range(c_smallint)
(partition create_columnar_table_partition_p1 values less than(1),
partition create_columnar_table_partition_p2 values less than(3),
partition create_columnar_table_partition_p3 values less than(7),
partition create_columnar_table_partition_p4 values less than(2341),
partition create_columnar_table_partition_p5 values less than(11121),
partition create_columnar_table_partition_p6 values less than(22222)) ;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;

--CREATE TABLE: non-partitioned relation
start transaction;
create table if not exists columnar_storage.create_columnar_table_152_plain_table (c_smallint smallint not null,c_text text not null);
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and c.relkind = 'r' and c.oid >=16384 order by 1, 2;
commit;

--CREATE INDEX: partitioned index
start transaction;
create index idx1_create_columnar_table_152 on  columnar_storage.create_columnar_table_152(c_smallint) local
(
	partition idx1_create_columnar_table_152_p1,
	partition idx1_create_columnar_table_152_p2,
	partition idx1_create_columnar_table_152_p3,
	partition idx1_create_columnar_table_152_p4,
	partition idx1_create_columnar_table_152_p5,
	partition idx1_create_columnar_table_152_p6
);
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;
create index idx2_create_columnar_table_152 on  columnar_storage.create_columnar_table_152(c_smallint, c_text) local
(
	partition idx2_create_columnar_table_152_p1,
	partition idx2_create_columnar_table_152_p2,
	partition idx2_create_columnar_table_152_p3,
	partition idx2_create_columnar_table_152_p4,
	partition idx2_create_columnar_table_152_p5,
	partition idx2_create_columnar_table_152_p6
);

--CREATE INDEX: non-partitioned index
start transaction;
create index idx1_create_columnar_table_152_plain_table on  columnar_storage.create_columnar_table_152_plain_table(c_smallint);
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
commit;
create index idx2_create_columnar_table_152_plain_table on  columnar_storage.create_columnar_table_152_plain_table(c_smallint, c_text);

-- INSERT: partitioned relation
start transaction;
insert into columnar_storage.create_columnar_table_152 values (1, 'text_1');
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
--  on dn node, lock on partition of insert command should appear like following, but no locks on partition on cn node
--               relname               |       mode
-- ------------------------------------+------------------
--  create_columnar_table_partition_p2 | RowExclusiveLock
commit;

-- INSERT: non-partitioned relation
start transaction;
insert into columnar_storage.create_columnar_table_152_plain_table values (1, 'text_1');
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
commit;

-- UPDATE: partitioned relation
start transaction;
update columnar_storage.create_columnar_table_152 set c_text = 'text_001' where c_smallint = 1;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- on dn node, only one lock on relation: RowExclusiveLock
-- but on cn node, 2 locks on relation: AccessShareLock and RowExclusiveLock
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;

-- UPDATE: non-partitioned relation
start transaction;
update columnar_storage.create_columnar_table_152_plain_table set c_text = 'text_001' where c_smallint = 1;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
commit;

-- DELETE: partitioned relation
start transaction;
delete from columnar_storage.create_columnar_table_152 where c_smallint = 1;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- on dn node, only one lock on relation: RowExclusiveLock
-- but on cn node, 2 locks on relation: AccessShareLock and RowExclusiveLock
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;

-- DELETE: non-partitioned relation
start transaction;
delete from columnar_storage.create_columnar_table_152_plain_table where c_smallint = 1;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
commit;


--set an index to UNUSABLE
-- partitioned index
start transaction;
ALTER INDEX columnar_storage.idx1_create_columnar_table_152 UNUSABLE;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;

-- non-partitioned index
start transaction;
ALTER INDEX columnar_storage.idx1_create_columnar_table_152_plain_table UNUSABLE;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
commit;

--rebuid an INDEX
-- partitioned index
start transaction;
ALTER INDEX columnar_storage.idx1_create_columnar_table_152 REBUILD;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;
-- non-partitioned index
start transaction;
ALTER INDEX columnar_storage.idx1_create_columnar_table_152_plain_table REBUILD;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
commit;


--UNUSABLE a partition of an index
-- partitioned index
start transaction;
ALTER INDEX columnar_storage.idx1_create_columnar_table_152 MODIFY PARTITION idx1_create_columnar_table_152_p1 UNUSABLE;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;

--rebuild a partition of an index
start transaction;
ALTER INDEX columnar_storage.idx1_create_columnar_table_152 REBUILD PARTITION idx1_create_columnar_table_152_p1;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

start transaction;
REINDEX INDEX columnar_storage.idx1_create_columnar_table_152 PARTITION idx1_create_columnar_table_152_p1;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
commit;


--ALTER TABLE 'EXCHANGE PARTITIOM'
start transaction;
alter table columnar_storage.create_columnar_table_152 
	exchange partition (create_columnar_table_partition_p1) with table columnar_storage.create_columnar_table_152_plain_table;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--ALTER TABLE 'MERGE PARTITION'
start transaction;
alter table columnar_storage.create_columnar_table_152 
	merge partitions create_columnar_table_partition_p1, create_columnar_table_partition_p2 
	into partition create_columnar_table_partition_p1_2;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- on cn and dn, both are the same as following
--            relname             |        mode
----------------------------------+---------------------
-- create_columnar_table_152      | AccessExclusiveLock
-- create_columnar_table_152      | AccessShareLock
-- idx1_create_columnar_table_152 | AccessShareLock
-- idx2_create_columnar_table_152 | AccessShareLock
--(4 rows)
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
--  on cn
--               relname                |     mode      
----------------------------------------+---------------
-- create_columnar_table_partition_p1_2 | ExclusiveLock
-- idx1_create_columnar_table_152_p2    | ExclusiveLock
-- idx2_create_columnar_table_152_p2    | ExclusiveLock
--(3 rows)
--  on dn
--              relname               |        mode
--------------------------------------+---------------------
-- create_columnar_table_partition_p1 | AccessExclusiveLock
-- create_columnar_table_partition_p1 | ExclusiveLock
-- create_columnar_table_partition_p2 | AccessExclusiveLock
-- create_columnar_table_partition_p2 | ExclusiveLock
-- idx1_create_columnar_table_152_p1  | AccessExclusiveLock
-- idx1_create_columnar_table_152_p1  | ExclusiveLock
-- idx1_create_columnar_table_152_p2  | AccessExclusiveLock
-- idx1_create_columnar_table_152_p2  | ExclusiveLock
-- idx2_create_columnar_table_152_p1  | AccessExclusiveLock
-- idx2_create_columnar_table_152_p1  | ExclusiveLock
-- idx2_create_columnar_table_152_p2  | AccessExclusiveLock
-- idx2_create_columnar_table_152_p2  | ExclusiveLock
--(12 rows)
commit;


--ALTER TABLE 'SPLIT PARTITION'
start transaction;
alter table columnar_storage.create_columnar_table_152 
	split partition create_columnar_table_partition_p1_2
	into (partition create_columnar_table_partition_p1 values less than (1), partition create_columnar_table_partition_p2 values less than (3));
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- on both cn/dn node
--            relname             |        mode         
----------------------------------+---------------------
-- create_columnar_table_152      | AccessExclusiveLock
-- create_columnar_table_152      | AccessShareLock
-- idx1_create_columnar_table_152 | AccessShareLock
-- idx2_create_columnar_table_152 | AccessShareLock
--(4 rows)

-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
-- on cn node
--                         relname                          |        mode         
------------------------------------------------------------+---------------------
-- create_columnar_table_partition_p1                       | AccessExclusiveLock
-- create_columnar_table_partition_p1_c_smallint_c_text_idx | AccessExclusiveLock
-- create_columnar_table_partition_p1_c_smallint_idx        | AccessExclusiveLock
-- create_columnar_table_partition_p2                       | AccessExclusiveLock
-- create_columnar_table_partition_p2                       | ShareLock
-- create_columnar_table_partition_p2_c_smallint_c_text_idx | AccessExclusiveLock
-- create_columnar_table_partition_p2_c_smallint_idx        | AccessExclusiveLock
--(7 rows)
-- on dn node
--               relname                |        mode
----------------------------------------+---------------------
-- create_columnar_table_partition_p1_2 | AccessExclusiveLock
-- idx1_create_columnar_table_152_p2    | AccessExclusiveLock
-- idx2_create_columnar_table_152_p2    | AccessExclusiveLock
--(3 rows)
commit;

--set all local indexes of a table partition to UNUSABLE state.
--ALTER TABLE 'UNUSABLE INDEX'
start transaction;
ALTER TABLE columnar_storage.create_columnar_table_152 MODIFY PARTITION create_columnar_table_partition_p1 UNUSABLE LOCAL INDEXES;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
COMMIT;

--rebuild all local indexes of a table partition, after which all local indexes becomes into usable state.
--REINDEX PARTITION
start transaction;
ALTER TABLE columnar_storage.create_columnar_table_152 MODIFY PARTITION create_columnar_table_partition_p1 REBUILD UNUSABLE LOCAL INDEXES;
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
COMMIT;

--CLUSTER PARTITION
start transaction;
CLUSTER columnar_storage.create_columnar_table_152 partition (create_columnar_table_partition_p1);
-- locks on partitioned relation
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- locks on partition
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;


--1. check locks on partitions in 'analyze command'
start transaction;
analyze columnar_storage.create_columnar_table_152 ;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--2. check locks on relation and partitions in 'alter table truncate command'
start transaction;
alter table columnar_storage.create_columnar_table_152 truncate partition create_columnar_table_partition_p6;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--3. check locks on relation and partitions in 'alter table add partition'
start transaction;
alter table columnar_storage.create_columnar_table_152 add partition create_columnar_table_partition_p7 values less than (22299);
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--ALTER TABLE DROP PARTITION
start transaction;
alter table columnar_storage.create_columnar_table_152 drop partition create_columnar_table_partition_p6;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--4. check locks on relation and partitions in 'select'
--4.1 seq scan
set enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;

start transaction;
explain (costs off) select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;
--4.2 index scan
set enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;

start transaction;
explain (costs off) select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--4.3 index only scan
set enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;

start transaction;
explain (costs off) select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--4.4 bitmap scan
set enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;

start transaction;
explain (costs off) select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--4.5 tid scan
set enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;

start transaction;
explain (costs off) select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select * from columnar_storage.create_columnar_table_152 where c_smallint < 7;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
rollback;

--DROP INDEX: partitioned relation
start transaction;
drop index columnar_storage.idx1_create_columnar_table_152;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- on cn
--          relname          |        mode         
-----------------------------+---------------------
-- create_columnar_table_152 | AccessExclusiveLock
--(1 row)
-- on dn
--            relname             |        mode
----------------------------------+---------------------
-- create_columnar_table_152      | AccessExclusiveLock
-- idx1_create_columnar_table_152 | AccessExclusiveLock
--(2 rows)
select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
-- on cn , no locks on partition
-- on dn, 
--                      relname                      |        mode
-----------------------------------------------------+---------------------
-- create_columnar_table_partition_p1_c_smallint_idx | AccessExclusiveLock
-- create_columnar_table_partition_p2_c_smallint_idx | AccessExclusiveLock
-- idx1_create_columnar_table_152_p3                 | AccessExclusiveLock
-- idx1_create_columnar_table_152_p4                 | AccessExclusiveLock
-- idx1_create_columnar_table_152_p5                 | AccessExclusiveLock
-- idx1_create_columnar_table_152_p6                 | AccessExclusiveLock
--(6 rows)

rollback;

--DROP INDEX: non-partitioned relation
start transaction;
drop index columnar_storage.idx1_create_columnar_table_152_plain_table;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
rollback;

--DROP TABLE: partitioned relation
start transaction;
drop table columnar_storage.create_columnar_table_152;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'p' order by 1, 2;
-- on cn, no locks
-- on dn
--            relname             |        mode
----------------------------------+---------------------
-- create_columnar_table_152      | AccessExclusiveLock
-- idx1_create_columnar_table_152 | AccessExclusiveLock
-- idx2_create_columnar_table_152 | AccessExclusiveLock
--(3 rows)

select p.relname, l.mode from pg_locks as l inner join pg_partition as p
	 on l.classid = p.parentid and l.objid = p.oid
	 where l.locktype = 'partition' order by 1, 2;
-- on cn, no locks
-- on dn
--                         relname                          |        mode
------------------------------------------------------------+---------------------
-- create_columnar_table_partition_p1                       | AccessExclusiveLock
-- create_columnar_table_partition_p1_c_smallint_c_text_idx | AccessExclusiveLock
-- create_columnar_table_partition_p1_c_smallint_idx        | AccessExclusiveLock
-- create_columnar_table_partition_p2                       | AccessExclusiveLock
-- create_columnar_table_partition_p2_c_smallint_c_text_idx | AccessExclusiveLock
-- create_columnar_table_partition_p2_c_smallint_idx        | AccessExclusiveLock
-- create_columnar_table_partition_p3                       | AccessExclusiveLock
-- create_columnar_table_partition_p4                       | AccessExclusiveLock
-- create_columnar_table_partition_p5                       | AccessExclusiveLock
-- create_columnar_table_partition_p6                       | AccessExclusiveLock
-- idx1_create_columnar_table_152_p3                        | AccessExclusiveLock
-- idx1_create_columnar_table_152_p4                        | AccessExclusiveLock
-- idx1_create_columnar_table_152_p5                        | AccessExclusiveLock
-- idx1_create_columnar_table_152_p6                        | AccessExclusiveLock
-- idx2_create_columnar_table_152_p3                        | AccessExclusiveLock
-- idx2_create_columnar_table_152_p4                        | AccessExclusiveLock
-- idx2_create_columnar_table_152_p5                        | AccessExclusiveLock
-- idx2_create_columnar_table_152_p6                        | AccessExclusiveLock
--(18 rows)
commit;

--DROP TABLE: non-partitioned relation
start transaction;
drop table columnar_storage.create_columnar_table_152_plain_table;
select c.relname, l.mode from pg_locks as l inner join pg_class as c on l.relation = c.oid
	 where l.locktype = 'relation' and c.parttype = 'n' and (c.relkind = 'r' or c.relkind = 'i') and c.oid >=16384 order by 1, 2;
commit;

--cleanup
drop schema columnar_storage cascade;