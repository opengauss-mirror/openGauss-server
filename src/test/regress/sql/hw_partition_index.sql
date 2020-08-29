--
---- test partitioned index
--

drop table if exists hw_partition_index_rp;
create table hw_partition_index_rp
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition hw_partition_index_rp_p0 values less than (50),
	partition hw_partition_index_rp_p1 values less than (100),
	partition hw_partition_index_rp_p2 values less than (150)
);

--succeed
create index rp_index_local1 on hw_partition_index_rp (c1) local;

--succeed
create index rp_index_local2 on hw_partition_index_rp (c1) local
(
	partition,
	partition,
	partition
);

--fail , the gram.y is not support opt_index_name
create index rp_index_local3 on hw_partition_index_rp (c1) local
(
	partition srp1_index_local,
	partition srp2_index_local,
	partition srp3_index_local
);

--succeed
create index rp_index_local4 on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create index rp_index_local5 on hw_partition_index_rp (c1) local
(
	partition tablespace PG_DEFAULT ,
	partition tablespace PG_DEFAULT,
	partition tablespace PG_DEFAULT
);

--fail ,syntax doesnot support opt_index_name
create  index rp_index_local6 on hw_partition_index_rp (c1) local
(
	partition srp1_index_local,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create unique index rp_index_local7 on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create unique index on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);
--succeed

--expression
create index on hw_partition_index_rp ((c1+c2)) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create index on hw_partition_index_rp ((c1-c2)) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create index on hw_partition_index_rp ((c1-c2),c1,c2) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create index on hw_partition_index_rp using btree ((c1-c2),c1,c2) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create index on hw_partition_index_rp using hash ((c1-c2),c1,c2) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--fail ERROR: access method "HASH" does not support multicolumn indexes
create index on hw_partition_index_rp using gin ((c1-c2),c1,c2) local
(
 partition srp1_index_local tablespace PG_DEFAULT,
 partition srp2_index_local tablespace PG_DEFAULT,
 partition srp3_index_local tablespace PG_DEFAULT
);

--fail ERROR: data type INTEGER has no default operator class for access method "GIN"
--not support CONCURRENTLY
create unique index CONCURRENTLY on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--fail
create unique index on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT
);

--fail 
create unique index on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT,
	partition srp4_index_local tablespace PG_DEFAULT
);

--fail 
create unique index on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT
);

--fail same partition name
create unique index on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace xx,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT
);

--fail wrong tablespace
create unique index on hw_partition_index_rp (c1) local
(
	srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT
);

--fail wrong syntax
create unique index global_index on hw_partition_index_rp (c1);
--fail
drop table hw_partition_index_rp;

--unique index , index para must contain partition key
create table hw_partition_index_rp
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition hw_partition_index_rp_p0 values less than (50),
	partition hw_partition_index_rp_p1 values less than (100),
	partition hw_partition_index_rp_p2 values less than (150)
);

create unique index rp_index_local on hw_partition_index_rp (c1) local
(
	partition srp1_index_local tablespace PG_DEFAULT ,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create unique index rp_index_loca2 on hw_partition_index_rp (c1,c2) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create unique index rp_index_loca3 on hw_partition_index_rp (c2) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--fail
--insert into table 
insert into hw_partition_index_rp values(100,200);

INSERT INTO hw_partition_index_rp VALUES(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10);
create unique index on hw_partition_index_rp (c1,c2) local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
select * from hw_partition_index_rp order by 1, 2;

--to select all index object
select part.relname, part.parttype, part.rangenum,
		part.intervalnum,
		part.partstrategy,
		part.relallvisible,
		part.reltoastrelid,
		part.partkey,
		part.interval,
		part.boundaries
		from pg_class class , pg_partition part , pg_index ind where class.relname = 'hw_partition_index_rp' and ind.indrelid = class.oid and part.parentid = ind.indexrelid order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

drop index rp_index_local,rp_index_loca2;

drop table if exists hw_partition_index_rp;

select count(*) from pg_class class , pg_partition part , pg_index ind where class.relname = 'hw_partition_index_rp' and ind.indrelid = class.oid and part.parentid = ind.indexrelid;

--is any index object?
create table hw_partition_index_rp(c1 text , c2 text )
partition by range(c1)
(
	partition hw_partition_index_rp_p1 values less than ('AAAAAAA'),
	partition hw_partition_index_rp_p2 values less than ('BBBBBBB'),
	partition hw_partition_index_rp_p3 values less than ('CCCCCCC')
);

create unique index rp_index_loca4 on hw_partition_index_rp (c1 COLLATE "de_DE") local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
); 

--fail
create unique index rp_index_loca4 on hw_partition_index_rp (c1 COLLATE "default") local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--succeed
create unique index rp_index_loca4 on hw_partition_index_rp (c1 COLLATE "xxxx") local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--fail
--create unique index rp_index_loca4 on hw_partition_index_rp ((c1 + c2)) local
--(
--	partition srp1_index_local tablespace PG_DEFAULT,
--	partition srp2_index_local tablespace PG_DEFAULT,
--	partition srp3_index_local tablespace PG_DEFAULT
--); 
create unique index rp_index_loca4 on hw_partition_index_rp (c2 COLLATE "xxxx") local
(
	partition srp1_index_local tablespace PG_DEFAULT,
	partition srp2_index_local tablespace PG_DEFAULT,
	partition srp3_index_local tablespace PG_DEFAULT
);

--fail
--create unique index rp_index_loca5 on hw_partition_index_rp ((c1 + c2),c1) local
--(
--	partition srp1_index_local tablespace PG_DEFAULT,
--	partition srp2_index_local tablespace PG_DEFAULT,
--	partition srp3_index_local tablespace PG_DEFAULT
--); 

--succeed
--create unique index rp_index_loca5 on hw_partition_index_rp ((c1 + c2),c1 collate "de_DE" ) local
--(
--	partition srp1_index_local tablespace PG_DEFAULT,
--	partition srp2_index_local tablespace PG_DEFAULT,
--	partition srp3_index_local tablespace PG_DEFAULT
--);

--fail
drop table if exists hw_partition_index_rp;

Create table hw_partition_index_t1
(
	C1 int,
	C2 int
);

Create unique index t1_unique_index_1 on hw_partition_index_t1 (c1, c2) local;

--fail non-partitioned table does not support local partitioned index
drop table hw_partition_index_t1;

create table partition_table_001
(
	SCORE int,
	NAME varchar(30)
)
partition by range (SCORE)
(
	partition SCORE1 values less than (60),
	partition SCORE2 values less than (80),
	partition SCORE3 values less than (100)
);

create unique index index_partition_table_001 on partition_table_001(SCORE) local;

insert into partition_table_001 values(10, 'aaa');
insert into partition_table_001 values(10, 'aaa');

--fail: unique index
drop table partition_table_001;

create table CREATE_PARTITION_PRIMARY_KEY_TABLE_022(
 C_CHAR_3 CHAR(10) ,
 C_VARCHAR_3 VARCHAR(1024),
 C_INT INTEGER,
 C_NUMERIC numeric(10,5),
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 CONSTRAINT CREATE_PARTITION_PRIMARY_KEY_CONSTRAINT_022 PRIMARY KEY(C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT)
)
partition by range (C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT)
(
	PARTITION CREATE_PARTITION_PRIMARY_KEY_TABLE_022_1 values less than ('AAAAA','aaaaa',400,'2000-5-12'),
	PARTITION CREATE_PARTITION_PRIMARY_KEY_TABLE_022_2 values less than ('FFFFF','fffff',1000,'2000-12-12')
);

alter table CREATE_PARTITION_PRIMARY_KEY_TABLE_022 add partition CREATE_PARTITION_PRIMARY_KEY_TABLE_022_3 values less than ('ZZZZZ','zzzzz',2000,'2005-12-12');

drop table CREATE_PARTITION_PRIMARY_KEY_TABLE_022;

create table INDEX_HASH_TABLE_013(
 C_CHAR_3 CHAR(102400),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT INTEGER,
 C_NUMERIC numeric(10,5),
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE
 )
partition by range (C_TS_WITHOUT)
(
	PARTITION INDEX_HASH_TABLE_013_1 values less than ('2000-5-12'),
	PARTITION INDEX_HASH_TABLE_013_2 values less than ('2000-12-12')
);

create index INDEX_HASH_INDEX_013_1 ON INDEX_HASH_TABLE_013 USING HASH (C_NUMERIC) LOCAL;
create index INDEX_HASH_INDEX_013_2 ON INDEX_HASH_TABLE_013 USING HASH (C_INT) LOCAL;

select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='index_hash_table_013_1') order by 1;
select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='index_hash_table_013_2') order by 1;

insert into INDEX_HASH_TABLE_013 values('AAAAA','aaaaa',100,10.2,'2000-5-12');
insert into INDEX_HASH_TABLE_013 values('BBBBB','bbbbb',200,20.2,'2000-6-12');
insert into INDEX_HASH_TABLE_013 values('CCCCC','ccccc',300,30.2,'2000-7-12');
insert into INDEX_HASH_TABLE_013 values('DDDDD','ddddd',400,40.2,'2000-8-12');
insert into INDEX_HASH_TABLE_013 values('EEEEE','eeeee',500,50.2,'2000-9-12');

alter table INDEX_HASH_TABLE_013 drop column C_NUMERIC;

select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='index_hash_table_013_1') order by 1;
select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='index_hash_table_013_2') order by 1;

select trim(c_char_3),trim(C_VARCHAR_3),c_int,c_ts_without from INDEX_HASH_TABLE_013 where C_INT=300 order by 1, 2, 3, 4;

analyze INDEX_HASH_TABLE_013;

drop table INDEX_HASH_TABLE_013;
-- test partition MERGE and BTREE index
CREATE TABLE hw_partition_index_00
(
id int,
val text
)
partition by range (id) 
(
partition p1 values less than (500), 
partition p2 values less than (1000), 
partition p3 values less than (2000)
);
-- expression index
CREATE INDEX idx_hw_partition_index_00 on hw_partition_index_00 using btree(lower(val)) local;
INSERT INTO hw_partition_index_00 VALUES(200, 'xxxxxxx'), (700, 'yyyyyyyy'), (1100, 'zzzzz');
alter table hw_partition_index_00 merge partitions p1, p2 into partition p4;
SELECT * FROM hw_partition_index_00 ORDER BY 1;
DROP TABLE hw_partition_index_00;
CREATE TABLE hw_partition_index_01
(
id int,
val text
)
partition by range (id) 
(
partition p1 values less than (500), 
partition p2 values less than (1000), 
partition p3 values less than (2000)
);
-- normal index
CREATE INDEX idx_hw_partition_index_01 on hw_partition_index_01 using btree(val) local;
INSERT INTO hw_partition_index_01 VALUES(200, 'xxxxxxx'), (700, 'yyyyyyyy'), (1100, 'zzzzz');
alter table hw_partition_index_01 merge partitions p1, p2 into partition p4;
SELECT * FROM hw_partition_index_01 ORDER BY 1;
DROP TABLE hw_partition_index_01;
