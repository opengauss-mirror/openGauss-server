--
---- test interval partitioned global index
--

--drop table and index
drop index if exists ip_index_global;
drop index if exists ip_index_local;
drop table if exists gpi_partition_global_index_interval;
drop index if exists index_interval_partition_table_001;
drop table if exists interval_partition_table_001;
drop table if exists interval_partition_table_002;
drop table if exists interval_partition_table_003;
drop table if exists interval_partition_table_004;

create table gpi_partition_global_index_interval
(
    c1 int,
    c2 int,
    logdate date not null
)
partition by range (logdate)
INTERVAL ('1 month') 
(
    PARTITION gpi_partition_global_index_interval_p0 VALUES LESS THAN ('2020-03-01'),
    PARTITION gpi_partition_global_index_interval_p1 VALUES LESS THAN ('2020-04-01'),
    PARTITION gpi_partition_global_index_interval_p2 VALUES LESS THAN ('2020-05-01')
);
--succeed

--to select all index object
select part.relname, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible, part.reltoastrelid, part.partkey, part.interval, part.boundaries
from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

select count(*) from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid;

create index ip_index_global on gpi_partition_global_index_interval(c1) global;
--succeed

--to select all index object
select part.relname, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible, part.reltoastrelid, part.partkey, part.interval, part.boundaries
from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

select count(*) from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid;

create index ip_index_local on gpi_partition_global_index_interval(c2) local;
--succeed

--to select all index object
select part.relname, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible, part.reltoastrelid, part.partkey, part.interval, part.boundaries
from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

select count(*) from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid;

--insert into table 
insert into gpi_partition_global_index_interval values(7,2,'2020-03-01');
insert into gpi_partition_global_index_interval values(3,1,'2020-04-01');
insert into gpi_partition_global_index_interval values(5,3,'2020-05-01');
insert into gpi_partition_global_index_interval values(7,5,'2020-06-01');
insert into gpi_partition_global_index_interval values(1,4,'2020-07-01');
--succeed

explain (costs off) select * from gpi_partition_global_index_interval where c1 = 7 order by 1, 2;
select * from gpi_partition_global_index_interval where c1 = 7 order by 1, 2;

--to select all index object
select part.relname, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible, part.reltoastrelid, part.partkey, part.interval, part.boundaries
from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

select count(*) from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_partition_global_index_interval' and ind.indrelid = class.oid and part.parentid = ind.indrelid;

--
---- test input for unique index
--
create table interval_partition_table_001
(
    c1 int,
    logdate date not null
)
partition by range (logdate)
INTERVAL ('1 month') 
(
    PARTITION interval_partition_table_001_p0 VALUES LESS THAN ('2020-03-01'),
    PARTITION interval_partition_table_001_p1 VALUES LESS THAN ('2020-04-01'),
    PARTITION interval_partition_table_001_p2 VALUES LESS THAN ('2020-05-01')
);

create unique index index_interval_partition_table_001 on interval_partition_table_001(logdate) global;
--fail: unique index
insert into interval_partition_table_001 values(10, '2020-06-01');
insert into interval_partition_table_001 values(10, '2020-06-01');

--
---- test with primary key
--
create table interval_partition_table_002
(
	c1 int,
	c2 int,
	logdate date not null,
	CONSTRAINT interval_partition_table_CONSTRAINT PRIMARY KEY(c2,logdate)
)
partition by range (logdate)
INTERVAL ('1 month')
(
	PARTITION interval_partition_table_002_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION interval_partition_table_002_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION interval_partition_table_002_p2 VALUES LESS THAN ('2020-05-01')
);

insert into interval_partition_table_002 values(10, 10, '2020-06-01');
insert into interval_partition_table_002 values(10, 10, '2020-06-01');
analyze interval_partition_table_002;

--
---- test with btree index
--
create table interval_partition_table_003
(
    c1 int,
    c2 int,
    logdate date not null,
    PRIMARY KEY(c2,logdate)
)
partition by range (logdate)
INTERVAL ('1 month')
(
    PARTITION interval_partition_table_003_p0 VALUES LESS THAN ('2020-03-01'),
    PARTITION interval_partition_table_003_p1 VALUES LESS THAN ('2020-04-01'),
    PARTITION interval_partition_table_003_p2 VALUES LESS THAN ('2020-05-01')
);

create index interval_partition_table_003_1 ON interval_partition_table_003 USING BTREE (logdate) global;
create index interval_partition_table_003_2 ON interval_partition_table_003 USING BTREE (c2) global;
create index interval_partition_table_003_3 ON interval_partition_table_003 USING BTREE (c1) global;

select relname from pg_class where relname like '%interval_partition_table_003%' order by 1;

select relname, parttype, partstrategy, boundaries from pg_partition
where parentid = (select oid from pg_class where relname = 'interval_partition_table_003')
order by relname;

insert into interval_partition_table_003 values(1,2,'2020-03-01');
insert into interval_partition_table_003 values(1,2,'2020-04-01');
insert into interval_partition_table_003 values(1,2,'2020-05-01');
insert into interval_partition_table_003 values(1,2,'2020-06-01');
insert into interval_partition_table_003 values(1,2,'2020-07-01');

alter table interval_partition_table_003 drop column C2;
insert into interval_partition_table_003 values(1,2,'2020-07-01');
insert into interval_partition_table_003 values(1,'2020-07-01');

select relname from pg_class where relname like '%interval_partition_table_003%' order by 1;

select relname, parttype, partstrategy, boundaries from pg_partition
where parentid = (select oid from pg_class where relname = 'interval_partition_table_003')
order by relname;

--
---- test partition BTREE index
--
create table interval_partition_table_004
(
	c1 int,
	c2 int,
	logdate date not null,
	PRIMARY KEY(c2,logdate)
)
partition by range (logdate)
INTERVAL ('1 month')
(
	PARTITION interval_partition_table_004_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION interval_partition_table_004_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION interval_partition_table_004_p2 VALUES LESS THAN ('2020-05-01')
);

-- expression index
CREATE INDEX interval_partition_table_004_index_01 on interval_partition_table_004 using btree(c2) global;
CREATE INDEX interval_partition_table_004_index_02 on interval_partition_table_004 using btree(logdate) global;
CREATE INDEX interval_partition_table_004_index_03 on interval_partition_table_004 using btree(c1) global;

cluster interval_partition_table_004 using interval_partition_table_004_index_01;

insert into interval_partition_table_004 values(7,1,'2020-03-01');
insert into interval_partition_table_004 values(3,2,'2020-04-01');
insert into interval_partition_table_004 values(5,3,'2020-05-01');
insert into interval_partition_table_004 values(7,4,'2020-06-01');
insert into interval_partition_table_004 values(1,5,'2020-07-01');
insert into interval_partition_table_004 values(7,2,'2020-03-01');
insert into interval_partition_table_004 values(3,3,'2020-04-01');
insert into interval_partition_table_004 values(5,4,'2020-05-01');
insert into interval_partition_table_004 values(7,5,'2020-06-01');
insert into interval_partition_table_004 values(1,1,'2020-07-01');
insert into interval_partition_table_004 values(7,3,'2020-03-01');
insert into interval_partition_table_004 values(3,4,'2020-04-01');
insert into interval_partition_table_004 values(5,5,'2020-05-01');
insert into interval_partition_table_004 values(7,1,'2020-06-01');
insert into interval_partition_table_004 values(1,2,'2020-07-01');
insert into interval_partition_table_004 values(7,4,'2020-03-01');
insert into interval_partition_table_004 values(3,5,'2020-04-01');
insert into interval_partition_table_004 values(5,1,'2020-05-01');
insert into interval_partition_table_004 values(7,2,'2020-06-01');
insert into interval_partition_table_004 values(1,3,'2020-07-01');

explain (costs off) SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

delete from interval_partition_table_004 where c1 = 1;

explain (costs off) SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

update interval_partition_table_004 set c1 = 100 where c1 = 7;

explain (costs off) SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 100;

SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 100;

update interval_partition_table_004 set c1 = 7 where c1 = 100;

reindex table interval_partition_table_004;

cluster;

reindex table interval_partition_table_004;

cluster;

explain (costs off) SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

start transaction;
insert into interval_partition_table_004 values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('2000-01-01', 'YYYY-MM-DD'),TO_DATE('2019-12-01', 'YYYY-MM-DD'),'1 day'));
SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;
commit;
SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

delete from interval_partition_table_004;

\parallel on
insert into interval_partition_table_004 values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('1990-01-01', 'YYYY-MM-DD'),TO_DATE('2020-12-01', 'YYYY-MM-DD'),'1 day'));
select true from (SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7) where count = 0 or count = 11293;
select true from (SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7) where count = 0 or count = 11293;
select true from (SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7) where count = 0 or count = 11293;
select true from (SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7) where count = 0 or count = 11293;
select true from (SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7) where count = 0 or count = 11293;
\parallel off

SELECT COUNT(*) FROM interval_partition_table_004 where c1 = 7;

\parallel on
DELETE from interval_partition_table_004 where c1 = 1;
VACUUM analyze interval_partition_table_004;
DELETE from interval_partition_table_004 where c1 = 2;
VACUUM interval_partition_table_004;
DELETE from interval_partition_table_004 where c1 = 3;
ANALYZE interval_partition_table_004;
DELETE from interval_partition_table_004 where c1 = 4;
VACUUM analyze interval_partition_table_004;
DELETE from interval_partition_table_004 where c1 = 5;
VACUUM interval_partition_table_004;
DELETE from interval_partition_table_004 where c1 = 6;
ANALYZE interval_partition_table_004;
\parallel off

explain (costs off) SELECT COUNT(*) FROM interval_partition_table_004 where c1 <= 7;
SELECT COUNT(*) FROM interval_partition_table_004 where c1 <= 7;
VACUUM full interval_partition_table_004;
explain (costs off) SELECT COUNT(*) FROM interval_partition_table_004 where c1 <= 7;
SELECT COUNT(*) FROM interval_partition_table_004 where c1 <= 7;

--drop table and index
drop index if exists ip_index_global;
drop index if exists ip_index_local;
drop table if exists gpi_partition_global_index_interval;
drop index if exists index_interval_partition_table_001;
drop table if exists interval_partition_table_001;
drop table if exists interval_partition_table_002;
drop table if exists interval_partition_table_003;
drop table if exists interval_partition_table_004;
