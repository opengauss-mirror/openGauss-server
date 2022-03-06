--
---- test interval partitioned index
--
drop table if exists hw_partition_index_ip;
create table hw_partition_index_ip
(
	c1 int,
	c2 int,
	logdate date not null
)
partition by range (logdate)
INTERVAL ('1 month') 
(
	PARTITION hw_partition_index_ip_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION hw_partition_index_ip_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION hw_partition_index_ip_p2 VALUES LESS THAN ('2020-05-01')
);
--succeed

create index ip_index_local1 on hw_partition_index_ip (c1) local;
--succeed

create index ip_index_local2 on hw_partition_index_ip (logdate) local
(
	partition,
	partition,
	partition
);
--fail , the gram.y is not support opt_index_name

create index ip_index_local3 on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local,
	partition sip2_index_local,
	partition sip3_index_local
);
--succeed

create index ip_index_local4 on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
--succeed

create  index ip_index_local6 on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
--succeed

create unique index ip_index_local7 on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
--succeed

create unique index on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
-- fail same partition name
create unique index on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT
);

--
---- expression, only test sample expression for inter1, more info see hw_partition_index.sql
--
create index on hw_partition_index_ip ((c1+c2)) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
--succeed

create index on hw_partition_index_ip ((c1-c2)) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
--succeed

--not support CONCURRENTLY
create unique index CONCURRENTLY on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);

-- Not enough index partition defined
create unique index on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT
);

-- number of partitions of LOCAL index must equal that of the underlying table
create unique index on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT,
	partition sip4_index_local tablespace PG_DEFAULT
);

create unique index on hw_partition_index_ip (logdate);
--succeed

drop table hw_partition_index_ip;

--unique index , index para must contain partition key
create table hw_partition_index_ip
(
	c1 int,
	c2 int,
	logdate date not null
)
partition by range (logdate)
INTERVAL ('1 month') 
(
	PARTITION hw_partition_index_ip_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION hw_partition_index_ip_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION hw_partition_index_ip_p2 VALUES LESS THAN ('2020-05-01')
);
-- succeed
create unique index ip_index_local on hw_partition_index_ip (c1,logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
-- fail without logdate 
create unique index ip_index_local2 on hw_partition_index_ip (c1) local
(
	partition sip1_index_local tablespace PG_DEFAULT ,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);
create unique index ip_index_local3 on hw_partition_index_ip (c2, c1) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);

--insert into table 
insert into hw_partition_index_ip values(7,2,'2020-03-01');
insert into hw_partition_index_ip values(3,1,'2020-04-01');
insert into hw_partition_index_ip values(5,3,'2020-05-01');
insert into hw_partition_index_ip values(7,5,'2020-06-01');
insert into hw_partition_index_ip values(1,4,'2020-07-01');

--succeed
select * from hw_partition_index_ip order by 1, 2;

--to select all index object
select part.relname, part.parttype, part.rangenum,
		part.intervalnum,
		part.partstrategy,
		part.relallvisible,
		part.reltoastrelid,
		part.partkey,
		part.interval,
		part.boundaries
		from pg_class class , pg_partition part , pg_index ind where class.relname = 'hw_partition_index_ip' and ind.indrelid = class.oid and part.parentid = ind.indexrelid order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

drop index ip_index_local;

drop table if exists hw_partition_index_ip;

select count(*) from pg_class class , pg_partition part , pg_index ind where class.relname = 'hw_partition_index_ip' and ind.indrelid = class.oid and part.parentid = ind.indexrelid;

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

create unique index index_interval_partition_table_001 on interval_partition_table_001(logdate) local;
--fail: unique index
insert into interval_partition_table_001 values(10, '2020-06-01');
insert into interval_partition_table_001 values(10, '2020-06-01');
analyze interval_partition_table_001;
drop table interval_partition_table_001;

--
---- test with primary key
--
create table interval_partition_table_002(
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
drop table interval_partition_table_002;

--
---- test with hash index, not support hash index yet;
--
create table interval_partition_table_003(
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

create index interval_partition_table_003_1 ON interval_partition_table_003 USING HASH (logdate) LOCAL;
create index interval_partition_table_003_2 ON interval_partition_table_003 USING HASH (c2) LOCAL;
create index interval_partition_table_003_3 ON interval_partition_table_003 USING HASH (c1) LOCAL;

select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='interval_partition_table_003_1') order by 1;
select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='interval_partition_table_003_2') order by 1;
select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='interval_partition_table_003_3') order by 1;

insert into interval_partition_table_003 values(1,2,'2020-03-01');
insert into interval_partition_table_003 values(1,2,'2020-04-01');
insert into interval_partition_table_003 values(1,2,'2020-05-01');
insert into interval_partition_table_003 values(1,2,'2020-06-01');
insert into interval_partition_table_003 values(1,2,'2020-07-01');

alter table interval_partition_table_003 drop column C2;
insert into interval_partition_table_003 values(1,2,'2020-07-01');
insert into interval_partition_table_003 values(1,'2020-07-01');

select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='interval_partition_table_003_1') order by 1;
select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='interval_partition_table_003_2') order by 1;
select relname from pg_partition where INDEXTBLID=(select RELFILENODE from pg_partition where relname='interval_partition_table_003_3') order by 1;

select relname, parttype, partstrategy, boundaries from pg_partition
	where parentid = (select oid from pg_class where relname = 'interval_partition_table_003')
	order by relname;

analyze interval_partition_table_003;
drop table interval_partition_table_003;

--
---- test partition BTREE index
--
create table interval_partition_table_004(
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
CREATE INDEX interval_partition_table_004_index_01 on interval_partition_table_004 using btree(c2) local;
CREATE INDEX interval_partition_table_004_index_02 on interval_partition_table_004 using btree(logdate) local;
CREATE INDEX interval_partition_table_004_index_03 on interval_partition_table_004 using btree(c1) local;

insert into interval_partition_table_004 values(7,2,'2020-03-01');
insert into interval_partition_table_004 values(3,1,'2020-04-01');
insert into interval_partition_table_004 values(5,3,'2020-05-01');
insert into interval_partition_table_004 values(7,5,'2020-06-01');
insert into interval_partition_table_004 values(1,4,'2020-07-01');
SELECT * FROM interval_partition_table_004 ORDER BY logdate;
SELECT * FROM interval_partition_table_004 ORDER BY 1;
SELECT * FROM interval_partition_table_004 ORDER BY 2;
DROP TABLE interval_partition_table_004;

-- test interval table with toast table and index
CREATE TABLE interval_sales
(prod_id NUMBER(6),
cust_id NUMBER,
time_id DATE,
channel_id CHAR(1),
promo_id NUMBER(6),
quantity_sold NUMBER(3),
amount_sold NUMBER(10,2)
)
PARTITION BY RANGE (time_id)
INTERVAL('1 MONTH')
( PARTITION p0 VALUES LESS THAN (TO_DATE('1-1-2008', 'DD-MM-YYYY')),
  PARTITION p1 VALUES LESS THAN (TO_DATE('6-5-2008', 'DD-MM-YYYY'))
);

select relname, case when reltoastrelid > 0 then 'TRUE' else 'FALSE' end as has_toastrelid, boundaries from pg_partition order by relname;

insert into interval_sales values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('2020-01-01', 'YYYY-MM-DD'),TO_DATE('2020-07-01', 'YYYY-MM-DD'),'1 day'), 1, 1, 1, 1);

select relname, case when reltoastrelid > 0 then 'TRUE' else 'FALSE' end as has_toastrelid, boundaries from pg_partition order by relname;

drop table interval_sales;

--
---- test exchange table whit index
--
