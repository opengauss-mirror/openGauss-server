
create schema distribute_distinct;
set current_schema = distribute_distinct;

create table distribute_table_01 (c1 int, c2 bigint, c3 numeric) ;
insert into distribute_table_01 values (11, 21, 31);
insert into distribute_table_01 values (12, 22, 32);
insert into distribute_table_01 values (12, NULL, 32);
insert into distribute_table_01 values (13, 23, 33);
insert into distribute_table_01 values (13, 23, NULL);
insert into distribute_table_01 values (14, 24, 34);
insert into distribute_table_01 values (NULL, 24, 34);
insert into distribute_table_01 values (15, 25, 35);
insert into distribute_table_01 values (16, 26, 36);
insert into distribute_table_01 values (17, 27, 36);
insert into distribute_table_01 values (18, 27, 36);
insert into distribute_table_01 values (12, 22, 32);
insert into distribute_table_01 values (17, 27, 36);
insert into distribute_table_01 values (18, 27, 38);
insert into distribute_table_01 values (17, 27, 37);

create table distribute_table_02 (c1 int, c2 bigint, c3 numeric) ;
insert into distribute_table_02 values (11, 13, 31);
insert into distribute_table_02 values (12, 14, 32);
insert into distribute_table_02 values (12, NULL, 32);
insert into distribute_table_02 values (13, 14, 33);
insert into distribute_table_02 values (NULL, 14, 33);
insert into distribute_table_02 values (14, 15, 34);
insert into distribute_table_02 values (15, 15, 34);

analyze distribute_table_01;
analyze distribute_table_02;

set enable_mergejoin=off; 
set enable_nestloop=off; 

--Test settings:
--1. Distinct within top query;
--2. Without Group;
--3. Without Agg;

explain (verbose, costs off) select distinct c2, c3 from distribute_table_01 order by c2, c3 asc;
explain (verbose, costs off) select distinct c1, c2, c3 from distribute_table_01 order by c1, c2, c3 asc;

-- distribute key is hashkey
select distinct c1 from distribute_table_01 order by c1;
select distinct c1, c2, c3 from distribute_table_01 order by c1, c2, c3;
-- distribute key is not hashkey
select distinct c2 from distribute_table_01 order by c2;

select distinct c2, c3 from (select distribute_table_01.c2, distribute_table_02.c3 from distribute_table_01, distribute_table_02 where distribute_table_01.c1=distribute_table_02.c1) a order by c2, c3 asc;


--Test settings:
-- Distribute with agg
select c2, count(c3) from (select distinct c1, c2, c3 from distribute_table_01 order by c1) a group by a.c2 order by a.c2, count(c3);

-- Distribute key is not hashkey with semi join
select c1, c2, c3 from distribute_table_01 where c1 in (select distinct c2 from distribute_table_02 where c2 >= 12 order by c2) order by distribute_table_01.c1, distribute_table_01.c2, distribute_table_01.c3;

-- Distinct within sub query
select c2, c3 from (select distinct c2, c3 from distribute_table_01 order by c2, c3 asc) as a order by c2, c3 asc;
select distinct c31 from (select distinct c2, count(c3) c31 from distribute_table_01 group by c2 order by c2) as a order by c31 asc;

--Test cased summarizd from user requirements
--1. With union;
--2. With or without distributed columns;
--3. Using agg or unique node;
explain (verbose, costs off) 
(select distinct c1, c2, c3 from distribute_table_01 where c1<13) 
union
(select distinct c1, c2, c3 from distribute_table_01 where c1>=13);

--distribute by a hash key with union
((select distinct c1, c2, c3 from distribute_table_01 where c1<13) 
union
(select distinct c1, c2, c3 from distribute_table_01 where c1>=13)) order by c1, c2, c3;

--distribute by not a key with union
((select distinct c2, c3 from distribute_table_01 where c2<23) 
union
(select distinct c2, c3 from distribute_table_01 where c2>=23)) order by c2, c3;

--unique is in DN,and append is on CN
((select distinct c1, c2, c3 from distribute_table_01 where c1<13 order by c1) 
union
(select distinct c1, c2, c3 from distribute_table_01 where c1>=13)) order by c1, c2, c3;


--Test cased summarizd from user requirements
--1. Distinct appears in top query and sub query simultaneously;
--2. Using agg and unique node;
select distinct a.c1, a.c2, b.cn from 
	(select c1, c2 from distribute_table_01 where c3 > 31) a,
	(select c1, count(*) cn from (select distinct c1, c2, c3 from distribute_table_01 where c3>32) group by c1 having count(*)>1) b
where a.c1=b.c1
order by cn desc, a.c1;

--Test settings:
--1. Distinct appears in top query;
--2. With Group;
select distinct c1, c2 from distribute_table_01 group by c1, c2 order by c1, c2;
select distinct c2, c3 from distribute_table_01 group by c2, c3 order by c2, c3;

drop table distribute_table_01;
drop table distribute_table_02;

reset current_schema;
drop schema distribute_distinct;

--group is on CN 
RESET SEARCH_PATH;
CREATE SCHEMA DISTRIBUTE_QUERY;

CREATE TABLE DISTRIBUTE_QUERY.table_group_b20_000(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE );

SET search_path='distribute_query';
CREATE INDEX idx_btree_TABLE_group_b20_001 on distribute_query.table_group_b20_000(C_INT);
CREATE INDEX idx_btree_TABLE_group_b20_002 on distribute_query.table_group_b20_000(C_BIGINT);
CREATE INDEX idx_btree_TABLE_group_b20_003 on distribute_query.table_group_b20_000(C_SMALLINT);

-- TEST
SET TIME ZONE 'PRC';
set datestyle to iso;
SET search_path='distribute_query';
set enable_hashagg to false;
SELECT C_INT FROM table_group_b20_000 WHERE C_INT>20 AND C_INT<=21 AND C_BIGINT<2 GROUP BY C_INT ;

RESET SEARCH_PATH;
DROP SCHEMA DISTRIBUTE_QUERY CASCADE;


CREATE TABLE CTH(ID INT, ROWID TEXT, ROWDT TIMESTAMP, ATTRIBUTE TEXT, VAL TEXT);
SELECT DISTINCT ATTRIBUTE FROM CTH WHERE ATTRIBUTE = 'A' ORDER BY 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT DISTINCT ATTRIBUTE FROM CTH WHERE ATTRIBUTE = 'A' ORDER BY 1;
DROP TABLE CTH;

--FIX: when sort is under group,result is wrong dual to wrong targetlist
DROP SCHEMA group_sort CASCADE;
CREATE SCHEMA group_sort;
SET CURRENT_SCHEMA='group_sort';

CREATE TABLE group_sort.table_group_b5000_000(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE );

CREATE OR REPLACE PROCEDURE func_insert_tbl_group_b5000_000()
AS
BEGIN
	FOR I IN 1..15 LOOP
		FOR j IN 1..10 LOOP
			INSERT INTO table_group_b5000_000 VALUES('A','b5000_0eq','b5000_000EFGGAHWGS','a','abcdx','b5000_0001111ABHTFADFADFDAFAFEFAGEAFEAFEAGEAGEAGEE_'||i,i,j,i+j,i+0.0001,i+0.00001,i+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
		END LOOP;
	END LOOP;
END;
/

CALL func_insert_tbl_group_b5000_000();

analyze table_group_b5000_000;
SET TIME ZONE 'PRC';
SET DATESTYLE TO ISO;
SET CURRENT_SCHEMA='group_sort';
SET ENABLE_HASHAGG TO FALSE;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT C_INT,C_BIGINT FROM TABLE_GROUP_B5000_000 WHERE C_INT>0 AND C_INT<11 AND C_BIGINT<10 GROUP BY C_INT,C_BIGINT ORDER BY C_INT,C_BIGINT;
SELECT C_INT,C_BIGINT FROM TABLE_GROUP_B5000_000 WHERE C_INT>0 AND C_INT<11 AND C_BIGINT<10 GROUP BY C_INT,C_BIGINT ORDER BY C_INT,C_BIGINT;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT C_INT,C_FLOAT,(C_INT+C_FLOAT),C_BIGINT FROM TABLE_GROUP_B5000_000 WHERE C_INT>0 AND C_INT<14 AND C_BIGINT<10 GROUP BY C_INT,C_FLOAT,C_BIGINT ORDER BY C_INT,C_BIGINT DESC;
SELECT C_INT,C_FLOAT,(C_INT+C_FLOAT),C_BIGINT FROM TABLE_GROUP_B5000_000 WHERE C_INT>0 AND C_INT<14 AND C_BIGINT<10 GROUP BY C_INT,C_FLOAT,C_BIGINT ORDER BY C_INT,C_BIGINT DESC;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT SUM(C_INT),C_FLOAT,(C_INT+C_FLOAT),C_BIGINT FROM TABLE_GROUP_B5000_000 WHERE C_INT<=14 AND C_BIGINT>0 GROUP BY C_INT,C_FLOAT,C_BIGINT ORDER BY C_INT,C_BIGINT DESC;
SELECT SUM(C_INT),C_FLOAT,(C_INT+C_FLOAT),C_BIGINT FROM TABLE_GROUP_B5000_000 WHERE C_INT<=14 AND C_BIGINT>0 GROUP BY C_INT,C_FLOAT,C_BIGINT ORDER BY C_INT,C_BIGINT DESC;

RESET CURRENT_SCHEMA;
DROP SCHEMA GROUP_SORT CASCADE;
