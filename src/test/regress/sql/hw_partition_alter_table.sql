-- section 1: range partitioned table, the boundary of last partition is NOT MAXVALUE
-- create range partitioned table
create table altertable_rangeparttable
(
	c1 int,
	c2 float,
	c3 real,
	c4 text
) 
partition by range (c1, c2, c3, c4)
(
	partition altertable_rangeparttable_p1 values less than (10, 10.00, 19.156, 'h'),
	partition altertable_rangeparttable_p2 values less than (20, 20.89, 23.75, 'k'),
	partition altertable_rangeparttable_p3 values less than (30, 30.45, 32.706, 's')
);
create index index_altertable_rangeparttable_local1 on altertable_rangeparttable (c1, c2) local
(
	partition index_altertable_rangeparttable_local1_srp1_index_local tablespace PG_DEFAULT,
	partition index_altertable_rangeparttable_local1_srp2_index_local tablespace PG_DEFAULT,
	partition index_altertable_rangeparttable_local1_srp3_index_local tablespace PG_DEFAULT
); 
create index index_altertable_rangeparttable_local2 on altertable_rangeparttable (c1, (c1+c2)) local
(
	partition index_altertable_rangeparttable_local2_srp1_index_local tablespace PG_DEFAULT,
	partition index_altertable_rangeparttable_local2_srp2_index_local tablespace PG_DEFAULT,
 	partition index_altertable_rangeparttable_local2_srp3_index_local tablespace PG_DEFAULT
);

-- fail: name conflict with existing partitions
alter table altertable_rangeparttable add partition altertable_rangeparttable_p3 values less than (38);
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: length of maxvalue not equal with number of partition keys
alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (35, 39.05, 'x');

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: partition boundary can not contain null
alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (NULL, 44.15, 48.897, 'X');

alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (40, NULL, 48.897, 'X');

alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (40, 44.15, NULL, 'X');

alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (40, 44.15, 48.897, NULL);

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: boudary of new adding partition NOT behind boudary of last partition
alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (5, 4.15, 8.28, 'd');

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: add a partition whose boundary is the same as the last partition's boundary
alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (30, 30.45, 32.706, 's');

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: add a new partition p4
alter table altertable_rangeparttable add partition altertable_rangeparttable_p4 values less than (36, 45.25, 37.39, 'u') tablespace PG_DEFAULT;

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: add a new partition p4
alter table altertable_rangeparttable add partition altertable_rangeparttable_p40 values less than (36, 45.25, 37.39, 'u') tablespace PG_DEFAULT;

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: add 2 partition in 1 alter table statement
alter table altertable_rangeparttable add partition altertable_rangeparttable_p6 values less than (MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE), add partition altertable_rangeparttable_p5 values less than (MAXVALUE, MAXVALUE, 58.02, 'w');

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: add 2 partition in 1 alter table statement
alter table altertable_rangeparttable add partition altertable_rangeparttable_p5 values less than (MAXVALUE, MAXVALUE, 58.02, 'w'), add partition altertable_rangeparttable_p6 values less than (MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE);

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: drop 2 partition in 1 alter table statement
alter table altertable_rangeparttable drop partition altertable_rangeparttable_p5, drop partition altertable_rangeparttable_p6;

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: add a new partition whose boundaries contain MAXVALUE.
alter table altertable_rangeparttable add partition altertable_rangeparttable_p5 values less than (MAXVALUE, MAXVALUE, 58.02, 'w');

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: boundaries are all MAXVALUE
alter table altertable_rangeparttable add partition altertable_rangeparttable_p6 values less than (MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE);

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: no more partition can be added
alter table altertable_rangeparttable add partition altertable_rangeparttable_p7 values less than (66, 49.25, 99.69, 'w');

with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: drop not existing partition
alter table altertable_rangeparttable drop partition altertable_rangeparttable_p8;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: p6 , not exist
alter table altertable_rangeparttable drop partition for (MAXVALUE, MAXVALUE, MAXVALUE, MAXVALUE);
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: drop partition p3
alter table altertable_rangeparttable drop partition altertable_rangeparttable_p3;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: drop partition p5, boundary not full
alter table altertable_rangeparttable drop partition for (40, 40.00, 45.004);
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: drop partition p5
alter table altertable_rangeparttable drop partition for (40, 40.00, 45.004, 'v');
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- fail: drop partition p5, already dropped in last step
alter table altertable_rangeparttable drop partition altertable_rangeparttable_p5;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: drop partition p4(-->p6)
alter table altertable_rangeparttable drop partition for (36, 45.25, 37.39, 'u');
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- success: p2, p4
alter table altertable_rangeparttable drop partition altertable_rangeparttable_p2, drop partition altertable_rangeparttable_p4;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- should not tips: "Cannot drop the only partition of a partitioned table"
-- but: partition "XXX" does not exist
alter table altertable_rangeparttable drop partition altertable_rangeparttable_p2;

-- fail: drop partition p1, the last one
alter table altertable_rangeparttable drop partition altertable_rangeparttable_p1;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'altertable_rangeparttable' or relname = 'index_altertable_rangeparttable_local1' or relname = 'index_altertable_rangeparttable_local2'
)
select relname, boundaries from pg_partition
	where parentid in (select oid from partitioned_obj_oids)
	order by relname;

-- drop table
drop table altertable_rangeparttable;

-- section 2: interval partitioned table
-- create interval partitioned table 
-- create table intervalPartTable
--(
--	c1 timestamp,
--	c2 float,
--	c3 real,
--	c4 text
--) 
--partition by range (c1)
--interval (interval '2 23:56:45' day to second)
--(
--	partition p1 values less than ('2012-01-01'),
--	partition p2 values less than ('2012-02-01'),
--	partition p3 values less than ('2012-03-01')
--);
--create index index_intervalPartTable_local1 on intervalPartTable (c1, c2) local
--(
--	partition srp1_index_local tablespace PG_DEFAULT,
--	partition srp2_index_local tablespace PG_DEFAULT,
-- 	partition srp3_index_local tablespace PG_DEFAULT
--); 
--create index index_intervalPartTable_local2 on intervalPartTable (c1, (c2+c3)) local
--(
--	partition srp1_index_local tablespace PG_DEFAULT,
--	partition srp2_index_local tablespace PG_DEFAULT,
-- 	partition srp3_index_local tablespace PG_DEFAULT
--);
-- fail: add partition p4, can not add partition against interval partitioned table
--alter table intervalPartTable add partition p1 values less than ('2012-09-01');
--select relname, boundaries from pg_partition order by relname;
-- dpl set off 2014-01-20
--insert into intervalPartTable values ('2013-04-01');
--insert into intervalPartTable values ('2013-05-01');
--insert into intervalPartTable values ('2013-06-01');
--alter table intervalPartTable drop partition for ('2013-06-01');
--alter table intervalPartTable drop partition for ('2013-05-01');
--alter table intervalPartTable drop partition for ('2013-04-01');
-- fail: annot drop last range partition of interval partitioned table
--alter table intervalPartTable drop partition p3;
--select relname, boundaries from pg_partition order by relname;

-- success: drop partition p1, p2
--alter table intervalPartTable drop partition p2;
--select relname, boundaries from pg_partition order by relname;

--alter table intervalPartTable drop partition p1;
--select relname, boundaries from pg_partition order by relname;

-- drop table
--drop table intervalPartTable;

CREATE TABLE DBA_IND_PARTITIONS_TABLE_011_1(
 C_CHAR_3 CHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT INTEGER,
 C_NUMERIC numeric(10,5),
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE)
 partition by range (C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT)
( 
     partition DBA_IND_PARTITIONS_011_1_1 values less than ('D', 'd', 400, '2000-04-01'),
     partition DBA_IND_PARTITIONS_011_1_2 values less than ('G', 'g', 700, '2000-07-01')
);
select relname ,boundaries from pg_partition 
	where parentid = (select oid from pg_class where relname = 'dba_ind_partitions_table_011_1')
	order by 1, 2;
alter table DBA_IND_PARTITIONS_TABLE_011_1 add partition DBA_IND_PARTITIONS_011_1_3 values less than ('Z', 'z',1000, '2000-10-01');
select relname ,boundaries from pg_partition where upper(relname) = upper('DBA_IND_PARTITIONS_011_1_3') order by 1, 2;
drop table DBA_IND_PARTITIONS_TABLE_011_1;

----------------------------------------------------------------------
--test the alter row movement
create table altertable_rowmovement_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition altertable_rowmovement_table_p0 values less than (50),
	partition altertable_rowmovement_table_p1 values less than (100),
	partition altertable_rowmovement_table_p2 values less than (150)
);
select relname, relrowmovement from pg_class where upper(relname) = upper('ALTERTABLE_ROWMOVEMENT_TABLE') order by 1, 2;
-- relrowmovement = f

alter table altertable_rowmovement_table disable row movement;
select relname, relrowmovement from pg_class where upper(relname) = upper('ALTERTABLE_ROWMOVEMENT_TABLE') order by 1, 2;
-- relrowmovement = f

alter table altertable_rowmovement_table enable row movement;
select relname, relrowmovement from pg_class where upper(relname) = upper('ALTERTABLE_ROWMOVEMENT_TABLE') order by 1, 2;
-- relrowmovement = t

alter table altertable_rowmovement_table disable row movement;
select relname, relrowmovement from pg_class where upper(relname) = upper('ALTERTABLE_ROWMOVEMENT_TABLE') order by 1, 2;
-- relrowmovement = f

alter table altertable_rowmovement_table enable row movement, disable row movement;
select relname, relrowmovement from pg_class where upper(relname) = upper('ALTERTABLE_ROWMOVEMENT_TABLE') order by 1, 2;
-- relrowmovement = f


alter table altertable_rowmovement_table add partition altertable_rowmovement_table_p3 values less than (200), enable row movement;
select relname, relrowmovement from pg_class where upper(relname) = upper('ALTERTABLE_ROWMOVEMENT_TABLE') order by 1, 2;
-- relrowmovement = t

drop table altertable_rowmovement_table;

--test the "movement" keyword ,use the "movement" key word as table name and column name
create table movement
(
	movement int
);
--create table succeed
drop table movement;


--test the non partitoned table , view , sequence 
create table altertable_rowmovement_table
(
	c1 int ,
	c2 int
);
alter table altertable_rowmovement_table enable row movement;
--error
alter table altertable_rowmovement_table disable row movement;
--error

create view altertable_rowmovement_view as select * from altertable_rowmovement_table;
alter table altertable_rowmovement_view enable row movement;
--error
alter table altertable_rowmovement_view disable row movement;
--error
alter view altertable_rowmovement_view enable row movement;
--error
alter view altertable_rowmovement_view disable row movement;
--error

drop view altertable_rowmovement_view;

create sequence altertable_rowmovement_seq;
alter table altertable_rowmovement_seq enable row movement;
--error
alter table altertable_rowmovement_seq disable row movement;
--error
alter sequence altertable_rowmovement_seq enable row movement;
--error
alter sequence altertable_rowmovement_seq disable row movement;
--error

drop sequence altertable_rowmovement_seq;

drop table altertable_rowmovement_table;

CREATE SCHEMA fvt_data_partition_alter_table;
CREATE TABLE fvt_data_partition_alter_table.TAB_OTHER_SQL_CMD_ANALYZE_013
(
	ANALYZE_COL_1 CHAR(102400),
	ANALYZE_COL_2 VARCHAR(1024),
	ANALYZE_COL_3 INTEGER,
	ANALYZE_COL_4 numeric(10,5),
	ANALYZE_COL_5 TIMESTAMP WITHOUT TIME ZONE
 )
partition by range (ANALYZE_COL_3)
(
	partition PAR_ANALYZE_013_1 values less than (400),
	partition PAR_ANALYZE_013_2 values less than (700),
	partition PAR_ANALYZE_013_3 values less than (1000)
);

INSERT INTO fvt_data_partition_alter_table.TAB_OTHER_SQL_CMD_ANALYZE_013 values(null,'abcdefg',111,1.0,null),('DEFGHIJ','defghij',444,4.44,'2000-04-04 04:04:04'),('GHIJKLM','ghijklm',777,7.77,'2000-07-07 07:07:07'),('B','bcdefghfsdaf',222,2.22222,null),('CD','cdefghifdsa',333,3.33333,'2000-03-03 03:03:03'),('EFGH','efghijk',555,5.55,'2000-05-05 05:05:05'),('FGHIJK',null,666,6.666666,'2000-06-06 06:06:06'),('HIJKLMN','hijklmn',888,8.88,'2000-08-08 08:08:08'),('I',null,999,9.99999,'2000-09-09 09:09:09');

ANALYZE fvt_data_partition_alter_table.TAB_OTHER_SQL_CMD_ANALYZE_013;

SELECT RELNAME,STAATTNUM,STANULLFRAC,STAWIDTH FROM PG_CLASS,PG_STATISTIC WHERE PG_STATISTIC.STARELID =  PG_CLASS.oid and  PG_CLASS.RELNAME = 'tab_other_sql_cmd_analyze_013' order by 1, 2, 3, 4;

ALTER TABLE fvt_data_partition_alter_table.TAB_OTHER_SQL_CMD_ANALYZE_013 DROP ANALYZE_COL_5;

SELECT RELNAME,STAATTNUM,STANULLFRAC,STAWIDTH FROM PG_CLASS,PG_STATISTIC WHERE PG_STATISTIC.STARELID =  PG_CLASS.oid and  PG_CLASS.RELNAME = 'tab_other_sql_cmd_analyze_013' order by 1, 2, 3, 4;

ANALYZE fvt_data_partition_alter_table.TAB_OTHER_SQL_CMD_ANALYZE_013;

SELECT RELNAME,STAATTNUM,STANULLFRAC,STAWIDTH FROM PG_CLASS,PG_STATISTIC WHERE PG_STATISTIC.STARELID =  PG_CLASS.oid and  PG_CLASS.RELNAME = 'tab_other_sql_cmd_analyze_013' order by 1, 2, 3, 4;

DROP SCHEMA fvt_data_partition_alter_table CASCADE;

-- Added by j0021848
--
---- Tese For  ALTER TABLE ADD table_constraint [ NOT VALID ]
--
CREATE TABLE TEST_CON (A INT4, B INT)
PARTITION BY RANGE (A)
(
	PARTITION TEST_CON_P0 VALUES LESS THAN (50),
	PARTITION TEST_CON_P1 VALUES LESS THAN (100),
	PARTITION TEST_CON_P2 VALUES LESS THAN (MAXVALUE)
);
SELECT P.RELNAME, P.PARTTYPE, P.PARTSTRATEGY, P.BOUNDARIES, C.RELNAME AS PARENT FROM PG_PARTITION P LEFT JOIN PG_CLASS C ON (P.PARENTID = C.OID)
	where P.parentid = (select oid from pg_class where relname = 'test_con')
	ORDER BY P.RELNAME;

-- CHECK CONSTRAINT
INSERT INTO TEST_CON VALUES (10, 10);
ALTER TABLE TEST_CON ADD CONSTRAINT TEST_CHECK_NOT_VALID CHECK(A < 0) NOT VALID;
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;

ALTER TABLE TEST_CON VALIDATE CONSTRAINT TEST_CHECK_NOT_VALID;
ALTER TABLE TEST_CON DROP CONSTRAINT TEST_CHECK_NOT_VALID;

ALTER TABLE TEST_CON ADD CHECK(A < 0) NOT VALID;
ALTER TABLE TEST_CON VALIDATE CONSTRAINT TEST_CON_A_CHECK;
ALTER TABLE TEST_CON DROP CONSTRAINT TEST_CON_A_CHECK;

ALTER TABLE TEST_CON ADD CHECK(A > 0) NOT VALID;
ALTER TABLE TEST_CON VALIDATE CONSTRAINT TEST_CON_A_CHECK;
ALTER TABLE TEST_CON DROP CONSTRAINT TEST_CON_A_CHECK;

-- UNIQUE CONSTRAINT
ALTER TABLE TEST_CON ADD CONSTRAINT TEST_UIQUE_A_NOT_VALID UNIQUE (A) NOT VALID; -- ERROR: UNIQUE CONSTRAINTS CANNOT BE MARKED NOT VALID

ALTER TABLE TEST_CON ADD CONSTRAINT TEST_UIQUE_VALID UNIQUE (A);
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
INSERT INTO TEST_CON VALUES (20, 201), (20, 202);
ALTER TABLE TEST_CON DROP CONSTRAINT IF EXISTS TEST_UIQUE_VALID;

ALTER TABLE TEST_CON ADD UNIQUE (A);
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
INSERT INTO TEST_CON VALUES (20, 201), (20, 202);
ALTER TABLE TEST_CON DROP CONSTRAINT IF EXISTS TEST_CON_A_KEY;


-- PRIMARY KEY CONSTRAINT
INSERT INTO TEST_CON VALUES (NULL, NULL);
ALTER TABLE TEST_CON ADD CONSTRAINT TEST_PRIMARY_KEY_A_NOT_VALID PRIMARY KEY (A) NOT VALID; -- ERROR: UNIQUE constraints cannot be marked NOT VALID
ALTER TABLE TEST_CON ADD CONSTRAINT TEST_PRIMARY_KEY_A_VALID PRIMARY KEY (A); -- ERROR: column "A" contains null values
ALTER TABLE TEST_CON ADD CONSTRAINT TEST_PRIMARY_KEY_B_VALID PRIMARY KEY (B); -- ERROR: unique index columns must contain the partition key and collation must be default collation

DELETE FROM TEST_CON WHERE A IS NULL;

ALTER TABLE TEST_CON ADD CONSTRAINT TEST_PRIMARY_KEY_A_VALID PRIMARY KEY (A);
INSERT INTO TEST_CON VALUES (NULL, NULL);
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
ALTER TABLE TEST_CON DROP CONSTRAINT IF EXISTS TEST_PRIMARY_KEY_A_VALID;

ALTER TABLE TEST_CON ADD PRIMARY KEY (A);
INSERT INTO TEST_CON VALUES (NULL, NULL);
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
ALTER TABLE TEST_CON DROP CONSTRAINT IF EXISTS TEST_CON_PKEY;

-- EXCLUDE CONSTRAINT
ALTER TABLE TEST_CON ADD CONSTRAINT TEST_EXCLUDE_NOT_VALID EXCLUDE (A WITH =) NOT VALID; -- ERROR: EXCLUDE constraints cannot be marked NOT VALID
ALTER TABLE TEST_CON ADD CONSTRAINT TEST_EXCLUDE_VALID EXCLUDE (A WITH =); -- ERROR: Partitioned table does not support EXCLUDE index

--
---- Tese For  ALTER TABLE ADD table_constraint_using_index
--

-- partitioned index does not support index with expression columns
-- ERROR: unique index columns must contain the partition key and the collation must be default collation
CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (INT4UP(A)) LOCAL;
--ERROR: partitioned table does not support global index
-- NEEDTOADJUST -- CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (INT4UP(A));

-- partitioned index does not support partial index
-- ERROR: syntax error at or near "LOCAL"
CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A) WHERE (A < 100) LOCAL;
-- ERROR: partitioned table does not support global index
-- NEEDTOADJUST -- CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A) WHERE (A < 100);

-- partitioned index does not support b-tree index without default sort ordering
CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A DESC NULLS LAST) LOCAL;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'index_on_test_con' or relname = 'test_con'
)
SELECT P.RELNAME, P.PARTTYPE, P.PARTSTRATEGY, C.RELNAME AS PARENT FROM PG_PARTITION P LEFT JOIN PG_CLASS C ON (P.PARENTID = C.OID) 
	where P.parentid in (select oid from partitioned_obj_oids)
	order by 1, 2, 3, 4;
-- ERROR: index "INDEX_ON_TEST_CON" does not have default sorting behavior
ALTER TABLE TEST_CON ADD CONSTRAINT CON UNIQUE USING INDEX INDEX_ON_TEST_CON;
DROP INDEX INDEX_ON_TEST_CON;

-- If PRIMARY KEY is specified, then this command will attempt to do ALTER COLUMN SET NOT NULL against each index's column
INSERT INTO TEST_CON VALUES (NULL, NULL);
CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A) LOCAL;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'index_on_test_con' or relname = 'test_con'
)
SELECT P.RELNAME, P.PARTTYPE, P.PARTSTRATEGY, P.BOUNDARIES, C.RELNAME AS PARENT FROM PG_PARTITION P LEFT JOIN PG_CLASS C ON (P.PARENTID = C.OID) 
	where P.parentid in (select oid from partitioned_obj_oids)
	order by 1, 2, 3, 4;
ALTER TABLE TEST_CON ADD PRIMARY KEY USING INDEX INDEX_ON_TEST_CON; -- ERROR: column "A" contains null values

DELETE FROM TEST_CON;
INSERT INTO TEST_CON VALUES (50, NULL);
ALTER TABLE TEST_CON ADD PRIMARY KEY USING INDEX INDEX_ON_TEST_CON;
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
ALTER TABLE TEST_CON DROP CONSTRAINT INDEX_ON_TEST_CON;

-- 1. If a constraint name is not provided, the constraint will be named the same as the index. 
-- 2. After this command is executed, the index is "owned" by the constraint
CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A) LOCAL;
ALTER TABLE TEST_CON ADD PRIMARY KEY USING INDEX INDEX_ON_TEST_CON;
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
ALTER TABLE TEST_CON DROP CONSTRAINT INDEX_ON_TEST_CON;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'test_con'
)
SELECT P.RELNAME, P.PARTTYPE, P.PARTSTRATEGY, P.BOUNDARIES, C.RELNAME AS PARENT FROM PG_PARTITION P LEFT JOIN PG_CLASS C ON (P.PARENTID = C.OID) 
	where P.parentid in (select oid from partitioned_obj_oids)
	ORDER BY 1, 2, 3, 4, 5;

CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A) LOCAL;
ALTER TABLE TEST_CON ADD UNIQUE USING INDEX INDEX_ON_TEST_CON;
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
ALTER TABLE TEST_CON DROP CONSTRAINT INDEX_ON_TEST_CON;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'test_con'
)
SELECT P.RELNAME, P.PARTTYPE, P.PARTSTRATEGY, P.BOUNDARIES, C.RELNAME AS PARENT FROM PG_PARTITION P LEFT JOIN PG_CLASS C ON (P.PARENTID = C.OID) 
	where P.parentid in (select oid from partitioned_obj_oids)
	ORDER BY 1, 2 , 3, 4, 5;

-- 1. If a constraint name is provided then the index will be renamed to match the constraint name. 
-- 2. After this command is executed, the index is "owned" by the constraint
CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A) LOCAL;
ALTER TABLE TEST_CON ADD CONSTRAINT CON UNIQUE USING INDEX INDEX_ON_TEST_CON;
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'con' or relname = 'test_con'
)
SELECT P.RELNAME, P.PARTTYPE, P.PARTSTRATEGY, P.BOUNDARIES, C.RELNAME AS PARENT FROM PG_PARTITION P LEFT JOIN PG_CLASS C ON (P.PARENTID = C.OID) 
	where P.parentid in (select oid from partitioned_obj_oids)
	order by 1, 2, 3, 4, 5;
SELECT SCHEMANAME, TABLENAME, INDEXNAME FROM PG_INDEXES WHERE upper(TABLENAME) = upper('TEST_CON') order by 1, 2, 3;
ALTER TABLE TEST_CON ADD CONSTRAINT CON_PK PRIMARY KEY USING INDEX INDEX_ON_TEST_CON; -- ERROR: index "INDEX_ON_TEST_CON" does not exist
ALTER TABLE TEST_CON ADD CONSTRAINT CON_PK PRIMARY KEY USING INDEX CON; -- ERROR: index "CON" is already associated with a constraint
ALTER TABLE TEST_CON DROP CONSTRAINT CON;

CREATE UNIQUE INDEX INDEX_ON_TEST_CON ON TEST_CON (A) LOCAL;
ALTER TABLE TEST_CON ADD CONSTRAINT CON_PK PRIMARY KEY USING INDEX INDEX_ON_TEST_CON;
SELECT CON.CONNAME, CON.CONTYPE, CON.CONVALIDATED, CLA.RELNAME AS CONRELNAME FROM PG_CONSTRAINT CON LEFT JOIN PG_CLASS CLA ON(CON. CONRELID = CLA.OID) WHERE upper(CLA.RELNAME) = upper('TEST_CON') order by 1, 2, 3, 4;
with partitioned_obj_oids as
(
	select oid 
	from pg_class 
	where relname = 'con_pk' or relname = 'test_con'
)
SELECT P.RELNAME, P.PARTTYPE, P.PARTSTRATEGY, P.BOUNDARIES, C.RELNAME AS PARENT FROM PG_PARTITION P LEFT JOIN PG_CLASS C ON (P.PARENTID = C.OID) 
	where P.parentid in (select oid from partitioned_obj_oids)
	order by 1, 2, 3, 4, 5;
SELECT SCHEMANAME, TABLENAME, INDEXNAME FROM PG_INDEXES WHERE upper(TABLENAME) = upper('TEST_CON');
ALTER TABLE TEST_CON ADD CONSTRAINT CON_PK2 PRIMARY KEY USING INDEX INDEX_ON_TEST_CON; -- ERROR: index "INDEX_ON_TEST_CON" does not exist
ALTER TABLE TEST_CON ADD CONSTRAINT CON_PK2 PRIMARY KEY USING INDEX CON_PK; -- ERROR: index "CON_PK" is already associated with a constraint
ALTER TABLE TEST_CON DROP CONSTRAINT CON_PK;

-- 
---- clean up
--
DROP TABLE TEST_CON;
