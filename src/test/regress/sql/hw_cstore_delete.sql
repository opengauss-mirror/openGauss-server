---
--case 1: delete from normal table
--
drop table if exists t_row;
drop table if exists t_col;
create table t_row(c1 int, c2 int default NULL);
insert into t_row select generate_series(1,1001);
create table t_col(c1 int, c2 int default NULL) with (orientation = column);
insert into t_col select * from t_row;
delete from t_col;
insert into t_col select * from t_row;
delete from t_col where c1>1000;
select * from t_col where c1 < 1000 order by c1;

----
--case 2: delete from partition table
---
drop table if exists t_row;
drop table if exists t_col;
create table t_row(c1 int)
partition by range(c1)
(
	partition p1 values less than (10),
    partition p2 values less than (6000)
);
insert into t_row select generate_series(0, 2000);

create table t_col(c1 int) with(orientation=column) 
partition by range(c1)
(
	partition p1 values less than (10),
    partition p2 values less than (6000)
);
insert into t_col select * from t_row;
delete from t_col;
select count(*) from t_col;
insert into t_col select * from t_row;
select count(*) from t_col;
delete from t_col;
select count(*) from t_col;
--
-- case 3: partition && where condition
--
CREATE TABLE hw_delete_tbl01
(
	a int,
	b int,
	c int
) with ( orientation=column )
PARTITION BY RANGE (b) (
PARTITION p1 VALUES LESS THAN (10),
PARTITION p2 VALUES LESS THAN (20),
PARTITION p3 VALUES LESS THAN (1000)
);
COPY hw_delete_tbl01 FROM stdin;
1	2	3
1	3	4
1	4	5
1	5	6
1	6	7
1	7	8
1	8	9
1	9	10
\.
COPY hw_delete_tbl01 FROM stdin;
1	12	13
1	13	14
1	14	15
1	15	16
1	16	17
1	17	18
1	18	19
1	19	20
\.
INSERT INTO hw_delete_tbl01 SELECT * FROM hw_delete_tbl01;
SELECT * FROM hw_delete_tbl01 WHERE b%7=3 or c%11=7 ORDER BY 1, 2, 3;
DELETE FROM hw_delete_tbl01 WHERE b%7=3 or c%11=7;
SELECT * FROM hw_delete_tbl01 WHERE b%7=3 or c%11=7 ORDER BY 1, 2, 3;
DROP TABLE if exists hw_delete_tbl01;
-----
---case 4: delete where condition using index scan
----
drop schema if exists storage cascade;
create schema STORAGE;
CREATE TABLE STORAGE.IDEX_PARTITION_TABLE_001(COL_INT int) with(orientation=column);
insert into STORAGE.IDEX_PARTITION_TABLE_001 values(1000);
delete from STORAGE.IDEX_PARTITION_TABLE_001 where col_int=1000;
select * from STORAGE.IDEX_PARTITION_TABLE_001 ;
drop schema if exists storage cascade;

-----
---case 5: delete where condition using hash join
----
create table hw_delete_c1
(
    c1 int,
    c2 int
) with (orientation = column) 
partition by range (c1)
(
 partition c1_p1 values less than (10),
 partition c1_p2 values less than (20),
 partition c1_p3 values less than (30)
)
;

create table hw_delete_c2
(
    c1 int,
    c2 int,
    c3 int
) with (orientation = column) 
partition by range (c2)
(
 partition c2_p1 values less than (20),
 partition c2_p2 values less than (40),
 partition c2_p3 values less than (60)
);
insert into hw_delete_c1 select generate_series(1,29),generate_series(1,29);
insert into hw_delete_c2 select generate_series(1,59),generate_series(1,59);
--analyze;

delete from hw_delete_c1 using hw_delete_c2 where hw_delete_c1.c1=hw_delete_c2.c2;
select count(*) from hw_delete_c1;

drop table if exists hw_delete_c1;
drop table if exists hw_delete_c2;

-----
--- partital sort for delete
-----
create table hw_delete_row_1(id int, cu int, num int);
insert into hw_delete_row_1 values (1, generate_series(1, 1000000), generate_series(1, 1000000));

create table hw_delete_c3 (id int, cu int, num int) with (orientation = column, partial_cluster_rows = 600000) /*distribute by hash(id)*/;
insert into hw_delete_c3 select * from hw_delete_row_1;
delete from hw_delete_c3;

create table hw_delete_c4 (id int, cu int, num int) with (orientation = column, partial_cluster_rows = 600000) /*distribute by hash(id);*/
partition by range(num)(
partition part1 values less than(100001),
partition part2 values less than(200001),
partition part3 values less than(maxvalue)
);
insert into hw_delete_c4 select * from hw_delete_row_1;
delete from hw_delete_c4 returning cu;
delete from hw_delete_c4;
drop table  if exists hw_delete_row_1;
drop table  if exists hw_delete_c3;
drop table  if exists hw_delete_c4;
drop schema if exists cstore;