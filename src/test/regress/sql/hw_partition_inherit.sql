-- create parent tables
CREATE TABLE non_partitioned_table_parent1 (NAME VARCHAR, AGE INT);
CREATE TABLE non_partitioned_table_parent2 (GENDER BOOLEAN, SALARY INT);
CREATE TABLE partitioned_table_parent (NAME VARCHAR, AGE INT)
PARTITION BY RANGE (AGE)
(
	PARTITION non_partitioned_table_parent1_P1 VALUES LESS THAN (10),
	PARTITION non_partitioned_table_parent1_P2 VALUES LESS THAN (20),
	PARTITION non_partitioned_table_parent1_P3 VALUES LESS THAN (30)
);
-- fail
CREATE TABLE create_partitioned_table1 (NAME VARCHAR, AGE INT)
INHERITS (non_partitioned_table_parent1)
PARTITION BY RANGE (AGE)
(
	PARTITION create_partitioned_table1_P1 VALUES LESS THAN (10),
	PARTITION create_partitioned_table1_P2 VALUES LESS THAN (20),
	PARTITION create_partitioned_table1_P3 VALUES LESS THAN (30)
);
-- fail
CREATE TABLE create_partitioned_table2 (NAME VARCHAR, AGE INT)
INHERITS (non_partitioned_table_parent1, non_partitioned_table_parent2)
PARTITION BY RANGE (AGE)
(
	PARTITION create_partitioned_table2_P1 VALUES LESS THAN (10),
	PARTITION create_partitioned_table2_P2 VALUES LESS THAN (20),
	PARTITION create_partitioned_table2_P3 VALUES LESS THAN (30)
);
--fail
CREATE TABLE create_partitioned_table3 (NAME VARCHAR, AGE INT)
INHERITS (partitioned_table_parent)
PARTITION BY RANGE (AGE)
(
	PARTITION create_partitioned_table3_P1 VALUES LESS THAN (10),
	PARTITION create_partitioned_table3_P2 VALUES LESS THAN (20),
	PARTITION create_partitioned_table3_P3 VALUES LESS THAN (30)
);
--fail
CREATE TABLE create_partitioned_table4 (NAME VARCHAR, AGE INT)
INHERITS (partitioned_table_parent, non_partitioned_table_parent2)
PARTITION BY RANGE (AGE)
(
	PARTITION create_partitioned_table4_P1 VALUES LESS THAN (10),
	PARTITION create_partitioned_table4_P2 VALUES LESS THAN (20),
	PARTITION create_partitioned_table4_P3 VALUES LESS THAN (30)
);

--sucess
CREATE TABLE create_partitioned_table5 (NAME VARCHAR, AGE INT)
PARTITION BY RANGE (AGE)
(
	PARTITION create_partitioned_table5_P1 VALUES LESS THAN (10),
	PARTITION create_partitioned_table5_P2 VALUES LESS THAN (20),
	PARTITION create_partitioned_table5_P3 VALUES LESS THAN (30)
);
-- create inherit tables, inherit from partitioned table
-- fail
CREATE TABLE non_partitioned_table_son (NAME VARCHAR, AGE INT)
INHERITS (create_partitioned_table5);
-- fail
CREATE TABLE partitioned_table_son (NAME VARCHAR, AGE INT)
INHERITS (create_partitioned_table5)
PARTITION BY RANGE (AGE)
(
	PARTITION partitioned_table_son_p1 VALUES LESS THAN (10),
	PARTITION partitioned_table_son_p2 VALUES LESS THAN (20),
	PARTITION partitioned_table_son_p3 VALUES LESS THAN (30)
);
-- success
CREATE TABLE non_partitioned_table_son (NAME VARCHAR, AGE INT);
-- success
CREATE TABLE partitioned_table_son (NAME VARCHAR, AGE INT)
PARTITION BY RANGE (AGE)
(
	PARTITION partitioned_table_son_p1 VALUES LESS THAN (10),
	PARTITION partitioned_table_son_p2 VALUES LESS THAN (20),
	PARTITION partitioned_table_son_p3 VALUES LESS THAN (30)
);
-- fail
alter table NON_PARTITIONED_TABLE_SON inherit create_partitioned_table5;
-- fail
alter table PARTITIONED_TABLE_SON inherit create_partitioned_table5;
-- fail
alter table PARTITIONED_TABLE_SON inherit NON_PARTITIONED_TABLE_PARENT2;
-- fail
alter table NON_PARTITIONED_TABLE_SON no inherit create_partitioned_table5;
-- fail
alter table PARTITIONED_TABLE_SON no inherit create_partitioned_table5;
-- fail
alter table PARTITIONED_TABLE_SON no inherit NON_PARTITIONED_TABLE_PARENT2;

-- cleanup
drop table non_partitioned_table_parent1;
drop table non_partitioned_table_parent2;
drop table partitioned_table_parent;
drop table create_partitioned_table5;
drop table non_partitioned_table_son;
drop table partitioned_table_son;