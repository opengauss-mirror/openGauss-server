create schema test_get_table_def;
set current_schema=test_get_table_def;

create table table_function_export_def_base (
    id integer primary key,
    name varchar(100)
);
create table table_function_export_def (
    id integer primary key,
    fid integer,
    constraint table_export_base_fkey foreign key (fid) references table_function_export_def_base(id)
);
select * from pg_get_tabledef('table_function_export_def');
drop table table_function_export_def;
drop table table_function_export_def_base;

--
---- test for partition table
--

--range table
create table table_range1 (id int, a date, b varchar)
partition by range (id)
(
    partition table_range1_p1 values less than(10),
    partition table_range1_p2 values less than(50),
    partition table_range1_p3 values less than(100),
    partition table_range1_p4 values less than(maxvalue)
);
select * from pg_get_tabledef('table_range1');
drop table table_range1;

create table table_range2 (id int, a date, b varchar)
partition by range (a)
(
    partition table_range2_p1 values less than('2020-03-01'),
    partition table_range2_p2 values less than('2020-05-01'),
    partition table_range2_p3 values less than('2020-07-01'),
    partition table_range2_p4 values less than(maxvalue)
);
select * from pg_get_tabledef('table_range2');
drop table table_range2;

create table table_range3 (id int, a date, b varchar)
partition by range (id, a)
(
    partition table_range3_p1 values less than(10, '2020-03-01'),
    partition table_range3_p2 values less than(50, '2020-05-01'),
    partition table_range3_p3 values less than(100, '2020-07-01'),
    partition table_range3_p4 values less than(maxvalue, maxvalue)
);
select * from pg_get_tabledef('table_range3');
drop table table_range3;

create table table_range4 (id int primary key, a date, b varchar)
partition by range (id)
(
    partition table_range4_p1 start (10) end (40) every (10),
    partition table_range4_p2 end (70),
    partition table_range4_p3 start (70),
    partition table_range4_p4 start (100) end (150) every (20)
);
select * from pg_get_tabledef('table_range4');
drop table table_range4;

--interval table
create table table_interval1 (id int, a date, b varchar)
partition by range (a)
interval ('1 day')
(
    partition table_interval1_p1 values less than('2020-03-01'),
    partition table_interval1_p2 values less than('2020-05-01'),
    partition table_interval1_p3 values less than('2020-07-01'),
    partition table_interval1_p4 values less than(maxvalue)
);
select * from pg_get_tabledef('table_interval1');
drop table table_interval1;

--list table
create table table_list1 (id int, a date, b varchar)
partition by list (id)
(
    partition table_list1_p1 values (1, 2, 3, 4),
    partition table_list1_p2 values (5, 6, 7, 8),
    partition table_list1_p3 values (9, 10, 11, 12)
);
select * from pg_get_tabledef('table_list1');
drop table table_list1;

create table table_list2 (id int, a date, b varchar)
partition by list (b)
(
    partition table_list2_p1 values ('1', '2', '3', '4'),
    partition table_list2_p2 values ('5', '6', '7', '8'),
    partition table_list2_p3 values ('9', '10', '11', '12')
);
select * from pg_get_tabledef('table_list2');
drop table table_list2;

create table table_list3 (id int primary key, a date, b varchar)
partition by list (b)
(
    partition table_list3_p1 values ('1', '2', '3', '4'),
    partition table_list3_p2 values ('5', '6', '7', '8'),
    partition table_list3_p3 values (default)
);
select * from pg_get_tabledef('table_list3');
drop table table_list3;

--hash table
create table table_hash1 (id int, a date, b varchar)
partition by hash (id)
(
    partition table_hash1_p1,
    partition table_hash1_p2,
    partition table_hash1_p3
);
select * from pg_get_tabledef('table_hash1');
drop table table_hash1;

--subpartition table
CREATE TABLE list_range_1 (
    col_1 integer primary key,
    col_2 integer,
    col_3 character varying(30) unique,
    col_4 integer
)
WITH (orientation=row, compression=no)
PARTITION BY LIST (col_1) SUBPARTITION BY RANGE (col_2)
(
    PARTITION p_list_1 VALUES (-1,-2,-3,-4,-5,-6,-7,-8,-9,-10)
    (
        SUBPARTITION p_range_1_1 VALUES LESS THAN (-10),
        SUBPARTITION p_range_1_2 VALUES LESS THAN (0),
        SUBPARTITION p_range_1_3 VALUES LESS THAN (10),
        SUBPARTITION p_range_1_4 VALUES LESS THAN (20),
        SUBPARTITION p_range_1_5 VALUES LESS THAN (50)
    ),
    PARTITION p_list_2 VALUES (1,2,3,4,5,6,7,8,9,10),
    PARTITION p_list_3 VALUES (11,12,13,14,15,16,17,18,19,20)
    (
        SUBPARTITION p_range_3_1 VALUES LESS THAN (15),
        SUBPARTITION p_range_3_2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p_list_4 VALUES (21,22,23,24,25,26,27,28,29,30)
    (
        SUBPARTITION p_range_4_1 VALUES LESS THAN (-10),
        SUBPARTITION p_range_4_2 VALUES LESS THAN (0),
        SUBPARTITION p_range_4_3 VALUES LESS THAN (10),
        SUBPARTITION p_range_4_4 VALUES LESS THAN (20),
        SUBPARTITION p_range_4_5 VALUES LESS THAN (50)
    ),
    PARTITION p_list_5 VALUES (31,32,33,34,35,36,37,38,39,40),
    PARTITION p_list_6 VALUES (41,42,43,44,45,46,47,48,49,50)
    (
        SUBPARTITION p_range_6_1 VALUES LESS THAN (-10),
        SUBPARTITION p_range_6_2 VALUES LESS THAN (0),
        SUBPARTITION p_range_6_3 VALUES LESS THAN (10),
        SUBPARTITION p_range_6_4 VALUES LESS THAN (20),
        SUBPARTITION p_range_6_5 VALUES LESS THAN (50)
    ),
    PARTITION p_list_7 VALUES (DEFAULT)
);
select * from pg_get_tabledef('list_range_1');
drop table list_range_1;

CREATE TABLE list_hash_2 (
    col_1 integer primary key,
    col_2 integer,
    col_3 character varying(30) unique,
    col_4 integer
)
WITH (orientation=row, compression=no)
PARTITION BY LIST (col_2) SUBPARTITION BY HASH (col_3)
(
    PARTITION p_list_1 VALUES (-1,-2,-3,-4,-5,-6,-7,-8,-9,-10)
    (
        SUBPARTITION p_hash_1_1,
        SUBPARTITION p_hash_1_2,
        SUBPARTITION p_hash_1_3
    ),
    PARTITION p_list_2 VALUES (1,2,3,4,5,6,7,8,9,10),
    PARTITION p_list_3 VALUES (11,12,13,14,15,16,17,18,19,20)
    (
        SUBPARTITION p_hash_3_1,
        SUBPARTITION p_hash_3_2
    ),
    PARTITION p_list_4 VALUES (21,22,23,24,25,26,27,28,29,30)
    (
        SUBPARTITION p_hash_4_1,
        SUBPARTITION p_hash_4_2,
        SUBPARTITION p_hash_4_3,
        SUBPARTITION p_hash_4_4,
        SUBPARTITION p_hash_4_5
    ),
    PARTITION p_list_5 VALUES (31,32,33,34,35,36,37,38,39,40),
    PARTITION p_list_6 VALUES (41,42,43,44,45,46,47,48,49,50)
    (
        SUBPARTITION p_hash_6_1,
        SUBPARTITION p_hash_6_2,
        SUBPARTITION p_hash_6_3,
        SUBPARTITION p_hash_6_4,
        SUBPARTITION p_hash_6_5
    ),
    PARTITION p_list_7 VALUES (DEFAULT)
);
create unique index list_hash_2_idx1 on list_hash_2(col_2, col_3, col_4) local;
create index list_hash_2_idx2 on list_hash_2(col_3, col_1) local;
create index list_hash_2_idx3 on list_hash_2(col_4) global;
select * from pg_get_tabledef('list_hash_2');
drop table list_hash_2;

reset current_schema;
drop schema test_get_table_def cascade;
