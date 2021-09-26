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

reset current_schema;
drop schema test_get_table_def cascade;

