-- range partition table with only 1st level partition
drop table if exists only_first_part;

create table only_first_part (id int, name varchar)
partition by range (id) (
    partition only_first_part_p1 values less than (1000),
    partition only_first_part_p2 values less than (2000),
    partition only_first_part_p3 values less than (MAXVALUE)
);
insert into only_first_part select generate_series(1, 5000), 'Alice';

analyze only_first_part;

-- preview, no statistic for partition
select relname, relpages, reltuples from pg_class where relname = 'only_first_part';
select relname, relpages, reltuples from pg_partition where relname in ('only_first_part_p1', 'only_first_part_p2', 'only_first_part_p3');
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('only_first_part_p1', 'only_first_part_p2', 'only_first_part_p3')) order by starelid, staattnum;

-- test analyze partition
analyze only_first_part partition (only_first_part_p1);
analyze only_first_part partition (only_first_part_p2);
analyze only_first_part partition (only_first_part_p3);

-- check partiiton statistic
select relname, relpages, reltuples from pg_partition where relname in ('only_first_part_p1', 'only_first_part_p2', 'only_first_part_p3');
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('only_first_part_p1', 'only_first_part_p2', 'only_first_part_p3')) order by starelid, staattnum;

-- test check the correctness of table-level statistic (test for functional independent)
explain (costs off) select * from only_first_part;
explain (costs off) select * from only_first_part where id > 2000;
explain (costs off) select * from only_first_part where id > 5000;
explain (costs off) select * from only_first_part where id < 1000;
explain (costs off) select * from only_first_part where id <= 1000;
explain (costs off) select * from only_first_part where id >= 2000;
explain (costs off) select * from only_first_part where name = 'Alice';
explain (costs off) select * from only_first_part where name = 'Bob';

-- check use of partition statistics
explain (costs off) select * from only_first_part partition (only_first_part_p1);
explain (costs off) select * from only_first_part partition (only_first_part_p2);
explain (costs off) select * from only_first_part partition (only_first_part_p3);
explain (costs off) select * from only_first_part partition (only_first_part_p1) where id > 1000;
explain (costs off) select * from only_first_part partition (only_first_part_p1) where id < 1000;
explain (costs off) select * from only_first_part partition (only_first_part_p1) where id <= 5000;
explain (costs off) select * from only_first_part partition (only_first_part_p1) where name = 'Alice';
explain (costs off) select * from only_first_part partition (only_first_part_p1) where id < 5000 and id > 1000;

-- clean up
drop table only_first_part;


-- list partition table with only 1st level partition
drop table if exists only_first_part_two;

create table only_first_part_two (c1 int, c2 int)
partition by list (c2) (
    partition list_p1 values (1),
    partition list_p2 values (2, 4, 6)
);

insert into only_first_part_two select generate_series(1, 1000), 1;
insert into only_first_part_two select generate_series(1, 10), 2;
insert into only_first_part_two select generate_series(1, 100), 4;
insert into only_first_part_two select generate_series(1, 10), 6;

analyze only_first_part_two;

-- preview, no statistic for partition
select relname, relpages, reltuples from pg_class where relname = 'only_first_part_two';
select relname, relpages, reltuples from pg_partition where relname in ('list_p1', 'list_p2');
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('list_p1', 'list_p2')) order by starelid, staattnum;

-- test analyze partition
analyze only_first_part_two partition (list_p1);
analyze only_first_part_two partition (list_p2);

-- check partiiton statistic
select relname, relpages, reltuples from pg_partition where relname in ('list_p1', 'list_p2');
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('list_p1', 'list_p2')) order by starelid, staattnum;

-- test check the correctness of table-level statistic (test for functional independent)
explain (costs off) select * from only_first_part_two;
explain (costs off) select * from only_first_part_two where c1 > 100;
explain (costs off) select * from only_first_part_two where c1 <= 100;
explain (costs off) select * from only_first_part_two where c2 in (1, 2);
explain (costs off) select * from only_first_part_two where c2 > 3;
explain (costs off) select * from only_first_part_two where c2 > 6;
explain (costs off) select * from only_first_part_two where c2 < 1;

-- check use of partition statistics
explain (costs off) select * from only_first_part_two partition (list_p1);
explain (costs off) select * from only_first_part_two partition (list_p2);
explain (costs off) select * from only_first_part_two partition (list_p1) where c2 = 1;
explain (costs off) select * from only_first_part_two partition (list_p1) where c2 = 2;
explain (costs off) select * from only_first_part_two partition (list_p1) where c2 = 4;
explain (costs off) select * from only_first_part_two partition (list_p1) where c2 = 3;
explain (costs off) select * from only_first_part_two partition (list_p1) where c1 > 1000;
explain (costs off) select * from only_first_part_two partition (list_p1) where c1 <= 1000;

-- clean up
drop table only_first_part_two;

-- test diferent datatype for partition table in statistic collection (use tange partition)
drop table if exists test_range_int2;

create table test_range_int2 (c1 int2)
partition by range (c1) (
    partition range_int2_p1 values less than (500),
    partition range_int2_p2 values less than (1000)
);

insert into test_range_int2 select generate_series(1, 999);
analyze test_range_int2 partition (range_int2_p1);
analyze test_range_int2 partition (range_int2_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_int2_p1', 'range_int2_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_int2_p1', 'range_int2_p2')) order by starelid, staattnum;

drop table if exists test_range_int4;
create table test_range_int4 (c1 int4)
partition by range (c1) (
    partition range_int4_p1 values less than (500),
    partition range_int4_p2 values less than (1000)
);
insert into test_range_int4 select generate_series(1, 999);
analyze test_range_int4 partition (range_int4_p1);
analyze test_range_int4 partition (range_int4_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_int4_p1', 'range_int4_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_int4_p1', 'range_int4_p2')) order by starelid, staattnum;

drop table if exists test_range_int8;
create table test_range_int8 (c1 int8)
partition by range (c1) (
    partition range_int8_p1 values less than (500),
    partition range_int8_p2 values less than (1000)
);
insert into test_range_int8 select generate_series(1, 999);
analyze test_range_int8 partition (range_int8_p1);
analyze test_range_int8 partition (range_int8_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_int8_p1', 'range_int8_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_int8_p1', 'range_int8_p2')) order by starelid, staattnum;

drop table if exists test_range_numeric;
create table test_range_numeric (c1 numeric)
partition by range (c1) (
    partition range_numeric_p1 values less than (7.1),
    partition range_numeric_p2 values less than (8.4)
);
insert into test_range_numeric select (generate_series(1, 1000) * 0.001 + 6.3);
insert into test_range_numeric select (generate_series(1, 1000) * 0.001 + 7.3);
analyze test_range_numeric partition (range_numeric_p1);
analyze test_range_numeric partition (range_numeric_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_numeric_p1', 'range_numeric_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_numeric_p1', 'range_numeric_p2')) order by starelid, staattnum;

drop table if exists test_range_float4;
create table test_range_float4 (c1 float4)
partition by range (c1) (
    partition range_float4_p1 values less than (7.1),
    partition range_float4_p2 values less than (8.4)
);
insert into test_range_float4 select (generate_series(1, 1000) * 0.001 + 6.3);
insert into test_range_float4 select (generate_series(1, 1000) * 0.001 + 7.3);
analyze test_range_float4 partition (range_float4_p1);
analyze test_range_float4 partition (range_float4_p2);

select relname, relpages, reltuples from pg_partition where relname in ('range_float4_p1', 'range_float4_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_float4_p1', 'range_float4_p2')) order by starelid, staattnum;

drop table if exists test_range_float8;
create table test_range_float8 (c1 float8)
partition by range (c1) (
    partition range_float8_p1 values less than (7.1),
    partition range_float8_p2 values less than (8.4)
);
insert into test_range_float8 select (generate_series(1, 1000) * 0.001 + 6.3);  
insert into test_range_float8 select (generate_series(1, 1000) * 0.001 + 7.3);
analyze test_range_float8 partition (range_float8_p1);
analyze test_range_float8 partition (range_float8_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_float8_p1', 'range_float8_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_float8_p1', 'range_float8_p2')) order by starelid, staattnum;

drop table if exists test_range_char;
create table test_range_char (c1 char, c2 int)
partition by range (c1) (
    partition range_char_p1 values less than ('B'),
    partition range_char_p2 values less than ('F')
);
insert into test_range_char select 'A', generate_series(1, 1000);
insert into test_range_char select 'C', generate_series(1, 1000);
analyze test_range_char partition (range_char_p1);
analyze test_range_char partition (range_char_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_char_p1', 'range_char_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_char_p1', 'range_char_p2')) order by starelid, staattnum;

drop table if exists test_range_varchar;
create table test_range_varchar (c1 varchar, c2 int)
partition by range (c1) (
    partition range_varchar_p1 values less than ('B'),
    partition range_varchar_p2 values less than ('F')
);
insert into test_range_varchar select 'A', generate_series(1, 1000);
insert into test_range_varchar select 'C', generate_series(1, 1000);
analyze test_range_varchar partition (range_varchar_p1);
analyze test_range_varchar partition (range_varchar_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_varchar_p1', 'range_varchar_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_varchar_p1', 'range_varchar_p2')) order by starelid, staattnum;

drop table if exists test_range_varchar_n;
create table test_range_varchar_n (c1 varchar(10), c2 int)
partition by range (c1) (
    partition range_varchar_n_p1 values less than ('B'),
    partition range_varchar_n_p2 values less than ('F')
);
insert into test_range_varchar_n select 'A', generate_series(1, 1000);
insert into test_range_varchar_n select 'C', generate_series(1, 1000);
analyze test_range_varchar_n partition (range_varchar_n_p1);
analyze test_range_varchar_n partition (range_varchar_n_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_varchar_n_p1', 'range_varchar_n_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_varchar_n_p1', 'range_varchar_n_p2')) order by starelid, staattnum;

drop table if exists test_range_char_n;
create table test_range_char_n (c1 char(10), c2 int)
partition by range (c1) (
    partition range_char_n_p1 values less than ('B'),
    partition range_char_n_p2 values less than ('F')
);
insert into test_range_char_n select 'A', generate_series(1, 1000);
insert into test_range_char_n select 'C', generate_series(1, 1000);
analyze test_range_char_n partition (range_char_n_p1);
analyze test_range_char_n partition (range_char_n_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_char_n_p1', 'range_char_n_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_char_n_p1', 'range_char_n_p2')) order by starelid, staattnum;

drop table if exists test_range_character_n;
create table test_range_character_n (c1 character(10), c2 int)
partition by range (c1) (
    partition range_character_n_p1 values less than ('B'),
    partition range_character_n_p2 values less than ('F')
);
insert into test_range_character_n select 'A', generate_series(1, 1000);
insert into test_range_character_n select 'C', generate_series(1, 1000);
analyze test_range_character_n partition (range_character_n_p1);
analyze test_range_character_n partition (range_character_n_p2);

drop table if exists test_range_text;
create table test_range_text (c1 text, c2 int)
partition by range (c1) (
    partition range_text_p1 values less than ('B'),
    partition range_text_p2 values less than ('F')
);
insert into test_range_text select 'A', generate_series(1, 1000);
insert into test_range_text select 'C', generate_series(1, 1000);
analyze test_range_text partition (range_text_p1);
analyze test_range_text partition (range_text_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_text_p1', 'range_text_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_text_p1', 'range_text_p2')) order by starelid, staattnum;

drop table if exists test_range_nvarchar2;
create table test_range_nvarchar2 (c1 nvarchar2, c2 int)
partition by range (c1) (
    partition range_nvarchar2_p1 values less than ('B'),
    partition range_nvarchar2_p2 values less than ('F')
);
insert into test_range_nvarchar2 select 'A', generate_series(1, 1000);
insert into test_range_nvarchar2 select 'C', generate_series(1, 1000);
analyze test_range_nvarchar2 partition (range_nvarchar2_p1);
analyze test_range_nvarchar2 partition (range_nvarchar2_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_nvarchar2_p1', 'range_nvarchar2_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_nvarchar2_p1', 'range_nvarchar2_p2')) order by starelid, staattnum;

drop table if exists test_range_date;
create table test_range_date (c1 date, c2 int)
partition by range (c1) (
    partition range_date_p1 values less than ('2020-11-11'),
    partition range_date_p2 values less than ('2020-12-12')
);
insert into test_range_date select '2020-11-01', generate_series(1, 1000);
insert into test_range_date select '2020-12-01', generate_series(1, 1000);
analyze test_range_date partition (range_date_p1);
analyze test_range_date partition (range_date_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_date_p1', 'range_date_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_date_p1', 'range_date_p2')) order by starelid, staattnum;

drop table if exists test_range_timestamp;
create table test_range_timestamp (c1 timestamp, c2 int)
partition by range (c1) (
    partition range_timestamp_p1 values less than ('2020-11-11'),
    partition range_timestamp_p2 values less than ('2020-12-12')
);
insert into test_range_timestamp select '2020-11-01', generate_series(1, 1000);
insert into test_range_timestamp select '2020-12-01', generate_series(1, 1000);
analyze test_range_timestamp partition (range_timestamp_p1);
analyze test_range_timestamp partition (range_timestamp_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_timestamp_p1', 'range_timestamp_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_timestamp_p1', 'range_timestamp_p2')) order by starelid, staattnum;


drop table if exists test_range_timestamptz;
create table test_range_timestamptz (c1 timestamptz, c2 int)
partition by range (c1) (
    partition range_timestamptz_p1 values less than ('2020-11-11'),
    partition range_timestamptz_p2 values less than ('2020-12-12')
);
insert into test_range_timestamptz select '2020-11-01', generate_series(1, 1000);
insert into test_range_timestamptz select '2020-12-01', generate_series(1, 1000);
analyze test_range_timestamptz partition (range_timestamptz_p1);
analyze test_range_timestamptz partition (range_timestamptz_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_timestamptz_p1', 'range_timestamptz_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_timestamptz_p1', 'range_timestamptz_p2')) order by starelid, staattnum;

drop table if exists test_range_name;
create table test_range_name (c1 name, c2 int)
partition by range (c1) (
    partition range_name_p1 values less than ('BCD'),
    partition range_name_p2 values less than ('FGH')
);
insert into test_range_name select 'ABC', generate_series(1, 1000);
insert into test_range_name select 'DEF', generate_series(1, 1000);
analyze test_range_name partition (range_name_p1);
analyze test_range_name partition (range_name_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_name_p1', 'range_name_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_name_p1', 'range_name_p2')) order by starelid, staattnum;


drop table if exists test_range_bpchar;
create table test_range_bpchar (c1 bpchar, c2 int)
partition by range (c1) (
    partition range_bpchar_p1 values less than ('B'),
    partition range_bpchar_p2 values less than ('F')
);
insert into test_range_bpchar select 'A', generate_series(1, 1000);
insert into test_range_bpchar select 'C', generate_series(1, 1000);
analyze test_range_bpchar partition (range_bpchar_p1);
analyze test_range_bpchar partition (range_bpchar_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_bpchar_p1', 'range_bpchar_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_bpchar_p1', 'range_bpchar_p2')) order by starelid, staattnum;

-- did not support
drop table if exists test_range_bool;
create table test_range_bool (c1 bool, c2 int)
partition by range (c1) (
    partition range_bool_p1 values less than (true),
    partition range_bool_p2 values less than (false)
);
insert into test_range_bool select true, generate_series(1, 1000);  
insert into test_range_bool select false, generate_series(1, 1000);
analyze test_range_bool partition (range_bool_p1);
analyze test_range_bool partition (range_bool_p2);
select relname, relpages, reltuples from pg_partition where relname in ('range_bool_p1', 'range_bool_p2') order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in
(select oid from pg_partition where relname in ('range_bool_p1', 'range_bool_p2')) order by starelid, staattnum;

-- clean up
drop table test_range_int2;
drop table test_range_int4;
drop table test_range_int8;
drop table test_range_numeric;
drop table test_range_float4;
drop table test_range_float8;
drop table test_range_char;
drop table test_range_varchar;
drop table test_range_varchar_n;
drop table test_range_char_n;
drop table test_range_character_n;
drop table test_range_text;
drop table test_range_nvarchar2;
drop table test_range_date;
drop table test_range_timestamp;
drop table test_range_timestamptz;
drop table test_range_name;
drop table test_range_bpchar;

-- combined partition table with subpartition
drop table if exists test_combined_two_level;
create table test_combined_two_level (c1 int, c2 int)
partition by range (c1) subpartition by list (c2) (
    partition range_p1 values less than (100) (
        subpartition list_p1_1 values (1),
        subpartition list_p1_2 values (2),
        subpartition list_p1_3 values (3)
    ),
    partition range_p2 values less than (200) (
        subpartition list_p2_1 values (1),
        subpartition list_p2_2 values (2),
        subpartition list_p2_3 values (3)
    ),
    partition range_p3 values less than (300) (
        subpartition list_p3_1 values (1),
        subpartition list_p3_2 values (2),
        subpartition list_p3_3 values (3)
    )
);

insert into test_combined_two_level select generate_series(1, 99), 1;
insert into test_combined_two_level select generate_series(1, 99), 2;
insert into test_combined_two_level select generate_series(1, 99), 3;
insert into test_combined_two_level select generate_series(1, 99), 1;
insert into test_combined_two_level select generate_series(1, 99), 2;
insert into test_combined_two_level select generate_series(1, 99), 3;
insert into test_combined_two_level select generate_series(1, 99), 1;
insert into test_combined_two_level select generate_series(1, 99), 2;
insert into test_combined_two_level select generate_series(1, 99), 3;
insert into test_combined_two_level select generate_series(101, 199), 1;
insert into test_combined_two_level select generate_series(101, 199), 2;
insert into test_combined_two_level select generate_series(101, 199), 3;
insert into test_combined_two_level select generate_series(101, 199), 1;
insert into test_combined_two_level select generate_series(101, 199), 2;
insert into test_combined_two_level select generate_series(101, 199), 3;
insert into test_combined_two_level select generate_series(101, 199), 1;
insert into test_combined_two_level select generate_series(101, 199), 2;
insert into test_combined_two_level select generate_series(101, 199), 3;

analyze test_combined_two_level;

select relname, relpages, reltuples from pg_class where relname = 'test_combined_two_level';
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in 
(select oid from pg_partition where relname in ('range_p1', 'range_p2', 'range_p3')) order by starelid, staattnum;

-- analyze partitions
analyze test_combined_two_level partition (range_p1);
analyze test_combined_two_level partition (range_p2);
analyze test_combined_two_level partition (range_p3);

select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in 
(select oid from pg_partition where relname in ('range_p1', 'range_p2', 'range_p3')) order by starelid, staattnum;

-- analyze subpartitions
explain (costs off) select * from test_combined_two_level subpartition (list_p1_1);

analyze test_combined_two_level subpartition (list_p1_1);
explain (costs off) select * from test_combined_two_level subpartition (list_p1_2);

-- clean up
drop table test_combined_two_level;

-- hash partition table
drop table if exists hash_part;
create table hash_part(id int, name varchar)
partition by hash (id) partitions 4;
insert into hash_part select generate_series(1, 5000), 'apppppache';

select relname, relpages, reltuples from pg_class where relname = 'hash_part' order by relname;
select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in 
(select oid from pg_partition where relname = 'hash_part_p1') order by starelid, staattnum;

-- analyze partitions
analyze hash_part partition (p0);
analyze hash_part partition (p1);
analyze hash_part partition (p2);
analyze hash_part partition (p3);

select stadistinct, stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5 from pg_statistic where starelkind = 'p' and starelid in 
(select oid from pg_partition where relname in ('p0', 'p1', 'p2', 'p3')) order by starelid, staattnum;

-- clean up
drop table hash_part;

-- multi-columns
drop table if exists test_multi_columns_part;
create table test_multi_columns_part (c1 int, c2 varchar)
partition by range (c1) (
    partition range_p1 values less than (1000),
    partition range_p2 values less than (2000),
    partition range_p3 values less than (MAXVALUE)
);

insert into test_multi_columns_part select generate_series(1, 500), 'BOB';
insert into test_multi_columns_part select generate_series(500, 1000), 'TOM';
insert into test_multi_columns_part select generate_series(500, 1000), 'CAT';
insert into test_multi_columns_part select generate_series(1000, 2000), 'TOM';
insert into test_multi_columns_part select generate_series(2000, 3000), 'TOM';
insert into test_multi_columns_part select generate_series(2000, 3000), 'JUICE';

set default_statistics_target = -100;
analyze test_multi_columns_part partition (range_p1);
analyze test_multi_columns_part partition (range_p2);
analyze test_multi_columns_part partition (range_p3);
analyze test_multi_columns_part((c1, c2));
analyze test_multi_columns_part(c1, c2) partition (range_p3);

explain (costs off) select * from test_multi_columns_part partition (range_p3) where c2 = 'TOM' and c1 > 2500;
explain (costs off) select * from test_multi_columns_part partition (range_p3) where c2 = 'JUICE' and c1 < 2500;
select count(*) from pg_statistic_ext where starelid = 'test_multi_columns_part'::regclass;

-- partition index
create index multi_columns_part_index on test_multi_columns_part(c1) local;
analyze test_multi_columns_part partition (range_p3);

drop table if exists test_vacuum_analyze;
create table test_vacuum_analyze
(
    month_code varchar2(30) not null,
    dept_code  varchar2(30) not null,
    user_no    varchar2(30) not null,
    sales_amt  int
)
partition by range (month_code) subpartition by list (dept_code)
(
    partition p1 values less than ('207903') (
        subpartition list_p1_1 values ('1'),
        subpartition list_p1_2 values ('2')
    ),
    partition p2 values less than ('207910') (
        subpartition list_p2_1 values ('1'),
        subpartition list_p2_2 values ('2')
    )
);

create index idx_month_code_local on test_vacuum_analyze(month_code) local;
create index idx_dept_code_global on test_vacuum_analyze(dept_code) global;
create index idx_user_no_global on test_vacuum_analyze(user_no) global;

insert into test_vacuum_analyze values ('207902', '1', '1', 100);
insert into test_vacuum_analyze values ('207902', '2', '1', 100);
insert into test_vacuum_analyze values ('207902', '1', '1', 100);
insert into test_vacuum_analyze values ('207903', '2', '2', 100);
insert into test_vacuum_analyze values ('207903', '1', '1', 100);
insert into test_vacuum_analyze values ('207903', '2', '1', 100);

analyze test_vacuum_analyze;
analyze test_vacuum_analyze partition (p1);
vacuum test_vacuum_analyze;
vacuum test_vacuum_analyze partition (p1);
vacuum analyze test_vacuum_analyze; 
vacuum analyze test_vacuum_analyze partition (p1);
vacuum full test_vacuum_analyze partition (p1);
analyze test_vacuum_analyze subpartition (list_p1_1);
vacuum test_vacuum_analyze subpartition (list_p1_1);
vacuum analyze test_vacuum_analyze subpartition (list_p1_1);

-- clean up
drop table test_vacuum_analyze;

drop table if exists test_index_ht;
create table test_index_ht(c1 int, c2 int, c3 int)
partition by hash (c1)
(
    partition p1,
    partition p2
);
insert into test_index_ht select generate_series(3, 6);
create index test_exchange_index_lt_ga on test_index_ht(c1) local;
analyze test_index_ht;
analyze test_index_ht partition (p1);

-- clean up
drop table test_index_ht;

-- index expr
drop table if exists test_index_expr;
create table test_index_expr(c1 int, c2 varchar, c3 varchar)
partition by range (c1)
(
    partition p1 values less than (1000),
    partition p2 values less than (2000),
    partition p3 values less than (MAXVALUE)
);
insert into test_index_expr values (generate_series(1, 1000), 'A', 'B');
insert into test_index_expr values (generate_series(1001, 2000), 'B', 'B');
insert into test_index_expr values (generate_series(2001, 3000), 'C', 'B');

create index test_expr_index on test_index_expr(lower(c2)) local;
analyze test_index_expr;
analyze test_index_expr partition (p1);

explain (costs off) select * from test_index_expr partition (p1) where lower(c2) = 'a';

-- clean up
drop table test_index_expr;
