-- range partition
-- type: int
drop table if exists test_int4;
create table test_int4 (c1 int, c2 int)
partition by range (c2) (
    partition test_int4_p1 values less than (10),
    partition test_int4_p2 values less than (20),
    partition test_int4_p3 values less than (30)
);

insert into test_int4 select generate_series(1, 1000), 1;
insert into test_int4 select generate_series(1, 100), 11;
insert into test_int4 select generate_series(1, 10), 21;
analyze test_int4;
explain (costs off) select * from test_int4 partition (test_int4_p1);
explain (costs off) select * from test_int4 partition (test_int4_p2);
explain (costs off) select * from test_int4 partition (test_int4_p3);

drop table if exists test_int4_maxvalue;
create table test_int4_maxvalue (c1 int, c2 varchar)
partition by range (c1) (
    partition test_int4_maxvalue_p1 values less than (1000),
    partition test_int4_maxvalue_p2 values less than (2000),
    partition test_int4_maxvalue_p3 values less than (MAXVALUE)
);
insert into test_int4_maxvalue select generate_series(1, 5000), 'test';
analyze test_int4_maxvalue;
explain (costs off) select * from test_int4_maxvalue partition (test_int4_maxvalue_p1);
explain (costs off) select * from test_int4_maxvalue partition (test_int4_maxvalue_p2);
explain (costs off) select * from test_int4_maxvalue partition (test_int4_maxvalue_p3);

-- type: int2
drop table if exists test_int2;
create table test_int2 (c1 int, c2 int2)
partition by range (c2) (
    partition test_int2_p1 values less than (1),
    partition test_int2_p2 values less than (2),
    partition test_int2_p3 values less than (maxvalue)
);
insert into test_int2 select generate_series(1, 100), 0;
insert into test_int2 select generate_series(1, 500), 1;
insert into test_int2 select generate_series(1, 1000), 2;
analyze test_int2;
explain (costs off) select * from test_int2 partition (test_int2_p1);
explain (costs off) select * from test_int2 partition (test_int2_p2);
explain (costs off) select * from test_int2 partition (test_int2_p3);

-- type: int8
drop table if exists test_int8;
create table test_int8 (c1 int, c2 int8)
partition by range (c2) (
    partition test_int8_p1 values less than (100),
    partition test_int8_p2 values less than (200),
    partition test_int8_p3 values less than (maxvalue)
);
insert into test_int8 select generate_series(1, 1000), 1;
insert into test_int8 select generate_series(1, 100), 101;
insert into test_int8 select generate_series(1, 10), 201;
analyze test_int8;
explain (costs off) select * from test_int8 partition (test_int8_p1);
explain (costs off) select * from test_int8 partition (test_int8_p2);
explain (costs off) select * from test_int8 partition (test_int8_p3);

-- type: decimal
drop table if exists test_decimal;
create table test_decimal (c1 int, c2 decimal)
partition by range (c2) (
    partition test_decimal_p1 values less than (7.1),
    partition test_decimal_p2 values less than (8.4),
    partition test_decimal_p3 values less than (maxvalue)
);
insert into test_decimal select generate_series(1, 1000), 2.1;
insert into test_decimal select generate_series(1, 100), 7.1;
insert into test_decimal select generate_series(1, 500), 9.0;
analyze test_decimal;
explain (costs off) select * from test_decimal partition (test_decimal_p1);
explain (costs off) select * from test_decimal partition (test_decimal_p2);
explain (costs off) select * from test_decimal partition (test_decimal_p3);

-- type: numeric
drop table if exists test_numeric;
create table test_numeric (c1 int, c2 numeric)
partition by range (c2) (
    partition test_numeric_p1 values less than (7.1),
    partition test_numeric_p2 values less than (8.4),
    partition test_numeric_p3 values less than (maxvalue)
);
insert into test_numeric select generate_series(1, 1000), 2.1;
insert into test_numeric select generate_series(1, 100), 7.1;
insert into test_numeric select generate_series(1, 500), 9.0;
analyze test_numeric;
explain (costs off) select * from test_numeric partition (test_numeric_p1);
explain (costs off) select * from test_numeric partition (test_numeric_p2);
explain (costs off) select * from test_numeric partition (test_numeric_p3);

-- type: real
drop table if exists test_real;
create table test_real (c1 int, c2 real)
partition by range (c2) (
    partition test_real_p1 values less than (7.1),
    partition test_real_p2 values less than (8.4    ),
    partition test_real_p3 values less than (maxvalue)
);
insert into test_real select generate_series(1, 1000), 2.1;
insert into test_real select generate_series(1, 100), 7.1;
insert into test_real select generate_series(1, 500), 9.0;
analyze test_real;
explain (costs off) select * from test_real partition (test_real_p1);
explain (costs off) select * from test_real partition (test_real_p2);
explain (costs off) select * from test_real partition (test_real_p3);

-- type: double
drop table if exists test_double;
create table test_double (c1 int, c2 double precision) 
partition by range (c2) (
    partition test_double_p1 values less than (7.1),
    partition test_double_p2 values less than (8.4),
    partition test_double_p3 values less than (maxvalue)
);
insert into test_double select generate_series(1, 1000), 2.1;
insert into test_double select generate_series(1, 100), 7.1;
insert into test_double select generate_series(1, 500), 9.0;
analyze test_double;
explain (costs off) select * from test_double partition (test_double_p1);
explain (costs off) select * from test_double partition (test_double_p2);
explain (costs off) select * from test_double partition (test_double_p3);

-- type: char
drop table if exists test_char;
create table test_char (c1 int, c2 char)
partition by range (c2) (
    partition test_char_p1 values less than ('a'),
    partition test_char_p2 values less than ('b'),
    partition test_char_p3 values less than (maxvalue)
);
insert into test_char select generate_series(1, 1000), 'a';
insert into test_char select generate_series(1, 100), 'b';
insert into test_char select generate_series(1, 500), 'c';
analyze test_char;
explain (costs off) select * from test_char partition (test_char_p1);
explain (costs off) select * from test_char partition (test_char_p2);
explain (costs off) select * from test_char partition (test_char_p3);

-- type: char(n)
drop table if exists test_char_n;
create table test_char_n (c1 int, c2 char(10))
partition by range (c2) (
    partition test_char_n_p1 values less than ('a'),
    partition test_char_n_p2 values less than ('b'),
    partition test_char_n_p3 values less than (maxvalue)
);
insert into test_char_n select generate_series(1, 1000), 'a';
insert into test_char_n select generate_series(1, 100), 'b';
insert into test_char_n select generate_series(1, 500), 'c';
analyze test_char_n;
explain (costs off) select * from test_char_n partition (test_char_n_p1);
explain (costs off) select * from test_char_n partition (test_char_n_p2);
explain (costs off) select * from test_char_n partition (test_char_n_p3);

-- type: varchar(n)
drop table if exists test_varchar_n;
create table test_varchar_n (c1 int, c2 varchar(10))
partition by range (c2) (
    partition test_varchar_n_p1 values less than ('a'),
    partition test_varchar_n_p2 values less than ('b'),
    partition test_varchar_n_p3 values less than (maxvalue)
);
insert into test_varchar_n select generate_series(1, 1000), 'a';
insert into test_varchar_n select generate_series(1, 100), 'b';
insert into test_varchar_n select generate_series(1, 500), 'c';
analyze test_varchar_n;
explain (costs off) select * from test_varchar_n partition (test_varchar_n_p1);
explain (costs off) select * from test_varchar_n partition (test_varchar_n_p2);
explain (costs off) select * from test_varchar_n partition (test_varchar_n_p3);

-- type: varchar
drop table if exists test_varchar;
create table test_varchar (c1 int, c2 varchar)
partition by range (c2) (
    partition test_varchar_p1 values less than ('a'),
    partition test_varchar_p2 values less than ('b'),
    partition test_varchar_p3 values less than (maxvalue)
);
insert into test_varchar select generate_series(1, 1000), 'a';
insert into test_varchar select generate_series(1, 100), 'b';
insert into test_varchar select generate_series(1, 500), 'c';
analyze test_varchar;
explain (costs off) select * from test_varchar partition (test_varchar_p1);
explain (costs off) select * from test_varchar partition (test_varchar_p2);
explain (costs off) select * from test_varchar partition (test_varchar_p3);

-- type: varchar2(n)
drop table if exists test_varchar2_n;
create table test_varchar2_n (c1 int, c2 varchar2(10))
partition by range (c2) (
    partition test_varchar2_n_p1 values less than ('a'),
    partition test_varchar2_n_p2 values less than ('b'),
    partition test_varchar2_n_p3 values less than (maxvalue)
);
insert into test_varchar2_n select generate_series(1, 1000), 'a';
insert into test_varchar2_n select generate_series(1, 100), 'b';
insert into test_varchar2_n select generate_series(1, 500), 'c';
analyze test_varchar2_n;
explain (costs off) select * from test_varchar2_n partition (test_varchar2_n_p1);
explain (costs off) select * from test_varchar2_n partition (test_varchar2_n_p2);
explain (costs off) select * from test_varchar2_n partition (test_varchar2_n_p3);

-- type: nvarchar2
drop table if exists test_nvarchar2;
create table test_nvarchar2 (c1 int, c2 nvarchar2)
partition by range (c2) (
    partition test_nvarchar2_p1 values less than ('a'),
    partition test_nvarchar2_p2 values less than ('b'),
    partition test_nvarchar2_p3 values less than (maxvalue)
);
insert into test_nvarchar2 select generate_series(1, 1000), 'a';
insert into test_nvarchar2 select generate_series(1, 100), 'b';
insert into test_nvarchar2 select generate_series(1, 500), 'c';
analyze test_nvarchar2;
explain (costs off) select * from test_nvarchar2 partition (test_nvarchar2_p1);
explain (costs off) select * from test_nvarchar2 partition (test_nvarchar2_p2);
explain (costs off) select * from test_nvarchar2 partition (test_nvarchar2_p3);

-- type: text   
drop table if exists test_text;
create table test_text (c1 int, c2 text)
partition by range (c2) (
    partition test_text_p1 values less than ('a'),
    partition test_text_p2 values less than ('b'),
    partition test_text_p3 values less than (maxvalue)
);
insert into test_text select generate_series(1, 1000), 'a';
insert into test_text select generate_series(1, 100), 'b';
insert into test_text select generate_series(1, 500), 'c';
analyze test_text;
explain (costs off) select * from test_text partition (test_text_p1);
explain (costs off) select * from test_text partition (test_text_p2);
explain (costs off) select * from test_text partition (test_text_p3);

-- type: date
drop table if exists test_date;
create table test_date (c1 int, c2 date)
partition by range (c2) (
    partition test_date_p1 values less than (TO_DATE('2020-01-01', 'YYYY-MM-DD')),
    partition test_date_p2 values less than (TO_DATE('2020-02-01', 'YYYY-MM-DD')),
    partition test_date_p3 values less than (TO_DATE('2020-03-01', 'YYYY-MM-DD'))
);
insert into test_date select generate_series(1, 1000), TO_DATE('2019-11-11', 'YYYY-MM-DD');
insert into test_date select generate_series(1, 100), TO_DATE('2020-01-11', 'YYYY-MM-DD');
insert into test_date select generate_series(1, 500), TO_DATE('2020-02-11', 'YYYY-MM-DD');
analyze test_date;
explain (costs off) select * from test_date partition (test_date_p1);
explain (costs off) select * from test_date partition (test_date_p2);
explain (costs off) select * from test_date partition (test_date_p3);

drop table if exists sales_table;
create table sales_table
(
    order_no        integer     not null,
    goods_name      char(20)    not null,
    sales_date      date        not null,
    sales_volume    integer,
    sales_store     char(20)
)
partition by range (sales_date)
(
    partition sales_table_p1 values less than (TO_DATE('2020-04-01', 'YYYY-MM-DD')),
    partition sales_table_p2 values less than (TO_DATE('2020-07-01', 'YYYY-MM-DD')),
    partition sales_table_p3 values less than (TO_DATE('2020-10-01', 'YYYY-MM-DD')),
    partition sales_table_p4 values less than (MAXVALUE)
);
insert into sales_table values (generate_series(1, 100), 'jacket', '2020-01-10', 3, 'Alaska');
insert into sales_table values (generate_series(101, 1000), 'hat', '2020-05-06', 5, 'Clolorado');
insert into sales_table values (generate_series(1001, 5000), 'shirt', '2020-09-17', 7, 'Florida');
insert into sales_table values (generate_series(5001, 10000), 'coat', '2020-10-21', 9, 'Hawaii');
analyze sales_table;
explain (costs off) select * from sales_table partition (sales_table_p1);
explain (costs off) select * from sales_table partition (sales_table_p2);
explain (costs off) select * from sales_table partition (sales_table_p3);
explain (costs off) select * from sales_table partition (sales_table_p4);

-- type: timestamp without time zone
drop table if exists test_timestamp_ntz;
create table test_timestamp_ntz (c1 int, c2 timestamp without time zone)
partition by range (c2)
(
    partition test_timestamp_ntz_p1 values less than (TO_TIMESTAMP('2020-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamp_ntz_p2 values less than (TO_TIMESTAMP('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamp_ntz_p3 values less than (TO_TIMESTAMP('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamp_ntz_p4 values less than (MAXVALUE)
);
insert into test_timestamp_ntz select generate_series(1, 1000), TO_TIMESTAMP('2019-11-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
insert into test_timestamp_ntz select generate_series(1, 100), TO_TIMESTAMP('2020-01-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
insert into test_timestamp_ntz select generate_series(1, 500), TO_TIMESTAMP('2020-02-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
analyze test_timestamp_ntz;
explain (costs off) select * from test_timestamp_ntz partition (test_timestamp_ntz_p1);
explain (costs off) select * from test_timestamp_ntz partition (test_timestamp_ntz_p2);
explain (costs off) select * from test_timestamp_ntz partition (test_timestamp_ntz_p3);
explain (costs off) select * from test_timestamp_ntz partition (test_timestamp_ntz_p4);

-- type: timestamp
drop table if exists test_timestamp;
create table test_timestamp (c1 int, c2 timestamp)
partition by range (c2)
(
    partition test_timestamp_p1 values less than (TO_TIMESTAMP('2020-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamp_p2 values less than (TO_TIMESTAMP('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamp_p3 values less than (TO_TIMESTAMP('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamp_p4 values less than (MAXVALUE)
);
insert into test_timestamp select generate_series(1, 1000), TO_TIMESTAMP('2019-11-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
insert into test_timestamp select generate_series(1, 100), TO_TIMESTAMP('2020-01-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
analyze test_timestamp;
explain (costs off) select * from test_timestamp partition (test_timestamp_p1);
explain (costs off) select * from test_timestamp partition (test_timestamp_p2);
explain (costs off) select * from test_timestamp partition (test_timestamp_p3);
explain (costs off) select * from test_timestamp partition (test_timestamp_p4);

-- type: timestamptz
drop table if exists test_timestamptz;
create table test_timestamptz (c1 int, c2 timestamptz)
partition by range (c2)
(
    partition test_timestamptz_p1 values less than (TO_TIMESTAMP('2020-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamptz_p2 values less than (TO_TIMESTAMP('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamptz_p3 values less than (TO_TIMESTAMP('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')),
    partition test_timestamptz_p4 values less than (MAXVALUE)
);
insert into test_timestamptz select generate_series(1, 1000), TO_TIMESTAMP('2019-11-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
insert into test_timestamptz select generate_series(1, 100), TO_TIMESTAMP('2020-01-11 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
analyze test_timestamptz;
explain (costs off) select * from test_timestamptz partition (test_timestamptz_p1);
explain (costs off) select * from test_timestamptz partition (test_timestamptz_p2);
explain (costs off) select * from test_timestamptz partition (test_timestamptz_p3);
explain (costs off) select * from test_timestamptz partition (test_timestamptz_p4);

-- type: name
drop table if exists test_name;
create table test_name (c1 int, c2 name)
partition by range (c2)
(
    partition test_name_p1 values less than ('a'),
    partition test_name_p2 values less than ('b'),
    partition test_name_p3 values less than (maxvalue)
);
insert into test_name select generate_series(1, 1000), 'a';
insert into test_name select generate_series(1, 100), 'b';
insert into test_name select generate_series(1, 500), 'c';
analyze test_name;
explain (costs off) select * from test_name partition (test_name_p1);
explain (costs off) select * from test_name partition (test_name_p2);
explain (costs off) select * from test_name partition (test_name_p3);

-- type: bpchar
drop table if exists test_bpchar;
create table test_bpchar (c1 int, c2 bpchar)
partition by range (c2)
(
    partition test_bpchar_p1 values less than ('a'),
    partition test_bpchar_p2 values less than ('b'),
    partition test_bpchar_p3 values less than (maxvalue)
);
insert into test_bpchar select generate_series(1, 1000), 'a';
insert into test_bpchar select generate_series(1, 100), 'b';
insert into test_bpchar select generate_series(1, 500), 'c';
analyze test_bpchar;
explain (costs off) select * from test_bpchar partition (test_bpchar_p1);
explain (costs off) select * from test_bpchar partition (test_bpchar_p2);
explain (costs off) select * from test_bpchar partition (test_bpchar_p3);

-- [start, end]
create table graderecord
(
    number  integer,
    name    char(20),
    class   char(20),
    grade   integer
)
partition by range (grade)
(
    partition pass START(60) END(90),
    partition excellent START(90) END(MAXVALUE)
);
insert into graderecord values (120321, 'zhangsan', 'class1', 85);
insert into graderecord values (120322, 'lisi', 'class2', 95);
insert into graderecord values (120323, 'wangwu', 'class3', 75);
insert into graderecord values (120324, 'zhaoliu', 'class2', 65);
insert into graderecord values (120325, 'zhouqi', 'class3', 98);
insert into graderecord values (120326, 'zhaowei', 'class2', 60);
insert into graderecord values (120327, 'liuyumei', 'class3', 100);
analyze graderecord;
explain (costs off) select * from graderecord partition (pass_0);
explain (costs off) select * from graderecord partition (pass_1);
explain (costs off) select * from graderecord partition (excellent);


-- list partition
-- type: int
drop table if exists test_list_int;
create table test_int_list (c1 int, c2 int)
partition by list (c2)
(
    partition test_int_list_p1 values (1),
    partition test_int_list_p2 values (2, 4, 6)
);
insert into test_int_list values (generate_series(1, 1000), 1);
insert into test_int_list values (generate_series(1, 100), 2);
analyze test_int_list;
explain (costs off) select * from test_int_list partition (test_int_list_p1);
explain (costs off) select * from test_int_list partition (test_int_list_p2);

-- type: smallint
drop table if exists test_list_smallint;
create table test_smallint_list (c1 int, c2 smallint)
partition by list (c2)
(
    partition test_smallint_list_p1 values (1),
    partition test_smallint_list_p2 values (2, 4, 6)
);
insert into test_smallint_list values (generate_series(1, 1000), 1);
insert into test_smallint_list values (generate_series(1, 100), 2);
analyze test_smallint_list;
explain (costs off) select * from test_smallint_list partition (test_smallint_list_p1);
explain (costs off) select * from test_smallint_list partition (test_smallint_list_p2);

-- type: bigint
drop table if exists test_list_bigint;
create table test_bigint_list (c1 int, c2 bigint)
partition by list (c2)
(
    partition test_bigint_list_p1 values (100000, 200000),
    partition test_bigint_list_p2 values (300000, 400000, 600000)
);
insert into test_bigint_list values (generate_series(1, 1000), 100000);
insert into test_bigint_list values (generate_series(1, 100), 300000);
analyze test_bigint_list;
explain (costs off) select * from test_bigint_list partition (test_bigint_list_p1);
explain (costs off) select * from test_bigint_list partition (test_bigint_list_p2);

-- type: char
drop table if exists test_list_char;
create table test_char_list (c1 int, c2 char)
partition by list (c2)
(
    partition test_char_list_p1 values ('a'),
    partition test_char_list_p2 values ('b', 'd', 'f')
);
insert into test_char_list values (generate_series(1, 1000), 'a');
insert into test_char_list values (generate_series(1, 100), 'b');
analyze test_char_list;
explain (costs off) select * from test_char_list partition (test_char_list_p1);
explain (costs off) select * from test_char_list partition (test_char_list_p2);

-- type: varchar
drop table if exists test_list_varchar;
create table test_varchar_list (c1 int, c2 varchar)
partition by list (c2)
(
    partition test_varchar_list_p1 values ('a'),
    partition test_varchar_list_p2 values ('b', 'd', 'f')
);
insert into test_varchar_list values (generate_series(1, 1000), 'a');
insert into test_varchar_list values (generate_series(1, 100), 'b');
analyze test_varchar_list;
explain (costs off) select * from test_varchar_list partition (test_varchar_list_p1);
explain (costs off) select * from test_varchar_list partition (test_varchar_list_p2);

-- type: nvarchar2
drop table if exists test_list_nvarchar2;
create table test_nvarchar2_list (c1 int, c2 nvarchar2)
partition by list (c2)
(
    partition test_nvarchar2_list_p1 values ('a'),
    partition test_nvarchar2_list_p2 values ('b', 'd', 'f')
);
insert into test_nvarchar2_list values (generate_series(1, 1000), 'a');
insert into test_nvarchar2_list values (generate_series(1, 100), 'b');
analyze test_nvarchar2_list;
explain (costs off) select * from test_nvarchar2_list partition (test_nvarchar2_list_p1);
explain (costs off) select * from test_nvarchar2_list partition (test_nvarchar2_list_p2);

-- type: timestamp without time zone    
drop table if exists test_list_timestamp_ntz;
create table test_timestamp_ntz_list (c1 int, c2 timestamp without time zone)
partition by list (c2)
(
    partition test_timestamp_ntz_list_p1 values ('2020-01-01 00:00:00'),
    partition test_timestamp_ntz_list_p2 values ('2020-02-01 00:00:00', '2020-03-01 00:00:00')
);
insert into test_timestamp_ntz_list values (generate_series(1, 1000), '2020-01-01 00:00:00');
insert into test_timestamp_ntz_list values (generate_series(1, 100), '2020-02-01 00:00:00');
analyze test_timestamp_ntz_list;
explain (costs off) select * from test_timestamp_ntz_list partition (test_timestamp_ntz_list_p1);
explain (costs off) select * from test_timestamp_ntz_list partition (test_timestamp_ntz_list_p2);

-- type: timestamp
drop table if exists test_list_timestamp;
create table test_timestamp_list (c1 int, c2 timestamp)
partition by list (c2)
(
    partition test_timestamp_list_p1 values ('2020-01-01 00:00:00'),
    partition test_timestamp_list_p2 values ('2020-02-01 00:00:00', '2020-03-01 00:00:00')
);
insert into test_timestamp_list values (generate_series(1, 1000), '2020-01-01 00:00:00');
insert into test_timestamp_list values (generate_series(1, 100), '2020-02-01 00:00:00');
analyze test_timestamp_list;
explain (costs off) select * from test_timestamp_list partition (test_timestamp_list_p1);
explain (costs off) select * from test_timestamp_list partition (test_timestamp_list_p2);

-- type: date
drop table if exists test_list_date;
create table test_date_list (c1 int, c2 date)
partition by list (c2)
(
    partition test_date_list_p1 values ('2020-01-01'),
    partition test_date_list_p2 values ('2020-02-01', '2020-03-01')
);
insert into test_date_list values (generate_series(1, 1000), '2020-01-01');
insert into test_date_list values (generate_series(1, 100), '2020-02-01');
analyze test_date_list;
explain (costs off) select * from test_date_list partition (test_date_list_p1);
explain (costs off) select * from test_date_list partition (test_date_list_p2);


-- interval partition
-- month
drop table if exists test_interval_month;
create table test_interval_month (c1 int, c2 date)
partition by range (c2) interval ('1 month')
(
    partition test_interval_month_starter values less than ('2020-01-01'),
    partition test_interval_month_later values less than ('2020-01-10')
);
insert into test_interval_month values (generate_series(1, 100), '2020-01-08');
insert into test_interval_month values (generate_series(101, 200), '2020-04-06');
insert into test_interval_month values (generate_series(201, 300), '2020-11-17');
insert into test_interval_month values (generate_series(301, 400), '2020-10-21');
analyze test_interval_month;
explain (costs off) select * from test_interval_month partition (test_interval_month_starter);
explain (costs off) select * from test_interval_month partition (test_interval_month_later);
explain (costs off) select * from test_interval_month partition (sys_p1);
explain (costs off) select * from test_interval_month partition (sys_p2);

-- interval partition
-- day
drop table if exists test_interval_day;
create table test_interval_day (c1 int, c2 date)
partition by range (c2) interval ('1 day')
(
    partition test_interval_day_starter values less than ('2020-01-01'),
    partition test_interval_day_later values less than ('2020-01-05')
);
insert into test_interval_day values (generate_series(1, 100), '2020-01-01');
insert into test_interval_day values (generate_series(101, 200), '2020-01-05');
insert into test_interval_day values (generate_series(201, 300), '2020-01-06');
analyze test_interval_day;
explain (costs off) select * from test_interval_day partition (test_interval_day_starter);
explain (costs off) select * from test_interval_day partition (test_interval_day_later);
explain (costs off) select * from test_interval_day partition (sys_p1);

-- subpartition
drop table if exists test_int4_range_list;
create table test_int4_range_list (c1 int, c2 int)
partition by range (c1) subpartition by list (c2)
(
    partition test_int4_range_list_p1 values less than (100) (
        subpartition test_int4_range_list_p1_1 values (1),
        subpartition test_int4_range_list_p1_2 values (2),
        subpartition test_int4_range_list_p1_3 values (3)
    ),
    partition test_int4_range_list_p2 values less than (200) (
        subpartition test_int4_range_list_p2_1 values (1),
        subpartition test_int4_range_list_p2_2 values (2),
        subpartition test_int4_range_list_p2_3 values (3)
    ),
    partition test_int4_range_list_p3 values less than (300) (
        subpartition test_int4_range_list_p3_1 values (1),
        subpartition test_int4_range_list_p3_2 values (2),
        subpartition test_int4_range_list_p3_3 values (3)
    )
);
insert into test_int4_range_list values (generate_series(1, 99), 1);
insert into test_int4_range_list values (generate_series(1, 99), 2);
insert into test_int4_range_list values (generate_series(1, 99), 3);
insert into test_int4_range_list values (generate_series(1, 99), 1);
insert into test_int4_range_list values (generate_series(1, 99), 2);
insert into test_int4_range_list values (generate_series(1, 99), 3);
insert into test_int4_range_list values (generate_series(1, 99), 1);
insert into test_int4_range_list values (generate_series(1, 99), 2);
insert into test_int4_range_list values (generate_series(1, 99), 3);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
insert into test_int4_range_list values (generate_series(101, 199), 1);
analyze test_int4_range_list;
explain (costs off) select * from test_int4_range_list partition (test_int4_range_list_p1);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p1_1);    
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p1_2);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p1_3);
explain (costs off) select * from test_int4_range_list partition (test_int4_range_list_p2);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p2_1);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p2_2);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p2_3);
explain (costs off) select * from test_int4_range_list partition (test_int4_range_list_p3);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p3_1);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p3_2);
explain (costs off) select * from test_int4_range_list subpartition (test_int4_range_list_p3_3);

-- hash分区，不支持，按原计划
drop table if exists test_hash_partition;
create table test_hash_partition (c1 int, c2 int)
partition by hash (c1)
(
    partition test_hash_partition_p1,
    partition test_hash_partition_p2
);
insert into test_hash_partition values (generate_series(1, 1000), 1);
analyze test_hash_partition;
explain (costs off) select * from test_hash_partition partition (test_hash_partition_p1);
explain (costs off) select * from test_hash_partition partition (test_hash_partition_p2);

-- multi partition keys
drop table if exists test_multi_partition_keys;
create table test_multi_partition_keys (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int)
partition by list (c1, c2, c3, c4)
(
    partition test_multi_partition_keys_p1 values ((100, 1, 1, 1), (100, 1, 2, 3), (100, 1, 2, 4)),
    partition test_multi_partition_keys_p2 values ((100, 2, 1, 1), (100, 2, 2, 3), (100, 2, 2, 4)),
    partition test_multi_partition_keys_p3 values ((200, 1, 1, 1), (200, 1, 2, 3), (200, 1, 2, 4)),
    partition test_multi_partition_keys_p4 values (DEFAULT)
);
insert into test_multi_partition_keys values (generate_series(1, 50), 1, 1, 1, 1, 1, 1);
insert into test_multi_partition_keys values (generate_series(51, 100), 1, 1, 2, 3, 1, 1);
insert into test_multi_partition_keys values (generate_series(101, 150), 2, 1, 2, 4, 1, 1);
insert into test_multi_partition_keys values (generate_series(151, 200), 1, 1, 2, 1, 1, 1);
insert into test_multi_partition_keys values (generate_series(151, 200), 1, 1, 2, 1, 1, 1);
analyze test_multi_partition_keys;
explain (costs off) select * from test_multi_partition_keys partition (test_multi_partition_keys_p1);
explain (costs off) select * from test_multi_partition_keys partition (test_multi_partition_keys_p2);
explain (costs off) select * from test_multi_partition_keys partition (test_multi_partition_keys_p3);
explain (costs off) select * from test_multi_partition_keys partition (test_multi_partition_keys_p4);

-- default
drop table if exists test_default;
create table test_default (c1 int, c2 int)
partition by list (c2)
(
    partition test_default_p1 values (100, 200),
    partition test_default_p2 values (300, 400, 500),
    partition test_default_p3 values (DEFAULT)
);
insert into test_default values (generate_series(1, 1000), 100);
insert into test_default values (generate_series(1001, 3000), 500);
insert into test_default values (generate_series(3001, 6000), 600);
analyze test_default;
explain (costs off) select * from test_default partition (test_default_p1);
explain (costs off) select * from test_default partition (test_default_p2);
explain (costs off) select * from test_default partition (test_default_p3);
explain (costs off) select * from test_default partition (test_default_p1) where c2 = 100;

drop table if exists test_equal;
create table test_equal (c1 int, c2 int, c3 int)
partition by range (c1, c2)
(
    partition test_equal_p1 values less than (100, 1),
    partition test_equal_p2 values less than (200, 1),
    partition test_equal_p3 values less than (300, 1)
);
insert into test_equal values (generate_series(1, 299), 1, 1);
analyze test_equal;
explain (costs off) select * from test_equal partition (test_equal_p1);
explain (costs off) select * from test_equal partition (test_equal_p1) where c1 = 150;

drop table if exists test_two_list_value;
create table test_two_list_value (c1 int, c2 int, c3 int)
partition by list (c2, c3)
(
    partition test_two_list_value_p1 values ((100, 200)),
    partition test_two_list_value_p2 values ((300, 400), (500, 600)),
    partition test_two_list_value_p3 values (DEFAULT)
);
insert into test_two_list_value values (generate_series(1, 100), 100, 200);
insert into test_two_list_value values (generate_series(101, 200), 300, 400);
insert into test_two_list_value values (generate_series(201, 300), 500, 600);
analyze test_two_list_value;
explain (costs off) select * from test_two_list_value partition (test_two_list_value_p1);
explain (costs off) select * from test_two_list_value partition (test_two_list_value_p2);
