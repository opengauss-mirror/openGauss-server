--I1.创建表，用于测试不同类型 skew_statistic_data_type*.sql
--S1.创建倾斜表
create schema skew_join;
set current_schema to skew_join;
create table skew_type(
    st int,
    st_int1 int1,
    st_int2 int2,
    st_int4 int,
    st_int8 INT8,
    st_num numeric(10, 5),
    st_char char(10),
    st_varchar varchar(25),
    st_text text,
    st_date date,
    st_time time,
    st_timez time with time zone,
    st_timestamp timestamp,
    st_timestampz timestamp with time zone,
    st_smalldatetime smalldatetime,
    st_interval interval day to second
    );

--S2.插入倾斜数据
insert into skew_type values(generate_series(1, 100), 10, 1000, 100000, 100000000, 4321.1234, 'abc', 'abcdeabcdeabcde', 'Tomorrow is always fresh, with no mistakes in it yet.', '12-10-2010', '9:10:10', '9:10:10 pst', '2010-12-12 9:10:10', '2010-12-12 9:10:10 pst', '2003-04-12 04:05:', INTERVAL '3' DAY);

--S3.创建非倾斜表
create table unskew_type(
    ut int,
    ut_int1 int1,
    ut_int2 int2,
    ut_int4 int,
    ut_int8 INT8,
    ut_num numeric(10, 5),
    ut_char char(10),
    ut_varchar varchar(25),
    ut_text text,
    ut_date date,
    ut_time time,
    ut_timez time with time zone,
    ut_timestamp timestamp,
    ut_timestampz timestamp with time zone,
    ut_smalldatetime smalldatetime,
    ut_interval interval day to second
    );

--S4.插入非倾斜数据
insert into unskew_type values(1, 10, 1000, 100000, 100000000, 4321.1234, 'abc', 'abcdeabcdeabcde', 'Tomorrow is always fresh, with no mistakes in it yet.', '12-10-2010', '9:10:10', '9:10:10 pst', '2010-12-12 9:10:10', '2010-12-12 9:10:10 pst', '2003-04-12 04:05:', INTERVAL '3' DAY);
insert into unskew_type values(2, 20, 2000, 200000, 200000000, 4563.1234, 'esdfgs', 'vasdfavacasdfasdf', 'is always fresh, with no mistakes in it yet.', '12-10-2011', '9:11:10', '9:11:10 pst', '2010-12-12 9:10:11', '2010-12-12 9:11:10 pst', '2003-04-13 04:05:', INTERVAL '4' DAY);
insert into unskew_type values(3, 30, 3000, 300000, 300000000, 844.1234, 'sdfgs', 'asdcadfasdcasd', 'Tomorrow always fresh, with no mistakes in it yet.', '12-10-2012', '9:12:10', '9:12:10 pst', '2010-12-12 9:10:12', '2010-12-12 9:12:10 pst', '2003-04-14 04:05:', INTERVAL '5' DAY);
insert into unskew_type values(4, 40, 4000, 400000, 400000000, 9687.1234, 'wryt', 'hjretjyr', 'Tomorrow is fresh, with no mistakes in it yet.', '12-10-2013', '9:13:10', '9:13:10 pst', '2010-12-12 9:10:13', '2010-12-12 9:13:10 pst', '2003-04-15 04:05:', INTERVAL '6' DAY);
insert into unskew_type values(5, 50, 5000, 500000, 500000000, 23465.1234, 'dssg', 'casdcqa', 'Tomorrow is always , with no mistakes in it yet.', '12-10-2014', '9:14:10', '9:14:10 pst', '2010-12-12 9:10:14', '2010-12-12 9:14:10 pst', '2003-04-16 04:05:', INTERVAL '7' DAY);
insert into unskew_type values(6, 60, 6000, 600000, 600000000, 453.1234, 'svbs', 'asdffaerw', 'Tomorrow is always fresh, no mistakes in it yet.', '12-10-2015', '9:15:10', '9:15:10 pst', '2010-12-12 9:10:15', '2010-12-12 9:15:10 pst', '2003-04-17 04:05:', INTERVAL '8' DAY);
insert into unskew_type values(7, 70, 7000, 700000, 700000000, 12.1234, 'gbsfdgw', 'crfhaac', 'Tomorrow is always fresh, with mistakes in it yet.', '12-10-2016', '9:16:10', '9:16:10 pst', '2010-12-12 9:10:16', '2010-12-12 9:16:10 pst', '2003-04-18 04:05:', INTERVAL '9' DAY);
insert into unskew_type values(8, 80, 8000, 800000, 800000000, 64356.1234, 'qgb', 'asdcqerfqc', 'Tomorrow is always fresh, with no in it yet.', '12-10-2017', '9:17:10', '9:17:10 pst', '2010-12-12 9:10:17', '2010-12-12 9:17:10 pst', '2003-04-19 04:05:', INTERVAL '10' DAY);
insert into unskew_type values(9, 90, 9000, 900000, 900000000, 808.1234, 'assdfa', 'cadfqvq', 'Tomorrow is always fresh, with no mistakes it yet.', '12-10-2018', '9:18:10', '9:18:10 pst', '2010-12-12 9:10:18', '2010-12-12 9:18:10 pst', '2003-04-20 04:05:', INTERVAL '11' DAY);
insert into unskew_type values(10, 100, 10000, 1000000, 1000000000, 68.1234, 'oio', 'vqefasdfa', 'Tomorrow is always fresh, with no mistakes in yet.', '12-10-2019', '9:19:10', '9:19:10 pst', '2010-12-12 9:10:20', '2010-12-12 9:19:10 pst', '2003-04-21 04:05:', INTERVAL '12' DAY);

--S5.analyze
analyze skew_type;
analyze unskew_type;

--I2.创建表，用于测试不同的单列倾斜
--S1.创建倾斜表
create table skew_scol(a int, b int, c int);

--S2.插入倾斜数据
insert into skew_scol values(generate_series(1, 200), 1);
insert into skew_scol values(generate_series(1, 100), generate_series(1, 100), generate_series(1, 100));

--S3.创建倾斜表
create table skew_scol1(a int, b int, c int);

--S4.插入倾斜数据
insert into skew_scol1 values(generate_series(1, 100), 1, 0);
insert into skew_scol1 values(generate_series(1, 100), generate_series(1, 100), generate_series(1, 100));

--S5.创建非倾斜表
create table test_scol(a int, b int, c int);

--S6.插入非倾斜数据
insert into test_scol values(generate_series(1, 100), generate_series(1, 100), generate_series(1, 100));

--S7.analyze
analyze skew_scol;
analyze skew_scol1;
analyze test_scol;

--I4.创建表，用于测试outer join 补空导致的倾斜
--S1.创建表
create table r(x int, a int);
create table s(y int, b int, c int);
create table t(z int, d int);

--S2.插入数据
insert into r values(generate_series(1, 100), generate_series(1, 100));
insert into s values(generate_series(1, 100), generate_series(100, 199), generate_series(100, 199));
insert into t values(generate_series(1, 100), generate_series(199, 298));

--S3.收集统计信息
analyze r;
analyze s;
analyze t;
