-- b compatibility case
drop database if exists b;
create database b dbcompatibility 'b';

--------------------concat--------------------
-- concat case in a compatibility
\c regression
select concat('','A');
select concat(null,'A');
select concat_ws(',', 'A', null);
select concat_ws(',', 'A', '');
create table text1 (a char(10));
insert into text1 values (concat('A',''));
insert into text1 values (concat('A',null));
select * from text1 where a is null;
drop table text1;

-- concat case in b compatibility
\c b
select concat('','A');
select concat(null,'A');
select concat_ws(',', 'A', null);
select concat_ws(',', 'A', '');
create table text1 (a char(10));
insert into text1 values (concat('A',''));
insert into text1 values (concat('A',null));
select * from text1 where a is null;
drop table text1;

-----------null is not equal to ''---------
-- null case in postgresql
\c regression
create table text2 (a char(10));
insert into text2 values('');
insert into text2 values (null);
select * from text2 where a is null;
select * from text2 where a='';
select * from text2 where a is not null;
drop table text2;

-- null case in b
\c b
create table text2 (a char(10));
insert into text2 values('');
insert into text2 values (null);
select * from text2 where a is null;
select * from text2 where a='';
select * from text2 where a is not null;
drop table text2;

-- test int8 int1in int2in int4in
\c regression
select '-'::int8;
select int1in('');
select int1in('.1');
select int2in('s');
select int4in('s');

\c b
select '-'::int8;
select int1in('');
select int1in('.1');
select int2in('s');
select int4in('s');
-- test substr
select substr(9, 2) + 1;
select substr(9, 2) + 1.2;
select substr(9, 2) + '1';
select substr(9, 2) + '1.2';
select substr(9, 2) + 'a';
select substr(1.2, 1, 3) + '1.2';
select 'a' + 1;
select 'a' + 1.2;
select 'a' + '1';
select 'a' + '1.2';
select 'a' + 'b';
select cast('.1' as int);
select cast('' as int);
select cast('1.1' as int);
select cast('s' as int);

--------------- limit #,#-------------------
-- limit case in postgresql
\c regression
create table test_limit(a int);
insert into test_limit values (1),(2),(3),(4),(5);
select * from test_limit order by 1 limit 2,3;
select * from test_limit order by 1 limit 2,6;
select * from test_limit order by 1 limit 6,2;
drop table test_limit;

-- limit case in b
\c b
create table test_limit(a int);
insert into test_limit values (1),(2),(3),(4),(5);
select * from test_limit order by 1 limit 2,3;
select * from test_limit order by 1 limit 2,6;
select * from test_limit order by 1 limit 6,2;
drop table test_limit;

--------------timestampdiff-----------------
-- timestamp with time zone
-- timestamp1 > timestamp2
\c b
select timestampdiff(year, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(quarter, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(week, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(month, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(day, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(hour, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(minute, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(second, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');
select timestampdiff(microsecond, '2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');

-- timestamp2 > timestamp1
select timestampdiff(year, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(quarter, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(week, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(month, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(day, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(hour, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(minute, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(second, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');
select timestampdiff(microsecond, '2019-02-02 02:02:02.000002', '2018-01-01 01:01:01.000001');

-- LEAP YEAR LEAP MONTH
select timestampdiff(day, '2016-01-01', '2017-01-01');
select timestampdiff(day, '2017-01-01', '2018-01-01');
select timestampdiff(day, '2016-01-01', '2016-02-01');
select timestampdiff(day, '2016-02-01', '2016-03-01');
select timestampdiff(day, '2016-03-01', '2016-04-01');
select timestampdiff(day, '2016-04-01', '2016-05-01');
select timestampdiff(day, '2016-05-01', '2016-06-01');
select timestampdiff(day, '2016-06-01', '2016-07-01');
select timestampdiff(day, '2016-07-01', '2016-08-01');
select timestampdiff(day, '2016-08-01', '2016-09-01');
select timestampdiff(day, '2016-09-01', '2016-10-01');
select timestampdiff(day, '2016-10-01', '2016-11-01');
select timestampdiff(day, '2016-11-01', '2016-12-01');
select timestampdiff(day, '2016-12-01', '2017-01-01');
select timestampdiff(day, '2000-02-01', '2000-03-01');
select timestampdiff(day, '1900-02-01', '1900-03-01');

-- timestamp without time zone
select timestampdiff(year, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(quarter, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(week, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(month, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(day, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(hour, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(minute, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(second, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);
select timestampdiff(microsecond, '2018-01-01 01:01:01.000001'::timestamp, '2019-02-02 02:02:02.000002'::timestamp);

-- now()
select timestampdiff(year, '2018-01-01', now());
select timestampdiff(quarter, '2018-01-01', now());
select timestampdiff(week, '2018-01-01', now());
select timestampdiff(month, '2018-01-01', now());
select timestampdiff(day, '2018-01-01', now());
select timestampdiff(hour, '2018-01-01', now());
select timestampdiff(minute, '2018-01-01', now());
select timestampdiff(second, '2018-01-01', now());
select timestampdiff(microsecond, '2018-01-01', now());

-- current_timestamp
select timestampdiff(year,'2018-01-01', current_timestamp);

-- test error
select timestampdiff(yearss, '2018-01-01', now());
select timestampdiff(century, '2018-01-01', now());
select timestampdiff(year, '-0001-01-01', '2019-01-01');
select timestampdiff(microsecond, '0001-01-01', '293000-12-31');
select timestampdiff(microsecond, '2018-13-01', '2019-12-31');
select timestampdiff(microsecond, '2018-01-01', '2019-12-32');

-- test table ref
create table timestamp(a timestamp, b timestamp with time zone);
insert into timestamp values('2018-01-01 01:01:01.000001', '2019-02-02 02:02:02.000002');

select timestampdiff(year, '2018-01-01 01:01:01.000001', b) from timestamp;
select timestampdiff(quarter, '2018-01-01 01:01:01.000001', b) from timestamp;
select timestampdiff(week, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(month, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(day, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(hour, '2018-01-01 01:01:01.000001', b) from timestamp;;
select timestampdiff(minute, '2018-01-01 01:01:01.000001',b) from timestamp;;
select timestampdiff(second, '2018-01-01 01:01:01.000001', b) from timestamp;
select timestampdiff(microsecond, '2018-01-01 01:01:01.000001', b) from timestamp;
drop table timestamp;

-- test char/varchar length
create table char_test(a char(10),b varchar(10));
insert into char_test values('零一二三四五六七八九','零一二三四五六七八九');
insert into char_test values('零1二3四5六7八9','零1二3四5六7八9');
insert into char_test values('零1二3四5六7八9','零1二3四5六7八90');
insert into char_test values('零1二3四5六7八90','零1二3四5六7八9');
insert into char_test values('零0','零1二3');
insert into char_test values('零0  ','零1二3');
insert into char_test values('零0','零1二3  ');
insert into char_test values('','');
insert into char_test values(null,null);
insert into char_test values('0','0');
select length(a),length(b) from char_test;
select lengthb(a),lengthb(b) from char_test;
select bit_length(a),bit_length(b) from char_test;

create index a on char_test(a);
create index b on char_test(b);
set enable_seqscan to off;
select * from char_test where a = '零0';
select * from char_test where b = '零1二3';
drop table char_test;

\c regression

drop database b;
