--change datestyle to Postgres
SET Datestyle = 'Postgres,DMY';
--change datestyle to ISO mode which is default output style of GaussDB
set datestyle = iso,ymd;
create schema hw_datatype_3;
set search_path = hw_datatype_3;
--smalldatetime
CREATE TABLE SMALLDATETIME_TBL (d1 smalldatetime);
SELECT LOCALTIMESTAMP;
INSERT INTO SMALLDATETIME_TBL VALUES ('now');
INSERT INTO SMALLDATETIME_TBL VALUES ('today');
SHOW TIMEZONE; 
SELECT * FROM SMALLDATETIME_TBL;
INSERT INTO SMALLDATETIME_TBL VALUES ('yesterday');
INSERT INTO SMALLDATETIME_TBL VALUES ('tomorrow');
INSERT INTO SMALLDATETIME_TBL VALUES ('tomorrow EST');
INSERT INTO SMALLDATETIME_TBL VALUES ('tomorrow zulu');
SELECT * FROM SMALLDATETIME_TBL;
EXPLAIN (VERBOSE on, COSTS on) SELECT count(*) AS One FROM SMALLDATETIME_TBL WHERE d1 = smalldatetime  'today';
SELECT count(*) AS One FROM SMALLDATETIME_TBL WHERE d1 = smalldatetime  'today';
SELECT count(*) AS Three FROM SMALLDATETIME_TBL WHERE d1 = smalldatetime  'tomorrow';
SELECT count(*) AS One FROM SMALLDATETIME_TBL WHERE d1 = smalldatetime  'yesterday';
DELETE FROM SMALLDATETIME_TBL;
-- Special values
INSERT INTO SMALLDATETIME_TBL VALUES ('-infinity');
INSERT INTO SMALLDATETIME_TBL VALUES ('infinity');
INSERT INTO SMALLDATETIME_TBL VALUES ('epoch');
-- Obsolete special values
INSERT INTO SMALLDATETIME_TBL VALUES ('invalid');
INSERT INTO SMALLDATETIME_TBL VALUES ('undefined');
INSERT INTO SMALLDATETIME_TBL VALUES ('current');
DROP TABLE SMALLDATETIME_TBL;
create table smalldatetime_test(i smalldatetime,k smalldatetime);
insert into smalldatetime_test values ('1997-01-01 03:04','1997-02-02 03:04');
insert into smalldatetime_test values ('1997-03-03 12:44','1997-02-02 07:44');
insert into smalldatetime_test values ('1997-03-03 11:04','1997-03-03 11:04');
insert into smalldatetime_test values ('1997-03-03 11:04:29','1997-03-03 11:04:12');
insert into smalldatetime_test values ('1997-03-03 11:59:29','1997-03-03 11:59:30');
insert into smalldatetime_test values ('1997-03-03 23:59:29','1997-03-03 11:59:31');
insert into smalldatetime_test values ('1997-03-03 23:59:56','1997-03-03 11:59:59');
insert into smalldatetime_test values ('1997-03-03 23:60:56','1997-03-03 11:60:59');
insert into smalldatetime_test values ('1997-03-03 23:60:01','1997-03-03 11:60:30');
select '' as one,* from smalldatetime_test where i < k ORDER BY i,k;
select '' as one,* from smalldatetime_test where i = k ORDER BY i,k;
select '' as one,* from smalldatetime_test where i > k ORDER BY i,k;
select '' as two,* from smalldatetime_test where i >= k ORDER BY i,k;
select '' as two,* from smalldatetime_test where i <= k ORDER BY i,k;
select i - k as diff from smalldatetime_test where i >= k ORDER BY i,k;
select '' as op ,i + interval '1 year' as three,i from smalldatetime_test order by i;
select '' as op,i - interval '1 year' as three,i from smalldatetime_test order by i;
select '' as op,i + interval '1 month' as three,i from smalldatetime_test order by i;
select '' as op,i - interval '1 month' as three,i from smalldatetime_test order by i;
select '' as op,i + interval '1 day' as three,i from smalldatetime_test order by i;
select '' as op,i - interval '1 day' as three,i from smalldatetime_test order by i;
select '' as op,i + interval '1 hour' as three,i from smalldatetime_test order by i;
select '' as op,i - interval '1 hour' as three,i from smalldatetime_test order by i;
select '' as op,i + interval '1 minute' as three,i from smalldatetime_test order by i;
select '' as op,i - interval '1 minute' as three,i from smalldatetime_test order by i;
select '' as years, extract(year from i),i from smalldatetime_test order by i,k;
select '' as months, extract(month from i),i from smalldatetime_test order by i,k;
select '' as days, extract(day from i),i from smalldatetime_test order by i,k;
select '' as hours, extract(hour from i),i from smalldatetime_test order by i,k;
select '' as minutes, extract(minute from i),i from smalldatetime_test order by i;
select '' as seconds, extract(seconds from i),i from smalldatetime_test order by i;
select date_trunc( 'month', i) AS week_trunc from smalldatetime_test order by i;
select date_trunc( 'day', i) AS week_trunc from smalldatetime_test order by i;
select date_trunc( 'hour', i) AS week_trunc from smalldatetime_test order by i;
select date_trunc( 'minute', i) AS week_trunc from smalldatetime_test order by i;
drop table smalldatetime_test;
create table smalldatetime_test(id int,dates smalldatetime, primary key(id,dates));
insert into smalldatetime_test values(1,'1997-01-01 03:04:11');
insert into smalldatetime_test values(2,'1997-01-02 03:05:30');
insert into smalldatetime_test values(3,'1997-01-02 03:05:30');
insert into smalldatetime_test values(4,'2001-11-12 11:05:31');
insert into smalldatetime_test values(5,'2007-12-32 11:05:31');                                          
insert into smalldatetime_test values(6,'2007-13-30 11:05:31');                                              
insert into smalldatetime_test values(6,'2007-12-30 24:05:31');
insert into smalldatetime_test values(6,'2007-12-30 23:60:31');                                            ^
insert into smalldatetime_test values(6,'2007-12-30 23:20:60');
select * from smalldatetime_test order by 1,2;
select * from smalldatetime_test where dates BETWEEN '1997-01-02' AND '2038-01-01' order by 1,2;
select id,cast(dates as date) from smalldatetime_test order by 1,2;
select id,cast(dates as time) from smalldatetime_test order by 1,2;
select id,cast(dates as timestamp) from smalldatetime_test order by 1,2;
select id,cast(dates as timestamptz) from smalldatetime_test order by 1,2;
select id,cast(dates as abstime) from smalldatetime_test order by 1,2;
select id,cast(dates as text) from smalldatetime_test order by 1,2;
select id,cast(dates as varchar) from smalldatetime_test order by 1,2;
select id,cast(dates as varchar2) from smalldatetime_test order by 1,2;
select id,cast(dates as bpchar) from smalldatetime_test order by 1,2;
--smalldatetime to timestamp
create table timestamp_test(t timestamp);
insert into timestamp_test select dates from smalldatetime_test;
select * from timestamp_test order by 1;
drop table timestamp_test;
--smalldatetime to timestamptz
create table timestamptz_test(t timestamptz);
insert into timestamptz_test select dates from smalldatetime_test;
select * from timestamptz_test order by 1;
drop table timestamptz_test;
--smalldatetime to abstime
create table abstime_test(t abstime);
insert into abstime_test select dates from smalldatetime_test;
select * from abstime_test order by 1;
drop table abstime_test;
--smalldatetime to time
create table time_test(t time);
insert into time_test select dates from smalldatetime_test;
select * from time_test order by 1;
drop table time_test;
--smalldatetime to date
create table date_test(t date);
insert into date_test select dates from smalldatetime_test;
select * from date_test order by 1;
drop table date_test;
--smalldatetime to text
create table text_test(t text);
insert into text_test select dates from smalldatetime_test;
select * from text_test order by 1;
drop table text_test;
--smalldatetime to varchar2
create table smalldate_varchar(t varchar2);
insert into smalldate_varchar select dates from smalldatetime_test;
select * from smalldate_varchar order by 1;
drop table smalldate_varchar;
--varchar to smalldatetime
create table varchar_smalldate(t varchar);
insert into varchar_smalldate select dates from smalldatetime_test;
select * from varchar_smalldate order by 1;
drop table varchar_smalldate;
-- TO_CHAR()
SELECT '' AS to_char_1, to_char(dates, 'DAY Day day DY Dy dy MONTH Month month RM MON Mon mon')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_2, to_char(dates, 'FMDAY FMDay FMday FMMONTH FMMonth FMmonth FMRM')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_3, to_char(dates, 'Y,YYY YYYY YYY YY Y CC Q MM WW DDD DD D J')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_4, to_char(dates, 'FMY,YYY FMYYYY FMYYY FMYY FMY FMCC FMQ FMMM FMWW FMDDD FMDD FMD FMJ')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_5, to_char(dates, 'HH HH12 HH24 MI')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_6, to_char(dates, E'"HH:MI is" HH:MI "\\"text between quote marks\\""')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_7, to_char(dates, 'HH24--text--MI--text')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_8, to_char(dates, 'YYYYTH YYYYth Jth')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_9, to_char(dates, 'YYYY A.D. YYYY a.d. YYYY bc HH:MI P.M. HH:MI p.m. HH:MI pm')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_10, to_char(dates, 'IYYY IYY IY I IW IDDD ID')
   FROM smalldatetime_test order by 2;
SELECT '' AS to_char_11, to_char(dates, 'FMIYYY FMIYY FMIY FMI FMIW FMIDDD FMID')
   FROM smalldatetime_test order by 2;
 
drop table smalldatetime_test;
select smalldatetime '2012-12-16 10:11:12' = smalldatetime '2012-12-16 10:11:16' as result;
select smalldatetime '2012-12-16 10:11:30' > smalldatetime '2012-12-16 10:11:16' as result;
select smalldatetime '2012-12-16 10:11:15' < smalldatetime '2012-12-16 10:11:31' as result;
select smalldatetime '2012-12-16 10:11:30' >= smalldatetime '2012-12-16 10:11:16' as result;
select smalldatetime '2012-12-16 10:11:15' <= smalldatetime '2012-12-16 10:11:31' as result;
select smalldatetime '2012-12-16 10:59:30' <>  smalldatetime '2012-12-16 10:59:20' as result;


--text to smalldatetime
create table text_smalldate (t smalldatetime);
insert into text_smalldate values('hello'::text);
insert into text_smalldate values('2013-12'::text);
insert into text_smalldate values('2013'::text);
insert into text_smalldate values('201203040506'::text);
insert into text_smalldate values('2012030405060708'::text);
insert into text_smalldate values('99990909'::text);
insert into text_smalldate values('99990909'::text);

drop table text_smalldate;
--date to smalldatetime
create table date_smalldate (t smalldatetime);
insert into date_smalldate values('1997-12-14 10:24:59'::date);
insert into date_smalldate values('1997-12-14 10:24'::date);
insert into date_smalldate values('1997-12-14 10:'::date);
insert into date_smalldate values('1997-12-14'::date);
insert into date_smalldate values('1997-12'::date);
insert into date_smalldate values('199712'::date);
insert into date_smalldate values('199712'::date);
drop table date_smalldate;

--INTERAL TO NUMERIC
create table test_num(mynum numeric);
select to_date('2013-03-14 14:27:00','YYYY-MM-DD HH24:MI:SS') - to_date('2013-3-1 9:00:00','YYYY-MM-DD HH24:MI:SS');
insert into test_num values(to_date('2013-03-14 14:27:00','YYYY-MM-DD HH24:MI:SS') - to_date('2013-3-1 9:00:00','YYYY-MM-DD HH24:MI:SS'));
select mynum from test_num order by 1;
insert into test_num values(interval '2 days 09:33:00');
select mynum from test_num order by 1;
select floor(mynum) from test_num order by 1;
drop table test_num;
select floor(interval '2 days 09:33:00');

create or replace procedure test_interval_to_num(my_num numeric)
as
begin
end;
/
call test_interval_to_num(interval '5 days 19:23:46');
drop procedure test_interval_to_num;

create table TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000(COL timestamp with time zone);
set datestyle to german, ymd;
insert into TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 values('1999-01-23 07:37:16.00 PST');
select * from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000;
set datestyle to german,dmy;
insert into TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 values('1999-11-23 07:37:16.00 PST');
select * from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 order by 1;
set datestyle to german,mdy;
insert into TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 values('1999-12-23 07:37:16.00 PST');
select * from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 order by 1;
delete from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000;
set datestyle to SQL, ymd;
insert into TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 values('1999-01-23 07:37:16.00 PST');
select * from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000;
set datestyle to SQL, dmy;
insert into TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 values('1999-12-23 07:37:16.00 PST');
select * from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 order by 1;
set datestyle to SQL, mdy;
insert into TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 values('1999-12-23 07:37:16.00 PST');
select * from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 order by 1;
set datestyle to german, mdy;
insert into TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 values('01.23.1999 07:37:16.00');
select * from TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000 order by 1;
drop table TIMEFORMAT_OUTPUTFORMAT_TIMESTAMP_000;
--test BLOB AND RAW BI-DIRECTION CONVERSION
CREATE TABLE TEST_RAW(B BLOB, R RAW);
INSERT INTO TEST_RAW VALUES('ABCD','1234');
CREATE OR REPLACE PROCEDURE SP_TEST_3(R RAW)
AS
BEGIN
    DBMS_OUTPUT.PUT_LINE('BLOB TO RAW CONVERSION SUCCESSFULLY'); 
END;
/
CREATE OR REPLACE PROCEDURE SP_TEST_4(B BLOB)
AS
BEGIN
    DBMS_OUTPUT.PUT_LINE( 'RAW TO BLOB CONVERSION SUCCESSFULLY'); 
END;
/
CREATE OR REPLACE PROCEDURE SP_INVOKER
AS
    B BLOB;
    R RAW;
BEGIN
    SELECT * INTO B,R FROM TEST_RAW;
    SP_TEST_3(B);
    SP_TEST_4(R);
    B := R;
    R := B;
    DBMS_OUTPUT.PUT_LINE( 'BLOB AND RAW BI-DIRECTION CONVERSION SUCCESSFULLY'); 
 --   EXCEPTION
 --       WHEN OTHERS THEN
END;
/
CALL SP_INVOKER();
DROP TABLE TEST_RAW;
DROP PROCEDURE SP_TEST_3;
DROP PROCEDURE SP_TEST_4;
DROP PROCEDURE SP_INVOKER;

--test bool<-> int1,int2,int4,int8

select (1<2) + 1::int1;
select (1<2) + 1::int2;
select (1<2) + 1::int4;
select (1<2) + 1::int8;
select (1<2) - 1::int1;
select (1<2) - 1::int2;
select (1<2) - 1::int4;
select (1<2) - 1::int8;
select (1<2) * 1::int1;
select (1<2) * 1::int2;
select (1<2) * 1::int4;
select (1<2) * 1::int8;
select (1<2) / 1::int1;
select (1<2) / 1::int2;
select (1<2) / 1::int4;
select (1<2) / 1::int8;
select 1::int1 + (1<2);
select 1::int2 + (1<2);
select 1::int4 + (1<2);
select 1::int8 + (1<2);
select 1::int1 - (1<2);
select 1::int2 - (1<2);
select 1::int4 - (1<2);
select 1::int8 - (1<2);
select 1::int1 * (1<2);
select 1::int2 * (1<2);
select 1::int4 * (1<2);
select 1::int8 * (1<2);
select 1::int1 / (1<2);
select 1::int2 / (1<2);
select 1::int4 / (1<2);
select 1::int8 / (1<2);


CREATE OR REPLACE FUNCTION SP_BOOL_01(P1 INT1) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_01(1<2);
CREATE OR REPLACE FUNCTION SP_BOOL_02(P1 INT2) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_02(1<2);
CREATE OR REPLACE FUNCTION SP_BOOL_03(P1 INT4) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_03(1<2);
CREATE OR REPLACE FUNCTION SP_BOOL_04(P1 INT8) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_04(1<2);
CREATE OR REPLACE FUNCTION SP_BOOL_05(P1 BOOL) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_05(1::INT1);
CREATE OR REPLACE FUNCTION SP_BOOL_06(P1 BOOL) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_06(1::INT2);
CREATE OR REPLACE FUNCTION SP_BOOL_07(P1 BOOL) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_07(1::INT4);
CREATE OR REPLACE FUNCTION SP_BOOL_08(P1 BOOL) RETURNS VOID AS $$BEGIN NULL; END;$$ LANGUAGE PLPGSQL; SELECT SP_BOOL_08(1::INT8);


DROP FUNCTION SP_BOOL_01;
DROP FUNCTION SP_BOOL_02;
DROP FUNCTION SP_BOOL_03;
DROP FUNCTION SP_BOOL_04;
DROP FUNCTION SP_BOOL_05;
DROP FUNCTION SP_BOOL_06;
DROP FUNCTION SP_BOOL_07;
DROP FUNCTION SP_BOOL_08;

CREATE OR REPLACE FUNCTION IMPLICIT_CONVERSION_064(tempdata nvarchar2) RETURNS VOID AS $$
BEGIN
 raise info'nvarchar2 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION IMPLICIT_CONVERSION_064(tempdata bpchar) RETURNS VOID AS $$
BEGIN
 raise info'bpchar type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
SELECT IMPLICIT_CONVERSION_064(cast('123' as clob));
DROP FUNCTION IMPLICIT_CONVERSION_064(NVARCHAR2);
DROP FUNCTION IMPLICIT_CONVERSION_064(bpchar);

--new data type support PK
create table tinyint_pk(id tinyint primary key);
insert into tinyint_pk values(255);
create table smalldatetime_pk(id smalldatetime primary key);
insert into smalldatetime_pk values('19880130');
create table nvarchar2_pk(id nvarchar2(20) primary key);
insert into nvarchar2_pk values('come on');
create table raw_pk(id raw primary key);
insert into raw_pk values('abc');
drop table tinyint_pk;
drop table smalldatetime_pk;
drop table nvarchar2_pk;
drop table raw_pk;

--test priority of two-character compare operator
create table priority_t1 (c1 char(10),c2 char(10));
insert into priority_t1 values('100','200');
select * from priority_t1 where '1111' <= '1111' || '11111';
select * from priority_t1 where '1111' <= ('1111' || '11111');
select * from priority_t1 where '1111' >= '1111' || '11111';
select * from priority_t1 where '1111' >= ('1111' || '11111');
select * from priority_t1 where '1111' = '1111' || '11111';
select * from priority_t1 where '1111' <> '1111' || '11111';
select * from priority_t1 where '1111' != '1111' || '11111';
select * from priority_t1 where (select c2 || c1 from priority_t1) <= (select c2 from priority_t1) || (select c1 from priority_t1);
select * from priority_t1 where (select c2 || c1 from priority_t1) >= (select c2 from priority_t1) || (select c1 from priority_t1);
select * from priority_t1 where (select c2 || c1 from priority_t1) <> (select c2 from priority_t1) || (select c1 from priority_t1);
select * from priority_t1 where (select c2 || c1 from priority_t1) != (select c2 from priority_t1) || (select c1 from priority_t1);
select * from priority_t1 where (select c2 || c1 from priority_t1) = (select c2 from priority_t1) || (select c1 from priority_t1);
drop table priority_t1;

create table tbl_oidvector(a int, b oidvector);
insert into tbl_oidvector values(1, '23 1700');
select oidvectortypes(b) from tbl_oidvector;
drop table tbl_oidvector;
reset search_path;
