create schema functions_test2;
set search_path = 'functions_test2';

select datediff(year, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(yy, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(yyyy, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(quarter, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(qq, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(q, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(month, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(mm, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(m, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(dayofyear, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(dy, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(y, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(day, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(dd, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(d, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(week, CAST('2037-03-01 23:30:05.523'AS timestamp),CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(wk, CAST('2037-03-01 23:30:05.523'AS timestamp),CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(ww, CAST('2037-03-01 23:30:05.523'AS timestamp),CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(weekday, CAST('2037-03-01 23:30:05.523'AS timestamp),CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(dw, CAST('2037-03-01 23:30:05.523'AS timestamp),CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(w, CAST('2037-03-01 23:30:05.523'AS timestamp),CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(hour, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(hh, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(minute,CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(mi, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(n, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(second, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(ss, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(s, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(millisecond, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));
select datediff(ms, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));
select datediff(microsecond, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));
select datediff(mcs, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));
select datediff(nanosecond, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));
select datediff(ns, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));

select datediff_big(year, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(quarter, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(month, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(dayofyear, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(day, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(week,CAST('2037-03-01 23:30:05.523'AS timestamp),CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(hour, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(minute,CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(second, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(millisecond, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));
select datediff_big(microsecond, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));
select datediff_big(nanosecond, CAST('2036-02-28 01:23:45.234'AS timestamp), CAST('2036-02-28 01:23:45.123'AS timestamp));

select datediff(month, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff_big(month, CAST('2037-03-01 23:30:05.523'AS timestamp), CAST('2036-02-28 23:30:05.523'AS timestamp));
select datediff(month, CAST('2037-03-01 23:30:05.523'AS timestamptz), CAST('2036-02-28 23:30:05.523'AS timestamptz));
select datediff_big(month, CAST('2037-03-01 23:30:05.523'AS timestamptz), CAST('2036-02-28 23:30:05.523'AS timestamptz));
select datediff(month, CAST('2037-03-01 23:30:05.523'AS date), CAST('2036-02-28 23:30:05.523'AS date));
select datediff_big(month, CAST('2037-03-01 23:30:05.523'AS date), CAST('2036-02-28 23:30:05.523'AS date));
select datediff(month, CAST('2037-03-01 23:30:05.523'AS time), CAST('2036-02-28 23:30:05.523'AS time));
select datediff_big(month, CAST('2037-03-01 23:30:05.523'AS time), CAST('2036-02-28 23:30:05.523'AS time));
select datediff(month, CAST('2037-03-01 23:30:05.523'AS smalldatetime), CAST('2036-02-28 23:30:05.523'AS smalldatetime));
select datediff_big(month, CAST('2037-03-01 23:30:05.523'AS smalldatetime), CAST('2036-02-28 23:30:05.523'AS smalldatetime));

select datediff(seconds, CAST(make_timestamp(1, 2, 12, 0, 0, 0) AS date), CAST(make_timestamp(10000, 2, 12, 0, 0, 0) AS date));
select datediff_big(nanoseconds, CAST(make_timestamp(1, 2, 12, 0, 0, 0) AS date), CAST(make_timestamp(10000, 2, 12, 0, 0, 0) AS date));

select datediff(abc, CAST(make_timestamp(1, 2, 12, 0, 0, 0) AS date), CAST(make_timestamp(2000, 2, 12, 0, 0, 0) AS date));
select datediff(years, CAST(make_timestamp(1, 2, 12, 0, 0, 0) AS date), CAST(make_timestamp(2000, 2, 12, 0, 0, 0) AS date));

select CHARINDEX('hello', 'hello world');
select CHARINDEX('hello  ', 'hello world');
select CHARINDEX('hello world', 'hello');
select CHARINDEX(NULL, NULL);
select CHARINDEX(NULL, 'string');
select CHARINDEX('pattern', NULL);
select CHARINDEX('pattern', 'string', NULL);
select CHARINDEX('hello', 'hello world', -1);
select CHARINDEX('hello', 'hello world', 0);
select CHARINDEX('hello', 'hello world', 1);
select CHARINDEX('hello', 'hello world', 2);
select CHARINDEX('world', 'hello world', 6);
select CHARINDEX('world', 'hello world', 7);
select CHARINDEX('world', 'hello world', 8);
select CHARINDEX('is', 'This is a string');
select CHARINDEX('is', 'This is a string', 4);

select atn2(1.23423::int1, 2.3412::int1);
select atn2(1.23423::int2, 2.3412::int2);
select atn2(1.23423::int4, 2.3412::int4);
select atn2(1.23423::int8, 2.3412::int8);
select atn2(1.23423::real, 2.3412::real);
select atn2(1.23423::numeric, 2.3412::numeric);
select atn2('1.23423', '2.3412');

select atn2(2::int4, 1.2345::real);
select atn2(1.2345::double precision, 1234::int4);
select atn2('123,345', 123);

CREATE VIEW atn2_1 AS (
    SELECT
        ATN2(CAST(2 AS INT), CAST(3 AS INT)) AS res1,
        ATN2(CAST(2.5 AS FLOAT), CAST(3.5 AS FLOAT)) AS res2,
        ATN2(CAST(2.5 AS REAL), CAST(3.5 AS REAL)) AS res3,
        ATN2(CAST(2.5 AS BIGINT), CAST(3.5 AS BIGINT)) AS res4,
        ATN2(CAST(2.5 AS SMALLINT), CAST(3.5 AS SMALLINT)) AS res5,
        ATN2(CAST(2.5 AS TINYINT), CAST(3.5 AS TINYINT)) AS res6,
        ATN2(CAST(2.5 AS DECIMAL), CAST(3.5 AS DECIMAL)) AS res7,
        ATN2(CAST(2.5 AS NUMERIC), CAST(3.5 AS NUMERIC)) AS res8,
        ATN2(CAST('2.5' AS CHAR), CAST('3.5' AS CHAR)) AS res9,
        ATN2(CAST('2.5' AS VARCHAR), CAST('3.5' AS VARCHAR)) AS res10,
        ATN2(CAST('2.5' AS BPCHAR), CAST('3.5' AS BPCHAR)) AS res11,
        ATN2(CAST('2.5' AS NVARCHAR2), CAST('3.5' AS NVARCHAR2)) AS res12
    );
select * from atn2_1;
drop view atn2_1;


select ISNULL(NULL, 1);
select ISNULL(2, 1);
select ISNULL();
select ISNULL();

select log10(10);
select log10(10::real);
select log10(10::double precision);
select log10(10::numeric);

select atn2('abc', '2.3412');
select atn2(123::varbinary, '2343'::varbinary);

select log10('abc');
select log10(10::vharchar);
select log10(now());

select ISNULL('abc', 123);

SELECT CONVERT(DATE, '');
SELECT CONVERT(DATE, '', 130);

SELECT CONVERT(DATE, '3-2-4');
SELECT CONVERT(DATE, '3-12-2024');
SELECT CONVERT(DATE, '11-12-2024');

SELECT CONVERT(DATE, '3.2.4');
SELECT CONVERT(DATE, '3.12.2024');
SELECT CONVERT(DATE, '11.12.2024');

SELECT CONVERT(DATE, '3/2/4');
SELECT CONVERT(DATE, '3/12/2024');
SELECT CONVERT(DATE, '11/12/2024');

SELECT CONVERT(DATE, '9999-12-30 23:59:59.9999999');
SELECT CONVERT(DATE, '9999-12-30 23:59:59.99999999');
SELECT CONVERT(DATE, '9999-12-30 23:59:59.999999999');

SELECT CONVERT(DATE, '9999-12-30 23:59:59.9999999999');
SELECT CONVERT(DATE, '9999-12-31 23:59:59.9999999');
SELECT CONVERT(DATE, '9999-12-31 23:59:59.99999999');

SELECT CONVERT(DATE, '9999-12-31 23:59:59.999999999');
SELECT CONVERT(DATE, '9999-12-31 23:59:59.9999999999');

SELECT CONVERT(DATE,'2023-00-01');

SELECT CONVERT(DATE,'0000-00-00');
SELECT CONVERT(DATE,'1752-01-01');
SELECT CONVERT(DATE,'1753-01-01');

SELECT CONVERT(DATE, 'Apr 12,2000');


SELECT CONVERT(TIME, '11 AM');
SELECT CONVERT(TIME, '11 PM');
SELECT CONVERT(TIME, '0 AM');
SELECT CONVERT(TIME, '0 PM');
SELECT CONVERT(TIME, '11:22.123 AM');
SELECT CONVERT(TIME, '11 AM -05:12');
SELECT CONVERT(TIME, '13 AM +05:12');

SELECT CONVERT(TIMESTAMP, '');
SELECT CONVERT(TIMESTAMP, '3-12-2024 14:30');
SELECT CONVERT(TIMESTAMP, '3.12.2024 14:30');
SELECT CONVERT(TIMESTAMP, '3/12/2024 14:30');
SELECT CONVERT(TIMESTAMP, '2022-10-30T03:00:00');
SELECT CONVERT(TIMESTAMP, '2022-10-30T03:00:00.123');
SELECT CONVERT(TIMESTAMP, '2022-10-30T03:00:00.123-12:12');
SELECT CONVERT(TIMESTAMP, '2022-10-30T03:00:00.123+12:12');
SELECT CONVERT(TIMESTAMP, '2022-10-30T03:00:00.12345');
SELECT CONVERT(TIMESTAMP, '2022-10-30T03:00:00:12345');

SELECT CONVERT(TIMESTAMPTZ, '');
SELECT CONVERT(TIMESTAMPTZ, '', 130);
SELECT CONVERT(TIMESTAMPTZ, '3-12-2024 14:30 +8:00');

select CONVERT(varchar(30), CAST('2017-08-25' AS DATE), 102);
select CONVERT(varchar(30), CAST('13:01:59' AS TIME), 8);
select CONVERT(varchar(30), CAST('13:01:59' AS TIME), 22);
select CONVERT(varchar(30), CAST('13:01:59' AS TIME), 22);
select CONVERT(varchar(30), CAST('2017-08-25 13:01:59' AS TIMESTAMP), 100);
select CONVERT(varchar(30), CAST('2017-08-25 13:01:59' AS TIMESTAMP), 109);
select CONVERT(DATE, '08/25/2017', 101);
select CONVERT(TIME, '12:01:59', 101);
select CONVERT(TIMESTAMP, '2017-08-25 01:01:59PM', 120);
select CONVERT(varchar, current_date, 8);
select CONVERT(varchar, current_date, 1);
select CONVERT(varchar, localtime, 1);
select CONVERT(varchar, localtime, 8);

select CONVERT(varchar(30), CAST(11234561231231.234 AS float), 1);
select CONVERT(varchar(30), CAST(11234561231231.234 AS float), 2);
select CONVERT(varchar(30), CAST(11234561231231.234 AS float), 3);

select CONVERT(varchar(10), CAST(4936.56 AS MONEY), 0);
select CONVERT(varchar(10), CAST(4936.56 AS MONEY), 1);
select CONVERT(varchar(10), CAST(4936.56 AS MONEY), 2);
select CONVERT(varchar(10), CAST(-4936.56 AS MONEY), 0);

SELECT CONVERT(int, 99.9);
SELECT CONVERT(smallint, 99.9);
SELECT CONVERT(bigint, 99.9);
SELECT CONVERT(int, -99.9);
SELECT CONVERT(int, '99');
SELECT CONVERT(int, CAST(99.9 AS double precision));
SELECT CONVERT(int, CAST(99.9 AS real));

select TRY_CONVERT(varchar(30), CAST('2017-08-25' AS DATE), 102);
select TRY_CONVERT(varchar(30), CAST('13:01:59' AS TIME), 8);
select TRY_CONVERT(varchar(30), CAST('13:01:59' AS TIME), 22);
select TRY_CONVERT(varchar(30), CAST('2017-08-25 13:01:59' AS TIMESTAMP), 109);
select TRY_CONVERT(varchar(30), CAST('11234561231231.234' AS float), 0);
select TRY_CONVERT(varchar(30), CAST('11234561231231.234'AS float), 1);
select TRY_CONVERT(varchar(10), CAST(4936.56 AS MONEY), 0);

select TRY_CAST('08/25/2017' AS date);
select TRY_CAST('12:01:59' AS time);
select TRY_CAST(123 AS float);
select TRY_CAST('123' AS int);
select TRY_CAST('123' AS text);
select TRY_CAST('123.456' AS numeric(6,3));
select TRY_CAST(123.456 AS numeric(6,3));
select TRY_CAST('123' As smallint);
select TRY_CAST('1234567890' AS bigint);

select TRY_CAST(CAST(12.56 as numeric(4,2)) As smallint);
select TRY_CAST(CAST(1.56 as real) As smallint);
select TRY_CAST(CAST(1.56 as money) As smallint);
select TRY_CAST(4936.56 AS MONEY);

reset search_path;
drop schema functions_test2 cascade;
