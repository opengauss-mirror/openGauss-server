set a_format_version='10c';
set a_format_dev_version='s1';
set nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';


--小数字符串转int
select cast('-.45' as int1);
select cast('-.55' as int1);
select cast('-123.45' as int1);
select cast('-123.55' as int1);
select cast('.45' as int1);
select cast('.55' as int1);
select cast('.65' as int1);
select cast('123.45' as int1);
select cast('123.55' as int1);
select cast('123.65' as int1);
select cast('255.45' as int1);
select cast('255.55' as int1);
select cast('s123.65' as int1);
select cast('123s.65' as int1);
select cast('123.65s' as int1);

select cast('-.45' as int2);
select cast('-.55' as int2);
select cast('-123.45' as int2);
select cast('-123.55' as int2);
select cast('.45' as int2);
select cast('.55' as int2);
select cast('.65' as int2);
select cast('123.45' as int2);
select cast('123.55' as int2);
select cast('123.65' as int2);
select cast('-32768.45' as int2);
select cast('-32768.55' as int2);
select cast('32767.45' as int2);
select cast('32767.55' as int2);
select cast('s123.65' as int2);
select cast('123s.65' as int2);
select cast('123.65s' as int2);

select cast('-.45' as int);
select cast('-.55' as int);
select cast('-123.45' as int);
select cast('-123.55' as int);
select cast('.45' as int);
select cast('.55' as int);
select cast('.65' as int);
select cast('123.45' as int);
select cast('123.55' as int);
select cast('123.65' as int);
select cast('-2147483648.45' as int);
select cast('-2147483648.55' as int);
select cast('2147483647.45' as int);
select cast('2147483647.55' as int);
select cast('s123.65' as int);
select cast('123s.65' as int);
select cast('123.65s' as int);

select cast('-.45' as int4);
select cast('-.55' as int4);
select cast('-123.45' as int4);
select cast('-123.55' as int4);
select cast('.45' as int4);
select cast('.55' as int4);
select cast('.65' as int4);
select cast('123.45' as int4);
select cast('123.55' as int4);
select cast('123.65' as int4);
select cast('-2147483648.45' as int4);
select cast('-2147483648.55' as int4);
select cast('2147483647.45' as int4);
select cast('2147483647.55' as int4);
select cast('s123.65' as int4);
select cast('123s.65' as int4);
select cast('123.65s' as int4);

select cast('-.45' as int8);
select cast('-.55' as int8);
select cast('-123.45' as int8);
select cast('-123.55' as int8);
select cast('.45' as int8);
select cast('.55' as int8);
select cast('.65' as int8);
select cast('123.45' as int8);
select cast('123.55' as int8);
select cast('123.65' as int8);
select cast('-9223372036854775808.45' as int8);
select cast('-9223372036854775808.55' as int8);
select cast('9223372036854775807.45' as int8);
select cast('9223372036854775807.55' as int8);
select cast('s123.65' as int8);
select cast('123s.65' as int8);
select cast('123.65s' as int8);

select cast('-.45' as int16);
select cast('-.55' as int16);
select cast('-123.45' as int16);
select cast('-123.55' as int16);
select cast('.45' as int16);
select cast('.55' as int16);
select cast('.65' as int16);
select cast('123.45' as int16);
select cast('123.55' as int16);
select cast('123.65' as int16);
select cast('-170141183460469231731687303715884105728.45' as int16);
select cast('-170141183460469231731687303715884105728.55' as int16);
select cast('170141183460469231731687303715884105727.45' as int16);
select cast('170141183460469231731687303715884105727.55' as int16);
select cast('s123.65' as int16);
select cast('123s.65' as int16);
select cast('123.65s' as int16);


--DEFAULT return_vaule ON CONVERSION ERROR
select cast('123.4s' as int DEFAULT 666 ON CONVERSION ERROR);
select cast('123.4s' as int DEFAULT '666' ON CONVERSION ERROR);
select cast('123.4s' as int DEFAULT '666s' ON CONVERSION ERROR);
select cast('123.4s' as float);
select cast('123.4s' as float DEFAULT 666.67 ON CONVERSION ERROR);
select cast('123.4s' as float DEFAULT '666.67' ON CONVERSION ERROR);
select cast('123.4s' as int DEFAULT '666.67s' ON CONVERSION ERROR);
SELECT CAST('1997-13-22' AS DATE);
SELECT CAST('1997-13-22' AS DATE DEFAULT '2022-10-01' ON CONVERSION ERROR);
SELECT CAST('1997-13-22 12:13:14' AS TIMESTAMP);
SELECT CAST('1997-13-22 12:13:14' AS TIMESTAMP DEFAULT '2022-10-01 12:13:14' ON CONVERSION ERROR);


--fmt
SELECT CAST ('$12,123.456' AS int);
SELECT CAST ('$12,123.456' AS float);
SELECT CAST ('$12,345.67' AS NUMBER);
SELECT CAST ('$12,123.456' AS int, '$99,999.999');
SELECT CAST ('$12,123.456' AS float, '$99,999.999');
SELECT CAST ('$12,345.67' AS NUMBER, '$99,999.99');
SELECT CAST ('11-Oct-1999' AS date, 'DD-Mon-YYYY');
SELECT CAST ('11-11-1999' AS date, 'DD-MM-YYYY');
SELECT CAST('January 15, 1989, 11:00 A.M.' AS date , 'Month dd, YYYY, HH:MI A.M.');
SELECT CAST ('11-11-1999 17:45:29' AS timestamp, 'DD-MM-YYYY HH24:MI:SS');

SELECT CAST (NULL AS int, 99999.999);
SELECT CAST (12123.456 AS int, 99999.999);
SELECT CAST ('$12,123.456' AS int, '$99,999.999');
SELECT CAST ('$12,123.456' AS int, to_char('$99,999.999'));
SELECT CAST ('$12,123.456' AS int, (SELECT '$99,999.999'));
SELECT CAST (to_char('$12,123.456') AS int, '$99,999.999');
SELECT CAST ((SELECT '$12,123.456') AS int, '$99,999.999');
SELECT CAST ((SELECT '$12,123.456') AS int, (SELECT '$99,999.999'));
SELECT CAST ((SELECT '$12,123.456') AS int, to_char('$99,999.999'));
SELECT CAST (to_char('$12,123.456') AS int, to_char('$99,999.999'));
SELECT CAST (to_char('$12,123.456') AS int, (SELECT '$99,999.999'));
SELECT CAST (cast('12123.456' AS float) AS int, to_char('99999.999'));

reset nls_timestamp_format;
set a_format_version='';
set a_format_dev_version='';

