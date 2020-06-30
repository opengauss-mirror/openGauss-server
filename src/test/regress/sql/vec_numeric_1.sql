---- prepare work
DROP SCHEMA IF EXISTS vec_numeric_to_bigintger_1 CASCADE;
CREATE SCHEMA vec_numeric_to_bigintger_1;
SET current_schema = vec_numeric_to_bigintger_1;
SET enable_fast_numeric = on;

---- NUMERIC TABLE
CREATE TABLE vec_numeric_3 (id int, val1 int, val2 numeric(19,4), val3 numeric(19,4)) WITH (orientation=column);
COPY vec_numeric_3 FROM STDIN;
1	1	9.99	9.99
1	-1	9.99	9.99
1	1	9.99	9.99
1	-1	9.99	9.99
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
1	1	99999999999999.9999	99999999999999.9999
1	-1	99999999999999.9999	99999999999999.9999
-1	-1	-9.99	-9.99
-1	1	-9.99	-9.99
-1	-1	-9.99	-9.99
-1	1	-9.99	-9.99
-1	-1	-9.99	-9.99
-1	1	-9.99	-9.99
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
-1	-1	-99999999999999.9999	-99999999999999.9999
-1	1	-99999999999999.9999	-99999999999999.9999
\.
analyze vec_numeric_3;

---- vnumeric_sum bi64add_64<use_ctl>
SELECT id, sum(val2 + val3), sum(CASE WHEN val1 < 0 THEN 100.12345 ELSE val2 END), sum(CASE WHEN val1 > 0 THEN val2 ELSE 999999999999999.99 END) FROM vec_numeric_3 GROUP BY id ORDER BY 1,2,3,4;

---- vnumeric_sum bi128add_128<use_ctl>
SELECT id, sum(val2 * val3 * 10.0), sum(CASE WHEN val1 < 0 THEN 112332100.1999999009892345 ELSE val2 END), sum(CASE WHEN val1 > 0 THEN val2 ELSE 99999999999998989898998999.99 END), sum(CASE WHEN val1 < 0 THEN val2 * val3 ELSE 123.1233212343898765 END) FROM vec_numeric_3 GROUP BY id ORDER BY 1,2,3,4,5;

---- NUMERIC TABLE, test agg operation
CREATE TABLE aggt_row(a numeric, b numeric, c numeric, d numeric);
INSERT INTO aggt_row VALUES(1, 1, generate_series(1, 1000)%10, generate_series(1, 1000)%5);
INSERT INTO aggt_row VALUES(1, 2, generate_series(1, 500)%10, generate_series(1, 500)%5);
INSERT INTO aggt_row VALUES (3,1,11,11),(4,2,12,12);

CREATE TABLE aggt_col(a numeric, b numeric, c numeric, d numeric) WITH(orientation=column);
INSERT INTO aggt_col SELECT * FROM aggt_row;
analyze aggt_col;

---- agg function
SELECT b, sum(a), count(a), avg(a), min(a), max(a) FROM aggt_col GROUP BY b ORDER BY 1,2,3,4,5,6;

SELECT b, sum(a + 0.2000), count(a / 10000.000000), avg(a * 100.00), min(a * -1), max(a * 100) FROM aggt_col GROUP BY b ORDER BY 1,2,3,4,5,6;

SELECT c, sum(a + b - a / 2), count(a / 10000.000000), avg(a - b + b / 2.333), min(a * -1), min(b * 100), max(a * 100 - 99.88) FROM aggt_col GROUP BY c ORDER BY 1,2,3,4,5,6;

SELECT c, sum(a + b - a / 2), count(a / 10000.000000), avg(a - b + b / 2), min(a * -1), min(b * 100), max(a * 100 - 99.88) FROM aggt_col WHERE d / 5 > 0.2 GROUP BY c ORDER BY 1,2,3,4,5,6;

---- NUMERIC TABLE, test numeric functions
create table vec_numeric_4(id int,num numeric(18, 8),str text) with (orientation=column);
COPY vec_numeric_4 FROM stdin DELIMITER as ',' NULL as '' ;
1,2218.952526,qmzpbkaufazdejdzclqdwietzlworntueaynemnhjodtvloulxyxroinwcmxoawhmqeyohmjjbiekeaavwv
2,,aaa
3,2.00000,
4,,bbb
5,-744640.241484,yxsehjxgfqhjlhlcwmnxvwnmoumtkliiehvrmqftlpofkrrvmstfoqeicsqdmbhwcphqxaiimdjajyhleppnyfelnwyvfbjsy
1,326045.788311,nsiuqdmmhcsmpfwyrqmwdbmmkqxlkzdnqntzuosduohqydlzewxwychhssajyjkhtqsemgqmhfyztugucpkslcvcat
2,-84412.909302,zsepamxeujcfwsfomwqwpxcgnptzrlgryuyncqnxfknptqjcmqdowdhyxtqgfjfouphkkeilfgeiuzipjebfnzdzdhjioaifhpy
3,507877.488149,cbpvzydgyitinfxycsqrfparcpchlnmsjbcwkdapmwrqtbzqe
4,-676950.406329,tlqoanhsvngberaousytsoutfzkgufwadxjqxhqehawytjncsm
5,,
6,66009.720181,uzrvzuhshtuaipvivqhwxkpazkfhjooiukixudrvdgoirrzynuyljdbgcmzxqnbjtppnlkqvkizkejbwcfzt
2,478104.384758,vqksuxhqyffufkugzizasiykfvtynhikwuwactncpdprbpgfuimwhhnupevaxtfdmfexlxdxxmvtlmakahxzun
3,537680.716987,zshwmcqxuwpeufvmcynjtqcgbzgttcujyehkeyesrdnpaikitsozbgpbnypibinedjlbtso
4,-722885.021450,ihnkvqcsdokkzbqbddqlllcrxxugfnkybzhrycvlno
5,963449.349791,
6,-937933.609806,ppypifenjhvxhaovaqsuapmjgpcvxospz
\.
analyze vec_numeric_4;

---- operator 
select num, abs(num), acos(num/1000000.0), asin(num/1000000.0), atan(num), ceil(num), cos(num), exp(num/1000000.00) from vec_numeric_4 order by 1;
select num, floor(num), ln(@num), log(@num), log(@num, 11), mod(num, 100), power(num % 10, 5) from vec_numeric_4 order by 1;
select num, round(num), round(num, 2), sign(num), sin(num), sqrt(@num), tan(num), trunc(num) from vec_numeric_4 order by 1;
select num + 1, cbrt(num), cot(num), degrees(num), radians(num), width_bucket(num,@num,@num-1, 10) from vec_numeric_4 order by 1;

---- transform function
select num, num::float4::numeric, num::float8::numeric(18,8) from vec_numeric_4 order by 1;
select num, num::char(20)::numeric(18,2), num::varchar(20)::numeric(18,6), num::bpchar(12)::numeric(18,8), num::text::numeric(18,2) from vec_numeric_4 order by 1;
select num, numtoday(num), to_char(num, '9,999,999.999') from vec_numeric_4 order by 1;
---- AGG function
select id, regr_syy(num-1, num+1), regr_sxy(num-1, num+1), regr_sxx(num-1, num+1), regr_slope(num-1, num+1), regr_r2(num-1, num+1)  from vec_numeric_4 group by id order by 1;
select id, regr_intercept(num-1, num+1), regr_count(num-1, num+1), regr_avgy(num-1, num+1), regr_avgx(num-1, num+1) from vec_numeric_4 group by id order by 1;
select id, every(num::int::bool), covar_samp(num,num), covar_pop(num,num), corr(num,num), bool_or(num::int::bool), bool_and(num::int::bool), bit_or(num), bit_and(num) from vec_numeric_4 group by id order by 1;
---- window function
select id, rank() over (partition by id order by num), row_number() over (partition by id order by num) from vec_numeric_4 order by 1,2,3;
---- conditional expression
select id, greatest(num, num + 10, num * 2), least(num, num + 10, num * 2), nvl(num, 100.00), nullif(num, @num), coalesce(num, 100.00), (case when num>0 then num else -num end) from vec_numeric_4 order by 1,2;
---- between and, inlist not inlist
select num + 1.00 from vec_numeric_4 where num between -100000000000 and 100000000000 order by 1;
select num from vec_numeric_4 where num in (select num from vec_numeric_4) order by 1;

---- DROP SCHEMA
DROP SCHEMA vec_numeric_to_bigintger_1 CASCADE;
