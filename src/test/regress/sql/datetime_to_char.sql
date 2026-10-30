-- Date/time to_char regression: core behavior and integer output.
-- Coverage:
-- 1. Common output, zero padding, signs, widths, ordinal suffixes, weekdays, day of year, and date boundaries.
-- 2. Individual timestamp/timestamptz integer fields, plus positive/negative intervals and values exceeding field widths in PG mode.
-- 3. NULLs, short-template boundaries, basic time zone and DateStyle behavior, and three-argument NLS calls.
-- 1. Separate A-mode database.
set client_min_messages = warning;
drop database if exists dch_to_char_core_a;
reset client_min_messages;
create database dch_to_char_core_a with template template0 encoding 'UTF8'
    lc_collate 'C' lc_ctype 'C' dbcompatibility 'A';
\c dch_to_char_core_a
set timezone = 'UTC';
set datestyle = 'ISO, YMD';
set intervalstyle = 'postgres';
set lc_time = 'C';
\pset null '<NULL>'

-- 2. Common formats and integer output.
-- 2.1 Five input types; A-mode intervals use the existing internal templates.
select case when
    to_char(timestamp '2020-02-29 07:08:09', 'YYYY-MM-DD HH24:MI:SS') = '2020-02-29 07:08:09' and
    to_char(timestamptz '2020-02-29 07:08:09+08', 'YYYY-MM-DD HH24:MI:SS') = '2020-02-28 23:08:09' and
    to_char(time '07:08:09', 'HH24:MI:SS') = '07:08:09' and
    to_char(timetz '07:08:09+08', 'HH24:MI:SS') = '07:08:09' and
    to_char(interval '-7 hours', 'HH24') = '-00 07:00:00'
then 'OK' else 'Failed' end as common_types;

-- 2.2 With and without zero padding; values exceeding the minimum width are not truncated.
select case when
    to_char(timestamp '0001-01-09 07:08:09', 'YYYY-MM-DD HH24:MI:SS') = '0001-01-09 07:08:09' and
    to_char(timestamp '0001-01-09 07:08:09', 'FMYYYY-FMMM-FMDD FMHH24:FMMI:FMSS') = '1-1-9 7:8:9' and
    to_char(timestamp '9999-12-31' + interval '1 day', 'YYYY-MM-DD') = '10000-01-01' and
    to_char(timestamp '2020-02-29 07:08:09', 'YYYYMMDDHH24MISS') = '20200229070809'
then 'OK' else 'Failed' end as integer_padding;

-- 2.3 Fractional-second precisions; FM does not suppress zero padding for fractional seconds.
select case when
    to_char(timestamp '2020-02-29 07:08:09.007008', 'MS US FF FF1 FF2 FF3 FF4 FF5 FF6') = '007 007008 007008 0 00 007 0070 00700 007008' and
    to_char(time '07:08:09.007008', 'FMMS FMUS FMFF2 FMFF4') = '007 007008 00 0070' and
    to_char(time '07:08:09.999999', 'SS.MS.US') = '09.999.999999'
then 'OK' else 'Failed' end as fractional_seconds;

-- 2.4 Ordinal suffixes and adjacent fields; suffixes must not overwrite the next field.
select case when
    to_char(timestamp '2020-02-01', 'DDTH-DDth-FMDDTH') = '01ST-01st-1ST' and
    to_char(timestamp '2020-02-11', 'DDTH DDth') = '11TH 11th' and
    to_char(timestamp '2020-02-22', 'DDTHMMDD') = '22ND0222' and
    to_char(time '00:00:00', 'HH HH12 AM A.M.') = '12 12 AM A.M.' and
    to_char(time '12:00:00', 'HH HH12 PM P.M.') = '12 12 PM P.M.'
then 'OK' else 'Failed' end as suffix_and_adjacent_fields;

-- 3. Weekdays, day of year, and date boundaries.
-- 3.1 Compute fields alone, weekday before day of year, the reverse order, and repeated fields.
select case when
    to_char(timestamp '2020-02-29', 'D') = '7' and
    to_char(timestamp '2020-02-29', 'DDD') = '060' and
    to_char(timestamp '2020-02-29', 'D DDD D DDD WW') = '7 060 7 060 09' and
    to_char(timestamp '2020-02-29', 'DDD D DDD D') = '060 7 060 7' and
    to_char(timestamp '2020-03-01', 'D ID DDD') = '1 7 061'
then 'OK' else 'Failed' end as weekday_and_yearday;

-- 3.2 Leap years, non-leap century years, ISO week-year boundaries, BC dates, and AD year 1.
select case when
    to_char(timestamp '1900-03-01', 'YYYY-DDD') = '1900-060' and
    to_char(timestamp '2000-02-29', 'YYYY-DDD') = '2000-060' and
    to_char(timestamp '2005-01-01', 'YYYY IYYY IW ID IDDD') = '2005 2004 53 6 370' and
    to_char(timestamp '0001-01-01 BC', 'YYYY BC') = '0001 BC' and
    to_char(timestamp '0001-01-01', 'YYYY AD') = '0001 AD'
then 'OK' else 'Failed' end as calendar_boundaries;

-- 4. Text lengths, template lengths, and NULLs.
-- 4.1 Brackets expose trailing padding in names; TM uses the lc_time cache.
select case when
    '[' || to_char(timestamp '2020-02-29', 'MONTH') || ']' = '[FEBRUARY ]' and
    '[' || to_char(timestamp '2020-02-29', 'DAY') || ']' = '[SATURDAY ]' and
    to_char(timestamp '2020-02-29', 'FMMonth FMDay') = 'February Saturday' and
    to_char(timestamp '2020-02-29', 'TMMonth TMDay') = 'February Saturday' and
    to_char(timestamp '2020-02-29', 'AD A.D. BC B.C. RM') = 'AD A.D. AD A.D. II  '
then 'OK' else 'Failed' end as text_padding;

-- 4.2 Templates of 127, 128, and 129 bytes cover the stack-buffer boundary and the long-template path.
select case when
    to_char(timestamp '2020-02-29', repeat('-', 123) || 'YYYY') = repeat('-', 123) || '2020' and
    to_char(timestamp '2020-02-29', repeat('-', 124) || 'YYYY') = repeat('-', 124) || '2020' and
    to_char(timestamp '2020-02-29', repeat('-', 125) || 'YYYY') = repeat('-', 125) || '2020' and
    to_char(timestamp '2020-02-29', '"日期:"YYYY-MM-DD') = '日期:2020-02-29'
then 'OK' else 'Failed' end as template_boundaries;

-- 4.3 NULL inputs, NULL templates, empty templates, and infinite timestamps.
select case when
    to_char(null::timestamp, 'YYYY') is null and
    to_char(null::timestamptz, 'YYYY') is null and
    to_char(null::time, 'HH24') is null and
    to_char(null::timetz, 'HH24') is null and
    to_char(null::interval, 'HH24') is null and
    to_char(timestamp '2020-02-29', null::text) is null and
    to_char(timestamp '2020-02-29', '') is null and
    to_char(timestamp 'infinity', 'YYYY') is null and
    to_char(timestamp '-infinity', 'YYYY') is null
then 'OK' else 'Failed' end as null_and_infinity;

-- 5. Integer-field comparisons across inputs.
-- 5.1 Dates and boundary values; input IDs match those in datetime_to_char_full.sql.
create table datetime_inputs(id integer primary key, ts timestamp, tz timestamptz);
insert into datetime_inputs values
    (1, '2020-02-29 07:08:09.007008', '2020-02-29 07:08:09.007008+00'),
    (2, '2026-01-01 00:00:00', '2026-01-01 00:00:00+00'),
    (3, '2026-12-31 12:59:59.999999', '2026-12-31 12:59:59.999999+00'),
    (4, '2005-01-01 23:59:59.000001', '2005-01-01 23:59:59.000001+00'),
    (5, '0001-01-01', '0001-01-01 00:00:00+00'),
    (6, '9999-12-31', '9999-12-31 00:00:00+00'),
    (7, '0001-01-01 BC', '0001-01-01 00:00:00+00 BC'),
    (8, null, null);

-- 5.2 All supported case variants of integer fields; check each field with both timestamp overloads.
create table datetime_formats(id integer primary key, picture text);
insert into datetime_formats values
    (1, 'CC'),
    (2, 'DDD'),
    (3, 'DD'),
    (4, 'D'),
    (5, 'FF1'),
    (6, 'FF2'),
    (7, 'FF3'),
    (8, 'FF4'),
    (9, 'FF5'),
    (10, 'FF6'),
    (11, 'FF'),
    (12, 'HH24'),
    (13, 'HH12'),
    (14, 'HH'),
    (15, 'IDDD'),
    (16, 'ID'),
    (17, 'IW'),
    (18, 'IYYY'),
    (19, 'IYY'),
    (20, 'IY'),
    (21, 'I'),
    (22, 'J'),
    (23, 'MI'),
    (24, 'MM'),
    (25, 'MS'),
    (26, 'Q'),
    (27, 'SSSSS'),
    (28, 'SS'),
    (29, 'US'),
    (30, 'WW'),
    (31, 'W'),
    (32, 'YYYY'),
    (33, 'YYY'),
    (34, 'YY'),
    (35, 'Y'),
    (36, 'cc'),
    (37, 'ddd'),
    (38, 'dd'),
    (39, 'd'),
    (40, 'ff1'),
    (41, 'ff2'),
    (42, 'ff3'),
    (43, 'ff4'),
    (44, 'ff5'),
    (45, 'ff6'),
    (46, 'ff'),
    (47, 'hh24'),
    (48, 'hh12'),
    (49, 'hh'),
    (50, 'iddd'),
    (51, 'id'),
    (52, 'iw'),
    (53, 'iyyy'),
    (54, 'iyy'),
    (55, 'iy'),
    (56, 'i'),
    (57, 'j'),
    (58, 'mi'),
    (59, 'mm'),
    (60, 'ms'),
    (61, 'q'),
    (62, 'sssss'),
    (63, 'ss'),
    (64, 'us'),
    (65, 'ww'),
    (66, 'w'),
    (67, 'yyyy'),
    (68, 'yyy'),
    (69, 'yy'),
    (70, 'y');

select v.id, f.id as format_id, f.picture,
       '[' || to_char(v.ts, f.picture) || ']' as timestamp_result,
       '[' || to_char(v.tz, f.picture) || ']' as timestamptz_result
from datetime_inputs v cross join datetime_formats f
order by v.id, f.id;

-- 5.3 Check FM, TH, th, and combinations of FM and suffixes for each field.
select f.id, f.picture,
       to_char(v.ts, 'FM' || f.picture) as fm_result,
       to_char(v.ts, f.picture || 'TH') as upper_suffix,
       to_char(v.ts, f.picture || 'th') as lower_suffix,
       to_char(v.ts, 'FM' || f.picture || 'TH') as fm_suffix
from datetime_inputs v cross join datetime_formats f
where v.id = 1
order by f.id;

-- 6. Configuration and call arguments.
-- 6.1 Time zones affect the date; explicit templates retain their own format.
set timezone = 'PRC';
select case when
    to_char(timestamptz '2020-02-29 20:08:09+00', 'YYYY-MM-DD HH24:MI:SS D DDD') = '2020-03-01 04:08:09 1 061' and
    to_char(timestamp '2020-02-29 20:08:09', 'YYYY-MM-DD HH24:MI:SS D DDD') = '2020-02-29 20:08:09 7 060'
then 'OK' else 'Failed' end as timezone_change;
reset timezone;

-- 6.2 DateStyle does not change explicit templates.
set datestyle = 'SQL, DMY';
select case when
    to_char(timestamp '2020-02-29 07:08:09', 'YYYY-MM-DD HH24:MI:SS') = '2020-02-29 07:08:09'
then 'OK' else 'Failed' end as explicit_template_datestyle;
reset datestyle;

-- 6.3 Three-argument calls and NLS parameters; invalid languages produce the original errors directly.
set timezone = 'UTC';
select to_char(timestamp '2020-02-29', 'YYYY-MM-DD', 'NLS_DATE_LANGUAGE=American'),
       to_char(timestamptz '2020-02-29 00:00:00+00', 'YYYY-MM-DD', 'NLS_DATE_LANGUAGE=English'),
       to_char(interval '-7 hours', 'HH24', 'NLS_DATE_LANGUAGE=English');

select to_char(timestamp '2020-02-29', 'YYYY', 'NLS_DATE_LANGUAGE=French');
select to_char(timestamptz '2020-02-29 00:00:00+00', 'YYYY', 'NLS_DATE_LANGUAGE=French');
select to_char(interval '7 hours', 'HH24', 'NLS_DATE_LANGUAGE=French');
reset timezone;

-- 7. PG-mode negative intervals: check signed integer output after the A-mode cases.
\c regression
drop database dch_to_char_core_a;
set client_min_messages = warning;
drop database if exists dch_to_char_core_pg;
reset client_min_messages;
create database dch_to_char_core_pg with template template0 encoding 'UTF8'
    lc_collate 'C' lc_ctype 'C' dbcompatibility 'PG';
\c dch_to_char_core_pg
set intervalstyle = 'postgres';

select case when
    to_char(interval '-7 hours', 'HH24') = '-7' and
    to_char(interval '-7 years', 'YYYY') = '-007' and
    to_char(interval '-7 years', 'FMYYYY') = '-7' and
    to_char(interval '-123 days', 'DD') = '-123' and
    to_char(interval '-7 minutes -8 seconds', 'MI:SS') = '-7:-8' and
    to_char(interval '-0.007008 seconds', 'MS US') = '-07 -07008' and
    to_char(interval '-7 hours', 'HH24TH') = '-7TH'
then 'OK' else 'Failed' end as signed_integer_output;

-- 7.1 More interval values: zero, positive/negative values, values exceeding field widths, whole years, and signed months.
create table interval_inputs(id integer primary key, value interval);
insert into interval_inputs values
    (1, interval '0'),
    (2, interval '7 years 2 months 3 days 4 hours 5 minutes 6.007008 seconds'),
    (3, interval '-7 years -2 months -3 days -4 hours -5 minutes -6.007008 seconds'),
    (4, interval '12345 years 123 days 123 hours'),
    (5, interval '-12345 years -123 days -123 hours'),
    (6, interval '12 months'), (7, interval '-12 months'),
    (8, interval '1 month'), (9, interval '-1 month'), (10, null);
create table interval_formats(id integer primary key, picture text);
insert into interval_formats values
    (1, 'YYYY YYY YY Y CC'), (2, 'FMYYYY FMYYY FMYY FMY FMCC'),
    (3, 'MM DD HH HH12 HH24 MI SS SSSSS'), (4, 'FMMM FMDD FMHH FMHH12 FMHH24 FMMI FMSS'),
    (5, 'MS US FF FF1 FF2 FF3 FF4 FF5 FF6'), (6, 'FMMS FMUS FMFF1 FMFF2 FMFF3 FMFF4 FMFF5 FMFF6'),
    (7, 'DDD WW W Q'), (8, 'RM rm FMRM'), (9, 'IYYY IYY IY I IW IDDD J'),
    (10, 'YYYYMMDDHH24MISS'), (11, 'SSMSUS'), (12, 'SSXFF6|SSxFF6');
select v.id, f.id as format_id, f.picture, '[' || to_char(v.value, f.picture) || ']' as result
from interval_inputs v cross join interval_formats f order by v.id, f.id;

-- 7.2 Ordinal suffixes on negative values and adjacent fields.
select to_char(interval '-7 years', 'YYYYTH|FMYYYYth|YYYY'),
       to_char(interval '-0.007008 seconds', 'USTH|US');

-- 7.3 Minimum and maximum 32-bit integers: signs, complete digits, ordinal suffixes, and adjacent fields.
select case when
    to_char(interval '-2147483648 days', 'DD|FMDD') = '-2147483648|-2147483648' and
    to_char(interval '-2147483648 days', 'DDTH|DDth|DDMI') = '-2147483648TH|-2147483648th|-214748364800'
then 'OK' else 'Failed' end as int32_min_output;

select case when
    to_char(interval '2147483647 days', 'DD|FMDD') = '2147483647|2147483647' and
    to_char(interval '2147483647 days', 'DDTH|DDth|DDMI') = '2147483647TH|2147483647th|214748364700'
then 'OK' else 'Failed' end as int32_max_output;

-- 7.4 Expected errors: intervals do not support era fields or weekday names.
select to_char(interval '1 day', 'AD');
select to_char(interval '1 day', 'DAY');
select to_char(interval '1 day', 'DY');

\c regression
drop database dch_to_char_core_pg;
