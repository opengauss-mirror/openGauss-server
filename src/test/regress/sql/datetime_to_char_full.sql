-- Date/time to_char regression: extended formats and call compatibility.
-- Coverage:
-- 1. Names and other format fields, time/timetz fields, modifiers, and combined templates.
-- 2. Templates from columns and parameters, long templates, casts, time zones, DST, DateStyle, localization, and collation.
-- 3. Internal template rules for A-mode intervals and original errors for unsupported fields.
-- 1. Separate A-mode database and input values.
set client_min_messages = warning;
drop database if exists dch_to_char_full_a;
reset client_min_messages;
create database dch_to_char_full_a with template template0 encoding 'UTF8'
    lc_collate 'C' lc_ctype 'C' dbcompatibility 'A';
\c dch_to_char_full_a
set timezone = 'UTC';
set datestyle = 'ISO, YMD';
set intervalstyle = 'postgres';
set lc_time = 'C';
set standard_conforming_strings = on;
\pset null '<NULL>'

-- 1.1 Common dates and boundary dates; each row stores values in four native data types.
create table datetime_inputs(id integer primary key, ts timestamp, tz timestamptz, t time, z timetz);
insert into datetime_inputs values
    (1, '2020-02-29 07:08:09.007008', '2020-02-29 07:08:09.007008+00', '07:08:09.007008', '07:08:09.007008+05:45'),
    (2, '2026-01-01 00:00:00', '2026-01-01 00:00:00+00', '00:00:00', '00:00:00-03:30'),
    (3, '2026-12-31 12:59:59.999999', '2026-12-31 12:59:59.999999+00', '12:59:59.999999', '12:59:59.999999+00'),
    (4, '2005-01-01 23:59:59.000001', '2005-01-01 23:59:59.000001+00', '23:59:59.000001', '23:59:59.000001+08'),
    (5, '0001-01-01', '0001-01-01 00:00:00+00', '09:10:11.010100', '09:10:11.010100+00'),
    (6, '9999-12-31', '9999-12-31 00:00:00+00', '11:12:13.100000', '11:12:13.100000+00'),
    (7, '0001-01-01 BC', '0001-01-01 00:00:00+00 BC', '13:14:15.000010', '13:14:15.000010+00'),
    (8, null, null, null, null);

-- 1.2 Names, other format fields, and time fields for time/timetz.
-- Timestamp integer fields are tested in datetime_to_char.sql; together, the files cover 118 field spellings.
create table datetime_formats(id integer primary key, picture text, category text);
insert into datetime_formats values
    (1, 'A.D.', 'other'),
    (2, 'A.M.', 'meridiem'),
    (3, 'AD', 'other'),
    (4, 'AM', 'meridiem'),
    (5, 'B.C.', 'other'),
    (6, 'BC', 'other'),
    (7, 'DAY', 'name'),
    (8, 'DY', 'name'),
    (9, 'Day', 'name'),
    (10, 'Dy', 'name'),
    (11, 'FF1', 'time_number'),
    (12, 'FF2', 'time_number'),
    (13, 'FF3', 'time_number'),
    (14, 'FF4', 'time_number'),
    (15, 'FF5', 'time_number'),
    (16, 'FF6', 'time_number'),
    (17, 'FF', 'time_number'),
    (18, 'FX', 'other'),
    (19, 'HH24', 'time_number'),
    (20, 'HH12', 'time_number'),
    (21, 'HH', 'time_number'),
    (22, 'MI', 'time_number'),
    (23, 'MONTH', 'name'),
    (24, 'MON', 'name'),
    (25, 'MS', 'time_number'),
    (26, 'Month', 'name'),
    (27, 'Mon', 'name'),
    (28, 'P.M.', 'meridiem'),
    (29, 'PM', 'meridiem'),
    (30, 'RM', 'other'),
    (31, 'RRRR', 'other'),
    (32, 'RR', 'other'),
    (33, 'SSSSS', 'time_number'),
    (34, 'SS', 'time_number'),
    (35, 'SYYYY', 'other'),
    (36, 'TZH', 'other'),
    (37, 'TZM', 'other'),
    (38, 'TZ', 'other'),
    (39, 'US', 'time_number'),
    (40, 'X', 'other'),
    (41, 'Y,YYY', 'other'),
    (42, 'a.d.', 'other'),
    (43, 'a.m.', 'meridiem'),
    (44, 'ad', 'other'),
    (45, 'am', 'meridiem'),
    (46, 'b.c.', 'other'),
    (47, 'bc', 'other'),
    (48, 'day', 'name'),
    (49, 'dy', 'name'),
    (50, 'ff1', 'time_number'),
    (51, 'ff2', 'time_number'),
    (52, 'ff3', 'time_number'),
    (53, 'ff4', 'time_number'),
    (54, 'ff5', 'time_number'),
    (55, 'ff6', 'time_number'),
    (56, 'ff', 'time_number'),
    (57, 'fx', 'other'),
    (58, 'hh24', 'time_number'),
    (59, 'hh12', 'time_number'),
    (60, 'hh', 'time_number'),
    (61, 'mi', 'time_number'),
    (62, 'month', 'name'),
    (63, 'mon', 'name'),
    (64, 'ms', 'time_number'),
    (65, 'p.m.', 'meridiem'),
    (66, 'pm', 'meridiem'),
    (67, 'rm', 'other'),
    (68, 'rrrr', 'other'),
    (69, 'rr', 'other'),
    (70, 'sssss', 'time_number'),
    (71, 'ss', 'time_number'),
    (72, 'syyyy', 'other'),
    (73, 'tzh', 'other'),
    (74, 'tzm', 'other'),
    (75, 'tz', 'other'),
    (76, 'us', 'time_number'),
    (77, 'x', 'other'),
    (78, 'y,yyy', 'other');

-- 2. Basic fields: call the existing type overloads directly for each field and input value.
-- 2.1 Names and other non-integer fields for timestamp and timestamptz.
select v.id, f.id as format_id, f.picture,
       '[' || to_char(v.ts, f.picture) || ']' as timestamp_result,
       '[' || to_char(v.tz, f.picture) || ']' as timestamptz_result
from datetime_inputs v cross join datetime_formats f
where f.category <> 'time_number'
order by v.id, f.id;

-- 2.2 Numeric time fields and AM/PM for time/timetz; unsupported date fields are tested separately in section 6.
select v.id, f.id as format_id, f.picture,
       '[' || to_char(v.t, f.picture) || ']' as time_result,
       '[' || to_char(v.z, f.picture) || ']' as timetz_result
from datetime_inputs v cross join datetime_formats f
where f.category in ('time_number', 'meridiem')
order by v.id, f.id;

-- 3. Modifiers and common combinations.
-- 3.1 FM and TM for names; compare letter case and trailing spaces.
select f.id, f.picture,
       '[' || to_char(v.ts, 'FM' || f.picture) || ']' as fm_result,
       '[' || to_char(v.ts, 'TM' || f.picture) || ']' as tm_result
from datetime_inputs v cross join datetime_formats f
where v.id = 1 and f.category = 'name'
order by f.id;

-- 3.2 Common combinations, repeated and adjacent fields, date-field evaluation orders, and quoted literals.
insert into datetime_formats values
    (79, 'YYYY-MM-DD HH24:MI:SS', 'combined'),
    (80, 'yyyy/mm/dd hh24:mi:ss.us', 'combined'),
    (81, 'FMYYYY-FMMM-FMDD FMHH24:FMMI:FMSS', 'combined'),
    (82, 'YYYYMMDDHH24MISS', 'combined'),
    (83, 'SSMSUS', 'combined'),
    (84, 'DDD D WW DAY DY ID DDD D', 'combined'),
    (85, 'D DDD ID DD DDD D', 'combined'),
    (86, 'IYYY-IW-ID IDDD YYYY-DDD', 'combined'),
    (87, 'DDTH-MM-DDth-FMDDTH', 'combined'),
    (88, 'TMMonth TMDay TMMon TMDy', 'combined'),
    (89, '"日期:"YYYY"年"MM"月"DD"日"', 'combined'),
    (90, 'FXYYYY-MM-DD|fmDD|DDSP|DDth', 'combined'),
    (91, 'SSXFF6|SSxFF6', 'combined'),
    (92, 'Y,YYYTH|Y,YYYth', 'combined');
select v.id, f.id as format_id, f.picture,
       '[' || to_char(v.ts, f.picture) || ']' as timestamp_result,
       '[' || to_char(v.tz, f.picture) || ']' as timestamptz_result
from datetime_inputs v cross join datetime_formats f
where f.category = 'combined'
order by v.id, f.id;

-- 3.3 Twelve-hour clock conversion.
-- Convert 1 PM to hour 13.
SELECT to_char(
    to_timestamp('2020-02-29 01:02:03 PM', 'YYYY-MM-DD HH12:MI:SS AM'),
    'YYYY-MM-DD HH24:MI:SS');
-- Convert 12 AM to hour 00.
SELECT to_char(
    to_timestamp('2020-02-29 12:02:03 AM', 'YYYY-MM-DD HH12:MI:SS AM'),
    'YYYY-MM-DD HH24:MI:SS');

-- 4. Template sources and boundaries.
-- 4.1 Column templates: short templates, the 128-byte boundary, and NULLs.
create table stored_formats(id integer primary key, picture text, varchar_picture varchar(5000));
insert into stored_formats values
    (1, 'YYYY-MM-DD', 'YYYY-MM-DD'),
    (2, repeat('-', 123) || 'YYYY', repeat('-', 123) || 'YYYY'),
    (3, repeat('-', 124) || 'YYYY', repeat('-', 124) || 'YYYY'),
    (4, repeat('-', 125) || 'YYYY', repeat('-', 125) || 'YYYY'),
    (5, null, null),
    (6, '', '');
select id, octet_length(picture) as template_bytes,
       length(to_char(timestamp '2020-02-29', picture)) as output_length,
       right(to_char(timestamp '2020-02-29', picture), 10) as text_tail,
       length(to_char(timestamp '2020-02-29', varchar_picture)) as varchar_length,
       right(to_char(timestamp '2020-02-29', varchar_picture), 10) as varchar_tail
from stored_formats order by id;

-- 4.1.1 Real mixed templates exceed 128 bytes; template 1 follows all 72 CASE labels in source order.
create table mixed_long_formats(id integer primary key, picture text);
insert into mixed_long_formats values
    (1, 'A.M. P.M. AM PM a.m. p.m. am pm HH HH12 HH24 MI SS MS FF3 FF1 FF2 FF4 FF5 US FF6 SSSSS tz tzh tzm TZ TZH TZM A.D. B.C. AD BC a.d. b.c. ad bc MONTH Month month MON Mon mon MM DAY Day day DY Dy dy DDD IDDD DD D ID WW IW Q CC Y,YYY YYYY IYYY YYY IYY YY IY Y I RM rm W X J'),
    (2, 'FMYYYY-FMMM-FMDD FMHH24:FMMI:FMSS.US DDTH DDth FMDDTH YYYYTH IYYY-IW-ID IDDD YYYY-DDD D WW W Q CC J TMMonth TMDay TMMon TMDy FMMonth FMDay FMMON FMDY FMYYYYFMMMFMDDHH24MISSFF6'),
    (3, 'A.M. P.M. AM PM a.m. p.m. am pm HH HH12 HH24 MI SS MS FF FF1 FF2 FF3 FF4 FF5 FF6 US SSSSS X hh hh12 hh24 mi ss ms ff ff1 ff2 ff3 ff4 ff5 ff6 us sssss x FMHH FMHH12 FMHH24 FMMI FMSS HH24TH MIth SSth');
select id, octet_length(picture) as template_bytes from mixed_long_formats order by id;

-- Leap day, ISO week-year boundary, and BC date: compare complete timestamp/timestamptz outputs.
select f.id as format_id, v.id as input_id,
       '[' || to_char(v.ts, f.picture) || ']' as timestamp_result,
       '[' || to_char(v.tz, f.picture) || ']' as timestamptz_result
from datetime_inputs v cross join mixed_long_formats f
where f.id in (1, 2) and v.id in (1, 4, 7)
order by f.id, v.id;

-- Time/timetz use only supported fields, including all fractional-second precisions and modifiers.
select v.id,
       '[' || to_char(v.t, f.picture) || ']' as time_result,
       '[' || to_char(v.z, f.picture) || ']' as timetz_result
from datetime_inputs v cross join mixed_long_formats f
where f.id = 3 and v.id in (1, 2, 3)
order by v.id;

-- Fixed expected text comes from the unoptimized version; compare the complete mixed output.
select case when to_char(v.ts, f.picture) = 'A.M. A.M. AM AM a.m. a.m. am am 07 07 07 08 09 007 007 0 00 0070 00700 007008 007008 25689  h m    A.D. A.D. AD AD a.d. a.d. ad ad FEBRUARY  February  february  FEB Feb feb 02 SATURDAY  Saturday  saturday  SAT Sat sat 060 062 29 7 6 09 09 1 21 2,020 2020 2020 020 020 20 20 0 0 II   ii   5 . 2458909'
                 and to_char(v.tz, f.picture) = 'A.M. A.M. AM AM a.m. a.m. am am 07 07 07 08 09 007 007 0 00 0070 00700 007008 007008 25689 utc utch utcm UTC UTC UTC A.D. A.D. AD AD a.d. a.d. ad ad FEBRUARY  February  february  FEB Feb feb 02 SATURDAY  Saturday  saturday  SAT Sat sat 060 062 29 7 6 09 09 1 21 2,020 2020 2020 020 020 20 20 0 0 II   ii   5 . 2458909'
then 'OK' else 'Failed' end as mixed_long_timestamp
from datetime_inputs v cross join mixed_long_formats f
where v.id = 1 and f.id = 1;
select case when to_char(v.t, f.picture) = 'A.M. A.M. AM AM a.m. a.m. am am 07 07 07 08 09 007 007008 0 00 007 0070 00700 007008 007008 25689 . 07 07 07 08 09 007 007008 0 00 007 0070 00700 007008 007008 25689 . 7 7 7 8 9 07TH 08th 09th'
                 and to_char(v.z, f.picture) = 'A.M. A.M. AM AM a.m. a.m. am am 07 07 07 08 09 007 007008 0 00 007 0070 00700 007008 007008 25689 . 07 07 07 08 09 007 007008 0 00 007 0070 00700 007008 007008 25689 . 7 7 7 8 9 07TH 08th 09th'
then 'OK' else 'Failed' end as mixed_long_time
from datetime_inputs v cross join mixed_long_formats f
where v.id = 1 and f.id = 3;

-- 4.2 Read templates through subqueries; change and reuse prepared-statement templates to check results after cache hits.
select to_char(timestamp '2020-02-29', (select picture from stored_formats where id = 1));
prepare datetime_format(timestamp, text) as select to_char($1, $2);
execute datetime_format('2020-02-29', 'D DDD');
execute datetime_format('2020-03-01', 'D DDD');
execute datetime_format('2020-02-29', 'DDD D');
execute datetime_format('2020-02-29', 'YYYY-MM-DD');
deallocate datetime_format;

-- 4.3 Casts and expression results use the existing formatting entry points.
select to_char(date '2020-02-29', 'YYYY-MM-DD'),
       to_char(smalldatetime '2020-02-29 07:08:00', 'YYYY-MM-DD HH24:MI:SS'),
       to_char(timestamp '2020-03-01' - interval '1 microsecond', 'YYYY-MM-DD HH24:MI:SS.US');
select to_char(timestamp '9999-12-31' + interval '1 day', 'YYYY YYY YY Y CC Y,YYY');

-- 5. GUC settings: query after SET, then RESET.
-- 5.1 Time zones affect timestamptz; check date boundaries and DST transitions.
set timezone = 'PRC';
select id, to_char(ts, 'YYYY-MM-DD HH24:MI:SS D DDD'),
       to_char(tz, 'YYYY-MM-DD HH24:MI:SS D DDD TZ')
from datetime_inputs where id <= 4 order by id;
reset timezone;
set timezone = 'America/New_York';
select to_char(timestamptz '2020-03-08 06:59:59+00', 'YYYY-MM-DD HH24:MI:SS TZ'),
       to_char(timestamptz '2020-03-08 07:00:00+00', 'YYYY-MM-DD HH24:MI:SS TZ'),
       to_char(timestamptz '2020-11-01 05:30:00+00', 'YYYY-MM-DD HH24:MI:SS TZ'),
       to_char(timestamptz '2020-11-01 06:30:00+00', 'YYYY-MM-DD HH24:MI:SS TZ');
reset timezone;

-- 5.2 DateStyle changes default output; explicit templates retain their own format.
set datestyle = 'SQL, DMY';
select id, to_char(ts), to_char(ts, 'YYYY-MM-DD HH24:MI:SS')
from datetime_inputs where id <= 4 order by id;
reset datestyle;

-- 5.3 Use portable C/POSIX values for lc_time to check cache invalidation and reloading.
set lc_time = 'POSIX';
select to_char(timestamp '2020-02-29', 'TMMonth TMDay TMMon TMDy');
reset lc_time;
set lc_time = 'C';
select to_char(timestamp '2020-02-29', 'TMMonth TMDay TMMon TMDy');
select to_char(timestamp '2020-02-29', 'TMMonth TMDay' collate "C");
reset lc_time;

-- 6. A-mode intervals and error cases.
-- 6.1 A mode selects internal templates by interval contents; check positive, negative, zero, and two- to four-digit year values.
create table interval_inputs(id integer primary key, value interval);
insert into interval_inputs values
    (1, interval '0'), (2, interval '7 hours'), (3, interval '-7 hours'),
    (4, interval '7 years 2 months'), (5, interval '-7 years -2 months'),
    (6, interval '123 years'), (7, interval '1234 years'), (8, interval '-1234 years'),
    (9, interval '-123 days -7 hours -8 minutes -9.007008 seconds');
select id, to_char(value, 'YYYY-MM-DD HH24:MI:SS.US'), to_char(value, 'HH24')
from interval_inputs order by id;
-- Long user templates still follow the original A-mode interval template replacement.
select v.id, octet_length(f.picture) as template_bytes, to_char(v.value, f.picture)
from interval_inputs v cross join mixed_long_formats f
where f.id = 1 and v.id in (2, 3, 4)
order by v.id;
set intervalstyle = 'iso_8601';
select id, to_char(value, 'YYYY-MM-DD HH24:MI:SS.US'), to_char(value, 'HH24')
from interval_inputs order by id;
reset intervalstyle;

-- 6.2 Expected error: A mode rejects intervals that combine year/month and day/time components.
select to_char(interval '1 year 1 day', 'YYYY DD');
-- Expected errors: time and timetz do not support weekday, era, or time zone format fields.
select to_char(time '07:08:09', 'DAY');
select to_char(timetz '07:08:09+00', 'D');
select to_char(time '07:08:09', 'AD');
select to_char(timetz '07:08:09+00', 'TZ');

-- 7. Non-A-mode cases run last: check long interval templates and unsupported fields in PG mode.
\c regression
drop database dch_to_char_full_a;
set client_min_messages = warning;
drop database if exists dch_to_char_full_pg;
reset client_min_messages;
create database dch_to_char_full_pg with template template0 encoding 'UTF8'
    lc_collate 'C' lc_ctype 'C' dbcompatibility 'PG';
\c dch_to_char_full_pg
set intervalstyle = 'postgres';
set lc_time = 'C';
\pset null '<NULL>'

-- 7.1 Long numeric templates for positive, negative, and zero intervals; no A-mode template replacement.
create table long_interval_inputs(id integer primary key, value interval);
insert into long_interval_inputs values
    (1, interval '7 years 2 months 123 days 27 hours 8 minutes 9.007008 seconds'),
    (2, interval '-7 years -2 months -123 days -27 hours -8 minutes -9.007008 seconds'),
    (3, interval '0');
create table long_interval_formats(picture text);
insert into long_interval_formats values
    ('YYYY YYY YY Y CC MM DD HH HH12 HH24 MI SS SSSSS MS US FF FF1 FF2 FF3 FF4 FF5 FF6 FMYYYY FMMM FMDD FMHH FMHH12 FMHH24 FMMI FMSS yyyy yyy yy y cc mm dd hh hh12 hh24 mi ss sssss ms us ff ff1 ff2 ff3 ff4 ff5 ff6');
select v.id, octet_length(f.picture) as template_bytes,
       '[' || to_char(v.value, f.picture) || ']' as interval_result
from long_interval_inputs v cross join long_interval_formats f
order by v.id;

select case when to_char(v.value, f.picture) = '-007 -07 -7 -7 00 -2 -123 -3 -3 -27 -8 -9 -97689 -07 -07008 -07008 0 00 -07 -070 -0700 -07008 -7 -2 -123 -3 -3 -27 -8 -9 -007 -07 -7 -7 00 -2 -123 -3 -3 -27 -8 -9 -97689 -07 -07008 -07008 0 00 -07 -070 -0700 -07008'
then 'OK' else 'Failed' end as mixed_long_negative_interval
from long_interval_inputs v cross join long_interval_formats f
where v.id = 2;

-- 7.2 Expected errors: intervals do not support weekday numbers or time zone fields.
select to_char(interval '1 day', 'D');
select to_char(interval '1 day', 'ID');
select to_char(interval '1 day', 'TZ');

\c regression
drop database dch_to_char_full_pg;
