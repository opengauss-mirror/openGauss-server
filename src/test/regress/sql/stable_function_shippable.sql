set DateStyle to ISO, MDY;
set TimeZone to 'UTC';
set IntervalStyle to postgres;
set lc_monetary to 'en_US.UTF-8';
--
--- abstime
--
--prepare
create table test(a int , b abstime);

--test abstimein
explain (costs off, verbose on) select * from test where b < abstimein('12-25-2001');
--test abstimeout
explain (costs off, verbose on) select abstimeout(b) from test;

--clean up
drop table test;


--
--- reltime
--
--prepare
create table test(a int , b reltime);

--test reltimein
explain (costs off, verbose on) select * from test where b < reltimein('@ 14 seconds ago');
--test reltimeout
explain (costs off, verbose on) select reltimeout(b) from test;

--clean up
drop table test;


--
--- tinterval
--
--prepare
create table test(a int , b tinterval);

--test reltimein
explain (costs off, verbose on) select * from test where b < tintervalin('["Aug 15 14:23:19 1980" "Sep 16 14:23:19 1990"]');
--test reltimeout
explain (costs off, verbose on) select tintervalout(b) from test;

--clean up
drop table test;


--
--- money
--
--prepare
create table test(a int , b money);

--test reltimein
explain (costs off, verbose on) select * from test where b < cash_in('$12.123');
--test reltimeout
explain (costs off, verbose on) select cash_out(b) from test;

--clean up
drop table test;


--
---- money & figure
--
--prepare
create table test(a int , b money);

--test money(numeric_cash)
explain (costs off, verbose on) select * from test where b < money('12.123'::numeric);
--test numeric(cash_numeric)
explain (costs off, verbose on) select * from test where b::numeric <> 12.23;
--test money(int4_cash)
explain (costs off, verbose on) select * from test where b < money('12'::int4);
--test money(int8_cash)
explain (costs off, verbose on) select * from test where b < money('12'::int8);

--clean up
drop table test;


--
--- date
--
--prepare
create table test(a int , b date);

--test date_in
explain (costs off, verbose on) select * from test where b < date_in('12-25-2001');
--test date_out
explain (costs off, verbose on) select date_out(date_in('12-25-2001')) from test;

--clean up
drop table test;


--
--- time
--
--prepare
create table test(a int , b time);

--test time_in
explain (costs off, verbose on) select * from test where b < time_in('12:25:55', 0 , -1);
--test time_out
explain (costs off, verbose on) select time_out(b) from test;

--clean up
drop table test;


--
--- timetz
--
--prepare
create table test(a int , b timetz);

--test timezt_in
explain (costs off, verbose on) select * from test where b < timetz_in('12:25:55 + 02', 0 , -1);
--test timezt_out
explain (costs off, verbose on) select timetz_out(b) from test;

--clean up
drop table test;


--
--- timestamptz
--
--prepare
create table test(a int , b timestamptz);

--test timestamptz_in
explain (costs off, verbose on) select * from test where b < timestamptz_in('2015-03-26 14:51:45.489334+00', 0 , -1);
--test timestamptz_out
explain (costs off, verbose on) select timestamptz_out(b) from test;

--clean up
drop table test;


--
--- timestamp
--
--prepare
create table test(a int , b timestamp);

--test timestamp_in
explain (costs off, verbose on) select * from test where b < timestamp_in('2015-03-26 14:51:45.489334', 0 , -1);
--test timestamp_out
explain (costs off, verbose on) select timestamp_out(b) from test;

--clean up
drop table test;


--
--- interval
--
--prepare
create table test(a int , b interval);

--test interval_in
explain (costs off, verbose on) select * from test where b < interval_in('3 4:05:06.7', 0 , -1);
--test interval_out
explain (costs off, verbose on) select interval_out(b) from test;

--clean up
drop table test;


--
--- smalldatetime
--
--prepare
create table test(a int , b smalldatetime);

--test smalldatetime_in
explain (costs off, verbose on) select * from test where b < smalldatetime_in('2015-03-26 14:51:45.489334', 0 , -1);
--test smalldatetime_out
explain (costs off, verbose on) select smalldatetime_out(b) from test;

--clean up
drop table test;


--
----extract field from XXXX
--
--prepare
create table test(a int , b interval, c timestamptz, d abstime, e reltime);

--extract field from timestamp with time zone
explain (costs off, verbose on) select * from test where date_part('year', c) > 1009;
--extract field from abstime
explain (costs off, verbose on) select * from test where date_part('year', d) > 1009;
--extract field from reltime
explain (costs off, verbose on) select * from test where date_part('year', e) > 1009;

--clean up
drop table test;


--
----convert: abstime timestamp timestamptz time date
--
--prepare
create table test(a int, b date, c time, d timetz, e timestamp, f timestamptz, g abstime);

--convert date to timestamp with time zone
explain (costs off, verbose on) select * from test where timestamptz(date_in('12-25-2001')) > f;
--convert date and time to timestamp with time zone
explain (costs off, verbose on) select * from test where timestamptz(date_in('12-25-2001'), c) > f;
--convert timestamp with time zone to date
explain (costs off, verbose on) select * from test where e > date_in('12-25-2001');
--convert abstime to date
explain (costs off, verbose on) select * from test where g > date_in('12-25-2001');
--convert abstime to time
explain (costs off, verbose on) select * from test where g::time>c;
--convert timestamp with time zone to time with time zone
explain (costs off, verbose on) select * from test where f::timetz>d;
--convert text to timestamp with time zone
explain (costs off, verbose on) select * from test where to_timestamp('23-12-2012', 'DD-MM-YYYY HH:MI:SS.FF AM')>e;
--to_date
explain (costs off, verbose on) select * from test where to_date('23-12-2012', 'DD-MM-YYYY HH:MI:SS.FF AM')>e;
--convert timestamp with time zone to time
explain (costs off, verbose on) select * from test where f::time>c;
--convert abstime to timestamp
explain (costs off, verbose on) select * from test where g::timestamp>e;
--convert timestamp with time zone to timestamp
explain (costs off, verbose on) select * from test where f::timestamp>e;
--convert timestamp to timestamp with time zone
explain (costs off, verbose on) select * from test where e::timestamptz>f;
--convert timestamp to abstime
explain (costs off, verbose on) select * from test where e::abstime>g;
--convert time to time with time zone
explain (costs off, verbose on) select * from test where c::timetz>d;

--clean up
drop table test;


--
----operation: interval reltime timestamptz
--
--prepare
create table test(a int, b interval, c timestamptz, d reltime, e timestamp);

--timestamptz plus interval
explain (costs off, verbose on) select * from test where timestamptz_pl_interval(c, b) > c;
--timestamptz minus interval
explain (costs off, verbose on) select * from test where timestamptz_mi_interval(c, b) > c;
--interval plus timestamptz
explain (costs off, verbose on) select * from test where interval_pl_timestamptz(b, c) > c;

--crosstype operations for date vs. timestamptz lt
explain (costs off, verbose on) select * from test where date_lt_timestamptz(date_in('12-25-2001'), c);
--crosstype operations for date vs. timestamptz le
explain (costs off, verbose on) select * from test where date_le_timestamptz(date_in('12-25-2001'), c);
--crosstype operations for date vs. timestamptz eq
explain (costs off, verbose on) select * from test where date_eq_timestamptz(date_in('12-25-2001'), c);
--crosstype operations for date vs. timestamptz gt
explain (costs off, verbose on) select * from test where date_gt_timestamptz(date_in('12-25-2001'), c);
--crosstype operations for date vs. timestamptz ge
explain (costs off, verbose on) select * from test where date_ge_timestamptz(date_in('12-25-2001'), c);
--crosstype operations for date vs. timestamptz ne
explain (costs off, verbose on) select * from test where date_ne_timestamptz(date_in('12-25-2001'), c);
--crosstype operations for date vs. timestamptz cmp
explain (costs off, verbose on) select * from test where date_cmp_timestamptz(date_in('12-25-2001'), c);

--crosstype operations for timestamptz vs. date lt
explain (costs off, verbose on) select * from test where timestamptz_lt_date(c, date_in('12-25-2001'));
--crosstype operations for timestamptz vs. date le            ,
explain (costs off, verbose on) select * from test where timestamptz_le_date(c, date_in('12-25-2001'));
--crosstype operations for timestamptz vs. date eq            ,
explain (costs off, verbose on) select * from test where timestamptz_eq_date(c, date_in('12-25-2001'));
--crosstype operations for timestamptz vs. date gt            ,
explain (costs off, verbose on) select * from test where timestamptz_gt_date(c, date_in('12-25-2001'));
--crosstype operations for timestamptz vs. date ge            ,
explain (costs off, verbose on) select * from test where timestamptz_ge_date(c, date_in('12-25-2001'));
--crosstype operations for timestamptz vs. date ne            ,
explain (costs off, verbose on) select * from test where timestamptz_ne_date(c, date_in('12-25-2001'));
--crosstype operations for timestamptz vs. date cmp
explain (costs off, verbose on) select * from test where timestamptz_cmp_date(c, date_in('12-25-2001'));

--crosstype operations for timestamp vs. timestamptz lt
explain (costs off, verbose on) select * from test where timestamp_lt_timestamptz(e, c);
--crosstype operations for timestamp vs. timestamptz le
explain (costs off, verbose on) select * from test where timestamp_le_timestamptz(e, c);
--crosstype operations for timestamp vs. timestamptz eq
explain (costs off, verbose on) select * from test where timestamp_eq_timestamptz(e, c);
--crosstype operations for timestamp vs. timestamptz gt
explain (costs off, verbose on) select * from test where timestamp_gt_timestamptz(e, c);
--crosstype operations for timestamp vs. timestamptz ge
explain (costs off, verbose on) select * from test where timestamp_ge_timestamptz(e, c);
--crosstype operations for timestamp vs. timestamptz ne
explain (costs off, verbose on) select * from test where timestamp_ne_timestamptz(e, c);
--crosstype operations for timestamp vs. timestamptz cmp
explain (costs off, verbose on) select * from test where timestamp_cmp_timestamptz(e, c);

--crosstype operations for timestamptz vs. timestamp lt
explain (costs off, verbose on) select * from test where timestamptz_lt_timestamp(c, e);
--crosstype operations for timestamptz vs. timestamp le
explain (costs off, verbose on) select * from test where timestamptz_le_timestamp(c, e);
--crosstype operations for timestamptz vs. timestamp eq
explain (costs off, verbose on) select * from test where timestamptz_eq_timestamp(c, e);
--crosstype operations for timestamptz vs. timestamp gt
explain (costs off, verbose on) select * from test where timestamptz_gt_timestamp(c, e);
--crosstype operations for timestamptz vs. timestamp ge
explain (costs off, verbose on) select * from test where timestamptz_ge_timestamp(c, e);
--crosstype operations for timestamptz vs. timestamp ne
explain (costs off, verbose on) select * from test where timestamptz_ne_timestamp(c, e);
--crosstype operations for timestamptz vs. timestamp cmp
explain (costs off, verbose on) select * from test where timestamptz_cmp_timestamp(c, e);

--clean up
drop table test;

--
----truncate: XXX to XXX
--
--prepare
create table test(a int, b timestamptz);

--truncate timestamp with time zone to specified units
explain (costs off, verbose on) select * from test where date_trunc('year', b) > 2015;

--clean up
drop table test;
