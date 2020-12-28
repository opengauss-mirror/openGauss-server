create table dcs_cpu(
  idle real check(idle > 0),
  vcpu_num int,
  node text,
  scope_name text,
  server_ip text not null,
  iowait real,
  time_string timestamp
);
insert  into dcs_cpu VALUES(1.0,1,'node_a','scope_a','1.1.1.1',1.0,'2019-07-12 00:10:10');
insert  into dcs_cpu VALUES(2.0,2,'node_b','scope_a','1.1.1.2',2.0,'2019-07-12 00:12:10');
insert  into dcs_cpu VALUES(3.0,3,'node_c','scope_b','1.1.1.3',3.0,'2019-07-12 00:13:10');
select  time_window(interval '1 min',time_string),server_ip from dcs_cpu order by server_ip;
select  time_fill(interval '1 min',time_string,'2019-07-12 00:09:00','2019-07-12 00:14:00'),avg(idle) from dcs_cpu group by time_fill order by time_fill;
select  time_fill(interval '1 min',time_string,'2019-07-12 00:09:00','2019-07-12 00:14:00'), fill_last(avg(idle)) from dcs_cpu group by time_fill order by time_fill;
select  first(array_agg(idle),array_agg(time_string)), sum(idle) from dcs_cpu group by scope_name order by scope_name;
select  last(array_agg(idle),array_agg(time_string)), sum(idle) from dcs_cpu group by scope_name order by scope_name;
drop table dcs_cpu;

---
--- time window
---
\set ON_ERROR_STOP 1
SELECT time_window(interval '1 microsecond', '2019-07-12 11:09:01.001'::timestamptz);

SELECT time_window(interval '1 millisecond', '2019-07-12 11:09:01.0000001'::timestamptz);

SELECT time_window(interval '1 second', '2019-07-12 11:09:01.001'::timestamptz);

SELECT time_window(interval '1 min', '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(interval '1 hour', '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(interval '1 day', '2019-07-12 11:09:01'::timestamptz);   ------------------------------------------

SELECT time_window(interval '1 week', '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(interval '1 microsecond', '2019-07-12 11:09:01.001'::timestamp);

SELECT time_window(interval '1 millisecond', '2019-07-12 11:09:01.0000001'::timestamp);

SELECT time_window(interval '1 second', '2019-07-12 11:09:01.001'::timestamp);

SELECT time_window(interval '1 min', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '1 hour', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '1 day', '2019-07-12 11:09:01'::timestamp);  ------------------------------------------

SELECT time_window(interval '1 week', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '10 week', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '100 day', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '1000 hour', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '1 min', '2019-07-12 11:09:01');

SELECT time_window(1, '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(NULL, '2019-07-12 11:09:01'::timestamptz);   ------------------------------------------

SELECT time_window(interval '1 min', NULL);  ------------------------------------------

SELECT time_window(interval '1 min', NULL);  ------------------------------------------



\set ON_ERROR_STOP 0

SELECT time_window(interval '1 month', '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(interval '1 year', '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(interval '1 month', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '1 year', '2019-07-12 11:09:01'::timestamp);

SELECT time_window(interval '1 min', 1);

SELECT time_window(-1, '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(10000000000000, '2019-07-12 11:09:01'::timestamptz);

SELECT time_window(interval '1 min', 'test');

\set ON_ERROR_STOP 1

-- test multiple time_window calls
SELECT
  time_window(interval '1 min',time),time_window(interval '3 min',time),time_window(interval '3 min',time,'2019-07-11 00:02:05'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:05'::timestamptz),('2019-07-12 00:02:10'::timestamptz)) v(time);

SELECT
  time_window(interval '1 min',time),time_window(interval '3 min',time)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1,2;

-- test nested time_window calls
SELECT
  time_window(interval '3 min',time_window(interval '1 min',time))
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz),('2019-07-12 00:05:01'::timestamptz)) v(time);

-- test references to different columns
SELECT
  time_window(interval '1 min',t) as t,
  min(t),max(t),min(v),max(v)
FROM(VALUES ('2019-07-12 00:00:01'::timestamptz,3), ('2019-07-12 00:00:01'::timestamptz,4),('2019-07-12 00:03:01'::timestamptz,5),('2019-07-12 00:03:01'::timestamptz,6)) tb(t,v)
GROUP BY 1 ORDER BY 1;

-- test gap fill without rows in resultset
SELECT
  time_window(interval '1 min',time),
  min(time)
FROM ( VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
WHERE false
GROUP BY 1 ORDER BY 1;

-- test coalesce

SELECT
  time_window(interval '1 min',time),
  coalesce(min(time),'2019-07-11 23:59:01'::timestamptz),
  coalesce(min(value),0),
  coalesce(min(value),7)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz,1),('2019-07-12 00:01:00'::timestamptz,NULL),('2019-07-12 00:03:00'::timestamptz,3)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test over
SELECT
  time_window(interval '1 min',time),
  min(time),
  4 as c,
  lag(min(time)) OVER ()
FROM (VALUES ('2019-07-12 00:01:01'::timestamptz),('2019-07-12 00:02:01'::timestamptz),('2019-07-12 00:03:01'::timestamptz)) v(time)
GROUP BY 1;


---
--- first / last
---

\set ON_ERROR_STOP 1

-- test different type

SELECT first(array_agg(1),array_agg('2019-07-12 00:01:01'::timestamptz)),last(array_agg(2),array_agg('2019-07-12 00:01:01'::timestamptz));

SELECT first(array_agg(NULL::int),array_agg('2019-07-12 00:01:01'::timestamptz)),last(array_agg(NULL::int),array_agg('2019-07-12 00:01:01'::timestamptz));

SELECT first(array_agg('as'::text),array_agg('2019-07-12 00:01:01'::timestamptz)),last(array_agg('sa'::text),array_agg('2019-07-12 00:01:01'::timestamptz));

SELECT first(array_agg(1::int),array_agg(NULL::timestamptz)),last(array_agg(2::int),array_agg(NULL::timestamptz));

SELECT first(array_agg('2019-07-12 00:02:01'::timestamptz),array_agg('2019-07-12 00:01:01'::timestamptz)),
       last(array_agg('2019-07-12 00:03:01'::timestamptz),array_agg('2019-07-12 00:01:01'::timestamptz));

SELECT first(array_agg(NULL::int),array_agg(NULL::timestamptz)),last(array_agg(NULL::int),array_agg(NULL::timestamptz));

SELECT first(array_agg(repeat('4',4)),array_agg(NULL::timestamptz)),last(array_agg(repeat('4',4)),array_agg(NULL::timestamptz));

-- test unmatched input length

SELECT first(ARRAY[1,2,3],array_agg('2019-07-12 00:01:01'::timestamptz)),last(ARRAY[1,2,3],array_agg('2019-07-12 00:01:01'::timestamptz));

SELECT first(ARRAY[2],ARRAY['2019-07-12 00:00:01'::timestamptz,'2019-07-12 00:01:01'::timestamptz]),
       last(ARRAY[1],ARRAY['2019-07-12 00:00:01'::timestamptz,'2019-07-12 00:01:01'::timestamptz]);


SELECT
  first(array_agg(value),array_agg(time)),last(array_agg(value),array_agg(time))
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, 2),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  first(array_agg(value),array_agg(time)),last(array_agg(value),array_agg(time))
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, 2),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value)
GROUP BY value;

SELECT
  first(array_agg(value),array_agg(time)),last(array_agg(value),array_agg(time))
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, 2),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value)
GROUP BY time;

\set ON_ERROR_STOP 0

SELECT first(1,2),last(1,2);

SELECT first(NULL,array_agg('2019-07-12 00:01:01'::timestamptz)), last(NULL,array_agg('2019-07-12 00:01:01'::timestamptz));

SELECT first(NULL,array_agg('2019-07-12 00:01:01'::timestamptz)), last(NULL,array_agg('2019-07-12 00:01:01'::timestamptz));

SELECT
  first(array_agg(time),array_agg(value)),last(array_agg(time),array_agg(value))
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, 2),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

-- test nested time_window calls
SELECT
  first(array_agg(last(value)),array_agg(time))
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, 2),('2019-07-12 00:02:00'::timestamptz, 1)) v(time,value)
GROUP BY value;

-- test result as group by reference
SELECT
  first(array_agg(value),array_agg(time)),last(array_agg(value),array_agg(time))
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, 2),('2019-07-12 00:02:00'::timestamptz, 1)) v(time,value)
GROUP BY 1;

\set ON_ERROR_STOP 1
-- test first without rows in resultset
SELECT
  first(array_agg(time),array_agg(time)),
  last(array_agg(time),array_agg(time)),
  min(time)
FROM ( VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
WHERE false
GROUP BY time ORDER BY 1;

-- test over
SELECT
  first(array_agg(time),array_agg(time)),
  last(array_agg(time),array_agg(time)),
  min(time),
  4 as c,
  lag(min(time)) OVER ()
FROM (VALUES ('2019-07-12 00:01:01'::timestamptz),('2019-07-12 00:02:01'::timestamptz),('2019-07-12 00:03:01'::timestamptz)) v(time)
GROUP BY time;

\set ON_ERROR_STOP 1
---
--- fill
---

-- test different argument in over 
SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over(partition by time) END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over(partition by value) END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over(partition by time,value) END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over(order by value,time) END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over(order by time,value) END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over(partition by time order by value) END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over(partition by value order by time) END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

-- test different type in fill
SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 'a'),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 'c')) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, ARRAY[1,2,3]),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, ARRAY[4,5,6])) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, repeat('4',4)),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, repeat('4',4))) v(time,value);

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, '2019-07-12 00:00:00'::timestamptz),
             ('2019-07-12 00:00:00'::timestamptz, NULL),
             ('2019-07-12 00:02:00'::timestamptz, '2019-07-12 00:00:00'::timestamptz)) v(time,value);

-- test duplicate call 
SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END ,CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);

-- test with group
SELECT
  min(time) , CASE WHEN min(value) is not null THEN min(value) else fill(min(value)) over() END ,tag
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1 ,'a'),('2019-07-12 00:00:00'::timestamptz, NULL ,'a'),('2019-07-12 00:02:00'::timestamptz, 3 ,'a')) v(time,value,tag)
GROUP BY 3;


\set ON_ERROR_STOP 0
-- test nested time_fill calls
SELECT
  time , CASE WHEN value is not null THEN value else fill(CASE WHEN value is not null THEN value else fill(value) over() END) over() END  
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value);
-- test with group
SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value)
GROUP BY 2;

SELECT
  time , CASE WHEN value is not null THEN value else fill(value) over() END 
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz, 1),('2019-07-12 00:00:00'::timestamptz, NULL),('2019-07-12 00:02:00'::timestamptz, 3)) v(time,value)
GROUP BY 1;

---
--- time fill
---

\set ON_ERROR_STOP 0
-- test fill_last  call errors out when used outside gapfill context
SELECT fill_last(1);
-- test fill_last call errors out when used outside gapfill context with NULL arguments
SELECT fill_last(NULL::int);

-- test time_fill not top level function call
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) + interval '1 min'
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;        -------------------------------------------------

-- test multiple time_fill calls
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

-- test nested time_fill calls
SELECT
  time_fill(interval '1 min',time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),'2019-07-11 23:59:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

-- test time_fill without aggregation
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time);

-- test time_fill with within group

SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
WITHIN GROUP (ORDER BY time)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time);


-- test NULL args
SELECT
  time_fill(NULL,time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

SELECT
  time_fill(interval '1 min',NULL,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

SELECT
  time_fill(interval '1 min',time,NULL,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,NULL)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

-- test interval is bigger than the distance of start and finish

SELECT
  time_fill(interval '1 day',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

-- test start is bigger than finish

SELECT
  time_fill(interval '1 min',time,'2019-07-12 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;


\set ON_ERROR_STOP 0
-- test tamstamp
SELECT
   time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamp)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

SELECT
   time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamp,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

\set ON_ERROR_STOP 1

SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
GROUP BY 1;

SELECT
   time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:00'::timestamp),('2019-07-12 00:02:00'::timestamp)) v(time)
GROUP BY 1;

-- simple fill query
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:12:01'::timestamptz) AS time,
  sum(value) AS value
FROM (values ('2019-07-12 00:00:01'::timestamptz,1),('2019-07-12 00:00:59'::timestamptz,2),('2019-07-12 00:01:01'::timestamptz,3),
  ('2019-07-12 00:03:01'::timestamptz,4),('2019-07-12 00:06:01'::timestamptz,5),('2019-07-12 00:10:01'::timestamptz,6),('2019-07-12 00:14:01'::timestamptz,7)) v(time,value)
GROUP BY 1 ORDER BY 1;

SELECT
  time_fill(0.001,time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:12:01'::timestamptz) AS time,
  sum(value) AS value
FROM (values ('2019-07-12 00:00:01'::timestamptz,1),('2019-07-12 00:00:59'::timestamptz,2),('2019-07-12 00:01:01'::timestamptz,3),
  ('2019-07-12 00:03:01'::timestamptz,4),('2019-07-12 00:06:01'::timestamptz,5),('2019-07-12 00:10:01'::timestamptz,6),('2019-07-12 00:14:01'::timestamptz,7)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test references to different columns
SELECT
  time_fill(interval '1 min',t,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz) as t,
  min(t),max(t),min(v),max(v)
FROM(VALUES ('2019-07-12 00:00:01'::timestamptz,3), ('2019-07-12 00:00:01'::timestamptz,4),('2019-07-12 00:03:01'::timestamptz,5),('2019-07-12 00:03:01'::timestamptz,6)) tb(t,v)
GROUP BY 1 ORDER BY 1;

SELECT
  time_fill(interval '1 min',t,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz) as t,
  min(t),max(t),min(v),max(v)
FROM(VALUES ('2019-07-12 00:00:01'::timestamptz,3), ('2019-07-12 00:00:02'::timestamptz,4),('2019-07-12 00:03:01'::timestamptz,5),('2019-07-12 00:03:02'::timestamptz,6)) tb(t,v)
GROUP BY 1 ORDER BY 1;


-- test passing of values outside boundaries
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz),
  min(time)
FROM (VALUES ('2019-07-11 23:57:01'::timestamptz),('2019-07-12 00:01:01'::timestamptz),('2019-07-12 00:03:01'::timestamptz),('2019-07-12 00:07:01'::timestamptz)) v(time)
GROUP BY 1 ORDER BY 1;


-- test gap fill without rows in resultset
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  min(time)
FROM ( VALUES ('2019-07-12 00:00:00'::timestamptz),('2019-07-12 00:02:00'::timestamptz)) v(time)
WHERE false
GROUP BY 1 ORDER BY 1;

-- test coalesce

SELECT
  time_fill(interval '1 min',time, '2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  coalesce(min(time),'2019-07-11 23:59:01'::timestamptz),
  coalesce(min(value),0),
  coalesce(min(value),7)
FROM (VALUES ('2019-07-12 00:00:00'::timestamptz,1),('2019-07-12 00:01:00'::timestamptz,2),('2019-07-12 00:03:00'::timestamptz,3)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test case
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  min(time),
  CASE WHEN min(time) IS NOT NULL THEN min(time) ELSE '2019-07-11 00:59:01'::timestamptz END,
  CASE WHEN min(time) IS NOT NULL THEN min(time) + interval '6 min' ELSE '2019-07-11 00:59:01'::timestamptz END,
  CASE WHEN 1 = 1 THEN 1 ELSE 0 END
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,1),('2019-07-12 00:01:01'::timestamptz,2),('2019-07-12 00:02:01'::timestamptz,3)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test constants
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  count(time), 4 as c
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz),('2019-07-12 00:01:01'::timestamptz),('2019-07-12 00:02:01'::timestamptz)) v(time)
GROUP BY 1 ORDER BY 1;

-- test column reordering
SELECT
  1 as c1, '2' as c2,
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  3.0 as c3,
  min(time), min(time), 4 as c4
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz),('2019-07-12 00:01:01'::timestamptz),('2019-07-12 00:02:01'::timestamptz)) v(time)
GROUP BY 3 ORDER BY 3;

-- test grouping by non-time columns
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  id,
  min(value) as m
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,1,1),('2019-07-12 00:01:01'::timestamptz,2,2)) v(time,id,value)
GROUP BY 1,id ORDER BY 2,1;

-- test grouping by non-time columns with no rows in resultset
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  id,
  min(value) as m
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,1,1),('2019-07-12 00:01:01',2,2)) v(time,id,value)
WHERE false
GROUP BY 1,id ORDER BY 2,1;

-- test duplicate columns in GROUP BY
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  id,
  id,
  min(value) as m
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,1,1),('2019-07-12 00:01:01'::timestamptz,2,2)) v(time,id,value)
GROUP BY 1,2,3 ORDER BY 2,1;

-- test grouping by columns not in resultset
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  min(value) as m
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,1,1),('2019-07-12 00:01:01'::timestamptz,2,2)) v(time,id,value)
GROUP BY 1,id ORDER BY id,1;

-- test grouping by  with text columns
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  color,
  min(value) as m
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'blue',1),('2019-07-12 00:01:01'::timestamptz,'red',2)) v(time,color,value)
GROUP BY 1,color ORDER BY 2,1;

-- test grouping by with text columns with no rows in resultset
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  color,
  min(value) as m
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'blue',1),('2019-07-12 00:01:01'::timestamptz,'red',2)) v(time,color,value)
WHERE false
GROUP BY 1,color ORDER BY 2,1;

-- test insert into SELECT
CREATE TABLE insert_test(id timestamptz);
INSERT INTO insert_test SELECT time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) 
  FROM (VALUES ('2019-07-12 00:03:01'::timestamptz),('2019-07-12 00:01:01'::timestamptz)) v(time) GROUP BY 1 ORDER BY 1;
SELECT * FROM insert_test order by id;
DROP TABLE insert_test;

-- test join

---------------- ERROR
----------------
----------------
SELECT t1.*,t2.* FROM
(
  SELECT
    time_fill(interval '1 min',time,'2019-07-11 23:58:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time, color, min(value) as m
  FROM
    (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:01:01'::timestamptz,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t1 INNER JOIN
(
  SELECT
    time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz) as time, color, min(value) as m
  FROM
    (VALUES ('2019-07-12 00:03:01'::timestamptz,'red',1),('2019-07-12 00:04:01'::timestamptz,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t2 ON t1.time = t2.time AND t1.color=t2.color;


-- test join with fill_last
SELECT t1.*,t2.m FROM
(
  SELECT
    time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz) as time, 
    color,
    fill_last(min(value)) as fill_last
  FROM
    (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t1 INNER JOIN
(
  SELECT
    time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz) as time,
    color,
    fill_last(min(value)) as m
  FROM
    (VALUES ('2019-07-12 00:03:01'::timestamptz,'red',1),('2019-07-12 00:04:01'::timestamptz,'blue',2)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t2 ON t1.time = t2.time AND t1.color=t2.color;


-- test join with time_window

SELECT t1.*,t2.* FROM
(
  SELECT
    time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:02:01'::timestamptz) as time, 
    color,
    fill_last(min(value)) as fill_last
  FROM
    (VALUES ('2019-07-12 00:00:01'::timestamptz,10,1),('2019-07-12 00:00:01'::timestamptz,11,2),('2019-07-12 00:01:01'::timestamptz,10,3)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t1  JOIN
(
  SELECT
    time_window(interval '1 min',time) as time, 
    color,
   min(value) as m
  FROM
    (VALUES ('2019-07-12 00:00:01'::timestamptz,10,1)) v(time,color,value)
  GROUP BY 1,color ORDER BY 2,1
) t2 ON 1 = 1;


---------------- ExecNestLoop

-- test fill_last
SELECT
  time_fill(interval '10 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:50:01'::timestamptz) AS time,
  fill_last(min(value)) AS value
FROM (values ('2019-07-12 00:00:01'::timestamptz,9),('2019-07-12 00:10:01'::timestamptz,3),('2019-07-12 00:40:01'::timestamptz,6)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test fill_last with constants
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  2,
  fill_last(min(value))
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,1,3),('2019-07-12 00:02:01'::timestamptz,2,3)) v(time,value)
GROUP BY 1 ORDER BY 1;

-- test fill_last with out of boundary lookup
SELECT
  time_fill(interval '10 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:50:01'::timestamptz) AS time,
  fill_last(min(value)) AS value
FROM (values ('2019-07-12 00:10:01'::timestamptz,9),('2019-07-12 00:30:01'::timestamptz,6)) v(time,value)
GROUP BY 1 ORDER BY 1;


-- test fill_last with different datatypes
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz) as time,
  fill_last(min(v1)) AS text,
  fill_last(min(v2)) AS "int[]",
  fill_last(min(v3)) AS "text 4/8k"
FROM (VALUES
  ('2019-07-12 00:01:01'::timestamptz,'foo',ARRAY[1,2,3],repeat('4',4)),
  ('2019-07-12 00:03:01'::timestamptz,'bar',ARRAY[3,4,5],repeat('8',8))
) v(time,v1,v2,v3)
GROUP BY 1;

-- test fill_last lookup query does not trigger when not needed
--

CREATE TABLE metrics_int(time timestamptz,device_id int, sensor_id int, value float);

INSERT INTO metrics_int VALUES
('2019-07-11 23:59:01'::timestamptz,1,1,0.0),
('2019-07-11 23:59:01'::timestamptz,1,2,-100.0),
('2019-07-12 00:00:01'::timestamptz,1,1,5.0),
('2019-07-12 00:00:03'::timestamptz,1,2,10.0),
('2019-07-12 00:01:01'::timestamptz,1,1,0.0),
('2019-07-12 00:01:01'::timestamptz,1,2,-100.0)
;

SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz) AS time,
  device_id,
  sensor_id,
  fill_last(min(value)::int) AS fill_last3
FROM metrics_int m1
WHERE time >= '2019-07-11 23:59:59'::timestamptz AND time < '2019-07-12 00:04:01'::timestamptz
GROUP BY 1,2,3 ORDER BY 2,3,1;
drop table metrics_int;


-- test cte with gap filling in outer query
WITH data AS (
  SELECT * FROM (VALUES ('2019-07-12 00:01:01'::timestamptz,1,1),('2019-07-12 00:02:01'::timestamptz,2,2)) v(time,id,value)
)
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  id,
  min(value) as m
FROM data
GROUP BY 1,id;

-- test cte with gap filling in inner query
WITH gapfill AS (
  SELECT
    time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
    id,
    min(value) as m
  FROM (VALUES ('2019-07-12 00:01:01'::timestamptz,1,1),('2019-07-12 00:02:01'::timestamptz,2,2)) v(time,id,value)
  GROUP BY 1,id
)
SELECT * FROM gapfill;

\set ON_ERROR_STOP 0

SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:05:01'::timestamptz),
  min(time),
  4 as c,
  lag(min(time)) OVER ()
FROM (VALUES ('2019-07-12 00:01:01'::timestamptz),('2019-07-12 00:02:01'::timestamptz),('2019-07-12 00:03:01'::timestamptz)) v(time)
GROUP BY 1;

\set ON_ERROR_STOP 1

-- test reorder
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz) as time,
  id,
  min(value) as m
FROM
  (VALUES ('2019-07-12 00:03:01'::timestamptz,1,1),('2019-07-12 00:01:01'::timestamptz,2,2)) v(time,id,value)
GROUP BY 1,id ORDER BY 1,id;

-- test order by fill_last
SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  fill_last(min(time))
FROM
  (VALUES ('2019-07-12 00:03:01'::timestamptz,1,1),('2019-07-12 00:01:01'::timestamptz,1,1)) v(time)
GROUP BY 1 ORDER BY 2,1;

SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  fill_last(min(time))
FROM
  (VALUES ('2019-07-12 00:03:01'::timestamptz,1,1),('2019-07-12 00:01:01'::timestamptz,1,1)) v(time)
GROUP BY 1 ORDER BY 2 NULLS FIRST,1;

SELECT
  time_fill(interval '1 min',time,'2019-07-11 23:59:01'::timestamptz,'2019-07-12 00:03:01'::timestamptz),
  fill_last(min(time))
FROM
  (VALUES ('2019-07-12 00:03:01'::timestamptz,1,1),('2019-07-12 00:01:01'::timestamptz,1,1)) v(time)
GROUP BY 1 ORDER BY 2 NULLS LAST,1;


\set ON_ERROR_STOP 0

-- NULL start expression and no usable time constraints
SELECT
  time_fill(interval '1 min',t,CASE WHEN length(version())>0 THEN '2019-07-11 23:59:01'::timestamptz ELSE '2019-07-12 00:02:01'::timestamptz END,'2019-07-12 00:05:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz),('2019-07-12 00:03:01'::timestamptz)) v(t)
WHERE true AND true
GROUP BY 1;

-- unsupported start expression and no usable time constraints
SELECT
  time_fill(interval '1 min',t,t,'2019-07-12 00:05:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz),('2019-07-12 00:03:01'::timestamptz)) v(t)
WHERE true AND true
GROUP BY 1;

\set ON_ERROR_STOP 1
-- expression with multiple column references
SELECT
  time_fill(interval '1 min',t1,'2019-07-11 23:59:01'::timestamptz + interval '1 min','2019-07-12 00:03:01'::timestamptz)
FROM (VALUES ('2019-07-12 00:01:01'::timestamptz,2),('2019-07-12 00:02:01'::timestamptz,2)) v(t1,t2)
WHERE true
GROUP BY 1;

-- percentile_cont WITHIN GROUP

\set ON_ERROR_STOP 0

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value,color) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value);

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY color) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value);

SELECT percentile_cont(0.5) WITHIN GROUP () FROM (VALUES (1),(2)) v(value);

SELECT percentile_cont(0.5) WITHIN GROUP (color) FROM (VALUES (1),(2)) v(value);
-- NULL
SELECT percentile_cont(NULL) WITHIN GROUP (ORDER BY value) FROM (VALUES (1),(2)) v(value);

-- test different type in WITHIN GROUP
SELECT percentile_cont(0.5) WITHIN GROUP (order by ARRAY[1,2,3]) FROM (VALUES (1),(2)) v(value);

SELECT percentile_cont(0.5) WITHIN GROUP (order by value) FROM (VALUES ('red'),('yellow')) v(value);

SELECT percentile_cont(0.5) WITHIN GROUP (order by value) FROM (VALUES (ARRAY[1,2,3]),(ARRAY[2,3,4])) v(value);

-- test not [0,1]
SELECT percentile_cont(2) WITHIN GROUP (ORDER BY value) FROM (VALUES (1),(2)) v(value);

SELECT percentile_cont(-1) WITHIN GROUP (ORDER BY value) FROM (VALUES (1),(2)) v(value);

----- OVER
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) over(partition by color) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value) ;

SELECT percentile_cont(0.5)  over(ORDER BY value) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value) ;


------ float8[]
SELECT percentile_cont(array[0.5,0.9]) WITHIN GROUP (ORDER BY value) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value);
------ multi-call
SELECT percentile_cont(0.5),percentile_cont(0.7) WITHIN GROUP (ORDER BY value) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value);

\set ON_ERROR_STOP 1

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value);
-- desc
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value desc) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value);
-- interval
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',interval '1 day'),('2019-07-12 00:00:01'::timestamptz,'blue','3 day')) v(time,color,value);

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM (VALUES (1),(2)) v(value);

SELECT percentile_cont(0.5) within group (order by value) FROM (SELECT generate_series(0.01, 1, 0.01) as value);

-- test compute
SELECT percentile_cont(0.5) within group (order by value * 2) FROM (SELECT generate_series(0.01, 1, 0.01) as value);

SELECT percentile_cont(0.5 * 2 - 0.3) within group (order by value) FROM (SELECT generate_series(0.01, 1, 0.01) as value);

-- test NULL
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY NULL) FROM (VALUES (1),(2)) v(value);

-- test GROUP BY
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value) GROUP BY color;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value) GROUP BY color,time;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM 
  (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',0.1),('2019-07-12 00:00:01'::timestamptz,'blue',-3),('2019-07-12 00:00:01'::timestamptz,'blue',-0.3),('2019-07-12 00:00:01'::timestamptz,'red',2)) 
  v(time,color,value) 
GROUP BY color;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM 
  (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',0.1),('2019-07-12 00:00:01'::timestamptz,'blue',-3),('2019-07-12 00:00:01'::timestamptz,'blue',-0.3),('2019-07-12 00:00:01'::timestamptz,'red',2)) 
  v(time,color,value) 
GROUP BY color,time,value;
-- test GROUP BY desc
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value desc) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value) GROUP BY color;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value desc) FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value) GROUP BY color,time;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value desc) FROM 
  (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',0.1),('2019-07-12 00:00:01'::timestamptz,'blue',-3),('2019-07-12 00:00:01'::timestamptz,'blue',-0.3),('2019-07-12 00:00:01'::timestamptz,'red',2)) 
  v(time,color,value) 
GROUP BY color;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value desc) FROM 
  (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',0.1),('2019-07-12 00:00:01'::timestamptz,'blue',-3),('2019-07-12 00:00:01'::timestamptz,'blue',-0.3),('2019-07-12 00:00:01'::timestamptz,'red',2)) 
  v(time,color,value) 
GROUP BY color,time,value;

-- test order by const
SELECT percentile_cont(0.5) WITHIN GROUP (order by 2) FROM (VALUES (1),(2)) v(value);

SELECT percentile_cont(0.5) WITHIN GROUP (order by 2);

SELECT percentile_cont(0.5) WITHIN GROUP (order by -2);

--- multiple percentile_cont
SELECT k, percentile_cont(k) within group (order by value)
FROM (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',1),('2019-07-12 00:00:01'::timestamptz,'blue',2)) v(time,color,value) , generate_series(0.1, 1, 0.01) as k
group by k;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value),percentile_cont(0.5) WITHIN GROUP (ORDER BY value2) FROM 
  (VALUES (0.1,'red',0.1),(-3,'blue',-3),(-0.3,'blue',-0.3),(2,'red',2)) 
  v(value2,color,value) 
GROUP BY color;

WITH s1 as (
  SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) as per,percentile_cont(0.5) WITHIN GROUP (ORDER BY value2) FROM 
    (VALUES (0.1,'red',0.1),(-3,'blue',-3),(-0.3,'blue',-0.3),(2,'red',2)) 
    v(value2,color,value)  GROUP BY color)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY s1.per) from s1;

--- with other aggregate
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value desc), sum(value),count(time),max(value)  FROM 
  (VALUES ('2019-07-12 00:00:01'::timestamptz,'red',0.1),('2019-07-12 00:00:01'::timestamptz,'blue',-3),('2019-07-12 00:00:01'::timestamptz,'blue',-0.3),('2019-07-12 00:00:01'::timestamptz,'red',2)) 
  v(time,color,value) 
GROUP BY color;

--- mix with time_fill

WITH s1 as (
  SELECT
    time_fill(interval '1 min',time,'2019-07-12 00:01:01'::timestamptz,'2019-07-12 00:06:01'::timestamptz) as rtime,
    fill_last(min(value)) as rvalue,
    fill_last(min(tag)) as rtag
  FROM
    (VALUES ('2019-07-12 00:03:01'::timestamptz,2,2),('2019-07-12 00:01:01'::timestamptz,1,1),('2019-07-12 00:05:01'::timestamptz,3,3)) v(time, value, tag)
  GROUP BY rtime ORDER BY 1
)
SELECT first(array_agg(s1.rvalue),array_agg(s1.rtime)),
       last(array_agg(s1.rvalue),array_agg(s1.rtime)),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY s1.rvalue)
       FROM s1;


CREATE TABLE metrics_int(time timestamptz,device_id int, sensor_id int, value float);

INSERT INTO metrics_int VALUES
('2019-07-11 23:59:01'::timestamptz,11,1,0.0),
('2019-07-11 23:59:01'::timestamptz,11,2,-100.0),
('2019-07-12 00:00:01'::timestamptz,12,3,5.0),
('2019-07-12 00:00:03'::timestamptz,12,4,10.0),
('2019-07-13 00:01:03'::timestamptz,11,5,0.0),
('2019-07-14 00:01:04'::timestamptz,11,5,0.0),
('2019-07-15 00:01:05'::timestamptz,11,5,0.0),
('2019-07-16 00:01:06'::timestamptz,11,5,0.0),
('2019-07-17 00:01:07'::timestamptz,11,5,0.0),
('2019-07-12 00:01:01'::timestamptz,12,6,-100.0)
;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM metrics_int;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY value) FROM metrics_int group by device_id;

drop table metrics_int;

---
--- combination
--

-- combine time_fill, fill_last, time_window, first, last, percentile
WITH s1 as (
  SELECT
    time_fill(interval '1 min',time,'2019-07-12 00:01:01'::timestamptz,'2019-07-12 00:06:01'::timestamptz) as rtime,
    fill_last(min(value)) as rvalue,
    fill_last(min(tag)) as rtag
  FROM
    (VALUES ('2019-07-12 00:03:01'::timestamptz,2,2),('2019-07-12 00:01:01'::timestamptz,1,1),('2019-07-12 00:05:01'::timestamptz,3,3)) v(time, value, tag)
  GROUP BY rtime ORDER BY 1
)
SELECT first(array_agg(s1.rvalue),array_agg(s1.rtime)),
       last(array_agg(s1.rvalue),array_agg(s1.rtime)),
       percentile_cont(0.5) WITHIN GROUP (ORDER BY rvalue)
       FROM s1;

-- combine time_fill, fill_last, time_window, first, last, percentile, fill
WITH s1 as (
  SELECT 
       time_window(interval '1 min',time) as t_time,
       first(array_agg(value),array_agg(time)) as f_value,
       last(array_agg(value),array_agg(time)) as l_value,
       first(array_agg(tag),array_agg(time)) as f_tag,
       last(array_agg(tag),array_agg(time)) as l_tag,
       CASE WHEN first(array_agg(tag),array_agg(time)) is NOT NULL THEN first(array_agg(tag),array_agg(time)) ELSE fill(first(array_agg(tag),array_agg(time))) over (order by 1) END as fill_tag
  FROM
    (VALUES ('2019-07-12 00:03:01'::timestamptz,2,2),('2019-07-12 00:01:01'::timestamptz,1,NULL),('2019-07-12 00:05:01'::timestamptz,3,3)) v(time, value, tag)
  GROUP BY t_time
) SELECT
    time_fill(interval '1 min',t_time,'2019-07-12 00:01:01'::timestamptz,'2019-07-12 00:06:01'::timestamptz) as rtime,
    min(f_value) as m_f_value,
    min(l_value) as m_l_value,
    min(f_tag) as m_f_tag,
    min(l_tag) as m_l_tag,
    min(fill_tag) as m_fill_tag,
    fill_last(min(f_value)) as fill_f_value,
    fill_last(min(l_value)) as fill_l_value,
    fill_last(min(f_tag)) as fill_last_f_tag,
    fill_last(min(l_tag)) as fill_last_l_tag
  FROM s1
  GROUP BY rtime
  ORDER BY rtime;


---
--- bottom_k
---
\set ON_ERROR_STOP 1

SELECT
  bottom_k(ARRAY[1,2,3], 0.95);

SELECT
  bottom_k(array_agg(v), 0.95)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);

SELECT
  bottom_k(array_agg(v), 2)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);


SELECT
  bottom_k(array_agg(v), 0)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);

SELECT
  bottom_k(array_agg(v), -2)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);


SELECT
  bottom_k(array_agg(v), 0.5),v2
FROM (VALUES (1,'a'),(2,'a'),(3,'a'),(4,'a'),(5,'a'),(6,'a')) v(v,v2) group by v2;

SELECT
  bottom_k(array_agg(v), 0.1),v2
FROM (VALUES (1,'a'),(2,'a'),(3,'a'),(4,'a'),(5,'a'),(6,'a')) v(v,v2) group by v2;


\set ON_ERROR_STOP 0
SELECT
  bottom_k(array_agg(v), 0.5),v2
FROM (VALUES (1,'a'),(2,'a'),(3,'a'),(4,'a'),(5,'a'),(6,'a')) v(v,v2);

SELECT
  bottom_k(v, 0.5)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);


---
--- top_k
---
\set ON_ERROR_STOP 1

SELECT
  top_k(ARRAY[1,2,3], 0.95);

SELECT
  top_k(array_agg(v), 0.95)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);

SELECT
  top_k(array_agg(v), 2)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);


SELECT
  top_k(array_agg(v), 0)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);

SELECT
  top_k(array_agg(v), -2)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);


SELECT
  top_k(array_agg(v), 0.5),v2
FROM (VALUES (1,'a'),(2,'a'),(3,'a'),(4,'a'),(5,'a'),(6,'a')) v(v,v2) group by v2;

SELECT
  top_k(array_agg(v), 0.1),v2
FROM (VALUES (1,'a'),(2,'a'),(3,'a'),(4,'a'),(5,'a'),(6,'a')) v(v,v2) group by v2;


\set ON_ERROR_STOP 0
SELECT
  top_k(array_agg(v), 0.5),v2
FROM (VALUES (1,'a'),(2,'a'),(3,'a'),(4,'a'),(5,'a'),(6,'a')) v(v,v2);

SELECT
  top_k(v, 0.5)
FROM (VALUES (1),(2),(-3),(-1.1),(5),(6.6),(0)) v(v);

-- delta agg

select delta(idle) over (rows 1 preceding) from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',2),('2020-04-17T17:00:03Z',3)) v(time, idle);

select delta(idle) over (rows 1 preceding) from (values('2020-04-17T17:00:03Z','a'),('2020-04-17T17:00:03Z','a'),('2020-04-17T17:00:03Z','a')) v(time, idle);

select delta(avg(idle)) over (rows 1 preceding) from (values('2020-04-17T17:00:03Z','a'),('2020-04-17T17:00:03Z','a'),('2020-04-17T17:00:03Z','a')) v(time, idle);

select delta(NULL::int) from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',2),('2020-04-17T17:00:03Z',3)) v(time, idle);

select delta(idle) over (rows 1 preceding) from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',3),('2020-04-17T17:00:03Z',3)) v(time, idle) group by time;

select delta(idle) over (rows 1 preceding) from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',3),('2020-04-17T17:00:03Z',3)) v(time, idle);

-- spread agg

select spread(idle)  from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',2),('2020-04-17T17:00:03Z',3)) v(time, idle);

select spread(idle)  from (values('2020-04-17T17:00:03Z','a'),('2020-04-17T17:00:03Z','b'),('2020-04-17T17:00:03Z','c')) v(time, idle);

select spread(idle)  from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:02Z',2),('2020-04-17T17:00:03Z',5),('2020-04-17T17:00:03Z',9),('2020-04-17T17:00:02Z',8),('2020-04-17T17:00:03Z',2)) v(time, idle) group by time;

select spread(avg(idle))  from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:02Z',2),('2020-04-17T17:00:03Z',5),('2020-04-17T17:00:03Z',9),('2020-04-17T17:00:02Z',8),('2020-04-17T17:00:03Z',2)) v(time, idle) group by time;

select spread(NULL)   from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:03Z',2),('2020-04-17T17:00:03Z',3)) v(time, idle);

select spread(idle)  over (partition by time)  from (values('2020-04-17T17:00:03Z',1),('2020-04-17T17:00:02Z',2),('2020-04-17T17:00:03Z',5),('2020-04-17T17:00:03Z',9),('2020-04-17T17:00:02Z',8),('2020-04-17T17:00:03Z',2)) v(time, idle) group by idle;
