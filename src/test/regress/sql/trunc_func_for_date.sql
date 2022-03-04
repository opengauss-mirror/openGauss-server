---
--- data type 1 : timestamp
--- 

-- format can recognize
select trunc(timestamp '2021-08-11 20:19:39', 'cc');    -- century
select trunc(timestamp '2021-08-11 20:19:39', 'yyyy');  -- year
select trunc(timestamp '2021-08-11 20:19:39', 'q');     -- quarter
select trunc(timestamp '2021-08-11 20:19:39', 'mm');    -- month
select trunc(timestamp '2021-08-11 20:19:39', 'j');     -- day
select trunc(timestamp '2021-08-11 20:19:39', 'dd');    -- day
select trunc(timestamp '2021-08-11 20:19:39', 'ddd');   -- day
select trunc(timestamp '2021-08-11 20:19:39', 'hh');    -- hour
select trunc(timestamp '2021-08-11 20:19:39', 'mi');    -- minute

-- format can not recognize
select trunc(timestamp '2021-08-11 20:19:39', 'qq');    -- quarter
select trunc(timestamp '2021-08-11 20:19:39', 'mmm');   -- month
select trunc(timestamp '2021-08-11 20:19:39', 'dddd');  -- day
select trunc(timestamp '2021-08-11 20:19:39', 'hhh');   -- hour

---
--- data type 2 : timestamptz
--- 

-- format can recognize
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'cc');    -- century
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'yyyy');  -- year
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'q');     -- quarter
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'mm');    -- month
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'j');     -- day
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'dd');    -- day
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'ddd');   -- day
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'hh');    -- hour
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'mi');    -- minute

-- format can't recognize
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'qq');    -- quarter
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'mmm');   -- month
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'dddd');  -- day
select trunc(timestamptz '2021-08-12 08:48:26.366526+08', 'hhh');   -- hour

---
--- data type 3 : interval
--- 

-- format can recognize
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'cc');    -- century
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'yyyy');  -- year
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'q');     -- quarter
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'mm');    -- month
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'j');     -- day
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'dd');    -- day
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'ddd');   -- day
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'hh');    -- hour
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'mi');    -- minute

-- format can not recognize
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'qq');    -- quarter
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'mmm');   -- month
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'dddd');  -- day
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'hhh');   -- hour

-- not supported
select trunc(interval '2 years 3 months 4 days 5 hours 6 minutes 7 seconds', 'w');     -- week