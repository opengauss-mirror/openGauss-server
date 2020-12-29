CREATE SCHEMA distribute_recursive_cte_1;
SET current_schema = distribute_recursive_cte_1;

CREATE TABLE recursive_001 (
    id_int integer NOT NULL,
    name_int text NOT NULL,
    pid_int integer NOT NULL,
    pname_int text NOT NULL,
    time_int timestamp without time zone NOT NULL,
    id_date timestamp(0) without time zone NOT NULL,
    name_date text,
    pid_date timestamp(0) without time zone,
    pname_date text,
    time_date timestamp without time zone,
    id_string character varying,
    name_string text,
    pid_string character varying,
    pname_string text,
    time_string timestamp without time zone,
    id_number numeric,
    name_number text,
    pid_number numeric,
    pname_number text,
    time_number timestamp without time zone
)
WITH (orientation=row, compression=no)
;

ALTER TABLE recursive_001 ADD PRIMARY KEY (id_int, name_int, pid_int);

--
---- case 1: REPLICATION + COLUMN
--
CREATE TABLE test_int_rep (
    id integer,
    name text,
    pid integer,
    pname text,
    "time" timestamp without time zone
)
WITH (orientation=column, compression=no)
;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 group by  name_int, pname_int ORDER BY 10,11
);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

--
---- case 2: REPLICATION + ROW
--
CREATE TABLE test_int_rep (
    id integer,
    name text,
    pid integer,
    pname text,
    "time" timestamp without time zone
)
WITH (orientation=row, compression=no)
;
explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 group by  name_int, pname_int ORDER BY 10,11
);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

--
---- case 3: HASH + ROW
--
DROP TABLE test_int_rep;
CREATE TABLE test_int_rep (
    id integer,
    name text,
    pid integer,
    pname text,
    "time" timestamp without time zone
)
WITH (orientation=row, compression=no)
;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 group by  name_int, pname_int ORDER BY 10,11
);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

--
---- case 4: HASH + COLUMN
--
DROP TABLE test_int_rep;
CREATE TABLE test_int_rep (
    id integer,
    name text,
    pid integer,
    pname text,
    "time" timestamp without time zone
)
WITH (orientation=column, compression=no)
;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 ORDER BY 10,11
);

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
SELECT * FROM 
(
    SELECT   
        1,
        1,
        1, 
        1,
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02',
        '2017-01-02'
        ,count(distinct tmp1.pid_date)
        ,count(distinct tmp1.id_string)
    FROM tmp1 group by  name_int, pname_int ORDER BY 10,11
);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

WITH RECURSIVE tmp1 AS 
(
   SELECT  * FROM recursive_001 WHERE pid_int <50 
   union 
   SELECT   tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid_int
)
insert into recursive_001
SELECT   
    1,
    1,
    1, 
    1,
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02',
    '2017-01-02'
    ,max(distinct tmp1.pid_date)
    ,min(distinct tmp1.id_string)
FROM tmp1 group by name_int, pname_int;

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS 
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
delete FROM recursive_001 using tmp1 WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1);

explain (costs off, verbose ON)
WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

WITH RECURSIVE tmp1 AS
(
    SELECT  * FROM test_int_rep WHERE id <50 
    union 
    SELECT tmp1.* FROM tmp1 JOIN  test_int_rep t1 ON t1.pid = tmp1.pid
)
update recursive_001 set id_int = id_int +1  WHERE recursive_001.id_int in (SELECT max(distinct tmp1.pid)+max(distinct tmp1.id) FROM tmp1 group by name_int, pname_int);

create table test_date(id date,name text,pid date, pname text,time timestamp)  ;
COPY test_date (id, name, pid, pname, "time") FROM stdin(DELIMITER  ',', NULL '');
2018-12-12,data_2018-12-12,2018-12-19,data_2018-12-19,2018-08-28
2018-12-13,data_2018-12-13,2018-12-29,data_2018-12-29,2018-05-14
2018-12-14,data_2018-12-14,2018-12-29,data_2018-12-29,2018-06-16
2018-12-15,data_2018-12-15,2018-12-20,data_2018-12-20,2018-05-27
2018-12-16,data_2018-12-16,2018-12-29,data_2018-12-29,2018-01-13
2018-12-17,data_2018-12-17,2018-12-25,data_2018-12-25,2018-11-23
2018-12-18,data_2018-12-18,2018-12-22,data_2018-12-22,2018-09-11
2018-12-19,data_2018-12-19,2018-12-29,data_2018-12-29,2018-10-13
2018-12-20,data_2018-12-20,2018-12-29,data_2018-12-29,2017-12-31
2018-12-21,data_2018-12-21,2018-12-22,data_2018-12-22,2018-11-17
2018-12-22,data_2018-12-22,2018-12-25,data_2018-12-25,2018-09-23
2018-12-23,data_2018-12-23,2018-12-29,data_2018-12-29,2018-03-30
2018-12-24,data_2018-12-24,2018-12-28,data_2018-12-28,2018-09-18
2018-12-25,data_2018-12-25,2018-12-27,data_2018-12-27,2018-07-27
2018-12-26,data_2018-12-26,2018-12-31,data_2018-12-31,2018-11-01
2018-12-27,data_2018-12-27,2018-12-29,data_2018-12-29,2018-06-01
2018-12-29,data_2018-12-29,2018-12-31,data_2018-12-31,2018-11-26
2018-12-30,data_2018-12-30,2018-12-31,data_2018-12-31,2018-09-15
\.
;

create index test_date_1 on test_date (id);
create index test_date_2 on test_date (pid); 
create index test_date_3 on test_date (id,pid);

create table test_date_rep_par(id date,name text,pid date, pname text,time timestamp)  
partition by range(id)	
(
PARTITION P1 VALUES LESS THAN('1991-08-16'),
PARTITION P2 VALUES LESS THAN('1991-09-11'),	
PARTITION P3 VALUES LESS THAN('1991-10-11'),	
PARTITION P4 VALUES LESS THAN('1992-01-01'),	
PARTITION P5 VALUES LESS THAN('1992-02-01'),
PARTITION P6 VALUES LESS THAN('1992-05-01'),	
PARTITION P7 VALUES LESS THAN('1992-08-23'),
PARTITION P8 VALUES LESS THAN(MAXVALUE));

insert into test_date_rep_par select * from test_date;
create index test_date_rep_par_1 on test_date_rep_par (id) local;
create index test_date_rep_par_2 on test_date_rep_par (pid) local;

create table test_date_hash_par(id date,name text,pid date, pname text,time timestamp)  
partition by range(id)
(
PARTITION P1 VALUES LESS THAN('1991-08-16'),
PARTITION P2 VALUES LESS THAN('1991-09-11'),
PARTITION P3 VALUES LESS THAN('1991-10-11'),	
PARTITION P4 VALUES LESS THAN('1992-01-01'),	
PARTITION P5 VALUES LESS THAN('1992-02-01'),	
PARTITION P6 VALUES LESS THAN('1992-05-01'),	
PARTITION P7 VALUES LESS THAN('1992-08-23'),
PARTITION P8 VALUES LESS THAN(MAXVALUE)); 
insert into test_date_hash_par select * from test_date; 

CREATE OR REPLACE PROCEDURE test_retry(max in integer) 
AS
DECLARE
    count int := 1;
    result timestamp;
    seed_sql text;
BEGIN
    seed_sql := 'set plan_mode_seed = ' || -1;
    EXECUTE IMMEDIATE  seed_sql;
    LOOP 
        IF count >= max THEN 
            RAISE INFO '% times retry with correct result' ,max;
            EXIT;
        ELSE
            with recursive  tmp1 as (
            select t1.id, t1.pname ,t1.pid ,count(8)
            from test_date_rep_par t1
            where  id in (select id from test_date where id in (select id from test_date_hash_par where id in (select id from test_date_rep_par )))
            group by 3,1,2
            union all
            select test_date_hash_par.id,test_date_hash_par.pname ,test_date_hash_par.pid,2
            from test_date_hash_par
            join tmp1
            on test_date_hash_par.id = tmp1.pid
             )
            select max(id) into result from tmp1
            where exists ( select id from test_date where id in (select id from test_date_hash_par where id in (select id from test_date_rep_par  where id in ( with recursive  tmp1 as (
            select t1.id, t1.pname ,t1.pid ,count(8)
            from test_date_rep_par t1
            where  id in (select id from test_date where id in (select id from test_date_hash_par where id not in (select id from test_date_rep_par where id between '1998-09-08' and '1999-09-10')))        
            group by 3,1,2
            union all
            select test_date_hash_par.id,test_date_hash_par.pname ,test_date_hash_par.pid,2
            from test_date_hash_par
            join tmp1
            on test_date_hash_par.id = tmp1.pid
             )select id from tmp1 t1 where t1.id = tmp1.id  ))))
            or id =(select max(id) from tmp1 as t1 where t1.id = tmp1.id group by t1.pname) order by 1 ;

            IF result <> '2018-12-30 00:00:00'::timestamp THEN
                RAISE INFO 'incorrect result with plan_mode_seed = %', count;  
                EXIT;
            END IF;
            count:=count+1; 
        END IF; 
    END LOOP; 
END;
/
call test_retry(10);

CREATE OR REPLACE PROCEDURE test_retry(max in integer) 
AS
DECLARE
    count int := 1;
    result timestamp;
    seed_sql text;
BEGIN
    seed_sql := 'set plan_mode_seed = ' || -1;
    EXECUTE IMMEDIATE  seed_sql;
    LOOP 
        IF count >= max THEN 
            RAISE INFO '% times retry with correct result' ,max;
            EXIT;
        ELSE
            with recursive  tmp1 as 
            (
                select t1.id, t1.pname ,t1.pid ,count(8) from test_date_rep_par t1  where  id in (select id from test_date where id in (select id from test_date_hash_par where id in (select id from test_date_rep_par ))) group by 3,1,2
                union all
                select test_date_hash_par.id,test_date_hash_par.pname ,test_date_hash_par.pid,2 from test_date_hash_par join tmp1 on test_date_hash_par.id = tmp1.pid
            )
            
            select
                max(id) into result
            from tmp1 where  id =(select max(id) from tmp1 as t1 where t1.id = tmp1.id group by t1.pname) order by 1 ;

            IF result <> '2018-12-30 00:00:00'::timestamp THEN
                RAISE INFO 'incorrect result with plan_mode_seed = %', count;  
                EXIT;
            END IF;
            count:=count+1;
        END IF; 
    END LOOP; 
END;
/
call test_retry(10);

RESET search_path;
DROP SCHEMA distribute_recursive_cte_1 CASCADE;
