-- create table
create table t_cte(a int, b int);
insert into t_cte select generate_series(1,5), generate_series(1,5);

create table int8_tbl (q1 int8, q2 int8);
INSERT INTO INT8_TBL VALUES
  ('  123   ','  456');


-- check case where CTE reference is removed due to optimization
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q1 FROM
(
  WITH t_cte AS (SELECT * FROM int8_tbl t)
  SELECT q1, (SELECT q2 FROM t_cte WHERE t_cte.q1 = i8.q1) AS t_sub
  FROM int8_tbl i8
) ss;

SELECT q1 FROM
(
  WITH t_cte AS (SELECT * FROM int8_tbl t)
  SELECT q1, (SELECT q2 FROM t_cte WHERE t_cte.q1 = i8.q1) AS t_sub
  FROM int8_tbl i8
) ss;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT q1 FROM
(
  WITH t_cte AS MATERIALIZED (SELECT * FROM int8_tbl t)
  SELECT q1, (SELECT q2 FROM t_cte WHERE t_cte.q1 = i8.q1) AS t_sub
  FROM int8_tbl i8
) ss;

SELECT q1 FROM
(
  WITH t_cte AS MATERIALIZED (SELECT * FROM int8_tbl t)
  SELECT q1, (SELECT q2 FROM t_cte WHERE t_cte.q1 = i8.q1) AS t_sub
  FROM int8_tbl i8
) ss;


SELECT ( WITH table2 AS NOT MATERIALIZED ( SELECT 1 ) SELECT 1 FROM ( SELECT ( SELECT 1 FROM table2 ) BETWEEN 1 AND 1 ) AS alias6 ) ;

drop table t_cte;
drop table int8_tbl cascade;
