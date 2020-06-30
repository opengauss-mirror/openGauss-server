CREATE TABLE INT8_TBL(q1 int8, q2 int8) ;
create temp table t1 ( a int default 10, f1 int8_tbl[]) ;
insert into t1 (f1[5].q1) values(42);
select * from t1;
update t1 set f1[5].q2 = 43;
select * from t1;
create table t2 ( a int default 10, f1 int8_tbl[]) ;
insert into t2 (f1[5].q1) values(42);
select * from t2;
update t2 set f1[5].q2 = 43;
select * from t2;
drop table t1;
drop table t2;
drop table INT8_TBL;
set behavior_compat_options = 'return_null_string';
SELECT pg_catalog.oidvectortypes(' '::oidvector) AS a,
LENGTH(pg_catalog.oidvectortypes(' '::oidvector)) AS b
FROM dual;
CREATE TABLE test_oidvectortypes  AS
SELECT pg_catalog.oidvectortypes(' '::oidvector) AS a,
LENGTH(pg_catalog.oidvectortypes(' '::oidvector)) AS b
FROM dual;
SELECT * from test_oidvectortypes;
SELECT pg_catalog.oidvectortypes(' '::oidvector) AS a,
LENGTH(pg_catalog.oidvectortypes(' '::oidvector)) AS b
FROM dual WHERE a IS NULL;
SELECT * FROM test_oidvectortypes WHERE a IS NULL;
DROP TABLE test_oidvectortypes;
set behavior_compat_options = '';
SELECT pg_catalog.oidvectortypes(' '::oidvector) AS a,
LENGTH(pg_catalog.oidvectortypes(' '::oidvector)) AS b
FROM dual;
CREATE TABLE test_oidvectortypes  AS
SELECT pg_catalog.oidvectortypes(' '::oidvector) AS a,
LENGTH(pg_catalog.oidvectortypes(' '::oidvector)) AS b
FROM dual;
SELECT * from test_oidvectortypes;
SELECT pg_catalog.oidvectortypes(' '::oidvector) AS a,
LENGTH(pg_catalog.oidvectortypes(' '::oidvector)) AS b
FROM dual WHERE a IS NULL;
SELECT * FROM test_oidvectortypes WHERE a IS NULL;
DROP TABLE test_oidvectortypes;
