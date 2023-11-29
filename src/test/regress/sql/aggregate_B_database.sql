-- test normal db
create database group_concat_test1 dbcompatibility 'A';
\c group_concat_test1
CREATE TABLE t(id int, v text);
INSERT INTO t(id, v) VALUES(1, 'A'),(2, 'B'),(1, 'C'),(2, 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDD');
select id, group_concat(v separator ';') from t group by id order by id asc;
CREATE TABLE agg_t1 ( c53 INT , c47 INT );
CREATE TABLE agg_t2 ( c17 INT , c52 INT );
SELECT 1 FROM agg_t1 CROSS JOIN agg_t2 GROUP BY c52 HAVING + GROUP_CONCAT( 1 ORDER BY 1 SEPARATOR '' ) ;
create database group_concat_test2 dbcompatibility 'C';
\c group_concat_test2
CREATE TABLE t(id int, v text);
INSERT INTO t(id, v) VALUES(1, 'A'),(2, 'B'),(1, 'C'),(2, 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDD');
select id, group_concat(v separator ';') from t group by id order by id asc;
create database group_concat_test3 dbcompatibility 'PG';
\c group_concat_test3
CREATE TABLE t(id int, v text);
INSERT INTO t(id, v) VALUES(1, 'A'),(2, 'B'),(1, 'C'),(2, 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDD');
select id, group_concat(v separator ';') from t group by id order by id asc;
\c regression
drop database group_concat_test1;
drop database group_concat_test2;
drop database group_concat_test3;
-- test group_concat (in B db)
create database test_group_concat_B_db dbcompatibility 'B';
\c test_group_concat_B_db
set group_concat_max_len to 20480;
CREATE TABLE emp
(
   empno INTEGER CONSTRAINT pk_emp PRIMARY KEY,
   phone BIGINT,
   sign BLOB,
   ename VARCHAR(20),
   job CHAR(10),
   address TEXT,
   email TEXT,
   mgrno INT4,
   workhour INT2,
   hiredate DATE,
   termdate DATE,
   offtime TIMESTAMP,
   overtime TIMESTAMPTZ,
   vacationTime INTERVAL,
   salPerHour FLOAT4,
   bonus NUMERIC(8,2),
   deptno NUMERIC(2)
);

INSERT INTO emp VALUES (7369,08610086,'58','SMITH','CLERK','宝山区示范新村37号403室','smithWu@163.com',7902,8,to_date('17-12-1999', 'dd-mm-yyyy'),NULL, '2018/12/1','2019-2-20 pst', INTERVAL '5' DAY, 60.35, 2000.80,20);
INSERT INTO emp VALUES (7499,08610086,'59','ALLEN','SALESMAN','虹口区西康南路125弄34号201室','66allen_mm@qq.com',7698,5,to_date('20-2-2015', 'dd-mm-yyyy'),'2018-1-1','2013-12-24 12:30:00','2017-12-12 UTC', '4 DAY 6 HOUR', 9.89,899.00,30);
INSERT INTO emp VALUES (7521,08610010,'5A','WARD','SALESMAN','城东区亨达花园7栋702','hello702@163.com',7698,10,to_date('22-2-2010','dd-mm-yyyy'),'2016/12/30', '2016-06-12 8:12:00','2012-7-10 pst',INTERVAL '30 DAY', 52.98, 1000.01,30);
INSERT INTO emp VALUES (7566,08610010,'58','JONES','MANAGER','莲花五村龙昌里34号601室','jonesishere@gmal.com',7839,3,to_date('2-4-2001','dd-mm-yyyy'),'2013-1-30','2010-10-13 24:00:00','2009-10-12 CST',NULL,200.00,999.10,20);
INSERT INTO emp VALUES (7654,08610000,'59','MARTIN','SALESMAN','开平路53号国棉四厂二宿舍1号楼2单元','mm213n@qq.com',7698,12,to_date('28-9-1997','dd-mm-yyyy'),NULL,'2018/9/25 23:00:00','1999-1-18 CST', '24 HOUR', 1.28,99.99,30);
INSERT INTO emp VALUES (7698,08610000,'5A','BLAKE','MANAGER','建国门大街23号楼302室','blake-life@fox.mail',7839,1,to_date('1-5-1981','dd-mm-yyyy'),'2012-10-13','2009-4-29 05:35:00','2010-12-1 pst','1 YEAR 1 MONTH', 38.25,2399.50,30);
INSERT INTO emp VALUES (7782,08610145,'58','CLARK','MANAGER','花都大道100号世纪光花小区3023号','flower21@gmail.com',7839,5,to_date('9-6-1981','dd-mm-yyyy'),NULL,'1999-8-18 24:00:00','1999-5-12 pst','10 DAY',100.30,10000.01,10);
INSERT INTO emp VALUES (7788,08610145,'59','SCOTT','ANALYST','温泉路四海花园1号楼','bigbigbang@sina.com',7566,9,to_date('13-7-1987','dd-mm-yyyy')-85,'2000-10-1','1998-1-19 00:29:00','2000-2-29 UTC','1 WEEK 2 DAY',99.25,1001.01,20);
INSERT INTO emp VALUES (7839,08610086,'5A','KING','PRESIDENT','温江区OneHouse高级别墅1栋','houseme123@yahoo.com',NULL,2,to_date('17-11-1981','dd-mm-yyyy'),NULL,NULL,NULL,'1 YEAR 30 DAY',19999.99,23011.88,10);
INSERT INTO emp VALUES (7844,08610012,'58','TURNER','SALESMAN','城北梁家巷132号','tur789@qq.com',7698,15,to_date('8-9-1981','dd-mm-yyyy'),'2011-07-15','1998-1-18 23:12:00','1999-1-16 pst','2 MONTH 10 DAY',99.12,9,30);
INSERT INTO emp VALUES (7876,08610012,'59','ADAMS','CLERK','如北街江心美寓小区1号','aking_clerk@sina.com',7788,8,to_date('13-7-1987', 'dd-mm-yyyy')-51,'2018-10-23','1999-1-18 23:12:00','2017-12-30 pst','36 DAY',2600.12,1100.0,20);
INSERT INTO emp VALUES (7900,08610012,'5A','JAMES','CLERK','锦尚路MOCO公寓10楼','whoMe@gmail.com',7698,10,to_date('3-12-1981','dd-mm-yyyy'),'2006/12/2','2005-9-10 5:00:00','2004-11-8 pst','12 DAY 12 HOUR',95,1000.22,30);
INSERT INTO emp VALUES (7902,08610012,'58','FORD','ANALYST','方西区正街3号巷66号','analyse666@163.com',7566,8,to_date('3-12-1981','dd-mm-yyyy'),'2012-12-23','2012-05-12 23:00:00','2011-03-21 CST','10 WEEK',199.23,2002.12,20);
INSERT INTO emp VALUES (7934,08610086,'59','MILLER','CLERK','四方区洛阳路34号3号楼4单元402户','Miller*mail@sina.com',7782,'10',to_date('23-1-1982','dd-mm-yyyy'),'2016-12-30','2015-10-12 24:00:00','2015-09-12 pst','40 DAY',112.23,10234.21,10);

analyze emp;

-- test for normal cases: different parameters or separator
SELECT mgrno, group_concat(empno ORDER BY empno SEPARATOR '.') AS empno_order_by_empno_group_by_mgr_integer FROM emp GROUP BY mgrno ORDER BY 1;
SELECT deptno, group_concat(ename ORDER BY ename SEPARATOR ',') AS ename_order_by_ename_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(phone ORDER BY phone SEPARATOR '-') AS phone_order_by_phone_int8 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(sign,workhour ORDER BY sign SEPARATOR 'α') AS sign_workhour_order_by_sign_blob FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct sign ORDER BY sign SEPARATOR '''') AS dst_sign_order_by_sign_blob FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(job ORDER BY job SEPARATOR '`') AS job_order_by_job_char FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct job ORDER BY job SEPARATOR '-') AS dst_job_order_by_job_char FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(address ORDER BY address SEPARATOR '//') AS address_order_by_address_text_zh FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct address ORDER BY address SEPARATOR '//') AS dst_address_order_by_address_text_zh FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(email ORDER BY email SEPARATOR '##') AS email_order_by_email_text_en FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(mgrno ORDER BY mgrno SEPARATOR 'separator') AS mgrno_order_by_mgrno_int4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct mgrno ORDER BY mgrno SEPARATOR 'separator') AS dst_mgrno_order_by_mgrno_int4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(workhour ORDER BY workhour SEPARATOR '(hours); ') AS workhour_order_by_workhour_int2 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(hiredate ORDER BY hiredate SEPARATOR ' 日期：') AS hiredate_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct hiredate ORDER BY hiredate SEPARATOR ' 日期：') AS dst_hiredate_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(offtime ORDER BY offtime SEPARATOR ' ~ ') AS offtime_order_by_offtime_timestamp FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct offtime ORDER BY offtime SEPARATOR ' ~ ') AS dst_offtime_order_by_offtime_timestamp FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(overtime ORDER BY overtime SEPARATOR ' & ') AS overtime_order_by_overtime_timestamptz FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(vacationTime ORDER BY vacationTime SEPARATOR '^') AS vacationTime_order_by_vtime_ASC_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct vacationTime ORDER BY vacationTime SEPARATOR '^') AS dst_vTime_order_by_vtime_ASC_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(termdate-hiredate ORDER BY 1 DESC SEPARATOR '; ') AS onwork_order_by_1_desc_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(termdate-hiredate ORDER BY termdate-hiredate DESC SEPARATOR '; ') AS onwork_order_by_time_desc_interval FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(salPerHour ORDER BY salPerHour DESC SEPARATOR '"') AS salPH_order_by_salPH_desc_float4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(salPerHour*workhour ORDER BY salPerHour*workhour SEPARATOR '\\') AS totalincome_order_by_tin_float8 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(CAST(workhour*7*30 AS INT8) ORDER BY CAST(workhour*7*30 AS INT8) DESC SEPARATOR '(Hours); ') AS hoursPerYear_order_by_hpy_DESC_int8 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(bonus ORDER BY bonus SEPARATOR '(￥); ') AS bonus_order_by_bonus_numeric FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct bonus ORDER BY bonus SEPARATOR '(￥); ') AS dst_bonus_order_by_bonus_numeric FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(termdate ORDER BY termdate ASC SEPARATOR '') AS termdate_order_by_termdate_date FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(mgrno ORDER BY mgrno SEPARATOR ' ') AS mgrno_order_by_mgrno_in4 FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(address ORDER BY NLSSORT(address, 'NLS_SORT=SCHINESE_PINYIN_M') SEPARATOR ' || ') AS address_order_by_pinyin_text FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, mgrno, group_concat(ename ORDER BY ename SEPARATOR ',') AS ename_order_by_ename_varchar FROM emp GROUP BY grouping sets(deptno,mgrno) ORDER BY 1,2;

-- SEPARATOR can only be used in GROUP_CONCAT
SELECT mgrno, max(empno ORDER BY empno SEPARATOR '.') AS empno_order_by_empno_group_by_mgr_integer FROM emp GROUP BY mgrno ORDER BY 1;
SELECT deptno, min(distinct sign ORDER BY sign SEPARATOR '''') AS dst_sign_order_by_sign_blob FROM emp GROUP BY deptno ORDER BY 1;

--test for group_concat return type
create view v_emp(id,sign) as SELECT deptno, group_concat(ename ORDER BY ename SEPARATOR ',') AS employees_order_by_ename_varchar FROM emp GROUP BY deptno ORDER BY 1;
\d v_emp;
select * from v_emp;
drop view v_emp;
create view v_emp(id,sign) as SELECT deptno, group_concat(sign ORDER BY sign SEPARATOR ',') AS employees_order_by_sign FROM emp GROUP BY deptno ORDER BY 1;
\d v_emp;
select * from v_emp;
drop view v_emp;
create view v_emp(id,sign) as SELECT deptno, group_concat(sign,ename ORDER BY sign SEPARATOR ',') AS employees_order_by_sign FROM emp GROUP BY deptno ORDER BY 1;
\d v_emp;
select * from v_emp;
drop view v_emp;

-- test for complex parameters
SELECT deptno, group_concat(bonus,offtime,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address,address ORDER BY address SEPARATOR '//') AS multi_address_order_by_address_text FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(NULL ORDER BY NULL) AS default_sep_NULL FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat('''拼接''') AS default_sep_const FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(ARRAY[ename,job]  ORDER BY job) AS array_orderby_job_default_sep_text FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct empno,ARRAY[ename,job]) AS empno_array_default_sep_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(VARIADIC ARRAY[ename,':',job]  ORDER BY ename) AS variadic_default_sep_varchar FROM emp GROUP BY deptno ORDER BY 1;
SELECT mgrno, group_concat(all empno ORDER BY ename using >) AS all_empno_orderby_ename_default_sep_int FROM emp GROUP BY mgrno ORDER BY 1;
SELECT deptno, group_concat(distinct empno,job ORDER BY job using >) AS dst_2args_orderby_1arg_default_sep FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(concat(address,sign) ORDER BY NLSSORT(address, 'NLS_SORT=SCHINESE_PINYIN_M') desc) AS func_order_by_pinyin_text_blob FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(email||email ORDER BY email,email||email desc) AS expr_orderby_2args_text FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(sign order by deptno,empno) AS mgrno_order_by_mgrno_noarg2_blob FROM emp GROUP BY deptno ORDER BY 1;

-- test for window function err cases
SELECT job, address, ename, group_concat(bonus SEPARATOR '￥') OVER(PARTITION BY job) AS bonus FROM emp ORDER BY 1,2,3;
SELECT mgrno, ename, job, group_concat(ename,job) OVER(PARTITION BY mgrno) AS employees_in_manager FROM emp ORDER BY 1,2,3;

-- test for plan changes, dfx
SET explain_perf_mode=pretty;
EXPLAIN (costs off) SELECT deptno, group_concat(ename ORDER BY ename SEPARATOR ',') AS employees_order_by_ename_varchar FROM emp GROUP BY deptno;
EXPLAIN (costs off) SELECT deptno, group_concat(sign ORDER BY email SEPARATOR '##') AS email_order_by_email_text_en FROM emp GROUP BY deptno;
EXPLAIN (costs off) SELECT deptno, group_concat(VARIADIC ARRAY[ename,':',job]  ORDER BY ename) AS bonus_order_by_bonus_numeric FROM emp GROUP BY deptno;

-- test for date print format
SET datestyle = 'SQL,DMY';
SELECT deptno, group_concat(hiredate ORDER BY hiredate SEPARATOR ', ') AS hiredate_dmy_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;
SET datestyle = 'SQL,MDY';
SELECT deptno, group_concat(hiredate ORDER BY hiredate SEPARATOR ', ') AS hiredate_dmy_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;
SET datestyle = 'Postgres,DMY';
SELECT deptno, group_concat(hiredate ORDER BY hiredate SEPARATOR ', ') AS hiredate_dmy_order_by_hiredate_date FROM emp GROUP BY deptno ORDER BY 1;

-- test for abnormal cases: alias, no column, no keyword, invalid parameter, other orders, etc. errors.
SELECT deptno, group_concat() AS no_args FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(ORDER BY ename) AS no_args FROM emp GROUP BY deptno;
SELECT deptno, group_concat(ORDER BY ename SEPARATOR ',') AS no_args FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(*) AS star_arg FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(* ORDER BY ename SEPARATOR ',') AS star_arg FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(ename ',') AS no_sep_keyword FROM emp GROUP BY deptno;
SELECT deptno, group_concat(ename ORDER BY ename SEPARATOR '(' || ')') AS expr_sep FROM emp GROUP BY deptno ORDER BY 1;
SELECT mgrno, group_concat(empno ORDER BY empno SEPARATOR ) AS no_sep FROM emp GROUP BY mgrno ORDER BY 1;
SELECT deptno, group_concat(ename SEPARATOR ', ' ORDER BY hiredate) AS wrong_syntax FROM emp GROUP BY deptno;
SELECT deptno, group_concat(distinct job SEPARATOR ',' ORDER BY job) AS wrong_syntax FROM emp GROUP BY deptno;
SELECT deptno, group_concat(distinct ename SEPARATOR '(' || MAX(deptno)|| ')' ORDER BY NULL) AS wrong_syntax FROM emp GROUP BY deptno;
SELECT deptno, group_concat(mgrno ORDER BY mgrno SEPARATOR NULL) AS null_sep FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(mgrno ORDER BY mgrno SEPARATOR ename) AS col_sep FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(ename alias ORDER BY alias) AS arg_alias FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(ename AS NM ORDER BY NM SEPARATOR ',') AS arg_alias FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct job ORDER BY empno) AS order_not_in_distinct FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(VARIADIC ename ORDER BY 1) AS wrong_variadic FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(empno, VARIADIC ARRAY[ename,':',job]  ORDER BY ename) AS wrong_variadic FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(distinct VARIADIC ARRAY[ename,':',job]  ORDER BY 1) AS wrong_variadic FROM emp GROUP BY deptno ORDER BY 1;
SELECT deptno, group_concat(ename) WITHIN GROUP(ORDER BY ename) AS within_clause FROM emp GROUP BY deptno ORDER BY 1;

-- test group_concat in pl/sql
CREATE TABLE test_group_concat_bin
(
BT_COL1 INTEGER,
BT_COL2 BLOB,
BT_COL3 RAW,
BT_COL4 BYTEA
) ;
INSERT INTO test_group_concat_bin VALUES(1, '3F', HEXTORAW('3F'),E'\\x3F');
INSERT INTO test_group_concat_bin VALUES(2, '21', HEXTORAW('21'),E'\\x21');

CREATE OR REPLACE PROCEDURE proc_insert_concat
IS
BEGIN
    INSERT INTO test_group_concat_bin
    SELECT deptno, 
        hextoraw(group_concat(sign order by sign separator '')), 
        hextoraw(group_concat(sign order by sign separator '')), 
        decode(group_concat(sign order by sign separator '') , 'hex') 
    FROM emp group by deptno;
END;
/
CALL proc_insert_concat();
DROP PROCEDURE proc_insert_concat;

-- test group_concat with blob/raw/bytea
select * from test_group_concat_bin order by bt_col1;
select group_concat(BT_COL2,BT_COL3,BT_COL4 order by BT_COL1 separator '') from test_group_concat_bin;
set bytea_output=escape;
select * from test_group_concat_bin order by bt_col1;
select group_concat(BT_COL2,BT_COL3,BT_COL4 order by BT_COL1 separator '') from test_group_concat_bin;

\c regression
clean connection to all force for database test_group_concat_B_db;
drop database test_group_concat_B_db;

create database test_group_concat_max_len dbcompatibility 'B';
\c test_group_concat_max_len;

CREATE TABLE t(id int, v text);
INSERT INTO t(id, v) VALUES(1, 'A'),(2, 'B'),(1, 'C'),(2, 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDD');

--select into statement
select group_concat(id,v separator ';') into tmp_table from t;
select * from tmp_table;

--show default value (current session)
show group_concat_max_len;
select id, group_concat(v separator ';') from t group by id order by id asc;

--alter database XXX set XXX to XXX (current session)
alter database test_group_concat_max_len set group_concat_max_len to 10;
show group_concat_max_len;
select id, group_concat(v separator ';') from t group by id order by id asc;
--new session
\c regression
\c test_group_concat_max_len
show group_concat_max_len;
select id, group_concat(v separator ';') from t group by id order by id asc;

--set XXX to XXX (current session)
set group_concat_max_len to 1;
--value changed
show group_concat_max_len;
select id, group_concat(v separator ';') from t group by id order by id asc;

--show database value above
\c regression
\c test_group_concat_max_len
show group_concat_max_len;
select id, group_concat(v separator ';') from t group by id order by id asc;

--error cases
set group_concat_max_len to -1;
set group_concat_max_len to 9223372036854775808;

\c regression
clean connection to all force for database test_group_concat_max_len;
drop database test_group_concat_max_len;