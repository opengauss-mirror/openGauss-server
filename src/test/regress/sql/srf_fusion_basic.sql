DROP SCHEMA IF EXISTS srf_fusion_basic CASCADE;
CREATE SCHEMA srf_fusion_basic;
SET search_path = srf_fusion_basic;

------------------------------------------
-- sqlbypass_partition BEGIN
SET enable_expr_fusion = on;
SHOW enable_expr_fusion;
set enable_opfusion=on;
set enable_partition_opfusion=on;
set enable_bitmapscan=off;
set enable_seqscan=off;
set opfusion_debug_mode = 'log';
set log_min_messages=debug;
set logging_module = 'on(OPFUSION)';
set sql_beta_feature = 'index_cost_with_leaf_pages_only';
--create table
drop table if exists test_bypass_sql_partition;
create table test_bypass_sql_partition(col1 int, col2 int, col3 text)
partition by range (col1)
(
partition test_bypass_sql_partition_1 values less than(10),
partition test_bypass_sql_partition_2 values less than(20),
partition test_bypass_sql_partition_3 values less than(30),
partition test_bypass_sql_partition_4 values less than(40),
partition test_bypass_sql_partition_5 values less than(50),
partition test_bypass_sql_partition_6 values less than(60),
partition test_bypass_sql_partition_7 values less than(70),
partition test_bypass_sql_partition_8 values less than(80)
);
create index itest_bypass_sql_partition on test_bypass_sql_partition(col1,col2) local;
--nobypass
explain insert into test_bypass_sql_partition values(0,generate_series(1,100),'test');
insert into test_bypass_sql_partition values(0,generate_series(1,100),'test');

RESET enable_opfusion;
RESET enable_partition_opfusion;
RESET enable_bitmapscan;
RESET enable_seqscan;
RESET opfusion_debug_mode;
RESET log_min_messages;
RESET logging_module;
RESET sql_beta_feature;
-- sqlbypass_partition END
------------------------------------------
-- single_node_union BEGIN
SET enable_expr_fusion = on;
SHOW enable_expr_fusion;
-- Test that we push quals into UNION sub-selects only when it's safe
set query_dop = 1;
explain (verbose, costs off)
SELECT * FROM
  (SELECT 1 AS t, generate_series(1,10) AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;

SELECT * FROM
  (SELECT 1 AS t, generate_series(1,10) AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;
-- Test that single node stream plan handles UNION sub-selects correctly
set query_dop = 10; -- srf dont support this, so there is no ProjectSet in plan
explain (verbose, costs off)
SELECT * FROM
  (SELECT 1 AS t, generate_series(1,10) AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;

SELECT * FROM
  (SELECT 1 AS t, generate_series(1,10) AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;
RESET query_dop;
-- single_node_union END
------------------------------------------
-- merge_where_col BEGIN
SET enable_expr_fusion = on;
SHOW enable_expr_fusion;
--
-- MERGE INTO 
--

-- part 1
-- initial
DROP SCHEMA IF EXISTS srf_fusion_merge_where_col;
CREATE SCHEMA srf_fusion_merge_where_col;
SET current_schema = srf_fusion_merge_where_col;

drop table if exists merge_nest_tab1,dt2;
create table merge_nest_tab1(co1 numeric(20,4),co2 varchar2,co3 number,co4 date);
insert into merge_nest_tab1 values(generate_series(1,10),'hello'||generate_series(1,10),generate_series(1,10)*10,sysdate);
create table dt2(c1 numeric(20,4),c2 boolean,c3 character(40),c4 binary_double,c5 nchar(20)) WITH (ORIENTATION = COLUMN);
insert into dt2 values(generate_series(20,50),false,generate_series(20,50)||'gauss',generate_series(20,50)-0.99,'openopen');

-- we can't use columns of target table in insertion subquery(co1<45) for 'where'
BEGIN; 
merge into merge_nest_tab1 a
USING dt2 b
    ON a.co1=b.c1-20
    WHEN NOT matched THEN
    insert(co1,co2,co3) values(100,
    (SELECT 666)||'good',
        (SELECT sum(c.c1)
        FROM dt2 c
        INNER JOIN merge_nest_tab1 d
            ON c.c1=d.co1 ))
    WHERE co1<45;
END; 

-- we can use columns of source table in insertion subquery(c1<45) for 'where'
BEGIN; 
merge into merge_nest_tab1 a
USING dt2 b
    ON a.co1=b.c1-20
    WHEN NOT matched THEN
    insert(co1,co2,co3) values(100,
    (SELECT 666)||'good',
        (SELECT sum(c.c1)
        FROM dt2 c
        INNER JOIN merge_nest_tab1 d
            ON c.c1=d.co1 ))
    WHERE c1<45;
SELECT co1, co2, co3 FROM merge_nest_tab1 order by 1; 
ROLLBACK; 

-- we can use columns of source table in insert subquery(c1<45) for 'where'
BEGIN; 
merge into merge_nest_tab1 a
USING dt2 b
    ON a.co1=b.c1-20
    WHEN matched THEN
        UPDATE SET a.co3=a.co3 + b.c4,
         a.co2='hello',
         a.co4=(SELECT last_day(sysdate))
    WHERE c1 BETWEEN 1 AND 50;
SELECT co1, co2, co3 FROM merge_nest_tab1 order by 1; 
ROLLBACK; 

-- part 2
-- initial
drop table if exists tb_a,tb_b;

create table tb_a(id int, a int, b int, c int, d int);
create table tb_b(id int, a int, b int, c int, d int);
insert into tb_a values(1, 1, 1, 1, 1);
insert into tb_a values(2, 2, 2, 2, 2);
insert into tb_a values(3, 3, 3, 3, 3);
insert into tb_a values(4, 4, 4, 4, 4);

insert into tb_b values(1, 100, 1, 1, 1);
insert into tb_b values(2, 2, 2, 2, 2);
insert into tb_b values(3, 3, 3, 3, 3);
insert into tb_b values(4, 4, 4, 4, 4);

-- if the column has the same name, the column in the target table takes precedence
BEGIN; 
MERGE INTO tb_b bt
USING tb_a at
    ON (at.id = bt.id)
    WHEN MATCHED THEN
    UPDATE SET a = at.a + 100 WHERE a =100;
SELECT * FROM tb_b ORDER BY  1; 
ROLLBACK; 

create table col_com_base_1(
col_int                      integer,
col_double                   double precision,
col_date                     date
);

create table col_com_base_2(
col_int                      integer,
col_double                   double precision,
col_date                     date
);

MERGE INTO col_com_base_1 Table_004 USING col_com_base_2 Table_003
    ON ( Table_003.col_double = Table_004.col_double ) 
WHEN MATCHED THEN UPDATE SET col_date = col_date
WHERE  Table_004.col_int = ( select SUM(Table_004.col_int) from col_com_base_1);

UPDATE col_com_base_1 Table_004 SET col_int = 2 where Table_004.col_int = ( select SUM(Table_004.col_int) from col_com_base_1);
UPDATE col_com_base_1 Table_004 SET col_int = 2 where Table_004.col_int = ( select SUM(col_int) from col_com_base_1);

-- clean up
DROP SCHEMA srf_fusion_merge_where_col CASCADE;
-- merge_where_col END;
------------------------------------------
-- multi_update BEGIN
create database multiupdate DBCOMPATIBILITY = 'B';
\c multiupdate;
\h update
SET enable_expr_fusion = on;
SHOW enable_expr_fusion;
-- three relation
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
drop table if exists t_t_mutil_t3;
create table t_t_mutil_t1(col1 int,col2 int);
create table t_t_mutil_t2(col1 int,col2 int);
create table t_t_mutil_t3(col1 int,col2 int);
insert into t_t_mutil_t1 values(1,1),(1,1);
insert into t_t_mutil_t2 values(1,1),(1,2);
insert into t_t_mutil_t3 values(1,1),(1,3);
begin;
update t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t3 c set b.col2=5,a.col2=4,c.col2=6 where a.col1=b.col1 and b.col1=c.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col2=7,b.col2=8 where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
-- subselect
begin;
update t_t_mutil_t1 a,t_t_mutil_t2 b set b.col2=5,a.col2=4 where a.col1 in (select col1 from t_t_mutil_t2);
rollback;
-- setof type, report error
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col1 = generate_series(2,3), b.col1=1;
update t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1 = generate_series(2,3), a.col1=1;
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col1 = generate_series(2,3),b.col1 =123;
\c regression
drop database multiupdate;
-- multi_update END
------------------------------------------
-- vec_set_func BEGIN
SET enable_expr_fusion = on;
SHOW enable_expr_fusion;
set enable_vector_engine=on;
create table hl_test002(a int,b varchar2(15), c varchar2(15)); 
insert into hl_test002 values(1,'gauss,ap', 'xue,dong,pu'); 
insert into hl_test002 values(1,'gauss,ap', NULL); 
insert into hl_test002 values(1,'xue,dong,pu', 'gauss,ap,db'); 
insert into hl_test002 values(1,'xue,dong,pu', NULL); 
insert into hl_test002 values(2,'xi,an', 'wang,,rui'); 
insert into hl_test002 values(2,'xi,an', NULL); 
insert into hl_test002 values(2,'wang,,rui', 'xi,an'); 
insert into hl_test002 values(2,'wang,,rui', NULL);

create table hl_test001(a int,b varchar2(15), c varchar2(15)) with (ORIENTATION = COLUMN); 
insert into hl_test001 select * from hl_test002;

create table hl_test003(a int,b int[5]) with (ORIENTATION = COLUMN);
insert into hl_test003 values(1, array[1,2,3]),(2,array[5,4,6]);

select a,b,c,regexp_split_to_table(b,E',') from hl_test001 order by 1, 2, 3, 4 nulls last;
select a,b,c,regexp_split_to_table(b,NULL) from hl_test001 order by 1, 2, 3, 4 nulls last;
select a,b,c,regexp_split_to_table(b,E','), regexp_split_to_table(c,E',') from hl_test001 order by 1, 2, 3, 4, 5 nulls last;
explain (verbose, costs off)
select regexp_split_to_table(b,E','), generate_series(1, 3) from hl_test001;
select regexp_split_to_table(b,E','), generate_series(1, 3) from hl_test001;
select a, b, unnest(b) from hl_test003;

select a,b,c,regexp_split_to_table(regexp_split_to_table(b,E','), E'u') from hl_test001 order by 1, 2, 3, 4 nulls last;
select a,b,c,substring(regexp_split_to_table(b,E','), 1, 100) from hl_test001 order by 1, 2, 3, 4 nulls last;
select a,b,c,regexp_split_to_table(substring(b,1, 100), E',') from hl_test001 order by 1, 2, 3, 4 nulls last;

drop table hl_test001;
drop table hl_test002;
drop table hl_test003;
reset enable_vector_engine;
-- vec_set_func END
------------------------------------------
