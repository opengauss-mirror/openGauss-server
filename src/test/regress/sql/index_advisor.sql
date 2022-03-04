/*
 * This file is used to test index advisor function
 */
create database pl_test_ind_adv DBCOMPATIBILITY 'pg';
\c pl_test_ind_adv;
CREATE TABLE t1 (col1 int, col2 int, col3 text);
INSERT INTO t1 VALUES(generate_series(1, 3000),generate_series(1, 3000),repeat( chr(int4(random()*26)+65),4));
ANALYZE t1;
CREATE TABLE t2 (col1 int, col2 int);
INSERT INTO t2 VALUES(generate_series(1, 1000),generate_series(1, 1000));
ANALYZE t2;
CREATE TEMP TABLE mytemp1 (col1 int, col2 int, col3 text);
INSERT INTO mytemp1 VALUES(generate_series(1, 3000),generate_series(1, 3000),repeat( chr(int4(random()*26)+65),4));
ANALYZE mytemp1;

---single query
--test where
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT * FROM t1 WHERE col1 = 10') as a;
--test join
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT * FROM t1 join t2 on t1.col1 = t2.col1') as a;
--test multi table
SELECT  a.schema, a.table, a.column  FROM gs_index_advise('SELECT count(*), t2.col1 FROM t1 join t2 on t1.col2 = t2.col2 WHERE t2.col2 > 2 GROUP BY t2.col1 ORDER BY t2.col1') as a;
--test order by
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT * FROM t1 ORDER BY 2') as a;
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT * FROM t1 as a WHERE a.col2 in (SELECT col1 FROM t2 ORDER BY 1) ORDER BY 2') as a;
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT * FROM t1 WHERE col1 > 10 ORDER BY 1,col2') as a;
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT *, *FROM t1 ORDER BY 2, 4') as a;
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT *, col2 FROM t1 ORDER BY 1, 3') as a;
--test string overlength
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT * FROM t1 where col3 in (''aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'',''bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'',''ccccccccccccccccccccccccccccccccccccccc'',''ddddddddddddddddddddddddddddddddddddddd'',''ffffffffffffffffffffffffffffffffffffffff'',''ggggggggggggggggggggggggggggggggggggggggggggggggggg'',''ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt'',''vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'',''ggmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm'')') as a;
--test union all
SELECT a.schema, a.table, a.column FROM gs_index_advise('select * from ((select col1, col2 from t1 where col1=1) union all (select col1, col2 from t2 where col1=1))') as a;
--test insert
SELECT a.schema, a.table, a.column FROM gs_index_advise('INSERT INTO t2 (SELECT col1, col2 from t1 where col1=1)') as a;
--test delete
SELECT a.schema, a.table, a.column FROM gs_index_advise('DELETE FROM t1 where col1 > (SELECT COUNT(*) from t1 where col1<1000)') as a;
--test update
SELECT a.schema, a.table, a.column FROM gs_index_advise('UPDATE t1 SET col1=(SELECT col2 from t2 where col1=10)') as a;
--test nested select
SELECT a.schema, a.table, a.column FROM gs_index_advise('select count(*) from (select t1.col1, t2.col2 from t1 join t2 on t1.col1 = t2.col1)') as a;
--test temp table
SELECT  a.schema, a.table, a.column FROM gs_index_advise('SELECT * FROM mytemp1 WHERE col1 = 10') as a;
--test complex sql
SELECT a.schema, a.table, a.column FROM gs_index_advise('select * from ((select t1.col1, t2.col2 from t1 join t2 on t1.col1 = t2.col1) union all (select col1, col2 from t1 where col1=col2 and col2>200 and col3 in (''aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'',''bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'',''ccccccccccccccccccccccccccccccccccccccc'',''ddddddddddddddddddddddddddddddddddddddd'',''ffffffffffffffffffffffffffffffffffffffff'',''ggggggggggggggggggggggggggggggggggggggggggggggggggg'',''ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt'',''vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'',''ggmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm'') order by col3)) order by col2 limit 10') as a;
SELECT a.schema, a.table, a.column FROM gs_index_advise('select * from ((SELECT t1.col1, t1.col2 from t1 where col1=col2 and col1<99) UNION ALL (select col2, col1 from t1 where col1=col2 and col2>200 order by col1 DESC)) as t3 join t2 on t3.col1 = t2.col1 where t2.col2=4 and t3.col1<100 and t2.col1=4 order by t3.col1, t2.col2 DESC limit 100') as a;


---virtual index
--test hypopg_create_index
SELECT * FROM hypopg_create_index('CREATE INDEX ON t1(col1)');
SELECT * FROM hypopg_create_index('CREATE INDEX ON t2(col1)');
SELECT * FROM hypopg_create_index('SELECT * from t1');
SELECT * FROM hypopg_create_index('UPDATE t2 SET col1=(SELECT col2 from t2 where col1=10)');
SELECT * FROM hypopg_create_index('DELETE from t2 where col1 <10');
SELECT * FROM hypopg_create_index('INSERT INTO t2 VALUES(generate_series(1001, 2000),generate_series(1001, 2000))');
--test explain
set enable_hypo_index = on;explain SELECT * FROM t1 WHERE col1 = 100;
explain UPDATE t1 SET col1=0 where col1=2;
explain UPDATE t1 SET col1=(SELECT col2 from t2 where col1=10);
explain INSERT INTO t1 SELECT * from t1 where col1=10;
explain DELETE FROM t1 where col1 > (SELECT COUNT(*) from t1 where col1<1000);

--test partition table
create table range_part_a(
stu_id varchar2(100),
stu_name varchar2(100),
sex varchar2(1),
credit integer default 0
)partition by range (credit)
(partition p_range_1 values less than (60),
partition p_range_2 values less than (120),
partition p_range_3 values less than (180),
partition p_range_4 values less than (240),
partition p_range_6 values less than (maxvalue)
);
create table range_part_b(
stu_id varchar2(100),
stu_name varchar2(100),
sex varchar2(1),
credit integer default 0
)partition by range (credit)
(partition p_range_1 values less than (60),
partition p_range_2 values less than (120),
partition p_range_3 values less than (180),
partition p_range_4 values less than (240),
partition p_range_6 values less than (maxvalue)
);
CREATE TABLE range_subpart_a(
col_1 int,
col_2 int,
col_3 VARCHAR2 ( 30 ) ,
col_4 int
)PARTITION BY RANGE (col_1) SUBPARTITION BY RANGE (col_2)
(PARTITION p_range_1 VALUES LESS THAN( 1000 )
  (SUBPARTITION p_range_1_1 VALUES LESS THAN( 50 ),
  SUBPARTITION p_range_1_2 VALUES LESS THAN( MAXVALUE )
  ),
PARTITION p_range_2 VALUES LESS THAN( 2001 )
  (SUBPARTITION p_range_2_1 VALUES LESS THAN( 50 ),
  SUBPARTITION p_range_2_2 VALUES LESS THAN( MAXVALUE )
  )
);
CREATE TABLE range_subpart_b(
col_1 int,
col_2 int,
col_3 VARCHAR2 ( 30 ) ,
col_4 int
)PARTITION BY RANGE (col_1) SUBPARTITION BY RANGE (col_2)
(PARTITION p_range_1 VALUES LESS THAN( 1000 )
  (SUBPARTITION p_range_1_1 VALUES LESS THAN( 50 ),
  SUBPARTITION p_range_1_2 VALUES LESS THAN( MAXVALUE )
  ),
PARTITION p_range_2 VALUES LESS THAN( 30001 )
  (SUBPARTITION p_range_2_1 VALUES LESS THAN( 50 ),
  SUBPARTITION p_range_2_2 VALUES LESS THAN( MAXVALUE )
  )
);
INSERT INTO range_part_a VALUES(repeat( chr(int4(random()*26)+65),4),repeat( chr(int4(random()*26)+65),4),repeat( chr(int4(random()*26)+65),1),generate_series(1, 2000));
ANALYZE range_part_a;
INSERT INTO range_part_b VALUES(repeat( chr(int4(random()*26)+65),4),repeat( chr(int4(random()*26)+65),4),repeat( chr(int4(random()*26)+65),1),generate_series(1, 3000));
ANALYZE range_part_b;
INSERT INTO range_subpart_a VALUES(generate_series(1, 2000),generate_series(1, 2000),repeat( chr(int4(random()*26)+65),1),generate_series(1, 2000));
ANALYZE range_subpart_a;
INSERT INTO range_subpart_b VALUES(generate_series(1, 3000),generate_series(1, 3000),repeat( chr(int4(random()*26)+65),1),generate_series(1, 3000));
ANALYZE range_subpart_b;

--single query
--test syntax error
select * from gs_index_advise('select * from range_part_a  as a where a.stu_id in (select stu_id from range_part_a order by 4)order by 4');

--test local index
--partion
select * from gs_index_advise('select * from range_part_a where credit = 4');
select * from gs_index_advise('select * from range_part_a where stu_id = ''10'' and credit = 4');
select * from gs_index_advise('select * from range_part_a partition(p_range_1) where stu_id = ''10''');
--subpartition
select * from gs_index_advise('select * from range_subpart_a partition(p_range_1) where col_2 = 2');
select * from gs_index_advise('select * from range_subpart_a subpartition(p_range_1_1 ) where col_3 =''2''');
select * from gs_index_advise('select * from range_subpart_a where col_1 =2 and col_2 = 3');

--test global index
--partion
select * from gs_index_advise('select * from range_part_a where stu_id = ''10''');
--subpartition
select * from gs_index_advise('select * from range_subpart_a where col_1 = 10');


--test subquery
--partition
select * from gs_index_advise('select * from range_part_a where stu_id = (select stu_id from range_part_a where stu_id=''10'') and credit = 2');
--subpartition
select * from gs_index_advise('select * from range_subpart_a where col_1 = (select col_2 from range_part_a where col_3=''10'') and col_2 = 2');

--test join
--partition
select * from gs_index_advise('select * from range_part_a join range_part_b on range_part_b.credit = range_part_a.credit  where range_part_a.stu_id = ''12''');
select * from gs_index_advise('select * from range_part_a join range_part_b partition(p_range_1) on range_part_a.stu_id = range_part_b.stu_id  where range_part_a.stu_id = ''12''');
select * from gs_index_advise('select * from range_part_a partition(p_range_1) join range_part_b partition(p_range_1) on range_part_a.stu_id = range_part_b.stu_id  where range_part_a.stu_id = ''12''');
--subpartition
select * from gs_index_advise('select * from range_subpart_a join range_subpart_b on range_subpart_b.col_2 = range_subpart_a.col_2  where range_subpart_a.col_3 = ''12''');
select * from gs_index_advise('select * from range_part_a join range_subpart_b on range_part_a.credit = range_subpart_b.col_2  where range_subpart_b.col_3 = ''12''');
select * from gs_index_advise('select * from range_subpart_a partition(p_range_1) join range_subpart_b subpartition(p_range_1_1) on range_subpart_a.col_3 = range_subpart_b.col_3  where range_subpart_a.col_3 = ''12''');

--virtual index
select * from hypopg_create_index('create index on range_part_a(credit) local');
select * from hypopg_create_index('create index on range_subpart_a(col_2) local');
explain select * from range_part_a where stu_id = '10' and credit = 2;
select * from hypopg_create_index('create index on range_part_a(credit)');
select * from hypopg_create_index('create index on range_subpart_a(col_2)');

--test hypopg_display_index
SELECT * FROM hypopg_display_index();
--test hypopg_reset_index
SELECT * FROM hypopg_reset_index();

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE range_part_a;
DROP TABLE range_part_b;
DROP TABLE range_subpart_a;
DROP TABLE range_subpart_b;
\c regression;
drop database IF EXISTS pl_test_ind_adv;
