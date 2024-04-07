create database multiupdate DBCOMPATIBILITY = 'B';
\c multiupdate;
\h update
-- issue
CREATE TEMPORARY TABLE t0 ( c54 INT , c9 INT ) ;
INSERT INTO t0 VALUES ( 25 , -8 ) , ( -88 , -77 ) ;
UPDATE t0 , ( SELECT t2 . c54 AS c17 FROM t0 , t0 AS t1 LEFT OUTER JOIN t0 AS t2 USING ( c9 , c54 ) ) AS t3 JOIN t0 AS t4 ON t4 . c9 = t4 . c54 NATURAL INNER JOIN t0 AS t5 SET t4 . c54 = -32 WHERE t4 . c54 = ( SELECT c54 AS c4 FROM t0 LIMIT 1 ) ;
WITH t6 AS ( SELECT c54 AS c12 FROM t0 ) SELECT t0 . c9 AS c9 FROM t0 CROSS JOIN t0 AS t7 WHERE t0 . c54 = -33 ;
drop table t0;
create table t0(c1 int, c2 int, c3 int, c4 int);
insert into t0 values(25, -8, -88, -8),(-88, -77, 25, -8);
update t0 a, t0 b set b.c1=10, a.c2=200, b.c3=20, a.c4=100;
select * from t0;
drop table t0;
drop table if exists tt0;
create table tt0(a int, b int);
insert into tt0 values(1,2),(2,3);
begin;
update tt0 a0, tt0 a1 set a0.a=5;
select * from tt0;
rollback;
begin;
update tt0 a, tt0 b set a.a=10;
select * from tt0;
rollback;
drop table tt0;
create type newtype as(a int, b int);
create table test(a newtype,b int);
insert into test values(ROW(1,2),3);
update test set test.a=ROW(10,20);
select * from test;
update test t set t.a=ROW(11,21);
select * from test;
--Ambiguous scene
--update field a of column a rather than column a of table a
update test a set a.a=12;
--update field b of column a rather than column b of table a
update test a set a.b=22;
select * from test;
--fail
update test a set a.a=ROW(13,23);
update test a set a.c=10;
update test b set b.c=10;
--must compatible with previous features, though not perfect
update test a set a.a.a=12;
select * from test;
drop table test;
drop type newtype;
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
-- condition is false
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col1=2,b.col1=2  where a.col1 = 1 and a.col1=2;
-- duplicate col
update t_t_mutil_t1 a,t_t_mutil_t2 b set col1= 3; -- error
begin;
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col1=3,t_t_mutil_t1.col2=3;
rollback;
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col1=3,t_t_mutil_t1.col1=3; --error
-- different plan
begin;
update/*+nestloop(a b)*/ t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
begin;
update/*+hashjoin(a b)*/ t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
begin;
update/*+mergejoin(a b)*/ t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
-- procedure
CREATE OR REPLACE PROCEDURE proc_mutil
(
c1 int,
c2 int
)
IS
BEGIN
update t_t_mutil_t1 a,t_t_mutil_t2 b set b.col2=c1, a.col2=c2 where a.col1=b.col1;
END;
/
CALL proc_mutil(5,6);
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
-- update one rel of two
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col2 =1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
update t_t_mutil_t1 a,t_t_mutil_t2 b set b.col2 =2;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
-- hash col
CREATE SCHEMA multiblock1 WITH BLOCKCHAIN;
drop table if exists multiblock1.t_mutil_bc_1;
drop table if exists multiblock1.t_mutil_bc_2;
CREATE TABLE multiblock1.t_mutil_bc_1(col1 int, col2 int);
CREATE TABLE multiblock1.t_mutil_bc_2(col1 int, col2 int);
insert into multiblock1.t_mutil_bc_1 values(1,2),(2,2);
insert into multiblock1.t_mutil_bc_2 values(2,2),(3,3);
update multiblock1.t_mutil_bc_1 a,multiblock1.t_mutil_bc_2 b set a.col2=5,b.col2=4 where a.col1=b.col1;
create table hashtable(hash int);
update multiblock1.t_mutil_bc_1 a,hashtable set hashtable.hash=1;
select * from multiblock1.t_mutil_bc_1;
select * from multiblock1.t_mutil_bc_2;
-- subquery
update t_t_mutil_t1 a,(select * from t_t_mutil_t2) b set b.col1=5,a.col2=4 where a.col1=b.col1; --error
-- mateview
CREATE MATERIALIZED VIEW mate_multiview1 as select * from t_t_mutil_t1;
CREATE MATERIALIZED VIEW mate_multiview2 as select * from t_t_mutil_t2;
update t_t_mutil_t1 a,mate_multiview1 b,mate_multiview2 c set a.col1 = 4, b.col2 = 5,c.col2 =6; --error
drop MATERIALIZED VIEW mate_multiview1;
drop MATERIALIZED VIEW mate_multiview2;
-- same relname
begin;
update t_t_mutil_t1 a,t_t_mutil_t1 b set a.col2=5,b.col2=4 where a.col1=b.col1;
select * from t_t_mutil_t1;
rollback;
update t_t_mutil_t1 a,t_t_mutil_t2 a set a.col2=5; --error
-- different explain plan
explain(verbose) update/*+nestloop(a b)*/ t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
explain(verbose) update/*+hashjoin(a b)*/ t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
explain(verbose) update/*+mergejoin(a b)*/ t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
explain(format xml) update public.t_t_mutil_t1 a,t_t_mutil_t2 b set b.col2=5,a.col2=4 where a.col1=b.col1;
explain(format json) update public.t_t_mutil_t1 a,t_t_mutil_t2 b set b.col2=5,a.col2=4 where a.col1=b.col1;
explain(format yaml) update public.t_t_mutil_t1 a,t_t_mutil_t2 b set b.col2=5,a.col2=4 where a.col1=b.col1;
-- temp table
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
create temporary table t_t_mutil_t1(col1 int, col2 int);
create temporary table t_t_mutil_t2(col1 int,col2 int);
insert into t_t_mutil_t1 values(1,2),(2,2);
insert into t_t_mutil_t2 values(2,2),(3,3);
update t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
-- order by or limit
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(col1 int,col2 int);
create table t_mutil_t2(col1 int,col2 int);
insert into t_mutil_t1 values(1,2),(1,2);
insert into t_mutil_t2 values(2,3),(2,3);
update t_mutil_t1 a,t_mutil_t2 b set a.col2=4,b.col1=4 where a.col1=b.col2 order by a.col2;  --error
update t_mutil_t1 a,t_mutil_t2 b set a.col2=4,b.col1=4 where a.col1=b.col2 limit 1;    --error
select * from t_mutil_t1;
select * from t_mutil_t2;
-- returning
update t_mutil_t1 a,t_mutil_t2 b set a.col2=4,b.col1=4 where a.col1=b.col2 returning *; --error
-- left join
begin;
update t_mutil_t1 a left join t_mutil_t2 b on a.col1=b.col1 set a.col2=7,b.col2=8;
select * from t_mutil_t1;
select * from t_mutil_t2;
rollback;
-- ustore
drop table if exists t_u_mutil_t1;
drop table if exists t_u_mutil_t2;
drop table if exists t_u_mutil_t3;
create table t_u_mutil_t1(col1 int, col2 int) with(storage_type = ustore);
create table t_u_mutil_t2(col1 int, col2 int) with(storage_type = ustore);
create table t_u_mutil_t3(col1 int, col2 int) with(storage_type = ustore);
insert into t_u_mutil_t1 values(1,2);
insert into t_u_mutil_t2 values(1,2);
insert into t_u_mutil_t3 values(1,2);
update t_u_mutil_t1 a,t_u_mutil_t2 b,t_u_mutil_t3 c set a.col1=3,b.col1=3,c.col1=3 where a.col2=b.col2 and b.col2=c.col2;
select * from t_u_mutil_t1;
select * from t_u_mutil_t2;
select * from t_u_mutil_t3;
-- cstore
drop table if exists t_c_mutil_t1;
drop table if exists t_c_mutil_t2;
create table t_c_mutil_t1(col1 int, col2 int) with(ORIENTATION=column);
create table t_c_mutil_t2(col1 int, col2 int) with(ORIENTATION=column);
update t_c_mutil_t1 a,t_c_mutil_t2 b set a.col1=3,b.col1=3 where a.col2=b.col2 --error;
-- partition
drop table if exists t_p_mutil_t1;
drop table if exists t_p_mutil_t2;
create table t_p_mutil_t1(t1 int,t2 int)
partition by hash(t1)
(
partition p1,
partition p2
);
create table t_p_mutil_t2(t1 int,t2 int)
partition by hash(t1)
(
partition p1,
partition p2
);
insert into t_p_mutil_t1 values(1,2),(2,3);
insert into t_p_mutil_t2 values(2,3),(3,4);
update t_p_mutil_t1 a,t_p_mutil_t2 b set a.t1=5,b.t2=6 where a.t2=b.t1;
select * from t_p_mutil_t1;
select * from t_p_mutil_t2;
-- subpartition
drop table if exists t_p_mutil_t1;
drop table if exists t_p_mutil_t2;
CREATE TABLE t_p_mutil_t1
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into t_p_mutil_t1 values('201902', '1', '1', 1);
insert into t_p_mutil_t1 values('201902', '2', '1', 1);
insert into t_p_mutil_t1 values('201902', '1', '1', 1);
insert into t_p_mutil_t1 values('201903', '2', '1', 1);
insert into t_p_mutil_t1 values('201903', '1', '1', 1);
insert into t_p_mutil_t1 values('201903', '2', '1', 1);

CREATE TABLE t_p_mutil_t2
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int,
	c1 int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into t_p_mutil_t2 values('201902', '1', '1', 1,2);
insert into t_p_mutil_t2 values('201902', '2', '1', 1,2);
insert into t_p_mutil_t2 values('201902', '1', '1', 1,2);
insert into t_p_mutil_t2 values('201903', '2', '1', 1,2);
insert into t_p_mutil_t2 values('201903', '1', '1', 1,2);
insert into t_p_mutil_t2 values('201903', '2', '1', 1,2);
begin;
update t_p_mutil_t1 a,t_p_mutil_t2 b set a.user_no=3,b.user_no=4 where a.user_no=1 and b.user_no=1;
select * from t_p_mutil_t1;
select * from t_p_mutil_t2;
rollback;
-- default col
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(col1 int default 1,col2 int);
create table t_mutil_t2(col1 int,col2 int default 3);
insert into t_mutil_t1 values(default,2);
insert into t_mutil_t2 values(2,default);
update t_mutil_t1 a,t_mutil_t2 b set a.col2=4,b.col1=4 where a.col2=b.col1; 
select * from t_mutil_t1;
select * from t_mutil_t2;
update t_mutil_t1 a,t_mutil_t2 b set a.col1=5,b.col2=6 where a.col2=b.col1; 
select * from t_mutil_t1;
select * from t_mutil_t2;
update t_mutil_t1 a,t_mutil_t2 b set a.col1=default,b.col2=default where a.col2=b.col1; 
select * from t_mutil_t1;
select * from t_mutil_t2;
-- generated col
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(col1 int generated always as (col2*2) stored,col2 int);
create table t_mutil_t2(col1 int,col2 int generated always as (col1*3) stored);
insert into t_mutil_t1 values(default,2);
select * from t_mutil_t1;
update t_mutil_t1 set col1 = default,col2=4;
select * from t_mutil_t1;
-- has index
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(col1 int);
create table t_mutil_t2(col1 int,col2 int);
create index idx1 on t_mutil_t1(col1);
create index idx2 on t_mutil_t2(col1);
insert into t_mutil_t1 values(1),(2);
insert into t_mutil_t2 values(2,2),(3,3);
update t_mutil_t1 a,t_mutil_t2 b set a.col1=4,b.col1=5 where a.col1=b.col1;
select * from t_mutil_t1;
select * from t_mutil_t2;
-- primary key
drop table if exists t_mutil_primary;
create table t_mutil_primary(col1 int primary key);
insert into t_mutil_primary values(1),(2);
update t_mutil_primary a,t_mutil_primary b set a.col1=3,b.col1=3;
-- col of one table is the name of the other table
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(t_mutil_t2 int);
create table t_mutil_t2(col1 int);
insert into t_mutil_t1 values(1);
insert into t_mutil_t2 values(2);
update t_mutil_t1,t_mutil_t2 set t_mutil_t1.t_mutil_t2=3,t_mutil_t2.col1=4;
select * from t_mutil_t1;
select * from t_mutil_t2;
update t_mutil_t1,t_mutil_t2 set t_mutil_t2.col1=4,t_mutil_t1.t_mutil_t2=3;
-- duplicate tablename
update t_mutil_t1,t_mutil_t1 set t_mutil_t1.t_mutil_t2 = 5; -- error
-- duplicate colname
update t_mutil_t1 set t_mutil_t1.t_mutil_t2=4,t_mutil_t1.t_mutil_t2=5; -- error
update t_mutil_t1,t_mutil_t2 set t_mutil_t1.t_mutil_t2=4,t_mutil_t1.t_mutil_t2=5; -- error
-- synonym
CREATE SYNONYM s_mutil_t1 FOR t_mutil_t1;
CREATE SYNONYM s_mutil_t2 FOR t_mutil_t2;
begin;
update s_mutil_t1,s_mutil_t2 set s_mutil_t1.t_mutil_t2=3,s_mutil_t2.col1=4;
select * from t_mutil_t1;
select * from t_mutil_t2;
rollback;
-- ARupdate trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_update_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl1 SET des1_id3 = NEW.src1_id3 WHERE des1_id1=OLD.src1_id1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_update_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl2 SET des2_id3 = NEW.src2_id3 WHERE des2_id1=OLD.src2_id1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER update_ar_trigger1
AFTER UPDATE ON test_trigger_src_tbl1 
FOR EACH ROW
EXECUTE PROCEDURE tri_update_func1();

CREATE TRIGGER update_ar_trigger2
AFTER UPDATE ON test_trigger_src_tbl2 
FOR EACH ROW
EXECUTE PROCEDURE tri_update_func2();
UPDATE test_trigger_src_tbl1 a,test_trigger_src_tbl2 b SET a.src1_id3=700,b.src2_id1=600 WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- BRupdate trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_update_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl1 SET des1_id3 = 800;
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_update_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl2 SET des2_id3 = 800;
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER update_ar_trigger1
BEFORE UPDATE ON test_trigger_src_tbl1 
FOR EACH ROW
EXECUTE PROCEDURE tri_update_func1();

CREATE TRIGGER update_ar_trigger2
BEFORE UPDATE ON test_trigger_src_tbl2 
FOR EACH ROW
EXECUTE PROCEDURE tri_update_func2();
UPDATE test_trigger_src_tbl1 a,test_trigger_src_tbl2 b SET a.src1_id3=700,b.src2_id1=600 WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- ASupdate trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_update_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl1 SET des1_id3 = 800;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_update_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl2 SET des2_id3 = 900;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER update_ar_trigger1
BEFORE UPDATE ON test_trigger_src_tbl1 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_update_func1();

CREATE TRIGGER update_ar_trigger2
BEFORE UPDATE ON test_trigger_src_tbl2 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_update_func2();
UPDATE test_trigger_src_tbl1 a,test_trigger_src_tbl2 b SET a.src1_id3=700,b.src2_id1=600 WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- BSupdate trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_update_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl1 SET des1_id3 = 800;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_update_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl2 SET des2_id3 = 900;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER update_ar_trigger1
BEFORE UPDATE ON test_trigger_src_tbl1 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_update_func1();

CREATE TRIGGER update_ar_trigger2
BEFORE UPDATE ON test_trigger_src_tbl2 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_update_func2();
UPDATE test_trigger_src_tbl1 a,test_trigger_src_tbl2 b SET a.src1_id3=700,b.src2_id1=600 WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- ASupdate trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_update_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl1 SET des1_id3 = 800;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_update_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE test_trigger_des_tbl2 SET des2_id3 = 900;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER update_ar_trigger1
AFTER UPDATE ON test_trigger_src_tbl1 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_update_func1();

CREATE TRIGGER update_ar_trigger2
AFTER UPDATE ON test_trigger_src_tbl2 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_update_func2();
UPDATE test_trigger_src_tbl1 a,test_trigger_src_tbl2 b SET a.src1_id3=700,b.src2_id1=600 WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- new type
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
create type newtype as(a int, b int);
create table t_t_mutil_t1(col1 newtype,col2 int);
create table t_t_mutil_t2(col1 newtype,col2 int);
insert into t_t_mutil_t1 values(ROW(1,2),3);
insert into t_t_mutil_t2 values(ROW(10,20),3);
update t_t_mutil_t1 a,t_t_mutil_t2 b set a.col1.a=3,b.col1.b=4;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
-- new set type
drop table if exists settest01;
drop table if exists settest07;
create table settest07(c1 int,c2 SET('开席','上菜','夹菜'));
insert into settest07 values(5,3);   
insert into settest07 values(1,'开席');
create table settest01(id int,rowid number);
insert into settest01 values(3,5);
update settest01 a,settest07 b set b.c2=4 where a.id>b.c1;

drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
drop table if exists t_t_mutil_t3;
create table t_t_mutil_t1(col1 int,col2 int);
create table t_t_mutil_t2(col1 int,col2 int,col3 int);
create table t_t_mutil_t3(col1 int,col2 int);
insert into t_t_mutil_t1 values(1,1),(1,1);
insert into t_t_mutil_t2 values(1,1),(1,2);
insert into t_t_mutil_t3 values(1,1),(1,3);
begin;
update t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t3 c set b.col2=5,a.col2=4 where a.col1=b.col1 and b.col1=c.col1;
rollback;

begin;
update t_t_mutil_t1 t1,t_t_mutil_t2 t2 set t2.col2 = 3, t1.col1 = 2;
select * from t_t_mutil_t1;
rollback;
begin;
update t_t_mutil_t1 t1, t_t_mutil_t1 t2, t_t_mutil_t1 t3 set t2.col2 = 3, t3.col1 = 2;
rollback;
begin;
update t_t_mutil_t2 t1, t_t_mutil_t2 t2, t_t_mutil_t2 t3 set t1.col1 = 3, t3.col2 = 2, t2.col3 = 3;
rollback;
begin;
update t_t_mutil_t1 t1 inner join t_t_mutil_t2 t2 on t1.col1=t2.col1 inner join t_t_mutil_t3 t3 on t1.col1=t3.col1 set t1.col2=3,t2.col2=4;
rollback;

CREATE SYNONYM s_t_mutil_t1 FOR t_t_mutil_t1;
CREATE SYNONYM s_t_mutil_t2 FOR t_t_mutil_t1;
begin;
update t_t_mutil_t1 t1,s_t_mutil_t1 set s_t_mutil_t1.col2 = 3, t1.col1 = 2;
select * from t_t_mutil_t1;
rollback;
begin;
update s_t_mutil_t2,s_t_mutil_t1 set s_t_mutil_t1.col2 = 3, s_t_mutil_t2.col1 = 2;
select * from t_t_mutil_t1;
rollback;
begin;
update s_t_mutil_t2,s_t_mutil_t1 set s_t_mutil_t1.col2 = 3, s_t_mutil_t2.col2 = 4;
select * from t_t_mutil_t1;
rollback;
-- view
create view multiview1 as select * from t_t_mutil_t1;
create view multiview2 as select * from t_t_mutil_t2;
create view multiview3 as select * from t_t_mutil_t3;
update multiview1 a,multiview2 b,multiview3 c set a.col2 = 6, b.col2 = 7,c.col2 =8 where a.col1 = b.col1 and a.col1 = c.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
update t_t_mutil_t1 a,multiview2 b,t_t_mutil_t3 c set a.col2 = 4, b.col2 = 5,c.col2 = 6 where a.col1 = b.col1 and a.col1 = c.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
alter table t_t_mutil_t1 alter column col2 set default 999;
update t_t_mutil_t1 a,multiview2 b,t_t_mutil_t3 c set a.col2 = default, b.col2 = 5,c.col2 = 6 where a.col1 = b.col1 and a.col1 = c.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
-- left join
update multiview1 a left join multiview2 b on a.col1=b.col1 set a.col2=7,b.col2=8;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
-- with check option
create or replace view multiview1 as select * from t_t_mutil_t1 where col2 > 3 with local check option;
create or replace view multiview2 as select * from t_t_mutil_t2 where col2 < 10 with local check option;
update multiview1 a,multiview2 b,multiview3 c set a.col2 = 2, b.col2 = 7,c.col2 =8 where a.col1 = b.col1 and a.col1 = c.col1; --error
update multiview1 a,multiview2 b,multiview3 c set a.col2 = 5, b.col2 = 15,c.col2 =8 where a.col1 = b.col1 and a.col1 = c.col1; --error
update multiview1 a,multiview2 b,multiview3 c set a.col2 = 5, b.col2 = 7,c.col2 =8 where a.col1 = b.col1 and a.col1 = c.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
-- update same relation
update t_t_mutil_t1 a,multiview2 b,t_t_mutil_t2 c,t_t_mutil_t3 d set a.col2 = 6, b.col2 = 9, c.col3 = 10, d.col2 = 6 where a.col1 = b.col1 and a.col1 = d.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
-- view with rules or triggers should fail
create rule multiview1_rule as on insert to multiview1 do instead insert into t_t_mutil_t1 values (new.col1, new.col2);
update multiview1 a,multiview2 b,t_t_mutil_t3 c set a.col2 = 6, b.col2 = 7,c.col2 =8 where a.col1 = b.col1 and a.col1 = c.col1; --error
drop rule multiview1_rule on multiview1;
CREATE OR REPLACE FUNCTION trigger_func_update_multiview1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
UPDATE t_t_mutil_t1 SET col2 = NEW.col2 WHERE col1=OLD.col1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER update_multiview1_trigger
INSTEAD OF UPDATE ON multiview1 
FOR EACH ROW
EXECUTE PROCEDURE trigger_func_update_multiview1();
update multiview1 a,multiview2 b,t_t_mutil_t3 c set a.col2 = 6, b.col2 = 7,c.col2 =8 where a.col1 = b.col1 and a.col1 = c.col1; --error
drop trigger update_multiview1_trigger on multiview1;
drop function trigger_func_update_multiview1();
drop view multiview1;
drop view multiview2;
drop view multiview3;
-- multi update with join
CREATE TABLE t1 (f1 int);
CREATE TABLE t2 (f1 int);
INSERT INTO t2  VALUES (1);
CREATE VIEW v1 AS SELECT * FROM t2;

UPDATE t2 AS A NATURAL JOIN v1 B SET B.f1 = 1;
drop table t1;
drop view v1;
drop table t2;

-- except no core
set query_dop = 6;
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
drop table if exists t_t_mutil_t3;
create table t_t_mutil_t1(col1 int,col2 int);
create table t_t_mutil_t2(col1 int,col2 int);
create table t_t_mutil_t3(col1 int,col2 int);
insert into t_t_mutil_t1 values(generate_series(1,1000000),generate_series(1,1000000));
insert into t_t_mutil_t2 values(generate_series(1,1000000),generate_series(1,1000000));
insert into t_t_mutil_t3 values(generate_series(1,1000000),generate_series(1,1000000));
explain(costs off, verbose) update/*+nestloop(a b)*/ t_t_mutil_t1 a,t_t_mutil_t2 b set b.col1=5,a.col2=4 where a.col1=b.col1;
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
drop table if exists t_t_mutil_t3;
reset query_dop;

\c regression
drop database multiupdate;
