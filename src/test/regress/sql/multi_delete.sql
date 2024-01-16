create database multidelete DBCOMPATIBILITY = 'B';
\c multidelete;
\h delete
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
-- subselect
begin;
delete t_t_mutil_t1,t_t_mutil_t2 from (select * from t_t_mutil_t3);
rollback;
begin;
delete from t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t3 c where a.col2=b.col2 and b.col2=c.col2;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
rollback;
-- delete xx from xxx;
begin;
delete t_t_mutil_t1 a from t_t_mutil_t2 b,t_t_mutil_t3 c where a.col2=b.col2 and b.col2=c.col2;
rollback;
begin;
delete a from t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t3 c where a.col2=b.col2 and b.col2=c.col2;
rollback;
begin;
delete t_t_mutil_t1 a,t_t_mutil_t2 b from t_t_mutil_t3 c where a.col2=b.col2 and b.col2=c.col2;
rollback;
begin;
delete a,b from t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t3 c where a.col2=b.col2 and b.col2=c.col2;
rollback;
begin;
delete a from t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2;
rollback;
delete a from t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error
delete a from t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 order by a.col2; -- error
delete a from t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 returning *; -- error
delete t_t_mutil_t1 a from t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error
-- condition is false
delete from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col1 = 1 and a.col1=2;
-- different plan
begin;
delete/*+nestloop(a b)*/ from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
begin;
delete/*+hashjoin(a b)*/ from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
begin;
delete/*+mergejoin(a b)*/ from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
-- left join
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(col1 int,col2 int);
create table t_mutil_t2(col1 int,col2 int);
insert into t_mutil_t1 values(1,2),(1,4);
insert into t_mutil_t2 values(2,3),(3,3);
delete from t_mutil_t1,t_mutil_t2 using t_mutil_t2 left join t_mutil_t1 on t_mutil_t1.col2=t_mutil_t2.col1;
select * from t_mutil_t1;
select * from t_mutil_t2;
-- procedure
CREATE OR REPLACE PROCEDURE proc_mutil
(
c1 int,
c2 int
)
IS
BEGIN
delete/*+nestloop(a b)*/ from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=c1 and b.col2=c2;
END;
/
begin;
CALL proc_mutil(1,2);
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;
-- delete plan_table
delete from t_t_mutil_t1, plan_table; --error
-- blockchain
CREATE SCHEMA multiblock2 WITH BLOCKCHAIN;
drop table if exists multiblock2.t_d_mutil_bc_1;
drop table if exists multiblock2.t_d_mutil_bc_2;
CREATE TABLE multiblock2.t_d_mutil_bc_1(col1 int, col2 int);
CREATE TABLE multiblock2.t_d_mutil_bc_2(col1 int, col2 int);
insert into multiblock2.t_d_mutil_bc_1 values(1,2),(2,2);
insert into multiblock2.t_d_mutil_bc_2 values(2,2),(3,3);
delete from multiblock2.t_d_mutil_bc_1 a,multiblock2.t_d_mutil_bc_2 b where a.col2=b.col2;
select * from multiblock2.t_d_mutil_bc_1;
select * from multiblock2.t_d_mutil_bc_2;
-- subquery
delete from t_t_mutil_t1 a,(select * from t_t_mutil_t2) b where a.col1=b.col1; --error
-- mateview
CREATE MATERIALIZED VIEW mate_multiview1 as select * from t_t_mutil_t1;
CREATE MATERIALIZED VIEW mate_multiview2 as select * from t_t_mutil_t2;
delete t_t_mutil_t1 a,mate_multiview1 b,mate_multiview2 c ;
drop MATERIALIZED VIEW mate_multiview1;
drop MATERIALIZED VIEW mate_multiview2;
-- different explain plan
explain(verbose) delete/*+nestloop(a b)*/ from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
explain(verbose) delete/*+hashjoin(a b)*/ from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
explain(verbose) delete/*+mergejoin(a b)*/ from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
explain(format xml) delete from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
explain(format json) delete from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
explain(format yaml) delete from t_t_mutil_t1 a,t_t_mutil_t2 b where a.col2=b.col2;
-- temp table
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
create temporary table t_t_mutil_t1(col1 int, col2 int);
create temporary table t_t_mutil_t2(col1 int,col2 int);
insert into t_t_mutil_t1 values(1,2),(2,2);
insert into t_t_mutil_t2 values(2,2),(3,3);
delete from  t_t_mutil_t1 a,t_t_mutil_t2 b where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
-- order by or limit
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(col1 int,col2 int);
create table t_mutil_t2(col1 int,col2 int);
insert into t_mutil_t1 values(1,2),(1,2),(3,3);
insert into t_mutil_t2 values(2,3),(2,3),(1,3);
delete from t_mutil_t1 a,t_mutil_t2 b where a.col1=b.col2 order by a.col2;  --error
delete from t_mutil_t1 a,t_mutil_t2 b where a.col1=b.col2 limit 1;    --error
select * from t_mutil_t1;
select * from t_mutil_t2;
-- returning
delete from t_mutil_t1 a,t_mutil_t2 b where a.col1=b.col2 returning *; --error
-- left join
begin;
delete from t_mutil_t1 a,t_mutil_t2 b using t_mutil_t1 a left join t_mutil_t2 b on a.col1=b.col1;
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
delete from t_u_mutil_t1 a,t_u_mutil_t2 b,t_u_mutil_t3 c where a.col2=b.col2 and b.col2=c.col2;
select * from t_u_mutil_t1;
select * from t_u_mutil_t2;
select * from t_u_mutil_t3;
-- cstore
drop table if exists t_c_mutil_t1;
drop table if exists t_c_mutil_t2;
create table t_c_mutil_t1(col1 int, col2 int) with(ORIENTATION=column);
create table t_c_mutil_t2(col1 int, col2 int) with(ORIENTATION=column);
delete from t_c_mutil_t1 a,t_c_mutil_t2 b  where a.col2=b.col2 --error;
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
insert into t_p_mutil_t2 values('201902', '1', '1', 1);
insert into t_p_mutil_t2 values('201902', '2', '1', 1);
insert into t_p_mutil_t2 values('201902', '1', '1', 1);
insert into t_p_mutil_t2 values('201903', '2', '1', 1);
insert into t_p_mutil_t2 values('201903', '1', '1', 1);
insert into t_p_mutil_t2 values('201903', '2', '1', 1);
begin;
delete from t_p_mutil_t1 a partition(p_201901_a, p_201901_b),t_p_mutil_t2 b partition(p_201901_a)  where a.user_no = 1 and b.user_no = 1;
select * from t_p_mutil_t1;
select * from t_p_mutil_t2;
rollback;
-- has index
drop table if exists t_mutil_t1;
drop table if exists t_mutil_t2;
create table t_mutil_t1(col1 int);
create table t_mutil_t2(col1 int,col2 int);
create index idx1 on t_mutil_t1(col1);
create index idx2 on t_mutil_t2(col1);
insert into t_mutil_t1 values(1),(2);
insert into t_mutil_t2 values(2,2),(3,3);
begin;
delete from t_mutil_t1 a,t_mutil_t2 b where a.col1=b.col1;
select * from t_mutil_t1;
select * from t_mutil_t2;
rollback;
-- synonym
CREATE SYNONYM s_mutil_t1 FOR t_mutil_t1;
CREATE SYNONYM s_mutil_t2 FOR t_mutil_t2;
begin;
delete from s_mutil_t1,s_mutil_t2 ;
delete from a,b using s_mutil_t1 a,s_mutil_t2 b where a.col1=b.col1;
delete from a,s_mutil_t2 using s_mutil_t1 a,s_mutil_t2 where a.col1=s_mutil_t2.col1;
delete from a,s_mutil_t2 using s_mutil_t1 a,s_mutil_t2,t_mutil_t1;
delete from a,b using s_mutil_t1 a,s_mutil_t2 b where a.col1=b.col1;
rollback;
-- ARdelete trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_delete_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl1 WHERE des1_id1=OLD.src1_id1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_delete_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl2 WHERE des2_id1=OLD.src2_id1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER delete_ar_trigger1
AFTER delete ON test_trigger_src_tbl1 
FOR EACH ROW
EXECUTE PROCEDURE tri_delete_func1();

CREATE TRIGGER delete_ar_trigger2
AFTER delete ON test_trigger_src_tbl2 
FOR EACH ROW
EXECUTE PROCEDURE tri_delete_func2();
delete test_trigger_src_tbl1 a,test_trigger_src_tbl2 b  WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- BRdelete trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_delete_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl1;
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_delete_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl2;
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER delete_ar_trigger1
BEFORE delete ON test_trigger_src_tbl1 
FOR EACH ROW
EXECUTE PROCEDURE tri_delete_func1();

CREATE TRIGGER delete_ar_trigger2
BEFORE delete ON test_trigger_src_tbl2 
FOR EACH ROW
EXECUTE PROCEDURE tri_delete_func2();
delete test_trigger_src_tbl1 a,test_trigger_src_tbl2 b WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- ASdelete trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_delete_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_delete_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl2;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER delete_ar_trigger1
BEFORE delete ON test_trigger_src_tbl1 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_delete_func1();

CREATE TRIGGER delete_ar_trigger2
BEFORE delete ON test_trigger_src_tbl2 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_delete_func2();
delete test_trigger_src_tbl1 a,test_trigger_src_tbl2 b WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- BSdelete trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_delete_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_delete_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl2;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER delete_ar_trigger1
BEFORE delete ON test_trigger_src_tbl1 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_delete_func1();

CREATE TRIGGER delete_ar_trigger2
BEFORE delete ON test_trigger_src_tbl2 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_delete_func2();
delete test_trigger_src_tbl1 a,test_trigger_src_tbl2 b WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- ASdelete trig
drop table if exists test_trigger_src_tbl1;
drop table if exists test_trigger_src_tbl2;
drop table if exists test_trigger_des_tbl1;
drop table if exists test_trigger_des_tbl2;
CREATE TABLE test_trigger_src_tbl1(src1_id1 INT, src1_id2 INT, src1_id3 INT);
CREATE TABLE test_trigger_src_tbl2(src2_id1 INT, src2_id2 INT, src2_id3 INT);
CREATE TABLE test_trigger_des_tbl1(des1_id1 INT, des1_id2 INT, des1_id3 INT);
CREATE TABLE test_trigger_des_tbl2(des2_id1 INT, des2_id2 INT, des2_id3 INT);
CREATE OR REPLACE FUNCTION tri_delete_func1() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl1;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION tri_delete_func2() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
delete test_trigger_des_tbl2;
RETURN OLD;
END
$$ LANGUAGE PLPGSQL;
INSERT INTO test_trigger_src_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_src_tbl2 VALUES(300,400,500);
INSERT INTO test_trigger_des_tbl1 VALUES(100,200,300);
INSERT INTO test_trigger_des_tbl2 VALUES(300,400,500);
CREATE TRIGGER delete_ar_trigger1
AFTER delete ON test_trigger_src_tbl1 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_delete_func1();

CREATE TRIGGER delete_ar_trigger2
AFTER delete ON test_trigger_src_tbl2 
FOR EACH STATEMENT
EXECUTE PROCEDURE tri_delete_func2();
delete test_trigger_src_tbl1 a,test_trigger_src_tbl2 b WHERE a.src1_id3=b.src2_id1;
SELECT * FROM test_trigger_src_tbl1;
SELECT * FROM test_trigger_src_tbl2;
SELECT * FROM test_trigger_des_tbl1; 
SELECT * FROM test_trigger_des_tbl2; 
-- delete alias
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
create table t_t_mutil_t1(col1 int,col2 int);
create table t_t_mutil_t2(col1 int,col2 int);
delete from t_t_mutil_t1 a,t_t_mutil_t2 b using t_t_mutil_t1 a,t_t_mutil_t2 b where a.col1=b.col1;
delete from a,b using t_t_mutil_t1 a,t_t_mutil_t2 b where a.col1=b.col1;
delete from t_t_mutil_t1 a,t_t_mutil_t2 b using a,b where a.col1=b.col1;
delete from t_t_mutil_t1 a where a.col1=1;
delete from t_t_mutil_t1 a using a where a.col1=1;
delete from a using t_t_mutil_t1 a where a.col1=1;
delete from t_t_mutil_t1 a using t_t_mutil_t2 a where a.col1=1;
delete from a using t_t_mutil_t1 a where a.col1=1;
delete from t_t_mutil_t1,t_t_mutil_t1 using t_t_mutil_t1,t_t_mutil_t1 where t_t_mutil_t1.col1=1;
delete from t_t_mutil_t1 using t_t_mutil_t1,t_t_mutil_t1 where t_t_mutil_t1.col1=1;
delete from t_t_mutil_t1 a,t_t_mutil_t1 a where a.col1=1;
delete from t_t_mutil_t1 a,t_t_mutil_t1 b where a.col1=1;
-- view
drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
drop table if exists t_t_mutil_t3;
create table t_t_mutil_t1(col1 int,col2 int);
create table t_t_mutil_t2(col1 int,col2 int,col3 int);
create table t_t_mutil_t3(col1 int,col2 int);
insert into t_t_mutil_t1 values(1,1),(1,1);
insert into t_t_mutil_t2 values(1,1),(1,2);
insert into t_t_mutil_t3 values(1,1),(1,3);
create view multiview1 as select * from t_t_mutil_t1;
create view multiview2 as select * from t_t_mutil_t2;
create view multiview3 as select * from t_t_mutil_t3;
delete multiview1 a,multiview2 b,multiview3 c where a.col1 = b.col1 and b.col2 = c.col2;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
select * from t_t_mutil_t3;
-- left join
insert into t_t_mutil_t1 values(1,1),(1,1);
insert into t_t_mutil_t2 values(1,1),(1,2);
delete from multiview1,t_t_mutil_t2 using t_t_mutil_t2 left join multiview1 on multiview1.col2=t_t_mutil_t2.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
drop view multiview1;
drop view multiview2;
drop view multiview3;

-- issue
WITH with_t1 AS ( SELECT TRUE AS c23 , -9213573085711696683 AS c49 ) DELETE with_t1 FROM with_t1;
DELETE subq_t1 FROM (SELECT 100 AS A) subq_t1;
DELETE func_t1 FROM generate_series(1, 10) func_t1;

-- support syntax like t.*
drop table  if exists delete_1;
create table delete_1(a int);
insert into delete_1 values(1),(1),(2),(2);
select * from delete_1;
delete t.* from delete_1 t where a = 1;
select * from delete_1;

drop table  if exists delete_2;
create table public.delete_2(a int);
insert into delete_2 values(1),(1),(2),(2);
select * from delete_2;
delete public.t.* from delete_2 t where a = 1;    -- mysql is error, can't recognize t is alias
select * from delete_2;
delete public.delete_2.* from delete_2 where a = 2;
select * from delete_2;

\c regression
drop database multidelete;
