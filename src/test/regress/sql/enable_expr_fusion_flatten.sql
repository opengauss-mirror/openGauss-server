show enable_expr_fusion;
set enable_expr_fusion = on;
show enable_expr_fusion;

drop table if exists t1;
create table t1 (id int, score int, pk int);
insert into t1 values(1,1,1);
insert into t1 values(1,2,1);
insert into t1 values(1,3,1);
insert into t1 values(2,1,1);
insert into t1 values(1,1,2);

select grouping(id,score), sum(pk), id, score from t1 group by rollup(id,score) order by 
grouping, sum, id, score;
select * from t1 where (id =10) or (score = 10) or (pk =1 and score = 2 and id =1);
select * from t1 where id is unknown;
select * from t1 where id is not unknown;
select rownum from t1 ;

select * from t1 where id is distinct from score;

create type newtype as (a int ,b int);
create table test_flt(a newtype, b int);
insert into test_flt values (ROW(1,2),3);
update test_flt a set a.a = 12;
select * from test_flt;


drop table if exists test_flt;
drop type newtype;

select (1,2) > (3,4);
select (1,2) >= (3,4);
select (1,2) < (3,4);
select (1,2) <= (3,4);

select id is true from t1;
select id is false from t1;
select id is not true from t1;
select id is not false from t1;

select * from t1 where id is not null;
select nullif(id,'') from t1;

create table employees (
    salary numeric CHECK(salary > 0)
);

insert into employees values (-100);

drop table if exists employees;

create sequence sequence_test2_flt start with 32;
create sequence  sequence_test3
  increment by 1 
  minvalue 1 maxvalue 30 
  start 1 
  cache 5;

SELECT * FROM information_schema.sequences WHERE sequence_name IN
  ('sequence_test2_flt', 'sequence_test3', 'serialtest2_f3_seq',
   'serialtest2_f4_seq', 'serialtest2_f5_seq', 'serialtest2_f6_seq')
  ORDER BY sequence_name ASC;

CREATE TYPE compfoo AS (f1 int, f2 text);
create table t_group_array (a compfoo[], b int, c int[]);
insert into t_group_array (a[5].f1) values(32);
insert into t_group_array values(array[cast((1,'syr') as compfoo),cast((2,'sss') as compfoo)],1,array[1,2]);
insert into t_group_array (a[5].f1) values(32);
update t_group_array set a[5].f2='sss';
drop table t_group_array;
drop type compfoo;


create table test_d(a int[3],b int);
create table test_s(a int[3],b int);
insert into test_d values('{1,2,3}',4);
insert into test_s values('{10,20,30}',4);
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a=test_s.a;
select * from test_d;
truncate table test_s;
insert into test_s values('{11,21,31}',4);
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a=test_s.a;
select * from test_d;
--must compatible with previous features, though not perfect
merge into test_d using test_s on(test_d.b=test_s.b) when matched then update set test_d.a[1,3]=test_s.a[1,3];
select * from test_d;
merge into test_d d using test_s on(d.b=test_s.b) when matched then update set d.a[1,3]=test_s.a[1,3];
select * from test_d;
drop table test_d;
drop table test_s;

create table xcreturn_tab1 (id int, v varchar);
ALTER TABLE xcreturn_tab1 ADD PRIMARY KEY(id);
create table xcreturn_tab2 (id int, v varchar);
ALTER TABLE xcreturn_tab2 ADD PRIMARY KEY(id);
insert into xcreturn_tab1 values (1, 'firstrow'), (2, 'secondrow');
WITH wcte AS ( INSERT INTO xcreturn_tab2 VALUES (999, 'opop'), (333, 'sss') , ( 42, 'new' ), (55, 'ppp') RETURNING id AS newid )
UPDATE xcreturn_tab1 SET id = id + newid FROM wcte;

drop table xcreturn_tab1, xcreturn_tab2;



CREATE TABLE INT4_FLT(f1 int4);

INSERT INTO INT4_FLT(f1) VALUES ('   0  ');

INSERT INTO INT4_FLT(f1) VALUES ('123456     ');

INSERT INTO INT4_FLT(f1) VALUES ('    -123456');

INSERT INTO INT4_FLT(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_FLT(f1) VALUES ('2147483647');

INSERT INTO INT4_FLT(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
INSERT INTO INT4_FLT(f1) VALUES ('1000000000000');
INSERT INTO INT4_FLT(f1) VALUES ('asdf');
INSERT INTO INT4_FLT(f1) VALUES ('     ');
INSERT INTO INT4_FLT(f1) VALUES ('   asdf   ');
INSERT INTO INT4_FLT(f1) VALUES ('- 1234');
INSERT INTO INT4_FLT(f1) VALUES ('123       5');
INSERT INTO INT4_FLT(f1) VALUES ('');

select q from (select max(f1) from int4_FLT order by f1) q;
drop table INT4_FLT;

with r(a,b) as materialized
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1 (a int primary key, b int, c int);
create table t2 (a int primary key, b int, c int);
create table t3 (a int primary key, b int, c int);
-- insert some data to suppress no statistics warning
insert into t1 values(1,1,1);
insert into t2 values(1,1,1);
insert into t3 values(1,1,1);

EXPLAIN  (verbose, costs off) select * from t1 where exists (select /*+ noEXPLAIN  (verbose, costs off)and*/ t2.a from t2 where t2.a = t1.a);
select * from t1 where exists (select /*+ noEXPLAIN  (verbose, costs off)and*/ t2.a from t2 where t2.a = t1.a);

EXPLAIN  (verbose, costs off) select * from t1 where t1.a in (select /*+ noEXPLAIN  (verbose, costs off)and*/ /*+ noEXPLAIN  (verbose, costs off)and noEXPLAIN  (verbose, costs off)and*/ t2.a from t2);
select * from t1 where t1.a in (select /*+ noEXPLAIN  (verbose, costs off)and*/ /*+ noEXPLAIN  (verbose, costs off)and noEXPLAIN  (verbose, costs off)and*/ t2.a from t2);


drop table if exists t1;
drop table if exists t2;
drop table if exists t3;


create database test_select_into_var dbcompatibility 'b';
\c test_select_into_var
set enable_expr_fusion = on;
drop table if exists t;
create table t(i int, t text, b bool, f float, bi bit(3), vbi bit varying(5));
insert into t(i, t, b, f, bi, vbi)
values(1, 'aaa', true, 1.11, B'101', B'00'),
      (2, 'bbb', false, 2.22, B'100', B'10'),
      (3, null, true, 3.33, B'101', B'00'),
      (4, 'ddd', null, 4.44, B'100', B'10'),
      (5, 'eee', false, null, B'101', B'00'),
      (6, 'fff', true, 6.66, null, B'00'),
      (7, 'ggg', false, 7.77, B'100', null),
      (null, 'hhh', true, 8.88, B'101', B'10');
select * from t where i in (1,2);


\c regression
drop database if exists test_select_into_var;
set enable_expr_fusion = on;

create table t1(a int);
create server my_server foreign data wrapper file_fdw;
create foreign table test_fore(id int) server my_server options(filename '/tmp');
select table_name,external from adm_tables where table_name = 'test_fore' or table_name = 't1';
drop table t1;
drop foreign table test_fore;

select ROW(1,2,NULL) < ROW(1,3,0);

select * from ADM_ARGUMENTS where OBJECT_NAME = 'proarg01';

create table t1 (c1 int primary key, c2 int);
create index t1_c2_idx on t1 using btree(c2);
create table t2 (c1 int, c2 int);
create index t2_c2_idx on t2 using btree(c2);
-- insert some data to suppress no statistics warning
insert into t1 values(1,1);
insert into t2 values(1,1);

insert into t1 select c1, c2 from t2 where c2 = 1 on duplicate key update c2 = c2 + 1;

drop table t1;
drop table t2; 

CREATE TABLE hash_hash
(
    col_1 int ,
    col_2 int NOT NULL ,
    col_3 VARCHAR2 ( 30 ) ,
    col_4 int
)
PARTITION BY hash (col_3) SUBPARTITION BY hash (col_2)
(
    PARTITION p_hash_1
    (
        SUBPARTITION p_hash_1_1 ,
        SUBPARTITION p_hash_1_2 ,
        SUBPARTITION p_hash_1_3 ,
        SUBPARTITION p_hash_1_4
    ),
    PARTITION p_hash_2
    (
        SUBPARTITION p_hash_2_1 ,
        SUBPARTITION p_hash_2_2
    ),
    PARTITION p_hash_3,
    PARTITION p_hash_4
    (
        SUBPARTITION p_hash_4_1
    ),
    PARTITION p_hash_5
);

INSERT INTO hash_hash VALUES(null,1,1,1);
alter table hash_hash add constraint con_hash_hash check(col_1 is not null) NOT VALID ;
INSERT INTO hash_hash VALUES(null,2,1,1); --error
INSERT INTO hash_hash VALUES(1,3,1,1); --success
alter table hash_hash VALIDATE CONSTRAINT con_hash_hash; --error
delete from hash_hash where col_1 is null;
alter table hash_hash VALIDATE CONSTRAINT con_hash_hash; --success

drop table hash_hash cascade;

create table test_range (a int, b int, c int) WITH (STORAGE_TYPE=USTORE)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT; 

insert into test_range values(1,1,1);
insert into test_range values(3001,1,1);

prepare p1 as select  * from test_range where ctid = '(0,1)' and a = $1;
explain (costs off)execute p1(1);
execute p1(1);
execute p1(3001);
drop table test_range;


set client_encoding ='UTF8';
drop table if exists test_area;
create table test_area(id int4,name text, fatherid int4, name_desc text);
insert into test_area values (1, '中国',  0,  'China');
insert into test_area values (2, '湖南省',1 , 'Hunan');
insert into test_area values (3, '广东省',1 , 'Guangdong');
insert into test_area values (4, '海南省',1 , 'Hainan');
insert into test_area values (5, '河北省',1 , 'Hebei');
insert into test_area values (6, '河南省',1 , 'Henan');
insert into test_area values (7, '山东省',1 , 'Shandong');
insert into test_area values (8, '湖北省',1 , 'Hubei');
insert into test_area values (9, '江苏省',1 , 'Jiangsu');
insert into test_area values (10,'深圳市',3 , 'Shenzhen');
insert into test_area values (11,'长沙市',2 , 'Changsha');
insert into test_area values (22,'祁北县',13, 'Qibei');
insert into test_area values (12,'南山区',10, 'Nanshan');
insert into test_area values (21,'祁西县',13, 'Qixi');
insert into test_area values (13,'衡阳市',2 , 'Hengyang');
insert into test_area values (14,'耒阳市',13, 'Leiyang');
insert into test_area values (15,'龙岗区',10, 'Longgang');
insert into test_area values (16,'福田区',10, 'Futian');
insert into test_area values (17,'宝安区',10, 'Baoan');
insert into test_area values (19,'祁东县',13, 'Qidong');
insert into test_area values (18,'常宁市',13, 'Changning');
insert into test_area values (20,'祁南县',13, 'Qinan');

EXPLAIN (COSTS OFF)
SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

SELECT *, LEVEL, connect_by_isleaf, connect_by_iscycle, connect_by_root(name_desc), sys_connect_by_path(name_desc, '@')
FROM test_area
START WITH name = '中国'
CONNECT BY PRIOR id = fatherid;

drop table test_area;


create table test_rownum_push_qual_flt(id int);

insert into test_rownum_push_qual_flt values(generate_series(1, 20));

select rownum, * from test_rownum_push_qual_flt group by id,rownum having ROWNUM < 10 and id between 10 and 20 order by 1; -- expect 0 rows

drop table test_rownum_push_qual_flt;


drop table if exists main_table_flt;
drop function if exists trigger_func_flt;
CREATE TABLE main_table_flt (a int, b int);
ALTER TABLE main_table_flt ADD PRIMARY KEY(A, B);

CREATE FUNCTION trigger_func_flt() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
	RAISE NOTICE ''trigger_func_flt(%) called: action = %, when = %, level = %'', TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;';

CREATE TRIGGER modified_a BEFORE UPDATE OF a ON main_table_flt
FOR EACH ROW WHEN (OLD.a <> NEW.a) EXECUTE PROCEDURE trigger_func_flt('modified_a');
CREATE TRIGGER modified_any BEFORE UPDATE OF a ON main_table_flt
FOR EACH ROW WHEN (OLD.* IS DISTINCT FROM NEW.*) EXECUTE PROCEDURE trigger_func_flt('modified_any');
CREATE TRIGGER insert_a AFTER INSERT ON main_table_flt
FOR EACH ROW WHEN (NEW.a = 123) EXECUTE PROCEDURE trigger_func_flt('insert_a');
CREATE TRIGGER delete_a AFTER DELETE ON main_table_flt
FOR EACH ROW WHEN (OLD.a = 123) EXECUTE PROCEDURE trigger_func_flt('delete_a');
CREATE TRIGGER insert_when BEFORE INSERT ON main_table_flt
FOR EACH STATEMENT WHEN (true) EXECUTE PROCEDURE trigger_func_flt('insert_when');
CREATE TRIGGER delete_when AFTER DELETE ON main_table_flt
FOR EACH STATEMENT WHEN (true) EXECUTE PROCEDURE trigger_func_flt('delete_when');
INSERT INTO main_table_flt (a) VALUES (123), (456);

drop table if exists main_table_flt;
drop function if exists trigger_func_flt;

CREATE TABLE interval_normal_exchange (logdate date not null) 
PARTITION BY RANGE (logdate)
INTERVAL ('1 month') 
(
	PARTITION interval_normal_exchange_p1 VALUES LESS THAN ('2020-03-01'),
	PARTITION interval_normal_exchange_p2 VALUES LESS THAN ('2020-04-01'),
	PARTITION interval_normal_exchange_p3 VALUES LESS THAN ('2020-05-01')
);

select * from interval_normal_exchange where logdate > '2020-06-01' order by logdate;
drop table interval_normal_exchange;

drop table if exists hw_partition_index_ip;
create table hw_partition_index_ip
(
	c1 int,
	c2 int,
	logdate date not null
)
partition by range (logdate)
INTERVAL ('1 month') 
(
	PARTITION hw_partition_index_ip_p0 VALUES LESS THAN ('2020-03-01'),
	PARTITION hw_partition_index_ip_p1 VALUES LESS THAN ('2020-04-01'),
	PARTITION hw_partition_index_ip_p2 VALUES LESS THAN ('2020-05-01')
);

create unique index CONCURRENTLY on hw_partition_index_ip (logdate) local
(
	partition sip1_index_local tablespace PG_DEFAULT,
	partition sip2_index_local tablespace PG_DEFAULT,
	partition sip3_index_local tablespace PG_DEFAULT
);

drop table if exists hw_partition_index_ip;



drop table if exists inventory_table_02;

create table inventory_table_02
(
    inv_date_sk               integer               not null,
    inv_item_sk               numeric               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer
)
partition by range(inv_date_sk)
(
    partition p1 values less than(10000),
    partition p2 values less than(20000),
    partition p3 values less than(30000),
    partition p4 values less than(40000),
    partition p5 values less than(50000),
    partition p6 values less than(60000),
    partition p7 values less than(maxvalue)
);

select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_date_sk') where correlation = 1;
select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_item_sk') where correlation = 1;

drop table if exists inventory_table_02;

drop table if exists t_t_mutil_t1_flt;
drop table if exists t_t_mutil_t2_flt;
create table t_t_mutil_t1_flt(col1 int,col2 int);
create table t_t_mutil_t2_flt(col1 int,col2 int);
delete from t_t_mutil_t1_flt a,t_t_mutil_t2_flt b where a.col1=b.col1;
drop table if exists t_t_mutil_t1_flt;
drop table if exists t_t_mutil_t2_flt;

CREATE TABLE CASE_TBL (
  i integer,
  f double precision
);

CREATE TABLE CASE2_TBL (
  i integer,
  j integer
);

INSERT INTO CASE_TBL VALUES (1, 10.1);
INSERT INTO CASE_TBL VALUES (2, 20.2);
INSERT INTO CASE_TBL VALUES (3, -30.3);
INSERT INTO CASE_TBL VALUES (4, NULL);

INSERT INTO CASE2_TBL VALUES (1, -1);
INSERT INTO CASE2_TBL VALUES (2, -2);
INSERT INTO CASE2_TBL VALUES (3, -3);
INSERT INTO CASE2_TBL VALUES (2, -4);
INSERT INTO CASE2_TBL VALUES (1, NULL);
INSERT INTO CASE2_TBL VALUES (NULL, -6);

SELECT '' AS Five, NULLIF(a.i,b.i) AS "NULLIF(a.i,b.i)",
  NULLIF(b.i, 4) AS "NULLIF(b.i,4)"
  FROM CASE_TBL a, CASE2_TBL b 
  ORDER BY 2, 3;

DROP TABLE CASE_TBL;
DROP TABLE CASE2_TBL;

drop table if exists t5;
CREATE TABLE t5 (
    col1 INT,
    col2 INT DEFAULT 1,
    col3 BIGSERIAL,
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 INTEGER(10, 5) DEFAULT RANDOM() + 1
) ;

TRUNCATE t5;
CREATE UNIQUE INDEX u_t5_index2 ON t5(col1, col5) WHERE col1 > 2;
INSERT INTO t5 VALUES (3), (3), (3) ON DUPLICATE KEY UPDATE col2 = col3;
drop table if exists t5;

set behavior_compat_options='aformat_null_test';
with r(a,b) as materialized
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;
reset behavior_compat_options;

CREATE TABLE gtest22c (a int, b int GENERATED ALWAYS AS (a * 2) STORED);
CREATE INDEX gtest22c_b_idx ON gtest22c (b);
CREATE INDEX gtest22c_expr_idx ON gtest22c ((b * 3));
CREATE INDEX gtest22c_pred_idx ON gtest22c (a) WHERE b > 0;
\d gtest22c

INSERT INTO gtest22c VALUES (1), (2), (3);
DROP TABLE gtest22c;

show enable_expr_fusion ;
reset enable_expr_fusion;
