DROP SCHEMA partition_minmax CASCADE;
CREATE SCHEMA partition_minmax;
SET CURRENT_SCHEMA TO partition_minmax;
--range单列分区单列索引
drop table test_range_pt;
create table test_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;
 
create index idx_range_a on test_range_pt(a) local;
create index idx_range_b on test_range_pt(b) local;
insert into test_range_pt values(1, 1 ,1);
insert into test_range_pt values(2001, 3 ,1);
insert into test_range_pt values(3001, 2 ,1);
insert into test_range_pt values(4001, 4 ,1);

explain(costs off, verbose on) select min(a) from test_range_pt;
select min(a) from test_range_pt;
explain(costs off, verbose on) select max(a) from test_range_pt;
select max(a) from test_range_pt;

explain(costs off, verbose on) select min(b) from test_range_pt;
select min(b) from test_range_pt;
explain(costs off, verbose on) select max(b) from test_range_pt;
select max(b) from test_range_pt;

--range单列分区多列索引
drop index idx_range_a;
drop index idx_range_b;

create index idx_range_ab on test_range_pt(a,b) local;

explain(costs off, verbose on) select min(a) from test_range_pt;
select min(a) from test_range_pt;
explain(costs off, verbose on) select max(a) from test_range_pt;
select max(a) from test_range_pt;
--多列索引非第一列不优化
explain(costs off, verbose on) select min(b) from test_range_pt;
select min(b) from test_range_pt;
explain(costs off, verbose on) select max(b) from test_range_pt;
select max(b) from test_range_pt;

--range多列分区
drop table test_range_pt1;
create table test_range_pt1 (a int, b int, c int)
partition by range(a,b)
(
	partition p1 values less than (2000,10),
	partition p2 values less than (3000,20),
	partition p3 values less than (4000,30),
	partition p4 values less than (5000,40)
)ENABLE ROW MOVEMENT;
 
create index idx_range1_a on test_range_pt1(a) local;
create index idx_range1_b on test_range_pt1(b) local;

insert into test_range_pt1 values(1, 1 ,1);
insert into test_range_pt1 values(2001, 3 ,1);
insert into test_range_pt1 values(3001, 2 ,1);
insert into test_range_pt1 values(2001, 30 ,1);
insert into test_range_pt1 values(3001, 20 ,1);

explain(costs off, verbose on) select min(a) from test_range_pt1;
select min(a) from test_range_pt1;
explain(costs off, verbose on) select max(a) from test_range_pt1;
select max(a) from test_range_pt1;

explain(costs off, verbose on) select min(b) from test_range_pt1;
select min(b) from test_range_pt1;
explain(costs off, verbose on) select max(b) from test_range_pt1;
select max(b) from test_range_pt1;

--range多列分区多列索引
drop index idx_range1_a;
drop index idx_range1_b;

create index idx_range1_ab on test_range_pt1(a,b) local;

explain(costs off, verbose on) select min(a) from test_range_pt1;
select min(a) from test_range_pt1;
explain(costs off, verbose on) select max(a) from test_range_pt1;
select max(a) from test_range_pt1;
--多列索引非第一列不优化
explain(costs off, verbose on) select min(b) from test_range_pt1;
select min(b) from test_range_pt1;
explain(costs off, verbose on) select max(b) from test_range_pt1;
select max(b) from test_range_pt1;

--list分区单列索引
drop table test_list_lt;
create table test_list_lt (a int, b int )
partition by list(a)
(
	partition p1 values (2000),
	partition p2 values (3000),
	partition p3 values (4000)
) ;
create index idx_list_a on test_list_lt(a) local;
create index idx_list_b on test_list_lt(b) local;

insert into test_list_lt values(3000, 2);
insert into test_list_lt values(2000, 30);
insert into test_list_lt values(4000, 20);

explain(costs off, verbose on) select min(a) from test_list_lt;
select min(a) from test_list_lt;
explain(costs off, verbose on) select max(a) from test_list_lt;
select max(a) from test_list_lt;

explain(costs off, verbose on) select min(b) from test_list_lt;
select min(b) from test_list_lt;
explain(costs off, verbose on) select max(b) from test_list_lt;
select max(b) from test_list_lt;

--list分区多列索引
drop index idx_list_a;
drop index idx_list_b;

create index idx_list_ab on test_list_lt(a,b) local;

explain(costs off, verbose on) select min(a) from test_list_lt;
select min(a) from test_list_lt;
explain(costs off, verbose on) select max(a) from test_list_lt;
select max(a) from test_list_lt;
--多列索引非第一列不优化
explain(costs off, verbose on) select min(b) from test_list_lt;
select min(b) from test_list_lt;
explain(costs off, verbose on) select max(b) from test_list_lt;
select max(b) from test_list_lt;

--hash分区单列索引
drop table test_hash_ht;
create table test_hash_ht (a int, b int)
partition by hash(a)
(
	partition p1, 
	partition p2,  
	partition p3,
	partition p4,
	partition p5,
	partition p6,
	partition p7,
	partition p8
);
create index idx_hash_a on test_hash_ht(a) local;
create index idx_hash_b on test_hash_ht(b) local;

insert into test_hash_ht values(generate_series(1,100),generate_series(1,100));

explain(costs off, verbose on) select min(a) from test_hash_ht;
select min(a) from test_hash_ht;
explain(costs off, verbose on) select max(a) from test_hash_ht;
select max(a) from test_hash_ht;

explain(costs off, verbose on) select min(b) from test_hash_ht;
select min(b) from test_hash_ht;
explain(costs off, verbose on) select max(b) from test_hash_ht;
select max(b) from test_hash_ht;

--hash分区多列索引
drop index idx_hash_a;
drop index idx_hash_b;

create index idx_hash_ab on test_hash_ht(a,b) local;

explain(costs off, verbose on) select min(a) from test_hash_ht;
select min(a) from test_hash_ht;
explain(costs off, verbose on) select max(a) from test_hash_ht;
select max(a) from test_hash_ht;
--多列索引非第一列不优化
explain(costs off, verbose on) select min(b) from test_hash_ht;
select min(b) from test_hash_ht;
explain(costs off, verbose on) select max(b) from test_hash_ht;
select max(b) from test_hash_ht;

--二级分区单列索引
drop table range_list;
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2'),
	SUBPARTITION p_201901_c values ('3')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2'),
	SUBPARTITION p_201902_c values ('3')
  )
);
create index idx_month_code on range_list(month_code) local;
create index idx_dept_code on range_list(dept_code) local;
create index idx_sales_amt on range_list(sales_amt) local;
insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 5);
insert into range_list values('201902', '3', '1', 3);
insert into range_list values('201903', '1', '1', 2);
insert into range_list values('201904', '2', '1', 6);
insert into range_list values('201905', '3', '1', 4);

explain(costs off, verbose on) select min(month_code) from range_list;
select min(month_code) from range_list;
explain(costs off, verbose on) select max(month_code) from range_list;
select max(month_code) from range_list;

explain(costs off, verbose on) select min(dept_code) from range_list;
select min(dept_code) from range_list;
explain(costs off, verbose on) select max(dept_code) from range_list;
select max(dept_code) from range_list;

explain(costs off, verbose on) select min(sales_amt) from range_list;
select min(sales_amt) from range_list;
explain(costs off, verbose on) select max(sales_amt) from range_list;
select max(sales_amt) from range_list;

--二级分区多列索引
drop index idx_month_code;
drop index idx_dept_code;
drop index idx_sales_amt;

create index idx_1_2 on range_list(month_code, dept_code) local;

explain(costs off, verbose on) select min(month_code) from range_list;
select min(month_code) from range_list;
explain(costs off, verbose on) select max(month_code) from range_list;
select max(month_code) from range_list;
--多列索引非第一列不优化
explain(costs off, verbose on) select min(dept_code) from range_list;
select min(dept_code) from range_list;
explain(costs off, verbose on) select max(dept_code) from range_list;
select max(dept_code) from range_list;

drop index idx_1_2;

create index idx_4_2 on range_list(sales_amt, dept_code) local;

explain(costs off, verbose on) select min(sales_amt) from range_list;
select min(sales_amt) from range_list;
explain(costs off, verbose on) select max(sales_amt) from range_list;
select max(sales_amt) from range_list;
--多列索引非第一列不优化
explain(costs off, verbose on) select min(dept_code) from range_list;
select min(dept_code) from range_list;
explain(costs off, verbose on) select max(dept_code) from range_list;
select max(dept_code) from range_list;


--其它场景
drop table test_list_lt;
create table test_list_lt (a int, b int )
partition by list(a)
(
	partition p1 values (2000),
	partition p2 values (3000),
	partition p3 values (4000)
) ;
create index idx_list_a on test_list_lt(a) local;
create index idx_list_b on test_list_lt(b) local;

insert into test_list_lt values(3000, 2);
insert into test_list_lt values(2000, 30);
insert into test_list_lt values(4000, 20);

explain(costs off, verbose on) select min(t1.a) from test_list_lt t1 join test_list_lt t2 on t1.a = t2.a;
explain(costs off, verbose on) select min(t1.a) from test_list_lt t1 join test_list_lt t2 on t1.b = t2.b;

explain(costs off, verbose on) select min(t1.a) from test_list_lt t1 union all select min(t2.a) from test_list_lt t2;
select min(t1.a) from test_list_lt t1 union all select min(t2.a) from test_list_lt t2;
explain(costs off, verbose on) select min(t1.a) from test_list_lt t1 union all select min(t2.b) from test_list_lt t2;
select min(t1.a) from test_list_lt t1 union all select min(t2.b) from test_list_lt t2;

explain(costs off, verbose on) select * from test_list_lt t1 where a = (select min(t2.a) from test_list_lt t2);
select * from test_list_lt t1 where a = (select min(t2.a) from test_list_lt t2);

explain(costs off, verbose on) select * from test_list_lt s1 where (select min(s2.a) from test_list_lt s2) > 1 order by 1;
select * from test_list_lt s1 where (select min(s2.a) from test_list_lt s2) > 1 order by 1;

explain(costs off, verbose on) select * from test_list_lt s1, (select min(s2.a) from test_list_lt s2) tmp(a) where tmp.a = s1.a;
select * from test_list_lt s1, (select min(s2.a) from test_list_lt s2) tmp(a) where tmp.a = s1.a;

set enable_seqscan = off;
set enable_bitmapscan = off;

explain(costs off, verbose on) select min(a) from test_list_lt where a = 2000;
select min(a) from test_list_lt where a = 2000;

explain(costs off, verbose on) select min(b) from test_list_lt where b = 20;
select min(b) from test_list_lt where b = 20;
reset enable_seqscan;
reset enable_bitmapscan;

drop table  hash_hash_jade cascade;
create table  hash_hash_jade(hjid int,rjid int,jname varchar2)partition by hash (hjid) subpartition by hash(rjid)
(
  partition hhp1(
    subpartition hhp1_1,
    subpartition hhp1_2),
  partition hhp2(
    subpartition hhp2_1,
    subpartition hhp2_2),
  partition hhp3(
    subpartition hhp3_1,
    subpartition hhp3_2),
  partition hhp4(
    subpartition hhp4_1,
    subpartition hhp4_2)
);

insert into  hash_hash_jade(hjid,rjid,jname)values(generate_series(1,100),generate_series(200,300),'jade');
insert into  hash_hash_jade(hjid,rjid,jname)values(generate_series(400,500),generate_series(700,800),'twojade');
insert into hash_hash_jade values(1,1,null);

drop table  hash_hash_jade2 cascade;
create table  hash_hash_jade2(hjid int,rjid int,jname varchar2)partition by hash (hjid) subpartition by hash(rjid)
(
  partition hhp1(
    subpartition hhp1_1,
    subpartition hhp1_2),
  partition hhp2(
    subpartition hhp2_1,
    subpartition hhp2_2),
  partition hhp3(
    subpartition hhp3_1,
    subpartition hhp3_2),
  partition hhp4(
    subpartition hhp4_1,
    subpartition hhp4_2)
);

insert into  hash_hash_jade2(hjid,rjid,jname)values(generate_series(50,400),generate_series(266,700),'jade');
insert into  hash_hash_jade2(hjid,rjid,jname)values(generate_series(351,852),generate_series(700,800),'twojade');
insert into hash_hash_jade2 values(1,1,null);

drop index hh1;
drop index hh2;
drop index hh3;
drop index hh4;
create index hh1 on hash_hash_jade(hjid) local;
create index hh2 on hash_hash_jade(rjid) local;
create index hh3 on hash_hash_jade2(hjid) local;
create index hh4 on hash_hash_jade2(rjid) local;

explain(costs off, verbose on)  select max(hjid),min(rjid) from hash_hash_jade where exists(select min(hjid) from hash_hash_jade2 group by rjid);
select max(hjid),min(rjid) from hash_hash_jade where exists(select min(hjid) from hash_hash_jade2 group by rjid);

drop table hash_hash_jade cascade;
drop table  hash_hash_jade2 cascade;

drop table test_list_lt;
create table test_list_lt (a int, b int )
partition by list(a)
(
	partition p1 values (2000),
	partition p2 values (3000),
	partition p3 values (4000)
) ;
create index idx_list_a on test_list_lt(a) local;
create index idx_list_b on test_list_lt(b) local;

insert into test_list_lt values(3000,300),(2000,2000),(4000,4000);
alter index idx_list_b  MODIFY PARTITION p3_b_idx UNUSABLE;
explain(costs off, verbose on)   select max(b) from test_list_lt;
select max(b) from test_list_lt;

drop table test_range_pt;
drop table test_range_pt1;
drop table test_list_lt;
drop table test_hash_ht;
drop table range_list;

drop table if exists partition_range_pbe_tbl_001;
create table partition_range_pbe_tbl_001(
c_id int,c_d_id int,c_w_id int,c_first varchar2(20),c_middle char(2),c_last varchar2(30),
c_date date,c_timestamp timestamp,c_clob clob,c_blob blob,c_text text)
partition by range(c_id) subpartition by list (c_d_id)(
partition p1 values less than(10)
(
    SUBPARTITION p1_a values (1,2,3,4),
    SUBPARTITION p1_b values (5,6),
    SUBPARTITION p1_c values (7),
    SUBPARTITION p1_d values (8,9),
    SUBPARTITION p1_e values (10)
),
partition p2 values less than(20)
(
    SUBPARTITION p2_a values (1,2,3,4),
    SUBPARTITION p2_b values (5,6),
    SUBPARTITION p2_c values (7),
    SUBPARTITION p2_d values (8,9),
    SUBPARTITION p2_e values (10)
),
partition p3 values less than(30)
(
    SUBPARTITION p3_a values (1,2,3,4),
    SUBPARTITION p3_b values (5,6),
    SUBPARTITION p3_c values (7),
    SUBPARTITION p3_d values (8,9),
    SUBPARTITION p3_e values (10)
),
partition p4 values less than(40)

(
    SUBPARTITION p4_a values (1,2,3,4),
    SUBPARTITION p4_b values (5,6),
    SUBPARTITION p4_c values (7),
    SUBPARTITION p4_d values (8,9),
    SUBPARTITION p4_e values (10)
),
partition p5 values less than(maxvalue)
(
    SUBPARTITION p5_a values (1,2,3,4),
    SUBPARTITION p5_b values (5,6),
    SUBPARTITION p5_c values (7),
    SUBPARTITION p5_d values (8,9),
    SUBPARTITION p5_e values (10)
));
insert into partition_range_pbe_tbl_001 values(generate_series(1,100),generate_series(1,10));
create index partition_range_pbe_idx_001 on partition_range_pbe_tbl_001(c_id) local;
set enable_seqscan = off;
explain(costs off, verbose on) select max(c_id) from partition_range_pbe_tbl_001 where c_d_id = 1;
select max(c_id) from partition_range_pbe_tbl_001 where c_d_id = 1;
create index partition_range_pbe_idx_002 on partition_range_pbe_tbl_001(c_d_id) local;
explain(costs off, verbose on) select max(c_id) from partition_range_pbe_tbl_001 where c_d_id = 1;
select max(c_id) from partition_range_pbe_tbl_001 where c_d_id = 1;
reset enable_seqscan;

drop table partition_range_pbe_tbl_001;

drop table test_range_pt;
create table test_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (20),
	partition p2 values less than (30),
	partition p3 values less than (40),
	partition p4 values less than (50),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;
insert into test_range_pt values(generate_series(1,100),generate_series(1,100),generate_series(1,10));
create index idx_range_a on test_range_pt(a) local;
create index idx_range_b on test_range_pt(b) local;
explain(costs off, verbose on) select max(b) from test_range_pt where c = 1;
select max(b) from test_range_pt where c = 1;
create index idx_range_c on test_range_pt(c) local;
explain(costs off, verbose on) select max(b) from test_range_pt where c = 1;
select max(b) from test_range_pt where c = 1;

drop table test_range_pt;

DROP SCHEMA partition_minmax CASCADE;
