DROP SCHEMA subpartition_index CASCADE;
CREATE SCHEMA subpartition_index;
SET CURRENT_SCHEMA TO subpartition_index;

CREATE TABLE source
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
);

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
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

insert into source values('201902', '1', '1', 1);
insert into source values('201902', '2', '1', 1);
insert into source values('201902', '1', '1', 1);
insert into source values('201903', '2', '1', 1);
insert into source values('201903', '1', '1', 1);
insert into source values('201903', '2', '1', 1);

insert into range_list select * from source;

CREATE INDEX range_list_idx ON range_list(month_code) LOCAL
(
 PARTITION p_201901_idx
 (
  SUBPARTITION p_201901_a_idx,
  SUBPARTITION p_201901_b_idx
 ),
 PARTITION p_201902_idx
 (
  SUBPARTITION p_201902_a_idx,
  SUBPARTITION p_201902_b_idx
 )
);

-- test subpartition index scan
explain (costs off) select * from range_list where month_code = '201902';
select * from range_list where month_code = '201902' order by 1,2,3,4;
explain (costs off) select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201902';
select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201902' order by 1,2,3,4;

-- test index unusable and rebuild
ALTER INDEX range_list_idx MODIFY PARTITION p_201901_a_idx UNUSABLE;
select indisusable from pg_partition where relname = 'p_201901_a_idx';
REINDEX INDEX range_list_idx PARTITION p_201901_a_idx;
select indisusable from pg_partition where relname = 'p_201901_a_idx';

truncate table range_list;
ALTER INDEX range_list_idx MODIFY PARTITION p_201901_a_idx UNUSABLE;
ALTER INDEX range_list_idx MODIFY PARTITION p_201901_b_idx UNUSABLE;
ALTER INDEX range_list_idx MODIFY PARTITION p_201902_a_idx UNUSABLE;
ALTER INDEX range_list_idx MODIFY PARTITION p_201902_b_idx UNUSABLE;
insert into range_list select * from source;

explain (costs off) select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201902';
explain (costs off) select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201903';

REINDEX INDEX range_list_idx;

explain (costs off) select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201902';
explain (costs off) select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201903';
select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201902' order by 1,2,3,4;
select /*+ indexscan(range_list range_list_idx)*/* from range_list where month_code = '201903' order by 1,2,3,4;

-- wrong case
CREATE INDEX range_list_idxx ON range_list(month_code) LOCAL
(
 PARTITION p_201902_idx
 (
  SUBPARTITION p_201901_a_idx,
  SUBPARTITION p_201901_b_idx,
  SUBPARTITION p_201902_a_idx,
  SUBPARTITION p_201902_b_idx
 )
);

CREATE INDEX range_list_idxx ON range_list(month_code) LOCAL
(
 PARTITION p_201901_idx
 (
  SUBPARTITION p_201901_a_idx,
  SUBPARTITION p_201901_b_idx
 ),
 PARTITION p_201902_idx
 (
  SUBPARTITION p_201902_a_idx,
  SUBPARTITION p_201902_b_idx
 ),
 PARTITION p_201903_idx
 (
  SUBPARTITION p_201902_a_idx
 )
);

CREATE INDEX range_list_idxx ON range_list(month_code) LOCAL
(
 PARTITION p_201901_idx
 (
  SUBPARTITION p_201901_a_idx
 ),
 PARTITION p_201902_idx
 (
  SUBPARTITION p_201902_a_idx,
  SUBPARTITION p_201902_b_idx
 )
);

CREATE INDEX range_list_idxx ON range_list(month_code) LOCAL
(
 PARTITION p_201901_idx
 (
  SUBPARTITION p_201901_a_idx
 ),
 PARTITION p_201902_idx
 (
  SUBPARTITION p_201901_b_idx,
  SUBPARTITION p_201902_a_idx,
  SUBPARTITION p_201902_b_idx
 )
);

drop table source;
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

prepare p1 as select * from partition_range_pbe_tbl_001 where c_id=$1;
explain(costs off, verbose on)  execute p1(50);
execute p1(50);
alter index partition_range_pbe_idx_001 MODIFY PARTITION  p5_b_c_id_idx unusable; 
explain(costs off, verbose on)  execute p1(50);
execute p1(50);
deallocate p1;

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
insert into test_range_pt values(generate_series(1,100),generate_series(1,100));
create index idx_range_a on test_range_pt(a) local;

prepare p1 as select * from test_range_pt where a = $1;
explain(costs off, verbose on)  execute p1(25);
execute p1(25);
alter index idx_range_a MODIFY PARTITION  p2_a_idx unusable; 
explain(costs off, verbose on)  execute p1(25);
execute p1(25);
deallocate p1;

drop table test_range_pt;

drop table test_range_pt;
create table test_range_pt (a int, b int, c int)
partition by range(a, b)
(
partition p1 values less than (100,100),
partition p2 values less than (200,200),
partition p3 values less than (300,300),
partition p4 values less than (maxvalue,maxvalue)
)ENABLE ROW MOVEMENT;

--部分索引失效
create index idx_range_a on test_range_pt(a) local;
alter index idx_range_a modify partition p3_a_idx unusable; --部分失效

--插入数据
insert into test_range_pt values(199, 100 ,1); --P2
insert into test_range_pt values(199, 200 ,1); --P2
insert into test_range_pt values(200, 300 ,1);
insert into test_range_pt values(200, 400 ,1);

insert into test_range_pt values(generate_series(1,400), random()*1000 ,1);

set enable_seqscan=off;
set enable_pbe_optimization = on;
prepare p1 as select max(a) from test_range_pt where a>$1;
execute p1(10);
explain execute p1(10);
deallocate p1;
drop table test_range_pt;

reset current_schema;
DROP SCHEMA subpartition_index CASCADE;
