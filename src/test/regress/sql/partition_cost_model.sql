DROP SCHEMA partition_cost_model CASCADE;
CREATE SCHEMA partition_cost_model;
SET CURRENT_SCHEMA TO partition_cost_model;
set partition_page_estimation = off;

create table test_range_pt (a int, b int, c int)
partition by range(a)
(
        partition p1 values less than (2000),
        partition p2 values less than (3000),
        partition p3 values less than (4000),
        partition p4 values less than (5000),
        partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;
insert into test_range_pt values(generate_series(1,10000), generate_series(1,10000), generate_series(1,10000));
insert into test_range_pt select * from test_range_pt;
insert into test_range_pt select * from test_range_pt;
insert into test_range_pt select * from test_range_pt;
insert into test_range_pt select * from test_range_pt;
insert into test_range_pt select * from test_range_pt;

create index  idx_local on test_range_pt(a) local;
analyze test_range_pt;
explain (costs off) select *from test_range_pt where a = 1;
explain (costs off) select /*+ set(partition_page_estimation on) */*from test_range_pt where a = 1;
set partition_page_estimation = on;
explain (costs off) select *from test_range_pt where a = 1;
drop table test_range_pt;

drop table test_hash_ht;
create table test_hash_ht ( a int,b int, c int, d int) with (storage_type=ustore)
partition by hash(a)
(
	partition p1, 
	partition p2, 
	partition p3,
	partition p4,
	partition p5,
	partition p6,
	partition p7,
	partition p8,
	partition p9,
	partition p10,
	partition p11,
	partition p12,
	partition p13,
	partition p14,
	partition p15,
	partition p16,
	partition p17,
	partition p18,
	partition p19,
	partition p20
);

create index idx_hash_global on test_hash_ht(a,b) global;

insert into test_hash_ht values(generate_series(0,49), generate_series(1,10000));
insert into test_hash_ht values(50, generate_series(1,1000));
analyze test_hash_ht;

set enable_bitmapscan = off;
set partition_page_estimation = off;

explain  (costs off, verbose on) select a,b,c from test_hash_ht where a = 50;

explain (costs off, verbose on) select /*+indexscan(test_hash_ht idx_hash_global)*/ a,b,c from test_hash_ht where a = 50;
explain (costs off, verbose on) select /*+indexscan(test_hash_ht idx_hash_global) set(partition_page_estimation on)*/ a,b,c from test_hash_ht where a = 50;

set partition_page_estimation = on;

explain  (costs off, verbose on)  select a,b,c from test_hash_ht where a = 50;

explain  (costs off, verbose on) select /*+indexscan(test_hash_ht idx_hash_global)*/ a,b,c from test_hash_ht where a = 50;

drop table test_hash_ht;

reset enable_bitmapscan;

drop table partition_reindex_table3;
create table partition_reindex_table3
(
	c1 int,
	c2 int,
	C3 date not null
)
partition by range (C3)
INTERVAL ('1 month') 
(
	PARTITION partition_reindex_table3_p0 VALUES LESS THAN ('2020-02-01'),
	PARTITION partition_reindex_table3_p1 VALUES LESS THAN ('2020-05-01'),
	PARTITION partition_reindex_table3_p2 VALUES LESS THAN ('2020-06-01')
)
enable row movement;
create index partition_reindex_table3_ind3 on partition_reindex_table3(c3) local;
set partition_page_estimation = off;
insert into partition_reindex_table3 values (generate_series(1,10), generate_series(1,10), generate_series(TO_DATE('2020-01-01', 'YYYY-MM-DD'),TO_DATE('2020-07-01', 'YYYY-MM-DD'),'1, day'));
analyze partition_reindex_table3;
explain (costs off) select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
explain (costs off) select /*+ set(partition_page_estimation on)*/* from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
set partition_page_estimation = on;
explain (costs off) select * from partition_reindex_table3 where c3 = TO_DATE('2020-04-21', 'YYYY-MM-DD') and c2 = 8;
drop table partition_reindex_table3;

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
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
CREATE TABLE source
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
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
set partition_page_estimation = off;
explain (costs off) select * from range_list where month_code = '201902';
explain (costs off) select /*+ set(partition_page_estimation on)*/* from range_list where month_code = '201902';
set partition_page_estimation = on;
explain (costs off) select * from range_list where month_code = '201902';
drop table range_list;
DROP SCHEMA partition_cost_model CASCADE;
