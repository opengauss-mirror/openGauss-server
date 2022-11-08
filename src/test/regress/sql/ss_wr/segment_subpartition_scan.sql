--prepare
DROP SCHEMA segment_subpartition_scan CASCADE;
CREATE SCHEMA segment_subpartition_scan;
SET CURRENT_SCHEMA TO segment_subpartition_scan;

--scan
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
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

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);

explain(costs off, verbose on) select * from range_list order by 1, 2, 3, 4;
select * from range_list order by 1, 2, 3, 4;

create index idx_month_code on range_list(month_code) local;
create index idx_dept_code on range_list(dept_code) local;
create index idx_user_no on range_list(user_no) local;

set enable_seqscan = off;
explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

set enable_bitmapscan = off;
explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

reset enable_seqscan;
reset enable_bitmapscan;


create table range_range_jade(jid int,jn int,name varchar2) WITH (SEGMENT=ON) partition by range (jid) subpartition by range(jn)
(
  partition hrp1 values less than(16)(
    subpartition hrp1_1 values less than(16),
subpartition hrp1_2 values less than(26),
subpartition hrp1_3 values less than(36),
    subpartition hrp1_4 values less than(maxvalue)),
  partition hrp2 values less than(26)(
    subpartition hrp2_1 values less than(maxvalue)),
  partition hrp3 values less than(36)(
    subpartition hrp3_1 values less than(16),
subpartition hrp3_2 values less than(26),
    subpartition hrp3_3 values less than(maxvalue)),
  partition hrp4 values less than(maxvalue)(
    subpartition hrp4_1 values less than(16),
    subpartition hrp4_2 values less than(maxvalue))
)ENABLE ROW MOVEMENT;
-- no errors
set enable_partition_opfusion = on;
insert into range_range_jade values(1,2,'jade');
reset enable_partition_opfusion;

CREATE TABLE IF NOT EXISTS list_range_02
(
 col_1 int ,
 col_2 int,
col_3 VARCHAR2 ( 30 ) ,
 col_4 int
) WITH (SEGMENT=ON)
PARTITION BY list (col_1) SUBPARTITION BY range (col_2)
(
 PARTITION p_list_1 VALUES(-1,-2,-3,-4,-5,-6,-7,-8,-9,-10 )
 (
 SUBPARTITION p_range_1_1 VALUES LESS THAN( -10 ),
 SUBPARTITION p_range_1_2 VALUES LESS THAN( 0 ),
 SUBPARTITION p_range_1_3 VALUES LESS THAN( 10 ),
 SUBPARTITION p_range_1_4 VALUES LESS THAN( 20 ),
 SUBPARTITION p_range_1_5 VALUES LESS THAN( 50 )
 ),
 PARTITION p_list_2 VALUES(1,2,3,4,5,6,7,8,9,10 ),
 PARTITION p_list_3 VALUES(11,12,13,14,15,16,17,18,19,20)
 (
 SUBPARTITION p_range_3_1 VALUES LESS THAN( 15 ),
 SUBPARTITION p_range_3_2 VALUES LESS THAN( MAXVALUE )
 ),
 PARTITION p_list_4 VALUES(21,22,23,24,25,26,27,28,29,30)
 (
 SUBPARTITION p_range_4_1 VALUES LESS THAN( -10 ),
 SUBPARTITION p_range_4_2 VALUES LESS THAN( 0 ),
 SUBPARTITION p_range_4_3 VALUES LESS THAN( 10 ),
 SUBPARTITION p_range_4_4 VALUES LESS THAN( 20 ),
 SUBPARTITION p_range_4_5 VALUES LESS THAN( 50 )
 ),
 PARTITION p_list_5 VALUES(31,32,33,34,35,36,37,38,39,40)
 (
 SUBPARTITION p_range_5_1 VALUES LESS THAN( MAXVALUE )
 ),
 PARTITION p_list_6 VALUES(41,42,43,44,45,46,47,48,49,50)
 (
 SUBPARTITION p_range_6_1 VALUES LESS THAN( -10 ),
 SUBPARTITION p_range_6_2 VALUES LESS THAN( 0 ),
 SUBPARTITION p_range_6_3 VALUES LESS THAN( 10 ),
 SUBPARTITION p_range_6_4 VALUES LESS THAN( 20 ),
 SUBPARTITION p_range_6_5 VALUES LESS THAN( 50 )
 ),
 PARTITION p_list_7 VALUES(default)
) ENABLE ROW MOVEMENT;

create index index_01 on list_range_02(col_2) local ;

explain (costs off)  select * from list_range_02 where col_2 in
          (select col_1 from list_range_02 subpartition(p_list_2_subpartdefault1)
           where col_1 >10 and col_1 <100) and col_1 +col_2 =50 and col_2 in  (100,200,300 );

