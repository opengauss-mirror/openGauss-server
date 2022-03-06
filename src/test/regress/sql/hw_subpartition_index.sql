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

reset current_schema;
DROP SCHEMA subpartition_index CASCADE;
