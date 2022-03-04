-- prepare
DROP SCHEMA segment_subpartition_analyze_vacuum CASCADE;
CREATE SCHEMA segment_subpartition_analyze_vacuum;
SET CURRENT_SCHEMA TO segment_subpartition_analyze_vacuum;

-- base function

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

create index idx_month_code_local on range_list(month_code) local;
create index idx_dept_code_global on range_list(dept_code) global;
create index idx_user_no_global on range_list(user_no) global;

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '2', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select * from range_list order by 1, 2, 3, 4;
delete from range_list where month_code = '201902';
select * from range_list order by 1, 2, 3, 4;
analyze range_list;
analyze range_list partition (p_201901);
vacuum range_list;
vacuum range_list partition (p_201901);

drop table range_list;

-- clean
DROP SCHEMA segment_subpartition_analyze_vacuum CASCADE;
RESET CURRENT_SCHEMA;
