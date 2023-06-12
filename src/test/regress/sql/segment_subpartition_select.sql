--prepare
DROP SCHEMA segment_subpartition_select CASCADE;
CREATE SCHEMA segment_subpartition_select;
SET CURRENT_SCHEMA TO segment_subpartition_select;

--select
CREATE TABLE t1
(
	c1 int,
	c2 int
) WITH (SEGMENT=ON);
insert into t1 values(generate_series(201901,201910), generate_series(1,10));

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
insert into range_list values('201902', '3', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '3', '1', 1);

select * from range_list order by 1, 2, 3, 4;

select * from range_list where user_no is not null order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code = user_no order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code in ('2') order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code  <> '2' order by 1, 2, 3, 4;
select * from range_list partition (p_201901) order by 1, 2, 3, 4;
select * from range_list partition (p_201902) order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code  <> '2' UNION ALL select * from range_list partition (p_201902) order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code  <> '2' UNION ALL select * from range_list partition (p_201902) where dept_code in ('2') order by 1, 2, 3, 4;



CREATE TABLE range_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);

insert into range_hash values('201902', '1', '1', 1);
insert into range_hash values('201902', '2', '1', 1);
insert into range_hash values('201902', '1', '1', 1);
insert into range_hash values('201903', '2', '1', 1);
insert into range_hash values('201903', '1', '1', 1);
insert into range_hash values('201903', '2', '1', 1);

select * from range_hash order by 1, 2, 3, 4;

select * from range_hash where user_no is not null order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code = user_no order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code in ('2') order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code  <> '2' order by 1, 2, 3, 4;
select * from range_hash partition (p_201901) order by 1, 2, 3, 4;
select * from range_hash partition (p_201902) order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code  <> '2' UNION ALL select * from range_hash partition (p_201902) order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code  <> '2' UNION ALL select * from range_hash partition (p_201902) where dept_code in ('2') order by 1, 2, 3, 4;


CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('201902', '2', '1', 1);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('201903', '2', '1', 1);
insert into range_range values('201903', '1', '1', 1);
insert into range_range values('201903', '2', '1', 1);

select * from range_range order by 1, 2, 3, 4;

select * from range_range where user_no is not null order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code = user_no order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code in ('2') order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code  <> '2' order by 1, 2, 3, 4;
select * from range_range partition (p_201901) order by 1, 2, 3, 4;
select * from range_range partition (p_201902) order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code  <> '2' UNION ALL select * from range_range partition (p_201902) order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code  <> '2' UNION ALL select * from range_range partition (p_201902) where dept_code in ('2') order by 1, 2, 3, 4;

--view
create view view_temp  as select * from range_list;
select * from view_temp;
--error
select * from view_temp partition (p_201901);
select * from view_temp partition (p_201902);
drop view view_temp;

with tmp1 as (select * from range_list ) select * from tmp1 order by 1, 2, 3, 4;
with tmp1 as (select * from range_list partition (p_201901)) select * from tmp1 order by 1, 2, 3, 4;

--join normal table 
select * from range_list left join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list left join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_list right join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list right join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_list full join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list full join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_list inner join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list inner join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;


select * from range_hash left join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash left join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_hash right join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash right join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_hash full join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash full join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_hash inner join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash inner join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;


select * from range_range left join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range left join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_range right join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range right join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_range full join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range full join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_range inner join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range inner join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

--join range_list and range_hash

select * from range_list left join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list left join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_list right join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list right join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_list full join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list full join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_list inner join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list inner join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

--join range_hash and range_range

select * from range_hash left join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash left join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash right join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash right join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash full join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash full join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

--join range_hash and range_range

select * from range_hash left join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash left join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash right join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash right join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash full join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash full join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

DROP SCHEMA segment_subpartition_select CASCADE;
RESET CURRENT_SCHEMA;
