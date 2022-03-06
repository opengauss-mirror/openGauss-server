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

drop table list_range_02;
CREATE TABLE IF NOT EXISTS list_range_02
(
    col_1 int ,
    col_2 int, 
	col_3 VARCHAR2 ( 30 )  ,
    col_4  int
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

INSERT INTO list_range_02 VALUES (GENERATE_SERIES(0, 19),GENERATE_SERIES(0, 1000),GENERATE_SERIES(0, 99));
 explain (costs off, verbose on) select *  from list_range_02 where col_2 >500 and col_2 <8000 order by col_1;

drop index index_01;
drop table list_range_02;

create table pjade(jid int,jn int,name varchar2) WITH (SEGMENT=ON) partition by range(jid) subpartition by range(jn)
(
  partition hrp1 values less than(16)(
    subpartition hrp1_1 values less than(16),
    subpartition hrp1_2 values less than(maxvalue)),
  partition hrp2 values less than(maxvalue)(
    subpartition hrp3_1 values less than(16),
    subpartition hrp3_3 values less than(maxvalue))
);

create table cjade(jid int,jn int,name varchar2) WITH (SEGMENT=ON);
insert into pjade values(6,8,'tom'),(8,18,'jerry'),(16,8,'jade'),(18,20,'jack');
insert into cjade values(6,8,'tom'),(8,18,'jerry'),(16,8,'jade'),(18,20,'jack');
select * from pjade subpartition(hrp1_1) union select * from cjade order by 1,2,3;
select * from pjade subpartition(hrp1_1) p union select * from cjade order by 1,2,3;
select * from pjade subpartition(hrp1_1) union select * from cjade order by 1,2,3;
select * from pjade subpartition(hrp1_1) p union select * from cjade order by 1,2,3;
drop table pjade;
drop table cjade;

DROP SCHEMA segment_subpartition_select CASCADE;
RESET CURRENT_SCHEMA;
