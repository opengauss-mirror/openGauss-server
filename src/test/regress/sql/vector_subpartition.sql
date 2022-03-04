DROP SCHEMA IF exists vector_subpartition;
CREATE SCHEMA vector_subpartition;
set current_schema=vector_subpartition;

set try_vector_engine_strategy=force;

CREATE TABLE IF NOT EXISTS range_range_02
(col_1 int, col_2 int, col_3 int , col_4 int)
PARTITION BY RANGE (col_1) SUBPARTITION BY RANGE (col_2)
(
  PARTITION p_range_1 VALUES LESS THAN( 10 )
  (
    SUBPARTITION p_range_1_1 VALUES LESS THAN( 5 ),
    SUBPARTITION p_range_1_2 VALUES LESS THAN( MAXVALUE )
  ),
  PARTITION p_range_2 VALUES LESS THAN( 20 )
  (
    SUBPARTITION p_range_2_1 VALUES LESS THAN( 5 ),
    SUBPARTITION p_range_2_2 VALUES LESS THAN( 10 ),
    SUBPARTITION p_range_2_3 VALUES LESS THAN( MAXVALUE )
  ),
  PARTITION p_range_3 VALUES LESS THAN( MAXVALUE )
) ENABLE ROW MOVEMENT;

INSERT INTO range_range_02 VALUES (GENERATE_SERIES(-190, 1900),GENERATE_SERIES(-290, 1800),GENERATE_SERIES(-90, 2000));

create unique index on range_range_02 (col_1,col_2 nulls first) where col_2 < 4;
create unique index on range_range_02 (col_1,col_2,col_3 nulls first) where col_2 < 4;

explain (costs off) select /*+ indexscan(range_range_02
 range_range_02_col_1_col_2_idx)*/ * from range_range_02 where col_2 in (select
 col_1 from range_range_02 aa where col_1 >10 and col_1 <100) and col_1 +col_2 =50
 and col_2 < 4;

CREATE TABLE list_list
(
  col_1 int primary key,
  col_2 int NOT NULL ,
  col_3 VARCHAR2 ( 30 ) NOT NULL ,
  col_4 int generated always as(2*col_2) stored ,
  check (col_4 >= col_2)
) with(FILLFACTOR=80)
PARTITION BY list (col_1) SUBPARTITION BY list (col_2)
(
  PARTITION p_list_1 VALUES(-1,-2,-3,-4,-5,-6,-7,-8,-9,-10 )
  (
    SUBPARTITION p_list_1_1 VALUES ( 0,-1,-2,-3,-4,-5,-6,-7,-8,-9 ),
    SUBPARTITION p_list_1_2 VALUES ( default )
  ),
  PARTITION p_list_2 VALUES(0,1,2,3,4,5,6,7,8,9)
  (
    SUBPARTITION p_list_2_1 VALUES ( 0,1,2,3,4,5,6,7,8,9 ),
    SUBPARTITION p_list_2_2 VALUES ( default ),
    SUBPARTITION p_list_2_3 VALUES ( 10,11,12,13,14,15,16,17,18,19),
    SUBPARTITION p_list_2_4 VALUES ( 20,21,22,23,24,25,26,27,28,29 ),
    SUBPARTITION p_list_2_5 VALUES ( 30,31,32,33,34,35,36,37,38,39 )
  ),
  PARTITION p_list_3 VALUES(10,11,12,13,14,15,16,17,18,19)
  (
    SUBPARTITION p_list_3_2 VALUES ( default )
  ),
  PARTITION p_list_4 VALUES(default ),
  PARTITION p_list_5 VALUES(20,21,22,23,24,25,26,27,28,29)
  (
    SUBPARTITION p_list_5_1 VALUES ( 0,1,2,3,4,5,6,7,8,9 ),
    SUBPARTITION p_list_5_2 VALUES ( default ),
    SUBPARTITION p_list_5_3 VALUES ( 10,11,12,13,14,15,16,17,18,19),
    SUBPARTITION p_list_5_4 VALUES ( 20,21,22,23,24,25,26,27,28,29 ),
    SUBPARTITION p_list_5_5 VALUES ( 30,31,32,33,34,35,36,37,38,39 )
  ),
  PARTITION p_list_6 VALUES(30,31,32,33,34,35,36,37,38,39),
  PARTITION p_list_7 VALUES(40,41,42,43,44,45,46,47,48,49)
  (
    SUBPARTITION p_list_7_1 VALUES ( default )
  )
) ENABLE ROW MOVEMENT;

insert into list_list values(1,1,'aa');
insert into list_list values(5,5,'bb');
insert into list_list values(11,2,'cc');
insert into list_list values(19,8,'dd');

explain (costs off) select * from list_list;
select * from list_list;

drop table if exists list_list_02;
CREATE TABLE IF NOT EXISTS list_list_02
(
  col_1 int ,
  col_2 int DEFAULT 20 ,
  col_3 int ,
  col_4 int
)
PARTITION BY list (col_1) SUBPARTITION BY list (col_2)
(
  PARTITION p_list_1 VALUES(-1,-2,-3,-4,-5,-6,-7,-8,-9,-10 )
  (
    SUBPARTITION p_list_1_1 VALUES ( 0,-1,-2,-3,-4,-5,-6,-7,-8,-9 ),
    SUBPARTITION p_list_1_2 VALUES ( default )
  ),
  PARTITION p_list_2 VALUES(0,1,2,3,4,5,6,7,8,9)
  (
    SUBPARTITION p_list_2_1 VALUES ( 0,1,2,3,4,5,6,7,8,9 ),
    SUBPARTITION p_list_2_2 VALUES ( default ),
    SUBPARTITION p_list_2_3 VALUES ( 10,11,12,13,14,15,16,17,18,19),
    SUBPARTITION p_list_2_4 VALUES ( 20,21,22,23,24,25,26,27,28,29 ),
    SUBPARTITION p_list_2_5 VALUES ( 30,31,32,33,34,35,36,37,38,39 )
  ),
  PARTITION p_list_3 VALUES(10,11,12,13,14,15,16,17,18,19)
  (
    SUBPARTITION p_list_3_2 VALUES ( default )
  ),
  PARTITION p_list_4 VALUES(default ),
  PARTITION p_list_5 VALUES(20,21,22,23,24,25,26,27,28,29)
  (
    SUBPARTITION p_list_5_1 VALUES ( 0,1,2,3,4,5,6,7,8,9 ),
    SUBPARTITION p_list_5_2 VALUES ( default ),
    SUBPARTITION p_list_5_3 VALUES ( 10,11,12,13,14,15,16,17,18,19),
    SUBPARTITION p_list_5_4 VALUES ( 20,21,22,23,24,25,26,27,28,29 ),
    SUBPARTITION p_list_5_5 VALUES ( 30,31,32,33,34,35,36,37,38,39 )
  ),
  PARTITION p_list_6 VALUES(30,31,32,33,34,35,36,37,38,39),
  PARTITION p_list_7 VALUES(40,41,42,43,44,45,46,47,48,49)
  (
    SUBPARTITION p_list_7_1 VALUES ( default )
  )
) ENABLE ROW MOVEMENT;

insert into list_list_02(col_1,col_3,col_4) values(1,1,1),(5,5,5);
select * from list_list_02;

set try_vector_engine_strategy=force;
select * from list_list_02 partition(p_list_2);

truncate list_list_02;
insert into list_list_02 values(0,0,0,0);
insert into list_list_02 values(-11,1,1,1),(-14,1,4,4),(-25,15,5,5),(-808,8,8,8),(-9,9,9,9);
insert into list_list_02 values(1,11,1,12),(4,41,4,48),(5,54,5,57),(8,87,8,84),(9,19,9,97);
insert into list_list_02 values(11,1,1,13),(14,1,4,49),(15,5,5,52),(18,8,8,81),(19,1,9,93);

create index index_01 on list_list_02(col_2  ASC ) local;
create index index_02 on list_list_02(col_2  DESC) local;
create index index_03 on list_list_02(col_2  NULLS first) local;
create index index_04 on list_list_02(col_2  NULLS LAST ) local;

explain (analyze on, timing off) select /*+ indexscan (list_list_02 index_01)*/ * from list_list_02 where col_2 in  (select col_1 from list_list_02 aa where col_1 >10 and col_1<100) order by 2 asc limit 100;

explain (analyze on, timing off) select * from list_list_02 where ctid='(0,2)';

set current_schema=public;
drop schema vector_subpartition cascade;
