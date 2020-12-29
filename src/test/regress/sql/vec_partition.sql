set current_schema=vector_engine;
set enable_vector_engine=on;
select * from LINEITEM_partition where L_ORDERKEY = 1::int8;
select L_ORDERKEY from LINEITEM_partition where L_ORDERKEY > 2000::int8 and L_ORDERKEY < 2017::int8;
select L_ORDERKEY from LINEITEM_partition where L_ORDERKEY = 8001::int8;
explain (verbose on, costs off) select L_ORDERKEY from LINEITEM_partition where L_ORDERKEY > 8000::int8 ;

reset enable_vector_engine;


create table test_vec_sortinfo_row(b1 VARCHAR(1000), b2 INT,  b4 INT) 
 partition by range(b2) 
(
PARTITION  p1 VALUES LESS THAN(1),
PARTITION  p50001 VALUES LESS THAN(50001),
PARTITION  p100001  VALUES LESS THAN(100001),
PARTITION  p150001  VALUES LESS THAN(150001),
partition p_max values less than(maxvalue)
) enable row movement;
create table test_vec_sortinfo(b1 VARCHAR(1000), b2 INT,  b4 INT) with(orientation=column) 
 partition by range(b2) 
(
PARTITION  p1 VALUES LESS THAN(1),
PARTITION  p50001 VALUES LESS THAN(50001),
PARTITION  p100001  VALUES LESS THAN(100001),
PARTITION  p150001  VALUES LESS THAN(150001),
partition p_max values less than(maxvalue)
) enable row movement;
insert into test_vec_sortinfo_row values('a',1,2);
insert into test_vec_sortinfo_row values('a',1,2);
insert into test_vec_sortinfo_row values('a',1,2);
insert into test_vec_sortinfo_row values('a',1,2);
insert into test_vec_sortinfo_row values('a',1,2);
insert into test_vec_sortinfo select * from test_vec_sortinfo_row;

explain (verbose, costs off) select distinct b2 from test_vec_sortinfo order by 1;

drop table test_vec_sortinfo;
