DROP SCHEMA partition_pruning;
CREATE SCHEMA partition_pruning;
SET CURRENT_SCHEMA TO partition_pruning;

drop table test_range;
create table test_range (a int, b int, c int) WITH (STORAGE_TYPE=USTORE)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT; 

insert into test_range values(1,1,1);
insert into test_range values(3001,1,1);

prepare p1 as select  * from test_range where ctid = '(0,1)' and a = $1;
explain (costs off)execute p1(1);
execute p1(1);
execute p1(3001);
drop table test_range;

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
insert into test_range_pt values(1,1),(2001,2),(3001,3),(4001,4),(5001,5);

deallocate p1;
prepare p1 as select * from test_range_pt  where a = $1 or a is null;
explain (costs off)execute p1(2001);
execute p1(2001);
deallocate p1;
prepare p1 as select * from test_range_pt  where a = $1 or a = $2;
explain (costs off)execute p1(2001,3001);
execute p1(2001,3001);
deallocate p1;
prepare p1 as select * from test_range_pt  where a = $1 and a = $2;
explain (costs off)execute p1(2001,3001);
execute p1(2001,3001);
drop table test_range_pt;

DROP SCHEMA partition_pruning;
