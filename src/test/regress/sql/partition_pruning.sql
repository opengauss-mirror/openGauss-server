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

create table par4_1188069(id int,a1 text,a2 date,a3 varchar(30))
partition by range (a3)
(
partition p1 values less than('d'),
partition p2 values less than('k'),
partition p3 values less than('q'),
partition p4 values less than('z'));

insert into par4_1188069 values(generate_series(1,100),'d',generate_series(DATE '2022-01-01', DATE '2022-4-10', '1 day'),chr(65 + (generate_series(1,100)-1)%25));
insert into par4_1188069 values(generate_series(101,200),'k',generate_series(DATE '2022-01-01', DATE '2022-4-10', '1 day'),chr(65 + (generate_series(1,100)-1)%25));
insert into par4_1188069 values(generate_series(201,300),'q',generate_series(DATE '2022-01-01', DATE '2022-4-10', '1 day'),chr(65 + (generate_series(1,100)-1)%25));
insert into par4_1188069 values(generate_series(301,400),null,generate_series(DATE '2022-01-01', DATE '2022-4-10', '1 day'),chr(65 + (generate_series(1,100)-1)%25)); 

prepare l7_1188069(varchar,varchar) as select * from par4_1188069 where a3 in($1,$2) limit 3;
explain (analyze,costs off) execute l7_1188069('h','v');

execute l7_1188069('H','V');

deallocate l7_1188069;
drop table par4_1188069;

DROP SCHEMA partition_pruning;
