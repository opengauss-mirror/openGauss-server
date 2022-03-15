set enable_opfusion=on;
set enable_partition_opfusion=on;
set enable_bitmapscan=off;
set enable_seqscan=off;
set opfusion_debug_mode = 'log';
set log_min_messages=debug;
set logging_module = 'on(OPFUSION)';
set sql_beta_feature = 'index_cost_with_leaf_pages_only';
--create table
drop table if exists test_bypass_sql_partition;
create table test_bypass_sql_partition(col1 int, col2 int, col3 text)
partition by range (col1)
(
partition test_bypass_sql_partition_1 values less than(10),
partition test_bypass_sql_partition_2 values less than(20),
partition test_bypass_sql_partition_3 values less than(30),
partition test_bypass_sql_partition_4 values less than(40),
partition test_bypass_sql_partition_5 values less than(50),
partition test_bypass_sql_partition_6 values less than(60),
partition test_bypass_sql_partition_7 values less than(70),
partition test_bypass_sql_partition_8 values less than(80)
);
create index itest_bypass_sql_partition on test_bypass_sql_partition(col1,col2) local;
--insert
explain insert into test_bypass_sql_partition values (0,0,'test_insert');
insert into test_bypass_sql_partition values (0,0,'test_insert');
explain insert into test_bypass_sql_partition values (0,1,'test_insert');
insert into test_bypass_sql_partition values (0,1,'test_insert');
explain insert into test_bypass_sql_partition values (11,1,'test_insert');
insert into test_bypass_sql_partition values (11,1,'test_insert');
explain insert into test_bypass_sql_partition values (11,2,'test_insert');
insert into test_bypass_sql_partition values (11,2,'test_insert');
explain insert into test_bypass_sql_partition values (0,10,'test_insert2');
insert into test_bypass_sql_partition values (0,10,'test_insert2');
explain insert into test_bypass_sql_partition values (2,12,'test_insert2');
insert into test_bypass_sql_partition values (2,12,'test_insert2');
explain insert into test_bypass_sql_partition values (30,0,'test_insert3');
insert into test_bypass_sql_partition values (30,0,'test_insert3');
explain insert into test_bypass_sql_partition values (3,3,'test_insert3');
insert into test_bypass_sql_partition values (3,3,'test_insert3');
explain insert into test_bypass_sql_partition(col1,col2) values (1,1);
insert into test_bypass_sql_partition(col1,col2) values (1,1);
explain insert into test_bypass_sql_partition(col1,col2) values (22,2);
insert into test_bypass_sql_partition(col1,col2) values (22,2);
explain insert into test_bypass_sql_partition(col1,col2) values (33,3);
insert into test_bypass_sql_partition(col1,col2) values (33,3);
--error
explain insert into test_bypass_sql_partition values (null,null,null);
insert into test_bypass_sql_partition values (null,null,null);
--nobypass
explain insert into test_bypass_sql_partition values(0,generate_series(1,100),'test');
insert into test_bypass_sql_partition values(0,generate_series(1,100),'test');
--select
drop table if exists test_bypass_sql_partition;
create table test_bypass_sql_partition(col1 int, col2 int, col3 text)
partition by range (col1)
(
partition test_bypass_sql_partition_1 values less than(10),
partition test_bypass_sql_partition_2 values less than(20),
partition test_bypass_sql_partition_3 values less than(30),
partition test_bypass_sql_partition_4 values less than(40),
partition test_bypass_sql_partition_5 values less than(50),
partition test_bypass_sql_partition_6 values less than(60),
partition test_bypass_sql_partition_7 values less than(70),
partition test_bypass_sql_partition_8 values less than(80)
);
create index itest_bypass_sql_partition on test_bypass_sql_partition(col1,col2) local;
insert into test_bypass_sql_partition select generate_series(0,79,1), generate_series(0,100,10), repeat('a',7);
--bypass
set enable_indexonlyscan=off;
explain select * from test_bypass_sql_partition where col1=0 and col2=0;
select * from test_bypass_sql_partition where col1=0 and col2=0;
explain select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 and col2 <= 20 order by col1,col2;
select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 and col2 <= 20 order by col1,col2;
explain select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 order by col1,col2 limit 1;
select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 order by col1,col2 limit 1;
explain select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 order by col1,col2 for update limit 1;
select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 order by col1,col2 for update limit 1;
explain select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 order by col1,col2 limit 0;
select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 and col2>0 order by col1,col2 limit 0;
explain select col1,col2 from test_bypass_sql_partition where col1=10 and col2=0 order by col1,col2 for update limit 0;
select col1,col2 from test_bypass_sql_partition where col1=10 and col2=0 order by col1,col2 for update limit 0;

explain select col1,col2 from test_bypass_sql_partition where col1 is not null and col2 is not null order by col1,col2;
select col1,col2 from test_bypass_sql_partition where col1 is not null and col2 is not null order by col1,col2;
explain select * from test_bypass_sql_partition where col1 is not null and col2 = 0 order by col1;
select * from test_bypass_sql_partition where col1 is not null and col2 = 0 order by col1;
explain select * from test_bypass_sql_partition where col1=0 and col2=-1;
select * from test_bypass_sql_partition where col1=0 and col2=-1;
reset enable_indexonlyscan;
--bypass though index only scan
set enable_indexscan = off;
explain select col1,col2 from test_bypass_sql_partition where col1=10 and col2=10;
select col1,col2 from test_bypass_sql_partition where col1=10 and col2=10;
explain select col1,col2 from test_bypass_sql_partition where col1=10 and col2=10 order by col1 limit 1;
select col1,col2 from test_bypass_sql_partition where col1=10 and col2=10 order by col1 limit 1;
explain select col1,col2 from test_bypass_sql_partition where col1=10 and col2=10 order by col1 limit 0;
select col1,col2 from test_bypass_sql_partition where col1=10 and col2=10 order by col1 limit 0;
reset enable_indexscan;
--error
explain select * from test_bypass_sql_partition where col1=0 and col2=0 order by col1 limit -1;
select * from test_bypass_sql_partition where col1=0 and col2=0 order by col1 limit -1;
explain select * from test_bypass_sql_partition where col1=0 and col2=0 order by col1 for update limit -1;
select * from test_bypass_sql_partition where col1=0 and col2=0 order by col1 for update limit -1;
--nobypass
explain select * from test_bypass_sql_partition where col1 is null and col2 is null;
select * from test_bypass_sql_partition where col1 is null and col2 is null;
select col1, col2 from test_bypass_sql_partition where col1 <= 30 and col1 >= 10 order by col1 limit 10;
explain select col1, col2 from test_bypass_sql_partition where col1 <= 30 and col1 >= 10 order by col1 limit 10;
select col1, col2 from test_bypass_sql_partition order by col1 limit 10;
explain select col1, col2 from test_bypass_sql_partition order by col1 limit 10;
select col1, col2 from test_bypass_sql_partition where col1 > 0 order by col1 limit 10;
explain select col1, col2 from test_bypass_sql_partition where col1 > 0 order by col1 limit 10;
select col1, col2 from test_bypass_sql_partition where col1 < 20 order by col1 limit 10;
explain select col1, col2 from test_bypass_sql_partition where col1 < 20 order by col1 limit 10;

--update 
--bypass

explain update test_bypass_sql_partition set col2=col2-1,col3='test_update' where col1=0 and col2=0;
update test_bypass_sql_partition set col2=col2-1,col3='test_update' where col1=10 and col2=0;
explain update test_bypass_sql_partition set col2=col1-1,col3='test_update' where col1=20 and col2=2;
update test_bypass_sql_partition set col2=col1-1,col3='test_update' where col1=20 and col2=2;
explain update test_bypass_sql_partition set col2=mod(5,3)  where col1=1 and col2=10;
update test_bypass_sql_partition set col2=mod(5,3)  where col1=1 and col2=10;
--not bypass
explain insert into test_bypass_sql_partition values(0,generate_series(1,100),'test');
explain select * from test_bypass_sql_partition where col3 is not null;
explain update test_bypass_sql_partition set col3='test_null' where col1 is null and col2 is null;
update test_bypass_sql_partition set col3='test_null' where col1 is null and col2 is null;


--bypass
explain update test_bypass_sql_partition set col2=111,col3='test_update2' where  col1=0;
update test_bypass_sql_partition set col2=111,col3='test_update2' where  col1=0;
explain select * from test_bypass_sql_partition where col1=0 order by col1;
select * from test_bypass_sql_partition where col1=0 order by col1;
explain select * from test_bypass_sql_partition where col1=0 order by col1 for update limit 2;
select * from test_bypass_sql_partition where col1=0 order by col1 for update limit 2;
explain select * from test_bypass_sql_partition where col1=1 and col2=20 order by col1 for update limit 1;
select * from test_bypass_sql_partition where col1=1 and col2=20 order by col1 for update limit 1;

--nobypass
explain select * from test_bypass_sql_partition where col2=20 order by col1;
select * from test_bypass_sql_partition where col2=20 order by col1;
explain select col1,col2 from test_bypass_sql_partition where col1>0 order by col1 limit 10;
select col1,col2 from test_bypass_sql_partition where col1>0 order by col1 limit 10;
explain select col1,col2 from test_bypass_sql_partition where col1>0 order by col1 limit 3;
select col1,col2 from test_bypass_sql_partition where col1>0 order by col1 limit 3;
explain select col1,col2 from test_bypass_sql_partition where col2<50 order by col1 limit 10;
select col1,col2 from test_bypass_sql_partition where col2<50 order by col1 limit 10;
explain select col1,col2 from test_bypass_sql_partition where col1>=0 and col2>0 order by col1 limit 10;
select col1,col2 from test_bypass_sql_partition where col1>=0 and col2>0 order by col1 limit 10;
explain select * from test_bypass_sql_partition where col1>=0 and col2>0 order by col1 limit 3;
select * from test_bypass_sql_partition where col1>=0 and col2>0 order by col1 limit 3;


drop table if exists test_bypass_sql_partition;
create table test_bypass_sql_partition(col1 int, col2 int, col3 text)
partition by range (col1)
(
partition test_bypass_sql_partition_1 values less than(10),
partition test_bypass_sql_partition_2 values less than(20),
partition test_bypass_sql_partition_3 values less than(30),
partition test_bypass_sql_partition_4 values less than(40),
partition test_bypass_sql_partition_5 values less than(50),
partition test_bypass_sql_partition_6 values less than(60),
partition test_bypass_sql_partition_7 values less than(70),
partition test_bypass_sql_partition_8 values less than(80)
);
create index itest_bypass_sql_partition on test_bypass_sql_partition(col1,col2) local;
insert into test_bypass_sql_partition select generate_series(0,79,1), generate_series(0,100,10), repeat('a',7);

--bypass though index only scan
set enable_indexscan = off;
explain select col1,col2 from  test_bypass_sql_partition where col1=0 order by col2;
select col1,col2 from  test_bypass_sql_partition where col1=0 order by col2;
explain select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 limit 10;
select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 limit 10;
explain select col2,col1 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 limit 3;
select col2,col1 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 limit 3;
explain select col1,col2 from test_bypass_sql_partition where col1>=0 and col1<10 and col2>0 order by col1 limit 3;
select col1,col2 from test_bypass_sql_partition where col1>=0 and col1<10 and col2>0 order by col1 limit 3;
explain select col1,col2 from test_bypass_sql_partition where col1>=0 and col1<10 and col2>0 order by col1 limit null;
select col1,col2 from test_bypass_sql_partition where col1>=0 and col1<10 and col2>0 order by col1 limit null;
--nobypass
explain select col2,col1 from test_bypass_sql_partition where col2=2 order by col1 limit 10;
select col2,col1 from test_bypass_sql_partition where col2=2 order by col1 limit 10;

explain select col1,col2 from test_bypass_sql_partition where col1 is null and col2 is null limit 10;
select col1,col2 from test_bypass_sql_partition where col1 is null and col2 is null limit 10;

explain select col1,col2 from test_bypass_sql_partition where col2<5 order by col1 limit 10;
select col1,col2 from test_bypass_sql_partition where col2<5 order by col1 limit 10;
explain select col1,col2 from test_bypass_sql_partition where col1>=0 and col1<=10 and col2>0 order by col1 limit 10;
select col1,col2 from test_bypass_sql_partition where col1>=0 and col1<=10 and col2>0 order by col1 limit 10;

reset enable_indexscan;

--nobypass
explain select * from test_bypass_sql_partition where col1>col2 limit 10;
explain select * from test_bypass_sql_partition where col1=3 and col2=3 for update;
select * from test_bypass_sql_partition where col1=3 and col2=3 for update;
explain select * from test_bypass_sql_partition where col3='test_update2';

explain select * from test_bypass_sql_partition where col1>0 and col1<10 and col2>0 order by col1 limit 3 offset 3;
select * from test_bypass_sql_partition where col1>0 and col1<10 and col2>0 order by col1 limit 3 offset 3;
explain select * from test_bypass_sql_partition where col1>0 and col1<10 order by col1 for update limit 3 offset 3;
explain select * from test_bypass_sql_partition where col1>0 and col1<10 order by col1 for update limit 3 offset null;
explain select * from test_bypass_sql_partition where col1>0 and col1<10 and col2>0 order by col1 offset 3;
select * from test_bypass_sql_partition where col1>0 and col1<10 and col2>0 order by col1 offset 3;
explain select * from test_bypass_sql_partition where col1>0 and col1<10 order by col1 for update offset 3;
explain update test_bypass_sql_partition set col2=3*7  where col1=3 and col2=2;
update test_bypass_sql_partition set col2=3*7  where col1=3 and col2=2;
explain delete from  test_bypass_sql_partition where col1=1 and col2=1;
delete from  test_bypass_sql_partition where col1=1 and col2=1;
--error
explain delete from test_bypass_sql_partition where col1 is null and col2 is null;
delete from test_bypass_sql_partition where col1 is null and col2 is null;
explain insert into test_bypass_sql_partition values (null,null,null);
insert into test_bypass_sql_partition values (null,null,null);
--bypass / set  enable_bitmapscan=off;
select * from test_bypass_sql_partition where col1=3;
explain select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 desc;
select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 limit 10 desc; --order by is supported when ordered col is in index
explain select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 limit 10 order by col1;
select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 limit 10 order by col1;
--not bypass
explain select col1,col2 from test_bypass_sql_partition order by col1,col2 limit 10;
select col1,col2 from test_bypass_sql_partition order by col1,col2 limit 10;
explain select * from test_bypass_sql_partition where col1 > 0 order by col1,col2 desc limit 10;
--bypass
explain select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 order by col1,col2 limit 10 ;
select col1,col2 from test_bypass_sql_partition where col1>0 and col1<10 order by col1,col2 limit 10;
--not bypass
explain select * from test_bypass_sql_partition where true;
explain select col1, col2 from test_bypass_sql_partition where true order by col1 limit 10;
select col1, col2 from test_bypass_sql_partition where true order by col1 limit 10;
--bypass

select col2, col1 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 limit 10;
select col1, col2 from test_bypass_sql_partition where col1>0 and col1<10 order by col1 desc limit 10;
explain insert into test_bypass_sql_partition select * from test_bypass_sql_partition where col1=0;

delete from test_bypass_sql_partition where col1=1;
delete from test_bypass_sql_partition where col1 > 10 and col1 < 0;
delete from test_bypass_sql_partition where col1 <= 11 and col1 >= 15;

--nobypass
delete from test_bypass_sql_partition where col1 > 10;
delete from test_bypass_sql_partition where col1 < 10;
delete from test_bypass_sql_partition where col1 >= 10 and col1 <= 30;

create table t1(
crcrd_acg_setl_dt char(8) not null,
cst_id char(18),
multi_tenancy_id char(5)
)
partition by range (multi_tenancy_id, crcrd_acg_setl_dt)
(
	partition p1 values less than ('CN000', '20191201'),
    partition p2 values less than ('CN000', '20200201'),
	partition p3 values less than ('CN000', '20200202'),
	partition p4 values less than ('CN000', '20200203'),
	partition p5 values less than ('CN000', '20200204'),
	partition p6 values less than ('ZZZZZ', '21000101')
)
enable row movement
;
 
create index on t1(crcrd_acg_setl_dt, cst_id, multi_tenancy_id) local;
 
insert into t1 values('20200201', '107190000103394943', 'CN000');
insert into t1 values('20200225', '107190000103394943', 'CN000');
insert into t1 values('20200228', '107190000103394943', 'CN000');
insert into t1 values('20200301', '107190000103394943', 'CN000');
insert into t1 values('20200310', '107190000103394943', 'CN000');
 
set enable_seqscan = off;
 
select max(crcrd_acg_setl_dt) from t1 where cst_id='107190000103394943' and multi_tenancy_id = 'CN000';
 
prepare p1 as select max(crcrd_acg_setl_dt) from t1 where cst_id=$1 and multi_tenancy_id = $2;
 
execute p1 ('107190000103394943','CN000');
deallocate p1;

drop table t1;
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
 
create index idx1 on test_range_pt(a) local;
prepare p1 as select max(a) from test_range_pt where a>$1;
execute p1 (1);
deallocate p1;
select max(a) from test_range_pt where a>b+1;
drop table test_range_pt;
drop table test_list_lt1;
create table test_list_lt1 (a int, b int )
partition by list(a)
(
	partition p1 values (2000),
	partition p2 values (3000),
	partition p3 values (4000)
) ;
prepare p1 as select * from test_list_lt1 where  a = $1 and ctid = '(0,1)';
execute p1 (1);
deallocate p1;
drop table test_list_lt1;

reset enable_partition_opfusion;
drop table test_bypass_sql_partition;

