--
--simple query support
--
set enable_opfusion=on;
set enable_bitmapscan=off;
set enable_seqscan=off;
set opfusion_debug_mode = 'log';
set log_min_messages=debug;
set logging_module = 'on(OPFUSION)';
set sql_beta_feature = 'index_cost_with_leaf_pages_only';

-- create table
drop table if exists test_bypass_sq1;
create table test_bypass_sq1(col1 int, col2 int, col3 text);
create index itest_bypass_sq1 on test_bypass_sq1(col1,col2);
-- bypass insert data
 explain insert into test_bypass_sq1 values (0,0,'test_insert');
 insert into test_bypass_sq1 values (0,0,'test_insert');
 explain insert into test_bypass_sq1 values (0,1,'test_insert');
 insert into test_bypass_sq1 values (0,1,'test_insert');
 explain insert into test_bypass_sq1 values (1,1,'test_insert');
 insert into test_bypass_sq1 values (1,1,'test_insert');
 explain insert into test_bypass_sq1 values (1,2,'test_insert');
 insert into test_bypass_sq1 values (1,2,'test_insert');
 explain insert into test_bypass_sq1 values (0,0,'test_insert2');
 insert into test_bypass_sq1 values (0,0,'test_insert2');
 explain insert into test_bypass_sq1 values (2,2,'test_insert2');
 insert into test_bypass_sq1 values (2,2,'test_insert2');
 explain insert into test_bypass_sq1 values (0,0,'test_insert3');
 insert into test_bypass_sq1 values (0,0,'test_insert3');
 explain insert into test_bypass_sq1 values (3,3,'test_insert3');
 insert into test_bypass_sq1 values (3,3,'test_insert3');
 explain insert into test_bypass_sq1(col1,col2) values (1,1);
 insert into test_bypass_sq1(col1,col2) values (1,1);
 explain insert into test_bypass_sq1(col1,col2) values (2,2);
 insert into test_bypass_sq1(col1,col2) values (2,2);
 explain insert into test_bypass_sq1(col1,col2) values (3,3);
 insert into test_bypass_sq1(col1,col2) values (3,3);
 explain insert into test_bypass_sq1 values (null,null,null);
 insert into test_bypass_sq1 values (null,null,null);
--bypass
set enable_indexonlyscan=off;
explain select * from test_bypass_sq1 where col1=0 and col2=0;
select * from test_bypass_sq1 where col1=0 and col2=0;
explain select col1,col2 from test_bypass_sq1 where col1>0 and col2>0 order by col1,col2;
select col1,col2 from test_bypass_sq1 where col1>0 and col2>0 order by col1,col2;
explain select col1,col2 from test_bypass_sq1 where col1>0 and col2>0 order by col1,col2 limit 1;
select col1,col2 from test_bypass_sq1 where col1>0 and col2>0 order by col1,col2 limit 1;
explain select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1,col2 for update limit 1;
select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1,col2 for update limit 1;
explain select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1,col2 limit 0;
select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1,col2 limit 0;
explain select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1,col2 for update limit 0;
select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1,col2 for update limit 0;
reset enable_indexonlyscan;
--bypass though index only scan
set enable_indexscan = off;
explain select col1,col2 from test_bypass_sq1 where col1=0 and col2=0;
select col1,col2 from test_bypass_sq1 where col1=0 and col2=0;
explain select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1 limit 1;
select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1 limit 1;
explain select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1 limit 0;
select col1,col2 from test_bypass_sq1 where col1=0 and col2=0 order by col1 limit 0;
reset enable_indexscan;
--error
explain select * from test_bypass_sq1 where col1=0 and col2=0 order by col1 limit -1;
explain select * from test_bypass_sq1 where col1=0 and col2=0 order by col1 for update limit -1;
--bypass
explain update  test_bypass_sq1 set col3='test_null' where col1 is null and col2 is null;
update  test_bypass_sq1 set col3='test_null' where col1 is null and col2 is null;
explain select * from test_bypass_sq1 where col1 is null and col2 is null;
select * from test_bypass_sq1 where col1 is null and col2 is null;
explain select col1,col2 from test_bypass_sq1 where col1 is not null and col2 is not null order by col1,col2;
select col1,col2 from test_bypass_sq1 where col1 is not null and col2 is not null order by col1,col2;
explain select * from test_bypass_sq1 where col1 is not null and col2 = 0 order by col1;
select * from test_bypass_sq1 where col1 is not null and col2 = 0 order by col1;
explain update test_bypass_sq1 set col2=col2-1,col3='test_update' where col1=0 and col2=0;
update test_bypass_sq1 set col2=col2-1,col3='test_update' where col1=0 and col2=0;
explain update test_bypass_sq1 set col2=col1 where col1=0 and col2=0;
update test_bypass_sq1 set col2=col1 where col1=0 and col2=0;
explain update test_bypass_sq1 set col2=col1-1,col3='test_update' where col1=2 and col2=2;
update test_bypass_sq1 set col2=col1-1,col3='test_update' where col1=2 and col2=2;
explain select * from test_bypass_sq1 where col1=0 and col2=-1;
select * from test_bypass_sq1 where col1=0 and col2=-1;
--not bypass
explain insert into test_bypass_sq1 values(0,generate_series(1,100),'test');
explain select * from test_bypass_sq1 where col3 is not null;
--bypass
explain update test_bypass_sq1 set col2=mod(5,3)  where col1=1 and col2=1;
update test_bypass_sq1 set col2=mod(5,3)  where col1=1 and col2=1;
--bypass / set  enable_bitmapscan=off;
explain update test_bypass_sq1 set col2=111,col3='test_update2' where  col1=0;
update test_bypass_sq1 set col2=111,col3='test_update2' where  col1=0;
explain select * from test_bypass_sq1 where col1=0 order by col1;
select * from test_bypass_sq1 where col1=0 order by col1;
explain select * from test_bypass_sq1 where col2=2 order by col1;
select * from test_bypass_sq1 where col2=2 order by col1;
explain select col1,col2 from test_bypass_sq1 where col1>0 order by col1;
select col1,col2 from test_bypass_sq1 where col1>0 order by col1;
explain select col1,col2 from test_bypass_sq1 where col1>0 order by col1 limit 3;
select col1,col2 from test_bypass_sq1 where col1>0 order by col1 limit 3;
explain select * from test_bypass_sq1 where col1=0 order by col1 for update limit 2;
select * from test_bypass_sq1 where col1=0 order by col1 for update limit 2;
explain select col1,col2 from test_bypass_sq1 where col2<5 order by col1;
select col1,col2 from test_bypass_sq1 where col2<5 order by col1;
explain select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1;
select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1;
explain select * from test_bypass_sq1 where col1>=0 and col2>0 order by col1 limit 3;
select * from test_bypass_sq1 where col1>=0 and col2>0 order by col1 limit 3;
explain select * from test_bypass_sq1 where col1=1 and col2=2 order by col1 for update limit 1;
select * from test_bypass_sq1 where col1=1 and col2=2 order by col1 for update limit 1;
--bypass though index only scan
set enable_indexscan = off;
explain select col1,col2 from  test_bypass_sq1 where col1=0 order by col2;
select col1,col2 from  test_bypass_sq1 where col1=0 order by col2;
explain select col2,col1 from test_bypass_sq1 where col2=2 order by col1;
select col2,col1 from test_bypass_sq1 where col2=2 order by col1;
explain select col1,col2 from test_bypass_sq1 where col1>0 order by col1;
select col1,col2 from test_bypass_sq1 where col1>0 order by col1;
explain select col1,col2 from test_bypass_sq1 where col1 is null and col2 is null;
select col1,col2 from test_bypass_sq1 where col1 is null and col2 is null;
explain select col2,col1 from test_bypass_sq1 where col1>0 order by col1 limit 3;
select col2,col1 from test_bypass_sq1 where col1>0 order by col1 limit 3;
explain select col1,col2 from test_bypass_sq1 where col2<5 order by col1;
select col1,col2 from test_bypass_sq1 where col2<5 order by col1;
explain select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1;
select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1;
explain select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1 limit 3;
select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1 limit 3;
explain select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1 limit null;
select col1,col2 from test_bypass_sq1 where col1>=0 and col2>0 order by col1 limit null;
reset enable_indexscan;
--not bypass
explain select * from test_bypass_sq1 where col1>col2;
explain select * from test_bypass_sq1 where col1=3 and col2=3 for update;
select * from test_bypass_sq1 where col1=3 and col2=3 for update;
explain select * from test_bypass_sq1 where col3='test_update2';
--bypass
explain select * from test_bypass_sq1 where col1>0 and col2>0 order by col1 limit 3 offset 3;
select * from test_bypass_sq1 where col1>0 and col2>0 order by col1 limit 3 offset 3;
select * from test_bypass_sq1 where col1>0 and col2>0 order by col1 limit 3 offset 30;
explain select * from test_bypass_sq1 where col1>0  order by col1 for update limit 3 offset 3;
explain select * from test_bypass_sq1 where col1>0  order by col1 for update limit 3 offset null;
explain select * from test_bypass_sq1 where col1>0  order by col1 for update limit 3 offset 30;
explain select * from test_bypass_sq1 where col1>0 and col2>0 order by col1 offset 3;
select * from test_bypass_sq1 where col1>0 and col2>0 order by col1 offset 3;
select * from test_bypass_sq1 where col1>0  order by col1 for update limit 3 offset 30;
explain select * from test_bypass_sq1 where col1>0 order by col1 for update offset 3;
explain update test_bypass_sq1 set col2=3*7  where col1=3 and col2=2;
update test_bypass_sq1 set col2=3*7  where col1=3 and col2=2;
explain delete from  test_bypass_sq1 where col1=1 and col2=1;
delete from  test_bypass_sq1 where col1=1 and col2=1;
explain delete from test_bypass_sq1 where col1 is null and col2 is null;
delete from test_bypass_sq1 where col1 is null and col2 is null;
explain insert into test_bypass_sq1 values (null,null,null);
insert into test_bypass_sq1 values (null,null,null);
--bypass / set  enable_bitmapscan=off;
select * from test_bypass_sq1 where col1=3;
explain select col1,col2 from test_bypass_sq1 order by col1 desc;
select col1,col2 from test_bypass_sq1 order by col1 desc; --order by is supported when ordered col is in index
explain select col1,col2 from test_bypass_sq1 order by col1;
select col1,col2 from test_bypass_sq1 order by col1;
--not bypass
explain select col1,col2 from test_bypass_sq1 order by col1,col2;
select col1,col2 from test_bypass_sq1 order by col1,col2;
explain select * from test_bypass_sq1 where col1 > 0 order by col1,col2 desc;
--bypass
explain select col1,col2 from test_bypass_sq1 where col1 > 0 order by col1,col2;
select col1,col2 from test_bypass_sq1 where col1 > 0 order by col1,col2;
--not bypass
explain select * from test_bypass_sq1 where true;
--bypass
explain select col1, col2 from test_bypass_sq1 where true order by col1;
select col1, col2 from test_bypass_sq1 where true order by col1;
select col2, col1 from test_bypass_sq1 order by col1;
select col1, col2 from test_bypass_sq1 order by col1 desc;
explain insert into test_bypass_sq1 select * from test_bypass_sq1 where col1>0;
insert into test_bypass_sq1 select * from test_bypass_sq1 where col1>0;

--
drop table if exists test_bypass_sq2;
create table test_bypass_sq2(col1 int not null, col2 int);
create index itest_bypass_sq2 on test_bypass_sq2(col1);
--bypass
explain insert into test_bypass_sq2(col1) values (0);
insert into test_bypass_sq2(col1) values (0);
--error
explain insert into test_bypass_sq2(col1) values (null);
--bypass
explain insert into test_bypass_sq2(col1,col2) values (1,1);
insert into test_bypass_sq2(col1,col2) values (1,1);
insert into test_bypass_sq2(col1,col2) values (3,3);
insert into test_bypass_sq2(col1,col2) values (-1,-1);
insert into test_bypass_sq2(col1,col2) values (1,1);
insert into test_bypass_sq2(col1,col2) values (2,2);
insert into test_bypass_sq2(col1,col2) values (3,3);
explain insert into test_bypass_sq2(col1,col2) values (null,null);--error

--bypass
set enable_indexonlyscan=off;
explain update test_bypass_sq2 set col2 = col2+1 where col1 = 0;
update test_bypass_sq2 set col2 = col2+1 where col1 = 0;
explain select * from test_bypass_sq2  where col1 = 0 order by col1;
select * from test_bypass_sq2  where col1 = 0 order by col1;
explain select * from test_bypass_sq2  where col1 >= 0 order by col1;
select * from test_bypass_sq2  where col1 >= 0 order by col1;
explain select * from test_bypass_sq2  where col1 >= 0 order by col1 limit 4;
select * from test_bypass_sq2  where col1 >= 0 order by col1 limit 4;
explain select * from test_bypass_sq2  where col1 = 1 order by col1 for update limit 1;
select * from test_bypass_sq2  where col1 = 1 order by col1 for update limit 1;
explain select col1 from test_bypass_sq2 order by col1 limit 2;
select col1 from test_bypass_sq2 order by col1 limit 2;
explain select * from test_bypass_sq2  where col1 > 0 order by col1 limit 2 offset 2;
select * from test_bypass_sq2  where col1 > 0 order by col1 limit 2 offset 2;
explain select * from test_bypass_sq2  where col1 > 0 order by col1 for update limit 2 offset 2;
reset enable_indexonlyscan;
--not bypass
explain select * from test_bypass_sq2  where col2 is null;
explain select * from test_bypass_sq2  where col1 = 0 and col2 = 0;
explain select t1.col3, t2.col2  from test_bypass_sq1 as t1 join test_bypass_sq2 as t2 on t1.col1=t2.col1;
explain select count(*),col1 from test_bypass_sq1 group by col1;
--bypass (order by is supported when ordered col is  in index)
select col1 from test_bypass_sq2 order by col1 desc;
select col1 from test_bypass_sq2 order by col1;
--not bypass
explain select * from test_bypass_sq2 order by col1,col2;
--
drop table if exists test_bypass_sq3;
create table test_bypass_sq3(col1 int default 1, col2 int, col3 timestamp);
create index itest_bypass_sq3 on test_bypass_sq3(col1);
--bypass
insert into test_bypass_sq3(col2,col3) values (3,null);
--bypass (default is null)
insert into test_bypass_sq3(col2,col3) values(1,default);
insert into test_bypass_sq3 values(2,3,null);
insert into test_bypass_sq3 values (3,3,null);
--not bypass
explain insert into test_bypass_sq3 values(3,3,current_timestamp);
explain select * from test_bypass_sq3 where col1 = 1 order by col2;
--bypass
select * from test_bypass_sq3 where col1 = 1 limit 1;
select col2 from test_bypass_sq3 where col1 = 1 for update;
update test_bypass_sq3 set col2 = col2*3 where col1 = 1;
--bypass (col1 is default 1)
insert into test_bypass_sq3 values(default,default,default);

--test random index pos
drop table if exists test_bypass_sq4;
create table test_bypass_sq4(col1 int, col2 int, col3 int);
create index itest_bypass_sq4 on test_bypass_sq4(col3,col2);
insert into test_bypass_sq4 values (11,21,31);
insert into test_bypass_sq4 values (11,22,32);
insert into test_bypass_sq4 values (12,23,32);
insert into test_bypass_sq4 values (12,23,33);
insert into test_bypass_sq4 values (13,24,33);
insert into test_bypass_sq4 values (13,24,34);
insert into test_bypass_sq4 values (14,25,34);
insert into test_bypass_sq4 values (14,25,35);
insert into test_bypass_sq4 values (55,55,55);
insert into test_bypass_sq4 values (55,55,null);
insert into test_bypass_sq4 values (55,null,55);
insert into test_bypass_sq4 values (55,null,null);
insert into test_bypass_sq4 values (null,null,null);
explain select col3, col1, col2 from test_bypass_sq4 where col2 >22 order by 1,3;
select col3, col1, col2 from test_bypass_sq4 where col2 >22 order by 1,3;
explain select * from test_bypass_sq4 where col2 =22 and col3= 32 order by col2;
select * from test_bypass_sq4 where col2 =22 and col3= 32 order by col2;
explain select col3,col2,col3 from test_bypass_sq4 where col3 >= 33 and col2 >= 22 order by col3,col2;
select col3,col2,col3 from test_bypass_sq4 where col3 >= 33 and col2 >= 22 order by col3,col2;
select col2,col3,col2 from test_bypass_sq4 where col3 >= 34 and col2 >= 22 order by col3,col2;
explain select col3,col2,col3 from test_bypass_sq4 where col3 >= 33 and col2 >= 22 order by col3 for update;
explain select col2,col3,col2 from test_bypass_sq4 where col3 >= 34 and col2 >= 22 order by col3,col2 for update;
select col2,col3,col2 from test_bypass_sq4 where col3 is null and col2 is null order by col3,col2;
explain select col2,col3 from test_bypass_sq4 where col3 is null and col2 is not null;
select col2,col3 from test_bypass_sq4 where col3 is null and col2 is not null;
explain select col2,col3 from test_bypass_sq4 where col3 is not null order by col3 desc,col2 desc;
select col2,col3 from test_bypass_sq4 where col3 is not null order by col3 desc,col2 desc;

drop table if exists test_bypass_sq5;
create table test_bypass_sq5(col1 int, col2 int, col3 int default 1);
create index itest_bypass_sq5 on test_bypass_sq5(col2);
insert into test_bypass_sq5 values (1,2,3);
insert into test_bypass_sq5 values (2,3,4);
-- permission
DROP ROLE IF EXISTS bypassuser;
CREATE USER bypassuser PASSWORD 'ttest@123';
GRANT select ON test_bypass_sq5 TO bypassuser;
GRANT update ON test_bypass_sq5 TO bypassuser;
SET SESSION AUTHORIZATION bypassuser PASSWORD 'ttest@123';
SELECT SESSION_USER, CURRENT_USER;
select * from test_bypass_sq5 order by col2;
select * from test_bypass_sq5 where col2>2 for update;
insert into test_bypass_sq5 values (2,3,4); --fail
update test_bypass_sq5 set col3 = col3+1 where col2 > 0;
delete from test_bypass_sq5; --fail
RESET SESSION AUTHORIZATION;
revoke update on test_bypass_sq5 from bypassuser;
revoke select on test_bypass_sq5 from bypassuser;
DROP OWNED BY bypassuser;
DROP ROLE bypassuser;
-- bypass transaction
start transaction;
    insert into test_bypass_sq5 values (3,4,5);
    select * from test_bypass_sq5 order by col2;
    savepoint s1;
    update test_bypass_sq5 set col3 = col3+1 where col2 > 0;
    select * from test_bypass_sq5 order by col2;
    rollback to savepoint s1;
    select * from test_bypass_sq5 order by col2;
    savepoint s2;
    delete from test_bypass_sq5 where col2 < 3;
    select * from test_bypass_sq5 order by col2;
    rollback to savepoint s2;
    select * from test_bypass_sq5 order by col2;
rollback;
start transaction read only;
    insert into test_bypass_sq5 values (3,4,5);
rollback;
start transaction read only;
    update test_bypass_sq5 set col3 = col3+1 where col2 > 0;
rollback;
start transaction read only;
    delete from test_bypass_sq5 where col2 < 3;
rollback;
select * from test_bypass_sq5 order by col2;
-- maybe wrong savepoint and cursor
drop table if exists test_bypass_sq5;
create table test_bypass_sq5(col1 int, col2 int, col3 int default 1);
create index itest_bypass_sq5 on test_bypass_sq5(col2);
insert into test_bypass_sq5 values (1,2,3);
insert into test_bypass_sq5 values (2,3,4);
start transaction;
    insert into test_bypass_sq5 values (3,4,5);
    select * from test_bypass_sq5 order by col2;
    cursor tt for select * from test_bypass_sq5 order by col2;
    update test_bypass_sq5 set col3 = col3+1 where col2 > 0;
    select * from test_bypass_sq5 order by col2;
    fetch 2 from tt;
    savepoint s3;
    update test_bypass_sq5 set col3 = col3+1 where col2 > 0;
    select * from test_bypass_sq5 order by col2;
    rollback to savepoint s3;
    select * from test_bypass_sq5 order by col2;
    fetch 2 from tt;
commit;
select * from test_bypass_sq5 order by col2;

-- test complex type
drop table if exists test_bypass_sq6;
create type complextype AS (f1 int, f2 text);
create table test_bypass_sq6(col1 int, col2 complextype,col3 text);
create index itest_bypass_sq6 on test_bypass_sq6(col1,col3);
--not bypass
explain insert into test_bypass_sq6 values (1,ROW(1, 'Simon1'::text),'test'::text);
-- just insert
reset opfusion_debug_mode;
insert into test_bypass_sq6 values (1,ROW(1, 'Simon1'::text),'test'::text);
set opfusion_debug_mode = 'error';
--bypass
select * from test_bypass_sq6 where col1 is not null;
select * from test_bypass_sq6 where col3 ='test'::text for update;
update test_bypass_sq6 set col3='test_2'::text where col1 = 1;
select * from test_bypass_sq6 where col3 is not null;
select col1 from test_bypass_sq6;
select col3 from test_bypass_sq6 order by col1,col3;
select col1, col3 from test_bypass_sq6 where true;
--notbypass
explain update test_bypass_sq6 set col2=ROW(2,'Ruby2'::text) where col1 = 1;
--bypass
delete from test_bypass_sq6 where col1 = 1;
--test QPS
set opfusion_debug_mode='error';
drop table if exists test_bypass_sq7;
create table test_bypass_sq7(col1 int, col2 int, col3 int);
create index test_bypass_sq7_index on test_bypass_sq7(col1,col2);
insert into test_bypass_sq7 values (11,21,31);
insert into test_bypass_sq7 values (11,22,32);
insert into test_bypass_sq7 values (12,23,32);
SET track_activities=on;
SET track_sql_count=on;
DROP USER IF EXISTS qps CASCADE;
CREATE USER qps PASSWORD 'TTEST@123';
GRANT INSERT on TABLE test_bypass_sq7 to qps;
GRANT SELECT on TABLE test_bypass_sq7 to qps;
GRANT UPDATE on TABLE test_bypass_sq7 to qps;
GRANT DELETE on TABLE test_bypass_sq7 to qps;
SET SESSION SESSION AUTHORIZATION qps PASSWORD 'TTEST@123';
SELECT node_name,select_count, update_count, insert_count, delete_count,  ddl_count, dml_count FROM gs_sql_count where user_name='qps';
SELECT * from test_bypass_sq7 where col1 >10 and col2 >10 order by col1,col2;
update test_bypass_sq7 set col2 =1 where col1 =11;
insert into test_bypass_sq7 values (1,2,3);
delete test_bypass_sq7 where col1 =1;
SELECT node_name,select_count, update_count, insert_count, delete_count,  ddl_count, dml_count FROM gs_sql_count where user_name='qps';
CREATE OR REPLACE FUNCTION tri_bypass() RETURNS trigger AS $$
BEGIN
    NEW.col2 = NEW.col2 + 2;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
set opfusion_debug_mode = 'log';
drop table if exists test_bypass_sq8;
create table test_bypass_sq8(col1 int, col2 int, col3 text);
create index itest_bypass_sq8 on test_bypass_sq8(col1,col2);
create trigger tri_bypass after insert on test_bypass_sq8 for each row execute procedure tri_bypass();
insert into test_bypass_sq8 values(1,1,'test');
explain select * from test_bypass_sq8 where col1 = 1;
explain update test_bypass_sq8 set col2 = 2 where col1 = 1;
explain delete test_bypass_sq8 where col1 = 1;
explain insert into test_bypass_sq8 values(2,2,'testinsert');
set opfusion_debug_mode=off;
RESET SESSION AUTHORIZATION;
SELECT node_name,select_count, update_count, insert_count, delete_count,  ddl_count, dml_count FROM pgxc_sql_count where user_name='qps' order by node_name;
revoke insert on test_bypass_sq7 from qps;
revoke delete on test_bypass_sq7 from qps;
revoke update on test_bypass_sq7 from qps;
revoke select on test_bypass_sq7 from qps;
DROP OWNED BY qps;
DROP ROLE qps;

-- test rule do nothing
create table test(a int);
create view v_test as select * from test;

CREATE OR REPLACE RULE v_delete as ON DELETE TO v_test DO INSTEAD NOTHING;
delete from v_test;

set explain_perf_mode=pretty;
explain delete from v_test;
reset explain_perf_mode;

drop table test cascade;

--test enable_opfusion_reuse
set enable_opfusion_reuse=on;
create table test_reuse_t1(a int,b int,c text);
create table test_reuse_t2(a int,b int,c text,d text);

begin;
insert into test_reuse_t1 values(1,1,'2');
insert into test_reuse_t1 values(2,3,'2');
insert into test_reuse_t2 values(1,2,'3','4');
insert into test_reuse_t1 values(1,3,'4');
select * from test_reuse_t1;
commit;

truncate test_reuse_t1;
truncate test_reuse_t2;

begin;
insert into test_reuse_t1 values(1,1,'2');
alter table test_reuse_t1 add column colf int;
insert into test_reuse_t1 values(1,1,'2');
select * from test_reuse_t1;
commit;

drop table test_reuse_t1;
drop table test_reuse_t2;
reset enable_opfusion_reuse;

-- end
reset track_activities;
set track_sql_count = off;
reset enable_seqscan;
reset enable_bitmapscan;
reset opfusion_debug_mode;
reset enable_opfusion;
reset enable_indexscan;
reset enable_indexonlyscan;
reset log_min_messages;
reset logging_module;

drop table test_bypass_sq1;
drop table test_bypass_sq2;
drop table test_bypass_sq3;
drop table test_bypass_sq4;
drop table test_bypass_sq5;
drop table test_bypass_sq6;
drop table test_bypass_sq7;
drop table test_bypass_sq8;
drop function tri_bypass;
drop type complextype;
