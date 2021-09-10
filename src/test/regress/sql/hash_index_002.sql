-------------------------------------
---------- hash index part2----------
-------------------------------------

set enable_seqscan = off;
set enable_indexscan = off;

-- continue to hash_index_001
explain (costs off) select * from hash_table_7 where id = 80;
drop table hash_table_7 cascade;

-- low maintenance_work_mem
set maintenance_work_mem = '1MB';

drop table if exists hash_table_8;
create table hash_table_8(id int, name varchar, sex varchar default 'male');
insert into hash_table_8 select random()*100, 'XXX', 'XXX' from generate_series(1,50000);
create index hash_t8_id1 on hash_table_8 using hash(id) with (fillfactor = 30);
explain (costs off) select * from hash_table_8 where id = 80;
drop table hash_table_8 cascade;

-- vacuum one page
set enable_indexscan = on;
set enable_bitmapscan = off;
set maintenance_work_mem = '100MB';
alter system set autovacuum = off;

drop table if exists hash_table_9;
create table hash_table_9(id int, name varchar, sex varchar default 'male');
insert into hash_table_9 select random()*100, 'XXX', 'XXX' from generate_series(1,50000);
create index hash_t9_id1 on hash_table_9 using hash(id) with (fillfactor = 10);
create or replace procedure hash_proc_9(sid in integer)
is
begin
delete from hash_table_9 where id = sid;
perform * from hash_table_9 where id = sid;
insert into hash_table_9 select sid, random() * 10, 'xxx' from generate_series(1,5000);
end;
/
call hash_proc_9(1);
call hash_proc_9(1);
call hash_proc_9(1);
call hash_proc_9(1);

drop table hash_table_9 cascade;
drop procedure hash_proc_9;

-- some dml operator
drop table if exists hash_table_10;
create table hash_table_10(id int, num int, sex varchar default 'male');
create index hash_t10_id1 on hash_table_10 using hash (id);
insert into hash_table_10 select random()*10, random()*10, 'XXX' from generate_series(1,5000);
insert into hash_table_10 select random()*10, random()*10, 'XXX' from generate_series(1,5000);
delete from hash_table_10 where id = 7 and num = 1;
insert into hash_table_10 select 7, random()*3, 'XXX' from generate_series(1,500);
delete from hash_table_10 where id = 5;
vacuum hash_table_10;
insert into hash_table_10 select random()*50, random()*3, 'XXX' from generate_series(1,50000);
delete from hash_table_10 where num = 2;
vacuum hash_table_10;
drop table hash_table_10 cascade;

--reset all parameters
reset enable_indexscan;
reset enable_bitmapscan;
reset enable_seqscan;
reset maintenance_work_mem;
alter system set autovacuum = on;