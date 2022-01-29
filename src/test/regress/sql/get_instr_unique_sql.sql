drop table if exists unique_sql_test1;
drop table if exists unique_sql_test2;
create table unique_sql_test1(a int, b int);
create table unique_sql_test2(a int, b int);
insert into unique_sql_test1 select GENERATE_SERIES(0, 15000),GENERATE_SERIES(0, 15000);
insert into unique_sql_test2 select GENERATE_SERIES(0, 15000),GENERATE_SERIES(0, 15000);
select reset_unique_sql('global','ALL',0);

--explain sql won't record unique sql info
explain performance select * from unique_sql_test1 where b in (select b from unique_sql_test2) and a = 66 order by b;
select * from unique_sql_test1 where b in (select b from unique_sql_test2) and a = 66 order by b;
select sort_count,hash_count from get_instr_unique_sql() where query like '%select * from unique_sql_test1 where b in (select b from unique_sql_test2%';

--test sort with sqlbypass
create index i_unique_sql_test1 on unique_sql_test1(a);
create index i_unique_sql_test2 on unique_sql_test2(a);
set enable_beta_opfusion = on;
set enable_bitmapscan = off;
select reset_unique_sql('global','ALL',0);

explain (costs off) select * from unique_sql_test1 where a = 66 order by b;
select * from unique_sql_test1 where a = 66 order by b;

select sort_count,hash_count from get_instr_unique_sql() where query like '%select * from unique_sql_test1 where%';

drop table if exists workmem_t1;
drop table if exists workmem_t2;
create table workmem_t1(a int not null, b varchar(100)) with (orientation = column);;
create table workmem_t2(a int not null, b varchar(100)) with (orientation = column);;
insert into workmem_t1 select GENERATE_SERIES(0, 1500),'test01-'||GENERATE_SERIES(0, 1500);
insert into workmem_t2 select GENERATE_SERIES(0, 1500)*2,'test02-'||GENERATE_SERIES(0, 1500)*2;
select reset_unique_sql('global','ALL',0);
--explain sql won't record unique sql info
explain performance select * from workmem_t1 where a in (select a from workmem_t2) order by b limit 10;
select * from workmem_t1 where a in (select a from workmem_t2) order by b limit 10;
select sort_count from get_instr_unique_sql() where query like '%select * from workmem_t1 where a in (select a from workmem_t2)%';