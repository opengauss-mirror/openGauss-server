----delete from table order by xxx limit xxx 
drop table if exists sytc_t1;
drop table if exists sytc_t2;
drop table if exists sytc_t3;
create table sytc_t1 (f1 int, f2 int ,f3 int);
create index sytc_t1_idx on sytc_t1(f1);
create table sytc_t2 (f1 int, f2 int ,f3 int);
create index sytc_t2_idx on sytc_t2(f1);
create table sytc_t3 (f1 int, f2 int ,f3 int);

insert into sytc_t1 select generate_series(1,5000), generate_series(1,5000), generate_series(1,5000);
insert into sytc_t2 select generate_series(1,5000), generate_series(1,5000), (random()*(6^2))::integer;
insert into sytc_t3 select generate_series(100,5100), (random()*10000)::integer, (random()*(6^2))::integer;

explain delete from sytc_t1 order by f1 limit 1;
explain delete from sytc_t1 order by f1 desc limit 1;
explain delete from sytc_t1 order by f2 desc limit 1;

explain select * from sytc_t1 where f1 in (select f1 from sytc_t2) order by f2 limit 10;
begin;
delete from sytc_t1 where f1 in (select f1 from sytc_t2) order by f2 desc limit 10;
select * from sytc_t1 where f1 in (select f1 from sytc_t2) order by f2 desc limit 10;
rollback;

explain (costs off) delete from sytc_t1 where f1 in (select f1 from sytc_t2) order by f1 limit 10;

----delete mul-table report error
explain delete from sytc_t1, sytc_t2 where sytc_t1.f1=sytc_t2.f1 order by f1 limit 1;

----delete mul-table join
explain delete from sytc_t1 using sytc_t2 where sytc_t1.f1=sytc_t2.f1 order by sytc_t1.f1 limit 1;
explain delete from sytc_t1 using sytc_t2 where sytc_t1.f1=sytc_t2.f1 order by sytc_t1.f2 limit 1;

----for with CTE
begin;
explain WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc
) select sytc_t1.f1 FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f1 desc limit 1;

WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc
) select sytc_t1.f1 FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f1 desc limit 1;

WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc
) delete FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f1 desc limit 1;

WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc
) select sytc_t1.f1 FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f1 desc limit 1;
rollback;

begin;
explain WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc limit 20
) DELETE FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f2 desc limit 5;

WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc limit 20
) select * FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f2 desc limit 5;

WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc limit 20
) DELETE FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f2 desc limit 5;

WITH max_table as (
    SELECT f1, max(f2) mx FROM sytc_t2 GROUP BY f1 order by f1 desc limit 20
) select * FROM sytc_t1 WHERE f1 = (SELECT mx FROM max_table where max_table.f1 = sytc_t1.f1) order by f2 desc limit 5;
rollback;

----update table set col=xxx order by xxx limit xxx 
explain update sytc_t1 set f2 = 1000 order by f2 desc;
explain update sytc_t1 set f2 = 1000 where f1 > 4900 order by f1 limit 10;

begin;
explain update sytc_t1 set f2 = 123 where f1 > 4900 order by f2 limit 10;
select f1,f2 from sytc_t1 where f2 = 123;
update sytc_t1 set f2 = 123 where f1 >4900 order by f2 limit 10;
select f1,f2 from sytc_t1 where f2 = 123;
rollback;

begin;
explain update sytc_t1 set f2 = 123 where f1 > 4900 order by f2 desc limit 10;
select f1,f2 from sytc_t1 where f2 = 123 order by f2 desc;
update sytc_t1 set f2 = 123 where f1 >4900 order by f2 desc limit 10;
select f1,f2 from sytc_t1 where f2 = 123 order by f2 desc;
rollback;

begin;
explain update sytc_t1 set f3 = 123 where f1 > 4900 order by f2 desc limit 10;
select f1,f2,f3 from sytc_t1 where f3 = 123 order by f2 desc;
update sytc_t1 set f3 = 123 where f1 >4900 order by f2 desc limit 10;
select f1,f2,f3 from sytc_t1 where f3 = 123 order by f2 desc;
rollback;

begin;
explain update sytc_t1 set f3 = 123 where f1 > 4900 order by f3 desc limit 10;
select f1,f2,f3 from sytc_t1 where f3 = 123 order by f3 desc;
update sytc_t1 set f3 = 123 where f1 >4900 order by f3 desc limit 10;
select f1,f2,f3 from sytc_t1 where f3 = 123 order by f3 desc;
rollback;

begin;
explain update sytc_t1 set f3 = 123 where f1 > 4900 order by f2 desc,f3 desc limit 10;
select f1,f2,f3 from sytc_t1 where f3 = 123 order by f2 desc,f3 desc;
update sytc_t1 set f3 = 123 where f1 >4900 order by f2 desc,f3 desc limit 10;
select f1,f2,f3 from sytc_t1 where f3 = 123 order by f2 desc,f3 desc;
rollback;

begin;
explain update sytc_t1 set f3 = f1+10 where f1 > 4900 order by f2 desc,f3 desc limit 10;
select f1,f2,f3 from sytc_t1 where f1 >4900 order by f2 desc,f3 desc limit 10;
update sytc_t1 set f3 = f1+10 where f1 >4900 order by f2 desc,f3 desc limit 10;
select f1,f2,f3 from sytc_t1 where f1 >4900 order by f2 desc,f3 desc limit 10;
rollback;

explain update sytc_t1 set f3 = 123 where f1 > 4900 order by nlssort(f2);
explain delete sytc_t1 where f1 > 4900 order by nlssort(f2);
update sytc_t1 set f3 = 123 where f1 > 4900 order by nlssort(f2);
delete sytc_t1 where f1 > 4900 order by nlssort(f2);

explain update sytc_t1 set f3 = 123 where f1 > 4900 order by abcdefg;
explain delete sytc_t1 where f1 > 4900 order by abcdefg;
update sytc_t1 set f3 = 123 where f1 > 4900 order by abcdefg;
delete sytc_t1 where f1 > 4900 order by abcdefg;

drop table if exists sytc_test_sql1;
create table sytc_test_sql1(id int,name varchar(20),queryd int);
drop index if exists sytc_test_index;
create index sytc_test_index on sytc_test_sql1(id);
insert into sytc_test_sql1 values(generate_series(1,5),'name',generate_series(1,5));
select * from sytc_test_sql1 where id<6 order by NLSSORT(id, 'NLS_SORT = generic_m_ci') limit 1;
explain update sytc_test_sql1 set name ='fd' where id<6 order by NLSSORT(id,'NLS_SORT = generic_m_ci') limit 1;
update sytc_test_sql1 set name ='fd' where id<6 order by NLSSORT(id,'NLS_SORT = generic_m_ci') limit 1;
select * from sytc_test_sql1 where id<6 order by NLSSORT(id, 'NLS_SORT = generic_m_ci');

explain delete sytc_test_sql1 where id<6 order by NLSSORT(id,'NLS_SORT = generic_m_ci') limit 1;
delete sytc_test_sql1 where id<6 order by NLSSORT(id,'NLS_SORT = generic_m_ci') limit 1;
select * from sytc_test_sql1 where id<6 order by NLSSORT(id, 'NLS_SORT = generic_m_ci');
