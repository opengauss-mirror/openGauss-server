create database mysql_test dbcompatibility 'B';
\c mysql_test
create table tb666(c1 timestamp default current_timestamp on update current_timestamp, c2 int, c3 timestamp generated always as(c1+c2) stored);
insert into tb666 values(default, 1);
select * from tb666;
begin;
update tb666 set c2 = 2;
select * from tb666;
select pg_sleep(2);
commit;