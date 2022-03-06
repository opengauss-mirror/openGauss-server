create database utf8test template template0 encoding 'utf8';
\c utf8test
create table rawlike_t1(c1 raw);
insert into rawlike_t1 values(hextoraw('D'));
select * from rawlike_t1 where c1 like hextoraw('D');
insert into rawlike_t1 values(hextoraw('D9'));
select * from rawlike_t1 where c1 like hextoraw('D9');
insert into rawlike_t1 values(hextoraw('D9a'));
select * from rawlike_t1 where c1 like hextoraw('D9a');
select * from rawlike_t1 where c1 like hextoraw('D9f');
drop table rawlike_t1;
\c postgres
drop database utf8test;