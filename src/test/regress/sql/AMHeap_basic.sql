create table Heap_TAB_TEST1(id int, val varchar(200)) with (hashbucket = off); 
insert into Heap_TAB_TEST1 values(11, 'Heap_TAB_TEST1'); 
insert into Heap_TAB_TEST1 values(12, 'hello hash table'); 
insert into Heap_TAB_TEST1 values(1, 'hello world'), (2, 'hello c/c++'), (3,'abc');
insert into Heap_TAB_TEST1 select v, 'CHAR-'||v from generate_series(1, 10) as v;

select * from Heap_TAB_TEST1 order by id;

select count(*) from Heap_TAB_TEST1;

select * from Heap_TAB_TEST1 where val like 'hello%' order by id, val;
select * from Heap_TAB_TEST1 where val = 'CHAR-5' order by id, val;

select * from Heap_TAB_TEST1 where id = 1 order by val;
select * from Heap_TAB_TEST1 where id = 2 order by val;

insert into Heap_TAB_TEST1 select id + 100, val from Heap_TAB_TEST1;

select count(*) from Heap_TAB_TEST1;

select * from Heap_TAB_TEST1 where val like 'hello%' order by id, val;
select * from Heap_TAB_TEST1 where val = 'CHAR-5' order by id, val;

select * from Heap_TAB_TEST1 where id = 1 order by val;
select * from Heap_TAB_TEST1 where id = 2 order by val;

drop table Heap_TAB_TEST1;
