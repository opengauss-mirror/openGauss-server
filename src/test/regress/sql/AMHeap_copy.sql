create table Heap_TAB_COPY1(id int, name varchar(200), price double precision) 
	with (hashbucket = off);
	
insert into Heap_TAB_COPY1 select 0, 'NAME-'||0, 0 from generate_series(1, 5) as v;	

copy Heap_TAB_COPY1 to stdout;

copy Heap_TAB_COPY1(name) to stdout;

delete from Heap_TAB_COPY1;

insert into Heap_TAB_COPY1 select v, 'NAME-'||v, exp(v / 111)*1000 from generate_series(1, 10) as v;

copy (select * from Heap_TAB_COPY1 where id = 1) to stdout;

drop table Heap_TAB_COPY1;
