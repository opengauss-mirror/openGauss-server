create foreign table test1 (x integer primary key, y integer);
create foreign table test2 (x integer primary key, y integer);

insert into test1 values (generate_series(1, 100), generate_series(1, 100));
insert into test2 values (generate_series(1, 100), generate_series(1, 100));

--delete from test1 using test2  where test2.x = test1.x;
select * from test1 order by x,y asc limit 10;
select * from test2 order by x,y asc limit 10;

truncate test1;

select * from test1 order by x,y asc limit 10;
select * from test2 order by x,y asc limit 10;

insert into test1 values (generate_series(1, 100), generate_series(1, 100));

delete from test1 where 1;
select * from test1 order by x,y asc limit 10;

drop foreign table test1;
drop foreign table test2;
