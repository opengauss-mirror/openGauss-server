create foreign table test1  (i integer not null, y int not null);
create index text_idx on test1(y,i);
insert into test1 values (generate_series(1,1000), generate_series(1,1000));
select * from test1 order by i,y asc limit 10;

reindex index text_idx;

drop foreign table test1;
