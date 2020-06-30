create foreign table test1  (i integer, y int);
create table test2  (i integer, y int);

insert into test2 values (generate_series(1,1000));
insert into test1 values (generate_series(1,1000));

analyze test2;
analyze test1;

drop foreign table test1;
drop table test2;
