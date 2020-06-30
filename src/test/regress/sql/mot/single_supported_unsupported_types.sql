-- supported types
create foreign table test0 (b boolean);
create foreign table test1 (c char, v varchar(10), t varchar(1000));
create foreign table test2 (i integer, n numeric, f4 float4, f8 float8, s serial);
create foreign table test3 (d date, t time, ts timestamp, i interval);

-- unsupported types 
create foreign table test4 (u uuid);
create foreign table test5 (arr int[]);
create foreign table test6 (j json);
create foreign table test7 (h hstore);
create foreign table test8 (b box);
create foreign table test9 (l line);
create foreign table test10 (p point);
create foreign table test11 (l lseg);
create foreign table test12 (p polygon);
create foreign table test13 (i inet);
create foreign table test14 (m macaddr);

drop foreign table test0;
drop foreign table test1;
drop foreign table test2;
drop foreign table test3;
