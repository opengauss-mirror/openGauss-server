set cstore_insert_mode=main;

-- precision 18
drop table if exists t1, t2, t3, t4, t5;

create table t1(c1 dec(18,1), c2 dec(18,5), c3 dec(18,9),  c4 dec(18,13)) tablespace hdfs_ts;
insert into t1 values( 99999999999999999.9,  9999999999999.99999,  999999999.999999999,  99999.9999999999999);
insert into t1 values( 90000000000000000  ,  9000000000000      ,  900000000          ,  90000              );
insert into t1 values(-99999999999999999.9, -9999999999999.99999, -999999999.999999999, -99999.9999999999999);
insert into t1 values(-90000000000000000  , -9000000000000      , -900000000          , -90000              );
select * from t1 order by 1;

create table t2(c1 dec(18,2), c2 dec(18,6), c3 dec(18,10), c4 dec(18,14)) tablespace hdfs_ts;
insert into t2 values( 9999999999999999.99,  999999999999.999999,  99999999.9999999999,  9999.99999999999999);
insert into t2 values( 9000000000000000   ,  900000000000       ,  90000000           ,  9000               );
insert into t2 values(-9999999999999999.99, -999999999999.999999, -99999999.9999999999, -9999.99999999999999);
insert into t2 values(-9000000000000000   , -900000000000       , -90000000           , -9000               );
select * from t2 order by 1;

create table t3(c1 dec(18,3), c2 dec(18,7), c3 dec(18,11), c4 dec(18,15)) tablespace hdfs_ts;
insert into t3 values( 999999999999999.999,  99999999999.9999999,  9999999.99999999999,  999.999999999999999);
insert into t3 values( 900000000000000    ,  90000000000        ,  9000000            ,  900                );
insert into t3 values(-999999999999999.999, -99999999999.9999999, -9999999.99999999999, -999.999999999999999);
insert into t3 values(-900000000000000    , -90000000000        , -9000000            , -900                );
select * from t3 order by 1;

create table t4(c1 dec(18,4), c2 dec(18,8), c3 dec(18,12), c4 dec(18,16)) tablespace hdfs_ts;
insert into t4 values( 99999999999999.9999,  9999999999.99999999,  999999.999999999999,  99.9999999999999999);
insert into t4 values( 90000000000000     ,  9000000000         ,  900000             ,  90                 );
insert into t4 values(-99999999999999.9999, -9999999999.99999999, -999999.999999999999, -99.9999999999999999);
insert into t4 values(-90000000000000     , -9000000000         , -900000             , -90                 );
select * from t4 order by 1;

create table t5(c1 dec(18,17), c2 dec(18,18)) tablespace hdfs_ts;
insert into t5 values( 9.99999999999999999,  .999999999999999999);
insert into t5 values( 1.00000000000000001,  .000000000000000001);
insert into t5 values(-9.99999999999999999, -.999999999999999999);
insert into t5 values(-1.00000000000000001, -.000000000000000001);
select * from t5 order by 1;

-- precision 38
drop table if exists t1, t2, t3, t4, t5;

create table t1(c1 dec(38,1), c2 dec(38,2), c3 dec(38,3),  c4 dec(38,4)) tablespace hdfs_ts;
insert into t1 values( 9999999999999999999999999999999999999.9,  999999999999999999999999999999999999.99,  99999999999999999999999999999999999.999,  9999999999999999999999999999999999.9999);
insert into t1 values(                                      .1,                                      .01,                                     .001,                                    .0001);
insert into t1 values( 9000000000000000000000000000000000000  ,  900000000000000000000000000000000000   ,  90000000000000000000000000000000000    ,  9000000000000000000000000000000000     );
insert into t1 values(-9999999999999999999999999999999999999.9, -999999999999999999999999999999999999.99, -99999999999999999999999999999999999.999, -9999999999999999999999999999999999.9999);
insert into t1 values(-                                     .1, -                                    .01, -                                   .001, -                                  .0001);
insert into t1 values(-9000000000000000000000000000000000000  , -900000000000000000000000000000000000   , -90000000000000000000000000000000000    , -9000000000000000000000000000000000     );
select * from t1 order by 1;

create table t2(c1 dec(38,33), c2 dec(38,34), c3 dec(38,35), c4 dec(38,36)) tablespace hdfs_ts;
insert into t2 values( 99999.999999999999999999999999999999999,  9999.9999999999999999999999999999999999,  999.99999999999999999999999999999999999,  99.999999999999999999999999999999999999);
insert into t2 values(      .000000000000000000000000000000001,      .0000000000000000000000000000000001,     .00000000000000000000000000000000001,    .000000000000000000000000000000000001);
insert into t2 values( 90000                                  ,  9000                                   ,  900                                    ,  90                 );
insert into t2 values(-99999.999999999999999999999999999999999, -9999.9999999999999999999999999999999999, -999.99999999999999999999999999999999999, -99.999999999999999999999999999999999999);
insert into t2 values(-     .000000000000000000000000000000001, -    .0000000000000000000000000000000001, -   .00000000000000000000000000000000001, -  .000000000000000000000000000000000001);
insert into t2 values(-90000                                  , -9000                                   , -900                                    , -90                 );
select * from t2 order by 1;

create table t3(c1 dec(38,37), c2 dec(38,38)) tablespace hdfs_ts;
insert into t3 values( 9.9999999999999999999999999999999999999,  .99999999999999999999999999999999999999);
insert into t3 values(  .0000000000000000000000000000000000001,  .00000000000000000000000000000000000001);
insert into t3 values( 9                                      ,  .9                                     );
insert into t3 values(-9.9999999999999999999999999999999999999, -.99999999999999999999999999999999999999);
insert into t3 values(- .0000000000000000000000000000000000001, -.00000000000000000000000000000000000001);
insert into t3 values(-9                                      , -.9                                     );
select * from t3 order by 1;


create table t4(c1 dec(38,19), c2 dec(38,20), c3 dec(38,21), c4 dec(38,22)) tablespace hdfs_ts;
insert into t4 values( 9999999999999999999.9999999999999999999,  999999999999999999.99999999999999999999,  99999999999999999.999999999999999999999, 9999999999999999.9999999999999999999999);
insert into t4 values( 1000000000000000000.1                  ,  100000000000000000.1                   ,  10000000000000000.1                    , 1000000000000000.1                     );
insert into t4 values( 1000000000000000000.0001               ,  100000000000000000.0001                ,  10000000000000000.0001                 , 1000000000000000.0001                  );
insert into t4 values( 1000000000000000000.00001              ,  100000000000000000.00001               ,  10000000000000000.00001                , 1000000000000000.00001                 );
insert into t4 values( 1000000000000000000.00000001           ,  100000000000000000.00000001            ,  10000000000000000.00000001             , 1000000000000000.00000001              );
insert into t4 values( 1000000000000000000.000000001          ,  100000000000000000.000000001           ,  10000000000000000.000000001            , 1000000000000000.000000001             );
insert into t4 values( 1000000000000000000.000000000001       ,  100000000000000000.000000000001        ,  10000000000000000.000000000001         , 1000000000000000.000000000001          );
insert into t4 values( 1000000000000000000                    ,  100000000000000000                     ,  10000000000000000                      , 1000000000000000                       );
insert into t4 values(           100000000.1                  ,           100000000.1                   ,          100000000.1                    ,        100000000.1                     );
insert into t4 values(            10000000.1                  ,            10000000.1                   ,           10000000.1                    ,         10000000.1                     );
insert into t4 values(               10000.1                  ,               10000.1                   ,              10000.1                    ,            10000.1                     );
insert into t4 values(                1000.1                  ,                1000.1                   ,               1000.1                    ,             1000.1                     );
insert into t4 values(                   1                    ,                   1                     ,                  1                      ,                1                       );
insert into t4 values(                    .1                  ,                    .1                   ,                   .1                    ,                 .1                     );
insert into t4 values(                    .0000000000000000001,                    .00000000000000000001,                   .000000000000000000001,                 .0000000000000000000001);
select * from t4 order by 1;

drop table if exists t1, t2, t3, t4;

-- NaN test
create table rowt(c1 int, a int, c2 dec(10,3), c3 dec(30,3), c4 dec(40,3));
insert into rowt values (1, 2, 1.12, 1.12, 1.12);
insert into rowt values (1, 3, NULL, 1.12, 1.12);
insert into rowt values (1, 4, 1.12, NULL, 1.12);
insert into rowt values (1, 5, 1.12, 1.12, NULL);
insert into rowt values (1, 7, NULL, NULL, NULL);
insert into rowt values (1, 8, 1.12, 1.12, 1.12);

create table t1(c1 int, a int, c2 dec(10,3), c3 dec(30,3), c4 dec(40,3)) tablespace hdfs_ts;
insert into t1 select * from rowt;

select * from t1 order by 2;

drop table if exists t1, rowt;

-- predicate pushdown
create table rowt(c1 int, c2 dec(10,3), c3 dec(30,3), c4 dec(40,3)) tablespace hdfs_ts;
insert into rowt values (1, 1.1, 1.1, 1.1);
insert into rowt values (1, 2.2, 2.2, 2.2);
insert into rowt values (1, 3.3, 3.3, 3.3);
insert into rowt values (1, 4.4, 4.4, 4.4);

create table t1(c1 int, c2 dec(10,3), c3 dec(30,3), c4 dec(40,3)) tablespace hdfs_ts;
insert into t1 select * from rowt;
select * from t1 order by c2;

select * from t1 where c2=2.2;
select * from t1 where c3=3.3;
select * from t1 where c4=4.4;

drop table if exists t1, rowt;

create table t1(c1 int, c2 dec(38,3)) tablespace hdfs_ts;
insert into t1 values (1, 100), (1, -100), (1, 295147905179352825856), (1, -295147905179352825856), (1, 123456789012345678901234567890), (1, -123456789012345678901234567890);
select * from t1 where c2=100;
select * from t1 where c2=-100;
select * from t1 where c2=295147905179352825856;
select * from t1 where c2=-295147905179352825856;
select * from t1 where c2=123456789012345678901234567890;
select * from t1 where c2=-123456789012345678901234567890;

drop table if exists t1;
