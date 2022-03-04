
--4.7  real
create table t_pruning_datatype_real(c1 real,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100.11),
 partition p2 values less than(200.22),
 partition p3 values less than(300.33),
 partition p4 values less than(500.55));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_real where c1>=-100.11 AND c1<50.0 OR c1>300.33 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_real where c1 = 100.11; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_real where c1 = 100.11::real; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_real where c1 IN (100.11,250.0, 300.33,700); 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_real where c1 IN (150,250,500,600); 

--4.8  double precision
create table t_pruning_datatype_double(c1 double precision,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100.11),
 partition p2 values less than(200.22),
 partition p3 values less than(300.33),
 partition p4 values less than(500.55));
 
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_double where c1>=-100.11 AND c1<50.0 OR c1>300.33 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_double where c1 IN (100.11,250.0, 300.33,700); 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_double where c1 IN (150,250,500,600); 

--4.9  smallserial
create table t_pruning_datatype_smallserial(c1 smallserial,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_smallserial where c1>=-100 AND c1<50 OR c1>300 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_smallserial where c1 IN (150,250,500,600); 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_smallserial where c1 IN (150,250,500,null); 

--4.10 serial
create table t_pruning_datatype_serial(c1 serial,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_serial where c1>=-100 AND c1<50 OR c1>300 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_serial where c1 IN (150,250,500,600); 

--4.11 bigserial
create table t_pruning_datatype_bigserial(c1 bigserial,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_bigserial where c1>=-100 AND c1<50 OR c1>300 AND c1<700; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_bigserial where c1 IN (150,250,500,600); 

--4.12 character varying(n), varchar(n)
create table t_pruning_datatype_varchar(c1 varchar,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than('fffff'),
 partition p2 values less than('mmmmm'),
 partition p3 values less than('qqqqqqqqqqqq'),
 partition p4 values less than(MAXVALUE));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_varchar where c1>='abcd' AND c1<'cdef' OR c1>'qqqqqqqqqqqq' AND c1<'yyyyy'; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_varchar where c1 IN ('abcd','dabc','hhcdddda','zzzz'); 

--4.13 character(n), char(n)
create table t_pruning_datatype_charn(c1 char(40),c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than('fffff'),
 partition p2 values less than('mmmmm'),
 partition p3 values less than('qqqqqqqqqqqq'),
 partition p4 values less than(MAXVALUE));
 
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_charn where c1>='abcd' AND c1<'cdef' OR c1>'qqqqqqqqqqqq' AND c1<'yyyyy'; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_charn where c1 IN ('abcd','dabc','hhcdddda','zzzz'); 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_charn where c1 IN ('abcd','dabc','hhcdddda',null); 

--4.14 character.char
create table t_pruning_datatype_char(c1 char,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than('f'),
 partition p2 values less than('m'),
 partition p3 values less than('q'),
 partition p4 values less than(MAXVALUE));
 
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_char where c1>='b' AND c1<'h' OR c1>'q' AND c1<'y'; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_char where c1 IN ('a','d','h','z'); 

--4.15 text
create table t_pruning_datatype_text(c1 text,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than('fffff'),
 partition p2 values less than('mmmmm'),
 partition p3 values less than('qqqqqqqqqqqq'),
 partition p4 values less than(MAXVALUE));
 
explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_text where c1>='abcd' AND c1<'cdef' OR c1>'qqqqqqqqqqqq' AND c1<'yyyyy'; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_text where c1 IN ('abcd','dabc','hhcdddda','zzzz'); 

--4.16 nvarchar2

--4.17 name
create table t_pruning_datatype_name(c1 name,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than('fffff'),
 partition p2 values less than('mmmmm'),
 partition p3 values less than('qqqqqqqqqqqqq'),
 partition p4 values less than(MAXVALUE));
 
 explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_name where c1>='abcd' AND c1<'cdef' OR c1>'qqqqqqqqqqqqq' AND c1<'yyyyy'; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_name where c1 IN ('abcd','dabc','hhcdddda','zzzz'); 

--4.18 timestamp [ (p) ] [ without time zone ]
create table t_pruning_datatype_timestamp(c1 timestamp,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(to_timestamp('2012-11-12','YYYY-MM-DD')),
 partition p2 values less than('2012-12-26'),
 partition p3 values less than(to_timestamp('2013-06-12','YYYY-MM-DD')),
 partition p4 values less than('2013-12-26'));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_timestamp where c1>=to_timestamp('2012-05-12','YYYY-MM-DD') AND c1<'2012-11-12' OR c1>'2013-06-12' AND c1<'2014-09-08'; 

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_timestamp where c1 IN ('2012-05-12','2012-12-23') OR c1>'2013-06-12'; 

--4.19 timestamp [ (p) ] with time zone

--4.20 date
create table t_pruning_datatype_date(c1 date,c2 int,c3 int,c4 text)
partition by range(c1)
(partition p1 values less than(to_date('2012-11-12','YYYY-MM-DD')),
 partition p2 values less than('2012-12-26'),
 partition p3 values less than(to_date('2013-06-12','YYYY-MM-DD')),
 partition p4 values less than('2013-12-26'));

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_date where c1>=to_date('2012-05-12','YYYY-MM-DD') AND c1<'2012-11-12' OR c1>'2013-06-12' AND c1<'2014-09-08';

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_date where c1 IN (to_date('2012-05-12','YYYY-MM-DD'),'2012-12-23') OR c1>'2013-06-12';

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_datatype_date where c1 = to_date('2012-05-12','YYYY-MM-DD')::text;

drop table t_pruning_datatype_real;
drop table t_pruning_datatype_double;
drop table t_pruning_datatype_smallserial;
drop table t_pruning_datatype_serial;
drop table t_pruning_datatype_bigserial;
drop table t_pruning_datatype_varchar;
drop table t_pruning_datatype_charn;
drop table t_pruning_datatype_char;
drop table t_pruning_datatype_text;
drop table t_pruning_datatype_name;
drop table t_pruning_datatype_timestamp;
drop table t_pruning_datatype_date;

--multi column partiton key
create table pruning_partition_table_000( C_INT INTEGER,C_NUMERIC numeric(10,5),C_VARCHAR_3 VARCHAR(1024),C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,C_CHAR_3 CHAR(102400),C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10), 
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_BIGINT BIGINT,
 C_SMALLINT SMALLINT,
 C_FLOAT FLOAT,
 C_DP double precision,
 C_DATE DATE, 
 C_TS_WITH TIMESTAMP WITH TIME ZONE)
partition by range (C_INT,C_NUMERIC,C_VARCHAR_3,C_TS_WITHOUT)
( 
     partition pruning_partition_000_1 values less than (10, 12.34, 'hello', '2000-07-09 19:50:01.234'),
     partition pruning_partition_000_2 values less than (50, 123.456, 'World', '2013-07-09 19:50:01.234'),
     partition pruning_partition_000_3 values less than (100, 12345.678, 'select', '2019-07-09 19:50:01.234')
);

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
SELECT * FROM pruning_partition_table_000 WHERE C_INT>=int4(10.5) and C_INT<=50;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
SELECT * FROM pruning_partition_table_000 WHERE  C_INT=int4(12.33);

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
SELECT * FROM pruning_partition_table_000 WHERE  C_INT<10;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
SELECT * FROM pruning_partition_table_000 WHERE  C_INT>10;

drop table pruning_partition_table_000;

create table t_pruning_TESTTABLE_1(c1 int,c2 text)
partition by range(c1)
( 
	partition p1 values less than(100),
	partition p2 values less than(200),
	partition p3 values less than(300)
);

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_1 where c1 IS NULL;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_1 where c1 IS NOT NULL;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_1 where c1=null;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_1 where c2=null;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_1 where c2 IS NULL;

drop table t_pruning_TESTTABLE_1;

create table t_pruning_TESTTABLE_2(c1 int,c2 text)
partition by range(c1)
( 
	partition p1 values less than(100),
	partition p2 values less than(200),
	partition p3 values less than(300),
	partition p4 values less than(MAXVALUE)
);

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where c1 IS NULL;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where c1 IS NOT NULL;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where c1=null;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where null=c1;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where c2=null;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where c2 IS NULL;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where c1 IS NULL and c1>150;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from t_pruning_TESTTABLE_2 where c1 IS NULL OR c1<150;

drop table t_pruning_TESTTABLE_2;



-- where condition has function
CREATE TABLE wb_swry(wybz varchar(46),
yycsdm varchar(14),
yycsmc varchar(70),
dzqh varchar(6),
dz varchar(100),
swryxm varchar(30),
zjlx varchar(5),
zjhm  varchar(18),
fzjgmc varchar(70),
gj varchar(30),
swkssj timestamp,
xwsj timestamp,
swzdh varchar(20))distribute by hash(zjhm) partition by range (swkssj)
(partition part1202 values less than ('2012-02-01 00:00:00'),
partition part1203 values less than ('2012-03-01 00:00:00'),
partition part1204 values less than ('2012-04-01 00:00:00'),
partition part1205 values less than ('2012-05-01 00:00:00'),
partition part1206 values less than ('2012-06-01 00:00:00'),
partition part1207 values less than ('2012-07-01 00:00:00'),
partition part1208 values less than ('2012-08-01 00:00:00'),
partition part1209 values less than ('2012-09-01 00:00:00'),
partition part1210 values less than ('2012-10-01 00:00:00'),
partition part1211 values less than ('2012-11-01 00:00:00'),
partition part1212 values less than ('2012-12-01 00:00:00'),
partition part1301 values less than ('2013-01-01 00:00:00'),
partition part1302 values less than ('2013-02-01 00:00:00'),
partition part1303 values less than ('2013-03-01 00:00:00'),
partition part1304 values less than ('2013-04-01 00:00:00'),
partition part1305 values less than ('2013-05-01 00:00:00'),
partition part1306 values less than ('2013-06-01 00:00:00'),
partition part1307 values less than ('2013-07-01 00:00:00'),
partition part1308 values less than ('2013-08-01 00:00:00'),
partition part1309 values less than ('2013-09-01 00:00:00'),
partition part1310 values less than ('2013-10-01 00:00:00'),
partition part1311 values less than ('2013-11-01 00:00:00'),
partition part1312 values less than ('2013-12-01 00:00:00'),
partition part1401 values less than ('2014-01-01 00:00:00'),
partition part1402 values less than ('2014-02-01 00:00:00'),
partition part1403 values less than ('2014-03-01 00:00:00'),
partition part1404 values less than ('2014-04-01 00:00:00'),
partition part1405 values less than ('2014-05-01 00:00:00'),
partition part1406 values less than ('2014-06-01 00:00:00'),
partition part1407 values less than ('2014-07-01 00:00:00'),
partition part1408 values less than ('2014-08-01 00:00:00'),
partition part1409 values less than ('2014-09-01 00:00:00'),
partition part1410 values less than ('2014-10-01 00:00:00'),
partition part1411 values less than ('2014-11-01 00:00:00'),
partition part1412 values less than ('2014-12-01 00:00:00'),
partition part1501 values less than ('2015-01-01 00:00:00'),
partition part1502 values less than ('2015-02-01 00:00:00'),
partition part1503 values less than ('2015-03-01 00:00:00'),
partition part1504 values less than ('2015-04-01 00:00:00'),
partition part1505 values less than ('2015-05-01 00:00:00'),
partition part1506 values less than ('2015-06-01 00:00:00'),
partition part1507 values less than ('2015-07-01 00:00:00'),
partition part1508 values less than ('2015-08-01 00:00:00'),
partition part1509 values less than ('2015-09-01 00:00:00'),
partition part1510 values less than ('2015-10-01 00:00:00'),
partition part1511 values less than ('2015-11-01 00:00:00'),
partition part1512 values less than ('2015-12-01 00:00:00'),
partition part1601 values less than ('2016-01-01 00:00:00')
);

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from wb_swry where swkssj >= to_date('2012-01-01 11:12:57', 'yyyy-mm-dd hh24:mi:ss') AND swkssj <= to_date('2012-01-31 11:13:02','yyyy-mm-dd hh24:mi:ss');

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from wb_swry where swkssj >= '2012-01-01 11:12:57' AND swkssj <= '2012-01-31 11:13:02';

drop table wb_swry;


-- where condition is false
create table test_where_condition_is_false (a int)
partition by range (a)
(
	partition p1 values less than (10),
	partition p2 values less than (20),
	partition p3 values less than (30)
);

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false where 1=1;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false where 1=4;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false where a<10;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false where a<10 and 1=1;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false where a<10 and 1=4;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false where a<10 or 1=1;

explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false)
select * from test_where_condition_is_false where a<10 or 1=4;

drop table test_where_condition_is_false;



--create table partition_pruning_f2(f1 int)compress partition by range(f1) interval (1) 
--(
--	partition p1_partition_pruning_f2 values less than (-100),
--	partition p2_partition_pruning_f2 values less than (-0)
--		
--);

--insert into partition_pruning_f2 values(32764);
--insert into partition_pruning_f2 values(0);



--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 < 32765 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 < 32764 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 <= 32765 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 <= 32764 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 > 32765 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 > 32764 ;

--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 >= 32765 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 >= 32764 ;


--drop table partition_pruning_f2;

--create table partition_pruning_f2(f1 int)compress partition by range(f1) interval (3) 
--(
--	partition f1 values less than (-100),
--	partition p2_partition_pruning_f2 values less than (-0)
--		
--);
--insert into partition_pruning_f2 values(98294);
--insert into partition_pruning_f2 values(0);

--interval max is  0 + 3 * (32767-2) = 98295

--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 <= 98295 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 >= 98295 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 < 98296 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 <= 98296 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 < 98294 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 <= 98294 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 > 98296 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 >= 98296 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 > 98294 ;
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where f1 >= 98294 ;

--drop table partition_pruning_f2;



--create table partition_pruning_f2 (c1 timestamp  without time zone)
--partition by range (c1)
--interval( numtodsinterval(1,'day'))
--( 
--    partition DUMP_NEW_SQL_TAB_16_1  values less than ('2009-08-01 19:01:01.234')
--);

--inerval max is '2099-04-17 19:01:01.234'
--insert into partition_pruning_f2 values('2099-04-17 19:01:00.234');
--insert into partition_pruning_f2 values('2099-04-17 19:01:02.234');


--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 > '2099-04-17 19:01:01.234';
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 >= '2099-04-17 19:01:01.234';
--no partition

--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 < '2099-04-17 19:01:01.234';
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 <= '2099-04-17 19:01:01.234';
--two partition


--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 > '2099-04-17 19:01:00.234';
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 >= '2099-04-17 19:01:00.234';
-- one partition

--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 < '2099-04-17 19:01:00.234';
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 <= '2099-04-17 19:01:00.234';
--two partitoin

--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 > '2099-04-17 19:01:02.234';
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 >= '2099-04-17 19:01:02.234';
-- no partition

--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 < '2099-04-17 19:01:00.234';
--explain (ANALYZE false,VERBOSE false, COSTS false,BUFFERS false,TIMING false) select * from partition_pruning_f2 where c1 <= '2099-04-17 19:01:00.234';
--two partitoin


--drop table partition_pruning_f2;


--create table partition_pruning_f2(f1 int)compress partition by range(f1) interval (1000) (partition f1 values less than (1000));
--insert into partition_pruning_f2 values(1);
--select * from partition_pruning_f2 where f1 < 100000000;
--drop table partition_pruning_f2;
