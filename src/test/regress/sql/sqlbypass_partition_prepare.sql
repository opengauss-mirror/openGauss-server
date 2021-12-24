set enable_opfusion=on;
set enable_partition_opfusion=on;
set enable_bitmapscan=off;
set enable_seqscan=off;
set opfusion_debug_mode = 'log';
set log_min_messages=debug;
set logging_module = 'on(OPFUSION)';
set sql_beta_feature = 'index_cost_with_leaf_pages_only';
--create table
drop table if exists test_bypass_sql_partition;
create table test_bypass_sql_partition(col1 int, col2 int, col3 text)
partition by range (col1)
(
	partition test_bypass_sql_partition_1 values less than(10),
	partition test_bypass_sql_partition_2 values less than(20),
	partition test_bypass_sql_partition_3 values less than(30),
	partition test_bypass_sql_partition_4 values less than(40),
	partition test_bypass_sql_partition_5 values less than(50),
	partition test_bypass_sql_partition_6 values less than(60),
	partition test_bypass_sql_partition_7 values less than(70),
	partition test_bypass_sql_partition_8 values less than(80)
);
create index itest_bypass_sql_partition on test_bypass_sql_partition(col1,col2) local;
insert into test_bypass_sql_partition select generate_series(0,79,1), generate_series(0,100,10), repeat('a',7);
prepare p11 as insert into test_bypass_sql_partition values($1,$2,$3);
prepare p12 as insert into test_bypass_sql_partition(col1,col2) values ($1,$2);



explain execute p11(0,0,'test_insert');
execute p11(10,10,'test_insert');
execute p11(12,12,'test_insert');
execute p11(-1,-1,'test_insert2');
execute p11(23,23,'test_insert2');
execute p11(33,33,'test_insert3');
execute p11(0,0,'test_insert3');
explain execute p12(1,1);
execute p12(1,1);
execute p12(22,22);
execute p12(20,20);

--nobypass
execute p11(null,null,null);
prepare p_insert1 as insert into test_bypass_sql_partition values(0,generate_series(1,100),'test');
execute p_insert1;
--bypass
prepare p13 as select * from test_bypass_sql_partition where col1=$1 and col2=$2;
explain execute p13(0,0);
execute p13(0,0);
execute p13(10,0);
execute p13(20,0);
execute p13(30,0);
execute p13(40,0);
set enable_indexonlyscan=off;
explain execute p13(0,0);
execute p13(0,0);
reset enable_indexonlyscan;
prepare p15 as update test_bypass_sql_partition set col2=col2+$4,col3=$3 where col1=$1 and col2=$2;
explain execute p15 (0,0,'test_update',-1);
execute p15 (0,0,'test_update',-1);
prepare p16 as update test_bypass_sql_partition set col2=mod($1,$2)  where col1=$3 and col2=$4;
explain execute p16(5,3,1,1);
execute p16(5,3,1,1);


prepare p101 as update test_bypass_sql_partition set col2=$1,col3=$2 where  col1=$3 ;
execute p101 (111,'test_update2',-1);
prepare p102 as select * from  test_bypass_sql_partition where col1=$1;
execute p102 (1);


prepare p1011 as insert into test_bypass_sql_partition values(-3,-3,'test_pbe');
prepare p1013 as update test_bypass_sql_partition set col2=10 where col1=-3;
prepare p1012 as select * from test_bypass_sql_partition where col1=-3;
prepare p1014 as select * from test_bypass_sql_partition where col1=-3 for update;
prepare p1015 as delete from test_bypass_sql_partition where col1=-3;
explain execute p1011;
execute p1011;
explain execute p1012;
execute p1012;
explain execute p1013;
execute p1013;
explain execute p1014;
execute p1014;
prepare p10141 as select col1,col2 from test_bypass_sql_partition where col1=-3 for update;
explain execute p10141;
execute p10141;

prepare p1021 as select * from  test_bypass_sql_partition where col1=$1 limit 1;
explain execute p1021(1);
execute p1021(1);
--bypass through index only scan

prepare p10211 as select col1,col2 from  test_bypass_sql_partition where col1=$1 limit 1;
explain execute p10211(1);
execute p10211(1);
prepare p10212 as select col1,col2 from  test_bypass_sql_partition where col1=$1 offset 1;
explain execute p10212(1);
execute p10212(1);
prepare p10213 as select col1,col2 from  test_bypass_sql_partition where col1=$1 limit 1 offset null;
explain execute p10213(1);
execute p10213(1);
prepare p10214 as select col1,col2 from  test_bypass_sql_partition where col1=$1 limit null offset null;
explain execute p10214(1);
execute p10214(1);



prepare p1024 as select * from  test_bypass_sql_partition where col1=0 order by col1 for update limit 1;
prepare p1025 as select * from  test_bypass_sql_partition where col1=$1 for update limit 1;
execute p1024;
execute p1025(1);
prepare p104 as select * from test_bypass_sql_partition where col1=$1 order by col1;
execute p104 (3);
prepare p1052 as select col1,col2 from test_bypass_sql_partition where col1=$1 order by col1 for update limit 2;
execute p1052(2);
prepare p10601 as select col1,col2 from test_bypass_sql_partition where col1>$1 and col1<$2 and col2>$3 order by col1;
execute p10601 (0,10,1);

--nobypass
prepare p1020 as  select * from  test_bypass_sql_partition where col1>0 order by col1 limit 1;
execute p1020;
prepare p10200 as  select col1,col2 from  test_bypass_sql_partition where col1>0 order by col1 limit 1;
explain execute p10200;
execute p10200;
prepare p1022 as select * from  test_bypass_sql_partition where col1=$1 limit $2;
explain execute p1022(0,3);
execute p1022(0,3);
explain execute p1022(0,null);
execute p1022(0,null);
prepare p1023 as select * from  test_bypass_sql_partition where col1=$1 limit $2 offset $3;
explain execute p1023(0,3,2);
execute p1023(0,3,2);
prepare p1026 as select * from  test_bypass_sql_partition where col1=$1 for update limit $2;
prepare p1027 as select * from  test_bypass_sql_partition where col1=$1 for update limit $2 offset $3;
explain execute p1026(0,3);
execute p1026(0,3);
explain execute p1027(0,3,2);
execute p1027(0,3,2);
prepare p103 as select col1,col2 from test_bypass_sql_partition where col2=$1 order by col1;
execute p103 (2);
prepare p105 as select col1,col2 from test_bypass_sql_partition where col1<$1 order by col1 limit 2;
execute p105 (5);
prepare p1051 as select col1,col2 from test_bypass_sql_partition where col1<$1 and col1>$2 order by col1 limit 3;
execute p1051(5,3);
prepare p106 as select col1,col2 from test_bypass_sql_partition where col1>$1 and col2>$2 order by col1 limit 3;
execute p106 (0,1);

--bypass
prepare p17 as select * from test_bypass_sql_partition where col1=$1 and col2=$2 for update;
execute p17 (3,3);
prepare p18 as update test_bypass_sql_partition set col2=$1*$2  where col1=$3 and col2=$4;
explain execute p18(3,7,3,3);
execute p18(3,7,3,3);
prepare p111 as select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 order by col1 desc;
execute p111;
prepare p112 as select col1,col2 from test_bypass_sql_partition where col1>10 and col1<20 order by col1 limit 10;
execute p112;
--nobypass
prepare p181 as update test_bypass_sql_partition set col2= $1 where col1 is null;
explain execute p181(111);
execute p181(111);

prepare p191 as select * from test_bypass_sql_partition where col1 is null  and col2 is null;
prepare p192 as delete from  test_bypass_sql_partition where col1 is null  and col2 is null;
execute p191;
execute p192;

--error
execute p11(null,null,'test_null');
--bypass through index only scan
prepare p11301 as select col1,col2 from test_bypass_sql_partition where col1 = $1 order by col1,col2 desc;
explain execute p11301 (2);
execute p11301 (2);
prepare p11401 as select col1,col2 from test_bypass_sql_partition where col1 = $1 order by col1,col2;
explain execute p11401 (2);
execute p11401 (2);
set enable_indexonlyscan=off;
explain execute p11301 (2);
execute p11401 (2);
reset enable_indexonlyscan;
--nobypass
prepare p120 as insert into test_bypass_sql_partition select * from test_bypass_sql_partition where col1>$1 and col1<$2;
execute p120 (0, 9);
--not bypass
prepare p115 as select * from test_bypass_sql_partition order by col2 desc;
explain execute p115;
execute p115;
prepare p116 as select * from test_bypass_sql_partition order by col2;
explain execute p116;
execute p116;
prepare p117 as select col1, col2 from test_bypass_sql_partition where true order by col1 limit 10;
prepare p118 as select col2, col1 from test_bypass_sql_partition order by col1 limit 10;
prepare p119 as select col1, col2 from test_bypass_sql_partition order by col1 desc limit 10;
execute p117;
execute p118;
execute p119;


--bypass
prepare p_select1 as select * from test_bypass_sql_partition where col1 = $1 limit 10;
execute p_select1(0);
prepare p_select2 as select * from test_bypass_sql_partition where col1 = $1 limit 10 for update;
execute p_select2(0);

--nobypass


prepare p_select3 as select col1, col2 from test_bypass_sql_partition limit 10;
execute p_select3;

prepare p_select4 as select col1, col2 from test_bypass_sql_partition where col1 < $1 and col1 > $2 limit 10;
execute p_select4(20,10);
prepare p_select5 as select col1, col2 from test_bypass_sql_partition where col1 <= $1 and col1 >=$2 limit 10;
execute p_select5(11,15);
--bypass
prepare p_update1 as update test_bypass_sql_partition set col3=$1 where col1=$2;
execute p_update1(1, 10);
prepare p_update2 as update test_bypass_sql_partition set col3=$1,col2=col1-$2 where col1=$3;
execute p_update2('test_null',1,1);
--nobypass
update test_bypass_sql_partition set col3='test_null' where col1 < 70;
update test_bypass_sql_partition set col3='test_null' where true;
update test_bypass_sql_partition set col1 = 1 where col1 > 1 and col1 < 10;
prepare p_update3 as update test_bypass_sql_partition set col1 = $1 where col1 > $2 and col1 < $3;
execute p_update3(1,1,10);

prepare p_update4 as update test_bypass_sql_partition set col3=$1,col2=mod(5,3) where col1 > $2 and col1 < $3;
execute p_update4('test_null',11,15);
prepare p_update5 as update test_bypass_sql_partition set col3=$1,col2=col1-1 where col1 >= $2 and col1 <= $3;
execute p_update5('test_null',1,10);

--bypass
prepare p_delete1 as delete from test_bypass_sql_partition where col1=$1;
execute p_delete1 (1);

--nobypass
prepare p_delete3 as delete from test_bypass_sql_partition where col1 > $1 and col1 < $2;
execute p_delete3(1,10);
prepare p_delete4 as delete from test_bypass_sql_partition where col1 <= $1 and col1 >= $2;
execute p_delete4(1,10);
prepare p_delete2 as delete from test_bypass_sql_partition where true;
execute p_delete2;
reset enable_partition_opfusion;
drop table test_bypass_sql_partition;

