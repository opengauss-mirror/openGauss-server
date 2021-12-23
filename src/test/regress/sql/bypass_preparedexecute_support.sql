--
--prepared execute bypass
--
set enable_opfusion=on;
set enable_bitmapscan=off;
set enable_seqscan=off;
set opfusion_debug_mode = 'log';
set log_min_messages=debug;
set logging_module = 'on(OPFUSION)';
set sql_beta_feature = 'index_cost_with_leaf_pages_only';

drop table if exists test_bypass_sq1;
create table test_bypass_sq1(col1 int, col2 int, col3 text);
create index itest_bypass_sq1 on test_bypass_sq1(col1,col2);

prepare p11 as insert into test_bypass_sq1 values($1,$2,$3);
prepare p12 as insert into test_bypass_sq1(col1,col2) values ($1,$2);
--bypass
explain execute p11(0,0,'test_insert');
execute p11(0,0,'test_insert');
execute p11(1,1,'test_insert');
execute p11(-1,-1,'test_insert2');
execute p11(2,2,'test_insert2');
execute p11(3,3,'test_insert3');
execute p11(0,0,'test_insert3');
explain execute p12(1,1);
execute p12(1,1);
execute p12(2,2);
execute p12(0,0);
execute p11(null,null,null);
--bypass
prepare p13 as select * from test_bypass_sq1 where col1=$1 and col2=$2;
explain execute p13(0,0);
execute p13(0,0);
prepare p13 as select col1,col2 from test_bypass_sq1 where col1=$1 and col2=$2;
--bypass through index only scan
explain execute p13(0,0);
execute p13(0,0);
set enable_indexonlyscan=off;
execute p13(0,0);
reset enable_indexonlyscan;
--not bypass (distribute key)
--prepare p14 as update test_bypass_sq1 set col1=col1+$4,col2=$5,col3=$3 where col1=$1 and col2=$2;
--execute p14 (0,0,'test_update',3,-1);
--bypass
prepare p15 as update test_bypass_sq1 set col2=col2+$4,col3=$3 where col1=$1 and col2=$2;
execute p15 (0,0,'test_update',-1);
--bypass
prepare p16 as update test_bypass_sq1 set col2=mod($1,$2)  where col1=$3 and col2=$4;
execute p16(5,3,1,1);
--bypass / set  enable_bitmapscan=off;
prepare p101 as update test_bypass_sq1 set col2=$1,col3=$2 where  col1=$3 ;
execute p101 (111,'test_update2',-1);
prepare p102 as select * from  test_bypass_sq1 where col1=$1;
execute p102 (1);
prepare p1011 as insert into test_bypass_sq1 values(-3,-3,'test_pbe');
prepare p1013 as update test_bypass_sq1 set col2=10 where col1=-3;
prepare p1012 as select * from test_bypass_sq1 where col1=-3;
prepare p1014 as select * from test_bypass_sq1 where col1=-3 for update;
prepare p1015 as delete from test_bypass_sq1 where col1=-3;
explain execute p1011;
execute p1011;
explain execute p1012;
execute p1012;
explain execute p1013;
execute p1013;
explain execute p1014;
execute p1014;
--bypass through index only scan
prepare p10141 as select col1,col2 from test_bypass_sq1 where col1=-3 for update;
execute p10141;
execute p1015;

prepare p1020 as  select * from  test_bypass_sq1 where col1>0 order by col1 limit 1;
execute p1020;
prepare p1021 as select * from  test_bypass_sq1 where col1=$1 limit 1;
execute p1021(1);
--bypass through index only scan
prepare p10200 as  select col1,col2 from  test_bypass_sq1 where col1>0 order by col1 limit 1;
explain execute p10200;
execute p10200;
prepare p10211 as select col1,col2 from  test_bypass_sq1 where col1=$1 limit 1;
explain execute p10211(1);
execute p10211(1);
prepare p10212 as select col1,col2 from  test_bypass_sq1 where col1=$1 offset 1;
explain execute p10212(1);
execute p10212(1);
prepare p10213 as select col1,col2 from  test_bypass_sq1 where col1=$1 limit 1 offset null;
explain execute p10213(1);
execute p10213(1);
prepare p10214 as select col1,col2 from  test_bypass_sq1 where col1=$1 limit null offset null;
explain execute p10214(1);
execute p10214(1);
--not bypass
prepare p1022 as select * from  test_bypass_sq1 where col1=$1 limit $2;
explain execute p1022(0,3);
explain execute p1022(0,null);
prepare p1023 as select * from  test_bypass_sq1 where col1=$1 limit $2 offset $3;
explain execute p1023(0,3,2);
--bypass
prepare p1024 as select * from  test_bypass_sq1 where col1=0 order by col1 for update limit 1;
prepare p1025 as select * from  test_bypass_sq1 where col1=$1 for update limit 1;
execute p1024;
execute p1025(1);
--not bypass
prepare p1026 as select * from  test_bypass_sq1 where col1=$1 for update limit $2;
prepare p1027 as select * from  test_bypass_sq1 where col1=$1 for update limit $2 offset $3;
explain execute p1026(0,3);
execute p1026(0,3);
explain execute p1027(0,3,2);
execute p1027(0,3,2);
--bypass
prepare p103 as select col1,col2 from test_bypass_sq1 where col2=$1 order by col1;
execute p103 (2);
prepare p104 as select * from test_bypass_sq1 where col1=$1 order by col1;
execute p104 (3);
prepare p105 as select col1,col2 from test_bypass_sq1 where col2<$1 order by col1;
execute p105 (5);
prepare p1051 as select col1,col2 from test_bypass_sq1 where col2<$1 order by col1 limit 3;
execute p1051(5);
prepare p1052 as select col1,col2 from test_bypass_sq1 where col1=$1 order by col1 for update limit 2;
execute p1052(2);
prepare p106 as select col1,col2 from test_bypass_sq1 where col1>$1 and col2>$2 order by col1;
explain execute p106 (0,1);
execute p106 (0,1);
prepare p1061 as select col1,col2 from test_bypass_sq1 where col1 is not null and col2 is not null order by col1;
explain execute p1061;
execute p1061;
prepare p1062 as select col1,col2 from test_bypass_sq1 where col1 is not null and col2 < $1 order by col1;
execute p1062 (3);
--bypass through index only scan
prepare p10601 as select col1,col2 from test_bypass_sq1 where col1>$1 and col2>$2 order by col1;
execute p10601 (0,1);
--bypass
prepare p17 as select * from test_bypass_sq1 where col1=$1 and col2=$2 for update;
execute p17 (3,3);
--bypass (?)
prepare p18 as update test_bypass_sq1 set col2=$1*$2  where col1=$3 and col2=$4;
explain execute p18(3,7,3,3);
execute p18(3,7,3,3);
prepare p181 as update test_bypass_sq1 set col2= $1 where col1 is null;
explain execute p181(111);
execute p181(111);
--bypass
prepare p19 as delete from  test_bypass_sq1 where col1=$1 and col2=$2;
execute p19 (0,-1);

execute p11(null,null,'test_null');
prepare p191 as select * from test_bypass_sq1 where col1 is null  and col2 is null;
prepare p192 as delete from  test_bypass_sq1 where col1 is null  and col2 is null;
execute p191;
execute p192;
--bypass (order by is supported when ordered col is  in index)
prepare p111 as select col1,col2 from test_bypass_sq1 order by col1 desc;
execute p111;
prepare p112 as select col1,col2 from test_bypass_sq1 order by col1;
execute p112;
--bypass through index only scan
prepare p11301 as select col1,col2 from test_bypass_sq1 where col1 = $1 order by col1,col2 desc;
explain execute p11301 (2);
prepare p11401 as select col1,col2 from test_bypass_sq1 where col1 = $1 order by col1,col2;
explain execute p11401 (2);
set enable_indexonlyscan=off;
explain execute p11301 (2);
explain execute p11401 (2);
reset enable_indexonlyscan;
--not bypass
prepare p115 as select * from test_bypass_sq1 order by col2 desc;
explain execute p115;
prepare p116 as select * from test_bypass_sq1 order by col2;
explain execute p116;
-- bypass
prepare p117 as select col1, col2 from test_bypass_sq1 where true order by col1;
prepare p118 as select col2, col1 from test_bypass_sq1 order by col1;
prepare p119 as select col1, col2 from test_bypass_sq1 order by col1 desc;
execute p117;
execute p118;
execute p119;

prepare p120 as insert into test_bypass_sq1 select * from test_bypass_sq1 where col1>$1;
execute p120 (0);
--
drop table if exists test_bypass_sq2;
create table test_bypass_sq2(col1 int not null, col2 int);
create index itest_bypass_sq2 on test_bypass_sq2(col1);
--bypass
prepare p21 as insert into test_bypass_sq2(col1) values ($1);
execute p21(0);
--error
execute p21(null);
--bypass
prepare p22 as insert into test_bypass_sq2(col1,col2) values ($1,$2);
explain execute p22(1,null);
execute p22(1,null);
execute p22(3,3);
execute p22(-1,-1);
execute p22(1,1);
execute p22(2,2);
execute p22(3,3);

--bypass
prepare p24 as update test_bypass_sq2 set col2 = col2+$1 where col1 = $2;
execute p24(1,0);
prepare p25 as select * from test_bypass_sq2  where col1 = $1;
execute p25(0);
--bypass
prepare p26 as select * from test_bypass_sq2  where col1 >= $1 order by col1;
execute p26 (0);
prepare p261 as select * from test_bypass_sq2  where col1 >= $1 order by col1 limit 1;
execute p261 (0);
prepare p262 as select * from test_bypass_sq2  where col1 = $1 order by col1 for update limit 2;
explain execute p262 (0);
execute p262 (0);
--bypass through index only scan
prepare p26101 as select col1 from test_bypass_sq2  where col1 >= $1 order by col1 limit 2;
execute p26101 (0);
--not bypass
prepare p201 as select * from test_bypass_sq2  where col2 = $1;
explain execute p201 (0);
explain execute p201 (null);
prepare p202 as select * from test_bypass_sq2  where col1 = $1 and col2 = $2;
explain execute p202 (0,0);
--not bypass
prepare p203 as select t1.col3, t2.col2  from test_bypass_sq1 as t1 join test_bypass_sq2 as t2 on t1.col1=t2.col1;
explain execute p203;
prepare p204 as select count(*),col1 from test_bypass_sq1 group by col1;
explain execute p204;
--bypass (order by is supported when ordered col is in index)
prepare p211 as select col1,col2 from test_bypass_sq1 order by col1 desc;
execute p211;
prepare p212 as select col1,col2 from test_bypass_sq1 order by col1;
execute p212;
--not bypass
prepare p213 as select * from test_bypass_sq2 order by col1,col2;
explain execute p213;
--bypass
prepare p27 as select * from test_bypass_sq2 where col1 = $1 limit 1;
execute p27(0);
execute p27(1);
prepare p270 as select * from test_bypass_sq2 where col1 = $1 for update limit 1;
execute p270(0);
execute p270(1);
prepare p271 as select * from test_bypass_sq2 where col1 > $1 order by col1 limit 2 offset 1;
execute p271(0);
prepare p2710 as select * from test_bypass_sq2 where col1 > $1 for update limit 2 offset 1;
explain execute p2710(0);

--not bypass
prepare p272 as select * from test_bypass_sq2 where col1 = $1 limit $2;
explain execute p272(0,3);
prepare p2720 as select * from test_bypass_sq2 where col1 = $1 for update limit $2;
explain execute p2720(0,3);
prepare p273 as select * from test_bypass_sq2 where col1 = $1 limit -1;
explain execute p273(0);
--bypass
prepare p2730 as select * from test_bypass_sq2 where col1 = $1 for update limit -1;
explain execute p2730(0);
prepare p274 as select * from test_bypass_sq2 where col1 = $1 limit 0;
execute p274(0);
prepare p2740 as select * from test_bypass_sq2 where col1 = $1 for update limit 0;
execute p2740(0);
prepare p275 as select * from  test_bypass_sq2 where col1 is not null;
explain execute p275;

--
drop table if exists test_bypass_sq3;
create table test_bypass_sq3(col1 int default 1, col2 int, col3 timestamp);
create index itest_bypass_sq3 on test_bypass_sq3(col1);
--bypass
prepare p31 as insert into test_bypass_sq3(col2,col3) values ($1,$2);
--wrong input
execute p31(1,default);
--bypass
execute p31(3,null);
prepare p32 as insert into test_bypass_sq3 values($1,$2,$3);
execute p32(2,3,null);
execute p32 (3,3,null);
execute p32 (null,3,null);
--bypass(?)
execute p32 (-1,-1,current_timestamp);
--bypass
prepare p33 as select * from test_bypass_sq3 where col1 >= $1 order by col1;
execute p33(1);
execute p33(2);

prepare p34 as select col2 from test_bypass_sq3 where col1 = $1 for update;
prepare p35 as update test_bypass_sq3 set col2 = col2*3 where col1 = $1;
execute p34(2);
execute p35(2);
execute p34(1);
execute p35(1);
--not bypass
prepare p36 as select * from test_bypass_sq3 where col2 > 0;
explain execute p36(3);
--bypass
prepare p37 as select * from test_bypass_sq3 where col1 is null;
execute p37;


--test random index pos
drop table if exists test_bypass_sq4;
create table test_bypass_sq4(col1 int, col2 int, col3 int);
create index itest_bypass_sq4 on test_bypass_sq4(col3,col2);
prepare p40 as insert into test_bypass_sq4 values ($1,$2,$3);
execute p40 (11,21,31);
execute p40 (11,22,32);
execute p40 (12,23,32);
execute p40 (12,23,33);
execute p40 (13,24,33);
execute p40 (13,24,34);
execute p40 (14,25,34);
execute p40 (14,25,35);
execute p40 (55,55,55);
execute p40 (55,55,null);
execute p40  (55,null,55);
execute p40  (55,null,null);
explain execute p40 (null,null,null);
execute p40 (null,null,null);
prepare p401 as select col3, col1, col2 from test_bypass_sq4 where col2 >$1 order by 1,3;
execute p401 (22);
prepare p402 as  select * from test_bypass_sq4 where col2 =$1 and col3= $2 order by col2;
execute p402(22,32);
prepare p403 as select col3,col2,col3 from test_bypass_sq4 where col3 >= $1 and col2 >= $2 order by col3,col2;
execute p403 (33,22);
prepare p404 as select col2,col3,col2 from test_bypass_sq4 where col3 >= $1 and col2 >= $2 order by col3,col2;
execute p404 (34,22);
prepare p405 as select col3,col2,col3 from test_bypass_sq4 where col3 >= $1 and col2 >= $2 order by col3 for update;
execute p406 (33,22);
prepare p406 as select col2,col3,col2 from test_bypass_sq4 where col3 >= $1 and col2 >= $2 order by col3,col2 for update;
execute p406 (34,22);
prepare p407 as select col2,col3,col2 from test_bypass_sq4 where col3 is null and col2 is null order by col3,col2;
execute p407;
prepare p408 as select col2,col3 from test_bypass_sq4 where col3 is null and col2 is not null;
execute p408;
prepare p409 as select col2,col3 from test_bypass_sq4 where col3 is not null order by col3 desc,col2 desc;
execute p409;


drop table if exists test_bypass_sq6;
create type complextype AS (f1 int, f2 text);
create table test_bypass_sq6(col1 int, col2 complextype,col3 text);
create index itest_bypass_sq6 on test_bypass_sq6(col1,col3);
--not bypass
prepare p601 as insert into test_bypass_sq6 values ($1,ROW($2, $3),$4);
explain execute p601 (1,1,'Simon1'::text,'test'::text);
-- just insert
reset opfusion_debug_mode;
execute p601 (1,1,'Simon1'::text,'test'::text);
set opfusion_debug_mode = 'error';
--bypass
prepare p602 as select * from test_bypass_sq6 where col1 is not null;
execute p602;
prepare p603 as select * from test_bypass_sq6 where col3 =$1 for update;
execute p603 ('test'::text);
prepare p604 as update test_bypass_sq6 set col3= $1 where col1 = $2;
execute p604 ('test_2'::text,1);
prepare p605 as select col1 from test_bypass_sq6;
execute p605;
prepare p606 as select col3 from test_bypass_sq6 order by col1,col3;
execute p606;
prepare p607 as select col1, col3 from test_bypass_sq6 where true;
execute p607;
prepare p608 as update test_bypass_sq6 set col2=$1 where col1 = $2;
execute p608(ROW(2,'Ruby2'::text),1);
--notbypass
prepare p6071 as select * from test_bypass_sq6 where true;
explain execute p6071;
prepare p609 as update test_bypass_sq6 set col2=ROW($1,$2) where col1 = $3;
explain execute p609 (3,'Ruby3'::text,1);
--bypass
prepare p6091 as delete from test_bypass_sq6 where col1 = $1;
execute p6091(1);

create table test_bypass_sq7(a int, b int);
create index itest_bypass_sq7 on test_bypass_sq7(a);
insert into test_bypass_sq7 values(1,2);
insert into test_bypass_sq7 values(2,2);
prepare p71 as select * from test_bypass_sq7 where a=$1;
execute p71(1);
explain execute p71(1);
execute p71(1);
drop index itest_bypass_sq7;
execute p71(1);

reset enable_seqscan;
reset enable_bitmapscan;
reset opfusion_debug_mode;
reset log_min_messages;
reset logging_module;

drop table test_bypass_sq1;
drop table test_bypass_sq2;
drop table test_bypass_sq3;
drop table test_bypass_sq4;
drop table test_bypass_sq6;
drop table test_bypass_sq7;
drop type complextype;
