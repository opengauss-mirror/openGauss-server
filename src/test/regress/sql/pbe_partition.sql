SET plan_cache_mode = force_generic_plan;


-- create range_partition table.

CREATE TABLE partition_scan1(a int, b int)
PARTITION BY RANGE (a)
(
	    PARTITION P1 VALUES LESS THAN(10),
	    PARTITION P2 VALUES LESS THAN(20),
	    PARTITION P3 VALUES LESS THAN(30),
	    PARTITION P4 VALUES LESS THAN(40)
);

CREATE UNIQUE INDEX index_on_partition_scan1 ON partition_scan1(a) LOCAL;

insert into partition_scan1 values(generate_series(1,39,1), generate_series(1,39,1));

-- 等于param
prepare p1(int) as SELECT * FROM partition_scan1 s1 where s1.a = $1 ORDER BY s1.a;
explain (costs off) execute p1(10);
execute p1(10);

--大于param
prepare p2(int) as SELECT * FROM partition_scan1 s1 where s1.a >$1 ORDER BY s1.a;
explain (costs off) execute p2(35);
execute p2(35);

--小于param
prepare p3(int) as SELECT * FROM partition_scan1 s1 where s1.a <$1 ORDER BY s1.a;
explain (costs off) execute p3(35);
execute p3(35);

-- 大于等于param
prepare p4(int) as SELECT * FROM partition_scan1 s1 where s1.a >=$1 ORDER BY s1.a;
explain (costs off) execute p4(35);
execute p4(35);

-- 小于等于param
prepare p5(int) as SELECT * FROM partition_scan1 s1 where s1.a <=$1 ORDER BY s1.a;
explain (costs off) execute p5(35);
execute p5(35);


-- 等于expr
prepare p6(int,int) as SELECT * FROM partition_scan1 s1 where s1.a = $1+$2+1 ORDER BY s1.a;
explain (costs off) execute p6(10,10);
execute p6(10,10);

--大于expr
prepare p7(int,int) as SELECT * FROM partition_scan1 s1 where s1.a > $1+$2+1 ORDER BY s1.a;
explain (costs off) execute p7(10,10);
execute p7(10,10);

--小于expr
prepare p8(int,int) as SELECT * FROM partition_scan1 s1 where s1.a < $1+$2+1 ORDER BY s1.a;
explain (costs off) execute p8(10,10);
execute p8(10,10);

-- 大于等于expr
prepare p9(int,int) as SELECT * FROM partition_scan1 s1 where s1.a >= $1+$2+1 ORDER BY s1.a;
explain (costs off) execute p9(10,10);
execute p9(10,10);

-- 小于等于expr
prepare p10(int,int) as SELECT * FROM partition_scan1 s1 where s1.a <= $1+$2+1 ORDER BY s1.a;
explain (costs off) execute p10(10,10);
execute p10(10,10);

--boolexpr_and
prepare p11(int,int) as SELECT * FROM partition_scan1 s1 where s1.a >= $1 and s1.b = $2 ORDER BY s1.a;
explain (costs off) execute p11(10,10);
execute p11(10,10);

prepare p12(int,int) as SELECT * FROM partition_scan1 s1 where s1.a > $1 and s1.b < $2;
explain (costs off) execute p12(10,10);

prepare p13(int,int) as SELECT * FROM partition_scan1 s1 where s1.a <= $1 and s1.b = $2+1;
explain (costs off) execute p13(10,10);

prepare p131(int,int) as SELECT * FROM partition_scan1 s1 where s1.a < $1+1 and s1.b > $2+1;
explain (costs off) execute p131(10,10);


--update
prepare p14(int) as UPDATE partition_scan1 set b = b + 10 where a = $1;
explain (costs off) execute p14(10);

prepare p15(int) as UPDATE partition_scan1 set b = b + 10 where a > $1;
explain (costs off) execute p15(10);

prepare p16(int) as UPDATE partition_scan1 set b = b + 10 where a >= $1+1;
explain (costs off) execute p16(10);

prepare p17(int) as UPDATE partition_scan1 set b = b + 10 where a <= $1;
explain (costs off) execute p17(10);

prepare p18(int) as UPDATE partition_scan1 set b = b + 10 where a < $1;
explain (costs off) execute p18(10);

prepare p181(int,int) as UPDATE partition_scan1 set b = b + 10 where a < $1 and b < $2;
explain (costs off) execute p181(10,10);



-- delete
prepare p19(int) as DELETE FROM partition_scan1 where a=$1; 
explain (costs off) execute p19(1);

prepare p20(int) as DELETE FROM partition_scan1 where a>$1;
explain (costs off) execute p20(38);

prepare p21(int) as DELETE FROM partition_scan1 where a<$1;
explain (costs off) execute p21(3);

prepare p22(int) as DELETE FROM partition_scan1 where a>$1+1;
explain (costs off) execute p22(37);

prepare p23(int) as DELETE FROM partition_scan1 where a<$1+1;
explain (costs off) execute p23(5);

prepare p24(int,int) as DELETE FROM partition_scan1 where a>$1 and b>$2;
explain (costs off) execute p24(30,1);