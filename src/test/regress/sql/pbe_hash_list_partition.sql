CREATE TABLE r(a int, b int)
PARTITION BY RANGE (a)
(
	    PARTITION P1 VALUES LESS THAN(10),
	    PARTITION P2 VALUES LESS THAN(20),
	    PARTITION P3 VALUES LESS THAN(30),
	    PARTITION P4 VALUES LESS THAN(40)
);
create table l(a int, b int) partition by list(a)
( partition a_1 values(1,2), 
	  partition a_2 values(3,6));
 
create table h(a int, b int) partition by hash(a)
( partition h1_p_1, 
	  partition h2_p_2);

insert into r values(generate_series(1,39,1), generate_series(1,39,1));
insert into l values(1,1);
insert into h values(generate_series(1,39,1), generate_series(1,39,1));


--select
prepare p1(int,int) as SELECT * FROM r INNER JOIN h ON r.a=$1 and h.a=$2;
explain  execute p1(10,10);
execute p1(10,10);

prepare p2(int,int,int,int) as select * from r inner join h on r.a=$1 and h.a=$2 inner join l on r.a=$3 and l.a=$4;
explain  execute p2(1,1,1,1);
execute p2(1,1,1,1);

prepare p3(int,int) as SELECT * FROM r right JOIN h ON r.a=$1 and h.a=$2;
explain  execute p3(10,10);

prepare p4(int,int) as SELECT * FROM r left JOIN h ON r.a=$1 and h.a=$2;
explain  execute p4(10,10);

prepare p5(int,int) as SELECT * FROM h right JOIN l ON h.a=$1 and l.a=$2;
explain  execute p5(10,10);

prepare p6(int,int) as SELECT * FROM h left JOIN l ON h.a=$1 and l.a=$2;
explain  execute p6(10,10);

prepare p7(int) as SELECT * FROM h where h.a=$1;
explain  execute p7(10);
execute p7(10);

prepare p8(int) as SELECT * FROM h where h.a<=$1+5*2 ORDER BY h.a;
explain  execute p8(18);
execute p8(18);

prepare p9(int) as SELECT * FROM l where l.a=$1;
explain  execute p9(1);
execute p9(1);

prepare p10(int,int) as SELECT * FROM l where l.a>=$1+$2/5 ORDER BY l.a;
explain  execute p10(-2,5);
execute p10(-2,5);

--update
prepare p13(int) as UPDATE l set b = b + 10 where a = $1+1;
execute p13(0);
select * from l ORDER BY l.a;


-- delete
prepare p14(int) as DELETE FROM h where a>$1+1;
execute p14(15);
select * from h ORDER BY h.a;

