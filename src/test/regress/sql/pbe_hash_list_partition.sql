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


drop table pbe_pt1;
create table pbe_pt1 (id int, name varchar2(100))
partition by range (id)
(
partition p1 values less than (100),
partition p2 values less than (200),
partition p3 values less than (MAXVALUE)
);

insert into pbe_pt1 values (1,'a'),(100,'b'),(200,'c');
select ctid, * from pbe_pt1;

-- tidscan
prepare pp1 as update pbe_pt1 set id = id + 10 where ctid = (select ctid from pbe_pt1 where id=$1) and id=$1;
explain (costs off) execute pp1 (1);

begin;
execute pp1 (1);
select ctid, * from pbe_pt1;
execute pp1 (100);
select ctid, * from pbe_pt1;
execute pp1 (200);
select ctid, * from pbe_pt1;
rollback;

-- seqscan
set enable_tidscan = 0;
prepare pp1_2 as update pbe_pt1 set id = id + 10 where ctid = (select ctid from pbe_pt1 where id=$1) and id=$1;
explain (costs off) execute pp1_2 (1);

begin;
execute pp1_2 (1);
select ctid, * from pbe_pt1;
execute pp1_2 (100);
select ctid, * from pbe_pt1;
execute pp1_2 (200);
select ctid, * from pbe_pt1;
rollback;

-- indexscan
set enable_indexscan = 1;
set enable_seqscan = 0;
set enable_bitmapscan = 0;
create index i_pbe_pt1 on pbe_pt1 (id) local;
prepare pp1_3 as update pbe_pt1 set id = id + 10 where ctid = (select ctid from pbe_pt1 where id=$1) and id=$1;
explain (costs off) execute pp1_3 (1);

begin;
execute pp1_3 (1);
select ctid, * from pbe_pt1;
execute pp1_3 (100);
select ctid, * from pbe_pt1;
execute pp1_3 (200);
select ctid, * from pbe_pt1;
rollback;

-- tidscan
set enable_tidscan = 1;
set enable_indexscan = 0;
prepare pp2 as select ctid, * from pbe_pt1 where id=$1 and ctid = (select ctid from pbe_pt1 where id=$1 and name=$2);
explain (costs off) execute pp2 (1, 'a');
execute pp2 (1,'a');
execute pp2 (100,'b');
execute pp2 (200,'c');

execute pp2 (100,'c');
execute pp2 (201,'c');

-- seqscan
set enable_seqscan = 1;
set enable_tidscan = 0;
prepare pp2_2 as select ctid, * from pbe_pt1 where id=$1 and ctid = (select ctid from pbe_pt1 where id=$1 and name=$2);
explain (costs off) execute pp2_2 (1, 'a');
execute pp2_2 (1,'a');
execute pp2_2 (100,'b');
execute pp2_2 (200,'c');

execute pp2_2 (100,'c');
execute pp2_2 (111,'c');

-- indexscan
set enable_indexscan = 1;
set enable_seqscan = 0;
prepare pp2_3 as select ctid, * from pbe_pt1 where id=$1 and ctid = (select ctid from pbe_pt1 where id=$1 and name=$2);
explain (costs off) execute pp2_3 (1, 'a');
execute pp2_3 (1,'a');
execute pp2_3 (100,'b');
execute pp2_3 (200,'c');

execute pp2_3 (100,'c');
execute pp2_3 (111,'c');

drop table pbe_pt1;
set enable_tidscan = 1;
set enable_indexscan = 1;
set enable_seqscan = 1;
set enable_bitmapscan = 1;


drop table pbe_sp1;
create table pbe_sp1(id int, name text, age int)
partition by hash(id)
subpartition by list(age)
(
partition p1
  (
    subpartition sp11 values (1,2,3,4,5),
    subpartition sp12 values (default)
  ),
partition p2
  (
    subpartition sp21 values (11,12,13,14,15),
    subpartition sp22 values (default)
  )
);
insert into pbe_sp1 values (3,'sp11',1), (4,'sp12',6), (5,'sp21',11), (6,'sp22', NULL);
select ctid, * from pbe_sp1 subpartition (sp11);
select ctid, * from pbe_sp1 subpartition (sp12);
select ctid, * from pbe_sp1 subpartition (sp21);
select ctid, * from pbe_sp1 subpartition (sp22);

-- tidscan
prepare pp3 as update pbe_sp1 set age=$4 where ctid = (select ctid from pbe_sp1 where name=$2 and (age=$3 or ($3 is null and age is null))) and id=$1 and (age=$3 or ($3 is null and age is null));
explain (costs off) execute pp3 (6, 'sp22', NULL, NULL);

begin;
execute pp3 (3, 'sp11', 1, 2);
select ctid, * from pbe_sp1 subpartition (sp11) order by id;
execute pp3 (3, 'sp11', 2, 7);
select ctid, * from pbe_sp1 subpartition (sp12) order by id;
execute pp3 (4, 'sp12', 6, 8);
select ctid, * from pbe_sp1 subpartition (sp12) order by id;
execute pp3 (4, 'sp12', 8, 5);
select ctid, * from pbe_sp1 subpartition (sp11) order by id;
execute pp3 (6, 'sp22', NULL, NULL);
select ctid, * from pbe_sp1 subpartition (sp22) order by id;
execute pp3 (6, 'sp22', NULL, 15);
select ctid, * from pbe_sp1 subpartition (sp21) order by id;
execute pp3 (5, 'sp21', 11, 12);
select ctid, * from pbe_sp1 subpartition (sp21) order by id;
execute pp3 (5, 'sp21', 12, NULL);
select ctid, * from pbe_sp1 subpartition (sp22) order by id;
rollback;

prepare pp4 as select ctid, * from pbe_sp1 where id=$1 and (age=$3 or ($3 is null and age is null)) and name=$2 and ctid=(select ctid from pbe_sp1 where id=$1 and name=$2);
explain (costs off) execute pp4 (3, 'sp11', 1);

execute pp4 (3, 'sp11', 1);
execute pp4 (4, 'sp12', 6);
execute pp4 (5, 'sp21', 11);
execute pp4 (6, 'sp22', NULL);

-- seqscan
set enable_tidscan=0;
prepare pp5 as select ctid, * from pbe_sp1 where id=$1 and name=$2 and ctid = (select ctid from pbe_sp1 where id=$1 and (age=$3 or ($3 is null and age is null)) and name=$2);
explain (costs off) execute pp5 (3, 'sp11', 1);

execute pp5 (3, 'sp11', 1);
execute pp5 (4, 'sp12', 6);
execute pp5 (5, 'sp21', 11);
execute pp5 (6, 'sp22', NULL);

-- indexscan
create unique index i_pbe_sp1 on pbe_sp1(id,age) local;
set enable_seqscan = 0;
set enable_indexscan = 1;
set enable_indexonlyscan = 0;
set enable_bitmapscan = 0;
set enable_tidscan = 0;
prepare pp6 as select ctid, * from pbe_sp1 where id=$1 and (age=$3 or ($3 is null and age is null)) and name=$2 and ctid=(select ctid from pbe_sp1 where id=$1 and name=$2);
explain (costs off) execute pp6 (3, 'sp11', 1);

execute pp6 (3, 'sp11', 1);
execute pp6 (4, 'sp12', 6);
execute pp6 (5, 'sp21', 11);
execute pp6 (6, 'sp22', NULL);

-- indexonlyscan
create index i2_pbe_sp1 on pbe_sp1(id,name,ctid) local;
set enable_indexscan = 0;
set enable_indexonlyscan = 1;
prepare pp7 as select ctid, id, name from pbe_sp1 where id=$1 and name = (select name from pbe_sp1 where id=$1 and name=$2);
explain (costs off) execute pp7 (3, 'sp11');

execute pp7 (3, 'sp11');
execute pp7 (4, 'sp12');
execute pp7 (5, 'sp21');
execute pp7 (6, 'sp22');

drop index i2_pbe_sp1;

-- bitmapscan
set enable_indexonlyscan = 0;
set enable_bitmapscan = 1;
prepare pp8 as select ctid, id from pbe_sp1 where id=$1 and name=$2 and ctid=(select ctid from pbe_sp1 where id=$1 and name=$2 and (age=$3 or ($3 is null and age is null)));
explain (costs off) execute pp8 (3, 'sp11', 1);

execute pp8 (3, 'sp11', 1);
execute pp8 (4, 'sp12', 6);
execute pp8 (5, 'sp21', 11);
execute pp8 (6, 'sp22', NULL);

drop table pbe_sp1;
