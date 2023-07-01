\c postgres
DROP DATABASE IF EXISTS db_1097149;
CREATE DATABASE db_1097149 DBCOMPATIBILITY 'B';
\c db_1097149
-- normal index 
create table db_1097149_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_1097149_tb values(1,1,1,'a');
insert into db_1097149_tb values(1,2,2,'a');
insert into db_1097149_tb values(2,2,2,'a');
insert into db_1097149_tb values(2,2,3,'b');
insert into db_1097149_tb values(2,3,3,'b');
insert into db_1097149_tb values(3,3,4,'b');
insert into db_1097149_tb values(3,3,4,'a');
insert into db_1097149_tb values(3,4,5,'c');
insert into db_1097149_tb values(4,4,5,'c');
insert into db_1097149_tb values(4,null,1,'c');

create index index_1097149_1 on db_1097149_tb (col1);
create index index_1097149_2 on db_1097149_tb (col2);
create index index_1097149_3 on db_1097149_tb (col3);
create index index_1097149_4 on db_1097149_tb (col4);
analyze db_1097149_tb;

select * from db_1097149_tb force key (index_1097149_2) where col2= 3;

select * from db_1097149_tb force key (index_1097149_4) where col2= 3 and col4 = 'a';

select * from db_1097149_tb FORCE key (index_1097149_1) where col2= 3;

explain (costs off,verbose true  )select * from db_1097149_tb force key (index_1097149_2) where col2= 3;

explain (costs off,verbose true  )select * from db_1097149_tb force key (index_1097149_4) where col2= 3 and col4 = 'a';

explain (costs off,verbose true ) select * from db_1097149_tb FORCE key (index_1097149_1) where col2= 3;


--mix use force  and use index error.

create table db_1097156_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_1097156_tb values(1,1,1,'a');
insert into db_1097156_tb values(1,2,2,'a');
insert into db_1097156_tb values(2,2,2,'a');
insert into db_1097156_tb values(2,2,3,'b');
insert into db_1097156_tb values(2,3,3,'b');
insert into db_1097156_tb values(3,3,4,'b');
insert into db_1097156_tb values(3,3,4,'a');
insert into db_1097156_tb values(3,4,5,'c');
insert into db_1097156_tb values(4,4,5,'c');
insert into db_1097156_tb values(4,null,1,'c');

create index index_1097156_1 on db_1097156_tb (col1);
create index index_1097156_2 on db_1097156_tb (col2);
create index index_1097156_3 on db_1097156_tb (col3);
create index index_1097156_4 on db_1097156_tb (col4);
analyze db_1097156_tb;

select * from db_1097156_tb use index (index_1097156_1) force index (index_1097156_2) where col2= 3;

select * from db_1097156_tb force index (index_1097156_2) use index (index_1097156_1) where col2= 3;

select * from db_1097156_tb use index (index_1097156_2) force index (index_1097156_2) where col2= 3;

select * from db_1097156_tb use index (index_1097156_1) force index (index_1097156_2) use index (index_1097156_3) use index (index_1097156_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b';

-- use ,choose low cost  plan in index or seqscan

create table db_1097155_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_1097155_tb values(1,1,1,'a');
insert into db_1097155_tb values(1,2,2,'a');
insert into db_1097155_tb values(2,2,2,'a');
insert into db_1097155_tb values(2,2,3,'b');
insert into db_1097155_tb values(2,3,3,'b');
insert into db_1097155_tb values(3,3,4,'b');
insert into db_1097155_tb values(3,3,4,'a');
insert into db_1097155_tb values(3,4,5,'c');
insert into db_1097155_tb values(4,4,5,'c');
insert into db_1097155_tb values(4,null,1,'c');

create index index_1097155_1 on db_1097155_tb (col1);
create index index_1097155_2 on db_1097155_tb (col2);
create index index_1097155_3 on db_1097155_tb (col3);
create index index_1097155_4 on db_1097155_tb (col4);
analyze db_1097155_tb;

select * from db_1097155_tb use index (index_1097155_1) use index (index_1097155_2) where col2= 3 order by 1,2,3,4;

select * from db_1097155_tb use index (index_1097155_2) use index (index_1097155_1) where col2= 3 order by 1,2,3,4;

select * from db_1097155_tb use index (index_1097155_2) use index (index_1097155_2) where col2= 3 order by 1,2,3,4;

select * from db_1097155_tb use index (index_1097155_1) use index (index_1097155_2)use index (index_1097155_3) use index (index_1097155_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b' order by 1,2,3,4;

select * from db_1097155_tb use index (index_1097155_1, index_1097155_2) where col2= 3 order by 1,2,3,4;

select * from db_1097155_tb use index (index_1097155_2, index_1097155_1) where col2= 3 order by 1,2,3,4;

select * from db_1097155_tb use index (index_1097155_2, index_1097155_2) where col2= 3 order by 1,2,3,4;

select * from db_1097155_tb use index (index_1097155_1, index_1097155_2, index_1097155_3, index_1097155_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b' order by 1,2,3,4;

explain (costs off )select * from db_1097155_tb use index (index_1097155_1) use index (index_1097155_2) where col2= 3;

explain (costs off )select * from db_1097155_tb use index (index_1097155_2) use index (index_1097155_1) where col2= 3;

explain (costs off )select * from db_1097155_tb use index (index_1097155_2) use index (index_1097155_2) where col2= 3;

explain (costs off )select * from db_1097155_tb use index (index_1097155_1) use index (index_1097155_2)use index (index_1097155_3) use index (index_1097155_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b';

explain (costs off  )select * from db_1097155_tb use index (index_1097155_1, index_1097155_2) where col2= 3;

explain (costs off  )select * from db_1097155_tb use index (index_1097155_2, index_1097155_1) where col2= 3;

explain (costs off  )select * from db_1097155_tb use index (index_1097155_2, index_1097155_2) where col2= 3;

explain (costs off )select * from db_1097155_tb use index (index_1097155_1, index_1097155_2, index_1097155_3, index_1097155_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b';

-- force ,choose index only, no seqscan
explain (costs off )select * from db_1097155_tb force index (index_1097155_1) use index (index_1097155_2) where col2= 3;

explain (costs off )select * from db_1097155_tb force index (index_1097155_2) use index (index_1097155_1) where col2= 3;

explain (costs off )select * from db_1097155_tb force index (index_1097155_2) use index (index_1097155_2) where col2= 3;

explain (costs off )select * from db_1097155_tb force index (index_1097155_1) use index (index_1097155_2)use index (index_1097155_3) use index (index_1097155_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b';

explain (costs off  )select * from db_1097155_tb force index (index_1097155_1, index_1097155_2) where col2= 3;

explain (costs off  )select * from db_1097155_tb force index (index_1097155_2, index_1097155_1) where col2= 3;

explain (costs off  )select * from db_1097155_tb force index (index_1097155_2, index_1097155_2) where col2= 3;

explain (costs off )select * from db_1097155_tb force index (index_1097155_1, index_1097155_2, index_1097155_3, index_1097155_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b';

-- test use index can choose best index and seqscan
insert into db_1097155_tb select * from db_1097155_tb;
insert into db_1097155_tb select * from db_1097155_tb;
insert into db_1097155_tb select * from db_1097155_tb;
insert into db_1097155_tb select * from db_1097155_tb;
insert into db_1097155_tb select * from db_1097155_tb;
insert into db_1097155_tb select * from db_1097155_tb;

explain (costs off )select * from db_1097155_tb use index (index_1097155_1)  where col1 > 1;

explain (costs off )select * from db_1097155_tb use index (index_1097155_1)  where col1 < 1;

-- index not exists 
create table db_1097157_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_1097157_tb values(1,1,1,'a');
insert into db_1097157_tb values(1,2,2,'a');
insert into db_1097157_tb values(2,2,2,'a');
insert into db_1097157_tb values(2,2,3,'b');
insert into db_1097157_tb values(2,3,3,'b');
insert into db_1097157_tb values(3,3,4,'b');
insert into db_1097157_tb values(3,3,4,'a');
insert into db_1097157_tb values(3,4,5,'c');
insert into db_1097157_tb values(4,4,5,'c');
insert into db_1097157_tb values(4,null,1,'c');

create index index_1097157_1 on db_1097157_tb (col1);
create index index_1097157_2 on db_1097157_tb (col2);
create index index_1097157_3 on db_1097157_tb (col3);
create index index_1097157_4 on db_1097157_tb (col4);
analyze db_1097157_tb;

create table db_1097157_tb_1 as select * from db_1097157_tb;

create index index_1097157_5 on db_1097157_tb_1 (col1);

select * from db_1097157_tb use index (index_1097157_5) where col2= 3;

select * from db_1097157_tb force index (index_1097157_5) where col2= 3;

select * from db_1097157_tb use index (index_1097157_6) where col2= 3;

select * from db_1097157_tb force index (index_1097157_6) where col2= 3;

-- index_hint in group by 

create table db_ID1097254_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_ID1097254_tb values(1,1,1,'a');
insert into db_ID1097254_tb values(1,2,2,'a');
insert into db_ID1097254_tb values(2,2,2,'a');
insert into db_ID1097254_tb values(2,2,3,'b');
insert into db_ID1097254_tb values(2,3,3,'b');
insert into db_ID1097254_tb values(3,3,4,'b');
insert into db_ID1097254_tb values(3,3,4,'a');
insert into db_ID1097254_tb values(3,4,5,'c');
insert into db_ID1097254_tb values(4,4,5,'c');
insert into db_ID1097254_tb values(4,null,1,'c');

create index "Index_ID1097254%%_1" on db_ID1097254_tb (col1);
create index INDEX_ID1097254_2 on db_ID1097254_tb (col2);

analyze db_ID1097254_tb;


select max(t1.col2)+1,t2.col2 from db_ID1097254_tb t1 force index ("Index_ID1097254%%_1") join db_ID1097254_tb t2 force index (INDEX_ID1097254_2) on t1.col3=t2.col3 where t1.col1>= 3 and t2.col2 <=4 group by 2,t1.col2 having t1.col2 in (select max(col2) from db_ID1097254_tb force index ("Index_ID1097254%%_1") where col1>= 3) order by 1,2 ;

explain (costs off )select max(t1.col2)+1,t2.col2 from db_ID1097254_tb t1 force index ("Index_ID1097254%%_1") join db_ID1097254_tb t2 force index (INDEX_ID1097254_2) on t1.col3=t2.col3 where t1.col1>= 3 and t2.col2 <=4 group by 2,t1.col2 having t1.col2 in (select max(col2) from db_ID1097254_tb force index ("Index_ID1097254%%_1") where col1>= 3) order by 1,2 ;

--test first index exists ,second not exists

create table tb_ih_1 (a int, b int);
insert into tb_ih_1 values(1,2),(3,4),(5,6);
create index INDEX_tb1 on tb_ih_1 (a);
create index INDEX_tb2 on tb_ih_1 (a);
analyze tb_ih_1;
--can report
explain (costs off) select a from tb_ih_1 force key (INDEX_tb1,idex_t2);
--can plan
explain (costs off) select a from tb_ih_1 force key (INDEX_tb1,INDEX_tb2);

--partition
drop table if exists startend_pt;
CREATE TABLE startend_pt (c1 INT, c2 INT) 
PARTITION BY RANGE (c2) (
    PARTITION p1 START(1) END(1000),
    PARTITION p2 END(2000)
);

create index idx_startend on startend_pt(c2) local (partition  idxp1,partition idxp2,partition idxp3);
insert into startend_pt values(2,2),(3,3),(4,4),(5,5);
insert into startend_pt values(202,202),(203,203),(204,204),(1999,1999);
analyze startend_pt;

explain(costs off) select c2 from startend_pt partition (p2) force index (idx_startend) where c2 > 1998;
explain(costs off) select c2 from startend_pt partition for (200) force index (idx_startend) where c2 > 1998;
explain(costs off) select c2 from startend_pt partition (p2) where c2 > 1998;

drop table if exists list_list;
CREATE TABLE list_list
(
    month_code int  NOT NULL ,
    dept_code  int  NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY LIST  (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 values(100)
  (
    SUBPARTITION pa1 values less than (100),
	SUBPARTITION pa2 values less than (200)
  )
);
create index idx_list on list_list(dept_code) local ;
insert into list_list values('100', '2', '1', 1);
insert into list_list values('100', '1', '1', 1);
analyze list_list;
explain (costs off) select  user_no from list_list subpartition (pa1) use index (idx_list) where user_no = 1;
explain (costs off)select  user_no from list_list subpartition for (100,4) use index (idx_list) where user_no = 1;

--ignore index test
create table db_1130449_tb (col1 int ,col2 int,col3 int,col4 varchar(10),primary key(col1));
insert into db_1130449_tb values(1,1,1,'a');
insert into db_1130449_tb values(2,2,2,'a');
insert into db_1130449_tb values(3,3,2,'a');
insert into db_1130449_tb values(4,4,3,'b');
insert into db_1130449_tb values(5,5,3,'b');
insert into db_1130449_tb values(6,6,6,'b');
insert into db_1130449_tb values(7,7,7,'a');
insert into db_1130449_tb values(8,8,8,'c');
insert into db_1130449_tb values(9,9,9,'c');
insert into db_1130449_tb values(10,null,1,'c');

create unique index index_1130449 on db_1130449_tb (col2);
analyze db_1130449_tb;

select * from db_1130449_tb ignore index (index_1130449) where col2= 3;

select * from db_1130449_tb ignore index (index_1130449) where col2= 3 and col4 = 'a';

select * from db_1130449_tb IGNORE INDEX (index_1130449) where col2= 3;

explain (costs off )select * from db_1130449_tb ignore index (index_1130449) where col2= 3;

explain (costs off )select * from db_1130449_tb ignore index (index_1130449) where col2= 3 and col4 = 'a';

explain (costs off) select * from db_1130449_tb IGNORE INDEX (index_1130449) where col2= 3;

explain (costs off) select * from db_1130449_tb FORCE INDEX (index_1130449) IGNORE INDEX (index_1130449) where col2= 3;

--multi index ignored gram test
create table db_1130452_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_1130452_tb values(1,1,1,'a');
insert into db_1130452_tb values(1,2,2,'a');
insert into db_1130452_tb values(2,2,2,'a');
insert into db_1130452_tb values(2,2,3,'b');
insert into db_1130452_tb values(2,3,3,'b');
insert into db_1130452_tb values(3,3,4,'b');
insert into db_1130452_tb values(3,3,4,'a');
insert into db_1130452_tb values(3,4,5,'c');
insert into db_1130452_tb values(4,4,5,'c');
insert into db_1130452_tb values(4,null,1,'c');

create index index_1130452_1 on db_1130452_tb (col1);
create index index_1130452_2 on db_1130452_tb (col2);
create index index_1130452_3 on db_1130452_tb (col3);
create index index_1130452_4 on db_1130452_tb (col4);
analyze db_1130452_tb;

select * from db_1130452_tb ignore index (index_1130452_1) ignore index (index_1130452_2) where col2= 3 order by 1,2,3;

select * from db_1130452_tb ignore index (index_1130452_2) ignore index (index_1130452_1) where col2= 3 order by 1,2,3;

select * from db_1130452_tb ignore index (index_1130452_2) ignore index (index_1130452_2) where col2= 3 order by 1,2,3;

select * from db_1130452_tb ignore index (index_1130452_1) ignore index (index_1130452_2)ignore index (index_1130452_3) ignore index (index_1130452_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b' order by 1,2,3;

select * from db_1130452_tb ignore index (index_1130452_1, index_1130452_2) where col2= 3 order by 1,2,3;

select * from db_1130452_tb ignore index (index_1130452_2, index_1130452_1) where col2= 3 order by 1,2,3;

select * from db_1130452_tb ignore index (index_1130452_2, index_1130452_2) where col2= 3 order by 1,2,3;

select * from db_1130452_tb ignore index (index_1130452_1, index_1130452_2, index_1130452_3, index_1130452_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b' order by 1,2,3;

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_1) ignore index (index_1130452_2) where col2= 3;

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_2) ignore index (index_1130452_1) where col2= 3;

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_2) ignore index (index_1130452_2) where col2= 3;

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_1) ignore index (index_1130452_2)ignore index (index_1130452_3) ignore index (index_1130452_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b';

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_1 ,index_1130452_2) where col2= 3;

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_2 ,index_1130452_1) where col2= 3;

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_2, index_1130452_2) where col2= 3;

explain (costs off )select * from db_1130452_tb ignore index (index_1130452_1, index_1130452_2, index_1130452_3, index_1130452_4) where col2= 3 and col1 = 2 and col3 = 3 and col4='b';

--error report
create table db_1130456_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_1130456_tb values(1,1,1,'a');
insert into db_1130456_tb values(1,2,2,'a');
insert into db_1130456_tb values(2,2,2,'a');
insert into db_1130456_tb values(2,2,3,'b');
insert into db_1130456_tb values(2,3,3,'b');
insert into db_1130456_tb values(3,3,4,'b');
insert into db_1130456_tb values(3,3,4,'a');
insert into db_1130456_tb values(3,4,5,'c');
insert into db_1130456_tb values(4,4,5,'c');
insert into db_1130456_tb values(4,null,1,'c');

create index index_1130456_1 on db_1130456_tb (col1);
create index index_1130456_2 on db_1130456_tb (col2);
create index index_1130456_3 on db_1130456_tb (col3);
create index index_1130456_4 on db_1130456_tb (col4);
analyze db_1130456_tb;

create table db_1130456_tb_1 as select * from db_1130456_tb;

create index index_1130456_5 on db_1130456_tb_1 (col1);

select * from db_1130456_tb ignore index (index_1130456_5) where col2= 3;

select * from db_1130456_tb ignore index (index_1130456_6) where col2= 3; 

--in partition table 
CREATE TABLE db_1130473_tb
(
col1 INTEGER NOT NULL,
col2 INTEGER NOT NULL,
col3 INTEGER ,
CA_STREET_NAME VARCHAR(60) ,
CA_STREET_TYPE CHAR(15) ,
CA_SUITE_NUMBER CHAR(10) ,
CA_CITY VARCHAR(60) ,
CA_COUNTY VARCHAR(30) ,
CA_STATE CHAR(2) ,
CA_ZIP CHAR(10) ,
CA_COUNTRY VARCHAR(20) ,
CA_GMT_OFFSET DECIMAL(5,2) ,
CA_LOCATION_TYPE CHAR(20)
)
PARTITION BY list(col1)
(
partition p1 values (4),
partition p2 values (5),
partition p3 values (1),
partition p4 values (2),
partition p5 values (3));



CREATE INDEX ds_db_1130473_tb_index2 ON db_1130473_tb(col1) LOCAL ;
insert into db_1130473_tb select 1,v,v from generate_series(1,120) as v;
insert into db_1130473_tb select 2,v,v from generate_series(1,200) as v;
insert into db_1130473_tb select 3,v,v from generate_series(1,100) as v;
insert into db_1130473_tb select 4,v,v from generate_series(1,300) as v;
insert into db_1130473_tb select 5,v,v from generate_series(1,500) as v;

create index "Index_1130473%%_1" on db_1130473_tb (col1) local;
create index INDEX_1130473_2 on db_1130473_tb (col2);

analyze db_1130473_tb;

select max(col2)+1 from db_1130473_tb ignore index ("Index_1130473%%_1") where col1>= 3 ;

select max(col2)+1 from db_1130473_tb ignore index ("Index_1130473%%_1") ignore index (INDEX_1130473_2) where col2>= 3 and col1 >=3 ;

explain (costs off) select max(col2)+1 from db_1130473_tb ignore index ("Index_1130473%%_1") where col1>= 3 ;

explain (costs off) select max(col2)+1 from db_1130473_tb ignore index ("Index_1130473%%_1") ignore index (INDEX_1130473_2) where col2>= 3 and col1 >=3 ;

select max(col2)+1 from db_1130473_tb partition (p1) ignore index ("Index_1130473%%_1") where col1>= 3 ;

select max(col2)+1 from db_1130473_tb partition (p1) ignore index ("Index_1130473%%_1") ignore index (INDEX_1130473_2) where col2>= 3 and col1 >=3 ;

explain (costs off) select max(col2)+1 from db_1130473_tb partition (p1) ignore index ("Index_1130473%%_1") where col1>= 3 ;

explain (costs off) select max(col2)+1 from db_1130473_tb partition (p1) ignore index ("Index_1130473%%_1") ignore index (INDEX_1130473_2) where col2>= 3 and col1 >=3 ;

select max(col2)+1 from db_1130473_tb partition (p1) ignore index (ds_db_1130473_tb_index2) where col1>= 3 ;

select max(col2)+1 from db_1130473_tb partition (p1) ignore index (ds_db_1130473_tb_index2) ignore index (INDEX_1130473_2) where col2>= 3 and col1 >=3 ;

explain (costs off) select max(col2)+1 from db_1130473_tb partition (p1) ignore index (ds_db_1130473_tb_index2) where col1>= 3 ;

explain (costs off) select max(col2)+1 from db_1130473_tb partition (p1) ignore index (ds_db_1130473_tb_index2) ignore index (INDEX_1130473_2) where col2>= 3 and col1 >=3 ;


-- with scan hint 

create table db_1131004_tb (col1 int ,col2 int,col3 int,col4 varchar(10),primary key(col1));
insert into db_1131004_tb values(1,1,1,'a');
insert into db_1131004_tb values(2,2,2,'a');
insert into db_1131004_tb values(3,3,2,'a');
insert into db_1131004_tb values(4,4,3,'b');
insert into db_1131004_tb values(5,5,3,'b');
insert into db_1131004_tb values(6,6,6,'b');
insert into db_1131004_tb values(7,7,7,'a');
insert into db_1131004_tb values(8,8,8,'c');
insert into db_1131004_tb values(9,9,9,'c');
insert into db_1131004_tb values(10,null,1,'c');

create unique index index_1131004 on db_1131004_tb (col2);
analyze db_1131004_tb;

select /*+ indexscan(db_1131004_tb ) */* from db_1131004_tb ignore index (index_1131004) where col2= 3;

explain (costs off) select /*+ indexscan(db_1131004_tb ) */* from db_1131004_tb ignore index (index_1131004) where col2= 3;

select /*+ indexonlyscan(db_1131004_tb ) */col2 from db_1131004_tb ignore index (index_1131004) where col2= 3;

explain (costs off) select /*+ indexonlyscan(db_1131004_tb ) */col2 from db_1131004_tb ignore index (index_1131004) where col2= 3;


select /*+ indexscan(db_1131004_tb index_1131004 ) */* from db_1131004_tb ignore index (index_1131004) where col2= 3;

explain (costs off) select /*+ indexscan(db_1131004_tb index_1131004) */* from db_1131004_tb ignore index (index_1131004) where col2= 3;

select /*+ indexonlyscan(db_1131004_tb index_1131004) */col2 from db_1131004_tb ignore index (index_1131004) where col2= 3;

explain (costs off) select /*+ indexonlyscan(db_1131004_tb index_1131004) */col2 from db_1131004_tb ignore index (index_1131004) where col2= 3;


\c postgres
DROP DATABASE IF EXISTS db_1097149;

DROP DATABASE IF EXISTS db_ID1097168;
CREATE DATABASE db_ID1097168 DBCOMPATIBILITY 'A';
\c db_ID1097168

-- not support in a database 

create table db_ID1097168_tb (col1 int ,col2 int,col3 int,col4 varchar(10));
insert into db_ID1097168_tb values(1,1,1,'a');
insert into db_ID1097168_tb values(1,2,2,'a');
insert into db_ID1097168_tb values(2,2,2,'a');
insert into db_ID1097168_tb values(2,2,3,'b');
insert into db_ID1097168_tb values(2,3,3,'b');
insert into db_ID1097168_tb values(3,3,4,'b');
insert into db_ID1097168_tb values(3,3,4,'a');
insert into db_ID1097168_tb values(3,4,5,'c');
insert into db_ID1097168_tb values(4,4,5,'c');
insert into db_ID1097168_tb values(4,null,1,'c');

create index "Index_ID1097168%%_1" on db_ID1097168_tb (col1);
create index INDEX_ID1097168_2 on db_ID1097168_tb (col2);
create index index_ID1097168_3 on db_ID1097168_tb (col3);

analyze db_ID1097168_tb;


select max(col2)+1 from db_ID1097168_tb force index ("Index_ID1097168%%_1") where col2>= 3 ;

\c postgres
DROP DATABASE IF EXISTS db_ID1097168;
