----
--- Create Talbe
----
create schema vector_window_engine;
set current_schema=vector_window_engine;

create table vector_window_engine.VECTOR_WINDOW_TABLE_01(
   depname	varchar  
  ,empno	bigint  
  ,salary	int  
  ,enroll	date  
  ,timeset	timetz
)with(orientation =column);

COPY vector_window_engine.VECTOR_WINDOW_TABLE_01(depname, empno, salary, enroll, timeset) FROM stdin;
develop	10	5200	2007-08-01	16:00:00+08
develop	2	5200	2007-08-01	16:00:00+08
sales	1	5000	2006-10-01	16:30:00+08
personnel	5	3500	2007-12-10	16:30:00+08
sales	4	4800	2007-08-08	08:00:00+08
develop	7	4200	2009-01-01	08:00:00+06
personnel	2	3900	2006-12-23	08:30:00+06
develop	7	4200	2008-01-01	16:00:00+08
develop	9	4500	2008-01-01	16:30:00+08
sales	3	4800	2007-08-01	16:40:00+08
develop	8	6000	2006-10-01	08:30:00+06
develop	11	5200	2007-08-15	08:30:00+06
develop	5	\N	2007-08-15	\N
develop	6	\N	\N	08:30:00+06
\N	\N	\N	\N	\N
\.

create table vector_window_engine.ROW_WINDOW_TABLE_02
(
   depname	varchar
  ,salary	int
  ,enroll	date
);

create table vector_window_engine.VECTOR_WINDOW_TABLE_02
(
   depname	varchar
  ,salary	int
  ,enroll	date
)with(orientation =column);

INSERT INTO vector_window_engine.row_window_table_02 select 'develop',4200,'2007-08-08';
INSERT INTO vector_window_engine.row_window_table_02 select 'personnel', 3900, '2008-08-01';
INSERT INTO vector_window_engine.row_window_table_02 select 'sales',6000,'2009-09-02';

insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;
insert into vector_window_engine.row_window_table_02 select * from vector_window_engine.row_window_table_02;

insert into vector_window_engine.row_window_table_02 values('develop',3800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',3800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',4800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',5800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',5800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('personnel',4800,'2008-08-01');
insert into vector_window_engine.row_window_table_02 values('sales',4800,'2009-09-02');
insert into vector_window_engine.row_window_table_02 values('sales',6800,'2009-09-02');
insert into vector_window_table_02 select * from row_window_table_02;

create table vector_window_engine.ROW_WINDOW_TABLE_03
(
   depname	varchar
  ,salary	int
  ,enroll	date
);

create table vector_window_engine.VECTOR_WINDOW_TABLE_03
(
   depname	varchar
  ,salary	int
  ,enroll	date
)with(orientation =column);

create table tmp_tt_1(depname varchar, enroll date);
insert into tmp_tt_1 values ('develop', '2007-08-08');

INSERT INTO vector_window_engine.row_window_table_03 select 'develop', generate_series(4200, 5000), '2007-08-08' from tmp_tt_1;
INSERT INTO vector_window_engine.row_window_table_03 VALUES('sales',6000,'2009-09-02');
INSERT INTO vector_window_engine.row_window_table_03 VALUES('sales',6000,'2009-09-02');
INSERT INTO vector_window_engine.row_window_table_03 VALUES('sales',6000,'2009-09-02');
INSERT INTO vector_window_engine.row_window_table_03 VALUES('sales',6000,'2009-09-02');
INSERT INTO vector_window_engine.row_window_table_03 VALUES('sales',6000,'2009-09-02');
INSERT INTO vector_window_engine.row_window_table_03 VALUES('sales',6000,'2009-09-02');

insert into vector_window_table_03 select * from row_window_table_03;

create table vector_window_engine.VECTOR_WINDOW_TABLE_04
(
   depid	int
  ,salary	int 
, partial cluster key(depid))with (orientation=column)
partition by range (depid)
(
  partition win_tab_hash_1 values less than (5),
  partition win_tab_hash_2 values less than (10),
  partition win_tab_hash_3 values less than (15)
);

insert into vector_window_table_04 select generate_series(0,10), generate_series(10,20);

create table vector_window_engine.ROW_WINDOW_TABLE_05
(
    a	int
   ,b	int
);

create table vector_window_engine.VECTOR_WINDOW_TABLE_05
(
    a	int
   ,b	int
)with(orientation=column)  ;

INSERT INTO vector_window_engine.row_window_table_05 values (1,1);

INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;
INSERT INTO vector_window_engine.row_window_table_05 select * from vector_window_engine.row_window_table_05;

insert into vector_window_table_05 select * from row_window_table_05;
insert into vector_window_table_05 values (1, 2);

create table vector_window_engine.vector_window_table_06
(
    col_int	int
   ,col_char	char(10)
   ,col_timetz	timetz
   ,col_interval	interval
   ,col_tinterval	tinterval
)with(orientation = column)  ;

copy vector_window_engine.vector_window_table_06(col_int, col_char, col_timetz, col_interval, col_tinterval) FROM stdin;
1	beijing	12:40:00+06	1 day 12:04:08	["Sep 5, 1983 23:59:12" "Oct 6, 1983 23:59:12"]
2	tianjing	12:40:00+06	2 day 12:04:08	["Sep 5, 1983 23:59:12" "Oct 6, 1983 23:59:12"]
1	beijing	10:40:00+08	1 day 12:04:08	["Sep 5, 1983 23:59:12" "Oct 6, 1987 23:59:12"]
2	tianjing	10:40:00+08	2 day 12:04:08	["Sep 5, 1983 23:59:12" "Oct 6, 1987 23:59:12"]
1	beijing	\N	3 day 12:04:08	["Sep 5, 1983 23:59:12" "Oct 6, 1983 23:59:12"]
1	shenzhen	10:40:00+08	1 day 12:04:08	["Sep 5, 1983 23:59:12" "Oct 6, 1987 23:59:12"]
2	shenzhen	12:40:00+08	1 day 12:04:08	["Sep 5, 1983 23:59:12" "Oct 6, 1987 23:59:12"]
\.


create table vector_window_engine.vector_window_table_07
(
    c1 timestamp, 
    c2 timestamp,
    c3 int,
    c4 int,
    c5 numeric,
    c6 int
) with(orientation=column)  ;

insert into vector_window_table_07 values('2018-06-20','2018-06-21',600090,1,2,1);
insert into vector_window_table_07 values('2018-06-20','2018-06-21',600090,1,4,2);
insert into vector_window_table_07 values('2018-06-20','2018-06-21',600090,2,2,generate_series(3,1001));
insert into vector_window_table_07 values('2018-06-20','2018-06-21',600090,3,6,2003);
insert into vector_window_table_07 values('2018-06-20','2018-06-21',600090,3,6,2003);
insert into vector_window_table_07 values('2018-06-20','2018-06-21',600090,4,6,2003);
insert into vector_window_table_07 values('2018-06-20','2018-06-21',600090,5,6,2003);

analyze vector_window_table_01;
analyze vector_window_table_02;
analyze vector_window_table_03;
analyze vector_window_table_04;
analyze vector_window_table_05;
analyze vector_window_table_06;
analyze vector_window_table_07;
