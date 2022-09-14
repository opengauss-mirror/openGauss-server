--prepare
DROP SCHEMA ustore_subpartition_split CASCADE;
CREATE SCHEMA ustore_subpartition_split;
SET CURRENT_SCHEMA TO ustore_subpartition_split;

--split subpartition
-- list subpartition
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( default )
  )
);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201902', '2', '1', 1);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201903', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
insert into list_list values('201903', '3', '1', 1);
select * from list_list order by 1,2,3,4;

select * from list_list subpartition (p_201901_a) order by 1,2,3,4;
select * from list_list subpartition (p_201901_b) order by 1,2,3,4;
alter table list_list split subpartition p_201901_b values (2) into
(
	subpartition p_201901_b,
	subpartition p_201901_c
);
select * from list_list subpartition (p_201901_a) order by 1,2,3,4;
select * from list_list subpartition (p_201901_b) order by 1,2,3,4;
select * from list_list subpartition (p_201901_c) order by 1,2,3,4;

select * from list_list partition (p_201901);

select * from list_list subpartition (p_201902_a) order by 1,2,3,4;
select * from list_list subpartition (p_201902_b) order by 1,2,3,4;
alter table list_list split subpartition p_201902_b values (2, 3) into
(
	subpartition p_201902_b,
	subpartition p_201902_c
);
select * from list_list subpartition (p_201902_a) order by 1,2,3,4;
select * from list_list subpartition (p_201902_b) order by 1,2,3,4;
select * from list_list subpartition (p_201902_c) order by 1,2,3,4;

--error
alter table list_list split subpartition p_201902_a values (3) into
(
	subpartition p_201902_ab,
	subpartition p_201902_ac
);

drop table list_list;

-- range subpartition
CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( MAXVALUE )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '6' )
  )
);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('201902', '2', '1', 1);
insert into range_range values('201902', '3', '1', 1);
insert into range_range values('201903', '1', '1', 1);
insert into range_range values('201903', '2', '1', 1);
insert into range_range values('201903', '5', '1', 1);
select * from range_range order by 1,2,3,4;

select * from range_range subpartition (p_201901_a) order by 1,2,3,4;
select * from range_range subpartition (p_201901_b) order by 1,2,3,4;
alter table range_range split subpartition p_201901_b at (3) into
(
	subpartition p_201901_c,
	subpartition p_201901_d
);
select * from range_range subpartition (p_201901_a) order by 1,2,3,4;
select * from range_range subpartition (p_201901_b) order by 1,2,3,4;
select * from range_range subpartition (p_201901_c) order by 1,2,3,4;
select * from range_range subpartition (p_201901_d) order by 1,2,3,4;

select * from range_range subpartition (p_201902_a) order by 1,2,3,4;
select * from range_range subpartition (p_201902_b) order by 1,2,3,4;
alter table range_range split subpartition p_201902_b at (3) into
(
	subpartition p_201902_c,
	subpartition p_201902_d
);
select * from range_range subpartition (p_201902_a) order by 1,2,3,4;
select * from range_range subpartition (p_201902_b) order by 1,2,3,4;
select * from range_range subpartition (p_201902_c) order by 1,2,3,4;
select * from range_range subpartition (p_201902_d) order by 1,2,3,4;

drop table range_range;
--test syntax
CREATE TABLE IF NOT EXISTS list_hash
(
    col_1 int ,
    col_2 int ,
    col_3 int ,
    col_4 int
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY list (col_1) SUBPARTITION BY hash (col_2)
(
  PARTITION p_list_1 VALUES (-1,-2,-3,-4,-5,-6,-7,-8,-9,-10 )
  (
    SUBPARTITION p_hash_1_1 ,
    SUBPARTITION p_hash_1_2 ,
    SUBPARTITION p_hash_1_3
  ),
  PARTITION p_list_2 VALUES (1,2,3,4,5,6,7,8,9,10 )
  (
    SUBPARTITION p_hash_2_1 ,
    SUBPARTITION p_hash_2_2 ,
    SUBPARTITION p_hash_2_3 ,
    SUBPARTITION p_hash_2_4 ,
    SUBPARTITION p_hash_2_5
  ),
  PARTITION p_list_3 VALUES (11,12,13,14,15,16,17,18,19,20),
  PARTITION p_list_4 VALUES (21,22,23,24,25,26,27,28,29,30 )
  (
    SUBPARTITION p_hash_4_1
  ),
  PARTITION p_list_5 VALUES (default)
  (
    SUBPARTITION p_hash_5_1
  ),
  PARTITION p_list_6 VALUES (31,32,33,34,35,36,37,38,39,40)
  (
    SUBPARTITION p_hash_6_1 ,
    SUBPARTITION p_hash_6_2 ,
    SUBPARTITION p_hash_6_3
  )
) ENABLE ROW MOVEMENT ;

alter table list_hash split subPARTITION p_hash_2_3 at(-10) into ( subPARTITION add_p_01 , subPARTITION add_p_02 );

drop table list_hash;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( MAXVALUE )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '6' )
  )
);
alter table range_range split subpartition p_201901_b values (3) into
(
	subpartition p_201901_c,
	subpartition p_201901_d
) update global index;
drop table range_range;

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( default )
  )
);
alter table list_list split subpartition p_201901_b at (2, 3) into
(
	subpartition p_201901_b,
	subpartition p_201901_c
);
drop table list_list;

CREATE   TABLE IF NOT EXISTS list_list_02
(
    col_1 int ,
    col_2 int  ,
    col_3 int ,
    col_4 int
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY list (col_1) SUBPARTITION BY list (col_2)
(
  PARTITION p_list_1 VALUES(-1,-2,-3,-4,-5,-6,-7,-8,-9,-10 )
  (
    SUBPARTITION p_list_1_1 VALUES ( 0,-1,-2,-3,-4,-5,-6,-7,-8,-9 ),
    SUBPARTITION p_list_1_2 VALUES ( default )
  ),
  PARTITION p_list_2 VALUES(0,1,2,3,4,5,6,7,8,9)
  (
    SUBPARTITION p_list_2_1 VALUES ( 0,1,2,3,4,5,6,7,8,9 ),
    SUBPARTITION p_list_2_2 VALUES ( default ),
    SUBPARTITION p_list_2_3 VALUES ( 10,11,12,13,14,15,16,17,18,19),
    SUBPARTITION p_list_2_4 VALUES ( 20,21,22,23,24,25,26,27,28,29 ),
    SUBPARTITION p_list_2_5 VALUES ( 30,31,32,33,34,35,36,37,38,39 )
  ),
  PARTITION p_list_3 VALUES(10,11,12,13,14,15,16,17,18,19)
  (
    SUBPARTITION p_list_3_2 VALUES ( default )
  ),
  PARTITION p_list_4 VALUES(default ),
  PARTITION p_list_5 VALUES(20,21,22,23,24,25,26,27,28,29)
  (
    SUBPARTITION p_list_5_1 VALUES ( 0,1,2,3,4,5,6,7,8,9 ),
    SUBPARTITION p_list_5_2 VALUES ( default ),
    SUBPARTITION p_list_5_3 VALUES ( 10,11,12,13,14,15,16,17,18,19),
    SUBPARTITION p_list_5_4 VALUES ( 20,21,22,23,24,25,26,27,28,29 ),
    SUBPARTITION p_list_5_5 VALUES ( 30,31,32,33,34,35,36,37,38,39 )
  ),
  PARTITION p_list_6 VALUES(30,31,32,33,34,35,36,37,38,39),
  PARTITION p_list_7 VALUES(40,41,42,43,44,45,46,47,48,49)
  (
    SUBPARTITION p_list_7_1 VALUES ( default )
  )
) ENABLE ROW MOVEMENT;

alter table list_list_02 split PARTITION for (5) at (8) into ( PARTITION add_p_01 , PARTITION add_p_02 );
drop table list_list_02;

drop table if exists col_com_alt_tbl_019_hash_range;
create table col_com_alt_tbl_019_hash_range (
col_tinyint		     tinyint,
col_smallint	     smallint,
col_int			     integer,
col_bigint		     bigint,
col_money            money,
col_numeric		     numeric,
col_real		     real,
col_double		     double precision,
col_decimal          decimal,
col_char		     char(30),
col_timestamp		 timestamp with time zone,
col_timestamptz		 timestamp without time zone,
col_date		     date,
col_time		     time without time zone,
col_timetz		     time with time zone,
col_smalldatetime	 smalldatetime,
col_boolean			 boolean,
col_varchar  	     varchar,
col_nvarchar2	     nvarchar2,
col_text		     text,
col_interval	     interval,
col_bytea			 bytea,
col_bit				 bit(20),
col_varbit			 varbit(10),
col_inet			 inet,
col_cidr			 cidr
) with (storage_type = ustore)
partition by hash(col_tinyint) 
subpartition by range (col_smallint)
(
	partition p1 
    (
      subpartition p1_1 values less than(20),
      subpartition p1_2 values less than(maxvalue)
    ),
    partition p2 ,
    partition p3 ,
    partition p54 ,
    partition p55 ,
    partition p56 ,
    partition p57
    (
      subpartition p57_1 values less than(50),
      subpartition p57_2 values less than(maxvalue)
    ) ,
    partition p58 ,
    partition p59 ,
    partition p60 ,
    partition p61 ,
    partition p62 ,
    partition p63 ,
    partition pd 
) enable row movement;

insert into col_com_alt_tbl_019_hash_range values(generate_series(1,100),generate_series(1,100));

begin;
alter table col_com_alt_tbl_019_hash_range split subpartition p1_2 at (40) into (subpartition p1_2_x, subpartition p1_2_y);
rollback;

drop table if exists col_com_alt_tbl_019_hash_range;

--clean
DROP SCHEMA ustore_subpartition_split CASCADE;
RESET CURRENT_SCHEMA;
