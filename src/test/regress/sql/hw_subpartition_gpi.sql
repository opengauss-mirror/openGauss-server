-- prepare
DROP SCHEMA subpartition_gpi CASCADE;
CREATE SCHEMA subpartition_gpi;
SET CURRENT_SCHEMA TO subpartition_gpi;

-- base function
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

create index idx_month_code_local on range_list(month_code) local;
create index idx_dept_code_global on range_list(dept_code) global;
create index idx_user_no_global on range_list(user_no) global;

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '2', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select * from range_list order by 1, 2, 3, 4;

set enable_seqscan = off;
explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

drop table range_list;

-- unique
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

create unique index idx_dept_code_global on range_list(dept_code) global;

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
select * from range_list subpartition (p_201901_a);
select * from range_list subpartition (p_201901_b);
select count(*) from range_list;
--error
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select count(*) from range_list;

delete from range_list;
drop index idx_dept_code_global;

create unique index idx_user_no_global on range_list(user_no) global;
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '2', 1);
insert into range_list values('201903', '1', '3', 1);
insert into range_list values('201903', '2', '4', 1);
select * from range_list subpartition (p_201901_a);
select * from range_list subpartition (p_201901_b);
select * from range_list subpartition (p_201902_a);
select * from range_list subpartition (p_201902_b);
select count(*) from range_list;
--error
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201902', '1', '2', 1);
insert into range_list values('201902', '2', '2', 1);
insert into range_list values('201903', '1', '2', 1);
insert into range_list values('201903', '2', '2', 1);
insert into range_list values('201902', '1', '3', 1);
insert into range_list values('201902', '2', '3', 1);
insert into range_list values('201903', '1', '3', 1);
insert into range_list values('201903', '2', '3', 1);
insert into range_list values('201902', '1', '4', 1);
insert into range_list values('201902', '2', '4', 1);
insert into range_list values('201903', '1', '4', 1);
insert into range_list values('201903', '2', '4', 1);
select count(*) from range_list;

drop table range_list;

-- truncate subpartition
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

create index idx_month_code_local on range_list(month_code) local;
create index idx_dept_code_global on range_list(dept_code) global;
create index idx_user_no_global on range_list(user_no) global;

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '2', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select * from range_list order by 1, 2, 3, 4;

set enable_seqscan = off;
explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

alter table range_list truncate subpartition p_201901_a update global index;

explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

alter table range_list truncate subpartition p_201901_b;

explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

drop table range_list;

-- split subpartition
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values (default)
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values (default)
  )
);

create index idx_month_code_local on range_list(month_code) local;
create index idx_dept_code_global on range_list(dept_code) global;
create index idx_user_no_global on range_list(user_no) global;

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '2', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select * from range_list order by 1, 2, 3, 4;

set enable_seqscan = off;
explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

alter table range_list split subpartition p_201901_b values ('3') into
(
	subpartition p_201901_b,
	subpartition p_201901_c
) update global index;

explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

alter table range_list split subpartition p_201902_b values ('3') into
(
	subpartition p_201902_b,
	subpartition p_201902_c
);

explain(costs off, verbose on) select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_list where user_no = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;

drop table range_list;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
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
    SUBPARTITION p_201902_b VALUES LESS THAN( MAXVALUE )
  )
);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('201902', '2', '1', 1);
insert into range_range values('201902', '3', '1', 1);
insert into range_range values('201903', '1', '1', 1);
insert into range_range values('201903', '2', '1', 1);
insert into range_range values('201903', '5', '1', 1);
select * from range_range;

create index idx_month_code_local on range_range(month_code) local;
create index idx_dept_code_global on range_range(dept_code) global;
create index idx_user_no_global on range_range(user_no) global;

set enable_seqscan = off;
explain(costs off, verbose on) select * from range_range where month_code = '201902' order by 1, 2, 3, 4;
select * from range_range where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_range where dept_code = '1' order by 1, 2, 3, 4;
select * from range_range where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_range where user_no = '1' order by 1, 2, 3, 4;
select * from range_range where user_no = '1' order by 1, 2, 3, 4;

alter table range_range split subpartition p_201901_b at ('3') into
(
	subpartition p_201901_c,
	subpartition p_201901_d
) update global index;

explain(costs off, verbose on) select * from range_range where month_code = '201902' order by 1, 2, 3, 4;
select * from range_range where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_range where dept_code = '1' order by 1, 2, 3, 4;
select * from range_range where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_range where user_no = '1' order by 1, 2, 3, 4;
select * from range_range where user_no = '1' order by 1, 2, 3, 4;

alter table range_range split subpartition p_201902_b at ('3') into
(
	subpartition p_201902_c,
	subpartition p_201903_d
);

explain(costs off, verbose on) select * from range_range where month_code = '201902' order by 1, 2, 3, 4;
select * from range_range where month_code = '201902' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_range where dept_code = '1' order by 1, 2, 3, 4;
select * from range_range where dept_code = '1' order by 1, 2, 3, 4;

explain(costs off, verbose on) select * from range_range where user_no = '1' order by 1, 2, 3, 4;
select * from range_range where user_no = '1' order by 1, 2, 3, 4;

drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) primary key,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int
)
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
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
select relkind from pg_class
  where relname = 'range_range_pkey'
  and relnamespace=(select oid from pg_namespace where nspname=CURRENT_SCHEMA);
drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) primary key,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int
)
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
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
select relkind from pg_class
  where relname = 'range_range_pkey'
  and relnamespace=(select oid from pg_namespace where nspname=CURRENT_SCHEMA);
drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) primary key,
    sales_amt  int
)
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
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
select relkind from pg_class
  where relname = 'range_range_pkey'
  and relnamespace=(select oid from pg_namespace where nspname=CURRENT_SCHEMA);
drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int,
	primary key(month_code, dept_code)
)
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
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
select relkind from pg_class
  where relname = 'range_range_pkey'
  and relnamespace=(select oid from pg_namespace where nspname=CURRENT_SCHEMA);
drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int,
	primary key(month_code, dept_code, user_no)
)
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
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
select relkind from pg_class
  where relname = 'range_range_pkey'
  and relnamespace=(select oid from pg_namespace where nspname=CURRENT_SCHEMA);
drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int,
	primary key(month_code, user_no)
)
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
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
select relkind from pg_class
  where relname = 'range_range_pkey'
  and relnamespace=(select oid from pg_namespace where nspname=CURRENT_SCHEMA);
drop table range_range;

-- truncate with gpi
CREATE TABLE range_hash_02
(
	col_1 int ,
	col_2 int,
	col_3 VARCHAR2 ( 30 ) ,
	col_4 int
)
PARTITION BY RANGE (col_1) SUBPARTITION BY hash (col_2)
(
 PARTITION p_range_1 VALUES LESS THAN( -10 )
 (
	SUBPARTITION p_hash_1_1 ,
	SUBPARTITION p_hash_1_2 ,
	SUBPARTITION p_hash_1_3
 ),
 PARTITION p_range_2 VALUES LESS THAN( 20 ),
 PARTITION p_range_3 VALUES LESS THAN( 30)
 (
	SUBPARTITION p_hash_3_1 ,
	SUBPARTITION p_hash_3_2 ,
	SUBPARTITION p_hash_3_3
 ),
   PARTITION p_range_4 VALUES LESS THAN( 50)
 (
	SUBPARTITION p_hash_4_1 ,
	SUBPARTITION p_hash_4_2 ,
	SUBPARTITION range_hash_02
 ),
  PARTITION p_range_5 VALUES LESS THAN( MAXVALUE )
) ENABLE ROW MOVEMENT;

create index idx on range_hash_02(col_1);

truncate range_hash_02;

drop table range_hash_02;

-- clean
DROP SCHEMA subpartition_gpi CASCADE;

