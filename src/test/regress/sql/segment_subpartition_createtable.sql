
--1.create table 
--list_list list_hash list_range range_list range_hash range_range

--prepare
DROP SCHEMA segment_subpartition_createtable CASCADE;
CREATE SCHEMA segment_subpartition_createtable;
SET CURRENT_SCHEMA TO segment_subpartition_createtable;

--1.1 normal table
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201902', '2', '1', 1);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
insert into list_list values('201903', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
select * from list_list;
drop table list_list;

CREATE TABLE list_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
insert into list_hash values('201902', '1', '1', 1);
insert into list_hash values('201902', '2', '1', 1);
insert into list_hash values('201902', '3', '1', 1);
insert into list_hash values('201903', '4', '1', 1);
insert into list_hash values('201903', '5', '1', 1);
insert into list_hash values('201903', '6', '1', 1);
select * from list_hash;
drop table list_hash;

CREATE TABLE list_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a values less than ('4'),
    SUBPARTITION p_201901_b values less than ('6')
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a values less than ('3'),
    SUBPARTITION p_201902_b values less than ('6')
  )
);
insert into list_range values('201902', '1', '1', 1);
insert into list_range values('201902', '2', '1', 1);
insert into list_range values('201902', '3', '1', 1);
insert into list_range values('201903', '4', '1', 1);
insert into list_range values('201903', '5', '1', 1);
insert into list_range values('201903', '6', '1', 1);

select * from list_range;
drop table list_range;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);

select * from range_list;
drop table range_list;

CREATE TABLE range_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
insert into range_hash values('201902', '1', '1', 1);
insert into range_hash values('201902', '2', '1', 1);
insert into range_hash values('201902', '1', '1', 1);
insert into range_hash values('201903', '2', '1', 1);
insert into range_hash values('201903', '1', '1', 1);
insert into range_hash values('201903', '2', '1', 1);

select * from range_hash;
drop table range_hash;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('201902', '2', '1', 1);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('201903', '2', '1', 1);
insert into range_range values('201903', '1', '1', 1);
insert into range_range values('201903', '2', '1', 1);

select * from range_range;
drop table range_range;

CREATE TABLE hash_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY hash (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into hash_list values('201901', '1', '1', 1);
insert into hash_list values('201901', '2', '1', 1);
insert into hash_list values('201901', '1', '1', 1);
insert into hash_list values('201903', '2', '1', 1);
insert into hash_list values('201903', '1', '1', 1);
insert into hash_list values('201903', '2', '1', 1);

select * from hash_list;
drop table hash_list;

CREATE TABLE hash_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY hash (month_code) SUBPARTITION BY hash (dept_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
insert into hash_hash values('201901', '1', '1', 1);
insert into hash_hash values('201901', '2', '1', 1);
insert into hash_hash values('201901', '1', '1', 1);
insert into hash_hash values('201903', '2', '1', 1);
insert into hash_hash values('201903', '1', '1', 1);
insert into hash_hash values('201903', '2', '1', 1);

select * from hash_hash;
drop table hash_hash;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY hash (month_code) SUBPARTITION BY range (dept_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a VALUES LESS THAN ( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN ( '3' )
  ),
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a VALUES LESS THAN ( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN ( '3' )
  )
);
insert into hash_range values('201901', '1', '1', 1);
insert into hash_range values('201901', '2', '1', 1);
insert into hash_range values('201901', '1', '1', 1);
insert into hash_range values('201903', '2', '1', 1);
insert into hash_range values('201903', '1', '1', 1);
insert into hash_range values('201903', '2', '1', 1);

select * from hash_range;
drop table hash_range;


--1.2 table with default subpartition
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
drop table list_list;

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
);
drop table list_list;

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' ),
  PARTITION p_201902 VALUES ( '201903' )
);
drop table list_list;


CREATE TABLE list_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
drop table list_hash;

CREATE TABLE list_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902 VALUES ( '201903' )
);
drop table list_hash;

CREATE TABLE list_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' ),
  PARTITION p_201902 VALUES ( '201903' )
);
drop table list_hash;

CREATE TABLE list_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a values less than ('3'),
    SUBPARTITION p_201902_b values less than ('6')
  )
);
drop table list_range;

CREATE TABLE list_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a values less than ('4'),
    SUBPARTITION p_201901_b values less than ('6')
  ),
  PARTITION p_201902 VALUES ( '201903' )
);
drop table list_range;

CREATE TABLE list_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' ),
  PARTITION p_201902 VALUES ( '201903' )
);
drop table list_range;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
drop table range_list;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
);
drop table range_list;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
);
drop table range_list;

CREATE TABLE range_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
drop table range_hash;

CREATE TABLE range_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
);
drop table range_hash;

CREATE TABLE range_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
);
drop table range_hash;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
);
drop table range_range;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
);
drop table range_range;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901,
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
drop table hash_range;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
  ),
  PARTITION p_201902
);
drop table hash_range;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901,
  PARTITION p_201902
);
drop table hash_range;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901,
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a VALUES( '2' ),
    SUBPARTITION p_201902_b VALUES( '3' )
  )
);
drop table hash_range;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a VALUES( '2' ),
    SUBPARTITION p_201901_b VALUES( '3' )
  ),
  PARTITION p_201902
);
drop table hash_range;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901,
  PARTITION p_201902
);
drop table hash_range;

CREATE TABLE hash_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901,
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
drop table hash_hash;

CREATE TABLE hash_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902
);
drop table hash_hash;

CREATE TABLE hash_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY HASH (month_code) SUBPARTITION BY HASH (dept_code)
(
  PARTITION p_201901,
  PARTITION p_201902
);
drop table hash_hash;


--1.3 subpartition name check
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_a VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901 VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201901_subpartdefault1 VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
drop table list_list;



--1.4 subpartition key check
--二级分区的键值一样

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '1' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);

--分区列不存在
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_codeXXX) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_codeXXX)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);


CREATE TABLE list_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a values less than ('4'),
    SUBPARTITION p_201901_b values less than ('4')
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a values less than ('3'),
    SUBPARTITION p_201902_b values less than ('6')
  )
);
drop table list_range;


--1.5 list subpartition whith default 

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( default )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201902', '2', '1', 1);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
insert into list_list values('201903', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
select  * from list_list partition (p_201901);
select  * from list_list partition (p_201902);
drop table list_list;

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
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
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
drop table list_list;

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
drop table list_list;

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( default )
  )
);
drop table list_list;

--1.6  declaration and definition of the subpatiiton type are same.
--error
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY hash (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( default )
  )
);

--1.7  add constraint
CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
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

alter table range_range  add constraint constraint_check CHECK (sales_amt IS NOT NULL);
insert into range_range values(1,1,1);
drop table range_range;

-- drop partition column
CREATE TABLE range_hash_02
(
	col_1 int ,
	col_2 int,
	col_3 VARCHAR2 ( 30 ) ,
	col_4 int
) WITH (SEGMENT=ON)
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

alter table range_hash_02 drop column col_1;

alter table range_hash_02 drop column col_2;

drop table range_hash_02;
--1.8 SET ROW MOVEMENT
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1', '2' ),
    SUBPARTITION p_201901_b VALUES ( default )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1', '2' ),
    SUBPARTITION p_201902_b VALUES ( default )
  )
);
alter table list_list disable ROW MOVEMENT;
insert into list_list values('201902', '1', '1', 1);
update list_list set month_code = '201903';
update list_list set dept_code = '3';
alter table list_list enable ROW MOVEMENT;
update list_list set month_code = '201903';
update list_list set dept_code = '3';
drop table list_list;

--1.9 without subpartition declaration
create table test(a int) WITH (SEGMENT=ON)
partition by range(a)
(
partition p1 values less than(100)
(
subpartition subp1 values less than(50),
subpartition subp2 values less than(100)
),
partition p2 values less than(200),
partition p3 values less than(maxvalue)
);

--1.10 create table like
CREATE TABLE range_range
(
    col_1 int primary key,
    col_2 int NOT NULL ,
    col_3 VARCHAR2 ( 30 ) NOT NULL ,
    col_4 int generated always as(2*col_2) stored ,
	check (col_4 >= col_2)
) WITH (SEGMENT=ON)
PARTITION BY RANGE (col_1) SUBPARTITION BY RANGE (col_2)
(
  PARTITION p_range_1 VALUES LESS THAN( 10 )
  (
    SUBPARTITION p_range_1_1 VALUES LESS THAN( 5 ),
    SUBPARTITION p_range_1_2 VALUES LESS THAN( MAXVALUE )
  ),
  PARTITION p_range_2 VALUES LESS THAN( 20 )
  (
    SUBPARTITION p_range_2_1 VALUES LESS THAN( 5 ),
    SUBPARTITION p_range_2_2 VALUES LESS THAN( 10 )
  )
) ENABLE ROW MOVEMENT;

CREATE TABLE range_range_02 (like range_range INCLUDING ALL );
drop table range_range;

--ROW LEVEL SECURITY POLICY
CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int,
	primary key(month_code, dept_code)
) WITH (SEGMENT=ON)
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
CREATE ROW LEVEL SECURITY POLICY range_range_rls ON range_range USING(user_no = CURRENT_USER);

drop table range_range;

-- 账本数据库
CREATE SCHEMA ledgernsp WITH BLOCKCHAIN;

CREATE TABLE ledgernsp.range_range
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int,
	primary key(month_code, dept_code)
) WITH (SEGMENT=ON)
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

DROP SCHEMA ledgernsp;
-- create table as
CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
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
) ENABLE ROW MOVEMENT;

insert into range_range values(201902,1,1,1),(201902,1,1,1),(201902,3,1,1),(201903,1,1,1),(201903,2,1,1),(201903,2,1,1);

select * from range_range subpartition(p_201901_a) where month_code in(201902,201903) order by 1,2,3,4;

create table range_range_copy WITH (SEGMENT=ON) as select * from range_range subpartition(p_201901_a) where month_code in(201902,201903);

select * from range_range_copy order by 1,2,3,4;

drop table range_range;
drop table range_range_copy;

--1.11 create index
create table range_range_03
(
  c_int int,
  c_char1 char(3000),
  c_char2 char(5000),
  c_char3 char(6000),
  c_varchar1 varchar(3000),
  c_varchar2 varchar(5000),
  c_varchar3 varchar,
  c_varchar4 varchar,
  c_text1 text,
  c_text2 text,
  c_text3 text,
  c int,
  primary key(c,c_int)
) with (parallel_workers=10, SEGMENT=ON)
partition by range (c_int) subpartition by range (c_char1)
(
  partition p1 values less than(50)
  (
    subpartition p1_1 values less than('c'),
    subpartition p1_2 values less than(maxvalue)
  ),
  partition p2 values less than(100)
  (
    subpartition p2_1 values less than('c'),
    subpartition p2_2 values less than(maxvalue)
  ),
  partition p3 values less than(150)
  (
    subpartition p3_1 values less than('c'),
    subpartition p3_2 values less than(maxvalue)
  ),
  partition p4 values less than(200)
  (
    subpartition p4_1 values less than('c'),
    subpartition p4_2 values less than(maxvalue)
  ),
  partition p5 values less than(maxvalue)(
    subpartition p5_1 values less than('c'),
    subpartition p5_2 values less than(maxvalue)
  )
) enable row movement;

create index range_range_03_idx1 on range_range_03 (c_varchar1) local; --success

create index range_range_03_idx2 on range_range_03 (c_varchar2) local (
  partition cpt7_p1,
  partition cpt7_p2,
  partition cpt7_p3,
  partition cpt7_p4,
  partition cpt7_p5
); --failed

create index range_range_03_idx3 on range_range_03 (c_varchar3); --success, default global

create index range_range_03_idx4 on range_range_03 (c_varchar4) global; --success

create index range_range_03_idx5 on range_range_03 (c_varchar4) local; --failed, can not be same column with global index

\d+ range_range_03

select pg_get_tabledef('range_range_03');

drop table range_range_03;

--unique local index columns must contain the partition key
CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
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
) ENABLE ROW MOVEMENT;
create unique index idx on range_range(month_code) local;
create unique index idx1 on range_range(month_code, user_no) local;
drop table range_range;

-- partkey has timestampwithzone type
drop table hash_range;
CREATE TABLE hash_range
(
    col_1 int PRIMARY KEY USING INDEX,
    col_2 int  NOT NULL ,
    col_3 int  NOT NULL ,
    col_4 int,
    col_19 TIMESTAMP WITH TIME ZONE
) WITH (SEGMENT=ON)
PARTITION BY HASH (col_2) SUBPARTITION BY RANGE (col_19)
(    partition p_hash_1
  (
    SUBPARTITION p_range_1_1 VALUES LESS THAN( 5 ),
    SUBPARTITION p_range_1_2 VALUES LESS THAN( MAXVALUE )
  ),
  partition p_hash_2,
    PARTITION p_hash_3,
    PARTITION p_hash_4,
    PARTITION p_hash_5,
    PARTITION p_hash_7
) ENABLE ROW MOVEMENT;

CREATE TABLE hash_range
(
    col_1 int PRIMARY KEY USING INDEX,
    col_2 int  NOT NULL ,
    col_3 int  NOT NULL ,
    col_4 int,
    col_19 TIMESTAMP WITH TIME ZONE
) WITH (SEGMENT=ON)
PARTITION BY HASH (col_19) SUBPARTITION BY RANGE (col_2)
(    partition p_hash_1
  (
    SUBPARTITION p_range_1_1 VALUES LESS THAN( 5 ),
    SUBPARTITION p_range_1_2 VALUES LESS THAN( MAXVALUE )
  ),
  partition p_hash_2,
    PARTITION p_hash_3,
    PARTITION p_hash_4,
    PARTITION p_hash_5,
    PARTITION p_hash_7
) ENABLE ROW MOVEMENT;
drop table hash_range;

-- test create table like only support range_range in subpartition
CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);

create table t1(like range_range including partition);
insert into t1 values('201902', '1', '1', 1);
insert into t1 values('201902', '2', '1', 1);
insert into t1 values('201902', '1', '1', 1);
insert into t1 values('201903', '2', '1', 1);
insert into t1 values('201903', '1', '1', 1);
insert into t1 values('201903', '2', '1', 1);

explain (costs off) select * from t1;
select * from t1;
drop table t1;
drop table range_range;

CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
create table t1(like list_list including partition);
drop table list_list;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
create table t1(like range_list including partition);
drop table range_list;

-- test the key of partition and subpartition is same column
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (month_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '201902' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into list_list values('201902', '1', '1', 1);
insert into list_list values('201903', '2', '1', 1);
insert into list_list values('2', '2', '1', 1);
EXPLAIN (costs false)
SELECT * FROM list_list SUBPARTITION FOR ('201902','201902') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM list_list WHERE month_code = '201902' ORDER BY 1,2;
drop table list_list;

CREATE TABLE list_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY HASH (month_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
insert into list_hash values('201902', '1', '1', 1);
insert into list_hash values('201902', '2', '1', 1);
insert into list_hash values('201903', '5', '1', 1);
insert into list_hash values('201903', '6', '1', 1);
SELECT * FROM list_hash SUBPARTITION (p_201901_a) ORDER BY 1,2;
SELECT * FROM list_hash SUBPARTITION (p_201901_b) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM list_hash SUBPARTITION FOR ('201902','201902') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM list_hash WHERE month_code = '201903' ORDER BY 1,2;
drop table list_hash;

CREATE TABLE list_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY LIST (month_code) SUBPARTITION BY RANGE (month_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a values less than ('1'),
    SUBPARTITION p_201901_b values less than ('2')
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a values less than ('3'),
    SUBPARTITION p_201902_b values less than ('4')
  )
);
insert into list_range values('201902', '1', '1', 1);
insert into list_range values('201903', '2', '1', 1);
SELECT * FROM list_range SUBPARTITION (p_201902_a) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM list_range SUBPARTITION FOR ('201903','201903') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM list_range WHERE month_code = '201903' ORDER BY 1,2;
drop table list_range;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (month_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('201901'),
    SUBPARTITION p_201901_b values ('201902')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a values ('201903'),
    SUBPARTITION p_201902_b values ('201904')
  )
);
insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201904', '2', '1', 1);
EXPLAIN (costs false)
SELECT * FROM range_list SUBPARTITION FOR ('201902','201902') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM range_list WHERE month_code = '201903' ORDER BY 1,2;
drop table range_list;

CREATE TABLE range_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY HASH (month_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
insert into range_hash values('201901', '1', '1', 1);
insert into range_hash values('201902', '2', '1', 1);
insert into range_hash values('201903', '2', '1', 1);
insert into range_hash values('20190322', '1', '1', 1);
SELECT * FROM range_hash SUBPARTITION (p_201901_a) ORDER BY 1,2;
SELECT * FROM range_hash SUBPARTITION (p_201901_b) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM range_hash SUBPARTITION FOR ('201902','201902') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM range_hash WHERE month_code = '201903' ORDER BY 1,2;
drop table range_hash;

CREATE TABLE range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (month_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '20190220' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '20190230' )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '20190320' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '20190330' )
  )
);
insert into range_range values('201902', '1', '1', 1);
insert into range_range values('20190222', '2', '1', 1);
insert into range_range values('201903', '2', '1', 1);
insert into range_range values('20190322', '1', '1', 1);
insert into range_range values('20190333', '2', '1', 1);
EXPLAIN (costs false)
SELECT * FROM range_range SUBPARTITION FOR ('201902','201902') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM range_range WHERE month_code = '201903' ORDER BY 1,2;
drop table range_range;

CREATE TABLE hash_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY hash (month_code) SUBPARTITION BY LIST (month_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a VALUES ( '201901' ),
    SUBPARTITION p_201901_b VALUES ( '201902' )
  ),
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a VALUES ( '201901' ),
    SUBPARTITION p_201902_b VALUES ( '201902' )
  )
);
insert into hash_list values('201901', '1', '1', 1);
insert into hash_list values('201901', '2', '1', 1);
insert into hash_list values('201902', '1', '1', 1);
insert into hash_list values('201902', '2', '1', 1);
insert into hash_list values('201903', '2', '1', 1);
SELECT * FROM hash_list SUBPARTITION (p_201901_a) ORDER BY 1,2;
SELECT * FROM hash_list SUBPARTITION (p_201901_b) ORDER BY 1,2;
SELECT * FROM hash_list SUBPARTITION (p_201902_a) ORDER BY 1,2;
SELECT * FROM hash_list SUBPARTITION (p_201902_b) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM hash_list SUBPARTITION FOR ('201902','201902') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM hash_list WHERE month_code = '201901' ORDER BY 1,2;
drop table hash_list;

CREATE TABLE hash_hash
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY hash (month_code) SUBPARTITION BY hash (month_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a,
    SUBPARTITION p_201901_b
  ),
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a,
    SUBPARTITION p_201902_b
  )
);
insert into hash_hash values('201901', '1', '1', 1);
insert into hash_hash values('201901', '2', '1', 1);
insert into hash_hash values('201903', '1', '1', 1);
insert into hash_hash values('201903', '2', '1', 1);
EXPLAIN (costs false)
SELECT * FROM hash_hash SUBPARTITION FOR ('201901','201901') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM hash_hash WHERE month_code = '201903' ORDER BY 1,2;
drop table hash_hash;

CREATE TABLE hash_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
) WITH (SEGMENT=ON)
PARTITION BY hash (month_code) SUBPARTITION BY range (month_code)
(
  PARTITION p_201901
  (
    SUBPARTITION p_201901_a VALUES LESS THAN ( '201902' ),
    SUBPARTITION p_201901_b VALUES LESS THAN ( '201903' )
  ),
  PARTITION p_201902
  (
    SUBPARTITION p_201902_a VALUES LESS THAN ( '201902' ),
    SUBPARTITION p_201902_b VALUES LESS THAN ( '201903' )
  )
);
insert into hash_range values('201901', '1', '1', 1);
insert into hash_range values('201901', '2', '1', 1);
insert into hash_range values('201902', '1', '1', 1);
insert into hash_range values('201902', '2', '1', 1);
insert into hash_range values('201903', '2', '1', 1);
SELECT * FROM hash_range SUBPARTITION (p_201901_a) ORDER BY 1,2;
SELECT * FROM hash_range SUBPARTITION (p_201901_b) ORDER BY 1,2;
SELECT * FROM hash_range SUBPARTITION (p_201902_a) ORDER BY 1,2;
SELECT * FROM hash_range SUBPARTITION (p_201902_b) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM hash_range SUBPARTITION FOR ('201901','201901') ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM hash_range WHERE month_code = '201902' ORDER BY 1,2;
drop table hash_range;

--clean
DROP SCHEMA segment_subpartition_createtable CASCADE;
RESET CURRENT_SCHEMA;
