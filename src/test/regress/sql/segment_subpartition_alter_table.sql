DROP SCHEMA segment_subpartition_alter_table CASCADE;
CREATE SCHEMA segment_subpartition_alter_table;
SET CURRENT_SCHEMA TO segment_subpartition_alter_table;

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

--change column type
alter table range_range alter column user_no set data type char(30);
alter table range_range alter column sales_amt set data type varchar;
\d+ range_range

-- rename
alter table range_range rename to hahahahahah;
alter table range_range rename partition p_201901 to hahahahahah;
alter table range_range rename partition p_201901_a to hahahahahah;

--cluster
create index idx_range_range on range_range(month_code,user_no);
alter table range_range cluster on idx_range_range;

-- move tablespace
CREATE TABLESPACE example1 RELATIVE LOCATION 'tablespace1/tablespace_1';
alter table range_range move PARTITION p_201901 tablespace  example1;
alter table range_range move PARTITION p_201901_a tablespace  example1;
DROP TABLESPACE example1;

-- merge
alter table range_range merge  PARTITIONS p_201901 , p_201902 into PARTITION p_range_3;
alter table range_range merge  SUBPARTITIONS p_201901 , p_201902 into PARTITION p_range_3;

-- exchange
CREATE TABLE ori
(
    month_code VARCHAR2 ( 30 ) ,
    dept_code  VARCHAR2 ( 30 ) ,
    user_no    VARCHAR2 ( 30 ) ,
    sales_amt  int,
	primary key(month_code, dept_code)
) WITH (SEGMENT=ON);
ALTER TABLE range_range EXCHANGE PARTITION (p_201901) WITH TABLE ori;
ALTER TABLE range_range EXCHANGE SUBPARTITION (p_201901) WITH TABLE ori;

-- drop
alter table range_range drop partition p_201901;
alter table range_range drop partition p_201901_a;
alter table range_range drop subpartition p_201901_a;

-- add
alter table range_range add partition p_range_4 VALUES LESS THAN('201904');

-- split
alter table range_range split PARTITION p_201901 at (8) into ( PARTITION add_p_01 , PARTITION add_p_02 );

drop table ori;
drop table range_range;

CREATE TABLE IF NOT EXISTS range_range_02
(
    col_1 int ,
    col_2 int ,
	col_3 VARCHAR2 ( 30 ) NOT NULL ,
    col_4 int
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

create index on range_range_02(col_2) local;

alter table range_range_02 MODIFY PARTITION p_range_2 UNUSABLE LOCAL INDEXES;

alter table range_range_02 MODIFY PARTITION p_range_2 REBUILD UNUSABLE LOCAL INDEXES;

alter table range_range_02 alter col_1 type char;

alter table range_range_02 alter col_2 type char;

drop table range_range_02;

--validate constraint
CREATE TABLE hash_hash
(
    col_1 int ,
    col_2 int NOT NULL ,
    col_3 VARCHAR2 ( 30 ) ,
    col_4 int
) WITH (SEGMENT=ON)
PARTITION BY hash (col_3) SUBPARTITION BY hash (col_2)
(
    PARTITION p_hash_1
    (
        SUBPARTITION p_hash_1_1 ,
        SUBPARTITION p_hash_1_2 ,
        SUBPARTITION p_hash_1_3 ,
        SUBPARTITION p_hash_1_4
    ),
    PARTITION p_hash_2
    (
        SUBPARTITION p_hash_2_1 ,
        SUBPARTITION p_hash_2_2
    ),
    PARTITION p_hash_3,
    PARTITION p_hash_4
    (
        SUBPARTITION p_hash_4_1
    ),
    PARTITION p_hash_5
);

INSERT INTO hash_hash VALUES(null,1,1,1);
alter table hash_hash add constraint con_hash_hash check(col_1 is not null) NOT VALID ;
INSERT INTO hash_hash VALUES(null,2,1,1); --error
INSERT INTO hash_hash VALUES(1,3,1,1); --success
alter table hash_hash VALIDATE CONSTRAINT con_hash_hash; --error
delete from hash_hash where col_1 is null;
alter table hash_hash VALIDATE CONSTRAINT con_hash_hash; --success

drop table hash_hash cascade;
-- clean
DROP SCHEMA ustore_subpartition_alter_table CASCADE;
RESET CURRENT_SCHEMA;
