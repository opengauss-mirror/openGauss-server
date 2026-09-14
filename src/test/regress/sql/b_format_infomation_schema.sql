\d INFORMATION_SCHEMA.PROFILING

select * from INFORMATION_SCHEMA.PROFILING;


create database test_b1 with dbcompatibility 'B';
\c test_b1

CREATE TABLE departments (
  dept_id INT PRIMARY KEY, 
  dept_name VARCHAR(50) NOT NULL UNIQUE 
);

CREATE TABLE employees (
  emp_id INT PRIMARY KEY,  
  emp_name VARCHAR(50) NOT NULL,  
  dept_id INT,  
  hire_date DATE,
  CONSTRAINT fk_emp_dept  
  FOREIGN KEY (dept_id)
  REFERENCES departments(dept_id)
  ON DELETE SET NULL
  ON UPDATE CASCADE
);

CREATE TABLE partition_table_test(c1 int, c2 int, c3 int)
PARTITION BY RANGE (c1) SUBPARTITION BY RANGE (c2)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5) 
    (
        SUBPARTITION P_RANGE1_1 VALUES LESS THAN (5),
        SUBPARTITION P_RANGE1_2 VALUES LESS THAN (10),
        SUBPARTITION P_RANGE1_3 VALUES LESS THAN (15),
        SUBPARTITION P_RANGE1_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE2 VALUES LESS THAN (10) 
    (
        SUBPARTITION P_RANGE2_1 VALUES LESS THAN (5),
        SUBPARTITION P_RANGE2_2 VALUES LESS THAN (10),
        SUBPARTITION P_RANGE2_3 VALUES LESS THAN (15),
        SUBPARTITION P_RANGE2_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE3 VALUES LESS THAN (15)
    (
        SUBPARTITION P_RANGE3_1 VALUES LESS THAN (5),
        SUBPARTITION P_RANGE3_2 VALUES LESS THAN (10),
        SUBPARTITION P_RANGE3_3 VALUES LESS THAN (15),
        SUBPARTITION P_RANGE3_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE4 VALUES LESS THAN (20)
    (
        SUBPARTITION P_RANGE4_1 VALUES LESS THAN (5),
        SUBPARTITION P_RANGE4_2 VALUES LESS THAN (10),
        SUBPARTITION P_RANGE4_3 VALUES LESS THAN (15),
        SUBPARTITION P_RANGE4_4 VALUES LESS THAN (20)
    ),
    PARTITION P_RANGE5 VALUES LESS THAN (25),
    PARTITION P_RANGE6 VALUES LESS THAN (30)
);

-- part1
select CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_TYPE,ENFORCED from information_schema.table_constraints order by 1,2,3,4 limit 1;

select CHARACTER_SET_NAME,DEFAULT_COLLATE_NAME,DESCRIPTION,MAXLEN from INFORMATION_SCHEMA.CHARACTER_SETS order by 1,2,3,4 limit 1;

select CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,CHECK_CLAUSE FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS order by 1,2,3,4 limit 1;

select COLLATION_NAME,CHARACTER_SET_NAME,IS_DEFAULT,IS_COMPILED,SORTLEN,PAD_ATTRIBUTE from INFORMATION_SCHEMA.COLLATIONS order by 1,2,3,4 limit 1;

select GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,PRIVILEGE_TYPE,IS_GRANTABLE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES order by 1,2,3,4 limit 1;

select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,PRIVILEGES,COLUMN_COMMENT,GENERATION_EXPRESSION,SRS_ID from INFORMATION_SCHEMA.COLUMNS order by 1,2,3,4 limit 1;

select CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,POSITION_IN_UNIQUE_CONSTRAINT,REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME from information_schema.key_column_usage order by 1,2,3,4 limit 1;

select WORD,RESERVED from INFORMATION_SCHEMA.KEYWORDS order by 1,2 limit 1;

select SPECIFIC_CATALOG,SPECIFIC_SCHEMA,SPECIFIC_NAME,ORDINAL_POSITION,PARAMETER_MODE,PARAMETER_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,DTD_IDENTIFIER,ROUTINE_TYPE from INFORMATION_SCHEMA.PARAMETERS order by 1,2,3,4 limit 1;

select QUERY_ID,SEQ,STATE,DURATION,CPU_USER,CPU_SYSTEM,CONTEXT_VOLUNTARY,CONTEXT_INVOLUNTARY,BLOCK_OPS_IN,BLOCK_OPS_OUT,MESSAGES_SENT,MESSAGES_RECEIVED,PAGE_FAULTS_MAJOR,PAGE_FAULTS_MINOR,SWAPS,SOURCE_FUNCTION,SOURCE_FILE,SOURCE_LINE FROM information_schema.profiling order by 1,2,3,4 limit 1;

select CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,UNIQUE_CONSTRAINT_CATALOG,UNIQUE_CONSTRAINT_SCHEMA,UNIQUE_CONSTRAINT_NAME,MATCH_OPTION,UPDATE_RULE,DELETE_RULE,TABLE_NAME,REFERENCED_TABLE_NAME from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS order by 1,2,3,4 limit 1;

select SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, DTD_IDENTIFIER, ROUTINE_BODY, ROUTINE_DEFINITION, EXTERNAL_NAME, EXTERNAL_LANGUAGE, PARAMETER_STYLE, IS_DETERMINISTIC, SQL_DATA_ACCESS, SQL_PATH, SECURITY_TYPE, CREATED, LAST_ALTERED, SQL_MODE , ROUTINE_COMMENT, DEFINER, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION from information_schema.routines order by 1,2,3,4 limit 1;

select CATALOG_NAME,SCHEMA_NAME,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME,SQL_PATH,DEFAULT_ENCRYPTION from information_schema.schemata order by 1,2,3,4 limit 1;

select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE,INDEX_SCHEMA,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME,CARDINALITY,SUB_PART,PACKED,NULLABLE,INDEX_TYPE,COMMENT,INDEX_COMMENT,IS_VISIBLE,EXPRESSION from INFORMATION_SCHEMA.STATISTICS order by 1,2,3,4 limit 1;

select GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PRIVILEGE_TYPE,IS_GRANTABLE from INFORMATION_SCHEMA.TABLE_PRIVILEGES order by 1,2,3,4 limit 1;

select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,VERSION,ROW_FORMAT,TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,AUTO_INCREMENT,CREATE_TIME,UPDATE_TIME,CHECK_TIME,TABLE_COLLATION,CHECKSUM,CREATE_OPTIONS,TABLE_COMMENT from information_schema.tables order by 1,2,3,4 limit 1;


select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME, PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION, PARTITION_METHOD, SUBPARTITION_METHOD, PARTITION_EXPRESSION, SUBPARTITION_EXPRESSION, PARTITION_DESCRIPTION, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, CHECK_TIME, CHECKSUM, PARTITION_COMMENT, NODEGROUP, TABLESPACE_NAME  from information_schema.partitions order by 1,2,3,4 limit 1;

-- part3:

select pg_typeof(MAXLEN), pg_typeof(DESCRIPTION) from INFORMATION_SCHEMA.CHARACTER_SETS limit 1;

select pg_typeof(character_set_name), pg_typeof(ID), pg_typeof(IS_DEFAULT), pg_typeof(IS_COMPILED), pg_typeof(SORTLEN) from INFORMATION_SCHEMA.COLLATIONS limit 1;

select pg_typeof(PRIVILEGES), pg_typeof(column_key), pg_typeof(SRS_ID) from INFORMATION_SCHEMA.COLUMNS limit 1;

select pg_typeof(REFERENCED_TABLE_SCHEMA), pg_typeof(REFERENCED_TABLE_NAME), pg_typeof(REFERENCED_COLUMN_NAME) from INFORMATION_SCHEMA.KEY_COLUMN_USAGE limit 1;

select pg_typeof(ROUTINE_TYPE) from INFORMATION_SCHEMA.PARAMETERS limit 1;

select pg_typeof(table_name), pg_typeof(referenced_table_name) from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS limit 1;

select pg_typeof(ROUTINE_COMMENT), pg_typeof(SQL_MODE), pg_typeof(DEFINER), pg_typeof(CHARACTER_SET_CLIENT), pg_typeof(COLLATION_CONNECTION),pg_typeof(DATABASE_COLLATION) from INFORMATION_SCHEMA.ROUTINES limit 1;

select pg_typeof(DEFAULT_COLLATION_NAME), pg_typeof(DEFAULT_ENCRYPTION) from INFORMATION_SCHEMA.SCHEMATA limit 1;

select pg_typeof(ENFORCED) from INFORMATION_SCHEMA.TABLE_CONSTRAINTS limit 1;

select pg_typeof(table_collation), pg_typeof(engine), pg_typeof(VERSION), pg_typeof(ROW_FORMAT), pg_typeof(TABLE_ROWS), pg_typeof(AVG_ROW_LENGTH), pg_typeof(DATA_LENGTH), pg_typeof(MAX_DATA_LENGTH), pg_typeof(INDEX_LENGTH), pg_typeof(DATA_FREE), pg_typeof(AUTO_INCREMENT), pg_typeof(CREATE_TIME), pg_typeof(UPDATE_TIME), pg_typeof(CHECK_TIME), pg_typeof(CHECKSUM), pg_typeof(CREATE_OPTIONS) from INFORMATION_SCHEMA.TABLES limit 1;

select pg_typeof(WORD), pg_typeof(RESERVED) from INFORMATION_SCHEMA.KEYWORDS limit 1;

select pg_typeof(TABLE_CATALOG), pg_typeof(TABLE_SCHEMA), pg_typeof(TABLE_NAME), pg_typeof(NON_UNIQUE), pg_typeof(INDEX_SCHEMA), pg_typeof(INDEX_NAME), pg_typeof(SEQ_IN_INDEX), pg_typeof(COLUMN_NAME), pg_typeof(cardinality), pg_typeof(SUB_PART), pg_typeof(PACKED), pg_typeof(NULLABLE), pg_typeof(INDEX_TYPE), pg_typeof(COMMENT), pg_typeof(INDEX_COMMENT), pg_typeof(IS_VISIBLE), pg_typeof(EXPRESSION) from INFORMATION_SCHEMA.STATISTICS limit 1;

select pg_typeof(table_catalog), pg_typeof(table_schema), pg_typeof(table_name), pg_typeof(PARTITION_NAME), pg_typeof(SUBPARTITION_NAME), pg_typeof(PARTITION_ORDINAL_POSITION), pg_typeof(SUBPARTITION_ORDINAL_POSITION), pg_typeof(PARTITION_METHOD), pg_typeof(SUBPARTITION_METHOD), pg_typeof(PARTITION_EXPRESSION), pg_typeof(SUBPARTITION_EXPRESSION), pg_typeof(PARTITION_DESCRIPTION), pg_typeof(TABLE_ROWS), pg_typeof(AVG_ROW_LENGTH), pg_typeof(DATA_LENGTH), pg_typeof(MAX_DATA_LENGTH), pg_typeof(INDEX_LENGTH), pg_typeof(DATA_FREE), pg_typeof(CREATE_TIME), pg_typeof(UPDATE_TIME), pg_typeof(CHECK_TIME), pg_typeof(CHECKSUM), pg_typeof(PARTITION_COMMENT), pg_typeof(NODEGROUP), pg_typeof(TABLESPACE_NAME) from INFORMATION_SCHEMA.PARTITIONS limit 1;


-- part4 function test
-- information_schema COLUMNS
CREATE TABLE test_index_table_1 (
	student_id INT PRIMARY KEY,
    course_id INT,
    score INT
);

CREATE TABLE test_index_table_2 (
    student_id INT,
    course_id INT,
    score INT
);

CREATE TABLE test_index_table_3 (
    student_id INT,
    course_id INT,
    score INT
);

CREATE TABLE test_index_table_4 (
    student_id INT,
    course_id INT,
    score INT
);

CREATE UNIQUE INDEX test_index_2 ON test_index_table_2(student_id);
CREATE INDEX test_index_3 ON test_index_table_2(course_id, student_id);
CREATE INDEX test_index_4 ON test_index_table_2(student_id, course_id, score);

select * from information_schema.columns where table_name = 'test_index_table_1' order by 1,2,3,4;
select * from information_schema.columns where table_name = 'test_index_table_2' order by 1,2,3,4;
select * from information_schema.columns where table_name = 'test_index_table_3' order by 1,2,3,4;
select * from information_schema.columns where table_name = 'test_index_table_4' order by 1,2,3,4;
drop table test_index_table_1;
drop table test_index_table_2;
drop table test_index_table_3;
drop table test_index_table_4;

-- INFORMATION_SCHEMA.KEY_COLUMN_USAGE
CREATE TABLE departments_test1 (
    id int PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE employees_test1 (
    id int PRIMARY KEY,
    employee_name VARCHAR(100) NOT NULL,
    department_id INTEGER,
    department_name VARCHAR(100),
	hire_date VARCHAR(50),
    CONSTRAINT fk_emp_dept_id FOREIGN KEY (department_id) REFERENCES departments_test1(id) ON DELETE SET NULL  ON UPDATE CASCADE,
    CONSTRAINT fk_emp_dept_name FOREIGN KEY (department_name) REFERENCES departments_test1(name) ON DELETE SET NULL ON UPDATE CASCADE
);

select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where constraint_name = 'fk_emp_dept_id' order by 1,2,3,4;
select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where constraint_name = 'fk_emp_dept_name' order by 1,2,3,4;



CREATE TABLE employees_test2 (
    id int PRIMARY KEY,
    employee_name VARCHAR(100) NOT NULL,
	department_name VARCHAR(100),
    department_id INTEGER,
	hire_date VARCHAR(50),
	CONSTRAINT fk_emp_dept_name1 FOREIGN KEY (department_name) REFERENCES departments_test1(name) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_emp_dept_id1 FOREIGN KEY (department_id) REFERENCES departments_test1(id) ON DELETE SET NULL  ON UPDATE CASCADE
);

select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where constraint_name = 'fk_emp_dept_id1' order by 1,2,3,4;
select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where constraint_name = 'fk_emp_dept_name1' order by 1,2,3,4;

select * from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS where constraint_name in ('fk_emp_dept_id', 'fk_emp_dept_name', 'fk_emp_dept_id1', 'fk_emp_dept_name1') order by constraint_name;

drop table employees_test1;
drop table employees_test2;
drop table departments_test1;

-- INFORMATION_SCHEMA.ROUTINES
select routine_comment from INFORMATION_SCHEMA.ROUTINES  where routine_name in('pg_get_index_type', 'pg_avg_row_length') order by 1;

-- INFORMATION_SCHEMA.TABLES
CREATE TABLE test_compression_1 (
    id INT,
    data TEXT
) WITH (orientation = column, compression = 'no');

CREATE TABLE test_compression_2 (
    id INT,
    data TEXT
) WITH (orientation = column, compression = 'yes');

CREATE TABLE test_compression_3 (
    id INT,
    data TEXT
) WITH (orientation = column, compression = 'low');

CREATE TABLE test_compression_4 (
    id INT,
    data TEXT
) WITH (orientation = column, compression = 'middle');

CREATE TABLE test_compression_5 (
    id INT,
    data TEXT
) WITH (orientation = column, compression = 'high');

CREATE TABLE test_compression_6 (
    id INT,
    data TEXT
) WITH (orientation = column);

CREATE TABLE test_compression_7 (
    id INT,
    data TEXT
);

select table_name,row_format,create_options from INFORMATION_SCHEMA.TABLES where table_name like 'test_compression%'  and create_time is not null and update_time is not null order by table_name;

create table test_size_1(c1 int, c2 float8, c3 text, c4 varchar(5));
insert into test_size_1 values (1, 1, 1, 1);
insert into test_size_1 values (2, 2, 2, 2);
insert into test_size_1 values (3, 3, 3, 3);
insert into test_size_1 values (4, 4, 4, 4);
insert into test_size_1 values (5, 5, 5, 5);
create index test_size_1_indx1 on test_size_1(c1, c2, c3, c4);
analyze test_size_1;
select table_rows,avg_row_length,data_length,max_data_length,index_length,data_free from INFORMATION_SCHEMA.TABLES where table_name = 'test_size_1' order by 1,2,3,4;


CREATE TABLE test_auto_increase (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    age INT
);
select column_name, extra from INFORMATION_SCHEMA.columns where table_name = 'test_auto_increase' order by ordinal_position;
CREATE TABLE test_decimal_column_type (amount DECIMAL(10,2));
select column_name, data_type, column_type from INFORMATION_SCHEMA.columns where table_name = 'test_decimal_column_type';
DROP TABLE test_decimal_column_type;
select get_auto_increment_nextval(1234, true);
select auto_increment from  INFORMATION_SCHEMA.tables where table_name = 'test_auto_increase';
insert into test_auto_increase(username, age) values ('users', 1);
select auto_increment from  INFORMATION_SCHEMA.tables where table_name = 'test_auto_increase';


insert into test_auto_increase values (2147483646, 'users2', 1);
select auto_increment from  INFORMATION_SCHEMA.tables where table_name = 'test_auto_increase';
insert into test_auto_increase values (2147483647, 'users3', 1);
select auto_increment from  INFORMATION_SCHEMA.tables where table_name = 'test_auto_increase';


CREATE TABLE test_auto_increase2 (
    id serial PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    age INT
);
select auto_increment from  INFORMATION_SCHEMA.tables where table_name = 'test_auto_increase2';
insert into test_auto_increase2(username, age) values ('users', 1);
select auto_increment from  INFORMATION_SCHEMA.tables where table_name = 'test_auto_increase2';

create table test_collation_1(c1 text) collate utf8_bin;
create table test_collation_2(c1 text);
select table_collation from INFORMATION_SCHEMA.TABLES where table_name in ('test_collation_1', 'test_collation_2');

drop table test_compression_1;
drop table test_compression_2;
drop table test_compression_3;
drop table test_compression_4;
drop table test_compression_5;
drop table test_compression_6;
drop table test_compression_7;
drop table test_size_1;
drop table test_auto_increase;
drop table test_auto_increase2;
drop table test_collation_1;
drop table test_collation_2;

--INFORMATION_SCHEMA.STATISTICS
CREATE TABLE test_index_table1 (
    id INT PRIMARY KEY,
    user_id INT NOT NULL,
    role_id INT NOT NULL,
    age int NOT NULL
);

CREATE UNIQUE INDEX test_index_table1_idx1 ON test_index_table1 (user_id, role_id);
CREATE INDEX test_index_table1_idx2 ON test_index_table1 (user_id, role_id, age);
CREATE INDEX test_index_table1_idx3 ON test_index_table1 (age, role_id, user_id);
CREATE INDEX test_index_table1_idx4 ON test_index_table1 (age) using hash;
CREATE INDEX test_index_table1_idx5 ON test_index_table1 (age) using ubtree;
CREATE INDEX test_index_table1_idx6 ON test_index_table1 (user_id desc, role_id desc, age desc);
CREATE INDEX test_index_table1_idx7 ON test_index_table1 (age desc, role_id desc, user_id desc);


insert into test_index_table1 values (1, 1, 1, 1);
insert into test_index_table1 values (2, 2, 2, 2);
insert into test_index_table1 values (3, 3, 3, 3);
insert into test_index_table1 values (4, 4, 4, 4);
analyze test_index_table1;
select * from INFORMATION_SCHEMA.STATISTICS where table_name = 'test_index_table1' order by 1,2,3,4,5,6,7,8,9,10;



CREATE TABLE test_index_table2 (
    id INT,
    user_id INT,
    role_id INT,
    age int
);
CREATE UNIQUE INDEX test_index_table2_idx1 ON test_index_table2 (id, user_id, role_id);
CREATE INDEX test_index_table2_idx2 ON test_index_table2 (user_id, role_id, age) COMMENT 'test_index_table1_idx2_d';
select * from INFORMATION_SCHEMA.STATISTICS where table_name = 'test_index_table2' order by 1,2,3,4,5,6,7,8,9,10;



CREATE TABLE test_index_table3 (
    id int,
    role_id varchar,
    age int
);
CREATE INDEX test_index_table3_idx1 ON test_index_table3 (trunc(id));
CREATE UNIQUE INDEX test_index_table3_idx2 ON test_index_table3 (SUBSTR(role_id, 1, 4));
select * from INFORMATION_SCHEMA.STATISTICS where table_name = 'test_index_table3' order by 1,2,3,4,5,6,7,8,9,10;

drop table test_index_table1;
drop table test_index_table2;
drop table test_index_table3;

--INFORMATION_SCHEMA.PARTITIONS
CREATE TABLE partition_table_test1(c1 int, c2 int, c3 int) 
PARTITION BY RANGE (c1)
(
    PARTITION P_RANGE1 VALUES LESS THAN (5),
	PARTITION P_RANGE2 VALUES LESS THAN (10),
	PARTITION P_RANGE3 VALUES LESS THAN (15),
	PARTITION P_RANGE4 VALUES LESS THAN (20)
);

select  table_catalog,table_schema,table_name,partition_name,subpartition_name,partition_ordinal_position,subpartition_ordinal_position,partition_method, subpartition_method,partition_expression,subpartition_expression,partition_description,table_rows,avg_row_length,data_length,max_data_length, index_length, data_free,check_time, partition_comment,nodegroup,tablespace_name  from INFORMATION_SCHEMA.PARTITIONS where table_name = 'partition_table_test1' order by 1,2,3,4;

CREATE TABLE partition_table_test2 (
  c1 INT,
  c2 INT,
  c3 INT
)
PARTITION BY RANGE (c1) 
SUBPARTITION BY HASH (c2) (
  PARTITION P_RANGE1 VALUES LESS THAN (5) (
    SUBPARTITION P_RANGE1_1,
    SUBPARTITION P_RANGE1_2,
    SUBPARTITION P_RANGE1_3,
    SUBPARTITION P_RANGE1_4
  ),
  PARTITION P_RANGE2 VALUES LESS THAN (10) (
    SUBPARTITION P_RANGE2_1,
    SUBPARTITION P_RANGE2_2,
    SUBPARTITION P_RANGE2_3,
    SUBPARTITION P_RANGE2_4
  ),
  PARTITION P_RANGE3 VALUES LESS THAN (15) (
    SUBPARTITION P_RANGE3_1,
    SUBPARTITION P_RANGE3_2,
    SUBPARTITION P_RANGE3_3,
    SUBPARTITION P_RANGE3_4
  )
);

select  table_catalog,table_schema,table_name,partition_name,subpartition_name,partition_ordinal_position,subpartition_ordinal_position,partition_method, subpartition_method,partition_expression,subpartition_expression,partition_description,table_rows,avg_row_length,data_length,max_data_length, index_length, data_free,check_time, partition_comment,nodegroup,tablespace_name  from INFORMATION_SCHEMA.PARTITIONS where table_name = 'partition_table_test2' order by 1,2,3,4;


CREATE TABLE partition_table_test3 (
    c1 INT PRIMARY KEY,
    c2 TIMESTAMP NOT NULL,  
    c3 NUMERIC(10,2)          
) 
PARTITION BY RANGE (EXTRACT(YEAR FROM c2)) 
(
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022), 
    PARTITION p2022 VALUES LESS THAN (2023), 
    PARTITION p_default VALUES LESS THAN (MAXVALUE)  
);

select  table_catalog,table_schema,table_name,partition_name,subpartition_name,partition_ordinal_position,subpartition_ordinal_position,partition_method, subpartition_method,partition_expression,subpartition_expression,partition_description,table_rows,avg_row_length,data_length,max_data_length, index_length, data_free,check_time, partition_comment,nodegroup,tablespace_name  from INFORMATION_SCHEMA.PARTITIONS where table_name = 'partition_table_test3' order by 1,2,3,4;


CREATE TABLE partition_table_test4 (
  c1 float4,
  c2 int,
  c3 TIMESTAMP
)
PARTITION BY RANGE (floor(c1))
SUBPARTITION BY RANGE (abs(c2)) (
  PARTITION P_RANGE1 VALUES LESS THAN (1) (
    SUBPARTITION P_RANGE1_1 VALUES LESS THAN (1),
    SUBPARTITION P_RANGE1_2 VALUES LESS THAN (2),
    SUBPARTITION P_RANGE1_3 VALUES LESS THAN (3),
    SUBPARTITION P_RANGE1_4 VALUES LESS THAN (4)
  ),
  PARTITION P_RANGE2 VALUES LESS THAN (2) (
    SUBPARTITION P_RANGE2_1 VALUES LESS THAN (1),
    SUBPARTITION P_RANGE2_2 VALUES LESS THAN (2),
    SUBPARTITION P_RANGE2_3 VALUES LESS THAN (3),
    SUBPARTITION P_RANGE2_4 VALUES LESS THAN (4)
  ),
  PARTITION P_RANGE3 VALUES LESS THAN (3) (
    SUBPARTITION P_RANGE3_1 VALUES LESS THAN (1),
    SUBPARTITION P_RANGE3_2 VALUES LESS THAN (2),
    SUBPARTITION P_RANGE3_3 VALUES LESS THAN (3),
    SUBPARTITION P_RANGE3_4 VALUES LESS THAN (4)
  )
);

select table_catalog,table_schema,table_name,partition_name,subpartition_name,partition_ordinal_position,subpartition_ordinal_position,partition_method, subpartition_method,partition_expression,subpartition_expression,partition_description,table_rows,avg_row_length,data_length,max_data_length, index_length, data_free,check_time, partition_comment,nodegroup,tablespace_name  from INFORMATION_SCHEMA.PARTITIONS where table_name = 'partition_table_test4' order by 1,4,6,7;

drop table partition_table_test1;
drop table partition_table_test2;
drop table partition_table_test3;
drop table partition_table_test4;

DROP TABLE employees;
DROP TABLE departments;


create table test_event_log(id int);

CREATE EVENT IF NOT EXISTS test_event ON SCHEDULE EVERY 30 MINUTE
 STARTS CURRENT_TIMESTAMP + interval 33 minute
 DO INSERT INTO test_event_log VALUES (1);

select * from INFORMATION_SCHEMA.events;

select EVENT_CATALOG, EVENT_SCHEMA, EVENT_NAME, DEFINER, TIME_ZONE, EVENT_BODY, EVENT_DEFINITION, EVENT_TYPE, EXECUTE_AT, INTERVAL_VALUE, INTERVAL_FIELD, SQL_MODE, STARTS, ENDS, STATUS, ON_COMPLETION, CREATED, LAST_ALTERED, LAST_EXECUTED, EVENT_COMMENT, ORIGINATOR, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION from INFORMATION_SCHEMA.events;

select pg_typeof(EVENT_CATALOG), pg_typeof(EVENT_SCHEMA), pg_typeof(EVENT_NAME), pg_typeof(DEFINER), pg_typeof(TIME_ZONE), pg_typeof(EVENT_BODY), pg_typeof(EVENT_DEFINITION), pg_typeof(EVENT_TYPE), pg_typeof(EXECUTE_AT), pg_typeof(INTERVAL_VALUE), pg_typeof(INTERVAL_FIELD), pg_typeof(SQL_MODE), pg_typeof(STARTS), pg_typeof(ENDS), pg_typeof(STATUS), pg_typeof(ON_COMPLETION), pg_typeof(CREATED), pg_typeof(LAST_ALTERED), pg_typeof(LAST_EXECUTED), pg_typeof(EVENT_COMMENT), pg_typeof(ORIGINATOR), pg_typeof(CHARACTER_SET_CLIENT), pg_typeof(COLLATION_CONNECTION), pg_typeof(DATABASE_COLLATION) from INFORMATION_SCHEMA.events;

drop EVENT IF  EXISTS test_event;
drop table test_event_log;
