create schema sys_view_test;
set search_path to sys_view_test;

-- show views struct
\d sys.sysobjects
\d sys.syscolumns
\d sys.sysindexes
\d sys.sysindexkeys

-- test select
select * from sys.sysobjects order by id limit 1;
select * from sys.syscolumns order by id limit 1;
select * from sys.sysindexes order by id, indid limit 1;
select * from sys.sysindexkeys order by id, indid limit 1;
select OrigFillFactor, StatVersion, FirstIAM from sys.sysindexes order by id, indid limit 1;
select origfillfactor, statversion, firstiam from sys.sysindexes order by id, indid limit 1;

-- prepare data
CREATE TABLE student
(
    std_id INT PRIMARY KEY,
    std_name VARCHAR(20) NOT NULL,
    std_sex VARCHAR(6),
    std_birth DATE,
    std_in DATE NOT NULL,
    std_address VARCHAR(100)
);

CREATE TABLE teacher
(
    tec_id INT PRIMARY KEY,
    tec_name VARCHAR(20) NOT NULL,
    tec_job VARCHAR(15),
    tec_sex VARCHAR(6),
    tec_age INT default 20,
    tec_in DATE NOT NULL
);

CREATE TABLE class
(
    cla_id INT PRIMARY KEY,
    cla_name VARCHAR(20) NOT NULL,
    cla_teacher INT NOT NULL
);
ALTER TABLE class ADD CONSTRAINT fk_tec_id FOREIGN KEY (cla_teacher) REFERENCES teacher(tec_id) ON DELETE CASCADE;

CREATE TABLE school_department
(
    depart_id INT PRIMARY KEY,
    depart_name VARCHAR(30) NOT NULL,
    depart_teacher INT NOT NULL
);
ALTER TABLE school_department ADD CONSTRAINT fk_depart_tec_id FOREIGN KEY (depart_teacher) REFERENCES teacher(tec_id) ON DELETE CASCADE;

CREATE TABLE course
(
    cor_id INT PRIMARY KEY,
    cor_name VARCHAR(30) NOT NULL,
    cor_type VARCHAR(20),
    credit DOUBLE PRECISION
);

create table teacher_salary
(
    tec_id int,
    tec_salary decimal(10, 2)
);

create or replace view teacher_info as
select c.cla_name, t.tec_name, t.tec_job, t.tec_sex, t.tec_age
from teacher t 
left join class c on c.cla_teacher = t.tec_id;

create table t_log (c1 int, c2 varchar(20), c3 timestamp);

create sequence if not exists t_seq
    increment by 1
    nominvalue
    nomaxvalue
    start with 1
    nocycle;
select t_seq.nextval;

create or replace function tg_log() returns trigger as
$$
begin
    insert into t_log values (new.std_id, new.std_name, SYSDATE);
    return new;
end;
$$ language plpgsql;

create trigger log_student_after_insert
after insert on student 
for each row 
execute procedure tg_log();

create or replace procedure test_sum(in a int, in b int, out c int)
as
begin
    c = a + b;
end;
/

create or replace function test_sub(num1 int, num2 int)
returns int as $$
begin
    return num1 - num2;
end;
$$ language plpgsql;

CREATE OR REPLACE SYNONYM syn_tbl FOR student;
CREATE OR REPLACE SYNONYM syn_tr FOR log_student_after_insert;

select obj.name, obj.xtype, obj.type from sys.sysobjects obj 
left join pg_namespace s on s.oid = obj.uid
where s.nspname = 'sys_view_test'
order by id;

select col.name, col.length, col.colid, col.status, col.prec, col.scale, col.iscomputed, col.isoutparam, col.isnullable, col.collation
from sys.syscolumns col
left join pg_class c on col.id = c.oid
left join pg_namespace s on s.oid = c.relnamespace
where s.nspname = 'sys_view_test'
order by id, colid;

select ind.name, ind.keycnt, ind.origfillfactor, ind.rows from sys.sysindexes ind
left join pg_class c on c.oid = ind.indid
left join pg_namespace s on c.relnamespace = s.oid
where s.nspname = 'sys_view_test'
order by id, indid;

select c_tab.relname as tabname, c_ind.relname as indname, ind.colid, ind.keyno from sys.sysindexkeys ind
left join pg_class c_ind on c_ind.oid = ind.indid
left join pg_class c_tab on c_tab.oid = ind.id
left join pg_namespace s on c_ind.relnamespace = s.oid
where s.nspname = 'sys_view_test'
order by id, indid;

drop synonym syn_tr;
drop synonym syn_tbl;
drop function test_sub;
drop procedure test_sum;
drop table t_log;
drop view teacher_info;
drop table teacher_salary;
drop table course;
drop table school_department;
drop table class;
drop table teacher;
drop table student;

-- test sys views
\d sys.all_objects
\d sys.objects
\d sys.tables
\d sys.views
\d sys.all_columns
\d sys.columns
\d sys.indexes
\d sys.procedures

drop table if exists t_index;
create table t_index (id int primary key, c2 int not null, c3 char(1), c4 text, c5 numeric(10, 2));
create unique index t_index_c2_uind on t_index(c2);
create index t_index_c3_c2_ind on t_index(c3, c2);
create index t_index_func_c4_ind on t_index(lower(c4));
create index t_index_hash_c3_ind on t_index using hash(c3);
create index t_index_filter_c5_ind on t_index (c5) where c5 > 100.00;

drop table if exists t_foreign1;
create table t_foreign1 (f_id int primary key, f_name text not null, f_age int default 18);
-- foreign key, check constraint
drop table if exists t_foreign2;
create table t_foreign2 (f_id int primary key not null, f_c2 int references t_foreign1(f_id), f_salary real check(f_salary > 0));

-- columns table
create table t_column (c1 int, c2 text, c3 char(1), constraint t_stats_col_pk primary key (c1)) with (orientation = column);

-- temporal table
create temp table t_temp_table as select * from t_index;

-- view
drop view if exists v_normal;
create view v_normal as select * from t_index;
-- view with check option
drop view if exists v_check;
create view v_check as select * from t_index where id > 10 with check option;
-- materialized view
drop materialized view if exists mv_normal;
create materialized view mv_normal as select * from t_foreign1;

-- sequence
create sequence if not exists seq_order
    increment by 1
    start with 1
    nocycle;
select seq_order.nextval;
create table t_orders (order_id int primary key default nextval('seq_order'), order_date date);

-- synonym
create synonym syn_tbl for t_orders;

-- trigger
create table t_tg_src (c1 int, c2 int, c3 int);
create table t_tg_des (c1 int, c2 int, c3 int);
create or replace function tri_insert_func() returns trigger as
$$
begin
	insert into t_tg_des values (NEW.c1, NEW.c2, NEW.c3);
	return NEW;
end
$$ language plpgsql;
create trigger insert_trigger
before insert on t_tg_src
for each row
execute procedure tri_insert_func();

-- function
create or replace function test_sub(a int, b int) returns int as $$
begin
	return a - b;
end;
$$ language plpgsql;

-- procedure
create or replace procedure test_sum(in a int, in b int, out c int) as
begin 
	c = a + b;
end;
/

select name, s.nspname, po.relname, type, type_desc, is_ms_shipped, is_published, is_schema_published
from sys.all_objects o
inner join pg_namespace s on o.schema_id = s.oid
left join pg_class po on po.oid = parent_object_id
where s.nspname = 'sys_view_test'
order by object_id;

select name, s.nspname, po.relname, type, type_desc, is_ms_shipped, is_published, is_schema_published
from sys.objects o
inner join pg_namespace s on o.schema_id = s.oid
left join pg_class po on po.oid = parent_object_id
where s.nspname = 'sys_view_test'
order by object_id;

select 
    name, s.nspname, type, type_desc, is_ms_shipped, is_published, is_schema_published,
    max_column_id_used, uses_ansi_nulls, is_replicated, is_memory_optimized, durability, durability_desc, temporal_type, temporal_type_desc
from sys.tables t
inner join pg_namespace s on t.schema_id = s.oid
where s.nspname = 'sys_view_test'
order by object_id;

select name, s.nspname, type, type_desc, is_ms_shipped, is_published, is_schema_published, with_check_option
from sys.views v
inner join pg_namespace s on v.schema_id = s.oid
where s.nspname = 'sys_view_test'
order by object_id;

select t.relname, c.name, column_id, max_length, precision, scale, is_nullable, is_computed, is_replicated, generated_always_type, generated_always_type_desc
from sys.all_columns c
inner join pg_class t on c.object_id = t.oid
inner join pg_namespace s on t.relnamespace = s.oid
where s.nspname = 'sys_view_test'
order by object_id, column_id;

select t.relname, c.name, column_id, max_length, precision, scale, is_nullable, is_computed, is_replicated, generated_always_type, generated_always_type_desc
from sys.columns c
inner join pg_class t on c.object_id = t.oid
inner join pg_namespace s on t.relnamespace = s.oid
where s.nspname = 'sys_view_test'
order by object_id, column_id;

select t.relname, name, i.type, type_desc, i.is_unique, is_primary_key, is_unique_constraint, fill_factor, is_disabled, has_filter, filter_definition
from sys.indexes i
inner join pg_class t on i.object_id = t.oid
inner join pg_namespace s on t.relnamespace = s.oid
where s.nspname = 'sys_view_test'
order by index_id;

select name, s.nspname, type, type_desc, is_ms_shipped, is_published
from sys.procedures p
inner join pg_namespace s on p.schema_id = s.oid
where s.nspname = 'sys_view_test'
order by object_id;

-- disable index
alter index t_index_func_c4_ind disable;

select t.relname, name, i.type, type_desc, is_disabled
from sys.indexes i
inner join pg_class t on i.object_id = t.oid
inner join pg_namespace s on t.relnamespace = s.oid
where s.nspname = 'sys_view_test' and name = 't_index_func_c4_ind';

drop procedure test_sum;
drop function test_sub;
drop table t_tg_des;
drop table t_tg_src;
drop function tri_insert_func;
drop synonym syn_tbl;
drop table t_orders;
drop sequence seq_order;
drop materialized view mv_normal;
drop view v_check;
drop view v_normal;
drop table t_temp_table;
drop table t_column;
drop table t_foreign2;
drop table t_foreign1;
drop table t_index;

reset search_path;
drop schema sys_view_test cascade;

create schema sys_view_test_02;
set search_path to sys_view_test_02;
-- show views struct
\d information_schema_tsql.check_constraints
\d information_schema_tsql.columns
\d information_schema_tsql.tables
\d information_schema_tsql.views
\d sys.sysdatabases
\d sys.schemas
\d sys.sysusers
\d sys.databases

-- prepare data
CREATE TABLE employees (
    name VARCHAR(50) NOT NULL,
    age INT,
    salary DECIMAL(10, 2),
    -- CHECK 约束：年龄必须 >= 18
    CONSTRAINT chk_age CHECK (age >= 18)
);

CREATE VIEW high_salary_employees AS
SELECT name, age, salary
FROM employees
WHERE age >= 18 AND salary > 5000;

INSERT INTO employees (name, age, salary) VALUES ('张三', 25, 8000.00); 
INSERT INTO employees (name, age, salary) VALUES ('李四', 26, 4000.00); 

-- test select
select constraint_schema, constraint_name, check_clause
from information_schema_tsql.check_constraints c
inner join pg_namespace s on c.constraint_schema = s.nspname
where s.nspname = 'sys_view_test_02';

select table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length,
character_octet_length, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision, character_set_name, collation_catalog,
collation_schema, collation_name, domain_catalog, domain_schema, domain_name
from information_schema_tsql.columns c
inner join pg_namespace s on c.table_schema = s.nspname
where s.nspname = 'sys_view_test_02' order by table_name,column_name;

select table_schema, table_name, table_type
from information_schema_tsql.tables t
inner join pg_namespace s on t.table_schema = s.nspname
where s.nspname = 'sys_view_test_02' order by table_name;

select table_schema, table_name, view_definition, check_option, is_updatable
from information_schema_tsql.views v
inner join pg_namespace s on v.table_schema = s.nspname
where s.nspname = 'sys_view_test_02';

select sid, mode, status, status2, crdate, reserved, category, cmplevel, filename, version
from sys.sysdatabases;

select name, schema_id, principal_id
from sys.schemas where schema_id < 10000;

select *
from sys.sysusers where uid >1000 and uid < 10000;

select *
from sys.databases where database_id = 1;

drop table employees;
drop view high_salary_employees;

reset search_path;
drop schema sys_view_test_02 cascade;
