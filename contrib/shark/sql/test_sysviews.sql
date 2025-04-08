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

reset search_path;
drop schema sys_view_test cascade;
