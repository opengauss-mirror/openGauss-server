create schema sys1;

set search_path to 'sys1';

CREATE TABLE sys1.students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT DEFAULT 0,
    grade DECIMAL(5, 2)
);

CREATE VIEW sys1.student_grades AS
SELECT name, grade
FROM students;

CREATE OR REPLACE FUNCTION sys1.calculate_total_grade()
RETURNS DECIMAL(10, 2) AS $$
DECLARE
    total_grade DECIMAL(10, 2) := 0;
BEGIN
    SELECT SUM(grade) INTO total_grade FROM students;
    RETURN total_grade;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sys1.insert_student(
    student_name VARCHAR(100),
    student_age INT,
    student_grade DECIMAL(5, 2)
) AS
BEGIN
    INSERT INTO students (name, age, grade) VALUES (student_name, student_age, student_grade);
END;
/

CREATE OR REPLACE FUNCTION sys1.update_total_grade()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Total grade updated: %', calculate_total_grade();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_total_grade
AFTER INSERT OR UPDATE OR DELETE ON students
FOR EACH STATEMENT EXECUTE PROCEDURE update_total_grade();

select object_id('sys1.students');
select object_id('sys1.students_pkey');
select object_id('sys1.student_grades');
select object_id('sys1.calculate_total_grade');
select object_id('sys1.insert_student');
select object_id('sys1.trigger_update_total_grade');

set search_path = 'sys1';
select object_id('contrib_regression.students');
select object_id('contrib_regression..students');
select object_id('contrib_regression...students');

select object_id('students', 'U');
select object_id('students_pkey', 'PK');
select object_id('student_grades', 'V');
select object_id('calculate_total_grade', 'FN');
select object_id('insert_student', 'P');
select object_id('trigger_update_total_grade', 'TR');

select object_id('sys1.students');
select object_id('contrib_regression.sys1.students');

select object_id('students', '');
select object_id('students','s');
select object_id('students','S');
select object_id('students','u');
select object_id('students',' u');
select object_id('students','v');
select object_id('students','u ');

select objectproperty(object_id('students'), 'istable') as istable;
select objectproperty(object_id('students'), 'ownerid') as ownerid;
select objectproperty(object_id('students'), 'isdefaultcnst') as isdefaultcnst;
select objectproperty(object_id('students'), 'execisquotedidenton') as execisquotedidenton;
select objectproperty(object_id('students'), 'isschemabound') as isschemabound;
select objectproperty(object_id('students'), 'execisansinullson') as execisansinullson;
select objectproperty(object_id('students'), 'tablefulltextpopulatestatus') as tablefulltextpopulatestatus;
select objectproperty(object_id('students'), 'tablehasvardecimalstorageformat') as tablehasvardecimalstorageformat;
select objectproperty(object_id('students'), 'issysshipped') as issysshipped;
select objectproperty(object_id('students'), 'isdeterministic') as isdeterministic;
select objectproperty(object_id('students'), 'isprocedure') asisprocedure;
select objectproperty(object_id('students'), 'isview') as isview;
select objectproperty(object_id('students'), 'isusertable') as isusertable;
select objectproperty(object_id('students'), 'istablefunction') as istablefunction;
select objectproperty(object_id('students'), 'isinlinefunction') as isinlinefunction;
select objectproperty(object_id('students'), 'isscalarfunction') as isscalarfunction;
select objectproperty(object_id('students'), 'isprimarykey') as isprimarykey;
select objectproperty(object_id('students'), 'isindexed') as isindexed;
select objectproperty(object_id('students'), 'isdefault') as isdefault;
select objectproperty(object_id('students'), 'isrule') as isrule;
select objectproperty(object_id('students'), 'istrigger') as istrigger;

select objectproperty(object_id('student_grades'), 'istable') as istable;
select objectproperty(object_id('student_grades'), 'ownerid') as ownerid;
select objectproperty(object_id('student_grades'), 'isdefaultcnst') as isdefaultcnst;
select objectproperty(object_id('student_grades'), 'execisquotedidenton') as execisquotedidenton;
select objectproperty(object_id('student_grades'), 'isschemabound') as isschemabound;
select objectproperty(object_id('student_grades'), 'execisansinullson') as execisansinullson;
select objectproperty(object_id('student_grades'), 'tablefulltextpopulatestatus') as tablefulltextpopulatestatus;
select objectproperty(object_id('student_grades'), 'tablehasvardecimalstorageformat') as tablehasvardecimalstorageformat;
select objectproperty(object_id('student_grades'), 'issysshipped') as issysshipped;
select objectproperty(object_id('student_grades'), 'isdeterministic') as isdeterministic;
select objectproperty(object_id('student_grades'), 'isprocedure') asisprocedure;
select objectproperty(object_id('student_grades'), 'isview') as isview;
select objectproperty(object_id('student_grades'), 'isusertable') as isusertable;
select objectproperty(object_id('student_grades'), 'istablefunction') as istablefunction;
select objectproperty(object_id('student_grades'), 'isinlinefunction') as isinlinefunction;
select objectproperty(object_id('student_grades'), 'isscalarfunction') as isscalarfunction;
select objectproperty(object_id('student_grades'), 'isprimarykey') as isprimarykey;
select objectproperty(object_id('student_grades'), 'isindexed') as isindexed;
select objectproperty(object_id('student_grades'), 'isdefault') as isdefault;
select objectproperty(object_id('student_grades'), 'isrule') as isrule;
select objectproperty(object_id('student_grades'), 'istrigger') as istrigger;

select objectproperty(object_id('calculate_total_grade'), 'istable') as istable;
select objectproperty(object_id('calculate_total_grade'), 'ownerid') as ownerid;
select objectproperty(object_id('calculate_total_grade'), 'isdefaultcnst') as isdefaultcnst;
select objectproperty(object_id('calculate_total_grade'), 'execisquotedidenton') as execisquotedidenton;
select objectproperty(object_id('calculate_total_grade'), 'isschemabound') as isschemabound;
select objectproperty(object_id('calculate_total_grade'), 'execisansinullson') as execisansinullson;
select objectproperty(object_id('calculate_total_grade'), 'tablefulltextpopulatestatus') as tablefulltextpopulatestatus;
select objectproperty(object_id('calculate_total_grade'), 'tablehasvardecimalstorageformat') as tablehasvardecimalstorageformat;
select objectproperty(object_id('calculate_total_grade'), 'issysshipped') as issysshipped;
select objectproperty(object_id('calculate_total_grade'), 'isdeterministic') as isdeterministic;
select objectproperty(object_id('calculate_total_grade'), 'isprocedure') asisprocedure;
select objectproperty(object_id('calculate_total_grade'), 'isview') as isview;
select objectproperty(object_id('calculate_total_grade'), 'isusertable') as isusertable;
select objectproperty(object_id('calculate_total_grade'), 'istablefunction') as istablefunction;
select objectproperty(object_id('calculate_total_grade'), 'isinlinefunction') as isinlinefunction;
select objectproperty(object_id('calculate_total_grade'), 'isscalarfunction') as isscalarfunction;
select objectproperty(object_id('calculate_total_grade'), 'isprimarykey') as isprimarykey;
select objectproperty(object_id('calculate_total_grade'), 'isindexed') as isindexed;
select objectproperty(object_id('calculate_total_grade'), 'isdefault') as isdefault;
select objectproperty(object_id('calculate_total_grade'), 'isrule') as isrule;
select objectproperty(object_id('calculate_total_grade'), 'istrigger') as istrigger;

select objectproperty(object_id('insert_student'), 'istable') as istable;
select objectproperty(object_id('insert_student'), 'ownerid') as ownerid;
select objectproperty(object_id('insert_student'), 'isdefaultcnst') as isdefaultcnst;
select objectproperty(object_id('insert_student'), 'execisquotedidenton') as execisquotedidenton;
select objectproperty(object_id('insert_student'), 'isschemabound') as isschemabound;
select objectproperty(object_id('insert_student'), 'execisansinullson') as execisansinullson;
select objectproperty(object_id('insert_student'), 'tablefulltextpopulatestatus') as tablefulltextpopulatestatus;
select objectproperty(object_id('insert_student'), 'tablehasvardecimalstorageformat') as tablehasvardecimalstorageformat;
select objectproperty(object_id('insert_student'), 'issysshipped') as issysshipped;
select objectproperty(object_id('insert_student'), 'isdeterministic') as isdeterministic;
select objectproperty(object_id('insert_student'), 'isprocedure') asisprocedure;
select objectproperty(object_id('insert_student'), 'isview') as isview;
select objectproperty(object_id('insert_student'), 'isusertable') as isusertable;
select objectproperty(object_id('insert_student'), 'istablefunction') as istablefunction;
select objectproperty(object_id('insert_student'), 'isinlinefunction') as isinlinefunction;
select objectproperty(object_id('insert_student'), 'isscalarfunction') as isscalarfunction;
select objectproperty(object_id('insert_student'), 'isprimarykey') as isprimarykey;
select objectproperty(object_id('insert_student'), 'isindexed') as isindexed;
select objectproperty(object_id('insert_student'), 'isdefault') as isdefault;
select objectproperty(object_id('insert_student'), 'isrule') as isrule;
select objectproperty(object_id('insert_student'), 'istrigger') as istrigger;

select objectproperty(object_id('trigger_update_total_grade'), 'istable') as istable;
select objectproperty(object_id('trigger_update_total_grade'), 'ownerid') as ownerid;
select objectproperty(object_id('trigger_update_total_grade'), 'isdefaultcnst') as isdefaultcnst;
select objectproperty(object_id('trigger_update_total_grade'), 'execisquotedidenton') as execisquotedidenton;
select objectproperty(object_id('trigger_update_total_grade'), 'isschemabound') as isschemabound;
select objectproperty(object_id('trigger_update_total_grade'), 'execisansinullson') as execisansinullson;
select objectproperty(object_id('trigger_update_total_grade'), 'tablefulltextpopulatestatus') as tablefulltextpopulatestatus;
select objectproperty(object_id('trigger_update_total_grade'), 'tablehasvardecimalstorageformat') as tablehasvardecimalstorageformat;
select objectproperty(object_id('trigger_update_total_grade'), 'issysshipped') as issysshipped;
select objectproperty(object_id('trigger_update_total_grade'), 'isdeterministic') as isdeterministic;
select objectproperty(object_id('trigger_update_total_grade'), 'isprocedure') asisprocedure;
select objectproperty(object_id('trigger_update_total_grade'), 'isview') as isview;
select objectproperty(object_id('trigger_update_total_grade'), 'isusertable') as isusertable;
select objectproperty(object_id('trigger_update_total_grade'), 'istablefunction') as istablefunction;
select objectproperty(object_id('trigger_update_total_grade'), 'isinlinefunction') as isinlinefunction;
select objectproperty(object_id('trigger_update_total_grade'), 'isscalarfunction') as isscalarfunction;
select objectproperty(object_id('trigger_update_total_grade'), 'isprimarykey') as isprimarykey;
select objectproperty(object_id('trigger_update_total_grade'), 'isindexed') as isindexed;
select objectproperty(object_id('trigger_update_total_grade'), 'isdefault') as isdefault;
select objectproperty(object_id('trigger_update_total_grade'), 'isrule') as isrule;
select objectproperty(object_id('trigger_update_total_grade'), 'istrigger') as istrigger;

select objectproperty(object_id('students_pkey'), 'istable') as istable;
select objectproperty(object_id('students_pkey'), 'ownerid') as ownerid;
select objectproperty(object_id('students_pkey'), 'isdefaultcnst') as isdefaultcnst;
select objectproperty(object_id('students_pkey'), 'execisquotedidenton') as execisquotedidenton;
select objectproperty(object_id('students_pkey'), 'isschemabound') as isschemabound;
select objectproperty(object_id('students_pkey'), 'execisansinullson') as execisansinullson;
select objectproperty(object_id('students_pkey'), 'tablefulltextpopulatestatus') as tablefulltextpopulatestatus;
select objectproperty(object_id('students_pkey'), 'tablehasvardecimalstorageformat') as tablehasvardecimalstorageformat;
select objectproperty(object_id('students_pkey'), 'issysshipped') as issysshipped;
select objectproperty(object_id('students_pkey'), 'isdeterministic') as isdeterministic;
select objectproperty(object_id('students_pkey'), 'isprocedure') asisprocedure;
select objectproperty(object_id('students_pkey'), 'isview') as isview;
select objectproperty(object_id('students_pkey'), 'isusertable') as isusertable;
select objectproperty(object_id('students_pkey'), 'istablefunction') as istablefunction;
select objectproperty(object_id('students_pkey'), 'isinlinefunction') as isinlinefunction;
select objectproperty(object_id('students_pkey'), 'isscalarfunction') as isscalarfunction;
select objectproperty(object_id('students_pkey'), 'isprimarykey') as isprimarykey;
select objectproperty(object_id('students_pkey'), 'isindexed') as isindexed;
select objectproperty(object_id('students_pkey'), 'isdefault') as isdefault;
select objectproperty(object_id('students_pkey'), 'isrule') as isrule;
select objectproperty(object_id('students_pkey'), 'istrigger') as istrigger;

--异常用例
CREATE TEMP TABLE sys1.temp_sales (
    product_name VARCHAR(255),
    sale_amount NUMERIC(10, 2),
    sale_date DATE
);
select object_id('temp_sales');
select objectproperty(object_id('temp_sales'), 'istable') as istable;

select object_id();
select object_id('[]');
select object_id('student');
select object_id('', 'U');
select object_id('students', '');
select object_id('students', 'FN');
select object_id('othersys.students');
select object_id('otherdb.sys.students');

select objectproperty('', 'istable') as istable;
select objectproperty('students', 'istable') as istable;
select objectproperty(object_id('students'), '') as istable;
select objectproperty('', '') as istable;
select objectproperty(object_id('student'), 'istable') as istable;
select objectproperty(object_id('students'), 'isview') as istable;
select objectproperty(object_id('students'), 'ssr') as istable;

create user object_user identified by 'Test@123';
SET SESSION AUTHORIZATION object_user PASSWORD 'Test@123';
create schema object_schema;
create table object_schema.t1 (n1 int);

RESET SESSION AUTHORIZATION;
select object_id('object_schema.t1');

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT DEFAULT 0,
    grade DECIMAL(5, 2)
);
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (
    id INT,
    name VARCHAR(100) NOT NULL,
    age INT DEFAULT 0,
    grade DECIMAL(5, 2)
);

select objectproperty(object_id('t1'), 'isindexed') as isindexed;
select objectproperty(object_id('t2'), 'isindexed') as isindexed;

select objectproperty(object_id('t1'), 'tablefulltextpopulatestatus') as isindexed;
select objectproperty(object_id('t2'), 'tablefulltextpopulatestatus') as isindexed;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
reset current_schema;
drop schema sys1 cascade; 