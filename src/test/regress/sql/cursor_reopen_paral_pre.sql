drop table if exists employees;
drop table if exists employees2;
create table employees (
   last_name varchar(20),
   job_id int
);
create table employees2 (
   age int,
   dep_id int
);

insert into employees values ('wang',1), ('hu',2), ('zhou',3);
insert into employees2 values (1,1), (2,2), (3,3);

CREATE or REPLACE PROCEDURE proc1 AS
DECLARE
    cv SYS_REFCURSOR;
    v_lastname  employees.last_name%TYPE;
    v_jobid     employees.job_id%TYPE;
    v_age       employees2.age%TYPE;
    v_depid     employees2.dep_id%TYPE;
    query_2 VARCHAR2(200) := 'SELECT * FROM employees2 order by age limit 1';
    v_employees employees%ROWTYPE;
BEGIN
    OPEN cv FOR
        SELECT last_name, job_id FROM employees
        order by job_id limit 1;
    LOOP
        FETCH cv INTO v_lastname, v_jobid;
        EXIT WHEN cv%NOTFOUND;
        raise info 'v_lastname is %',v_lastname;
        raise info 'v_jobid is %',v_jobid;
        insert into employees(last_name, job_id) values (v_lastname, v_jobid + 10);
    END LOOP;
    raise info '----------------------------';
    OPEN cv FOR query_2;
    LOOP
        FETCH cv INTO v_age, v_depid;
        EXIT WHEN cv%NOTFOUND;
        raise info 'v_age is %',v_age;
        raise info 'v_depid is %',v_depid;
        insert into employees2(age, dep_id) values (v_age + 10, v_depid + 10);
    END LOOP;
    raise info '----------------------------';
END;
/

CREATE or REPLACE PROCEDURE proc2 AS
DECLARE
    cv SYS_REFCURSOR;
    v_lastname  employees.last_name%TYPE;
    v_jobid     employees.job_id%TYPE;
    v_age       employees2.age%TYPE;
    v_depid     employees2.dep_id%TYPE;
    query_2 VARCHAR2(200) := 'SELECT * FROM employees2 order by age limit 1';
    v_employees employees%ROWTYPE;
BEGIN
    OPEN cv FOR
        SELECT last_name, job_id FROM employees
        order by job_id limit 1;
    LOOP
        FETCH cv INTO v_lastname, v_jobid;
        EXIT WHEN cv%NOTFOUND;
        raise info 'v_lastname is %',v_lastname;
        raise info 'v_jobid is %',v_jobid;
        insert into employees(last_name, job_id) values (v_lastname, v_jobid + 100);
    END LOOP;
    raise info '----------------------------';
    OPEN cv FOR query_2;
    LOOP
        FETCH cv INTO v_age, v_depid;
        EXIT WHEN cv%NOTFOUND;
        raise info 'v_age is %',v_age;
        raise info 'v_depid is %',v_depid;
        insert into employees2(age, dep_id) values (v_age + 100, v_depid + 100);
    END LOOP;
    raise info '----------------------------';
END;
/