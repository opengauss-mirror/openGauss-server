create database test_apply dbcompatibility='D';
\c test_apply
create extension shark;
-- cross and outer apply
create table departments(department_name varchar(50), department_id int);
create table employees(employee_id int, department_id int, last_name varchar(50));


insert into departments values ('Marketing', 1);
insert into departments values ('Public Relations', 2);
insert into departments values ('Operations', 3);
insert into departments values ('Develop', 4);
insert into departments values ('Research', 5);
insert into departments values ('CEO', 6);
insert into departments values ('CFO', 7);


insert into employees values(1, 1, 'zhangsan1');
insert into employees values(2, 1, 'zhangsan2');
insert into employees values(3, 1, 'zhangsan3');
insert into employees values(4, 1, 'zhangsan4');


insert into employees values(5, 2, 'lisi1');
insert into employees values(6, 2, 'lisi2');
insert into employees values(7, 2, 'lisi3');
insert into employees values(8, 2, 'lisi4');

insert into employees values(9,  3, 'wangwu1');
insert into employees values(10, 3, 'wangwu2');
insert into employees values(11, 3, 'wangwu3');
insert into employees values(12, 3, 'wangwu4');

insert into employees values(13, 4, 'heliu1');
insert into employees values(14, 4, 'heliu2');
insert into employees values(15, 4, 'heliu3');
insert into employees values(16, 4, 'heliu4');

insert into employees values(17, 5, 'chenqi1');
insert into employees values(18, 5, 'chenqi2');
insert into employees values(19, 5, 'chenqi3');
insert into employees values(20, 5, 'chenqi4');

create function fn_salar (departmentid int) returns table (employee_id int, department_id int, last_name varchar) language sql as 'select employee_id, department_id, concat(last_name,last_name) as last_name2 from employees WHERE department_id = departmentid';


select* from departments d cross apply employees e where e.department_id = d.department_id;

select* from departments d cross apply (select d.department_id from employees x) e where e.department_id = d.department_id;

SELECT * FROM employees AS e CROSS APPLY fn_Salar(e.department_id) AS f;

 
SELECT d.department_name, v.employee_id, v.last_name
  FROM departments d CROSS APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v
  WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations')
  ORDER BY d.department_name, v.employee_id;
  

select* from departments d outer apply employees e where e.department_id = d.department_id;

select* from departments d cross apply (select d.department_id from employees x) e where e.department_id = d.department_id;

SELECT * FROM employees AS e outer APPLY fn_Salar(e.department_id) AS f;

SELECT d.department_name, v.employee_id, v.last_name
  FROM departments d outer APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v
  WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations')
  ORDER BY d.department_name, v.employee_id;

create view v1 as SELECT d.department_name, v.employee_id, v.last_name
  FROM departments d CROSS APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v
  WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations')
  ORDER BY d.department_name, v.employee_id;


create view v2 as SELECT d.department_name, v.employee_id, v.last_name
  FROM departments d outer APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v
  WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations')
  ORDER BY d.department_name, v.employee_id;

select * from v1;
select * from v2;

CREATE TABLE tt1 AS
SELECT d.department_name, v.employee_id, v.last_name
FROM departments d
CROSS APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v
WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations')
ORDER BY d.department_name, v.employee_id;

CREATE TABLE tt2 AS
SELECT d.department_name, v.employee_id, v.last_name
FROM departments d
OUTER APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v
WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations')
ORDER BY d.department_name, v.employee_id;

select * from tt1;
select * from tt2;

create or replace procedure test_cursor_1 
as 
    company_name    varchar(100);
    last_name_name    varchar(100);
	type ref_cur_type is ref cursor;
    my_cur ref_cur_type;
    cursor c1 is SELECT e.last_name, CURSOR(SELECT d.department_name FROM departments d CROSS APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations') ORDER BY d.department_name, v.employee_id) abc FROM employees e;
begin 
    OPEN c1;
    loop
        fetch c1 into company_name, my_cur;
        exit when c1%notfound;
	    raise notice 'company_name : %  %',company_name, my_cur;
	    loop
	        fetch my_cur into last_name_name;
            exit when my_cur%notfound;
            raise notice '     last_name_name : %',last_name_name;
	    end loop;
    end loop; 
end;
/
drop procedure test_cursor_1;


create or replace procedure test_cursor_2 
as 
    company_name    varchar(100);
    last_name_name    varchar(100);
	type ref_cur_type is ref cursor;
    my_cur ref_cur_type;
    cursor c1 is SELECT e.last_name, CURSOR(SELECT d.department_name FROM departments d outer APPLY (SELECT * FROM employees e WHERE e.department_id = d.department_id) v WHERE d.department_name IN ('Marketing', 'Operations', 'Public Relations') ORDER BY d.department_name, v.employee_id) abc FROM employees e;
begin 
    OPEN c1;
    loop
        fetch c1 into company_name, my_cur;
        exit when c1%notfound;
	    raise notice 'company_name : %  %',company_name, my_cur;
	    loop
	        fetch my_cur into last_name_name;
            exit when my_cur%notfound;
            raise notice '     last_name_name : %',last_name_name;
	    end loop;
    end loop; 
end;
/
drop procedure test_cursor_2;

drop view v1;
drop view v2;
drop table departments;
drop table employees;

\c postgres
drop database test_apply;