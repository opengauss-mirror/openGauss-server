-- test for function and procedure.

create database nested_subprogram;
\c nested_subprogram

-- subprogram expect error.
CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=0;  
function sum1(a int, b int) return int;
procedure proc1();
BEGIN
q = sum1(3,4);
return q;
END;
$$ LANGUAGE plpgsql;


-- redefine
CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=0;  
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
	return res;
end;
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a - b;
	return res;
end;  
BEGIN
q = sum1(3,4);
return q;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=0;  
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
	return res;
end; 
BEGIN
q = sum1(3,4);
return q;
END;
$$ LANGUAGE plpgsql;
select func();

select prokind from pg_proc where proname = 'sum1';

CREATE or replace FUNCTION func(a int, b int) returns int AS $$
DECLARE 
ans int:=0;
function sum1(c int) return int
as
declare
    var int:=5;
	function sum2() return int
	as
	declare
		res int:=0;
	begin 
		res = res + c;
        return res;
	end;
begin 
    return sum2()+var+b;
end;  
BEGIN
sum2();
ans = sum1(a);
return ans;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=0;
p char:='a'; 
function sum1(c int) return int
as
declare
    res int;
begin 
    res = c;
	return res;
end; 
BEGIN
q = sum1(p);
return q;
END;
$$ LANGUAGE plpgsql;
select func();

-- error function call.
CREATE or replace FUNCTION func(a int, b int) returns int AS $$
DECLARE 
ans int:=0;
function sum1(c int) return int
as
declare
    var int:=5;
	function sum2() return int
	as
	declare
		res int:=0;
	begin 
		res = res + c;
        return res;
	end;
begin 
    return sum2()+var+b;
end;  
BEGIN
sum2();
ans = sum1(a);
return ans;
END;
$$ LANGUAGE plpgsql;


-- error function call.
CREATE or replace PROCEDURE proc(a int, b int) AS
DECLARE 
ans int:=0;
function sum1(c int) return int
as
declare
    var int:=5;
	function sum2() return int
	as
	declare
		res int:=0;
	begin 
		res = res + c;
        return res;
	end;
begin 
    return sum2()+var+b;
end;  
BEGIN
sum2();
ans = sum1(a);
raise notice 'ans=%',ans;
END;
/

CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=0;  
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
	return res;
end; 
BEGIN
q = sum1(3,4);
return q;
END;
$$ LANGUAGE plpgsql;
select func();

CREATE or replace FUNCTION func(a int, b int) returns int AS $$
DECLARE 
q int :=0;  
function sum1(c int, d int) return int
as
declare
    res int:=0;
begin 
    res = a + b + c + d;
	return res;
end; 
BEGIN
q = sum1(a,b);
return q;
END;
$$ LANGUAGE plpgsql;
select func(1,2);


CREATE or replace FUNCTION func(a int, b int, in c int) returns int AS $$
DECLARE 
q int:=2;  
function sum1(c1 int) return int
as
declare
    res int:=0;
begin 
    raise notice 'a=%', a;
    c = a-b;
    res = a + b + c1;
	return res;
end;
BEGIN
q = sum1(2);
raise notice 'q=%',q;
raise notice 'c=%',c;
return q;
END;
$$ LANGUAGE plpgsql;
select func(3,5,8);


CREATE or replace FUNCTION func(a int, b int, out c int) returns int AS $$
DECLARE 
q int:=2;  
function sum1(c1 int) return int
as
declare
    res int:=0;
begin 
    raise notice 'a=%', a;
    res = a + b + c1;
    c = res;
    raise notice 'c=%', c;
	return res;
end;
BEGIN
raise notice 'q=%',q;
a = a - 1;
b = b - 1;
q = sum1(2);
return q;
END;
$$ LANGUAGE plpgsql;
select func(3,5);


-- override
CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=5;
p char:='a';
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
	return res;
end;
function sum1(a int) return int
as
declare
    res int:=0;
begin 
    res = q + a;
	return res;
end;
function sum1(c char) return char
as
declare
    res char;
begin 
    res = c;
	return res;
end;
function sum1() return int
as
declare
    res int:=0;
begin 
    res = q + q;
	return res;
end;
BEGIN
q = sum1(3,4);
q = sum1(3);
q = sum1();
p = sum1('b');
raise notice 'p=%',p;
return q;
END;
$$ LANGUAGE plpgsql;
select func();


-- double nested
CREATE or replace FUNCTION func(a int, b int) returns int AS $$
DECLARE 
ans int:=0;
function sum1(c int) return int
as
declare
    var int:=5;
	function sum2() return int
	as
	declare
		res int:=0;
	begin 
		res = res + c;
        return res;
	end;
begin 
    return sum2()+var+b;
end;  
BEGIN 
ans = sum1(a);
return ans;
END;
$$ LANGUAGE plpgsql;
select func(1,2);


-- function call
CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=0;  
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
    raise notice 'func.sum1()';
	return res;
end; 
BEGIN
q = sum1(3, 4);
return q;
END;
$$ LANGUAGE plpgsql;


CREATE or replace FUNCTION sum1(a int, b int) returns int AS $$
DECLARE 
   res int:=0;
BEGIN
    res = a + b - 1;
    raise notice 'public.sum1()';
    return res;
END;
$$ LANGUAGE plpgsql;

-- call function subprogram
select func();

-- call public function
select sum1(3,4);

drop function sum1(int,int);
drop function func();


CREATE or replace PROCEDURE proc(a int, b int, in c char) AS
DECLARE 
q int:=2;  
function sum1(c1 int) return int
as
declare
    res int:=0;
begin 
    raise notice 'a=%',a;
    c = 'b';
    raise notice 'c=%',c;
    res = a + b + c1;
    a = a - b;
    b = b - a;
	return res;
end;
BEGIN
raise notice 'q=%',q;
q = sum1(2);
raise notice 'a=%',a;
raise notice 'b=%',b;
raise notice 'c=%',c;
raise notice 'q=%',q;
END;
/
select proc(3,4,'a');


CREATE or replace PROCEDURE proc(a int, b int, out c int) AS
DECLARE 
q int:=2;  
function sum1(c1 int) return int
as
declare
    res int:=0;
begin 
    raise notice 'a=%', a;
    res = a + b + c1;
    c = res;
    raise notice 'c=%', c;
	return res;
end;
BEGIN
raise notice 'q=%',q;
a = a - 1;
b = b - 1;
q = sum1(2);
c = q - 2;
raise notice 'c=%', c;
raise notice 'q=%',q;
END;
/
select proc(3,5);


-- override
CREATE or replace PROCEDURE proc() AS
DECLARE 
q int :=5;
p char:='a';
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
	return res;
end;
function sum1(a int) return int
as
declare
    res int:=0;
begin 
    res = q + a;
	return res;
end;
function sum1(c char) return char
as
declare
    res char;
begin 
    res = c;
	return res;
end;
function sum1() return int
as
declare
    res int:=0;
begin 
    res = q + q;
	return res;
end;
BEGIN
q = sum1(3,4);
q = sum1(3);
q = sum1();
p = sum1('b');
raise notice 'q=%',q;
END;
/
select proc();


-- double nested
CREATE or replace PROCEDURE proc(a int, b int) AS
DECLARE 
ans int:=0;
function sum1(c int) return int
as
declare
    var int:=5;
	function sum2() return int
	as
	declare
		res int:=0;
	begin 
		res = res + c;
        return res;
	end;
begin 
    return sum2() + var + b;
end;  
BEGIN 
ans = sum1(a);
raise notice 'ans=%',ans;
END;
/
select proc(1,2);


-- inline block test
-- subprogram which declare or define error.

-- redefine
DECLARE 
q int :=0;  
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
	return res;
end;
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a - b;
	return res;
end;  
BEGIN
q = sum1(3,4);
raise notice 'q=%',q;
END;
/


DECLARE 
q int :=0;  
function sum1(c int, d int) return int
as
declare
    res int:=0;
begin 
    res = c + d;
	return res;
end; 
BEGIN
q = sum1(1,5);
raise notice 'q=%',q;
END;
/

DECLARE 
q int:=2;
a int:=3;
b int:=4;
function sum1(c1 int) return int
as
declare
    res int:=0;
begin 
    raise notice 'a=%', a;
    res = a + b;
	return res;
end;
BEGIN
raise notice 'q=%',q;
q = sum1(2);
raise notice 'q=%',q;
END;
/

DECLARE 
q int:=2;
a int:=1;
b int:=3;
c int:=4; 
function sum1(c1 int) return int
as
declare
    res int:=0;
begin 
    raise notice 'a=%', a;
    c = a-b;
    raise notice 'c=%',c;
    res = a + b;
	return res;
end;
BEGIN
raise notice 'q=%',q;
q = sum1(2);
c = sum1(3);
raise notice 'c=%',c;
raise notice 'q=%',q;
END;
/

DECLARE 
q int:=2;
a int:=3;
b int:=5;
c int:=8;  
function sum1(c1 int) return int
as
declare
    res int:=0;
begin 
    raise notice 'a=%', a;
    res = a + b + c1;
    c = res;
    raise notice 'c=%', c;
	return res;
end;
BEGIN
raise notice 'q=%',q;
a = a - 1;
b = b - 1;
q = sum1(2);
c = c - 2;
raise notice 'c=%', c;
raise notice 'q=%',q;
END;
/

-- override
DECLARE 
q int :=5;
p char:='a';
function sum1(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a + b;
	return res;
end;
function sum1(a int) return int
as
declare
    res int:=0;
begin 
    res = q + a;
	return res;
end;
function sum1(c char) return char
as
declare
    res char;
begin 
    res = c;
	return res;
end;
function sum1() return int
as
declare
    res int:=0;
begin 
    res = q + q;
	return res;
end;
BEGIN
q = sum1(3,4);
q = sum1(3);
q = sum1();
raise notice 'q=%',q;
p = sum1(('b'::char));
raise notice 'p=%',p;
END;
/

-- double nested
DECLARE 
ans int:=0;
a int:=3;
b int:=5;
function sum1(c int) return int
as
declare
    var int:=5;
	function sum2() return int
	as
	declare
		res int:=0;
	begin 
		res = res + c;
        return res;
	end;
begin 
    return sum2()+var+b;
end;  
BEGIN 
ans = sum1(a);
raise notice 'ans=%',ans;
END;
/

CREATE or replace FUNCTION func(a int, b int) returns int AS $$
DECLARE 
type  test_type is record (c1 int, c2 char);
type1 test_type;
q int=0;
function test(c1 int) return record
as
declare
type2 test_type;
begin
type2.c1 = 2;
type2.c2 = 'b';
q=3;
raise notice 'type2.c1=%',type2.c1;
raise notice 'type2.c2=%',type2.c2;
return type2;
end;
BEGIN
type1.c1 = 1;
type1.c2 = 'a';
q=1;
raise notice 'type1.c1=%',type1.c1;
raise notice 'type1.c2=%',type1.c2;
raise notice 'q=%',q;
type1 = test(1);
raise notice 'type1.c1=%',type1.c1;
raise notice 'type1.c2=%',type1.c2;
raise notice 'q=%',q;
return q;
END;
$$ LANGUAGE plpgsql;
select func(1,2);


CREATE or replace FUNCTION func(a int, b int) returns int AS $$
DECLARE 
q int :=0;
type students is varray(2) of int;
names students;
function sum1(c int, d int) return int
as
declare
    res int:=0;
begin 
    names(1) := 3;
    names(2) := 4;
	return q;
end; 
BEGIN  
names := students(1, 2);
raise notice 'names(0)=%',names(1);
raise notice 'names(1)=%',names(2);
sum1(a,b);
raise notice 'names(0)=%',names(1);
raise notice 'names(1)=%',names(2);
return q;
END;
$$ LANGUAGE plpgsql;
select func(1,2);


--exception
CREATE or replace FUNCTION sum0(a int, b int) returns int AS $$
DECLARE 
   res int:=0;
BEGIN
    res = a + b - 1;
    return res;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION devi(a int, b int) returns int AS $$
DECLARE 
   res int:=0;
BEGIN
    res = a / b;
    return res;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION func() returns int AS $$
DECLARE 
q int :=0;  
function devi0(a int, b int) return int
as
declare
    res int:=0;
begin 
    res = a / b;
    EXCEPTION
    WHEN division_by_zero THEN
        RAISE NOTICE 'caught division_by_zero';
    RETURN sum0(1,2);
	return res;
end; 
BEGIN
q = devi0(4,0);
q = devi(4,2);
return q;
END;
$$ LANGUAGE plpgsql;
select func();

CREATE OR REPLACE procedure subpro_nest_pro
IS
TYPE My_AA IS TABLE OF VARCHAR2(20);
FUNCTION Init_My_AA RETURN My_AA IS
Ret My_AA;
BEGIN
Ret(1) := 'one';
Ret(2) := 'two';
Ret(3) := 'three';
Ret(2147483647) := 'test_end';
Ret(0) := 'zero';
Ret(-10) := '-ten';
RETURN Ret;
END;
begin
DECLARE
v CONSTANT My_AA :=Init_My_AA();
BEGIN
DECLARE
Idx int := v.FIRST;
BEGIN
WHILE Idx IS NOT NULL LOOP
raise notice 'v(Idx)=%',v(Idx);
--DBMS_OUTPUT.PUT_LINE(TO_CHAR(Idx)||' '||v(Idx));
Idx := v.NEXT(Idx);
END LOOP;
end;
END;
END;
/

call subpro_nest_pro();


--多次调用子程序
DECLARE
PROCEDURE p ( grade varchar2)
IS
appraisal VARCHAR2(20);
BEGIN
appraisal:=
CASE grade
WHEN 'A' THEN 'Excellent'
WHEN 'B' THEN 'Very Good'
WHEN 'C' THEN 'Good'
WHEN 'D' THEN 'Fair'
WHEN 'F' THEN 'Poor'
ELSE 'No such grade'
END;
raise notice 'Grade % is %',grade,appraisal;
END;
BEGIN
p('A');
p('B');
p('C');
p('D');
p('F');
p('E');
END;
/


--子程序相互调用
DECLARE
TYPE Roster IS TABLE OF VARCHAR2(15); -- nested table type
names Roster := Roster('D Caruso', 'JJ Hamil', 'D Piro', 'R Singh');

PROCEDURE print_names (heading VARCHAR2) IS
TYPE Roster IS TABLE OF VARCHAR2(15); -- nested table type
names Roster := Roster('D Caruso', 'JJ Hamil', 'D Piro', 'R Singh');
BEGIN
raise notice 'heading=%,names(1)=%,------',heading,names(1);
END;

PROCEDURE print_names1 (heading VARCHAR2) IS
TYPE Roster IS TABLE OF VARCHAR2(15); -- nested table type
names Roster := Roster('JJ Hamil', 'D Piro', 'R Singh');
BEGIN
raise notice 'heading=%,names(2)=%,------',heading,names(2);
print_names('Initial Values:');
END;

BEGIN
print_names('name(1):');
print_names1('name(2):');
END;
/


--子程序使用外层游标
create table emp_sub_cur_tb2
            ( employee_id number(6,0) primary key,
            first_name varchar2(20),
            last_name varchar2(25),
            department_id int);
INSERT INTO emp_sub_cur_tb2 VALUES ('100', 'Steven', 'King',30);
INSERT INTO emp_sub_cur_tb2 VALUES ('101', 'Neena', 'Kochhar',30);
INSERT INTO emp_sub_cur_tb2 VALUES ('102', 'Lex', 'De Haan',20);
INSERT INTO emp_sub_cur_tb2 VALUES ('103', 'Alexander', 'Hunold',30);
INSERT INTO emp_sub_cur_tb2 VALUES ('104', 'Bruce', 'Ernst',20);
INSERT INTO emp_sub_cur_tb2 VALUES ('105', 'David', 'Austin',10);

create or replace procedure subprogram_cur_1_3 (id2 int)
as
tmp varchar2(20);
CURSOR cur IS select employee_id from emp_sub_cur_tb2 where department_id= id2 order by 1;
id5 int;
function q ( id1 int) return int
IS
id4 int;
BEGIN
id4:=id1;
return id4;
END;

function p ( id1 int) return varchar2
IS
id3 int;
BEGIN
open cur;
LOOP
FETCH cur INTO id3;
EXIT WHEN cur%NOTFOUND;
raise notice 'q(id3)=%',q(id3);
END LOOP;
CLOSE cur;
return 'ok';
END;

BEGIN
open cur;
LOOP
FETCH cur INTO id5;
EXIT WHEN cur%NOTFOUND;
raise notice 'q(id5)=%',q(id5);
END LOOP;
CLOSE cur;
raise notice '----------------';
tmp:=p(id2);
raise notice 'tmp=%',tmp;
raise notice 'tmp=%',tmp;
raise notice '----------------';
tmp:=q(id2);
raise notice 'tmp=%',tmp;
raise notice '----------------';
END;
/

call subprogram_cur_1_3(20);

drop procedure subprogram_cur_1_3();
drop table emp_sub_cur_tb2;
drop procedure subpro_nest_pro();

CREATE TABLE employees (
    employee_id numeric(6,0),
    first_name varchar(20),
    last_name varchar(25),
    email varchar(25),
    phone_number varchar(20),
    hire_date date,
    job_id varchar(10),
    salary numeric(8,2),
    commission_pct numeric(2,2),
    manager_id numeric(6,0),
    department_id numeric(4,0)
)
WITH (orientation=row, compression=no, fillfactor=80);

insert into employees values (162,	'Clara',	'Vishney', 'CVISHNEY',	'011.44.1346.129268',	'2005-11-11',   'SA_REP',	10500,	.25,	147,    80),
                             (163,	'Danielle',	'Greene',   'DGREENE',	'011.44.1346.229268',	'2007-03-19',	'SA_REP',	9500,	.15,    147,	80),
                             (164,	'Mattea',	'Marvins',  'MMARVINS',	'011.44.1346.329268',	'2008-01-24',	'SA_REP',	7200,	.1, 	147,	80),
                             (165,	'David',	'Lee',      'DLEE',	    '011.44.1346.529268',	'2008-02-23',	'SA_REP',	6800,	.1,	    147,	80),
                             (166,	'Sundar',	'Ande',     'SANDE',	'011.44.1346.629268',	'2008-03-24',	'SA_REP',	6400,	.1,	    147,	80),
                             (167,	'Amit',	    'Banda',    'ABANDA',	'011.44.1346.729268',	'2008-04-21',	'SA_REP',	6200,	.1,	    147,	80),
                             (168,	'Lisa',	    'Ozer',     'LOZER',    '011.44.1343.929268',	'2005-03-11',	'SA_REP',	11500,	.25,	148,	80),
                             (169,	'Harrison',	'Bloom',    'HBLOOM',	'011.44.1343.829268',	'2006-03-23',	'SA_REP',	10000,	.2,	    148,	80),
                             (170,	'Tayler',	'Fox',      'TFOX',	    '011.44.1343.729268',	'2006-01-24',	'SA_REP',	9600,	.2,	    148,	80),
                             (171,	'William',	'Smith',    'WSMITH',	'011.44.1343.629268',	'2007-02-23',	'SA_REP',	7400,	.15,	148,	80),
                             (172,	'Elizabeth','Bates',    'EBATES',	'011.44.1343.529268',	'2007-03-24',	'SA_REP',	7300,	.15,	148,	80),
                             (173,	'Sundita',  'Kumar',    'SKUMAR',	'011.44.1343.329268',	'2008-04-21',	'SA_REP',	6100,	.1,	    148,	80),
                             (174,	'Ellen',	'Abel',     'EABEL',	'011.44.1644.429267',	'2004-05-11',	'SA_REP',	11000,	.3,	    149,	80),
                             (175,	'Alyssa',	'Hutton',   'AHUTTON',	'011.44.1644.429266',	'2005-03-19',	'SA_REP',	8800,	.25,	149,	80),
                             (176,	'Jonathon',	'Taylor',   'JTAYLOR',	'011.44.1644.429265',	'2006-03-24',	'SA_REP',	8600,	.2,	    149,	80);

DECLARE
  CURSOR c (job VARCHAR2, max_sal NUMBER) IS
    SELECT last_name, first_name, (salary - max_sal) overpayment
    FROM employees
    WHERE job_id = job
    AND salary > max_sal
    ORDER BY salary;
 
  PROCEDURE print_overpaid IS
    last_name_   employees.last_name%TYPE;
    first_name_  employees.first_name%TYPE;
    overpayment_      employees.salary%TYPE;
  BEGIN
    LOOP
      FETCH c INTO last_name_, first_name_, overpayment_;
      EXIT WHEN c%NOTFOUND;
      raise notice 'last_name_=%,first_name_=%,overpayment_=%',last_name_,first_name_,overpayment_;
    END LOOP;
  END ;
 
BEGIN
  raise notice '----------------------';
  raise notice 'Overpaid Stock Clerks:';
  raise notice '----------------------';
  OPEN c('ST_CLERK', 5000);
  print_overpaid; 
  CLOSE c;
 
  raise notice '-------------------------------';
  raise notice 'Overpaid Sales Representatives:';
  raise notice '-------------------------------';
  OPEN c('SA_REP', 10000);
  print_overpaid; 
  CLOSE c;
END;
/

DECLARE
CURSOR c (job VARCHAR2, max_sal NUMBER,
hired DATE) IS
SELECT last_name, first_name, (salary - max_sal) overpayment
FROM employees
WHERE job_id = job
AND salary > max_sal
AND hire_date > hired
ORDER BY salary;

PROCEDURE print_overpaid IS
last_name_ employees.last_name%TYPE;
first_name_ employees.first_name%TYPE;
overpayment_ employees.salary%TYPE;
BEGIN
LOOP
FETCH c INTO last_name_, first_name_, overpayment_;
EXIT WHEN c%NOTFOUND;
raise notice 'last_name_=%,first_name_=%,overpayment_=%',last_name_,first_name_,overpayment_;
END LOOP;
END;

BEGIN
raise notice '-------------------------------';
raise notice 'Overpaid Sales Representatives:';
raise notice '-------------------------------';
OPEN c('SA_REP', 10000,TO_DATE('31-DEC-1999', 'DD-MON-YYYY')); 
print_overpaid;
CLOSE c;

raise notice '------------------------------------------------';
raise notice 'Overpaid Sales Representatives Hired After 2004:';
raise notice '------------------------------------------------';

OPEN c('SA_REP', 10000, TO_DATE('31-DEC-2004', 'DD-MON-YYYY'));
-- new reference
print_overpaid;
CLOSE c;
END;
/

CREATE or replace PROCEDURE pro_sub_pro_2_1( t0 int,t1 out number )  NOT SHIPPABLE SECURITY INVOKER
 AS  DECLARE 
PROCEDURE p (sales NUMBER, bonus out NUMBER)
IS
BEGIN
IF sales > 50000 THEN
bonus := 1500;
ELSIF sales > 35000 THEN
bonus := 500;
ELSE
bonus := 100;
END IF;
END ;
PROCEDURE p1 (sales int, bonus out int)
IS
BEGIN
IF sales > 50000 THEN
bonus := 1500;
ELSIF sales > 35000 THEN
bonus := 500;
ELSE
bonus := 100;
END IF;
END ;
BEGIN
p(t0,t1);
p1(t0,t1);
END;
/

select pro_sub_pro_2_1(1);
drop procedure pro_sub_pro_2_1;

select '400'::varchar - '100'::varchar;

CREATE OR REPLACE procedure pro_unicom1( t0 int,t1 varchar2 )
as
t2 varchar2(20);
t3 number;
t4 varchar2(20);
email1 varchar2(50);
email2 varchar2(50);
--第一个函数raise_p
function raise_p (sales int) return number
IS
sales1 number;
BEGIN
IF sales > 50000 THEN
sales1 := sales+1500;
ELSE
sales1 := sales+ 100;
END IF;
return sales1;
END ;
BEGIN
t3:=raise_p(t1);
raise notice 't3=%',t3;
END;
/

CREATE OR REPLACE procedure pro_unicom1( t0 int,t1 varchar2 )
as
t2 varchar2(20);
t3 number;
t4 varchar2(20);
email1 varchar2(50);
email2 varchar2(50);
--第一个函数raise_p
function raise_p (sales NUMeric) return number
IS
sales1 number;
BEGIN
IF sales > 50000 THEN
sales1 := sales+1500;
ELSE
sales1 := sales+ 100;
END IF;
return sales1;
END ;
BEGIN
t3:=raise_p(t1);
raise notice 't3=%',t3;
END;
/
select proargtypes from pg_proc where proname='raise_p';

CREATE SCHEMA test;
set search_path = test;
--匿名块子程序
DECLARE
  FUNCTION f (n INTEGER) return INTEGER
  is
    y int;
  BEGIN
    y = n * n;
    return y;
  END;
BEGIN
  raise info 'res = %',f(2);
END;
/
set search_path = public;
drop schema test cascade;
DECLARE
 X int;
 function f (n int) return int 
 is
   type ty1 is record(a int);
   y ty1;
begin
  y.a := n*n;
  return y.a;
end;
begin
  raise info 'res = %',f(2);
end;
/
select count(*) from pg_type where typname like '%ty1';

create or replace function func1(a int) return int
is
    function rand() return int
    is
    begin
        return (random() * 100)::int;
    end;
    procedure proc1()
    is
    begin
    end;
begin
    return a + rand();
end;
/

create or replace function func2(a int) return int
is
begin
    return func1(a);
end;
/

\parallel on 2
-- replace func1, invalidate subprogram rand()
create or replace function func1(a int) return int
is
    function rand() return int
    is
    begin
        return 10;
    end;
    procedure proc1()
    is
    begin
    end;
begin
    return a + rand();
end;
/
begin
    perform pg_sleep(2);
    perform func2(10);
end;
/
\parallel off

create or replace function collation_bug() return void is
declare
    type varchar_arr is table of varchar(10);
    v_arr varchar_arr;
    function named_arg_func(arg1 varchar_arr)
    return varchar_arr as
    declare
    begin
        return arg1;
    end;
begin
    v_arr(1) := 'foo';
    v_arr(2) := 'bar';
    perform named_arg_func(arg1 => v_arr);
end;
/

select collation_bug() from dual;

create type ty_1174844_1 as (c1 varchar(2),c2 varchar(3),c3 varchar(7));
create type ty_1174844_2 as (c1 varchar(2),"C2" varchar(3),c3 varchar(7));
drop table  t_1174844;
CREATE TABLE t_1174844 (
no_w_id int NOT NULL,
no_d_id int NOT NULL,
no_o_id int NOT NULL,
id3 ty_1174844_1,
id2 ty_1174844_2
);
CREATE OR REPLACE PACKAGE pkg_1174844 is
function pkg_1174844_fun1(i int) return int;
function pkg_1174844_fun2(i int) return int;
id1 int:= 1;
id2 ty_1174844_2:=ty_1174844_2('01','abc','123sc');
id3 ty_1174844_1:=ty_1174844_1('01','abc','123sc');
end pkg_1174844;
/
create or replace procedure p_1174844(n1 number)
as
c2 varchar2(20);
procedure p_1(n int)  AS
begin
raise notice 'insert';
FOR i IN 1..n LOOP
insert into t_1174844 values(pkg_1174844.id1,i+1,i+2,pkg_1174844.id3,pkg_1174844.id2);
END LOOP;
raise notice 'commit';
end;
function f_1(n int) return varchar
as 
c1 int;
begin 
c1:=n;
raise notice 'update';
if n <=3 then 
pkg_1174844.id2:=ty_1174844_2('02','abc','123sc');
pkg_1174844.id3:=ty_1174844_1('03','abc','123sc');
update t_1174844 set no_d_id= pkg_1174844.id1,id2=pkg_1174844.id2,id3=pkg_1174844.id3 where no_d_id = n;
else 
delete from t_1174844 where no_d_id=n;
end if;
return 'OK';
end;
begin
p_1(n1);
c2:=f_1(n1);
raise notice 'c2';
END;
/
call p_1174844(3);
select * from t_1174844 order by no_o_id;

create or replace function func1(a int) return int
is
    -- subprogram
    function rand return int
    is
    begin
        return 100;
    end;
begin
    return a + rand();
end;
/

create or replace type ty_1176368_1 as(c1 varchar(2),c2 varchar(3),c3 varchar(7));
create or replace type ty_1176368_2 as (c1 varchar(2),"C2" varchar(3),c3 varchar(7));
drop table t_1176368;
CREATE TABLE t_1176368 (
no_w_id int NOT NULL,
no_d_id int NOT NULL,
no_o_id int NOT NULL,
id33 ty_1176368_1,
id22 ty_1176368_2
);

create or replace procedure p_1176368(n1 number)
as
c2 varchar2(20);
id1 int:= 1;
id2 ty_1176368_2:=ty_1176368_2('01','abc','123sc');
id3 ty_1176368_1:=ty_1176368_1('01','abc','123sc');
procedure p_1(n int) AS
begin
raise notice 'insert';
FOR i IN 1..n LOOP
insert into t_1176368 values(id1,i+1,i+2,id3,id2);
END LOOP;
raise notice 'commit';
end;
function f_1(n int) return varchar
as
c1 int;
begin
c1:=n;
raise notice 'update';
if n <=3 then
id2:=ty_1176368_2('02','abc','123sc');
id3:=ty_1176368_1('03','abc','123sc');
update t_1176368 set no_d_id= id1,id22=id2,id33=id3 where no_d_id = n;
else
delete from t_1176368 where no_d_id=n;
end if;
return 'OK';
end;
begin
p_1(n1);
c2:=f_1(n1);
raise notice 'c2=%',c2;
END;
/

call p_1176368(3);
select * from t_1176368 order by no_o_id;

create table emp_sub_ex_tb1
( employee_id number(6,0) primary key,
first_name varchar2(20),
last_name varchar2(25));
INSERT INTO emp_sub_ex_tb1 VALUES ('100', 'Steven', 'King');
INSERT INTO emp_sub_ex_tb1 VALUES ('101', 'Neena', 'Kochhar');
INSERT INTO emp_sub_ex_tb1 VALUES ('102', 'Lex', 'De Haan');
INSERT INTO emp_sub_ex_tb1 VALUES ('103', 'Alexander', 'Hunold');
INSERT INTO emp_sub_ex_tb1 VALUES ('104', 'Bruce', 'Ernst');
INSERT INTO emp_sub_ex_tb1 VALUES ('105', 'David', 'Austin');

DECLARE
PROCEDURE p ( c1 NUMBER, c2 varchar2, c3 varchar2)
IS
c4 number;
BEGIN
insert into emp_sub_ex_tb1(employee_id,first_name,last_name) values(c1,c2,c3);
select employee_id into c4 from emp_sub_ex_tb1 where employee_id =c1;
raise notice 'c4=%',c4;
exception
when others then
raise notice 'insert error!!';
UPDATE emp_sub_ex_tb1 set first_name = c2 ,last_name =c3 where employee_id =c1;
END ;
BEGIN
p(106, '10000', '120');
p(100, '10000', '121');
END;
/
select * from emp_sub_ex_tb1 order by 1,2,3;


-- 子程序为存储过程，存储过程有入参和出参
create or replace procedure subpro_pro_1(sales NUMBER,bonus out number)
is
PROCEDURE p (sales1 NUMBER,bonus1 out number )
IS
BEGIN
IF sales1 > 50000 THEN
bonus1 := 10;
ELSIF sales1 > 35000 THEN
bonus1 := 20;
ELSE
bonus1 := 30;
END IF;

raise notice 'Sales=%,bonus=%',sales,bonus;
END ;
BEGIN
p(sales,bonus);
raise notice 'out is %',bonus;
END;
/

declare
id1 int;
begin
subpro_pro_1(55000,id1);
raise notice 'out is %',id1;
subpro_pro_1(45000,id1);
raise notice 'out is %',id1;
subpro_pro_1(25000,id1);
raise notice 'out is %',id1;
end;
/


--存储过程子程序出参和入参是同一个变量
create or replace procedure subpro_pro_2(sales1 in out NUMBER)
is
PROCEDURE p (sales in out NUMBER )
IS
BEGIN
IF sales > 50000 THEN
sales := sales+1;
ELSIF sales > 35000 THEN
sales := sales-1;
ELSE
sales := sales-4;
END IF;

raise notice 'Sales = %',sales;
END ;
BEGIN
p(sales1);
raise notice 'out is %',sales1;
END;
/
drop procedure subpro_pro_2;

--匿名块中函数子程序返回值为record:
DECLARE
rec_p varchar2;
test_record_p varchar2;
TYPE test_record_type IS RECORD(
id_type varchar2,
col_type varchar2
);
rec_p1 test_record_type;
function proc_Pack_fun(
p_rec_col in clob,
p_id_type in varchar2,
p_col_type in varchar2,
rec out varchar2,
test_record out varchar2) return record
IS
BEGIN
rec := p_rec_col||p_id_type;
test_record := p_rec_col||p_col_type;
END;
begin
rec_p1:=proc_Pack_fun('test_rec_col','testcol1','testcol2',rec_p,test_record_p);
raise notice 'rec_p=%,test_record_p=%,rec_p1.id_type=%,rec_p1.col_type=%',rec_p,test_record_p,rec_p1.id_type,rec_p1.col_type;
raise info '%',rec_p;
raise info '%',test_record_p;
raise info '%',rec_p1.id_type;
raise info '%',rec_p1.col_type;
raise info '%',rec_p1; 
end;
/

DECLARE
x INTEGER;

FUNCTION f (n INTEGER)
RETURN INTEGER
IS
BEGIN
RETURN (n*n);
END;

BEGIN
raise notice 'f returns %.Execution returns here',f(2);

x := f('2');
raise notice 'Execution returns here (2).';
END;
/

DECLARE
x INTEGER;

FUNCTION f (n INTEGER)
RETURN INTEGER
IS
BEGIN
RETURN (n*n);
END;

BEGIN
raise notice 'f returns %.Execution returns here',f(2);
x := f('a');
raise notice 'Execution returns here (2).';
END;
/


create table tmp1041292(id varchar(10),name varchar(10));
insert into tmp1041292 values('a','a');
create or replace procedure proc2_1041292 is
a tmp1041292%rowtype;
procedure sub_proc2_1041292 is
res int;
begin
select * into a from tmp1041292;
select count(*) into res from tmp1041292 where name= trim(a.name);
raise notice 'res=%',res;
end;
begin
sub_proc2_1041292;
end;
/
call proc2_1041292();
drop table tmp1041292;

select prokind from pg_proc where proname = 'sub_proc2_1041292';

create table t_1202881(id int,name varchar(20));
insert into t_1202881 values(2,'clouds break');
create or replace procedure proc2_1202881 is
a t_1202881%rowtype;
procedure sub_proc2_1202881 is
res int;
begin
select * into a from t_1202881;
a.id:= '异常';
select count(*) into res from t_1202881 where name= a.name;
raise notice 'res=%',res;
end;
begin
sub_proc2_1202881;
end;
/
call proc2_1202881();

create or replace procedure proc2_1202881 is
a t_1202881%rowtype;
procedure sub_proc2_1202881 is
res int;
begin
select * into a from t_1202881;
a.id:= 3;
select count(*) into res from t_1202881 where name= a.name;
raise notice 'res=%',res;
end;
begin
sub_proc2_1202881;
end;
/
call proc2_1202881();
drop table t_1202881;
drop procedure proc2_1202881;


create or replace function p_1199752(i1 int,i2 varchar) return int
is
type tp1 is record(a int,b char(8));
type tp2 is varray(10) of tp1;
a1 tp1;
a2 tp2;
procedure p (id int,id1 varchar)
AS
begin
a1.a:=id;
a1.b:=id1;
raise notice '%',a1;
end;
begin
a1.a:=1;
a1.b:='c';
--a2.extend;
a2(1):=a1;
raise notice '%--%--%--%',a1.a,a1.b,a2(1).a,a2(1).b;
p(i1,i2);
raise notice '%--%--%--%',a1.a,a1.b,a2(1).a,a2(1).b;
return a2(1).a;
end;
/

call p_1199752(2,'a');
drop procedure p_1199752;

create or replace procedure proc(id int) is
begin
raise notice 'id=%',id;
end;
/

create or replace procedure proc_parent(c1 int) is
declare
    procedure proc(id int) is
        begin
            raise notice 'id+1=%',id+1;
        end;
    begin
        proc(c1);
    end;
/

drop procedure proc_parent;
drop procedure proc;

CREATE or replace FUNCTION func(a int, b int) returns int AS $$
DECLARE 
type  test_type is record (c1 int, c2 char);
type1 test_type;
q int=0;
function test(type2 out test_type) return record
as
declare
begin
type2.c1 = 2;
type2.c2 = 'b';
q=3;
raise notice 'type2.c1=%',type2.c1;
raise notice 'type2.c2=%',type2.c2;
return type2;
end;
BEGIN
type1.c1 = 1;
type1.c2 = 'a';
q=1;
raise notice 'type1.c1=%',type1.c1;
raise notice 'type1.c2=%',type1.c2;
raise notice 'q=%',q;
type1 = test(type1);
raise notice 'type1.c1=%',type1.c1;
raise notice 'type1.c2=%',type1.c2;
raise notice 'q=%',q;
return q;
END;
$$ LANGUAGE plpgsql;
select func(1,2);

create table t_1176353 (id int, info text);
create table t_1176353_1 (id int, info text);
insert into t_1176353_1 values(1, 'a');

CREATE OR REPLACE PROCEDURE p_1176353()
AS
OutCode1 varchar2(200);
OutMsg1 varchar2(4000);
SDATEBEGIN varchar2(10);
EMPLOYEEID varchar2(50);
num int;
DATA_EXCEPTION exception;
pragma exception_init(DATA_EXCEPTION, -20001);
a int;
procedure p (OUT outcode numeric, OUT outmsg varchar, v_date varchar, v_employee_id varchar) as
begin
a:= 3;
insert into t_1176353 select * from t_1176353_1;
commit;
select 1/0;
EXCEPTION
WHEN DATA_EXCEPTION THEN
outcode := 3;
WHEN OTHERS THEN
outcode := 4;
end;
begin 
SDATEBEGIN:='2023-08-25';
EMPLOYEEID:= 'abcdef';
num:= 1;
FOR i IN 1 .. num LOOP
p(OutCode1, OutMsg1, SDATEBEGIN, EMPLOYEEID);
end loop;
raise notice '%',OutCode1;
raise notice '%',Outmsg1;
end;
/

call p_1176353();

create table acid_table_5
( employee_id number(6,0),
first_name varchar2(20),
last_name varchar2(25),
email varchar2(25));
INSERT INTO acid_table_5 VALUES ('150', 'Steven', 'King', 'SKING');
INSERT INTO acid_table_5 VALUES  ('125', 'Jennifer', 'Whalen', 'JWHALEN');

create or replace procedure profun_1(id int,name1 varchar2,name2 varchar2)
as
ema varchar2(36);
nn1 int;
a1 varchar2(50);
a2 varchar2(50);
function autonomous_fun1_dou(id2 int,na1 varchar2,na2 varchar2) return varchar2
is
id1 int;
ema1 varchar2(36);
procedure creat_id1_dou(nn int)
is
begin
if nn>180 then
nn1:=nn+100;
a1:='the id is:' || nn1;
else
nn1:=nn+200;
a1:='the id is:' || nn1;
end if;
end;
function creat_email1_dou(n1 varchar2,n2 varchar2) return varchar2
is
begin
ema:=n1 || n2 || '@163.com';
return ema;
end;
begin
creat_id1_dou(id2);
ema1:=creat_email1_dou(name1,name2);
id1:=nn1;
insert into acid_table_5 values(id1,na1,na2,ema1);
update acid_table_5 set email='12345@163.com' where employee_id='125';
return a1;
end;
procedure autonomous_pro1_dou(id3 int,na1 varchar2,na2 varchar2)
is
id1 int;
ema1 varchar2(36);
procedure creat_id1_dou1(nn int)
is
begin
if nn>180 then
nn1:=nn+50;
a1:='the id is:' || nn1;
else
nn1:=nn+150;
a1:='the id is:' || nn1;
end if;
end;
function creat_email1_dou1(n1 varchar2,n2 varchar2) return varchar2
is
begin
ema:=n1 || n2 || '@139.com';
return ema;
end;
begin
creat_id1_dou1(id3);
ema1:=creat_email1_dou1(name1,name2);
id1:=nn1;
insert into acid_table_5 values(id1,na1,na2,ema1);
update acid_table_5 set email='12345@163.com' where employee_id='125';
end;
begin
a2:=autonomous_fun1_dou(id,name1,name2);
autonomous_pro1_dou(id,name1,name2);
end;
/
call profun_1(198,'Lemon','zhang');
select * from acid_table_5 order by employee_id;

create or replace procedure pro(id int) is
type tp is record(c1 int,c2 int);
begin
raise notice 'aa';
end;
/
drop procedure pro;

create or replace procedure pro1 as
declare
procedure pro2 as
	declare
	procedure pro3 as
		declare
		procedure pro4 as
			begin
				raise notice 'p4';
			end;
		begin
			raise notice 'p3';
	    end;
	begin
		raise notice 'p2';
	end;
begin
	raise notice 'p1';
end;
/

create or replace procedure pro1 as
declare
procedure pro2 as
	declare
	procedure pro3 as	
		begin
			raise notice 'p3';
	    end;
	begin
		pro3();
		raise notice 'p2';
	end;
begin
	pro2();
	raise notice 'p1';
end;
/

select pro1();
drop procedure pro1;

create table test(id int);
create or replace procedure pro1(id int) as
	procedure p_1(c1 int) as
		pragma autonomous_transaction;
		begin
			insert into test values(2);
		end;
begin
	p_1(1);
	insert into test values(1);
end;
/
drop table test;

CREATE OR REPLACE FUNCTION WM_CONCAT_START(A TEXT[], S TEXT)  RETURN TEXT[]  AS
BEGIN
RETURN A || S;
END;
/
CREATE OR REPLACE FUNCTION WM_CONCAT_END(A TEXT[]) RETURN TEXT AS
BEGIN
RETURN array_to_string(A, ',');
END;
/
CREATE AGGREGATE WM_CONCAT(
BASETYPE = TEXT,
SFUNC = WM_CONCAT_START,
STYPE = TEXT[],
FINALFUNC =WM_CONCAT_END
);

CREATE OR REPLACE FUNCTION WM_CONCAT_END(A TEXT[]) RETURN TEXT AS
BEGIN
RETURN array_to_string(A, ',');
END;
/

select * from pg_aggregate where aggfinalfn = 'WM_CONCAT_END'::regproc;
create or replace function func1(col1 int,col2 int,col3 int,col4 int)return int is 
function func1_1 return int is
function func1_1_1 return int is begin return 10; end;
begin return 10; end;
begin return 10; end;
/

\c regression
drop database nested_subprogram;
