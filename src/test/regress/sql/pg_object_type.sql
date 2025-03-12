--1.create type as object语法测试
show sql_compatibility; 

drop schema if exists vastbase_object_type;
create schema vastbase_object_type;
set search_path=vastbase_object_type;

--create type测试
--1.1不创建方法时
create type obj_1 as object(id int,name text);

--1.2创建对象类型： create or  replace type typname as object
create or replace type obj_2 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT,
MEMBER FUNCTION f1(a int) return int
);
--1.3构造函数测试
--1.3.0创建type 带self参数的构造函数
create or replace type obj_2 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT
);

--1.3.1构造函数名与类型名不一致:构造函数名必须与对象类型名保持一致
create or replace type obj_2 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_1( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT
);
--1.3.2 构造函数self参数类型与类型名不一致：self参数类型必须与类型名保持一致
create or replace type obj_2 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_1, id integer , name text ) RETURN SELF AS RESULT
);

--1.3.3构造函数重载
create or replace type obj_2 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT,
CONSTRUCTOR FUNCTION obj_2(id integer) RETURN SELF AS RESULT
);

--1.4 member方法测试
--1.4.1member函数带隐式参数self和不带隐式参数self测试
create or replace type obj_4 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_4( SELF IN OUT obj_4, id integer , name text ) RETURN SELF AS RESULT,
MEMBER FUNCTION f1(a int) return int,
MEMBER FUNCTION f2( SELF IN OUT obj_4, b int) return int
);
--1.4.2member存储过程self参数和不带self参数测试
create or replace type obj_4 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_4( SELF IN OUT obj_4, id integer , name text ) RETURN SELF AS RESULT,
MEMBER FUNCTION f1(a int) return int,
MEMBER FUNCTION f2( SELF IN OUT obj_4, b int) return int,

MEMBER PROCEDURE p1( SELF IN OUT obj_4, b int),
MEMBER PROCEDURE p2
);

--1.5 static方法测试
--1.5.1 static方法测试：static函数和存储过程测试
create or replace type obj_3 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_3( SELF IN OUT obj_3, id integer , name text ) RETURN SELF AS RESULT,
MEMBER FUNCTION f1(a int) return int,
MEMBER FUNCTION f2( SELF IN OUT obj_3, b int) return int,

MEMBER PROCEDURE p1( SELF IN OUT obj_3, b int),
MEMBER PROCEDURE p2,
STATIC FUNCTION s_f1() return int,
STATIC FUNCTION s_f2(b int) return int,
STATIC procedure s_p1 ,
STATIC procedure s_p2(name text)
);
--1.5.2 static函数和存储过程self参数测试：static方法没有self参数
create or replace type obj_3 as object (
id int,
name text,
STATIC FUNCTION s_f2( SELF IN OUT obj_3, b int) return int
);

--1.6 排序方法测试：map 和order方法
--1.6.1 map方法
create or replace type obj_3 as object (
id int,
name text,
MAP MEMBER FUNCTION map() return int
);

--1.6.2 map方法参数只能为self
create or replace type obj_3 as object (
id int,
name text,
MAP MEMBER FUNCTION map(a int) return int
);
--1.6.3 多个map方法
create or replace type obj_3 as object (
id int,
name text,
MAP MEMBER FUNCTION map(SELF IN OUT obj_3) return int,
MAP MEMBER FUNCTION map1() return int
);
--1.6.4 order方法：包含隐式参数self总共2个参数，且参数类型必须是对象类型
--order方法显示声明self参数
create or replace type obj_3 as object (
id int,
name text,
ORDER MEMBER FUNCTION map(SELF IN OUT obj_3, a obj_3) return int
);
--1.6.5 order方法参数校验--只有一个参数
create or replace type obj_3 as object (
id int,
name text,
ORDER MEMBER FUNCTION map(SELF IN OUT obj_3) return int
);
--1.6.6 order方法参数校验--没有参数
create or replace type obj_3 as object (
id int,
name text,
ORDER MEMBER FUNCTION map return int
);
--1.6.7 order方法参数类型时其他类型
create or replace type obj_3 as object (
id int,
name text,

ORDER MEMBER FUNCTION map(a obj_2) return int
);

--1.6.8 order方法声明除self参数外有两个参数
create or replace type obj_3 as object (
id int,
name text,

ORDER MEMBER FUNCTION map(a obj_3,b obj_3) return int
);
--1.6.9 声明两个order方法
create or replace type obj_3 as object (
id int,
name text,

ORDER MEMBER FUNCTION map(a obj_3) return int,
ORDER MEMBER FUNCTION map1(a obj_3) return int
);
--1.6.10 声明两个排序方法：同时声明order和map方法
create or replace type obj_3 as object (
id int,
name text,

ORDER MEMBER FUNCTION map(a obj_3) return int,
MAP MEMBER FUNCTION map1() return int
);


--创建type body测试
--2.1类型体定义构造函数/member/static方法
create or replace type obj_2 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT,
MEMBER FUNCTION f1(a int) return int,
MEMBER FUNCTION f2( SELF IN OUT obj_2, b int) return int,

MEMBER PROCEDURE p1( SELF IN OUT obj_2, b int),
MEMBER PROCEDURE p2,
STATIC FUNCTION s_f1() return int,
STATIC FUNCTION s_f2(b int) return int ,
STATIC procedure s_p1 ,
STATIC procedure s_p2(name text)
);

CREATE OR REPLACE TYPE BODY obj_2 AS
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT as
begin
	self.id = id;
	self.name = name;
end;
MEMBER FUNCTION f1(a int) return int as
ret int := a;
begin

	return ret;
end;

MEMBER FUNCTION f2( SELF IN OUT obj_2, b int) return int
as 
begin
	raise info 'member function f2 self:%', self;
	return b;
end;

MEMBER PROCEDURE p1( SELF IN OUT obj_2, b int) as
begin
	raise info 'member procedure p1 : name=>%', SELF.name;
end;

MEMBER PROCEDURE p2 as
begin
	raise info 'member procedure p2 : name=>%', SELF.name;
end;

STATIC FUNCTION s_f1() return int as 

begin
	raise info 'static function  s_f1 ';
	return 1;
end;
STATIC FUNCTION s_f2(b int) return int as 

begin
	raise info 'static function  s_f2  ';
	return b;
end;

STATIC procedure s_p1  as 

begin
	raise info 'static function  s_p1 ';
end;
STATIC procedure s_p2(name text)  as 

begin
	raise info 'static function  s_p2 name:%',name;
end;
END ;
/

--2.2 在plpgsql中使用对象类型构造函数、member、static方法
declare
var obj_2 := obj_2(id=>1,name=>'23');
result integer;
begin
result := var.f1(1);
raise info '%',var;
raise info 'member function f1:%',var.f1(1);
raise info 'member function f1:%',var.f2(1);
raise info 'member procedure p1:%', var.p1(1);
var.name := 'vbase';
raise info 'member procedure p2:%', var.p2();

raise info 'static function s_f1:%',obj_2.s_f1();
raise info 'static function s_f2:%',obj_2.s_f2(2);

raise info 'static procedure s_p1:%',obj_2.s_p1();

raise info 'static procedure s_p2:%',obj_2.s_p2('vbase');
end;
/

--2.3 排序函数测试
--2.3.1 order 函数

-- 修复缺陷：【ID1031526】创建type时指定schema，创建报错
create schema schema_for_type;

CREATE OR REPLACE TYPE schema_for_type.obj_5 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa schema_for_type.obj_5) RETURN int
);

CREATE OR REPLACE TYPE BODY schema_for_type.obj_5 IS
    ORDER MEMBER FUNCTION b (aa schema_for_type.obj_5) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

CREATE OR REPLACE TYPE regression.schema_for_type.obj_6 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa regression.schema_for_type.obj_6) RETURN int
);

CREATE OR REPLACE TYPE BODY regression.schema_for_type.obj_6 IS
    ORDER MEMBER FUNCTION b (aa regression.schema_for_type.obj_6) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

CREATE OR REPLACE TYPE regression.schema_for_type.obj_7 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa schema_for_type.obj_7) RETURN int
);

CREATE OR REPLACE TYPE BODY regression.schema_for_type.obj_7 IS
    ORDER MEMBER FUNCTION b (aa schema_for_type.obj_7) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

CREATE OR REPLACE TYPE regression.schema_for_type.obj_8 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa obj_8) RETURN int
);

CREATE OR REPLACE TYPE BODY regression.schema_for_type.obj_8 IS
    ORDER MEMBER FUNCTION b (aa obj_8) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

CREATE OR REPLACE TYPE obj_9 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa regression.schema_for_type.obj_9) RETURN int
);

CREATE OR REPLACE TYPE BODY obj_9 IS
    ORDER MEMBER FUNCTION b (aa regression.schema_for_type.obj_9) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

CREATE OR REPLACE TYPE obj_10 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa schema_for_type.obj_10) RETURN int
);

CREATE OR REPLACE TYPE BODY obj_10 IS
    ORDER MEMBER FUNCTION b (aa schema_for_type.obj_10) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

CREATE OR REPLACE TYPE schema_for_type.obj_11 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa regression.schema_for_type.obj_11) RETURN int
);

CREATE OR REPLACE TYPE BODY schema_for_type.obj_11 IS
    ORDER MEMBER FUNCTION b (aa regression.schema_for_type.obj_11) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

CREATE OR REPLACE TYPE schema_for_type.obj_12 AS  OBJECT  (
    a decimal(38),
    ORDER MEMBER FUNCTION b (aa obj_12) RETURN int
);

CREATE OR REPLACE TYPE BODY schema_for_type.obj_12 IS
    ORDER MEMBER FUNCTION b (aa obj_12) RETURN int
    IS
    BEGIN
        CASE
            WHEN a > aa.a
            THEN
                RETURN 1;
            WHEN a = aa.a
            THEN
                RETURN 0;
            WHEN a < aa.a
            THEN
                RETURN -1;
        END CASE;
        RETURN -2;
    END;
END;
/

drop type obj_9 CASCADE;
drop type obj_10 CASCADE;
drop schema schema_for_type cascade;

create or replace type obj_2 as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT,
ORDER MEMBER FUNCTION cmp(SELF IN OUT obj_2, a obj_2) return int
);
CREATE OR REPLACE TYPE BODY obj_2 AS
CONSTRUCTOR FUNCTION obj_2( SELF IN OUT obj_2, id integer , name text ) RETURN SELF AS RESULT as
begin
	self.id = id;
	self.name = name;
end;
ORDER MEMBER FUNCTION cmp(SELF IN OUT obj_2, a obj_2) return int as
begin
	IF SELF.id > a.id THEN
		RETURN 1 ;
	ELSIF SELF.id < a.id THEN
		RETURN -1 ;
	ELSE
		RETURN 0 ;
	END IF ;
end;
END ;
/

--创建列对象表，根据对应列order by时使用order方法自定义进行排序
create table obj_t1 (no int, obj obj_2);
insert into obj_t1 values(1,obj_2(1,'vbase1'));
insert into obj_t1 values(2,obj_2(2,'vbase2'));
insert into obj_t1 values(3,obj_2(3,'vbase3'));
insert into obj_t1 values(4,obj_2(4,'vbase4'));
insert into obj_t1 values(5,obj_2(5,'vbase5'));
SELECT * FROM obj_t1 ORDER BY obj;

--在plpgsql中对象进行大小比较时使用order方法进行两个对象的大小比较
declare
var1 obj_2 := obj_2(id=>100,name=>'23');
var2 obj_2 := obj_2(id=>44,name=>'23');
result integer;
begin
	if var1 > var2  THEN
		raise info 'var1.id large var2.id';
	ELSIF var1 < var2 THEN
		raise info'var1.id small var2.id';
	ELSE
		raise info'var1.id equal var2.id';
END IF ;
end;
/

--2.3.2 map方法测试：map方法将对象类型映射为标量类型
--创建type声明map方法
create or replace type obj_map as object (
id int,
name text,
CONSTRUCTOR FUNCTION obj_map( SELF IN OUT obj_map, id integer , name text ) RETURN SELF AS RESULT,
MAP MEMBER FUNCTION map return int
);
--创建类型体定义map方法
CREATE OR REPLACE TYPE BODY obj_map AS
CONSTRUCTOR FUNCTION obj_map( SELF IN OUT obj_map, id integer , name text ) RETURN SELF AS RESULT as
begin
	self.id = id;
	self.name = name;
end;
MAP MEMBER FUNCTION map return int as
begin
	return self.id;
end;
END ;
/
--创建对象表：列对象表，根据对象列进行排序时使用map方法自定义排序
create table objmap_t1 (no int, obj obj_map);
insert into objmap_t1 values(1,obj_map(1,'vbase1'));
insert into objmap_t1 values(2,obj_map(2,'vbase2'));
insert into objmap_t1 values(3,obj_map(3,'vbase3'));
insert into objmap_t1 values(4,obj_map(4,'vbase4'));
insert into objmap_t1 values(5,obj_map(5,'vbase5'));

SELECT * FROM objmap_t1 ORDER BY obj;

--plpgsql中两个对象实例比较时，使用map自定义比较方法进行比较
declare
var1 obj_map := obj_map(id=>100,name=>'23');
var2 obj_map := obj_map(id=>44,name=>'23');
result integer;
begin
	if var1 > var2  THEN
		raise info 'var1.id large var2.id';
	ELSIF var1 < var2 THEN
		raise info'var1.id small var2.id';
	ELSE
		raise info'var1.id equal var2.id';
END IF ;
end;
/

-- fix schema.obj.fun case
create or replace type address_typ as object( street varchar2(30), city varchar2(20),
  static function convert_addr(street varchar2(30), city varchar2(20)) return address_typ);

create or replace type body address_typ as 
  static function convert_addr(street varchar2(30), city varchar2(20)) return address_typ is
  declare
    v_addr address_typ;
  begin
    v_addr.street = street;
    v_addr.city = city;
    return v_addr;
  end;
end;
/

create table test_addr(col1 address_typ);

insert into test_addr values(vastbase_object_type.address_typ.convert_addr('a', 'b'));
insert into test_addr values(address_typ.convert_addr('a', 'b'));

declare
    v_addr    address_typ;
begin
    v_addr = vastbase_object_type.address_typ.convert_addr('a', 'b');
    insert into test_addr values(v_addr);
end;
/

declare
    v_addr    address_typ;
begin
    v_addr = address_typ.convert_addr('a', 'b');
    insert into test_addr values(v_addr);
end;
/
drop table test_addr;
drop type address_typ;


--对象表
--行对象表
create table objmap_t2 of obj_map;
insert into objmap_t2 values(1,'a');
insert into objmap_t2 values(2,'b');
insert into objmap_t2 values(3,'c');
insert into objmap_t2 values(4,'d');
insert into objmap_t2 values(5,'e');

SELECT * FROM objmap_t2 e ORDER BY e;
--value函数--适配一下
select value(e) from objmap_t2 e;

--3.类型继承场景
--3.1创建基类型
CREATE TYPE person_t AS OBJECT (name VARCHAR2(100), ssn NUMBER) NOT FINAL;
--创建继承类型
CREATE TYPE employee_t UNDER person_t (department_id NUMBER, salary NUMBER) NOT FINAL;
CREATE TYPE part_time_emp_t UNDER employee_t (num_hrs NUMBER);
CREATE TABLE persons OF person_t;

INSERT INTO persons VALUES ('Bob', 1234);
INSERT INTO persons VALUES ('Bob1', 12345);
select * from persons;

--plpgsql中使用子类型实例化对象
declare
var part_time_emp_t := part_time_emp_t(name=>'vbase',ssn=>11,department_id=>1, salary=>1111,num_hrs=>23);
result integer;
begin
raise info '%',var;
end;
/

drop type person_t CASCADE;
drop type employee_t CASCADE;
--3.2 子类型继承父类型的member、static方法
create or replace type person_t as object (
name VARCHAR2(100), 
ssn NUMBER,

FINAL  CONSTRUCTOR FUNCTION person_t( SELF IN OUT person_t, name VARCHAR2(100) , ssn NUMBER ) RETURN SELF AS RESULT,
CONSTRUCTOR FUNCTION person_t( SELF IN OUT person_t, name VARCHAR2(100)) RETURN SELF AS RESULT,
MEMBER FUNCTION f1(a int) return int,
MEMBER FUNCTION f2( SELF IN OUT person_t, b int) return int,


MEMBER PROCEDURE p1( SELF IN OUT person_t, b int),
MEMBER PROCEDURE p2,
STATIC FUNCTION s_f1() return int,
STATIC FUNCTION s_f2(b int) return int ,
STATIC procedure s_p1 ,
STATIC procedure s_p2(name text)

)NOT FINAL;

CREATE OR REPLACE TYPE BODY person_t AS
FINAL CONSTRUCTOR FUNCTION person_t( SELF IN OUT person_t, name VARCHAR2(100) , ssn NUMBER) RETURN SELF AS RESULT as
begin
	self.name = name;
	self.ssn = ssn;
end;

CONSTRUCTOR FUNCTION person_t( SELF IN OUT person_t, name VARCHAR2(100)) RETURN SELF AS RESULT as
begin
	self.name = name;
end;

MEMBER FUNCTION f1(a int) return int as
ret int := a;
begin

	return ret;
end;

MEMBER FUNCTION f2( SELF IN OUT person_t, b int) return int
as 
begin
	raise info 'member function f2 self:%', self;
	return b;
end;

MEMBER PROCEDURE p1( SELF IN OUT person_t, b int) as
begin
	raise info 'member procedure p1 : name=>%', SELF.name;
end;

MEMBER PROCEDURE p2 as
begin
	raise info 'member procedure p2 : name=>%', SELF.name;
end;

STATIC FUNCTION s_f1() return int as 

begin
	raise info 'static function  s_f1 ';
	return 1;
end;
STATIC FUNCTION s_f2(b int) return int as 

begin
	raise info 'static function  s_f2  ';
	return b;
end;

STATIC procedure s_p1  as 

begin
	raise info 'static function  s_p1 ';
end;
STATIC procedure s_p2(name text)  as 

begin
	raise info 'static function  s_p2 name:%',name;
end;
END ;
/

--子类型继承父类型的属性和非构造函数成员方法
CREATE OR REPLACE TYPE employee_t UNDER person_t (
department_id NUMBER, 
salary NUMBER,
MEMBER FUNCTION f4(a int) return int

) NOT FINAL;

CREATE OR REPLACE TYPE BODY employee_t AS
MEMBER FUNCTION f4(a int) return int as
ret int := a;
begin
	return ret;
end;
END ;
/

--plpgsql中实例化子类型，使用继承父类型的方法
declare
var employee_t := employee_t(name=>'vbase',ssn=>11,department_id=>1, salary=>1111);
result integer;
begin
result := var.f1(1);
raise info '%',var;
--子类型调用member方法
raise info 'member function f1:%',var.f1(1);
raise info 'member function f1:%',var.f2(1);
raise info 'member procedure p1:%', var.p1(1);
var.name := 'vbase';
raise info 'member procedure p2:%', var.p2();

--子类型调用静态方法
raise info 'static function s_f1:%',employee_t.s_f1();
raise info 'static function s_f2:%',employee_t.s_f2(2);

raise info 'static procedure s_p1:%',employee_t.s_p1();

raise info 'static procedure s_p2:%',employee_t.s_p2('vbase');
end;
/

--缺陷【ID1015618】使用type创建varray集合，构造函数方式插入数据失败
--基于phone_typ对象类型
create type phone_typ as object(
country_code varchar2(2),
area_code varchar2(3),
ph_number varchar2(7)
);
--基于varray集合，元素类型是phone_typ对象类型
create type phone_varray_typ as varray(5) of phone_typ;
--创建测试表，属性是varray类型
create table dept_phone_list(
dept_no number(5),
phone_list phone_varray_typ
);
--使用构造函数方式插入数据
insert into dept_phone_list values(100,
phone_varray_typ(phone_typ('01','650','5550123'),phone_typ('01','650','5550148'),phone_typ('01','650','5550192'))
);

select * from dept_phone_list;

--缺陷【ID1015929】创建map成员方法，map member function中使用自定义参数会宕机
--原因是当比较方法第一个参数是否为self参数，如果方法中没声明参数名，直接声明参数类型，比较参数名宕机
create or replace type emp_type4 as object(

name varchar2(10),
birthdate date,
--map方法参数
map member function get_birdate(emp,emp_type4) return varchar2
);

create or replace type emp_type4 as object(

name varchar2(10),
birthdate date,
--map 函数
map member function get_birdate(emp_type4,emp_type4) return varchar2
);

create or replace type emp_type4 as object(

name varchar2(10),
birthdate date,
--order
ORDER MEMBER FUNCTION map( emp_type4) return int
);

create or replace type emp_type4 as object(

name varchar2(10),
birthdate date,
--order
ORDER MEMBER FUNCTION map(emp_type4, emp_type4) return int
);

create or replace type emp_type4 as object(

name varchar2(10),
birthdate date,
--构造函数
CONSTRUCTOR FUNCTION emp_type4( integer , name text ) RETURN SELF AS RESULT,
--member函数
MEMBER FUNCTION f1(int, int) return int,
--static
STATIC FUNCTION f2(int, int) return int,
ORDER MEMBER FUNCTION map( emp_type4) return int
) NOT FINAL;

--子类继承父类
create or replace type emp_type4_sub UNDER  emp_type4(

name1 varchar2(10),
birthdate2 date,
MEMBER FUNCTION f1(int,int, int) return int,
STATIC FUNCTION f2(varchar, int) return int
);


--【ID1015910】使用type创建包含varray的嵌套表类型，创建失败
CREATE TYPE phone_list_typ_demo AS VARRAY(5) OF VARCHAR2(25);
CREATE TYPE cust_address_typ2 AS OBJECT
( street_address VARCHAR2(40)
, postal_code VARCHAR2(10)
, city VARCHAR2(30)
, state_province VARCHAR2(10)
, country_id CHAR(2)
, phone phone_list_typ_demo
);

CREATE TYPE cust_nt_address_typ AS TABLE OF cust_address_typ2;

--缺陷【ID1018446】【vb2211】使用create or replace type重复创建同名type，首次创建可以成功，第二次创建失败
create or replace type stringarray is table of varchar2(100);
create or replace type stringarray is table of varchar2(100);

--缺陷【ID1017663】在type中定义有map成员函数，order by子句通过value运算符别名进行排序，按map函数的返回值进行排序报错
CREATE OR REPLACE TYPE emp_object_map AS OBJECT(
atri_empno NUMBER(4) , -- 雇员编号
atri_ename VARCHAR2(10) , -- 雇员姓名
atri_sal NUMBER(7,2) , -- 雇员工资
atri_comm NUMBER(7,2) , -- 雇员佣金
-- 定义MAP函数，此函数会在进行排序时自动调用
MAP MEMBER FUNCTION compare RETURN NUMBER
) NOT FINAL ;

CREATE OR REPLACE TYPE BODY emp_object_map AS
MAP MEMBER FUNCTION compare RETURN NUMBER AS
BEGIN
RETURN SELF.atri_sal - SELF.atri_comm;
END ;
END ;
/

CREATE TABLE emp_object_map_tab OF emp_object_map ;
INSERT INTO emp_object_map_tab(atri_empno,atri_ename,atri_sal,atri_comm) VALUES (7369,'SMITH',800,0) ;
INSERT INTO emp_object_map_tab(atri_empno,atri_ename,atri_sal,atri_comm) VALUES (7902,'FORD',3000,0) ;
INSERT INTO emp_object_map_tab(atri_empno,atri_ename,atri_sal,atri_comm) VALUES (7499,'ALLEN',1600,300) ;
INSERT INTO emp_object_map_tab(atri_empno,atri_ename,atri_sal,atri_comm) VALUES (7521,'WARD',400,500) ;
INSERT INTO emp_object_map_tab(atri_empno,atri_ename,atri_sal,atri_comm) VALUES (7839,'KING',5000,0) ;

SELECT (atri_sal - atri_comm) results,VALUE(e) ve , e.atri_empno , e.atri_ename , e.atri_sal,e.atri_comm,e.atri_sal+e.atri_comm
FROM emp_object_map_tab e ORDER BY ve ;


--【ID1017661】使用抽象数据类型--value运算符访问基于自定义type类型的表，并将结果进行赋值报错
CREATE OR REPLACE TYPE solid_typ AS OBJECT (
len INTEGER,
wth INTEGER,
hgt INTEGER);

CREATE TABLE solids of solid_typ;
INSERT INTO solids VALUES(10, 101, 10);
INSERT INTO solids VALUES(3, 4, 5);

--value运算符通过表别名方式，返回type对象类型
select value(s) from solids s;

--可以将结果进行赋值
DECLARE
solid solid_typ;
BEGIN 
SELECT VALUE(s) INTO solid FROM solids s WHERE s.len = 10;
raise info 'solid:%',solid;
END;
/

DECLARE
obj emp_object_map;
BEGIN 
SELECT VALUE(s) INTO obj FROM emp_object_map_tab s WHERE s.atri_empno = 7369;
raise info 'obj:%',obj;
END;
/

--没有符合记录使用value函数
DECLARE
obj emp_object_map;
BEGIN 
SELECT VALUE(s) INTO obj FROM emp_object_map_tab s WHERE s.atri_empno = 7369;
raise info 'obj:%',obj;
END;
/

create table test_tb1 (id int,name text);
insert into test_tb1 values(1,'vbase');
DECLARE
obj emp_object_map;
BEGIN 
SELECT VALUE(s) INTO obj FROM test_tb1 s WHERE s.id = 1;
raise info 'obj:%',obj;
END;
/

--【ID1017629】【Oracle兼容性--type功能增强】使用抽象数据类型--创建order成员方法，plpgsql中对象比较大小测试报错
create or replace type emp_type5 as object(
name varchar2(10),
birthdate date,
order member function compare(emp5 emp_type5) return int
);

-- 为emp_type5 对象类型实现方法体
create or replace type body emp_type5 is
order member function compare(emp5 emp_type5) return int is
begin
case
when birthdate>emp5.birthdate then return 1;
when birthdate=emp5.birthdate then return 0;
when birthdate<emp5.birthdate then return -1;
end case;
	return 0;
end;
end;
/

-- 根据对象类型empa_type5 建立表emp_tab5
create table emp_tab5 (
eno number(6),sal number(6,2),
job varchar2(20),emp5 emp_type5
);

-- 插入数据到emp_tab5
insert into emp_tab5(eno,sal,job,emp5) values(0011,9000,'dba',emp_type5('jacker',to_date('1990-12-19','yyyy-mm-dd')));
insert into emp_tab5(eno,sal,job,emp5) values(0022,9900,'dba',emp_type5('tom',to_date('1970-12-2','yyyy-mm-dd')));

--比较数据
declare
type emp5_tab is table of emp_type5;
v_emp5_tab emp5_tab;
v_result varchar2(100);
begin
select emp5 bulk collect into v_emp5_tab from emp_tab5 ;
if v_emp5_tab(1).compare(v_emp5_tab(2))=1 then
v_result := v_emp5_tab(1).name ||' 比 '||v_emp5_tab(2).name ||'大';
else
v_result := v_emp5_tab(1).name ||' 比 '||v_emp5_tab(2).name ||'小';
end if;
raise notice '%',v_result;
end;
/

--缺陷【ID1016837】【Oracle兼容性】自定义类型成员方法与get关键字重名时，无法通过自定义变量名.函数名的方式调用
create or replace type test_obj as object(
num number,
constructor function test_obj(self in out test_obj) return self as result,
member function get(pos number) return number
);

create or replace type body test_obj as
	constructor function test_obj(self in out test_obj) return self as result as
	begin
		self.num := 1;
		return;
	end;

	member function get(pos number) return number as
	begin
		raise notice 'get';
		return pos;
	end;
end;
/

/*var识别错误*/
declare
var test_obj := test_obj();
t1 number;
begin
t1 := var.get(1);
end;
/

--缺陷【ID1015575】创建类型的属性带有其他自定义数据类型时，该数据类型的数据在空值判断时出现异常
create type obj_1 as object(num number,  str varchar2(4000));
-- obj_2属性存在自定义数据类型
create type obj_2 as object (num number, obj obj_1);

create or replace procedure test_proc as
	test_num number;
	test_obj1 obj_1;
	test_obj2 obj_2;
begin

	if test_num is null then
	raise notice 'test_num is null';
	end if;

	if test_num is not null then
	raise notice 'test_num is not null';
	end if;

	if test_obj1 is null then
	raise notice 'test_obj1 is null';
	end if;

	if test_obj1 is not null then
	raise notice 'test_obj1 is not null';
	end if;

	-- test_obj2 := null -- 即使显式赋空值也能复现
	if test_obj2 is null then
	raise notice 'test_obj2 is null';
	end if;

	if test_obj2 is not null then
	raise notice 'test_obj2 is not null';
	end if;
end;
/
call test_proc();
--显示赋空，不初始化，均为null;构造函数赋值，赋值等为not null
declare
test_var1 obj_2 := obj_2(1,null);
test_var2 obj_2 := (1,null);
test_var3 obj_2 := null;
test_var4 obj_2 := NULL;
test_var5 obj_2;
begin

	if test_var1 is null then
	raise notice 'test_var1 is null';
	end if;

	if test_var1 is not null then
	raise notice 'test_var1 is not null';
	end if;

	if test_var2 is null then
	raise notice 'test_var2 is null';
	end if;

	if test_var2 is not null then
	raise notice 'test_var2 is not null';
	end if;

	if test_var3 is null then
	raise notice 'test_var3 is null';
	end if;

	if test_var3 is not null then
	raise notice 'test_var3 is not null';
	end if;

	if test_var4 is null then
	raise notice 'test_var4 is null';
	end if;

	if test_var4 is not null then
	raise notice 'test_var4 is not null';
	end if;

	if test_var5 is null then
	raise notice 'test_var5 is null';
	end if;

	if test_var5 is not null then
	raise notice 'test_var5 is not null';
	end if;
end;
/

--【Oracle兼容性-type功能增强，定义成员变量，定义成员函数】增加一些报错信息
--a static method cannot declare a parameter named SELF
create or replace type emp_type4 as object(
name varchar2(10),
birthdate date,
static function getdate(self in out  emp_type4) return date
);

--没有self，成功
create or replace type emp_type9 as object(
name varchar2(10),
birthdate date,
static function getdate( in out emp_type9) return date
);

--Only a function may be a MAP,ORDER or CONSTRUCTOR method
create or replace type emp_type4 as object(
name varchar2(10),
birthdate date,
order member procedure compare(emp4 emp_type4) 
);

--Only a function may be a MAP,ORDER or CONSTRUCTOR method
create or replace type emp_type5 as object(
name varchar2(10),
birthdate date,
map member procedure get_birdate
);

--no attributes found in object type ""
create or replace  type addresstype as object
(
);


--缺陷【ID1015935】【Oracle兼容性-type功能增强，定义成员变量，定义成员函数】创建order成员方法，order member function中返回的类型不是int应该报错
create or replace type emp_type6 as object(
name varchar2(10),
birthdate date,
order member function compare(emp4 emp_type6) return varchar
);

--缺陷【ID1015573】【Oracle兼容性--type功能增强，定义成员变量，定义成员函数】定义抽象数据类型--使用 create or replace type 定义抽象数据子类型，父类型不加not final
create or replace type parenttype as object
(
province varchar(20),
city varchar(30),
street varchar(40)
);
create or replace type childrentype under parenttype
(
home varchar(20),
position varchar(30)
);

create schema "test";
create type "test".addresstype as object
(
province varchar(20),
city varchar(30),
street varchar(40)
);
create or replace type "test".addresstype as object
(
province varchar(30),
city varchar(30),
street varchar(40)
);

--缺陷【ID1016767】member procedure无法直接通过自定义变量名.存储过程名的形式调用
set  behavior_compat_options='';
create or replace type test_obj as object(
id int,
name varchar2(100),
constructor function test_obj(self in out test_obj, name varchar2,id int) return self as result,
member procedure p1_1,
member procedure p1_2(name varchar2),
member procedure p1_3(name varchar2,id out int) ,
member procedure p1_4(name varchar2,id out int, pro in out text) ,
member procedure p1_5(name varchar2, pro in out text),
member procedure p1_6(id out int, pro in out text) ,
member procedure p2_1(SELF IN test_obj) ,
member procedure p2_2(SELF IN test_obj,name varchar2) ,
member procedure p2_3(SELF IN test_obj,name varchar2,id out int),
member procedure p2_4(SELF IN test_obj,name varchar2,id out int, pro in out text),
member procedure p2_5(SELF IN test_obj,name varchar2, pro in out text),
member procedure p2_6(SELF IN test_obj,id out int, pro in out text) ,
member procedure p3_1(SELF IN OUT test_obj),
member procedure p3_2(SELF IN OUT  test_obj,name varchar2),
member procedure p3_3(SELF IN OUT test_obj,name varchar2,id out int),
member procedure p3_4(SELF IN OUT test_obj,name varchar2,id out int, pro in out text),
member procedure p3_5(SELF IN OUT test_obj,name varchar2, pro in out text),
member procedure p3_6(SELF IN OUT test_obj,id out int, pro in out text),
member FUNCTION p4_1 return text ,
member FUNCTION p4_2(name varchar2) return text,
member FUNCTION p4_3(name varchar2,id out int) return text,
member FUNCTION p4_4(name varchar2,id out int, pro in out text) return text,
member FUNCTION p4_5(name varchar2, pro in out text) return text,
member FUNCTION p4_6(id out int, pro in out text) return text,
member FUNCTION p5_1(SELF IN test_obj) return text,
member FUNCTION p5_2(SELF IN test_obj,name varchar2) return text ,
member FUNCTION p5_3(SELF IN test_obj,name varchar2,id out int) return text,
member FUNCTION p5_4(SELF IN test_obj,name varchar2,id out int, pro in out text) return text,
member FUNCTION p5_5(SELF IN test_obj,name varchar2, pro in out text) return text ,
member FUNCTION p5_6(SELF IN test_obj,id out int, pro in out text) return text,
member FUNCTION p6_1(SELF IN OUT test_obj) return text,
member FUNCTION p6_2(SELF IN OUT  test_obj,name varchar2) return text,
member FUNCTION p6_3(SELF IN OUT test_obj,name varchar2,id out int) return text,
member FUNCTION p6_4(SELF IN OUT test_obj,name varchar2,id out int, pro in out text) return text,
member FUNCTION p6_5(SELF IN OUT test_obj,name varchar2, pro in out text) return text,
member FUNCTION p6_6(SELF IN OUT test_obj,id out int, pro in out text) return text
) not final;

create or replace type body test_obj as
constructor function test_obj(self in out test_obj, name varchar2,id int) return self as result as
begin
self.name := name;
self.id := id;
return;
end;
--场景1：member方法中不声明self参数
--场景1.1：member方法中无参
member procedure p1_1 as
begin
self.name := '调用后...p1_1';
end;

--场景1.2：member方法中只含有in参数
member procedure p1_2(name varchar2) as
begin
self.name := '调用后...p1_2';
end;

--场景1.3：member方法中只含有in,out参数
member procedure p1_3(name varchar2,id out int) as
begin
self.name := '调用后...p1_3';
id := 10;
end;
--场景1.4：member方法中函数in,inout,out 参数
member procedure p1_4(name varchar2,id out int, pro in out text) as
begin
self.name := '调用后...p1_4';
id := 10;
pro := 'vbase';
end;

--场景1.5：member方法中函数in,inout 参数
member procedure p1_5(name varchar2, pro in out text) as
begin
self.name := '调用后...p1_5';
pro := 'vbase';
end;

--场景1.6：member方法中函数inout,out 参数
member procedure p1_6(id out int, pro in out text) as
begin
self.name := '调用后...p1_6';
pro := 'vbase';
id := 11;
end;


--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
member procedure p2_1(SELF IN test_obj) as
begin
self.name := '调用后...p2_1';
end;

--场景2.2：member方法中只含有in参数
member procedure p2_2(SELF IN test_obj,name varchar2) as
begin
self.name := '调用后...p2_2';
end;

--场景2.3：member方法中只含有in,out参数
member procedure p2_3(SELF IN test_obj,name varchar2,id out int) as
begin
self.name := '调用后...p2_3';
id := 10;
end;
--场景2.4：member方法中函数in,inout,out 参数
member procedure p2_4(SELF IN test_obj,name varchar2,id out int, pro in out text) as
begin
self.name := '调用后...p2_4';
id := 10;
pro := 'vbase';
end;

--场景2.5：member方法中函数in,inout 参数
member procedure p2_5(SELF IN test_obj,name varchar2, pro in out text) as
begin
self.name := '调用后...p2_5';
pro := 'vbase';
end;

--场景2.6：member方法中函数inout,out 参数
member procedure p2_6(SELF IN test_obj,id out int, pro in out text) as
begin
self.name := '调用后...p2_6';
pro := 'vbase';
id := 11;
end;

--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
member procedure p3_1(SELF IN OUT test_obj) as
begin
self.name := '调用后...p3_1';
end;

--场景3.2：member方法中只含有in参数
member procedure p3_2(SELF IN OUT  test_obj,name varchar2) as
begin
self.name := '调用后...p3_2';
end;

--场景3.3：member方法中只含有in,out参数
member procedure p3_3(SELF IN OUT test_obj,name varchar2,id out int) as
begin
self.name := '调用后...p3_3';
id := 10;
end;
--场景3.4：member方法中函数in,inout,out 参数
member procedure p3_4(SELF IN OUT test_obj,name varchar2,id out int, pro in out text) as
begin
self.name := '调用后...p3_4';
id := 10;
pro := 'vbase';
end;

--场景3.5：member方法中函数in,inout 参数
member procedure p3_5(SELF IN OUT test_obj,name varchar2, pro in out text) as
begin
self.name := '调用后...p3_5';
pro := 'vbase';
end;

--场景3.6：member方法中函数inout,out 参数
member procedure p3_6(SELF IN OUT test_obj,id out int, pro in out text) as
begin
self.name := '调用后...p3_6';
pro := 'vbase';
id := 11;
end;

--场景4：member方法(FUNCTION)中不声明self参数
--场景4.1：member方法中无参
member FUNCTION p4_1 return text as
begin
self.name := '调用后...p4_1';
return 'G100_p4_1';
end;

--场景4.2：member方法中只含有in参数
member FUNCTION p4_2(name varchar2) return text as
begin
self.name := '调用后...p4_2';
return 'G100_p4_2';
end;

--场景4.3：member方法中只含有in,out参数
member FUNCTION p4_3(name varchar2,id out int) return text as
begin
self.name := '调用后...p4_3';
id := 10;
return 'G100_p4_3';
end;
--场景4.4：member方法中函数in,inout,out 参数
member FUNCTION p4_4(name varchar2,id out int, pro in out text) return text as
begin
self.name := '调用后...p4_4';
id := 10;
pro := 'vbase';
return 'G100_p4_4';
end;

--场景4.5：member方法中函数in,inout 参数
member FUNCTION p4_5(name varchar2, pro in out text) return text as
begin
self.name := '调用后...p4_5';
pro := 'vbase';
return 'G100_p4_5';
end;

--场景4.6：member方法中函数inout,out 参数
member FUNCTION p4_6(id out int, pro in out text) return text as
begin
self.name := '调用后...p4_6';
pro := 'vbase';
id := 11;
return 'G100_p4_6';
end;

--场景5：member方法(FUNCTION)中声明self参数为in
--场景5.1：member方法中无参
member FUNCTION p5_1(SELF IN test_obj) return text as
begin
self.name := '调用后...p5_1';
return 'G100_p5_1';
end;

--场景5.2：member方法中只含有in参数
member FUNCTION p5_2(SELF IN test_obj,name varchar2) return text as
begin
self.name := '调用后...p5_2';
return 'G100_p5_2';
end;

--场景5.3：member方法中只含有in,out参数
member FUNCTION p5_3(SELF IN test_obj,name varchar2,id out int) return text as
begin
self.name := '调用后...p5_3';
id := 10;
return 'G100_p5_3';
end;
--场景5.4：member方法中函数in,inout,out 参数
member FUNCTION p5_4(SELF IN test_obj,name varchar2,id out int, pro in out text) return text as
begin
self.name := '调用后...p5_4';
id := 10;
pro := 'vbase';
return 'G100_p5_4';
end;

--场景5.5：member方法中函数in,inout 参数
member FUNCTION p5_5(SELF IN test_obj,name varchar2, pro in out text) return text as
begin
self.name := '调用后...p5_5';
pro := 'vbase';
return 'G100_p5_5';
end;

--场景5.6：member方法中函数inout,out 参数
member FUNCTION p5_6(SELF IN test_obj,id out int, pro in out text) return text as
begin
self.name := '调用后...p5_6';
pro := 'vbase';
id := 11;
return 'G100_p5_6';
end;

--场景6：member方法(FUNCTION)中声明self参数为in out
--场景6.1：member方法中无参
member FUNCTION p6_1(SELF IN OUT test_obj) return text as
begin
self.name := '调用后...p6_1';
return 'G100_p6_1';
end;

--场景6.2：member方法中只含有in参数
member FUNCTION p6_2(SELF IN OUT  test_obj,name varchar2) return text as
begin
self.name := '调用后...p6_2';
return 'G100_p6_2';
end;

--场景6.3：member方法中只含有in,out参数
member FUNCTION p6_3(SELF IN OUT test_obj,name varchar2,id out int) return text as
begin
self.name := '调用后...p6_3';
id := 10;
return 'G100_p6_3';
end;
--场景6.4：member方法中函数in,inout,out 参数
member FUNCTION p6_4(SELF IN OUT test_obj,name varchar2,id out int, pro in out text) return text as
begin
self.name := '调用后...p6_4';
id := 10;
pro := 'vbase';
return 'G100_p6_4';
end;

--场景6.5：member方法中函数in,inout 参数
member FUNCTION p6_5(SELF IN OUT test_obj,name varchar2, pro in out text) return text as
begin
self.name := '调用后...p6_5';
pro := 'vbase';
return 'G100_p6_5';
end;

--场景6.6：member方法中函数inout,out 参数
member FUNCTION p6_6(SELF IN OUT test_obj,id out int, pro in out text) return text as
begin
self.name := '调用后...p6_6';
pro := 'vbase';
id := 11;
return 'G100_p6_6';
end;
end;
/

-- 2. 语句块中执行存储过程
declare
var test_obj := test_obj(100,'调用前...');
pro text := 'G100';
id int := 0;
ret text;
begin
--场景1：普通传参，member方法（存储过程）不显示声明SELF参数
--场景1.1：member方法无参数调用
raise notice '%',var.name;
var.p1_1();
raise notice '%',var.name;

----场景1.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p1_2('vbase');
raise notice '%',var.name;

--场景1.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_3('vbase',id);
raise notice '%',var.name;


--场景1.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_4('vbase',id,pro);
raise notice '% %',var.name, pro;

--场景1.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_5('vbase',pro);
raise notice '% %',var.name, pro;

--场景1.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_6(id,pro);
raise notice '% %',var.name, pro;

--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
var.p2_1();
raise notice '%',var.name;

----场景2.2：member方法只有in参数调用
raise notice '%',var.name;
var.p2_2('vbase');
raise notice '%',var.name;

--场景2.3：member方法中只含有in,out参数
raise notice '%',var.name;
var.p2_3('vbase',id);
raise notice '%',var.name;

--场景2.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
var.p2_4('vbase',id,pro);
raise notice '% %',var.name, pro;

--场景2.5：member方法中函数in,inout 参数
raise notice '%',var.name;
var.p2_5('vbase',pro);
raise notice '% %',var.name, pro;

--场景2.6：member方法中函数inout,out 参数
raise notice '%',var.name;
var.p2_6(id,pro);
raise notice '% %',var.name, pro;

--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
raise notice '%',var.name;
var.p3_1();
raise notice '%',var.name;

----场景3.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p3_2('vbase');
raise notice '%',var.name;

--场景3.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_3('vbase',id);
raise notice '%',var.name;

--场景3.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_4('vbase',id,pro);
raise notice '% %',var.name, pro;

--场景3.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_5('vbase',pro);
raise notice '% %',var.name, pro;

--场景3.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_6(id,pro);
raise notice '% %',var.name, pro;

--场景4：member方法(FUNCTION)中不声明self参数
--场景4.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
ret := var.p4_1();
raise notice '% %',var.name, ret;

----场景4.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p4_2('vbase');
raise notice '% %',var.name, ret;

--场景4.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p4_3('vbase',id);
raise notice '% %',var.name, ret;

--场景4.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
ret := var.p4_4('vbase',id,pro);
raise notice '% %',var.name, ret;

--场景4.5：member方法中函数in,inout 参数
raise notice '%',var.name;
ret := var.p4_5('vbase',pro);
raise notice '% %',var.name, ret;

--场景4.6：member方法中函数inout,out 参数
raise notice '%',var.name;
ret := var.p4_6(id,pro);
raise notice '% %',var.name, ret;

--场景5：member方法(FUNCTION)中声明self参数为in
--场景5.1：member方法中无参
raise notice '%',var.name;
ret := var.p5_1();
raise notice '% %',var.name, ret;

----场景5.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p5_2('vbase');
raise notice '% %',var.name, ret;

--场景5.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p5_3('vbase',id);
raise notice '% %',var.name, ret;

--场景5.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
ret := var.p5_4('vbase',id,pro);
raise notice '% %',var.name, ret;

--场景5.5：member方法中函数in,inout 参数
raise notice '%',var.name;
var.p5_5('vbase',pro);
raise notice '% %',var.name, ret;

--场景5.6：member方法中函数inout,out 参数
raise notice '%',var.name;
ret := var.p5_6(id,pro);
raise notice '% %',var.name, ret;

--场景6：member方法(FUNCTION)中声明self参数为in out
--场景6.1：member方法中无参
raise notice '%',var.name;
ret := var.p6_1();
raise notice '% %',var.name, ret;

----场景6.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p6_2('vbase');
raise notice '% %',var.name, ret;

--场景6.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p6_3('vbase',id);
raise notice '% %',var.name, ret;

--场景6.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
ret := var.p6_4('vbase',id,pro);
raise notice '% %',var.name, ret;

--场景6.5：member方法中函数in,inout 参数
raise notice '%',var.name;
ret := var.p6_5('vbase',pro);
raise notice '% %',var.name, ret;

--场景6.6：member方法中函数inout,out 参数
raise notice '%',var.name;
ret := var.p6_6(id,pro);
raise notice '% %',var.name, ret;
end
/

declare
var test_obj := test_obj(100,'调用前...');
pro text := 'G100';
id int := 0;
ret text;
begin
--场景二：指定参数传参，member方法（存储过程）不显示声明SELF参数
--场景1.1：member方法无参数调用
raise notice '%',var.name;
var.p1_1();
raise notice '%',var.name;

----场景1.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p1_2(name=>'vbase');
raise notice '%',var.name;

--场景1.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_3(name=>'vbase',id=>id);
raise notice '%',var.name;

--场景1.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_4(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, pro;

--场景1.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_5(name=>'vbase',pro=>pro);
raise notice '% %',var.name, pro;

--场景1.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1_6(id=>id,pro=>pro);
raise notice '% %',var.name, pro;


--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
var.p2_1();
raise notice '%',var.name;

----场景2.2：member方法只有in参数调用
raise notice '%',var.name;
var.p2_2(name=>'vbase');
raise notice '%',var.name;

--场景2.3：member方法中只含有in,out参数
raise notice '%',var.name;
var.p2_3(name=>'vbase',id=>id);
raise notice '%',var.name;

--场景2.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
var.p2_4(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, pro;

--场景2.5：member方法中函数in,inout 参数
raise notice '%',var.name;
var.p2_5(name=>'vbase',pro=>pro);
raise notice '% %',var.name, pro;

--场景2.6：member方法中函数inout,out 参数
raise notice '%',var.name;
var.p2_6(id=>id,pro=>pro);
raise notice '% %',var.name, pro;



--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
raise notice '%',var.name;
var.p3_1();
raise notice '%',var.name;

----场景3.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p3_2(name=>'vbase');
raise notice '%',var.name;

--场景3.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_3(name=>'vbase',id=>id);
raise notice '%',var.name;

--场景3.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_4(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, pro;

--场景3.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_5(name=>'vbase',pro=>pro);
raise notice '% %',var.name, pro;

--场景3.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3_6(id=>id,pro=>pro);
raise notice '% %',var.name, pro;


--场景4：member方法(FUNCTION)中不声明self参数
--场景4.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
ret := var.p4_1();
raise notice '% %',var.name, ret;

----场景4.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p4_2(name=>'vbase');
raise notice '% %',var.name, ret;

--场景4.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p4_3(name=>'vbase',id=>id);
raise notice '% %',var.name, ret;

--场景4.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
ret := var.p4_4(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, ret;

--场景4.5：member方法中函数in,inout 参数
raise notice '%',var.name;
ret := var.p4_5(name=>'vbase',pro=>pro);
raise notice '% %',var.name, ret;

--场景4.6：member方法中函数inout,out 参数
raise notice '%',var.name;
ret := var.p4_6(id=>id,pro=>pro);
raise notice '% %',var.name, ret;

--场景5：member方法(FUNCTION)中声明self参数为in
--场景5.1：member方法中无参
raise notice '%',var.name;
ret := var.p5_1();
raise notice '% %',var.name, ret;

----场景5.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p5_2(name=>'vbase');
raise notice '% %',var.name, ret;

--场景5.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p5_3(name=>'vbase',id=>id);
raise notice '% %',var.name, ret;

--场景5.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
ret := var.p5_4(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, ret;

--场景5.5：member方法中函数in,inout 参数
raise notice '%',var.name;
ret := var.p5_5(name=>'vbase',pro=>pro);
raise notice '% %',var.name, ret;

--场景5.6：member方法中函数inout,out 参数
raise notice '%',var.name;
ret := var.p5_6(id=>id,pro=>pro);
raise notice '% %',var.name, ret;


--场景6：member方法(FUNCTION)中声明self参数为in out
--场景6.1：member方法中无参
raise notice '%',var.name;
ret := var.p6_1();
raise notice '% %',var.name, ret;

----场景6.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p6_2(name=>'vbase');
raise notice '% %',var.name, ret;

--场景6.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p6_3(name=>'vbase',id=>id);
raise notice '% %',var.name, ret;

--场景6.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
ret := var.p6_4(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, ret;

--场景6.5：member方法中函数in,inout 参数
raise notice '%',var.name;
ret := var.p6_5(name=>'vbase',pro=>pro);
raise notice '% %',var.name, ret;

--场景6.6：member方法中函数inout,out 参数
raise notice '%',var.name;
ret := var.p6_6(id=>id,pro=>pro);
raise notice '% %',var.name, ret;
end;
/


--重载的场景测试:出参不重载
create or replace type test_obj_overlad as object(
id int,
name varchar2(100),
constructor function test_obj_overlad(self in out test_obj_overlad, name varchar2,id int) return self as result,
member procedure p1,
member procedure p1(name varchar2),
member procedure p1(name varchar2, pro in out text),
member procedure p2(SELF IN test_obj_overlad) ,
member procedure p2(SELF IN test_obj_overlad,name varchar2) ,
member procedure p2(SELF IN test_obj_overlad,name varchar2, pro in out text),
member procedure p3(SELF IN OUT test_obj_overlad),
member procedure p3(SELF IN OUT  test_obj_overlad,name varchar2),
member procedure p3(SELF IN OUT test_obj_overlad,name varchar2, pro in out text),
member FUNCTION p4 return text ,
member FUNCTION p4(name varchar2) return text,
member FUNCTION p4(name varchar2, pro in out text) return text,
member FUNCTION p5(SELF IN test_obj_overlad) return text,
member FUNCTION p5(SELF IN test_obj_overlad,name varchar2) return text ,
member FUNCTION p5(SELF IN test_obj_overlad,name varchar2, pro in out text) return text ,
member FUNCTION p6(SELF IN OUT test_obj_overlad) return text,
member FUNCTION p6(SELF IN OUT  test_obj_overlad,name varchar2) return text,
member FUNCTION p6(SELF IN OUT test_obj_overlad,name varchar2, pro in out text) return text
) not final;

create or replace type body test_obj_overlad as
constructor function test_obj_overlad(self in out test_obj_overlad, name varchar2,id int) return self as result as
begin
self.name := name;
self.id := id;
return;
end;
--场景1：member方法中不声明self参数
--场景1.1：member方法中无参
member procedure p1 as
begin
self.name := '调用后...overload p1 no arg';
end;

--场景1.2：member方法中只含有in参数
member procedure p1(name varchar2) as
begin
self.name := '调用后...overload p1 include in arg';
end;

--场景1.3：member方法中函数in,inout 参数
member procedure p1(name varchar2, pro in out text) as
begin
self.name := '调用后...overload p1 include in and inout arg';
pro := 'vbase';
end;

--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
member procedure p2(SELF IN test_obj_overlad) as
begin
self.name := '调用后...overload p2 no arg';
end;

--场景2.2：member方法中只含有in参数
member procedure p2(SELF IN test_obj_overlad,name varchar2) as
begin
self.name := '调用后...overload p2 include in arg';
end;

--场景2.3：member方法中只含有in参数
member procedure p2(SELF IN test_obj_overlad,name varchar2, pro in out text) as
begin
self.name := '调用后...overload p2 include in and inout arg';
pro := 'vbase';
end;


--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
member procedure p3(SELF IN OUT test_obj_overlad) as
begin
self.name := '调用后...overload p3 no arg';
end;

--场景3.2：member方法中只含有in参数
member procedure p3(SELF IN OUT  test_obj_overlad,name varchar2) as
begin
self.name := '调用后...overload p3 include in arg';
end;


--场景3.3：member方法中只含有in参数
member procedure p3(SELF IN OUT test_obj_overlad,name varchar2, pro in out text) as
begin
self.name := '调用后...overload p3 include in and inout arg';
pro := 'vbase';
end;




--场景4：member方法(FUNCTION)中不声明self参数
--场景4.1：member方法中无参
member FUNCTION p4() return text as
begin
self.name := '调用后...overload p4 no arg';
return 'G100_p4_1';
end;

--场景4.2：member方法中只含有in参数
member FUNCTION p4(name varchar2) return text as
begin
self.name := '调用后...overload p4 include in arg';
return 'G100_p4_2';
end;


--场景4.3：member方法中函数in,inout 参数
member FUNCTION p4(name varchar2, pro in out text) return text as
begin
self.name := '调用后...overload p4 include in and inout arg';
pro := 'vbase';
return 'G100_p4_3';
end;


--场景5：member方法(FUNCTION)中声明self参数为in
--场景5.1：member方法中无参
member FUNCTION p5(SELF IN test_obj_overlad) return text as
begin
self.name := '调用后...overload p5 no arg';
return 'G100_p5_1';
end;

--场景5.2：member方法中只含有in参数
member FUNCTION p5(SELF IN test_obj_overlad,name varchar2) return text as
begin
self.name := '调用后...overload p5 include in arg';
return 'G100_p5_2';
end;


--场景5.3：member方法中函数in,inout 参数
member FUNCTION p5(SELF IN test_obj_overlad,name varchar2, pro in out text) return text as
begin
self.name := '调用后...overload p5 include in and inout arg';
pro := 'vbase';
return 'G100_p5_3';
end;


--场景6：member方法(FUNCTION)中声明self参数为in out
--场景6.1：member方法中无参
member FUNCTION p6(SELF IN OUT test_obj_overlad) return text as
begin
self.name :=  '调用后...overload p6 no arg';
return 'G100_p6_1';
end;

--场景6.2：member方法中只含有in参数
member FUNCTION p6(SELF IN OUT  test_obj_overlad,name varchar2) return text as
begin
self.name := '调用后...overload p6 include in arg';
return 'G100_p6_2';
end;

--场景6.3：member方法中函数in,inout 参数
member FUNCTION p6(SELF IN OUT test_obj_overlad,name varchar2, pro in out text) return text as
begin
self.name := '调用后...overload p6 include in and inout arg';
pro := 'vbase';
return 'G100_p6_3';
end;


end;
/

declare
var test_obj_overlad := test_obj_overlad(100,'调用前...');
pro text := 'G100';
id int := 0;
ret text;
begin

--场景1：普通传参，member方法（存储过程）不显示声明SELF参数
--场景1.1：member方法无参数调用
raise notice '%',var.name;
var.p1();
raise notice '%',var.name;

----场景1.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p1('vbase');
raise notice '%',var.name;

--场景1.3：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1('vbase',pro);
raise notice '% %',var.name, pro;

--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
var.p2();
raise notice '%',var.name;

----场景2.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p2('vbase');
raise notice '%',var.name;

--场景2.3：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p2('vbase',pro);
raise notice '% %',var.name, pro;


--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
raise notice '%',var.name;
var.p3();
raise notice '%',var.name;

----场景3.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p3('vbase');
raise notice '%',var.name;

--场景3.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3('vbase',pro);
raise notice '% %',var.name, pro;

--场景4：member方法(FUNCTION)中不声明self参数
--场景4.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
ret := var.p4();
raise notice '% %',var.name, ret;

----场景4.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p4('vbase');
raise notice '% %',var.name, ret;

--场景4.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p4('vbase',pro);
raise notice '% %',var.name, ret;

--场景5：member方法(FUNCTION)中声明self参数为in
--场景5.1：member方法中无参
raise notice '%',var.name;
ret := var.p5();
raise notice '% %',var.name, ret;

----场景5.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p5('vbase');
raise notice '% %',var.name, ret;

--场景5.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p5('vbase',pro);
raise notice '% %',var.name, ret;

--场景6：member方法(FUNCTION)中声明self参数为in out
--场景6.1：member方法中无参
raise notice '%',var.name;
ret := var.p6();
raise notice '% %',var.name, ret;

----场景6.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p6('vbase');
raise notice '% %',var.name, ret;

--场景6.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p6('vbase',pro);
raise notice '% %',var.name, ret;
--场景二：指定参数传参，member方法（存储过程）不显示声明SELF参数
--场景1.1：member方法无参数调用
raise notice '%',var.name;
var.p1();
raise notice '%',var.name;

----场景1.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p1(name=>'vbase');
raise notice '%',var.name;

--场景1.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1(name=>'vbase',pro=>pro);
raise notice '%',var.name;
--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
var.p2();
raise notice '%',var.name;

----场景2.2：member方法只有in参数调用
raise notice '%',var.name;
var.p2(name=>'vbase');
raise notice '%',var.name;

--场景2.3：member方法中只含有in,out参数
raise notice '%',var.name;
var.p2(name=>'vbase',pro=>pro);
raise notice '%',var.name;
--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
raise notice '%',var.name;
var.p3();
raise notice '%',var.name;

----场景3.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p3(name=>'vbase');
raise notice '%',var.name;

--场景3.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3(name=>'vbase',pro=>pro);
raise notice '%',var.name;
--场景4：member方法(FUNCTION)中不声明self参数
--场景4.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
ret := var.p4();
raise notice '% %',var.name, ret;

----场景4.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p4(name=>'vbase');
raise notice '% %',var.name, ret;

--场景4.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p4(name=>'vbase',pro=>pro);
raise notice '% %',var.name, ret;

--场景5：member方法(FUNCTION)中声明self参数为in
--场景5.1：member方法中无参
raise notice '%',var.name;
ret := var.p5();
raise notice '% %',var.name, ret;

----场景5.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p5(name=>'vbase');
raise notice '% %',var.name, ret;

--场景5.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p5(name=>'vbase',pro=>pro);
raise notice '% %',var.name, ret;


--场景6：member方法(FUNCTION)中声明self参数为in out
--场景6.1：member方法中无参
raise notice '%',var.name;
ret := var.p6();
raise notice '% %',var.name, ret;

----场景6.2：member方法只有in参数调用
raise notice '%',var.name;
ret := var.p6(name=>'vbase');
raise notice '% %',var.name, ret;

--场景6.3：member方法中只含有in,out参数
raise notice '%',var.name;
ret := var.p6(name=>'vbase',pro=>pro);
raise notice '% %',var.name, ret;

end;
/
------场景三：出参重载开关打开测试,出参重载
set  behavior_compat_options='proc_outparam_override';
create or replace type test_obj_overlad_out_param as object(
id int,
name varchar2(100),
constructor function test_obj_overlad_out_param(self in out test_obj_overlad_out_param, name varchar2,id int) return self as result,
member procedure p1,
member procedure p1(name varchar2),
member procedure p1(name varchar2,id out int) ,
member procedure p1(name varchar2,id out int, pro in out text) ,
member procedure p1(name varchar2, pro in out text),
member procedure p1(id out int, pro in out text) ,
member procedure p2(SELF IN test_obj_overlad_out_param) ,
member procedure p2(SELF IN test_obj_overlad_out_param,name varchar2) ,
member procedure p2(SELF IN test_obj_overlad_out_param,name varchar2,id out int),
member procedure p2(SELF IN test_obj_overlad_out_param,name varchar2,id out int, pro in out text),
member procedure p2(SELF IN test_obj_overlad_out_param,name varchar2, pro in out text),
member procedure p2(SELF IN test_obj_overlad_out_param,id out int, pro in out text) ,
member procedure p3(SELF IN OUT test_obj_overlad_out_param),
member procedure p3(SELF IN OUT  test_obj_overlad_out_param,name varchar2),
member procedure p3(SELF IN OUT test_obj_overlad_out_param,name varchar2,id out int),
member procedure p3(SELF IN OUT test_obj_overlad_out_param,name varchar2,id out int, pro in out text),
member procedure p3(SELF IN OUT test_obj_overlad_out_param,name varchar2, pro in out text),
member procedure p3(SELF IN OUT test_obj_overlad_out_param,id out int, pro in out text)
) not final;

create or replace type body test_obj_overlad_out_param as
constructor function test_obj_overlad_out_param(self in out test_obj_overlad_out_param, name varchar2,id int) return self as result as
begin
self.name := name;
self.id := id;
return;
end;
--场景1：member方法中不声明self参数
--场景1.1：member方法中无参
member procedure p1 as
begin
self.name := '调用后...overload p1 no arg';
end;

--场景1.2：member方法中只含有in参数
member procedure p1(name varchar2) as
begin
self.name := '调用后...overload p1 include in arg';
end;

--场景1.3：member方法中只含有in,out参数
member procedure p1(name varchar2,id out int) as
begin
self.name := '调用后...overload p1 include in and inout int arg';
id := 10;
end;
--场景1.4：member方法中函数in,inout,out 参数
member procedure p1(name varchar2,id out int, pro in out text) as
begin
self.name := '调用后...overload p1 include in and inout int，inout text arg';
id := 10;
pro := 'vbase';
end;

--场景1.5：member方法中函数in,inout 参数
member procedure p1(name varchar2, pro in out text) as
begin
self.name := '调用后...overload p1 include in and inout text arg';
pro := 'vbase';
end;

--场景1.6：member方法中函数inout,out 参数
member procedure p1(id out int, pro in out text) as
begin
self.name := '调用后...overload p1 include inout int  and inout text arg';
pro := 'vbase';
id := 11;
end;



--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
member procedure p2 (SELF IN test_obj_overlad_out_param) as
begin
self.name := '调用后...overload p2 no arg';
end;

--场景2.2：member方法中只含有in参数
member procedure p2 (SELF IN test_obj_overlad_out_param,name varchar2) as
begin
self.name := '调用后...overload p2 include in arg';
end;

--场景2.3：member方法中只含有in,out参数
member procedure p2 (SELF IN test_obj_overlad_out_param,name varchar2,id out int) as
begin
self.name := '调用后...overload p2 include in and inout int arg';
id := 10;
end;
--场景2.4：member方法中函数in,inout,out 参数
member procedure p2 (SELF IN test_obj_overlad_out_param,name varchar2,id out int, pro in out text) as
begin
self.name := '调用后...overload p2 include in and inout int，inout text arg';
id := 10;
pro := 'vbase';
end;

--场景2.5：member方法中函数in,inout 参数
member procedure p2 (SELF IN test_obj_overlad_out_param,name varchar2, pro in out text) as
begin
self.name := '调用后...overload p2 include in and inout text arg';
pro := 'vbase';
end;

--场景2.6：member方法中函数inout,out 参数
member procedure p2 (SELF IN test_obj_overlad_out_param,id out int, pro in out text) as
begin
self.name := '调用后...overload p2 include inout int  and inout text arg';
pro := 'vbase';
id := 11;
end;

--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
member procedure p3 (SELF IN OUT test_obj_overlad_out_param) as
begin
self.name := '调用后...overload p3 no arg';
end;

--场景3.2：member方法中只含有in参数
member procedure p3 (SELF IN OUT  test_obj_overlad_out_param,name varchar2) as
begin
self.name := '调用后...overload p3 include in arg';
end;

--场景3.3：member方法中只含有in,out参数
member procedure p3 (SELF IN OUT test_obj_overlad_out_param,name varchar2,id out int) as
begin
self.name := '调用后...overload p3 include in and inout int arg';
id := 10;
end;
--场景3.4：member方法中函数in,inout,out 参数
member procedure p3 (SELF IN OUT test_obj_overlad_out_param,name varchar2,id out int, pro in out text) as
begin
self.name := '调用后...overload p3 include in and inout int，inout text arg';
id := 10;
pro := 'vbase';
end;

--场景3.5：member方法中函数in,inout 参数
member procedure p3 (SELF IN OUT test_obj_overlad_out_param,name varchar2, pro in out text) as
begin
self.name := '调用后...overload p3 include in and inout text arg';
pro := 'vbase';
end;

--场景3.6：member方法中函数inout,out 参数
member procedure p3 (SELF IN OUT test_obj_overlad_out_param,id out int, pro in out text) as
begin
self.name := '调用后...overload p3 include inout int  and inout text arg';
pro := 'vbase';
id := 11;
end;
end;
/

declare
var test_obj_overlad_out_param := test_obj_overlad_out_param(100,'调用前...');
pro text := 'G100';
id int := 0;
ret text;
begin
--场景1：普通传参，member方法（存储过程）不显示声明SELF参数，存储过程出参重载时，普通传参出参课可以返回
--场景1.1：member方法无参数调用
raise notice '%',var.name;
var.p1();
raise notice '%',var.name;

----场景1.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p1('vbase');
raise notice '%',var.name;

--场景1.3：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1('vbase',pro);
raise notice '% %',var.name, pro;

--场景1.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1('vbase',id,pro);
raise notice '% %',var.name, pro;

--场景1.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1('vbase',pro);
raise notice '% %',var.name, pro;

--场景1.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1(id,pro);
raise notice '% %',var.name, pro;


--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
var.p2();
raise notice '%',var.name;

----场景2.2：member方法只有in参数调用
raise notice '%',var.name;
var.p2('vbase');
raise notice '%',var.name;

--场景2.3：member方法中只含有in,out参数
raise notice '%',var.name;
var.p2('vbase',id);
raise notice '%',var.name;

--场景2.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
var.p2('vbase',id,pro);
raise notice '% %',var.name, pro;

--场景2.5：member方法中函数in,inout 参数
raise notice '%',var.name;
var.p2('vbase',pro);
raise notice '% %',var.name, pro;

--场景2.6：member方法中函数inout,out 参数
raise notice '%',var.name;
var.p2(id,pro);
raise notice '% %',var.name, pro;


--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
raise notice '%',var.name;
var.p3();
raise notice '%',var.name;

----场景3.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p3('vbase');
raise notice '%',var.name;

--场景3.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3('vbase',id);
raise notice '%',var.name;

--场景3.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3('vbase',id,pro);
raise notice '% %',var.name, pro;

--场景3.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3('vbase',pro);
raise notice '% %',var.name, pro;

--场景3.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3(id,pro);
raise notice '% %',var.name, pro;


--场景二：指定参数传参，member方法（存储过程）不显示声明SELF参数
--场景1.1：member方法无参数调用
raise notice '%',var.name;
var.p1();
raise notice '%',var.name;

----场景1.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p1(name=>'vbase');
raise notice '%',var.name;

--场景1.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1(name=>'vbase',id=>id);
raise notice '%',var.name;

--场景1.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, pro;

--场景1.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1(name=>'vbase',pro=>pro);
raise notice '% %',var.name, pro;

--场景1.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p1(id=>id,pro=>pro);
raise notice '% %',var.name, pro;


--场景2：member方法中声明self参数为in
--场景2.1：member方法中无参
var.name := '调用前...';
raise notice '%',var.name;
var.p2();
raise notice '%',var.name;

----场景2.2：member方法只有in参数调用
raise notice '%',var.name;
var.p2(name=>'vbase');
raise notice '%',var.name;

--场景2.3：member方法中只含有in,out参数
raise notice '%',var.name;
var.p2(name=>'vbase',id=>id);
raise notice '%',var.name;

--场景2.4：member方法中函数in,inout,out 参数
raise notice '%',var.name;
var.p2(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, pro;

--场景2.5：member方法中函数in,inout 参数
raise notice '%',var.name;
var.p2(name=>'vbase',pro=>pro);
raise notice '% %',var.name, pro;

--场景2.6：member方法中函数inout,out 参数
raise notice '%',var.name;
var.p2(id=>id,pro=>pro);
raise notice '% %',var.name, pro;



--场景3：member方法中声明self参数为in out
--场景3.1：member方法中无参
raise notice '%',var.name;
var.p3();
raise notice '%',var.name;

----场景3.2：member方法只有in参数调用
var.name := '调用前...';
raise notice '%',var.name;
var.p3(name=>'vbase');
raise notice '%',var.name;

--场景3.3：member方法中只含有in,out参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3(name=>'vbase',id=>id);
raise notice '%',var.name;

--场景3.4：member方法中函数in,inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3(name=>'vbase',id=>id,pro=>pro);
raise notice '% %',var.name, pro;

--场景3.5：member方法中函数in,inout 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3(name=>'vbase',pro=>pro);
raise notice '% %',var.name, pro;

--场景3.6：member方法中函数inout,out 参数
var.name := '调用前...';
raise notice '%',var.name;
var.p3(id=>id,pro=>pro);
raise notice '% %',var.name, pro;

end;
/

--测试member方法self参数模式，只能为inout,in 模式
create or replace type test_obj_test as object(
id int,
name varchar2(100),
member procedure p2(SELF OUT test_obj_test) 

) not final;
create or replace type test_obj_test as object(
id int,
name varchar2(100),
member FUNCTION p2(SELF OUT test_obj_test) return text
) not final;

--【ID1021985】【电信10所】PLSQL关联数组下标无法引用对象属性
drop type key_value_obj cascade;
create type key_value_obj is object(key number,value text);
create type key_value_obj_array is table of key_value_obj;
--测试first,last,count,prior,next,exists
declare
l_sql_info  key_value_obj_array := key_value_obj_array();
BEGIN
l_sql_info.extend();
l_sql_info(l_sql_info.count)= (1,'a');
l_sql_info.extend();
l_sql_info(l_sql_info.count)= (2,'q');
raise info 'l_sql_info中key是%',l_sql_info(l_sql_info.first).key;
raise info 'l_sql_info中key是%',l_sql_info(l_sql_info.last).key;
raise info 'l_sql_info中key是%',l_sql_info(l_sql_info.count).key;
raise info 'l_sql_info中value是%',l_sql_info(l_sql_info.prior(2)).value;
raise info 'l_sql_info中key是%',l_sql_info(l_sql_info.next(1)).key;
raise info 'l_sql_info中value是%',l_sql_info(l_sql_info.exists(1)).value;
end;
/

--【ID1025833】【object】object 只有一个属性时，赋值报错
CREATE TYPE type1 as object (id int);
declare
o1 type1;
BEGIN
o1.id := 1;
raise info 'o1 = %',o1;
raise info 'o1.id = %',o1.id;
end;
/

declare
o1 type1;
BEGIN
o1 := type1(15);
raise info 'o1 = %',o1;
end;
/
--【ID1033021】自定义type，type中的字段名使用大写，插入数据报错
drop type if exists type1 CASCADE;
drop type if exists type2 CASCADE;
drop table if exists t1 CASCADE;
drop table if exists t2 CASCADE;
create type type1 as object (c1 varchar(2),c2 varchar(3),c3 varchar(7));
create type type2 as object (c1 varchar(2),"C2" varchar(3),c3 varchar(7));
create table t1( id int,id1 type1);
create table t2( id int,id1 type2);
insert into t1 values(1,type1('01','abc','123sc'));
insert into t2 values(1,type2('01','abc','123sc'));
drop type if exists type1 CASCADE;
drop type if exists type2 CASCADE;
drop table if exists t1 CASCADE;
drop table if exists t2 CASCADE;

--【ID1027478】【DBMS_AQADM】普通用户创建队列表，队列后，删除用户报错
-- 其实和高级包没关系，是object删除时候没有删除构造函数在pg_shdepend里的记录
create user u_1142032 password 'Gauss_123';
set role u_1142032 password 'Gauss_123';
CREATE TYPE work_order_1142032 as object (id int,name VARCHAR2(200));
drop type work_order_1142032;
reset role;
drop user u_1142032;


-- 测试：create type A is/as object a;a为其他模式定义的type
drop schema if exists s_1033815;
create schema s_1033815;
create type s_1033815.tp1 as object(id int,name varchar);
create or replace type tp2_new as object(id int,info s_1033815.tp1);

-- test bug1016055 【Oracle兼容性-type功能增强，定义成员变量，定义成员函数】创建行对象表，并插入数据失败
create type type_1016055 as object(a int,name varchar2(10),
constructor function type_1016055(m_a int) return self as result
);
create or replace type body type_1016055 as constructor function type_1016055(m_a int) return self as result is 
begin
a:=m_a;
name:='type_test';
return;
end;
end;
/
create table tab_1016055(col type_1016055);
insert into tab_1016055 values(type_1016055(3)),(type_1016055(1,'test1'));
select * from tab_1016055;

\c regression
drop database db_test;
drop schema if exists vastbase_object_type cascade;

reset search_path;
create schema hedgeadmin;
create or replace type hedgeadmin.ty_arbitrageapplmtattr as object(
productid char(8),
exchangid char(8),
settlementgroupid char(8),
arbitragetype char(1),
startdateexpr varchar2(1024),
startdateexorname char(100),
enddateexpr varchar2(1024),
enddateexprname char(100),
versionno number(10),
constructor function ty_arbitrageapplmtattr return self as result,
member function uf_tostring return varchar2);

create or replace type hedgeadmin.tyt_arbitrageapplmtattr  as table of hedgeadmin.ty_arbitrageapplmtattr;

drop schema hedgeadmin cascade;

create type int4 as object(id int);
create or replace type int4 as object(id int);
create type int_4 as object(id int);
create type int_4 as object(id int);
drop type int_4;

-- 重写没支持
--【ID1020991】【Oracle兼容性-type中可覆写成员函数】子类函数内部调用重写函数失败
create schema object_type_schema;
set search_path = object_type_schema;
set enable_proc_param_name_override=on;

create type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member function display_details(a int) return varchar2
)not final;


create type body t_person as
    member function display_details(a int) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name ;
    end;
end;
/

--2.创建子类
create type t_business_person under t_person (
title varchar2(10),
company varchar2(10),
overriding member function display_details(a int) return varchar2,
member function display_details(b int) return varchar2
);

--场景1：对象类型方法调用重载成员方法：调用的成员方法作为表达式处理（右值），且默认参数self不输入时
create type body t_business_person as
overriding member function display_details(a int) return varchar2 is
begin
    return 'id=' || id || ',name=' || first_name || ' ' || last_name || 'title=' || title;
end;

member function display_details(b int) return varchar2 is
begin
    return display_details(a=>1);
end;

end;
/

--3.调用
set serveroutput on;
DECLARE
    v_person t_person ;
    v_business_person t_business_person;
BEGIN
    v_person := t_person(1,'测试F','测试2');
    v_business_person  := t_business_person(1,'AA','BB','标题~','公司Co');

    dbms_output.put_line(v_business_person .display_details(b=>1));
end;
/


--场景2：对象类型方法调用重载成员方法：调用的成员方法作为表达式处理（右值），输入默认参数self
create or replace type body t_business_person as
overriding member function display_details(a int) return varchar2 is
begin
    return 'id=' || id || ',name=' || first_name || ' ' || last_name || 'title=' || title;
end;

member function display_details(b int) return varchar2 is
begin
    return display_details(self=>self, a=>1);
end;

end;
/

DECLARE
    v_person t_person ;
    v_business_person t_business_person;
BEGIN
    v_person := t_person(1,'测试F','测试2');
    v_business_person  := t_business_person(1,'AA','BB','标题~','公司Co');

    dbms_output.put_line(v_business_person .display_details(b=>1));
end;
/

--场景3：对象类型方法调用重载成员方法：调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），不输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member procedure display_details(a int)
)not final;


create or replace type body t_person as
    member procedure display_details(a int)  is
begin
    raise info 'id=%,first_name=%,last_name=%' , id, first_name,last_name ;
    end;
end;
/

--2.创建子类
create or replace type t_business_person under t_person (
title varchar2(10),
company varchar2(10),
overriding member procedure display_details(a int) ,
member procedure display_details(b int) 
);


create or replace type body t_business_person as
overriding member procedure display_details(a int)  is
begin
	 raise info 'id=%,first_name=%,last_name=%,title=%' , id, first_name,last_name,title ;
end;

member procedure display_details(b int) is
begin
     display_details(a=>1);
end;

end;
/

--3.调用
set serveroutput on;
DECLARE
    v_person t_person ;
    v_business_person t_business_person;
BEGIN
    v_person := t_person(1,'测试F','测试2');
    v_business_person  := t_business_person(1,'AA','BB','标题~','公司Co');

    v_business_person .display_details(b=>1);
end;
/


--场景4：对象类型方法调用重载成员方法：调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），输入默认参数self
create or replace type body t_business_person as
overriding member procedure display_details(a int)  is
begin
    raise info 'id=%,first_name=%,last_name=%,title=%' , id, first_name,last_name,title ;
end;

member procedure display_details(b int) is
begin
    --return 'id=' || id || ',name=' || first_name || ' ' || last_name || 'title=' || title || 'company=' || company ;
    display_details(self=>self, a=>1);
end;

end;
/

DECLARE
    v_person t_person ;
    v_business_person t_business_person;
BEGIN
    v_person := t_person(1,'测试F','测试2');
    v_business_person  := t_business_person(1,'AA','BB','标题~','公司Co');

    v_business_person .display_details(b=>1);
end;
/

----场景5：对象类型方法调用重载成员方法：重载函数参数传参时不指定参数名，使用构造表达式的处理（右值），不输入默认参数self
drop type  t_person cascade;
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member function display_details(a int) return varchar2,
member function display_details(a text) return varchar2,
member function print_int(a int) return varchar2,
member function print_text(a text) return varchar2
)not final;


create or replace type body t_person as
member function display_details(a int) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'int a:'||a;
end;

member function display_details(a text) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'text a:'|| a;
end;

member function print_int(a int) return varchar2 is
begin
    return display_details(a);
end;

member function print_text(a text) return varchar2 is
begin
    return display_details(a);
end;
end;
/


--3.调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    dbms_output.put_line(v_person.print_int(1));
    dbms_output.put_line(v_person.print_text('vbase'));
end;
/
----场景6：对象类型方法调用重载成员方法：重载函数参数传参时不指定参数名，使用构造表达式的处理（右值），输入默认参数self
create or replace type body t_person as
member function display_details(a int) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'int a:'||a;
end;

member function display_details(a text) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'text a:'|| a;
end;

member function print_int(a int) return varchar2 is
begin
    return display_details(self,a);
end;

member function print_text(a text) return varchar2 is
begin
    return display_details(self,a);
end;
end;
/


--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    dbms_output.put_line(v_person.print_int(1));
    dbms_output.put_line(v_person.print_text('vbase'));
end;
/
----场景7：对象类型方法调用重载成员方法：重载函数参数传参时不指定参数名，调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），不输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member procedure display_details(a int),
member procedure display_details(a text),
member procedure print_int(a int),
member procedure print_text(a text)
)not final;


create or replace type body t_person as
member procedure display_details(a int)  is
begin
	raise info 'id=%,first_name=%,last_name=%,int a=%' , id, first_name,last_name,a ;
end;

member procedure display_details(a text) is
begin
	raise info 'id=%,first_name=%,last_name=%,text a=%' , id, first_name,last_name,a ;
end;

member procedure print_int(a int)  is
begin
     display_details(a);
end;

member procedure print_text(a text)  is
begin
     display_details(a);
end;
end;
/


--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    v_person.print_int(1);
    v_person.print_text('vbase');
end;
/
----场景8：对象类型方法调用重载成员方法：重载函数参数传参时不指定参数名，调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member procedure display_details(a int),
member procedure display_details(a text),
member procedure print_int(a int),
member procedure print_text(a text)
)not final;


create or replace type body t_person as
member procedure display_details(a int)  is
begin
	raise info 'id=%,first_name=%,last_name=%,int a=%' , id, first_name,last_name,a ;
end;

member procedure display_details(a text) is
begin
	raise info 'id=%,first_name=%,last_name=%,text a=%' , id, first_name,last_name,a ;
end;

member procedure print_int(a int)  is
begin
     display_details(self,a);
end;

member procedure print_text(a text)  is
begin
     display_details(self,a);
end;
end;
/

--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    v_person.print_int(1);
    v_person.print_text('vbase');
end;
/
----场景9：对象类型方法调用非重载成员方法：使用构造表达式的处理（右值），不输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member function display_details_int(a int) return varchar2,
member function display_details_text(a text) return varchar2,
member function print_int(a int) return varchar2,
member function print_text(a text) return varchar2
)not final;


create or replace type body t_person as
member function display_details_int(a int) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'int a:'||a;
end;

member function display_details_text(a text) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'text a:'|| a;
end;

member function print_int(a int) return varchar2 is
begin
    return display_details_int(a);
end;

member function print_text(a text) return varchar2 is
begin
    return display_details_text(a);
end;
end;
/


--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    dbms_output.put_line(v_person.print_int(1));
    dbms_output.put_line(v_person.print_text('vbase'));
end;
/
----场景10：对象类型方法调用非重载成员方法：使用构造表达式的处理（右值），输入默认参数self

create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member function display_details_int(a int) return varchar2,
member function display_details_text(a text) return varchar2,
member function print_int(a int) return varchar2,
member function print_text(a text) return varchar2
)not final;


create or replace type body t_person as
member function display_details_int(a int) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'int a:'||a;
end;

member function display_details_text(a text) return varchar2 is
begin
    return 'id='|| id || ',name=' || first_name || ' ' || last_name || 'text a:'|| a;
end;

member function print_int(a int) return varchar2 is
begin
    return display_details_int(self,a);
end;

member function print_text(a text) return varchar2 is
begin
    return display_details_text(self,a);
end;
end;
/


--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    dbms_output.put_line(v_person.print_int(1));
    dbms_output.put_line(v_person.print_text('vbase'));
end;
/
----场景11：对象类型方法调用非重载成员方法：调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），不输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member procedure display_details_int(a int),
member procedure display_details_text(a text),
member procedure print_int(a int),
member procedure print_text(a text)
)not final;


create or replace type body t_person as
member procedure display_details_int(a int)  is
begin
	raise info 'id=%,first_name=%,last_name=%,int a=%' , id, first_name,last_name,a ;
end;

member procedure display_details_text(a text) is
begin
	raise info 'id=%,first_name=%,last_name=%,text a=%' , id, first_name,last_name,a ;
end;

member procedure print_int(a int)  is
begin
     display_details_int(a);
end;

member procedure print_text(a text)  is
begin
     display_details_text(a);
end;
end;
/

--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    v_person.print_int(1);
    v_person.print_text('vbase');
end;
/
----场景12：对象类型方法调用非重载成员方法：调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member procedure display_details_int(a int),
member procedure display_details_text(a text),
member procedure print_int(a int),
member procedure print_text(a text)
)not final;


create or replace type body t_person as
member procedure display_details_int(a int)  is
begin
	raise info 'id=%,first_name=%,last_name=%,int a=%' , id, first_name,last_name,a ;
end;

member procedure display_details_text(a text) is
begin
	raise info 'id=%,first_name=%,last_name=%,text a=%' , id, first_name,last_name,a ;
end;

member procedure print_int(a int)  is
begin
     display_details_int(self,a);
end;

member procedure print_text(a text)  is
begin
     display_details_text(self,a);
end;
end;
/

--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    v_person.print_int(1);
    v_person.print_text('vbase');
end;
/

----场景13：对象类型方法调用非重载成员方法：调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），指定参数名，不输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member procedure display_details_int(a int),
member procedure display_details_text(a text),
member procedure print_int(a int),
member procedure print_text(a text)
)not final;


create or replace type body t_person as
member procedure display_details_int(a int)  is
begin
	raise info 'id=%,first_name=%,last_name=%,int a=%' , id, first_name,last_name,a ;
end;

member procedure display_details_text(a text) is
begin
	raise info 'id=%,first_name=%,last_name=%,text a=%' , id, first_name,last_name,a ;
end;

member procedure print_int(a int)  is
begin
     display_details_int(a=>a);
end;

member procedure print_text(a text)  is
begin
     display_details_text(a=>a);
end;
end;
/

--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    v_person.print_int(1);
    v_person.print_text('vbase');
end;
/
----场景14：对象类型方法调用非重载成员方法：调用的成员方法直接调用（以函数调用的方式处理，不使用构造表达式的处理），指定参数名，输入默认参数self
create or replace type t_person as object (
id integer,
first_name varchar(10),
last_name varchar(10),
member procedure display_details_int(a int),
member procedure display_details_text(a text),
member procedure print_int(a int),
member procedure print_text(a text)
)not final;


create or replace type body t_person as
member procedure display_details_int(a int)  is
begin
	raise info 'id=%,first_name=%,last_name=%,int a=%' , id, first_name,last_name,a ;
end;

member procedure display_details_text(a text) is
begin
	raise info 'id=%,first_name=%,last_name=%,text a=%' , id, first_name,last_name,a ;
end;

member procedure print_int(a int)  is
begin
     display_details_int(self=>self, a=>a);
end;

member procedure print_text(a text)  is
begin
     display_details_text(self=>self,a=>a);
end;
end;
/

create schema test;  
create type array_type is table of int;

create type test.array_type is table of int;
select * from pg_type where typname = 'array_type';

drop type array_type cascade;
drop type test.array_type cascade;
drop schema test cascade;

--调用
set serveroutput on;
DECLARE
    v_person t_person ;
BEGIN
    v_person := t_person(1,'测试F','测试2');

    v_person.print_int(1);
    v_person.print_text('vbase');
end;
/

--ID1038178 定义as object复杂类型，匿名块赋值失败
create or replace type N_HUB as OBJECT
(
    id int,
    name varchar2(100),
    age int
);
create or replace type N_HUB1 as OBJECT
(
    id int,
    name varchar2(100),
    age int
);
DECLARE
t1 n_hub;
t2 n_hub1;
begin
t1.id=1;
t1.name='zhengyue01';
t2:=t1;
raise info 'The id is: %',t2.id;
raise info 'The name is: %',t2.name;
raise info 'The id is: %',t1.id;
raise info 'The name is: %',t1.name;
end;
/
drop type N_HUB;
drop type N_HUB1;

--【ID1038421】【支持ODCIAGGREGATE自定义函数】自定义嵌套表类型在OBJECT TYPE中调用，创建type body时报错
create or replace type TTN as table of number;

--创建object type
create or replace type TO_BALANCED_BUCKET as object
(
  summ TTN,
  result int,
  member function member_func(self in out TO_BALANCED_BUCKET, value in number) return number,
  map member function map_func(self in out TO_BALANCED_BUCKET) return number,
  constructor function TO_BALANCED_BUCKET(self in out TO_BALANCED_BUCKET, summ_in in TTN, result_in in int) return SELF  AS RESULT
);

create or replace type body TO_BALANCED_BUCKET is
    --member
    member function member_func(self in out TO_BALANCED_BUCKET, value in number) return number is
    begin
        self.summ.extend(1);
        summ.extend(1);
        self.summ(1) := 1;
        summ(1) := 1;
        return 1;
    end;
    --map
    map member function map_func(self in out TO_BALANCED_BUCKET) return number is
    begin
        self.summ.extend(1);
        summ.extend(1);
        self.summ(1) := 1;
        summ[1] := 1;
        return 1;
    end;
    --constructor
    constructor function TO_BALANCED_BUCKET(self in out TO_BALANCED_BUCKET, summ_in in TTN, result_in in int) return SELF AS RESULT as
    begin
        self.summ.extend(1);
        summ.extend(1);
        summ_in.extend(1);
        self.summ(1) := 1;
        summ(1) := 1;
    return self;
    end;
end;
/

drop type TO_BALANCED_BUCKET;
drop type TTN;

drop schema object_type_schema cascade;
