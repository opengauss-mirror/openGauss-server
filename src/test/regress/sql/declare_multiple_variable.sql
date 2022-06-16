--test create function/procedure definer=user 
CREATE DATABASE mysqltest DBCOMPATIBILITY  'B';
\c  mysqltest
--test declare mutile variable
DECLARE 
	DECLARE res1,res2 int default (3+4);
	DECLARE str1,str2 text default 'opengunass';
	DECLARE num1,num2,num3 number default 23.34;
	DECLARE mon1,mon2 money default 1000.00;
	DECLARE date1,date2 date default (date '2010-12-10 00:00:00');
	DECLARE point1,point2 point default (2,3);
	DECLARE lseg1,lseg2 lseg default   '[(2,3),(3,3)]' ;
	DECLARE cidr1,cidr2 cidr default '::10.2.3.4';
	DECLARE jsonb1,jsonb2 jsonb default '[1, " a ", {"a"   :1    }] ';
	type rectype is record(col1 int,col2 text);
	DECLARE rec1,rec2 rectype default  (1,'abc');
BEGIN
	res1 := 90;
	str1 := 'aa55';
	raise info 'type int res1:%,res2:%',res1,res2;
	raise info 'type text str1:%,str2:%',str1,str2;
	raise info 'type number num1:%,num2:%,num3:%',num1,num2,num3;
	raise info 'type money mon1:%,mon2:%',mon1,mon2;
	raise info 'type point point1:%,point2:%',point1,point2;
	raise info 'type lseg lseg1:%,lseg2:%',lseg1,lseg2;
	raise info 'type cidr cidr1:%,cidr2:%',cidr1,cidr2;
	raise info 'type jsonb jsonb1:%,jsonb2:%',jsonb1,jsonb2;
	raise info 'type record rec1:%,rec2:%',rec1,rec2;
END;
/


DECLARE 
	 res1,res2 int default (3+4);
	 str1,str2 text default 'opengunass';
	 num1,num2,num3 number default 23.34;
	 mon1,mon2 money default 1000.00;
	 date1,date2 date default (date '2010-12-10 00:00:00');
	 point1,point2 point default (2,3);
	 lseg1,lseg2 lseg default   '[(2,3),(3,3)]' ;
	 cidr1,cidr2 cidr default '::10.2.3.4';
	 jsonb1,jsonb2 jsonb default '[1, " a ", {"a"   :1    }] ';
	type rectype is record(col1 int,col2 text);
	rec1,rec2 rectype default  (1,'abc');
	
BEGIN
	res1 := 90;
	str1 := 'aa55';
	raise info 'type int res1:%,res2:%',res1,res2;
	raise info 'type text str1:%,str2:%',str1,str2;
	raise info 'type number num1:%,num2:%,num3:%',num1,num2,num3;
	raise info 'type money mon1:%,mon2:%',mon1,mon2;
	raise info 'type point point1:%,point2:%',point1,point2;
	raise info 'type lseg lseg1:%,lseg2:%',lseg1,lseg2;
	raise info 'type cidr cidr1:%,cidr2:%',cidr1,cidr2;
	raise info 'type jsonb jsonb1:%,jsonb2:%',jsonb1,jsonb2;
	raise info 'type record rec1:%,rec2:%',rec1,rec2;
END;
/

--在function中使用
create or replace procedure declare_test1 as
type t1 is varray(10) of int;
type t4 is varray(10) of text;
declare va1,va2 t1 default '{1,2}';
declare vt1,vt2 t4 ;
begin
	vt1[1] := 'a';
	vt1[2] := 'b';
	raise info 'array va1:%,va2:%', va1,va2;
	raise info 'text array  vt1:%,vt2:%', vt1,vt2;
end;
/
select declare_test1();
drop procedure declare_test1();
create or replace function declare_test2() return void as
type t1 is varray(10) of int;
type t4 is varray(10) of text;
declare va1,va2 t1 default '{1,2}';
declare vt1,vt2 t4 ;
begin
	vt1[1] := 'a';
	vt1[2] := 'b';
	raise info 'array va1:%,va2:%', va1,va2;
	raise info 'text array  vt1:%,vt2:%', vt1,vt2;
end;
/
select declare_test2();

drop function declare_test2();
--在packakge中使用
create or replace package  pkg_test1 is
var4,var5 int:=42;
var6,var7 int:=43;
res1,res2 int default (3+4);
str1,str2 text default 'opengunass';
num1,num2,num3 number default 23.34;
mon1,mon2 money default 1000.00;
date1,date2 date default (date '2010-12-10 00:00:00');
point1,point2 point default (2,3);
lseg1,lseg2 lseg default   '[(2,3),(3,3)]' ;
cidr1,cidr2 cidr default '::10.2.3.4';
jsonb1,jsonb2 jsonb default '[1, " a ", {"a"   :1    }] ';
ype rectype is record(col1 int,col2 text);
ec1,rec2 rectype default  (1,'abc');
procedure testpro1();
end pkg_test1;
/
create or replace package body pkg_test1 is
var1,var2 int:=46;
procedure testpro1()
is
begin
	res1 := 90;
	str1 := 'aa55';
	raise info 'type int res1:%,res2:%',res1,res2;
	raise info 'type text str1:%,str2:%',str1,str2;
	raise info 'type number num1:%,num2:%,num3:%',num1,num2,num3;
	raise info 'type money mon1:%,mon2:%',mon1,mon2;
	raise info 'type point point1:%,point2:%',point1,point2;
	raise info 'type lseg lseg1:%,lseg2:%',lseg1,lseg2;
	raise info 'type cidr cidr1:%,cidr2:%',cidr1,cidr2;
	raise info 'type jsonb jsonb1:%,jsonb2:%',jsonb1,jsonb2;
	raise info 'type record rec1:%,rec2:%',rec1,rec2;
end;

begin
testpro1();
raise info 'ceclare multiple variable in package type int var1:%,var2:%',var1,var2;
end pkg_test1;
/

drop package pkg_test1;
--test declare mutile variabe for cursor
DECLARE
    --定义游标
    TYPE CURSOR_TYPE IS REF CURSOR;
    C1,C2 CURSOR_TYPE;
BEGIN
END;
/

DECLARE 
	DECLARE C1,C2 SYS_REFCURSOR; 
BEGIN
END;
/

--test declare mutile variabe for alias
DECLARE 
	old1,old2 int;
	DECLARE alias1,alias2 ALIAS FOR old1; 
BEGIN
END;
/

\c regression

--在非mysql数据库兼容模式中声明多个变量
DECLARE 
	DECLARE res1,res2 int default (3+4);
	DECLARE str1,str2 text default 'opengunass';
	DECLARE num1,num2,num3 number default 23.34;
	DECLARE mon1,mon2 money default 1000.00;
	DECLARE date1,date2 date default (date '2010-12-10 00:00:00');
	DECLARE point1,point2 point default (2,3);
	DECLARE lseg1,lseg2 lseg default   '[(2,3),(3,3)]' ;
	DECLARE cidr1,cidr2 cidr default '::10.2.3.4';
	DECLARE jsonb1,jsonb2 jsonb default '[1, " a ", {"a"   :1    }] ';
	type rectype is record(col1 int,col2 text);
	DECLARE rec1,rec2 rectype default  (1,'abc');
BEGIN
	res1 := 90;
	str1 := 'aa55';
	raise info 'type int res1:%,res2:%',res1,res2;
	raise info 'type text str1:%,str2:%',str1,str2;
	raise info 'type number num1:%,num2:%,num3:%',num1,num2,num3;
	raise info 'type money mon1:%,mon2:%',mon1,mon2;
	raise info 'type point point1:%,point2:%',point1,point2;
	raise info 'type lseg lseg1:%,lseg2:%',lseg1,lseg2;
	raise info 'type cidr cidr1:%,cidr2:%',cidr1,cidr2;
	raise info 'type jsonb jsonb1:%,jsonb2:%',jsonb1,jsonb2;
	raise info 'type record rec1:%,rec2:%',rec1,rec2;
END;
/

--test declare mutile variabe for cursor
DECLARE
    --定义游标
    TYPE CURSOR_TYPE IS REF CURSOR;
    C1,C2 CURSOR_TYPE;
BEGIN
END;
/

DECLARE 
	DECLARE C1,C2 SYS_REFCURSOR; 
BEGIN
END;
/

--test declare mutile variabe for alias
DECLARE 
	old1,old2 int;
	DECLARE alias1,alias2 ALIAS FOR old1; 
BEGIN
END;
/

--在function中使用
create or replace procedure declare_test1 as
type t1 is varray(10) of int;
type t2 is table of int;
type t3 is table of varchar2(10) index by varchar2(10);
type t4 is varray(10) of text;
declare va1,va2 t1 default '{1,2}';
declare vb1,vb2 t2 default '{3,4,5}';
declare vc1,vc2 t3 default '{a,b,cd}';
declare vt1,vt2 t4 ;
begin
	vt1[1] := 'a';
	vt1[2] := 'b';
	raise info 'array va1:%,va2:%', va1,va2;
	raise info 'table  vb1:%,vb2:%', vb1,vb2;
	raise info 'table of  vc1:%,vc2:%', vc1,vc2;
	raise info 'text array  vt1:%,vt2:%', vt1,vt2;
end;
/

create or replace function declare_test2() return void as
type t1 is varray(10) of int;
type t2 is table of int;
type t3 is table of varchar2(10) index by varchar2(10);
type t4 is varray(10) of text;
declare va1,va2 t1 default '{1,2}';
declare vb1,vb2 t2 default '{3,4,5}';
declare vc1,vc2 t3 default '{a,b,cd}';
declare vt1,vt2 t4 ;
begin
	vt1[1] := 'a';
	vt1[2] := 'b';
	raise info 'array va1:%,va2:%', va1,va2;
	raise info 'table  vb1:%,vb2:%', vb1,vb2;
	raise info 'table of  vc1:%,vc2:%', vc1,vc2;
	raise info 'text array  vt1:%,vt2:%', vt1,vt2;
end;
/

--在packakge中使用
create or replace package  pkg_test1 is
var4,var5 int:=42;
var6,var7 int:=43;
res1,res2 int default (3+4);
str1,str2 text default 'opengunass';
num1,num2,num3 number default 23.34;
mon1,mon2 money default 1000.00;
date1,date2 date default (date '2010-12-10 00:00:00');
point1,point2 point default (2,3);
lseg1,lseg2 lseg default   '[(2,3),(3,3)]' ;
cidr1,cidr2 cidr default '::10.2.3.4';
jsonb1,jsonb2 jsonb default '[1, " a ", {"a"   :1    }] ';
ype rectype is record(col1 int,col2 text);
ec1,rec2 rectype default  (1,'abc');
procedure testpro1();
end pkg_test1;
/
create or replace package body pkg_test1 is
var1,var2 int:=46;
procedure testpro1()
is
begin
	res1 := 90;
	str1 := 'aa55';
	raise info 'type int res1:%,res2:%',res1,res2;
	raise info 'type text str1:%,str2:%',str1,str2;
	raise info 'type number num1:%,num2:%,num3:%',num1,num2,num3;
	raise info 'type money mon1:%,mon2:%',mon1,mon2;
	raise info 'type point point1:%,point2:%',point1,point2;
	raise info 'type lseg lseg1:%,lseg2:%',lseg1,lseg2;
	raise info 'type cidr cidr1:%,cidr2:%',cidr1,cidr2;
	raise info 'type jsonb jsonb1:%,jsonb2:%',jsonb1,jsonb2;
	raise info 'type record rec1:%,rec2:%',rec1,rec2;
end;

begin
testpro1();
raise info 'ceclare multiple variable in package type int var1:%,var2:%',var1,var2;
end pkg_test1;
/

