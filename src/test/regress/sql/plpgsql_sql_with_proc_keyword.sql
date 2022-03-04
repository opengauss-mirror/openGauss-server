drop schema if exists plpgsql_table;
create schema plpgsql_table;
set current_schema = plpgsql_table;

create table test1(a varchar2(10),b varchar2(10));

create or replace package keyword_pkg is
cursor c1 is select * from test1 where a=(case when b='1' then 1 else 0 end);
procedure p1();
end keyword_pkg;
/

create or replace package body keyword_pkg is
procedure p1() as 
declare
rd record;
begin
open c1;
fetch c1 into rd;
end;
end keyword_pkg;
/

call keyword_pkg.p1();

drop table test1;
drop package keyword_pkg;

create or replace package emp_bonus is
var1 int:=1;--公有变量
var2 int:=2;
procedure testpro1(var3 int);--公有存储过程，可以被外部调用
-------------------------------------------------

-----package用例测试

-----create package specification语法格式

--create [ or replace ] package [ schema ] package_name

--[ invoker_rights_clause ] { is | as } item_list_1 end package_name;

--package specification(包规格)声明了包内的公有变量、函数、异常等，可以

--被外部函数或者存储过程调用。在package specification中只能声明存储过

--程，函数，不能定义存储过程或者函数。

--package只支持集中式，无法在分布式中使用。

--•在package specification中声明过的函数或者存储过程，必须在package body中找到定义。

--•在实例化中，无法调用带有commit/rollback的存储过程。

--•不能在trigger中调用package函数。

--•不能在外部sql中直接使用package当中的变量。

--•不允许在package外部调用package的私有变量和存储过程。

--•不支持其它存储过程不支持的用法，例如，在function中不允许调用commit/rollback，则package的function中同样无法调用commit/rollback。

--•不支持schema与package同名。

--•只支持a风格的存储过程和函数定义。

--•不支持package内有同名变量，包括包内同名参数。

--•package的全局变量为session级，不同session之间package的变量不共享。

--•package中调用自治事务的函数，不允许使用公有变量，以及递归的使用公有变量的函数。

--•package中不支持声明ref cursor类型。

--•create package specification语法格式create [ or replace ] package [ schema ] package_name

--    [ invoker_rights_clause ] { is | as } item_list_1 end package_name;

--

--invoker_rights_clause可以被声明为authid definer或者authid invoker，分别为定义者权限和调用者权限。

--item_list_1可以为声明的变量或者存储过程以及函数。

--

--package specification(包规格)声明了包内的公有变量、函数、异常等，可以被外部函数或者存储过程调用。在package specification中只能声明存储过程，函数，不能定义存储过程或者函数。

--

--•create package body语法格式。create [ or replace ] package body [ schema ] package_name

--    { is | as } declare_section [ initialize_section ] end package_name;

--

--package body(包体内)定义了包的私有变量，函数等。如果变量或者函数没有在package specification中声明过，那么这个变量或者函数则为私有变量或者函数。

--

--package body也可以声明实例化部分，用来初始化package，详见示例。

-------------------------------------------------
end emp_bonus;
/

create or replace package body emp_bonus is
var3 int:=3;
var4 int:=4;
procedure testpro1(var3 int)
is
begin
create table if not exists test1(col1 int);
insert into test1 values(var1);
insert into test1 values(var3);
-------------------------------------------------

-----package用例测试

-----create package specification语法格式

--create [ or replace ] package [ schema ] package_name

--[ invoker_rights_clause ] { is | as } item_list_1 end package_name;

--package specification(包规格)声明了包内的公有变量、函数、异常等，可以

--被外部函数或者存储过程调用。在package specification中只能声明存储过

--程，函数，不能定义存储过程或者函数。

--package只支持集中式，无法在分布式中使用。

--•在package specification中声明过的函数或者存储过程，必须在package body中找到定义。

--•在实例化中，无法调用带有commit/rollback的存储过程。

--•不能在trigger中调用package函数。

--•不能在外部sql中直接使用package当中的变量。

--•不允许在package外部调用package的私有变量和存储过程。

--•不支持其它存储过程不支持的用法，例如，在function中不允许调用commit/rollback，则package的function中同样无法调用commit/rollback。

--•不支持schema与package同名。

--•只支持a风格的存储过程和函数定义。

--•不支持package内有同名变量，包括包内同名参数。

--•package的全局变量为session级，不同session之间package的变量不共享。

--•package中调用自治事务的函数，不允许使用公有变量，以及递归的使用公有变量的函数。

--•package中不支持声明ref cursor类型。

--•create package specification语法格式create [ or replace ] package [ schema ] package_name

--    [ invoker_rights_clause ] { is | as } item_list_1 end package_name;

--

--invoker_rights_clause可以被声明为authid definer或者authid invoker，分别为定义者权限和调用者权限。

--item_list_1可以为声明的变量或者存储过程以及函数。

--

--package specification(包规格)声明了包内的公有变量、函数、异常等，可以被外部函数或者存储过程调用。在package specification中只能声明存储过程，函数，不能定义存储过程或者函数。

--

--•create package body语法格式。create [ or replace ] package body [ schema ] package_name

--    { is | as } declare_section [ initialize_section ] end package_name;

--

--package body(包体内)定义了包的私有变量，函数等。如果变量或者函数没有在package specification中声明过，那么这个变量或者函数则为私有变量或者函数。

--

--package body也可以声明实例化部分，用来初始化package，详见示例。

-------------------------------------------------
end;
begin --实例化开始
var4:=9;
testpro1(var4);
end emp_bonus;
/

drop package if exists  emp_bonus;


drop schema plpgsql_table;


