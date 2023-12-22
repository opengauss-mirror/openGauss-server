--测试type重建，OID不变功能
create schema test_type_replace;
set search_path = test_type_replace;
-- CREATE OR REPLACE TYPE name AS ( xxx )

-- Case 1：若类型不存在，按create type语法创建。
-- Case 2：若类型已存在（同命名空间下，与create 语法的判断保持一致）：
--    Case 2.1：原类型为shell类型或自动生成的数组类型，按create type语法创建。
--    Case 2.2：原类型为复合类型：
--        Case2.2.1：原类型被表引用，报错。
--        Case2.2.2：原类型无表引用，转换为AlterTableStmt。
--    Case2.3:原类型为其他类型，报错。

--case 1
create or replace type typ1 as (a int, b text);

--case 2
--    case 2.1 原类型为shell类型
create type shell1;
create or replace type shell1 as (a int, b text);

--    case 2.1 原类型为自动生成的数组类型
create or replace type _typ1 as (a int, b text);

--    case 2.2 原类型为复合类型
--        case 2.2.1 原类型被表直接引用
create table tb1 (a typ1);
create or replace type typ1 as (a int, b text); --报错提示有表依赖
drop table tb1;

--       case 2.2.1 原类型被表间接引用
create type typ2 as (a typ1);
create table tb1 (a typ2);
create or replace type typ1 as (a int, b text); --报错提示有表依赖,tb1->typ2->typ1
drop table tb1;
drop type typ2;

--        case 2.2.1 原类型无表引用
--作为函数入参
create procedure proc1 (a typ1) as
begin
    raise info 'a = %',a;
end;
/

create or replace type typ1 as (a int, b text, c varchar);

declare
  var typ1;
begin
  proc1(var);
end;
/

--    case 2.3 原类型为其他类型
create type enum1 AS ENUM ('one', 'two', 'three');
create or replace type enum1 AS (a text);

drop type typ1 cascade;
drop type _typ1 cascade;
drop type shell1;
drop type enum1;

-- CREATE OR REPLACE TYPE name AS TABLE OF data_type

-- Case1:若类型不存在，则按create语法创建（TableOfTypeStmt）。
-- Case2:若类型已存在（同命名空间下，与create 语法的判断保持一致）：
--     Case2.1: 若类型为shell类型或自动生成的数组类型，按create语法创建。
--     Case2.2：若类型为table of 类型（o类型）：
--         Case2.2.1：若类型无表引用且新的被引用类型为非table of 类型（o类型），替换引用类型，更新依赖关系
--         Case2.2.2：其他：报错。
--     Case2.3：若类型为其他类型，报错。

--case 1
create or replace type typ1 as table of int;

--case 2
--    case 2.1 原类型为shell类型
create type shell1;
create or replace type shell1 as table of varchar;

--    case 2.1 原类型为自动生成的数组类型
create or replace type _typ1 as table of varchar;

--    case 2.2 原类型为复合类型
--        case 2.2.1 原类型被表引用
create table tb1 (a typ1);
create or replace type typ1 as table of varchar; --报错提示有表依赖
drop table tb1;

--        case 2.2.1 原类型无表引用
--作为函数入参
create procedure proc1 (a typ1) as
begin
    raise info 'a = %',a;
end;
/

create or replace type typ1 as table of varchar;

--    case 2.3 原类型为其他类型
create type enum1 AS ENUM ('one', 'two', 'three');
create or replace type enum1 AS table of varchar;

drop schema test_type_replace cascade;
