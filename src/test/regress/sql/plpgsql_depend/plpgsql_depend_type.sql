drop schema if exists plpgsql_depend_type cascade;
create schema plpgsql_depend_type;
set current_schema = plpgsql_depend_type;
set behavior_compat_options = 'plpgsql_dependency';

-- prepare data table
create or replace function create_data_table returns int as $$
begin
drop table if exists stu;
create table stu (sno int, name varchar, sex varchar, cno int);
insert into stu values(1,'zhang','M',1);
insert into stu values(1,'zhang','M',1);
insert into stu values(2,'wangwei','M',2);
insert into stu values(3,'liu','F',3);
return 1;
end;
$$
LANGUAGE plpgsql;
-- drop data table
create or replace function drop_data_table returns int as $$
begin
drop table if exists stu;
return 1;
end;
$$
LANGUAGE plpgsql;
-- drop data table cascade
create or replace function drop_data_table_cascade returns int as $$
begin
drop table if exists stu cascade;
return 1;
end;
$$
LANGUAGE plpgsql;
-- test 0 pkg_record_undefine_schema_type
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, b sr);
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (
    select Oid from gs_package where pkgname='pkg' and pkgnamespace = (
        select Oid from pg_namespace where nspname = 'plpgsql_depend_type')
    );
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (
    a.schemaname='plpgsql_depend_type' and a.packagename='pkg'
) or (
    b.schemaname='plpgsql_depend_type' and b.packagename='pkg'
) order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (
    a.schemaname='plpgsql_depend_type' and a.packagename='pkg'
) or (
    b.schemaname='plpgsql_depend_type' and b.packagename='pkg'
) order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 1 pkg_record_non_complete_define_schema_type
drop package if exists pkg2;
create or replace package pkg2
is
    type r2 is record(a int, b sr);
    procedure proc1;
end pkg2;
/
create or replace package pkg
is
    type r1 is record(a int, b pkg2.r2);
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove ref dependency, undefined
drop package pkg2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

 -- test 2 pkg_record_define_schema_type
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, b sr);
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove ref dependency, undefined
drop type sr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 3 pkg_tbl_undefine_schema_type
drop type if exists undefsr cascade;
drop package if exists pkg;
create or replace package pkg
is
    type tf1 is table of undefsr index by binary_integer;
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
create type undefsr as (a int, b int);
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
drop type undefsr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 4 pkg_tbl_define_schema_type
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    type tf1 is table of sr index by binary_integer;
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency
drop type sr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 5 pkg_varray_undefine_schema_type
drop type if exists undefsr cascade;
drop package if exists pkg;
create or replace package pkg
is
    type varr1 is varray(1024) of undefsr;
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
create type undefsr as (a int, b int);
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
drop type undefsr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 6 pkg_varray_define_schema_type
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    type t_arr_sr is varray(999) of sr;
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 7 pkg_tbl_undefine_pkg_type
drop package if exists pkg2;
drop package if exists pkg;
create or replace package pkg
is
    type tf1 is table of pkg2.r1 index by binary_integer;
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 8 pkg_tbl_define_pkg_type
drop package if exists pkg2;
create or replace package pkg2
is
    type r1 is record(a int, b int);
    procedure proc1;
end pkg2;
/
drop package if exists pkg;
create or replace package pkg
is
    type tf1 is table of pkg2.r1 index by binary_integer;
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop package pkg2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 9 pkg_varray_undefine_pkg_type
drop package if exists pkg2;
drop package if exists pkg;
create or replace package pkg
is
    type t_arr_sr is varray(999) of pkg2.r1;
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 10 pkg_varray_define_pkg_type
drop package if exists pkg2;
create or replace package pkg2
is
    type r1 is record(a int, b int);
    procedure proc1;
end pkg2;
/
drop package if exists pkg;
create or replace package pkg
is
    type tf1 is varray(999) of pkg2.r1;
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop package pkg2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 11 pkg_record_undefine_table_rowtype
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c undefStu%RowType);
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 12 pkg_record_define_table_rowtype
select create_data_table();
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c stu%RowType);
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

alter table stu add column z int;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

alter package pkg compile;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

alter table stu drop column z;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

alter package pkg compile;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

alter table stu rename to newstu;

-- remove dependency, no drop
select drop_data_table();
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, undefined
select drop_data_table_cascade();
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 13 pkg_record_undefine_table_undefine_col_type
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c undefStu.undefname%Type);
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 14 pkg_record_define_table_undefine_col_type
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c stu.undefname%Type);
    procedure proc1;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 15 pkg_record_define_table_col_type
select create_data_table();
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c stu.name%Type);
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, undefined
select drop_data_table();
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 16 pkg_record_define_pkg_v1_type
drop package if exists pkg2;
create or replace package pkg2
is
    v1 int;
    procedure proc1;
end pkg2;
/
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c pkg2.v1%Type);
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency, undefined
drop package pkg2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 16 pkg_record_define_pkg_r1_rowtype
drop package if exists pkg2;
create or replace package pkg2
is
    type r2 is record(a int, b int);
    procedure proc1;
end pkg2;
/
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c pkg2.r2%RowType);
    procedure proc1;
end pkg;
/
-- compile error
-- valid is none
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop package pkg2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 17 pkg_record_define_schema_pkg_r1_rowtype
drop package if exists pkg2;
create or replace package pkg2
is
    type r2 is record(a int, b int);
    procedure proc1;
end pkg2;
/
drop package if exists pkg;
create or replace package pkg
is
    type r1 is record(a int, c plpgsql_depend_type.pkg2.r2%RowType);
    procedure proc1;
end pkg;
/
-- compile error
-- valid is none
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop package pkg2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 18 pkg_body_record_define_schema
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    function proc1() return int;
end pkg;
/
create or replace package body pkg
is
    type r1 is record(a int, b sr);
    function proc1() return int as
    begin
        return 1;
    end;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 19 pkg_body_tableof_define_schema
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    function proc1() return int;
end pkg;
/
create or replace package body pkg
is
    type tf1 is table of sr index by binary_integer;
    function proc1() return int as
    begin
        return 1;
    end;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 20 pkg_body_varray_define_schema
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    function proc1() return int;
end pkg;
/
create or replace package body pkg
is
    type t_arr_sr is varray(999) of sr;
    function proc1() return int as
    begin
        return 1;
    end;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 21 pkg_body_record_undefine_schema_type
drop package if exists pkg;
create or replace package pkg
is
    function proc1() return int;
end pkg;
/
create or replace package body pkg
is
    type t_arr_sr is varray(999) of undefsr;
    function proc1() return int as
    begin
        return 1;
    end;
end pkg;
/
-- valid is false
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 22 pkg_spec_var_used_undefine_pkg_type
drop package if exists pkg;
create or replace package pkg
is
    v_1 pkg2.r1;
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop package pkg2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 23 pkg_spec_var_used_define_schema_type
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    v_1 sr;
    procedure proc1;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 24 pkg_body_var_used_define_schema_type
drop type if exists sr cascade;
create type sr as(a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    procedure proc1;
end pkg;
/
create or replace package body pkg
is
    v_1 sr;
    procedure proc1() as  begin null; end;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 25 pkg_proc_body_var_used_define_schema_type
drop type if exists sr cascade;
create type sr as(a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    procedure proc1;
end pkg;
/
create or replace package body pkg
is
    procedure proc1() as  
    declare
        v_1 sr;
    begin null; end;
end pkg;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency, undefined
drop type sr cascade;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop package pkg;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 26 proc_body_var_used_define_schema_type
drop type if exists sr cascade;
create type sr as(a int, b int);
drop procedure if exists proc1;
create or replace procedure proc1() as  
declare
    v_1 sr;
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

-- test 27 proc_body_var_used_define_pkg_tablof
drop type if exists sr cascade;
create type sr as(a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    type tf1 is table of sr index by binary_integer;
end pkg;
/
drop procedure if exists proc1;
create or replace procedure proc1() as  
declare
    v_tf1 pkg.tf1;
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- drop ref dependency
drop type sr cascade;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency
drop package pkg;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 28 proc_body_var_used_define_pkg_varray
drop type if exists sr cascade;
create type sr as(a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    type arr1 is varray(888) of sr;
    type tf1 is table of sr index by binary_integer;
end pkg;
/
drop procedure if exists proc1;
create or replace procedure proc1() as  
declare
    v_arr1 pkg.arr1;
    v_tf1 pkg.tf1;
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- drop ref dependency
drop type sr cascade;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove ref dependency
drop package pkg;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='B' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 29 proc_param_type_used_define_schema_type
drop type if exists sr cascade;
create type sr as(a int, b int);
drop procedure if exists proc1;
create or replace procedure proc1(v_1 sr) as  
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.sr)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.sr)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
drop type sr cascade;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(pg_catalog.undefined)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(pg_catalog.undefined)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
  -- create ref dependency
create type sr as (a int, b int);
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.sr)')
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.sr)')
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.sr)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.sr)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 30 proc_return_type_used_define_schema_type
drop type if exists sr cascade;
create type sr as(a int, b int);
drop function if exists proc1;
create or replace function proc1() return sr as  
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove ref dependency
drop type sr cascade;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- create ref dependency
create type sr as (a int, b int);
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()')
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()')
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop function proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 31 proc_param_type_used_undefine_schema_type
drop type if exists sr cascade;
drop procedure if exists proc1;
create or replace procedure proc1(v_1 sr) as  
begin 
    null;
end;
/
-- valid is false
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(pg_catalog.undefined)')
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(pg_catalog.undefined)')
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- create ref dependency
create type sr as (a int, b int);
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on
po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = 
(select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.sr)')
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.sr)')
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- define ref dependency
alter procedure proc1 compile;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
drop type sr cascade;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(pg_catalog.undefined)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(pg_catalog.undefined)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.sr)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.sr)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos
 from gs_dependencies where schemaname='plpgsql_depend_type'
 order by schemaname, packagename, objectname, refobjpos;
-- test 32 proc_return_type_used_undefine_schema_type
drop type if exists sr cascade;
drop function if exists proc1;
create or replace function proc1() return sr as  
begin 
    null;
end;
/
-- valid is false
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- create ref dependency
create type sr as (a int, b int);
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency
drop type sr cascade;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

--test 33 proc_param_type_used_define_pkg_type
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    type arr1 is varray(888) of sr;
    type tf1 is table of sr index by binary_integer;
    procedure proc1;
end pkg;
/
drop procedure if exists proc1;
create or replace procedure proc1(v_1 pkg.arr1) as
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- remove ref dependency
create or replace package pkg
is
    type tf1 is table of sr index by binary_integer;
    procedure proc1;
end pkg;
/
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(pg_catalog.undefined)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(pg_catalog.undefined)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
  -- create ref dependency
create or replace package pkg
is
    type arr1 is varray(888) of sr;
    procedure proc1;
end pkg;
/
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 34 proc_param_type_used_define_pkg_type
drop type if exists sr cascade;
create type sr as (a int, b int);
drop package if exists pkg;
create or replace package pkg
is
    type arr1 is varray(888) of sr;
    type tf1 is table of sr index by binary_integer;
    procedure proc1;
end pkg;
/
drop procedure if exists proc1;
create or replace function proc1() return pkg.tf1 as
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- remove ref dependency
create or replace package pkg
is
    type arr1 is varray(888) of sr;
    procedure proc1;
end pkg;
/
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
  -- create ref dependency
create or replace package pkg
is
    type tf1 is table of sr index by binary_integer;
    procedure proc1;
end pkg;
/
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1()') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1()') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- is clean
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

--test 35 proc_param_type_used_undefine_pkg_type
drop type if exists sr cascade;
drop package if exists pkg;
drop procedure if exists proc1;
create or replace procedure proc1(v_1 pkg.arr1) as
begin 
    null;
end;
/
-- valid is true
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(pg_catalog.undefined)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(pg_catalog.undefined)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
 -- remove ref dependency
create or replace package pkg
is
    type tf1 is table of sr index by binary_integer;
    procedure proc1;
end pkg;
/

select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(pg_catalog.undefined)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(pg_catalog.undefined)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

 -- create ref dependency
create or replace package pkg
is
    type arr1 is varray(888) of sr;
    procedure proc1;
end pkg;
/

select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

create type sr as (a int, b int);
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

alter package pkg compile;
drop type sr;
drop type sr cascade;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select valid from pg_object where object_type='S' and object_oid in (select Oid from gs_package where pkgname='pkg' and pkgnamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

drop package pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

-- remove dependency, no rows
drop procedure proc1;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc1' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_depend_type'));
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.pkg.arr1)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.pkg.arr1)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.packagename='pkg') or (b.schemaname='plpgsql_depend_type' and b.packagename='pkg') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

--test 36 schema_table_nest_record
create type ssr is (
    id integer,
    name varchar,
    addr text
);
create type stab is table of ssr;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'stab') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;

create or replace procedure proc1(a stab) as begin null; end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'stab') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
 b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast 
 from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid 
 where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.stab)') 
 or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.stab)') 
 order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');

create or replace procedure proc1(a ssr) as begin null; end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'stab') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.ssr)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.ssr)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.stab)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.stab)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');

drop type ssr;
drop type ssr cascade;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'stab') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.ssr)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.ssr)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.stab)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.stab)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(pg_catalog.undefined)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(pg_catalog.undefined)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');

create type ssr is (
    id integer,
    name varchar,
    addr text
);
create type stab is table of ssr;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'stab') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.ssr)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.ssr)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.stab)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.stab)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');

drop procedure proc1;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'stab') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.ssr)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.ssr)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.stab)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.stab)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');

drop type ssr;
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- is clean
drop type stab;
select * from gs_dependencies_obj where schemaname='plpgsql_depend_type' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_type' order by schemaname, packagename, objectname, refobjpos;

-- test 37
create type t2 as enum ('create', 'modify', 'closed');

alter type t2 rename value 'create' to 'newcreate';

create or replace procedure proc1(vb t2) as
declare
v_1 t2;
begin
null;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.t2)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.t2)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');

alter type t2 add value 'modif3' before 'closed';
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.t2)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.t2)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');

alter type t2 rename to t3;
drop type t2;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a right join gs_dependencies_obj as b on b.oid = a.refobjoid where (a.schemaname='plpgsql_depend_type' and a.objectname = 'proc1(plpgsql_depend_type.t2)') or (b.schemaname='plpgsql_depend_type' and b.name = 'proc1(plpgsql_depend_type.t2)') order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select pr.proname, po.valid, pr.proargtypes, pr.proargsrc from pg_proc as pr inner join pg_object as po on po.object_type='P' and po.object_oid = pr.Oid where pr.propackageid=0 and pr.proname='proc1' and pr.pronamespace = (select Oid from pg_namespace where nspname='plpgsql_depend_type');


-- clean
drop schema plpgsql_depend_type cascade;
reset behavior_compat_options;