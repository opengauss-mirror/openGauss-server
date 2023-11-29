
drop schema if exists plpgsql_depend_reftype cascade;
create schema plpgsql_depend_reftype;
set current_schema = plpgsql_depend_reftype;
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

-- test 1 ref type
select create_data_table();

--- define schema define table ---
--cur_db.def_schema.def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c regression.plpgsql_depend_reftype.stu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c undefdb.plpgsql_depend_reftype.stu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--cur_db.def_schema.def_rowtype_in_prochead
create or replace procedure proc12(v_1 regression.plpgsql_depend_reftype.stu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.def_rowtype_in_prochead
create or replace procedure proc12(v_1 undefdb.plpgsql_depend_reftype.stu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- define schema undefine table ---
--cur_db.def_schema.undef_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c regression.plpgsql_depend_reftype.undefstu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.undef_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c undefdb.plpgsql_depend_reftype.undefstu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--cur_db.def_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 regression.plpgsql_depend_reftype.undefstu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 undefdb.plpgsql_depend_reftype.undefstu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- undefine schema ---
--cur_db.undef_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 regression.undefplpgsql_depend_reftype.undefstu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;


--- define schema define table ---
--def_schema.def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c plpgsql_depend_reftype.stu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_schema.def_rowtype_in_prochead
create or replace procedure proc12(v_1 plpgsql_depend_reftype.stu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_schema.undef_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c plpgsql_depend_reftype.undefstu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 plpgsql_depend_reftype.undefstu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- define table ---
--def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c stu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_rowtype_in_prochead
create or replace procedure proc12(v_1 stu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--undef_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c undefstu%RowType);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--undef_rowtype_in_prochead
create or replace procedure proc12(v_1 undefstu%RowType)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_type_in_body
create or replace package pkg
is
    type r1 is record(a int, c regression.plpgsql_depend_reftype.stu.sno%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;
--def_type_in_prohead
create or replace procedure proc12(v_1 regression.plpgsql_depend_reftype.stu.sno%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--undef_type_in_body
create or replace package pkg
is
    type r1 is record(a int, c regression.plpgsql_depend_reftype.stu.undefsno%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;
--undef_type_in_prohead
create or replace procedure proc12(v_1 regression.plpgsql_depend_reftype.stu.undefsno%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

select drop_data_table_cascade();

drop schema if exists plpgsql_depend_reftype cascade;
reset behavior_compat_options;
create schema plpgsql_depend_reftype;
set current_schema = plpgsql_depend_reftype;
set behavior_compat_options = 'plpgsql_dependency';
--test 2 ref pkg var type
create or replace package refpkg as
type r1 is record(a int, b int);
procedure proc1();
end refpkg;
/
--- define table ---
--cur_db.def_schema.def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c regression.plpgsql_depend_reftype.refpkg.r1.a%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c undefdb.plpgsql_depend_reftype.refpkg.r1.a%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--cur_db.def_schema.def_rowtype_in_prochead
create or replace procedure proc12(v_1 regression.plpgsql_depend_reftype.refpkg.r1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.def_rowtype_in_prochead
create or replace procedure proc12(v_1 undefdb.plpgsql_depend_reftype.refpkg.r1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;


--- define schema define pkgrec ---
--cur_db.def_schema.undef_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c regression.plpgsql_depend_reftype.refpkg.undefr1.a%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.undef_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c undefdb.plpgsql_depend_reftype.refpkg.undefr1.a%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--cur_db.def_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 regression.plpgsql_depend_reftype.refpkg.undefr1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--other_db.def_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 undefdb.plpgsql_depend_reftype.refpkg.undefr1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- define schema undefine pkg---
--cur_db.def_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 regression.plpgsql_depend_reftype.undefpkg.undefr1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- undefine schema ---
--cur_db.undef_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 regression.undefplpgsql_depend_reftype.refpkg.undefr1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- define schema define pkg rec---
--def_schema.def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c plpgsql_depend_reftype.refpkg.r1.a%Type);
    procedure proc1;
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_schema.def_rowtype_in_prochead rsy
create or replace procedure proc12(v_1 plpgsql_depend_reftype.refpkg.r1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- define schema define pkg rec ---
--def_schema.undef_rowtype_in_body 
create or replace package pkg
is
    type r1 is record(a int, c plpgsql_depend_reftype.refpkg.undefr1.a%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_schema.undef_rowtype_in_prochead
create or replace procedure proc12(v_1 plpgsql_depend_reftype.refpkg.undefr1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- undefine schema --- rsy
create or replace procedure proc12(v_1 undefplpgsql_depend_reftype.refpkg.undefr1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;


--- define pkg rec ---
--def_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c refpkg.r1.a%Type);
    procedure proc1;
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--def_rowtype_in_prochead
create or replace procedure proc12(v_1 refpkg.r1.a%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--- define pkg rec ---
--undef_rowtype_in_body
create or replace package pkg
is
    type r1 is record(a int, c refpkg.undefr1%Type);
    procedure proc1();
end pkg;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

--undef_rowtype_in_prochead
create or replace procedure proc12(v_1 refpkg.undefr1%Type)
as
begin
    NULL;
end;
/
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;

drop package refpkg;
select * from gs_dependencies_obj where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, name, type;
select schemaname, packagename, objectname, refobjpos from gs_dependencies where schemaname='plpgsql_depend_reftype' order by schemaname, packagename, objectname, refobjpos;


-- clean
drop schema plpgsql_depend_reftype cascade;
reset behavior_compat_options;