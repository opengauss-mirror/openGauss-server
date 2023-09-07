drop schema if exists pkg_var_test cascade;
create schema pkg_var_test;
set current_schema = pkg_var_test;
set behavior_compat_options = 'plpgsql_dependency';

-- 1.test function depends on package var in assignment statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    func_var := test_pkg.ref_var;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 1.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    func_var := test_pkg.ref_var;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 2.test function depends on package var in return statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    func_var := 1;
    return test_pkg.ref_var;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 2.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    func_var := 1;
    return test_pkg.ref_var;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;


-- 3.test function depends on package var in if statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    if test_pkg.ref_var = 1 then
        func_var := 1;
    end if;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 3.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    if test_pkg.ref_var = 1 then
        func_var := 1;
    end if;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;


-- 4.test function depends on package var in while statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    while test_pkg.ref_var < 1 loop
        func_var := 1;
    end loop;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 4.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    while test_pkg.ref_var < 1 loop
        func_var := 1;
    end loop;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 5.test function depends on package var in case statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    case test_pkg.ref_var
        when 1 then
        func_var := 1;
    end case;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 5.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace function test_func return int
is
    func_var int;
begin
    case test_pkg.ref_var
        when 1 then
        func_var := 1;
    end case;
    return 1;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 6.test procedure depends on package var in assignment statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    proc_var := test_pkg.ref_var;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;

-- 6.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    proc_var := test_pkg.ref_var;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;


-- 7.test procedure depends on package var in if statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    if test_pkg.ref_var = 1 then
        proc_var := 1;
    end if;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;

-- 7.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    if test_pkg.ref_var = 1 then
        proc_var := 1;
    end if;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;

-- 8.test procedure depends on package var in while statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    while test_pkg.ref_var < 1 loop
        proc_var := 1;
    end loop;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;

-- 8.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    while test_pkg.ref_var < 1 loop
        proc_var := 1;
    end loop;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;


-- 9.test procedure depends on package var in case statement
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    case test_pkg.ref_var
        when 1 then
        proc_var := 1;
    end case;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    -- unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    -- ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;

-- 9.1 modifying the package var type
create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
create or replace procedure test_proc
is
    proc_var int;
begin
    case test_pkg.ref_var
        when 1 then
        proc_var := 1;
    end case;
end;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';


create or replace package test_pkg as
    ref_var int;
    unref_var varchar(16);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var varchar(16);
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

create or replace package test_pkg as
    ref_var int;
    unref_var int;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

alter procedure test_proc compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_proc%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_proc';

drop procedure test_proc;

-- 10.test the cursor var of the dependent package in the open statement
create table test_table (ref_col int, unref_col int);

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/

create or replace function test_func return int
is
begin
    open test_pkg.ref_var;
    return 1;
end;
/

select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    -- cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    -- cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;


-- 10.1 modifying the package var type

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/

create or replace function test_func return int
is
begin
    open test_pkg.ref_var;
    return 1;
end;
/

select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    unref_var varchar(32);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var varchar(32);
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;
drop table test_table;

-- 11.test the cursor var of the dependent package in the fetch statement
create table test_table (ref_col int, unref_col int);

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/

create or replace function test_func return int
is
    func_var int;
begin
    fetch test_pkg.ref_var into func_var;
    return func_var;
end;
/

select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    -- cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    -- cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;

-- 11.1 modifying the package var type

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/

create or replace function test_func return int
is
    func_var int;
begin
    fetch test_pkg.ref_var into func_var;
    return func_var;
end;
/

select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    unref_var varchar(32);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var varchar(32);
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;
drop table test_table;

-- 12.test the cursor var of the dependent package in the close statement
create table test_table (ref_col int, unref_col int);

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/

create or replace function test_func return int
is
begin
    close test_pkg.ref_var;
    return 1;
end;
/

select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    -- cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    -- cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;


-- 12.1 modifying the package var type

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/

create or replace function test_func return int
is
begin
    close test_pkg.ref_var;
    return 1;
end;
/

select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos,
        b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast
    from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid
    order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace 
    where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and
    pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    unref_var varchar(32);
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    ref_var varchar(32);
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


create or replace package test_pkg as
    cursor ref_var is select ref_col from test_table;
    cursor unref_var is select unref_col from test_table;
end test_pkg;
/
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


alter function test_func compile;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';


drop package test_pkg;
select a.schemaname as sn, a.packagename as pn, a.objectname as on, a.refobjpos as refpos, b.schemaname as refsn, b.packagename as refpn, b.name as refon, b.type as refot, b.objnode as refast from gs_dependencies as a, gs_dependencies_obj as b where a.schemaname = 'pkg_var_test' and a.objectname like 'test_func%' and a.refobjoid = b.oid order by a.schemaname, a.objectname, a.refobjpos, b.type, b.name;
select nspname, proname, valid from pg_proc,pg_object,pg_namespace where pg_proc.oid=pg_object.object_oid and pg_namespace.oid=pg_proc.pronamespace and pg_namespace.nspname='pkg_var_test' and pg_proc.proname='test_func';

drop function test_func;
drop table test_table;
-- clean
drop schema pkg_var_test cascade;
reset behavior_compat_options;