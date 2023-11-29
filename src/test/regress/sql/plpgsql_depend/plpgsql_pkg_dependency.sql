drop schema if exists plpgsql_pkg_dependency cascade;
create schema plpgsql_pkg_dependency;
set current_schema = plpgsql_pkg_dependency;
set behavior_compat_options = 'plpgsql_dependency';

create or replace package test_package_depend2_pkg is
type t is record (col1 int, col2 text);
procedure p1(param test_package_depend3_pkg.t);
end test_package_depend2_pkg;
/
create  or replace package body test_package_depend2_pkg is
procedure p1(param test_package_depend3_pkg.t) is
    begin
        RAISE INFO 'call param: %', param;
    end;
end test_package_depend2_pkg;
/

create or replace package test_package_depend3_pkg is
type t is record (col1 int, col2 text, col3 varchar);
procedure p1(param test_package_depend2_pkg.t);
end test_package_depend3_pkg;
/
create  or replace  package body test_package_depend3_pkg is
procedure p1(param test_package_depend2_pkg.t) is
    begin
        RAISE INFO 'call param: %', param;
    end;
end test_package_depend3_pkg;
/

call test_package_depend2_pkg.p1((1,'a','2023'));
call test_package_depend3_pkg.p1((1,'a'));

drop package if exists test_package_depend2_pkg;
drop package if exists test_package_depend3_pkg;

-- clean
drop schema plpgsql_pkg_dependency cascade;
reset behavior_compat_options;