-- PR 9664 / DTS18: public updates and inherited private package state.
SET client_min_messages = notice;
create schema dts18_schema;
set search_path = dts18_schema;
create package dts18_public_only as
    public_value int := 1;
end dts18_public_only;
/
create package body dts18_public_only as
end dts18_public_only;
/
create function dts18_public_only_auto() return int
is
    pragma autonomous_transaction;
begin
    dts18_public_only.public_value := dts18_public_only.public_value + 1;
    commit;
    return dts18_public_only.public_value;
end;
/
begin
    dts18_public_only.public_value := 10;
end;
/
select dts18_public_only_auto();
begin
    raise notice 'dts18_public_only=%', dts18_public_only.public_value;
end;
/

create package dts18_pkg_merge as
    public_value int := 1;
    procedure dts18_initialize(v int);
    function dts18_get_private() return int;
end dts18_pkg_merge;
/
create or replace package body dts18_pkg_merge as
    private_value int := 100;
    procedure dts18_initialize(v int) is
    begin
        public_value := v;
        private_value := v + 90;
    end;
    function dts18_get_private() return int is
    begin
        return private_value;
    end;
end dts18_pkg_merge;
/
create function dts18_pkg_merge_auto() return int
is
    pragma autonomous_transaction;
    private_result int;
begin
    dts18_pkg_merge.public_value := dts18_pkg_merge.public_value + 1;
    private_result := dts18_pkg_merge.dts18_get_private();
    commit;
    return dts18_pkg_merge.public_value * 1000 + private_result;
end;
/
begin
    dts18_pkg_merge.dts18_initialize(20);
end;
/
select dts18_pkg_merge_auto();
begin
    raise notice 'dts18_pkg_merge=%', dts18_pkg_merge.public_value;
end;
/

drop function dts18_public_only_auto();
drop package dts18_public_only;
drop function dts18_pkg_merge_auto();
drop package dts18_pkg_merge;
drop schema dts18_schema;
reset search_path;
RESET client_min_messages;
