create database test_b dbcompatibility = 'B';
\c test_b

set plsql_show_all_error to on;

create or replace function f(v int[]) return int
as
n int;
begin
n := v();
return n;
end;
/

create or replace function f(v int[]) return int
as
n int;
begin
n := v[];
return n;
end;
/

set plsql_show_all_error to off;

create or replace function f(v int[]) return int
as
n int;
begin
n := v();
return n;
end;
/

create or replace function f(v int[]) return int
as
n int;
begin
n := v[];
return n;
end;
/

\c regression
drop database test_b;

create database test_pg dbcompatibility = 'PG';
\c test_pg

set plsql_show_all_error to on;

create or replace function f(v int[]) return int
as
n int;
begin
n := v();
return n;
end;
/

create or replace function f(v int[]) return int
as
n int;
begin
n := v[];
return n;
end;
/

set plsql_show_all_error to off;

create or replace function f(v int[]) return int
as
n int;
begin
n := v();
return n;
end;
/

create or replace function f(v int[]) return int
as
n int;
begin
n := v[];
return n;
end;
/

\c regression
drop database test_pg;