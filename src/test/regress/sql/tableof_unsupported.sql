------ Prepare ------
-- create type of TYPTYPE_TABLEOF
create type r0 is (c1 int1, c2 int2);

create type t0 is table of int4;
create type t1 is (c1 int1, c2 t0);
create type t2 is table of r0;

-- create table
create table tableof_unsupported(id t0);
create table tableof_unsupported(id t1);
create table tableof_unsupported(id t2);