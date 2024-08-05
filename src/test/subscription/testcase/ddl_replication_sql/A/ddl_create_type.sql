create type atype as (id int, name text);
create type btype as object (id int, name text);
create type ctype as (id int, name text);
alter type ctype rename to dtype;
drop type dtype;