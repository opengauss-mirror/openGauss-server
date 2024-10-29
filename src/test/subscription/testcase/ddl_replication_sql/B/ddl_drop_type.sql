create table atable (id int, age int);

create type atype as (id int, name text);
drop type atype;

drop type typ_not_exit;

drop type public.typ_not_exit;

drop type schema_not_exit.typ_not_exit;