CREATE EXTENSION gms_sql;
set gms_sql_max_open_cursor_count = 501;
reset gms_sql_max_open_cursor_count;
show gms_sql_max_open_cursor_count;
do $$
declare
  c int;
  strval varchar;
  intval int;
  nrows int default 30;
begin
  c := gms_sql.open_cursor();
  gms_sql.parse(c, 'select ''ahoj'' || i, i from generate_series(1, :nrows) g(i)', gms_sql.v6);
  gms_sql.bind_variable(c, 'nrows', nrows);
  gms_sql.define_column(c, 1, strval);
  gms_sql.define_column(c, 2, intval);
  perform gms_sql.execute(c);
  while gms_sql.fetch_rows(c) > 0
  loop
    gms_sql.column_value(c, 1, strval);
    gms_sql.column_value(c, 2, intval);
    raise notice 'c1: %, c2: %', strval, intval;
  end loop;
  gms_sql.close_cursor(c);
end;
$$;

do $$
declare
  c int;
  strval varchar;
  intval int;
  nrows int default 30;
begin
  c := gms_sql.open_cursor();
  gms_sql.parse(c, 'select ''ahoj'' || i, i from generate_series(1, :nrows) g(i)', gms_sql.v7);
  gms_sql.bind_variable(c, 'nrows', nrows);
  gms_sql.define_column(c, 1, strval);
  gms_sql.define_column(c, 2, intval);
  perform gms_sql.execute(c);
  while gms_sql.fetch_rows(c) > 0
  loop
    strval := gms_sql.column_value_f(c, 1, strval);
    intval := gms_sql.column_value_f(c, 2, intval);
    raise notice 'c1: %, c2: %', strval, intval;
  end loop;
  gms_sql.close_cursor(c);
end;
$$;

drop table if exists foo;

create table foo(a int, b varchar, c numeric);

do $$
declare c int;
begin
  c := gms_sql.open_cursor();
  gms_sql.parse(c, 'insert into foo values(:a, :b, :c)', gms_sql.native);
  for i in 1..100
  loop
    gms_sql.bind_variable(c, 'a', i);
    gms_sql.bind_variable(c, 'b', 'Ahoj ' || i);
    gms_sql.bind_variable(c, 'c', i + 0.033);
    perform gms_sql.execute(c);
  end loop;
  gms_sql.close_cursor(c);
end;
$$;

select * from foo;
truncate foo;

do $$
declare c int;
begin
  c := gms_sql.open_cursor();
  gms_sql.parse(c, 'insert into foo values(:a, :b, :c)', gms_sql.native);
  for i in 1..100
  loop
    gms_sql.bind_variable_f(c, 'a', i);
    gms_sql.bind_variable_f(c, 'b', 'Ahoj ' || i);
    gms_sql.bind_variable_f(c, 'c', i + 0.033);
    perform gms_sql.execute(c);
  end loop;
  gms_sql.close_cursor(c);
end;
$$;

select * from foo;
truncate foo;

do $$
declare
  c int;
  a int[];
  b varchar[];
  ca numeric[];
begin
  c := gms_sql.open_cursor();
  gms_sql.parse(c, 'insert into foo values(:a, :b, :c)', gms_sql.v6);
  a := ARRAY[1, 2, 3, 4, 5];
  b := ARRAY['Ahoj', 'Nazdar', 'Bazar'];
  ca := ARRAY[3.14, 2.22, 3.8, 4];

  perform gms_sql.bind_array(c, 'a', a);
  perform gms_sql.bind_array(c, 'b', b);
  perform gms_sql.bind_array(c, 'c', ca);
  raise notice 'inserted rows %d', gms_sql.execute(c);
  gms_sql.close_cursor(c);
end;
$$;

select * from foo;
truncate foo;

do $$
declare
  c int;
  a int[];
  b varchar[];
  ca numeric[];
begin
  c := gms_sql.open_cursor();
  gms_sql.parse(c, 'insert into foo values(:a, :b, :c)', gms_sql.v7);
  a := ARRAY[1, 2, 3, 4, 5];
  b := ARRAY['Ahoj', 'Nazdar', 'Bazar'];
  ca := ARRAY[3.14, 2.22, 3.8, 4];

  perform gms_sql.bind_array(c, 'a', a, 2, 3);
  perform gms_sql.bind_array(c, 'b', b, 3, 4);
  perform gms_sql.bind_array(c, 'c', ca);
  raise notice 'inserted rows %d', gms_sql.execute(c);
  gms_sql.close_cursor(c);
end;
$$;

select * from foo;
truncate foo;

do $$
declare
  c int;
  a int[];
  b varchar[];
  ca numeric[];
begin
  c := gms_sql.open_cursor();
  gms_sql.parse(c, 'select i, ''Ahoj'' || i, i + 0.003 from generate_series(1, 35) g(i)', 0);
  gms_sql.define_array(c, 1, a, 10, 1);
  gms_sql.define_array(c, 2, b, 10, 1);
  gms_sql.define_array(c, 3, ca, 10, 1);

  perform gms_sql.execute(c);
  while gms_sql.fetch_rows(c) > 0
  loop
    gms_sql.column_value(c, 1, a);
    gms_sql.column_value(c, 2, b);
    gms_sql.column_value(c, 3, ca);
    raise notice 'a = %', a;
    raise notice 'b = %', b;
    raise notice 'c = %', ca;
  end loop;
  gms_sql.close_cursor(c);
end;
$$;

drop table foo;

do $$
declare
l_curid int;
l_cnt int;
l_desctab gms_sql.desc_tab;
l_sqltext varchar(2000);
begin
  l_sqltext='select * from pg_object;';
  l_curid := gms_sql.open_cursor();
  gms_sql.parse(l_curid, l_sqltext, 0);
  gms_sql.describe_columns(l_curid, l_cnt, l_desctab);
  for i in 1 .. l_desctab.count loop
    raise notice '%,% ', l_desctab(i).col_name,l_desctab(i).col_type;
  end loop;
  gms_sql.close_cursor(l_curid);
end;
$$;

create table t1(id int, name varchar(20));
insert into t1 select generate_series(1,3), 'abcddd';
create table t2(a int, b date);
insert into t2 values(1, '2022-12-11 10:00:01.123');
insert into t2 values(3, '2022-12-12 12:00:11.13');

do $$
declare
  c1 refcursor;
  c2 refcursor;
begin
  open c1 for select * from t1;
  gms_sql.return_result(c1);
  open c2 for select * from t2;
  gms_sql.return_result(c2);
end;
$$;


create procedure test_result() as
declare
  c1 refcursor;
  c2 refcursor;
begin
  open c1 for select * from t1;
  gms_sql.return_result(c1);
  open c2 for select * from t2;
  gms_sql.return_result(c2);
end;
/
call test_result();
drop procedure test_result;

create procedure aam() as
declare
id1 int;
id2 int;
begin
id1 :=gms_sql.open_cursor();
gms_sql.parse(id1,'select * from t1', 1);
perform gms_sql.execute(id1);
gms_sql.return_result(id1);
gms_sql.close_cursor(id1);
id2 :=gms_sql.open_cursor();
gms_sql.parse(id2,'select * from t2', 2);
perform gms_sql.execute(id2);
gms_sql.return_result(id2);
gms_sql.close_cursor(id2);
end;
/
call aam();
drop procedure aam;
create table col_name_too_long(aaaaabbbbbcccccdddddeeeeefffffggg int, col2 text);

do $$
declare
l_curid int;
l_cnt int;
l_desctab gms_sql.desc_tab;
l_desctab2 gms_sql.desc_tab2;
l_sqltext varchar(2000);
begin
  l_sqltext='select * from t1;';
  l_curid := gms_sql.open_cursor();
  gms_sql.parse(l_curid, l_sqltext, 1);
  gms_sql.describe_columns(l_curid, l_cnt, l_desctab);
  for i in 1 .. l_desctab.count loop
    raise notice '%', l_desctab(i).col_name;
  end loop;
  -- output col_name
  l_sqltext='select * from col_name_too_long;';
  gms_sql.parse(l_curid, l_sqltext, 1);
  gms_sql.describe_columns2(l_curid, l_cnt, l_desctab2);
  for i in 1 .. l_desctab2.count loop
    raise notice '%', l_desctab2(i).col_name;
  end loop;
  -- error
  l_sqltext='select * from col_name_too_long;';
  gms_sql.parse(l_curid, l_sqltext, 1);
  gms_sql.describe_columns(l_curid, l_cnt, l_desctab);
  for i in 1 .. l_desctab.count loop
    raise notice '%', l_desctab(i).col_name;
  end loop;
end;
$$;
select gms_sql.is_open(0);
select gms_sql.close_cursor(0);
do $$
declare
l_curid int;
l_cnt int;
l_desctab3 gms_sql.desc_tab3;
l_desctab4 gms_sql.desc_tab4;
l_sqltext varchar(2000);
begin
  l_sqltext='select * from col_name_too_long;';
  l_curid := gms_sql.open_cursor();
  gms_sql.parse(l_curid, l_sqltext, 1);
  gms_sql.describe_columns3(l_curid, l_cnt, l_desctab3);
  for i in 1 .. l_desctab3.count loop
    raise notice '%,%,%', l_desctab3(i).col_type,l_desctab3(i).col_type_name,l_desctab3(i).col_name;
  end loop;
  gms_sql.parse(l_curid, l_sqltext, 1);
  gms_sql.describe_columns3(l_curid, l_cnt, l_desctab4);
  for i in 1 .. l_desctab4.count loop
    raise notice '%,%,%,%', l_desctab3(i).col_type,l_desctab4(i).col_type_name,l_desctab4(i).col_type_name_len,l_desctab4(i).col_name_len;
  end loop;
  gms_sql.close_cursor(l_curid);
end;
$$;

drop table t1,t2, col_name_too_long;

select gms_sql.open_cursor();
select gms_sql.is_open(0);
select gms_sql.open_cursor();
select gms_sql.is_open(1);
select gms_sql.open_cursor();
select gms_sql.is_open(2);
select gms_sql.open_cursor();
select gms_sql.is_open(3);
select gms_sql.close_cursor(0);
select gms_sql.close_cursor(1);
select gms_sql.close_cursor(2);
select gms_sql.close_cursor(3);
select gms_sql.is_open(3);
select gms_sql.close_cursor(10000);
select gms_sql.close_cursor(-1);
