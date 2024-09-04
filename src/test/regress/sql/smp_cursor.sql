create schema smp_cursor;
set search_path=smp_cursor;

create table t1(a int, b int, c int, d bigint);
insert into t1 values(generate_series(1, 100), generate_series(1, 10), generate_series(1, 2), generate_series(1, 50));
analyze t1;

set query_dop=1002;

explain (costs off) select * from t1;

set enable_auto_explain = on;
set auto_explain_level = notice;
-- test cursor smp
begin;
declare xc no scroll cursor for select * from t1;
fetch xc;
end;

-- test plan hint
begin;
declare xc no scroll cursor for select /*+ set(query_dop 1) */ * from t1;
fetch xc;
end;

set query_dop = 1;
begin;
declare xc no scroll cursor for select /*+ set(query_dop 1002) */ * from t1;
fetch xc;
end;

-- scroll cursor can not smp
set query_dop = 1002;
begin;
declare xc cursor for select /*+ set(query_dop 1002) */ * from t1;
fetch xc;
end;

-- cursor declared with plpgsql can not smp
declare
    cursor xc no scroll is select * from t1;
    tmp t1%ROWTYPE;
begin
    open xc;
    fetch xc into tmp;
    close xc;
end;
/

-- test resource conflict checking
begin;
declare xc no scroll cursor for select * from t1;
drop table t1;
end;

-- test cursor with hold
begin;
declare xc no scroll cursor with hold for select * from t1;
fetch xc;
end;
fetch absolute 10 xc;
close xc;

-- test cursor backward error
begin;
declare xc no scroll cursor for select * from t1;
fetch absolute 10 xc;
fetch absolute 9 xc;
end;

-- test cursor other operate
begin;
declare xc no scroll cursor for select * from t1;
fetch first xc;
fetch forward xc;
fetch absolute 5 xc;
fetch relative 5 xc;
fetch all xc;
move xc;
end;

-- cursor expr in targetlist do not smp
set enable_auto_explain = off;
explain (costs off) select a, cursor(select * from t1) from t1 limit 10;
select a, cursor(select * from t1) from t1 limit 10;

-- smp hint in cursor expr among plpgsql does not work
set enable_auto_explain = on;
set auto_explain_level = notice;
-- test plan hint in cursor expression
DECLARE CURSOR c1 IS SELECT a, CURSOR(SELECT /*+ set(query_dop 1002) */ * FROM t1) abc FROM t1;
  id int;	
  type emp_cur_type is ref cursor;
  c2 emp_cur_type;
  tmp t1%rowtype;
BEGIN
  OPEN c1;
  fetch c1 into id,c2;
  fetch c2 into tmp;
  close c2;
  CLOSE c1;
END;
/
set enable_auto_explain = off;

drop schema smp_cursor cascade;