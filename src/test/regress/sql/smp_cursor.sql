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

drop schema smp_cursor cascade;