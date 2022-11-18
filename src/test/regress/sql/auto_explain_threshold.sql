create schema auto_explain_threshold;
set search_path = auto_explain_threshold;
set enable_auto_explain = off;
set auto_explain_level = notice;

-- simple query
-- takes 242ms approx.
set enable_auto_explain = on;
create table t (a int);
insert into t values (generate_series(1,100));
analyze t;

set auto_explain_log_min_duration = '10s';
select count(1) from t t1, t t2, t t3; -- no explain
set auto_explain_log_min_duration = '10ms';
select count(1) from t t1, t t2, t t3; -- show plan
set query_dop = 1004;
select count(1) from t t1, t t2, t t3; -- show smp plan
set query_dop = 1;

-- test threshold
set auto_explain_log_min_duration = '100ms';
select pg_sleep(0.01);
select pg_sleep(0.2);

-- nested execution with plpgsql function
set enable_auto_explain = off;
create table test_tab1(a int, b varchar);
insert into test_tab1 values(1,'test1');
insert into test_tab1 values(2,'test2');
insert into test_tab1 values(3,'test2');

create table test_tab2(a int, b varchar);
insert into test_tab2 values(1,'test1');
insert into test_tab2 values(2,'test2');
insert into test_tab2 values(3,'test2');

create table test_tab3(a int, b varchar);
insert into test_tab3 values(1,'test1');
insert into test_tab3 values(2,'test2');
insert into test_tab3 values(3,'test2');

analyze test_tab1;
analyze test_tab2;
analyze test_tab3;

CREATE OR REPLACE FUNCTION func_delete_trigger()
RETURNS TRIGGER AS $$
BEGIN
   DELETE FROM test_tab2 where test_tab2.a = OLD.a;
    RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER delete_trigger 
    AFTER DELETE ON test_tab1
    FOR EACH ROW
    EXECUTE PROCEDURE func_delete_trigger();
    
CREATE OR REPLACE FUNCTION func_update_trigger()
  RETURNS trigger AS $$
   BEGIN
      UPDATE test_tab3 SET test_tab3.b = NEW.b  where test_tab3.a = NEW.a;
      RETURN NEW;
   END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER update_trigger
    AFTER UPDATE ON test_tab1
    FOR EACH ROW
    EXECUTE PROCEDURE func_update_trigger();

create or replace function process_test() returns int as $$
declare status  int;
begin
select complicate_process() into status;
return status;
END
$$
LANGUAGE plpgsql;

create or replace function nested_func(cnt out int) as $$
begin
    select count(1) into cnt from test_tab1 t1, test_tab3 t3 where t1.a = t3.a;
end
$$
LANGUAGE plpgsql;

create or replace function open_cursor(myCursor OUT REFCURSOR) as $$
begin
open myCursor for select * from test_tab1;
END
$$
LANGUAGE plpgsql;


set enable_auto_explain = on;
set auto_explain_log_min_duration = '200ms';

-- log starts shortly after delete
begin;

create or replace function complicate_process(ret out int)  as $$
declare v_count1 int;
declare tt  REFCURSOR;
declare cur_a int;
declare cur_b varchar;
begin
    select count(1) into v_count1 from test_tab1;

    execute 'select pg_sleep(0.3);';

    delete test_tab1 where a = (select cnt from nested_func() limit 1);
    ret := 0;
END
$$
LANGUAGE plpgsql;

select process_test();
rollback;

-- log starts only before update
begin;

create or replace function complicate_process(ret out int)  as $$
declare v_count1 int;
declare tt  REFCURSOR;
declare cur_a int;
declare cur_b varchar;
begin
    select count(1) into v_count1 from test_tab1;


    delete test_tab1 where a = (select cnt from nested_func() limit 1);

    select open_cursor() into tt;
    fetch next from tt into cur_a,cur_b;
    update test_tab1 set b = 'new1' where a = cur_a;
    execute 'select pg_sleep(0.3);';
    fetch next from tt into cur_a,cur_b;
    update test_tab1 set b = 'new2' where a = cur_a;
    ret := 0;
END
$$
LANGUAGE plpgsql;

select process_test();
rollback;

drop schema auto_explain_threshold cascade;