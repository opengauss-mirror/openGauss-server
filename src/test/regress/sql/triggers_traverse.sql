-- As of now, the following plpgsql language constructs are unshippable
-- select count(*) into var from t2;ts
-- expressions such as var+var, var*var
-- reference to variables such as var in insert
-- reference to constant such as insert into t2 (a2, b2) values(100, 100)

set explain_perf_mode=pretty;
create table traverse_triggers_tbl1(a1 int, b1 int, c1 int) distribute by hash(a1,b1);
create table traverse_triggers_tbl2(a2 int, b2 int) distribute by hash(a2,b2);
create table traverse_triggers_tbl3(c1 varchar(255)) distribute by hash(c1);

------------------------------------
--Create trigger functions
------------------------------------

--assign----------------------------
drop function if exists trigger_function_assign() cascade;
create or replace function trigger_function_assign() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
	--select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	return NEW;
end
$$ language plpgsql;

--if--------------------------------
drop function if exists trigger_function_if() cascade;
create or replace function trigger_function_if() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
	--select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	if (var > 1) then
		var := var + 1;
	end if;
	--insert into traverse_triggers_tbl2 (a2, b2) values(var+var, var*var);
	raise notice 'traverse_triggers_tbl2 count: %', var;
	return NEW;
end
$$ language plpgsql;

--case------------------------------
drop function if exists trigger_function_case() cascade;
create or replace function trigger_function_case() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
	--select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	case var 
		when 1 then
			var := var + 1;
		else
			var := var + 2;			
	end case;
	--insert into traverse_triggers_tbl2 (a2, b2) values(var+var, var*var);
	raise notice 'traverse_triggers_tbl2 count: %', var;
	return NEW;
end
$$ language plpgsql;

--loop------------------------------
drop function if exists trigger_function_loop() cascade;
create or replace function trigger_function_loop() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
--	select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	loop
		var := var + 1;
		if var > 10 then
			exit; -- exit loop
		end if;
	end loop;
--	insert into traverse_triggers_tbl2 (a2, b2) values(var+var, var*var);
	raise notice 'traverse_triggers_tbl2 count: %', var;
	return NEW;
end
$$ language plpgsql;

--while-----------------------------
drop function if exists trigger_function_while() cascade;
create or replace function trigger_function_while() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
--	select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	while var < 10 loop
		var := var + 1;
	end loop;
--	insert into traverse_triggers_tbl2 (a2, b2) values(var+var, var*var);
	raise notice 'traverse_triggers_tbl2 count: %', var;
	return NEW;
end
$$ language plpgsql;

--fori------------------------------
drop function if exists trigger_function_fori() cascade;
create or replace function trigger_function_fori() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
--	select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	for i in reverse 10..1 by 2 loop
	-- i will take on the values 10,8,6,4,2 within the loop
		var := var + i;
	end loop;
--	insert into traverse_triggers_tbl2 (a2, b2) values(var+var, var*var);
	raise notice 'traverse_triggers_tbl2 count: %', var;
	return NEW;
end
$$ language plpgsql;

--fors------------------------------
drop function if exists trigger_function_fors() cascade;
create or replace function trigger_function_fors() returns trigger as
$$
declare 
	var integer := 1;
	r record;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
--	select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;

    for r in select tgrelid::regclass, tgenabled, tgname from pg_trigger loop
        insert into traverse_triggers_tbl3 values (r.tgrelid::regclass || ' + ' || r.tgenabled || ' + ' || r.tgname );
	end loop;

	return NEW;
end
$$ language plpgsql;

--forc------------------------------
drop function if exists trigger_function_forc() cascade;
create or replace function trigger_function_forc() returns trigger as
$$
declare 
	var integer := 1;
	cursor c no scroll for select tgname from pg_trigger;
	triggername varchar;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
--	select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	
	open c;
	fetch first from c into triggername;
	while found loop
		insert into traverse_triggers_tbl3 values (triggername);
		fetch next from c into triggername;
	end loop;
	close c;

	return NEW;

end
$$ language plpgsql;

--foreach_a-------------------------
drop function if exists trigger_function_foreach_a() cascade;
create or replace function trigger_function_foreach_a() returns trigger as
$$
declare 
	var integer := 1;
	myarray  integer[];
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
--	select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;

	myarray := array[1,2,3,4,5];
	foreach var in array myarray
	loop
		raise notice '%', var;
	end loop;

	return NEW;
end
$$ language plpgsql;

--exit------------------------------
drop function if exists trigger_function_exit() cascade;
create or replace function trigger_function_exit() returns trigger as
$$
declare 
	var integer := 1;
	myarray  integer[];
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
--	select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;

	myarray := array[1,2,3,4,5];
	foreach var in array myarray
	loop
		raise notice '%', var;
		exit when var > 2;
	end loop;

	return NEW;
end
$$ language plpgsql;

--return_next-----------------------
drop function if exists get_all_traverse_triggers_tbl1_doesnotwork() cascade;
create or replace function get_all_traverse_triggers_tbl1_doesnotwork() returns setof traverse_triggers_tbl1 as
$$
declare
	r traverse_triggers_tbl1%rowtype;
begin
	for r in
		select * from traverse_triggers_tbl1
	loop
		-- can do some processing here
		return next r; -- return current row of select
	end loop;
	return;
end
$$
language plpgsql;

drop function if exists get_all_traverse_triggers_tbl1_doesnotwork2() cascade;
create or replace function get_all_traverse_triggers_tbl1_doesnotwork2() returns table(a1 integer, b1 integer) as
$$
begin
	for a1, b1 in
		select traverse_triggers_tbl1.a1, traverse_triggers_tbl1.b1 from traverse_triggers_tbl1
	loop
		-- can do some processing here
		return next; -- return current row of select
	end loop;
end
$$
language plpgsql;

drop function if exists get_all_traverse_triggers_tbl1() cascade;
create or replace function get_all_traverse_triggers_tbl1() returns table(a1 integer, b1 integer) as
$$
declare
	r record;
begin
	for r in (select traverse_triggers_tbl1.a1, traverse_triggers_tbl1.b1 from traverse_triggers_tbl1)
	loop
		-- can do some processing here
		a1 := a1+10;
		b1 := b1+10;
		return next; -- return current row of select
	end loop;
end
$$
language plpgsql;

drop function if exists trigger_function_return_next() cascade;
create or replace function trigger_function_return_next() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
	--select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	--select * from get_all_traverse_triggers_tbl1();
	return NEW;
end
$$ language plpgsql;


--return_query----------------------
drop function if exists get_one_row_traverse_triggers_tbl1() cascade;
create or replace function get_one_row_traverse_triggers_tbl1() returns table(a1 integer, b1 integer, c1 integer) as
$$
begin
    return query select a1, b1, c1 from traverse_triggers_tbl1 limit 1;
    -- since execution is not finished, we can check whether rows were returned
    -- and --raise exception if not.
    if not found then
        raise exception 'no row at found';
    end if;
 end
$$
language plpgsql;

drop function if exists trigger_function_return_query() cascade;
create or replace function trigger_function_return_query() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
	--select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	--select * from get_one_row_traverse_triggers_tbl1();
	return NEW;
end
$$ language plpgsql;

--open------------------------------
--fetch-----------------------------
--cursor_direction------------------
--close-----------------------------
drop function if exists trigger_function_open_fetch_close_cursor_direction() cascade;
create or replace function trigger_function_open_fetch_close_cursor_direction() returns trigger as
$$
declare 
	var integer := 1;
	cursor c no scroll for select tgname from pg_trigger;
	triggername varchar;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
	--select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	
	open c;
	fetch first from c into triggername;
	while found loop
		insert into traverse_triggers_tbl3 values (triggername);
		fetch next from c into triggername;
	end loop;
	close c;

	return NEW;

end
$$ language plpgsql;

--perform---------------------------
drop function if exists trigger_function_perform() cascade;
create or replace function trigger_function_perform() returns trigger as
$$
declare 
	var integer := 1;
begin
	insert into traverse_triggers_tbl2 (a2, b2) values(NEW.a1, NEW.b1);
	--select count(*) into var from traverse_triggers_tbl2;
	raise notice 'traverse_triggers_tbl2 count: %', var;
	perform 2+2;
	return NEW;
end
$$ language plpgsql;

--dynexecute---------------------------
drop function if exists trigger_function_dynexecute() cascade;
create or replace function trigger_function_dynexecute() returns trigger as
$$
begin
	execute 'create table dyn_execute_create_table(id int);drop table dyn_execute_create_table;';
	return null;
end
$$ language plpgsql;
------------------------------------
--Before Insert Row Triggers
------------------------------------

drop trigger if exists trigger_function_assign ON traverse_triggers_tbl1;
create trigger trigger_function_assign before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_assign();

drop trigger if exists trigger_function_if ON traverse_triggers_tbl1;
create trigger trigger_function_if before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_if();

drop trigger if exists trigger_function_case ON traverse_triggers_tbl1;
create trigger trigger_function_case before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_case();

drop trigger if exists trigger_function_loop ON traverse_triggers_tbl1;
create trigger trigger_function_loop before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_loop();

drop trigger if exists trigger_function_while ON traverse_triggers_tbl1;
create trigger trigger_function_while before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_while();

drop trigger if exists trigger_function_fori ON traverse_triggers_tbl1;
create trigger trigger_function_fori before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_fori();

drop trigger if exists trigger_function_fors ON traverse_triggers_tbl1;
create trigger trigger_function_fors before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_fors();

drop trigger if exists trigger_function_forc ON traverse_triggers_tbl1;
create trigger trigger_function_forc before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_forc();

drop trigger if exists trigger_function_foreach_a ON traverse_triggers_tbl1;
create trigger trigger_function_foreach_a before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_foreach_a();

drop trigger if exists trigger_function_exit ON traverse_triggers_tbl1;
create trigger trigger_function_exit before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_exit();

drop trigger if exists trigger_function_return_next ON traverse_triggers_tbl1;
create trigger trigger_function_return_next before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_return_next();

drop trigger if exists trigger_function_return_query ON traverse_triggers_tbl1;
create trigger trigger_function_return_query before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_return_query();

drop trigger if exists trigger_function_open_fetch_close_cursor_direction ON traverse_triggers_tbl1;
create trigger trigger_function_open_fetch_close_cursor_direction before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_open_fetch_close_cursor_direction();

drop trigger if exists trigger_function_perform ON traverse_triggers_tbl1;
create trigger trigger_function_perform before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_perform();

drop trigger if exists trigger_function_dynexecute ON traverse_triggers_tbl1;
create trigger trigger_function_dynexecute before insert on traverse_triggers_tbl1 for each row execute procedure trigger_function_dynexecute();
------------------------------------
--Disable all Triggers
------------------------------------
alter table traverse_triggers_tbl1 disable trigger all;

------------------------------------------
--Trigger traverse function testcase: assign
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_assign;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_assign;
------------------------------------------
--Trigger traverse function testcase: if
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_if;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_if;
------------------------------------------
--Trigger traverse function testcase: case
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_case;
set enable_trigger_shipping=off;
explain verbose insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_case;
------------------------------------------
--Trigger traverse function testcase: loop
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_loop;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_loop;
------------------------------------------
--Trigger traverse function testcase: while
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_while;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_while;
------------------------------------------
--Trigger traverse function testcase: fori
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_fori;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_fori;
------------------------------------------
--Trigger traverse function testcase: fors
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_fors;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_fors;
------------------------------------------
--Trigger traverse function testcase: forc
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_forc;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_forc;
------------------------------------------
--Trigger traverse function testcase: foreach_a
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_foreach_a;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_foreach_a;
------------------------------------------
--Trigger traverse function testcase: exit
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_exit;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_exit;
------------------------------------------
--Trigger traverse function testcase: return_next
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_return_next;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_return_next;
------------------------------------------
--Trigger traverse function testcase: return_query
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_return_query;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_return_query;
------------------------------------------
--Trigger traverse function testcase: open_fetch_close_cursor_direction
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_open_fetch_close_cursor_direction;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_open_fetch_close_cursor_direction;
------------------------------------------
--Trigger traverse function testcase: perform
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_perform;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_perform;
------------------------------------------
--Trigger traverse function testcase: dynexecute
------------------------------------------
alter table traverse_triggers_tbl1 enable trigger trigger_function_dynexecute;
set enable_trigger_shipping=off;
explain performance insert into traverse_triggers_tbl1 values (1,1);
set enable_trigger_shipping=on;
explain performance insert into traverse_triggers_tbl1 values (1,1);
alter table traverse_triggers_tbl1 disable trigger trigger_function_dynexecute;

drop table IF EXISTS traverse_triggers_tbl1 cascade;
drop table IF EXISTS traverse_triggers_tbl2 cascade;
drop table IF EXISTS traverse_triggers_tbl3 cascade;
select time,type,detail_info from pg_query_audit('2000-01-01 00:00:00','9999-12-31') where detail_info like '%trigger_function_dynexecute%';
