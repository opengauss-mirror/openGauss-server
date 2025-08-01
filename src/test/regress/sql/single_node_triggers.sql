--
-- TRIGGERS
--
create table pkeys (pkey1 int4 not null, pkey2 text not null);
alter table pkeys add unique(pkey1, pkey2);
create table fkeys (fkey1 int4, fkey2 text, fkey3 int);
create table fkeys2 (fkey21 int4, fkey22 text, pkey23 int not null);

create index fkeys_i on fkeys (fkey1, fkey2);
create index fkeys2_i on fkeys2 (fkey21, fkey22);
create index fkeys2p_i on fkeys2 (pkey23);

insert into pkeys values (10, '1');
insert into pkeys values (20, '2');
insert into pkeys values (30, '3');
insert into pkeys values (40, '4');
insert into pkeys values (50, '5');
insert into pkeys values (60, '6');
create unique index pkeys_i on pkeys (pkey1, pkey2);

--
-- For fkeys:
-- 	(fkey1, fkey2)	--> pkeys (pkey1, pkey2)
-- 	(fkey3)		--> fkeys2 (pkey23)
--
create trigger check_fkeys_pkey_exist
	before insert or update on fkeys
	for each row
	execute procedure
	check_primary_key ('fkey1', 'fkey2', 'pkeys', 'pkey1', 'pkey2');

create trigger check_fkeys_pkey2_exist
	before insert or update on fkeys
	for each row
	execute procedure check_primary_key ('fkey3', 'fkeys2', 'pkey23');

--
-- For fkeys2:
-- 	(fkey21, fkey22)	--> pkeys (pkey1, pkey2)
--
create trigger check_fkeys2_pkey_exist
	before insert or update on fkeys2
	for each row
	execute procedure
	check_primary_key ('fkey21', 'fkey22', 'pkeys', 'pkey1', 'pkey2');

-- Test comments
COMMENT ON TRIGGER check_fkeys2_pkey_bad ON fkeys2 IS 'wrong';
COMMENT ON TRIGGER check_fkeys2_pkey_exist ON fkeys2 IS 'right';
COMMENT ON TRIGGER check_fkeys2_pkey_exist ON fkeys2 IS NULL;

--
-- For pkeys:
-- 	ON DELETE/UPDATE (pkey1, pkey2) CASCADE:
-- 		fkeys (fkey1, fkey2) and fkeys2 (fkey21, fkey22)
--
create trigger check_pkeys_fkey_cascade
	before delete or update on pkeys
	for each row
	execute procedure
	check_foreign_key (2, 'cascade', 'pkey1', 'pkey2',
	'fkeys', 'fkey1', 'fkey2', 'fkeys2', 'fkey21', 'fkey22');

--
-- For fkeys2:
-- 	ON DELETE/UPDATE (pkey23) RESTRICT:
-- 		fkeys (fkey3)
--
create trigger check_fkeys2_fkey_restrict
	before delete or update on fkeys2
	for each row
	execute procedure check_foreign_key (1, 'restrict', 'pkey23', 'fkeys', 'fkey3');

insert into fkeys2 values (10, '1', 1);
insert into fkeys2 values (30, '3', 2);
insert into fkeys2 values (40, '4', 5);
insert into fkeys2 values (50, '5', 3);
-- no key in pkeys
insert into fkeys2 values (70, '5', 3);

insert into fkeys values (10, '1', 2);
insert into fkeys values (30, '3', 3);
insert into fkeys values (40, '4', 2);
insert into fkeys values (50, '5', 2);
-- no key in pkeys
insert into fkeys values (70, '5', 1);
-- no key in fkeys2
insert into fkeys values (60, '6', 4);

delete from pkeys where pkey1 = 30 and pkey2 = '3';
delete from pkeys where pkey1 = 40 and pkey2 = '4';
update pkeys set pkey1 = 7, pkey2 = '70' where pkey1 = 50 and pkey2 = '5';
update pkeys set pkey1 = 7, pkey2 = '70' where pkey1 = 10 and pkey2 = '1';

DROP TABLE pkeys;
DROP TABLE fkeys;
DROP TABLE fkeys2;

-- -- I've disabled the funny_dup17 test because the new semantics
-- -- of AFTER ROW triggers, which get now fired at the end of a
-- -- query always, cause funny_dup17 to enter an endless loop.
-- --
-- --      Jan
--
-- create table dup17 (x int4);
--
-- create trigger dup17_before
-- 	before insert on dup17
-- 	for each row
-- 	execute procedure
-- 	funny_dup17 ()
-- ;
--
-- insert into dup17 values (17);
-- select count(*) from dup17;
-- insert into dup17 values (17);
-- select count(*) from dup17;
--
-- drop trigger dup17_before on dup17;
--
-- create trigger dup17_after
-- 	after insert on dup17
-- 	for each row
-- 	execute procedure
-- 	funny_dup17 ()
-- ;
-- insert into dup17 values (13);
-- select count(*) from dup17 where x = 13;
-- insert into dup17 values (13);
-- select count(*) from dup17 where x = 13;
--
-- DROP TABLE dup17;

create sequence ttdummy_seq increment 10 start 0 minvalue 0;

create table tttest (
	price_id	int4,
	price_val	int4,
	price_on	int4,
	price_off	int4 default 999999
);
ALTER TABLE tttest ADD PRIMARY KEY(price_id, price_val, price_on, price_off);

create trigger ttdummy
	before delete or update on tttest
	for each row
	execute procedure
	ttdummy (price_on, price_off);

create trigger ttserial
	before insert or update on tttest
	for each row
	execute procedure
	autoinc (price_on, ttdummy_seq);

insert into tttest values (1, 1, null);
insert into tttest values (2, 2, null);
insert into tttest values (3, 3, 0);

select * from tttest order by 1,2,3,4;
delete from tttest where price_id = 2;
select * from tttest order by 1,2,3,4;
-- what do we see ?

-- get current prices
select * from tttest where price_off = 999999 order by 1,2,3,4;

-- change price for price_id == 3
update tttest set price_val = 30 where price_id = 3;
select * from tttest order by 1,2,3,4;

-- now we want to change pric_id in ALL tuples
-- this gets us not what we need
update tttest set price_id = 5 where price_id = 3;
select * from tttest order by 1,2,3,4;

-- restore data as before last update:
select set_ttdummy(0);
delete from tttest where price_id = 5;
update tttest set price_off = 999999 where price_val = 30;
select * from tttest order by 1,2,3,4;

-- and try change price_id now!
update tttest set price_id = 5 where price_id = 3;
select * from tttest order by 1,2,3,4;
-- isn't it what we need ?

select set_ttdummy(1);

-- we want to correct some "date"
update tttest set price_on = -1 where price_id = 1;
-- but this doesn't work

-- try in this way
select set_ttdummy(0);
update tttest set price_on = -1 where price_id = 1;
select * from tttest order by 1,2,3,4;
-- isn't it what we need ?

-- get price for price_id == 5 as it was @ "date" 35
select * from tttest where price_on <= 35 and price_off > 35 and price_id = 5 order by 1,2,3,4;

drop table tttest;
drop sequence ttdummy_seq;

-- test gram support execute PROCEDURE | FUNCTION
drop table if exists audit_log;
create table audit_log (
  id serial primary key,
  username varchar(50),
  action varchar(50),
  action_time timestamp default current_timestamp
);
drop table if exists users;
create table users (username varchar(255), email varchar(255));
drop table if exists employees;
create table employees (username varchar(255), email varchar(255));

create or replace function log_user_action()
returns trigger as $$
begin
  insert into audit_log (username, action) values (new.username, 'insert');
  return new;
end;
$$ language plpgsql;

-- new support execute function
create trigger after_user_insert
after insert on users
for each row
execute function log_user_action();

insert into users values ('User', 'User@Person.com');
select id, username, action from audit_log order by id;

-- support execute procedure
create trigger after_employees_insert
after insert on employees
for each row
execute procedure log_user_action();

insert into employees values ('Employee', 'Employee@Person.com');
select id, username, action from audit_log order by id;

drop table audit_log;
drop table users;
drop table employees;

--
-- tests for per-statement triggers
--

CREATE TABLE log_table (tstamp timestamp default timeofday()::timestamp);

CREATE TABLE main_table (a int, b int);
ALTER TABLE MAIN_TABLE ADD PRIMARY KEY(A, B);

COPY main_table (a,b) FROM stdin;
5	10
20	20
30	10
50	35
80	15
\.

CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
	RAISE NOTICE ''trigger_func(%) called: action = %, when = %, level = %'', TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;';

CREATE TRIGGER before_ins_stmt_trig BEFORE INSERT ON main_table
FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func('before_ins_stmt');

CREATE TRIGGER after_ins_stmt_trig AFTER INSERT ON main_table
FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func('after_ins_stmt');

--
-- if neither 'FOR EACH ROW' nor 'FOR EACH STATEMENT' was specified,
-- CREATE TRIGGER should default to 'FOR EACH STATEMENT'
--
CREATE TRIGGER after_upd_stmt_trig AFTER UPDATE ON main_table
EXECUTE PROCEDURE trigger_func('after_upd_stmt');

CREATE TRIGGER after_upd_row_trig AFTER UPDATE ON main_table
FOR EACH ROW EXECUTE PROCEDURE trigger_func('after_upd_row');

INSERT INTO main_table DEFAULT VALUES;

UPDATE main_table SET a = a + 1 WHERE b < 30;
-- UPDATE that effects zero rows should still call per-statement trigger
UPDATE main_table SET a = a + 2 WHERE b > 100;

-- COPY should fire per-row and per-statement INSERT triggers
COPY main_table (a, b) FROM stdin;
30	40
50	60
\.

SELECT * FROM main_table ORDER BY a, b;

--
-- test triggers with WHEN clause
--

CREATE TRIGGER modified_a BEFORE UPDATE OF a ON main_table
FOR EACH ROW WHEN (OLD.a <> NEW.a) EXECUTE PROCEDURE trigger_func('modified_a');
CREATE TRIGGER modified_any BEFORE UPDATE OF a ON main_table
FOR EACH ROW WHEN (OLD.* IS DISTINCT FROM NEW.*) EXECUTE PROCEDURE trigger_func('modified_any');
CREATE TRIGGER insert_a AFTER INSERT ON main_table
FOR EACH ROW WHEN (NEW.a = 123) EXECUTE PROCEDURE trigger_func('insert_a');
CREATE TRIGGER delete_a AFTER DELETE ON main_table
FOR EACH ROW WHEN (OLD.a = 123) EXECUTE PROCEDURE trigger_func('delete_a');
CREATE TRIGGER insert_when BEFORE INSERT ON main_table
FOR EACH STATEMENT WHEN (true) EXECUTE PROCEDURE trigger_func('insert_when');
CREATE TRIGGER delete_when AFTER DELETE ON main_table
FOR EACH STATEMENT WHEN (true) EXECUTE PROCEDURE trigger_func('delete_when');
INSERT INTO main_table (a) VALUES (123), (456);
COPY main_table FROM stdin;
123	999
456	999
\.
;

DELETE FROM main_table WHERE a IN (123, 456);
UPDATE main_table SET a = 50, b = 60;
SELECT * FROM main_table ORDER BY a, b;
SELECT pg_get_triggerdef(oid, true) FROM pg_trigger WHERE tgrelid = 'main_table'::regclass AND tgname = 'modified_a';
SELECT pg_get_triggerdef(oid, false) FROM pg_trigger WHERE tgrelid = 'main_table'::regclass AND tgname = 'modified_a';
SELECT pg_get_triggerdef(oid, true) FROM pg_trigger WHERE tgrelid = 'main_table'::regclass AND tgname = 'modified_any';
DROP TRIGGER modified_a ON main_table;
DROP TRIGGER modified_any ON main_table;
DROP TRIGGER insert_a ON main_table;
DROP TRIGGER delete_a ON main_table;
DROP TRIGGER insert_when ON main_table;
DROP TRIGGER delete_when ON main_table;

-- Test column-level triggers
DROP TRIGGER after_upd_row_trig ON main_table;

CREATE TRIGGER before_upd_a_row_trig BEFORE UPDATE OF a ON main_table
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_upd_a_row');
CREATE TRIGGER after_upd_b_row_trig AFTER UPDATE OF b ON main_table
FOR EACH ROW EXECUTE PROCEDURE trigger_func('after_upd_b_row');
CREATE TRIGGER after_upd_a_b_row_trig AFTER UPDATE OF a, b ON main_table
FOR EACH ROW EXECUTE PROCEDURE trigger_func('after_upd_a_b_row');

CREATE TRIGGER before_upd_a_stmt_trig BEFORE UPDATE OF a ON main_table
FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func('before_upd_a_stmt');
CREATE TRIGGER after_upd_b_stmt_trig AFTER UPDATE OF b ON main_table
FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func('after_upd_b_stmt');

SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid = 'main_table'::regclass AND tgname = 'after_upd_a_b_row_trig';

UPDATE main_table SET a = 50;
UPDATE main_table SET b = 10;

--
-- Test case for bug with BEFORE trigger followed by AFTER trigger with WHEN
--

CREATE TABLE some_t (some_col boolean NOT NULL);
ALTER TABLE some_t ADD PRIMARY KEY(some_col);
CREATE FUNCTION dummy_update_func() RETURNS trigger AS $$
BEGIN
  RAISE NOTICE 'dummy_update_func(%) called: action = %, old = %, new = %',
    TG_ARGV[0], TG_OP, OLD, NEW;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER some_trig_before BEFORE UPDATE ON some_t FOR EACH ROW
  EXECUTE PROCEDURE dummy_update_func('before');
CREATE TRIGGER some_trig_aftera AFTER UPDATE ON some_t FOR EACH ROW
  WHEN (NOT OLD.some_col AND NEW.some_col)
  EXECUTE PROCEDURE dummy_update_func('aftera');
CREATE TRIGGER some_trig_afterb AFTER UPDATE ON some_t FOR EACH ROW
  WHEN (NOT NEW.some_col)
  EXECUTE PROCEDURE dummy_update_func('afterb');
INSERT INTO some_t VALUES (TRUE);
UPDATE some_t SET some_col = TRUE;
UPDATE some_t SET some_col = FALSE;
UPDATE some_t SET some_col = TRUE;
DROP TABLE some_t;

-- bogus cases
CREATE TRIGGER error_upd_and_col BEFORE UPDATE OR UPDATE OF a ON main_table
FOR EACH ROW EXECUTE PROCEDURE trigger_func('error_upd_and_col');
CREATE TRIGGER error_upd_a_a BEFORE UPDATE OF a, a ON main_table
FOR EACH ROW EXECUTE PROCEDURE trigger_func('error_upd_a_a');
CREATE TRIGGER error_ins_a BEFORE INSERT OF a ON main_table
FOR EACH ROW EXECUTE PROCEDURE trigger_func('error_ins_a');
CREATE TRIGGER error_ins_when BEFORE INSERT OR UPDATE ON main_table
FOR EACH ROW WHEN (OLD.a <> NEW.a)
EXECUTE PROCEDURE trigger_func('error_ins_old');
CREATE TRIGGER error_del_when BEFORE DELETE OR UPDATE ON main_table
FOR EACH ROW WHEN (OLD.a <> NEW.a)
EXECUTE PROCEDURE trigger_func('error_del_new');
CREATE TRIGGER error_del_when BEFORE INSERT OR UPDATE ON main_table
FOR EACH ROW WHEN (NEW.tableoid <> 0)
EXECUTE PROCEDURE trigger_func('error_when_sys_column');
CREATE TRIGGER error_stmt_when BEFORE UPDATE OF a ON main_table
FOR EACH STATEMENT WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE trigger_func('error_stmt_when');

-- check dependency restrictions
ALTER TABLE main_table DROP COLUMN b;
-- this should succeed, but we'll roll it back to keep the triggers around
start transaction;
DROP TRIGGER after_upd_a_b_row_trig ON main_table;
DROP TRIGGER after_upd_b_row_trig ON main_table;
DROP TRIGGER after_upd_b_stmt_trig ON main_table;
ALTER TABLE main_table DROP COLUMN b;
rollback;

-- Test enable/disable triggers

create table trigtest (i serial primary key);
-- test that disabling RI triggers works
create table trigtest2 (i int references trigtest(i) on delete cascade);

create function trigtest() returns trigger as $$
begin
	raise notice '% % % %', TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL;
	return new;
end;$$ language plpgsql;

create trigger trigtest_b_row_tg before insert or update or delete on trigtest
for each row execute procedure trigtest();
create trigger trigtest_a_row_tg after insert or update or delete on trigtest
for each row execute procedure trigtest();
create trigger trigtest_b_stmt_tg before insert or update or delete on trigtest
for each statement execute procedure trigtest();
create trigger trigtest_a_stmt_tg after insert or update or delete on trigtest
for each statement execute procedure trigtest();

insert into trigtest default values;
alter table trigtest disable trigger trigtest_b_row_tg;
insert into trigtest default values;
alter table trigtest disable trigger user;
insert into trigtest default values;
alter table trigtest enable trigger trigtest_a_stmt_tg;
insert into trigtest default values;
insert into trigtest2 values(1);
insert into trigtest2 values(2);
delete from trigtest where i=2;
select * from trigtest2 order by 1;
alter table trigtest disable trigger all;
delete from trigtest where i=1;
select * from trigtest2 order by 1;
-- ensure we still insert, even when all triggers are disabled
insert into trigtest default values;
select *  from trigtest order by 1;
drop table trigtest2;
drop table trigtest;


-- dump trigger data
CREATE TABLE trigger_test (
        i int,
        v varchar
);

CREATE OR REPLACE FUNCTION trigger_data()  RETURNS trigger
LANGUAGE plpgsql AS $$

declare

	argstr text;
	relid text;

begin

	relid := TG_relid::regclass;

	-- plpgsql can't discover its trigger data in a hash like perl and python
	-- can, or by a sort of reflection like tcl can,
	-- so we have to hard code the names.
	raise NOTICE 'TG_NAME: %', TG_name;
	raise NOTICE 'TG_WHEN: %', TG_when;
	raise NOTICE 'TG_LEVEL: %', TG_level;
	raise NOTICE 'TG_OP: %', TG_op;
	raise NOTICE 'TG_RELID::regclass: %', relid;
	raise NOTICE 'TG_RELNAME: %', TG_relname;
	raise NOTICE 'TG_TABLE_NAME: %', TG_table_name;
	raise NOTICE 'TG_TABLE_SCHEMA: %', TG_table_schema;
	raise NOTICE 'TG_NARGS: %', TG_nargs;

	argstr := '[';
	for i in 0 .. TG_nargs - 1 loop
		if i > 0 then
			argstr := argstr || ', ';
		end if;
		argstr := argstr || TG_argv[i];
	end loop;
	argstr := argstr || ']';
	raise NOTICE 'TG_ARGV: %', argstr;

	if TG_OP != 'INSERT' then
		raise NOTICE 'OLD: %', OLD;
	end if;

	if TG_OP != 'DELETE' then
		raise NOTICE 'NEW: %', NEW;
	end if;

	if TG_OP = 'DELETE' then
		return OLD;
	else
		return NEW;
	end if;

end;
$$;

CREATE TRIGGER show_trigger_data_trig
BEFORE INSERT OR UPDATE OR DELETE ON trigger_test
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

insert into trigger_test values(1,'insert');
update trigger_test set v = 'update' where i = 1;
delete from trigger_test;

DROP TRIGGER show_trigger_data_trig on trigger_test;

DROP FUNCTION trigger_data();

DROP TABLE trigger_test;

--
-- Test use of row comparisons on OLD/NEW
--

CREATE TABLE trigger_test1 (f1 int, f2 text, f3 text);
CREATE TABLE trigger_test2 (f1 int, f2 text, f3 text);
-- this is the obvious (and wrong...) way to compare rows
CREATE FUNCTION mytrigger() RETURNS trigger LANGUAGE plpgsql as $$
begin
	if row(old.*) = row(new.*) then
		raise notice 'row % not changed', new.f1;
	else
		raise notice 'row % changed', new.f1;
	end if;
	return new;
end$$;

CREATE TRIGGER t_trigger_test1
BEFORE UPDATE ON trigger_test1
FOR EACH ROW EXECUTE PROCEDURE mytrigger();
CREATE TRIGGER t_trigger_test2
BEFORE UPDATE ON trigger_test2
FOR EACH ROW EXECUTE PROCEDURE mytrigger();

INSERT INTO trigger_test1 VALUES(1, 'foo', 'bar');
INSERT INTO trigger_test2 VALUES(2, 'baz', 'quux');

UPDATE trigger_test1 SET f3 = 'bar';
UPDATE trigger_test2 SET f3 = 'bar';
UPDATE trigger_test1 SET f3 = NULL;
UPDATE trigger_test2 SET f3 = NULL;
-- this demonstrates that the above isn't really working as desired:
UPDATE trigger_test1 SET f3 = NULL;
UPDATE trigger_test2 SET f3 = NULL;


-- the right way when considering nulls is
CREATE OR REPLACE FUNCTION mytrigger() RETURNS trigger LANGUAGE plpgsql as $$
begin
	if row(old.*) is distinct from row(new.*) then
		raise notice 'row % changed', new.f1;
	else
		raise notice 'row % not changed', new.f1;
	end if;
	return new;
end$$;

UPDATE trigger_test1 SET f3 = 'bar';
UPDATE trigger_test2 SET f3 = 'bar';
UPDATE trigger_test1 SET f3 = NULL;
UPDATE trigger_test2 SET f3 = NULL;
UPDATE trigger_test1 SET f3 = NULL;
UPDATE trigger_test2 SET f3 = NULL;

DROP TABLE trigger_test1;
DROP TABLE trigger_test2;

DROP FUNCTION mytrigger();

-- Test snapshot management in serializable transactions involving triggers
-- per bug report in 6bc73d4c0910042358k3d1adff3qa36f8df75198ecea@mail.gmail.com
CREATE FUNCTION serializable_update_trig() RETURNS trigger LANGUAGE plpgsql AS
$$
declare
	rec record;
begin
	new.description = 'updated in trigger';
	return new;
end;
$$;

CREATE TABLE serializable_update_tab (
	id int,
	filler  text,
	description text
);
ALTER TABLE serializable_update_tab ADD PRIMARY KEY(id);

CREATE TRIGGER serializable_update_trig BEFORE UPDATE ON serializable_update_tab
	FOR EACH ROW EXECUTE PROCEDURE serializable_update_trig();

INSERT INTO serializable_update_tab SELECT a, repeat('xyzxz', 100), 'new'
	FROM generate_series(1, 50) a;

START TRANSACTION;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE serializable_update_tab SET description = 'no no', id = 1 WHERE id = 1;
COMMIT;
SELECT description FROM serializable_update_tab WHERE id = 1;
DROP TABLE serializable_update_tab;

-- minimal update trigger

CREATE TABLE min_updates_test (
	f1	text,
	f2 int,
	f3 int);
ALTER TABLE min_updates_test ADD PRIMARY KEY(F1, F2);

CREATE TABLE min_updates_test_oids (
	f1	text,
	f2 int,
	f3 int) WITH OIDS;
ALTER TABLE min_updates_test_oids ADD PRIMARY KEY(F1, F2);

INSERT INTO min_updates_test VALUES ('a',1,2),('b','2',null);

INSERT INTO min_updates_test_oids VALUES ('a',1,2),('b','2',null);

CREATE TRIGGER z_min_update
BEFORE UPDATE ON min_updates_test
FOR EACH ROW EXECUTE PROCEDURE suppress_redundant_updates_trigger();

CREATE TRIGGER z_min_update
BEFORE UPDATE ON min_updates_test_oids
FOR EACH ROW EXECUTE PROCEDURE suppress_redundant_updates_trigger();

\set QUIET false

UPDATE min_updates_test SET f1 = f1;

UPDATE min_updates_test SET f2 = f2 + 1;

UPDATE min_updates_test SET f3 = 2 WHERE f3 is null;

UPDATE min_updates_test_oids SET f1 = f1;

UPDATE min_updates_test_oids SET f2 = f2 + 1;

UPDATE min_updates_test_oids SET f3 = 2 WHERE f3 is null;

\set QUIET true

SELECT * FROM min_updates_test ORDER BY 1,2,3;

SELECT * FROM min_updates_test_oids ORDER BY 1,2,3;

DROP TABLE min_updates_test;

DROP TABLE min_updates_test_oids;

--
-- Test triggers on views
--

CREATE VIEW main_view AS SELECT a, b FROM main_table;

-- Updates should succeed without rules or triggers, cause this simple view are auto-updatable.
INSERT INTO main_view VALUES (1,2);
UPDATE main_view SET b = 20 WHERE a = 50;
DELETE FROM main_view WHERE a = 50;
-- Should succeed even when there are no matching rows, cause this simple view are auto-updatable.
DELETE FROM main_view WHERE a = 51;

-- VIEW trigger function
CREATE OR REPLACE FUNCTION view_trigger() RETURNS trigger
LANGUAGE plpgsql AS $$
declare
    argstr text := '';
begin
    for i in 0 .. TG_nargs - 1 loop
        if i > 0 then
            argstr := argstr || ', ';
        end if;
        argstr := argstr || TG_argv[i];
    end loop;

    raise notice '% % % % (%)', TG_RELNAME, TG_WHEN, TG_OP, TG_LEVEL, argstr;

    if TG_LEVEL = 'ROW' then
        if TG_OP = 'INSERT' then
            raise NOTICE 'NEW: %', NEW;
            INSERT INTO main_table VALUES (NEW.a, NEW.b);
            RETURN NEW;
        end if;

        if TG_OP = 'UPDATE' then
            raise NOTICE 'OLD: %, NEW: %', OLD, NEW;
            UPDATE main_table SET a = NEW.a, b = NEW.b WHERE a = OLD.a AND b = OLD.b;
            if NOT FOUND then RETURN NULL; end if;
            RETURN NEW;
        end if;

        if TG_OP = 'DELETE' then
            raise NOTICE 'OLD: %', OLD;
            DELETE FROM main_table WHERE a = OLD.a AND b = OLD.b;
            if NOT FOUND then RETURN NULL; end if;
            RETURN OLD;
        end if;
    end if;

    RETURN NULL;
end;
$$;

-- Before row triggers aren't allowed on views
CREATE TRIGGER invalid_trig BEFORE INSERT ON main_view
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_ins_row');

CREATE TRIGGER invalid_trig BEFORE UPDATE ON main_view
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_upd_row');

CREATE TRIGGER invalid_trig BEFORE DELETE ON main_view
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_del_row');

-- After row triggers aren't allowed on views
CREATE TRIGGER invalid_trig AFTER INSERT ON main_view
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_ins_row');

CREATE TRIGGER invalid_trig AFTER UPDATE ON main_view
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_upd_row');

CREATE TRIGGER invalid_trig AFTER DELETE ON main_view
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_del_row');

-- Truncate triggers aren't allowed on views
CREATE TRIGGER invalid_trig BEFORE TRUNCATE ON main_view
EXECUTE PROCEDURE trigger_func('before_tru_row');

CREATE TRIGGER invalid_trig AFTER TRUNCATE ON main_view
EXECUTE PROCEDURE trigger_func('before_tru_row');

-- INSTEAD OF triggers aren't allowed on tables
CREATE TRIGGER invalid_trig INSTEAD OF INSERT ON main_table
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_ins');

CREATE TRIGGER invalid_trig INSTEAD OF UPDATE ON main_table
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_upd');

CREATE TRIGGER invalid_trig INSTEAD OF DELETE ON main_table
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_del');

-- Don't support WHEN clauses with INSTEAD OF triggers
CREATE TRIGGER invalid_trig INSTEAD OF UPDATE ON main_view
FOR EACH ROW WHEN (OLD.a <> NEW.a) EXECUTE PROCEDURE view_trigger('instead_of_upd');

-- Don't support column-level INSTEAD OF triggers
CREATE TRIGGER invalid_trig INSTEAD OF UPDATE OF a ON main_view
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_upd');

-- Don't support statement-level INSTEAD OF triggers
CREATE TRIGGER invalid_trig INSTEAD OF UPDATE ON main_view
EXECUTE PROCEDURE view_trigger('instead_of_upd');

-- Valid INSTEAD OF triggers
CREATE TRIGGER instead_of_insert_trig INSTEAD OF INSERT ON main_view
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_ins');

CREATE TRIGGER instead_of_update_trig INSTEAD OF UPDATE ON main_view
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_upd');

CREATE TRIGGER instead_of_delete_trig INSTEAD OF DELETE ON main_view
FOR EACH ROW EXECUTE PROCEDURE view_trigger('instead_of_del');

-- Valid BEFORE statement VIEW triggers
CREATE TRIGGER before_ins_stmt_trig BEFORE INSERT ON main_view
FOR EACH STATEMENT EXECUTE PROCEDURE view_trigger('before_view_ins_stmt');

CREATE TRIGGER before_upd_stmt_trig BEFORE UPDATE ON main_view
FOR EACH STATEMENT EXECUTE PROCEDURE view_trigger('before_view_upd_stmt');

CREATE TRIGGER before_del_stmt_trig BEFORE DELETE ON main_view
FOR EACH STATEMENT EXECUTE PROCEDURE view_trigger('before_view_del_stmt');

-- Valid AFTER statement VIEW triggers
CREATE TRIGGER after_ins_stmt_trig AFTER INSERT ON main_view
FOR EACH STATEMENT EXECUTE PROCEDURE view_trigger('after_view_ins_stmt');

CREATE TRIGGER after_upd_stmt_trig AFTER UPDATE ON main_view
FOR EACH STATEMENT EXECUTE PROCEDURE view_trigger('after_view_upd_stmt');

CREATE TRIGGER after_del_stmt_trig AFTER DELETE ON main_view
FOR EACH STATEMENT EXECUTE PROCEDURE view_trigger('after_view_del_stmt');

\set QUIET false

-- Insert into view using trigger
INSERT INTO main_view VALUES (20, 30);
INSERT INTO main_view VALUES (21, 31) RETURNING a, b;

-- Table trigger will prevent updates
UPDATE main_view SET b = 31 WHERE a = 20;
UPDATE main_view SET b = 32 WHERE a = 21 AND b = 31 RETURNING a, b;

-- Remove table trigger to allow updates
DROP TRIGGER before_upd_a_row_trig ON main_table;
UPDATE main_view SET b = 31 WHERE a = 20;
UPDATE main_view SET b = 32 WHERE a = 21 AND b = 31 RETURNING a, b;

-- Before and after stmt triggers should fire even when no rows are affected
UPDATE main_view SET b = 0 WHERE false;

-- Delete from view using trigger
DELETE FROM main_view WHERE a IN (20,21);
DELETE FROM main_view WHERE a = 31 RETURNING a, b;

\set QUIET true

-- Describe view should list triggers
\d main_view

-- Test dropping view triggers
DROP TRIGGER instead_of_insert_trig ON main_view;
DROP TRIGGER instead_of_delete_trig ON main_view;
\d+ main_view
DROP VIEW main_view;

--
-- Test triggers on a join view
--
CREATE TABLE country_table (
    country_id        serial primary key,
    country_name    text unique not null,
    continent        text not null
);

INSERT INTO country_table (country_name, continent)
    VALUES ('Japan', 'Asia'),
           ('UK', 'Europe'),
           ('USA', 'North America')
    RETURNING *;

CREATE TABLE city_table (
    city_id        serial primary key,
    city_name    text not null,
    population    bigint,
    country_id    int references country_table
);

CREATE VIEW city_view AS
    SELECT city_id, city_name, population, country_name, continent
    FROM city_table ci
    LEFT JOIN country_table co ON co.country_id = ci.country_id;

CREATE FUNCTION city_insert() RETURNS trigger LANGUAGE plpgsql AS $$
declare
    ctry_id int;
begin
    if NEW.country_name IS NOT NULL then
        SELECT country_id, continent INTO ctry_id, NEW.continent
            FROM country_table WHERE country_name = NEW.country_name;
        if NOT FOUND then
            raise exception 'No such country: "%"', NEW.country_name;
        end if;
    else
        NEW.continent := NULL;
    end if;

    if NEW.city_id IS NOT NULL then
        INSERT INTO city_table
            VALUES(NEW.city_id, NEW.city_name, NEW.population, ctry_id);
    else
        INSERT INTO city_table(city_name, population, country_id)
            VALUES(NEW.city_name, NEW.population, ctry_id)
            RETURNING city_id INTO NEW.city_id;
    end if;

    RETURN NEW;
end;
$$;

CREATE TRIGGER city_insert_trig INSTEAD OF INSERT ON city_view
FOR EACH ROW EXECUTE PROCEDURE city_insert();

CREATE FUNCTION city_delete() RETURNS trigger LANGUAGE plpgsql AS $$
begin
    DELETE FROM city_table WHERE city_id = OLD.city_id;
    if NOT FOUND then RETURN NULL; end if;
    RETURN OLD;
end;
$$;

CREATE TRIGGER city_delete_trig INSTEAD OF DELETE ON city_view
FOR EACH ROW EXECUTE PROCEDURE city_delete();

CREATE FUNCTION city_update() RETURNS trigger LANGUAGE plpgsql AS $$
declare
    ctry_id int;
begin
    if NEW.country_name IS DISTINCT FROM OLD.country_name then
        SELECT country_id, continent INTO ctry_id, NEW.continent
            FROM country_table WHERE country_name = NEW.country_name;
        if NOT FOUND then
            raise exception 'No such country: "%"', NEW.country_name;
        end if;

        UPDATE city_table SET city_name = NEW.city_name,
                              population = NEW.population,
                              country_id = ctry_id
            WHERE city_id = OLD.city_id;
    else
        UPDATE city_table SET city_name = NEW.city_name,
                              population = NEW.population
            WHERE city_id = OLD.city_id;
        NEW.continent := OLD.continent;
    end if;

    if NOT FOUND then RETURN NULL; end if;
    RETURN NEW;
end;
$$;

CREATE TRIGGER city_update_trig INSTEAD OF UPDATE ON city_view
FOR EACH ROW EXECUTE PROCEDURE city_update();

\set QUIET false

-- INSERT .. RETURNING
INSERT INTO city_view(city_name) VALUES('Tokyo') RETURNING *;
INSERT INTO city_view(city_name, population) VALUES('London', 7556900) RETURNING *;
INSERT INTO city_view(city_name, country_name) VALUES('Washington DC', 'USA') RETURNING *;
INSERT INTO city_view(city_id, city_name) VALUES(123456, 'New York') RETURNING *;
INSERT INTO city_view VALUES(234567, 'Birmingham', 1016800, 'UK', 'EU') RETURNING *;

-- UPDATE .. RETURNING
UPDATE city_view SET country_name = 'Japon' WHERE city_name = 'Tokyo'; -- error
UPDATE city_view SET country_name = 'Japan' WHERE city_name = 'Takyo'; -- no match
UPDATE city_view SET country_name = 'Japan' WHERE city_name = 'Tokyo' RETURNING *; -- OK

UPDATE city_view SET population = 13010279 WHERE city_name = 'Tokyo' RETURNING *;
UPDATE city_view SET country_name = 'UK' WHERE city_name = 'New York' RETURNING *;
UPDATE city_view SET country_name = 'USA', population = 8391881 WHERE city_name = 'New York' RETURNING *;
UPDATE city_view SET continent = 'EU' WHERE continent = 'Europe' RETURNING *;
UPDATE city_view v1 SET country_name = v2.country_name FROM city_view v2
    WHERE v2.city_name = 'Birmingham' AND v1.city_name = 'London' RETURNING *;

-- DELETE .. RETURNING
DELETE FROM city_view WHERE city_name = 'Birmingham' RETURNING *;

\set QUIET true

-- read-only view with WHERE clause
CREATE VIEW european_city_view AS
    SELECT * FROM city_view WHERE continent = 'Europe';
SELECT count(*) FROM european_city_view;

CREATE FUNCTION no_op_trig_fn() RETURNS trigger LANGUAGE plpgsql
AS 'begin RETURN NULL; end';

CREATE TRIGGER no_op_trig INSTEAD OF INSERT OR UPDATE OR DELETE
ON european_city_view FOR EACH ROW EXECUTE PROCEDURE no_op_trig_fn();

\set QUIET false

INSERT INTO european_city_view VALUES (0, 'x', 10000, 'y', 'z');
UPDATE european_city_view SET population = 10000;
DELETE FROM european_city_view;

\set QUIET true

-- rules bypassing no-op triggers
CREATE RULE european_city_insert_rule AS ON INSERT TO european_city_view
DO INSTEAD INSERT INTO city_view
VALUES (NEW.city_id, NEW.city_name, NEW.population, NEW.country_name, NEW.continent)
RETURNING *;

CREATE RULE european_city_update_rule AS ON UPDATE TO european_city_view
DO INSTEAD UPDATE city_view SET
    city_name = NEW.city_name,
    population = NEW.population,
    country_name = NEW.country_name
WHERE city_id = OLD.city_id
RETURNING NEW.*;

CREATE RULE european_city_delete_rule AS ON DELETE TO european_city_view
DO INSTEAD DELETE FROM city_view WHERE city_id = OLD.city_id RETURNING *;

\set QUIET false

-- INSERT not limited by view's WHERE clause, but UPDATE AND DELETE are
INSERT INTO european_city_view(city_name, country_name)
    VALUES ('Cambridge', 'USA') RETURNING *;
UPDATE european_city_view SET country_name = 'UK'
    WHERE city_name = 'Cambridge';
DELETE FROM european_city_view WHERE city_name = 'Cambridge';

-- UPDATE and DELETE via rule and trigger
UPDATE city_view SET country_name = 'UK'
    WHERE city_name = 'Cambridge' RETURNING *;
UPDATE european_city_view SET population = 122800
    WHERE city_name = 'Cambridge' RETURNING *;
DELETE FROM european_city_view WHERE city_name = 'Cambridge' RETURNING *;

-- join UPDATE test
UPDATE city_view v SET population = 599657
    FROM city_table ci, country_table co
    WHERE ci.city_name = 'Washington DC' and co.country_name = 'USA'
    AND v.city_id = ci.city_id AND v.country_name = co.country_name
    RETURNING co.country_id, v.country_name,
              v.city_id, v.city_name, v.population;

\set QUIET true

SELECT * FROM city_view order by 1;

DROP TABLE city_table CASCADE;
DROP TABLE country_table;


-- Test pg_trigger_depth()

create table depth_a (id int not null primary key);
create table depth_b (id int not null primary key);
create table depth_c (id int not null primary key);

create function depth_a_tf() returns trigger
  language plpgsql as $$
begin
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  insert into depth_b values (new.id);
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  return new;
end;
$$;
create trigger depth_a_tr before insert on depth_a
  for each row execute procedure depth_a_tf();

create function depth_b_tf() returns trigger
  language plpgsql as $$
begin
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  begin
    execute 'insert into depth_c values (' || new.id::text || ')';
 -- exception
 --   when sqlstate 'U9999' then
 --     raise notice 'SQLSTATE = U9999: depth = %', pg_trigger_depth();
  end;
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  if new.id = 1 then
    execute 'insert into depth_c values (' || new.id::text || ')';
  end if;
  return new;
end;
$$;
create trigger depth_b_tr before insert on depth_b
  for each row execute procedure depth_b_tf();

create function depth_c_tf() returns trigger
  language plpgsql as $$
begin
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  if new.id = 1 then
    raise exception sqlstate 'U9999';
  end if;
  raise notice '%: depth = %', tg_name, pg_trigger_depth();
  return new;
end;
$$;
create trigger depth_c_tr before insert on depth_c
  for each row execute procedure depth_c_tf();

select pg_trigger_depth();
insert into depth_a values (1);
select pg_trigger_depth();
insert into depth_a values (2);
select pg_trigger_depth();

drop table depth_a, depth_b, depth_c;
drop function depth_a_tf();
drop function depth_b_tf();
drop function depth_c_tf();

-- test declare + on in one sql.
create schema trigger_declare_test;
set current_schema to trigger_declare_test;
create table declare_test(id int);
create index declare on declare_test(id);
\d declare_test
drop index declare;

create table declare(id int);
create index id_test on declare(id);
\d declare

CREATE OR REPLACE FUNCTION declare() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
   INSERT INTO declare_test VALUES(NEW.id);
   RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER declare AFTER INSERT ON declare FOR EACH ROW EXECUTE PROCEDURE declare();
insert into declare values(1);
drop TRIGGER declare on declare cascade;

CREATE TRIGGER declare_test AFTER INSERT ON declare FOR EACH ROW EXECUTE PROCEDURE declare();
insert into declare values(2);
drop TRIGGER declare_test on declare cascade;

CREATE OR REPLACE FUNCTION declare() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
   INSERT INTO declare VALUES(NEW.id);
   RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER declare AFTER INSERT ON declare_test FOR EACH ROW EXECUTE PROCEDURE declare();
insert into declare_test values(3);
drop TRIGGER declare on declare_test cascade;
select * from declare order by 1;
select * from declare_test order by 1;

drop table declare;
drop table declare_test;
drop schema trigger_declare_test cascade;


create schema part_bri_warnning;
set search_path = 'part_bri_warnning';

drop table if exists target;
create table target (c1 int, c2 int, c3 timestamp)
partition by range(c2)
(
  partition p1 values less than (2),
  partition p2 values less than (4),
  partition p3 values less than (6),
  partition p4 values less than (8),
  partition p5 values less than (MAXVALUE)
);

create function target_tri() return trigger
as
begin
    new.c1 = 2;
    return new;
end;
/

create trigger test_part_trigger 
    before update on target
    for each row
    execute procedure target_tri();
	
--merge updated
insert into target values (1, 1, now());
\parallel on 2

begin
    update target set c2 = 1 where c1 = 1;
    perform pg_sleep(4);
end;
/

begin
    perform pg_sleep(2);
	merge into target t1 using (select 1 c1) t2 on (t1.c1 = t2.c1)
	when matched then update set t1.c2 = 4;
end;
/

\parallel off

-- concurrent update
delete from target;
insert into target values (1, 1, now());

\parallel on 2
begin
    update target set c2 = 1 where c1 = 1;
    perform pg_sleep(4);
end;
/

begin
	perform pg_sleep(2);
    update target set c1=10 where c1=1;
end;
/
\parallel off
drop table target;
drop function target_tri;

--self modify
create table t1(c1 int, c2 text, c3 timestamp, c4 int)
partition by range(c4)
(
  partition p1 values less than (2),
  partition p2 values less than (4),
  partition p3 values less than (6),
  partition p4 values less than (8),
  partition p5 values less than (MAXVALUE)
);

create table t2 (c1 int);
insert into t1 values (1, 'a', '2023-09-15',1);

create or replace function t1_tri_func() return trigger as
begin
    new.c3 = '2023-09-16';
    return new;
end;
/
create trigger t1_tri
    before update on t1
    for each row
    execute procedure t1_tri_func();

insert into t2 values (1);
insert into t2 values (1);
merge into t1 using t2
on (t1.c1 = t2.c1)
when matched then update set c2 = 'e'
when not matched then insert values (5, 'e', '2023-09-20',0);

select * from t1 order by c1;

-- test for lock
create table test_lock (a int);
insert into test_lock values (1);
\parallel on 2
DECLARE
  v1 int;
  CURSOR emp_cur IS
    SELECT * from test_lock FOR update of test_lock ; 
begin
    OPEN emp_cur;
    perform pg_sleep(3);
    CLOSE emp_cur;
end;
/

begin
    perform pg_sleep(1);
    perform * from  test_lock for update nowait;
end;
/
\parallel off

drop table test_lock;
drop table t1;
drop table t2;
drop function t1_tri_func;
drop schema part_bri_warnning;

create schema test_schema_for_trigger;
set current_schema to test_schema_for_trigger;
--触发器测试
--测试时用到的package
CREATE OR REPLACE PACKAGE func_test_pkg1 AS
  -- integer 类型的变量
  int_val INTEGER := 0;

  -- record 类型的定义
  TYPE rec_type IS RECORD (
    msg VARCHAR2(100)
  );

  PROCEDURE proc1;
END func_test_pkg1;
/
CREATE OR REPLACE PACKAGE BODY func_test_pkg1 AS
  PROCEDURE proc1 AS
  BEGIN
    RAISE NOTICE 'In func_test_pkg1.proc1()';
  END proc1;
END func_test_pkg1;
/

CREATE TABLE trg_test(id int, name text);
create or replace function global_cache_trigger_test()
  return TRIGGER as  
 begin
	func_test_pkg1.int_val = func_test_pkg1.int_val + 1;
  raise notice 'func_test_pkg1.int_val=%',func_test_pkg1.int_val;
  return new;
end;
/

CREATE TRIGGER trigger_global_cache_trigger_test
AFTER INSERT ON trg_test
FOR EACH ROW
EXECUTE PROCEDURE global_cache_trigger_test();

insert into trg_test values(1,'引用package中的变量');
insert into trg_test values(1,'引用package中的变量');

drop package func_test_pkg1;


-- without for each row
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (id1 INT, id2 INT, id3 INT);
DROP FUNCTION IF EXISTS test_function;
CREATE FUNCTION test_function() RETURNS TRIGGER AS
$$
BEGIN
    UPDATE test_table SET id2 = 5 WHERE id1 = NEW.id1;
    RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS test_trigger ON test_table;

CREATE TRIGGER test_trigger
BEFORE INSERT ON test_table
EXECUTE PROCEDURE test_function();

ALTER TABLE test_table DISABLE TRIGGER test_trigger;

ALTER TABLE test_table ENABLE TRIGGER test_trigger;

INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);
INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);
INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);
INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);

SELECT * FROM test_table;
DROP TRIGGER test_trigger ON test_table;
DROP FUNCTION test_function;
DROP TABLE test_table;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (id1 INT, id2 INT, id3 INT);
DROP FUNCTION IF EXISTS test_function;
CREATE FUNCTION test_function() RETURNS TRIGGER AS
$$
BEGIN
    UPDATE test_table SET id2 = 5 WHERE id1 = NEW.id1;
    RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS test_trigger ON test_table;

CREATE TRIGGER test_trigger
BEFORE INSERT ON test_table
for each row
EXECUTE PROCEDURE test_function();

ALTER TABLE test_table DISABLE TRIGGER test_trigger;

ALTER TABLE test_table ENABLE TRIGGER test_trigger;

INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);
INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);
INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);
INSERT INTO test_table (id1, id2, id3) VALUES (1, 10, 100);

SELECT * FROM test_table;
DROP TRIGGER test_trigger ON test_table;
DROP FUNCTION test_function;
DROP TABLE test_table;


drop schema test_schema_for_trigger cascade;