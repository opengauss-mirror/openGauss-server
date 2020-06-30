--
-- XC_TRIGGER
--
set max_query_retry_times = 3;
-- Creation of a trigger-based method to improve run of count queries
-- by incrementation and decrementation of statement-based and row-based counters

-- Create tables
CREATE TABLE xc_trigger_rep_tab (a int, b int) DISTRIBUTE BY REPLICATION;
ALTER TABLE xc_trigger_rep_tab ADD PRIMARY KEY(A, B);
CREATE TABLE xc_trigger_hash_tab (a int, b int) DISTRIBUTE BY HASH(a);
CREATE TABLE xc_trigger_rr_tab (a int, b int) DISTRIBUTE BY ROUNDROBIN;
CREATE TABLE xc_trigger_modulo_tab (a int, b int) DISTRIBUTE BY MODULO(a);
CREATE TABLE table_stats (table_name text primary key,
num_insert_query int DEFAULT 0,
num_update_query int DEFAULT 0,
num_delete_query int DEFAULT 0,
num_truncate_query int DEFAULT 0,
num_insert_row int DEFAULT 0,
num_update_row int DEFAULT 0,
num_delete_row int DEFAULT 0);

-- Insert default entries
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_rep_tab');
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_hash_tab');
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_rr_tab');
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_modulo_tab');

-- Functions to manage stats of table
-- Count the number of queries run
CREATE FUNCTION count_insert_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_insert_query = num_insert_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_update_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_update_query = num_update_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_delete_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_delete_query = num_delete_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_truncate_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_truncate_query = num_truncate_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
-- Count the number of rows used
CREATE FUNCTION count_insert_row() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_insert_row = num_insert_row + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_update_row() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_update_row = num_update_row + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_delete_row() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_delete_row = num_delete_row + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';

-- Define the events for each table
-- Replicated table
CREATE TRIGGER rep_count_insert_query AFTER INSERT ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER rep_count_update_query BEFORE UPDATE ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER rep_count_delete_query BEFORE DELETE ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER rep_count_truncate_query BEFORE TRUNCATE ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER rep_count_insert_row BEFORE INSERT ON xc_trigger_rep_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER rep_count_update_row BEFORE UPDATE ON xc_trigger_rep_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER rep_count_delete_row BEFORE DELETE ON xc_trigger_rep_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();
-- Renaming of trigger based on a table
ALTER TRIGGER repcount_update_row ON my_table RENAME TO repcount_update_row2;
-- Hash table
CREATE TRIGGER hash_count_insert_query AFTER INSERT ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER hash_count_update_query BEFORE UPDATE ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER hash_count_delete_query BEFORE DELETE ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER hash_count_truncate_query BEFORE TRUNCATE ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER hash_count_insert_row BEFORE INSERT ON xc_trigger_hash_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER hash_count_update_row BEFORE UPDATE ON xc_trigger_hash_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER hash_count_delete_row BEFORE DELETE ON xc_trigger_hash_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();
-- Round robin table
CREATE TRIGGER rr_count_insert_query AFTER INSERT ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER rr_count_update_query BEFORE UPDATE ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER rr_count_delete_query BEFORE DELETE ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER rr_count_truncate_query BEFORE TRUNCATE ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER rr_count_insert_row BEFORE INSERT ON xc_trigger_rr_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER rr_count_update_row BEFORE UPDATE ON xc_trigger_rr_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER rr_count_delete_row BEFORE DELETE ON xc_trigger_rr_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();
-- Modulo table
CREATE TRIGGER modulo_count_insert_query AFTER INSERT ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER modulo_count_update_query BEFORE UPDATE ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER modulo_count_delete_query BEFORE DELETE ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER modulo_count_truncate_query BEFORE TRUNCATE ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER modulo_count_insert_row BEFORE INSERT ON xc_trigger_modulo_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER modulo_count_update_row BEFORE UPDATE ON xc_trigger_modulo_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER modulo_count_delete_row BEFORE DELETE ON xc_trigger_modulo_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();

-- Insert some values to test the INSERT triggers
INSERT INTO xc_trigger_rep_tab VALUES (1,2);
INSERT INTO xc_trigger_rep_tab VALUES (3,4);
INSERT INTO xc_trigger_rep_tab VALUES (5,6),(7,8),(9,10);
INSERT INTO xc_trigger_hash_tab VALUES (1,2);
INSERT INTO xc_trigger_hash_tab VALUES (3,4);
INSERT INTO xc_trigger_hash_tab VALUES (5,6),(7,8),(9,10);
INSERT INTO xc_trigger_rr_tab VALUES (1,2);
INSERT INTO xc_trigger_rr_tab VALUES (3,4);
INSERT INTO xc_trigger_rr_tab VALUES (5,6),(7,8),(9,10);
INSERT INTO xc_trigger_modulo_tab VALUES (1,2);
INSERT INTO xc_trigger_modulo_tab VALUES (3,4);
INSERT INTO xc_trigger_modulo_tab VALUES (5,6),(7,8),(9,10);
SELECT * FROM table_stats ORDER BY table_name;

-- Update some values to test the UPDATE triggers
UPDATE xc_trigger_rep_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_rep_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_rep_tab SET b = b+1 WHERE a IN (5,7,9);
UPDATE xc_trigger_hash_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_hash_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_hash_tab SET b = b+1 WHERE a IN (5,7,9);
UPDATE xc_trigger_rr_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_rr_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_rr_tab SET b = b+1 WHERE a IN (5,7,9);
UPDATE xc_trigger_modulo_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_modulo_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_modulo_tab SET b = b+1 WHERE a IN (5,7,9);
SELECT * FROM table_stats ORDER BY table_name;

-- Delete some values to test the DELETE triggers
DELETE FROM xc_trigger_rep_tab WHERE a = 1;
DELETE FROM xc_trigger_rep_tab WHERE a = 3;
DELETE FROM xc_trigger_rep_tab WHERE a IN (5,7,9);
DELETE FROM xc_trigger_hash_tab WHERE a = 1;
DELETE FROM xc_trigger_hash_tab WHERE a = 3;
DELETE FROM xc_trigger_hash_tab WHERE a IN (5,7,9);
DELETE FROM xc_trigger_rr_tab WHERE a = 1;
DELETE FROM xc_trigger_rr_tab WHERE a = 3;
DELETE FROM xc_trigger_rr_tab WHERE a IN (5,7,9);
DELETE FROM xc_trigger_modulo_tab WHERE a = 1;
DELETE FROM xc_trigger_modulo_tab WHERE a = 3;
DELETE FROM xc_trigger_modulo_tab WHERE a IN (5,7,9);
SELECT * FROM table_stats ORDER BY table_name;

-- Truncate the table to test the TRUNCATE triggers
TRUNCATE xc_trigger_rep_tab;
TRUNCATE xc_trigger_hash_tab;
TRUNCATE xc_trigger_rr_tab;
TRUNCATE xc_trigger_modulo_tab;
SELECT * FROM table_stats ORDER BY table_name;

-- Clean up everything
DROP TABLE xc_trigger_rep_tab, xc_trigger_hash_tab, xc_trigger_rr_tab, xc_trigger_modulo_tab, table_stats;
DROP FUNCTION count_tuple_increment();
DROP FUNCTION count_tuple_decrement();
DROP FUNCTION count_insert_query();
DROP FUNCTION count_update_query();
DROP FUNCTION count_delete_query();
DROP FUNCTION count_truncate_query();
DROP FUNCTION count_insert_row();
DROP FUNCTION count_update_row();
DROP FUNCTION count_delete_row();

-- Tests for INSTEAD OF
-- Replace operations on a view by operations on a table
CREATE TABLE real_table (a int, b int) distribute by replication;
CREATE VIEW real_view AS SELECT a,b FROM real_table;
CREATE FUNCTION insert_real() RETURNS TRIGGER AS $_$
	BEGIN
		INSERT INTO real_table VALUES (NEW.a, NEW.b);
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION update_real() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE real_table SET a = NEW.a, b = NEW.b WHERE a = OLD.a AND b = OLD.b;
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION delete_real() RETURNS TRIGGER AS $_$
	BEGIN
		DELETE FROM real_table WHERE a = OLD.a AND b = OLD.b;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE TRIGGER insert_real_trig INSTEAD OF INSERT ON real_view FOR EACH ROW EXECUTE PROCEDURE insert_real();
CREATE TRIGGER update_real_trig INSTEAD OF UPDATE ON real_view FOR EACH ROW EXECUTE PROCEDURE update_real();
CREATE TRIGGER delete_real_trig INSTEAD OF DELETE ON real_view FOR EACH ROW EXECUTE PROCEDURE delete_real();
-- Renaming of trigger based on a view
ALTER TRIGGER delete_real_trig ON real_view RENAME TO delete_real_trig2;
-- Actions
INSERT INTO real_view VALUES(1,1);
SELECT * FROM real_table;
UPDATE real_view SET b = 4 WHERE a = 1;
SELECT * FROM real_table;
DELETE FROM real_view WHERE a = 1;
SELECT * FROM real_table;
-- Clean up
DROP VIEW real_view CASCADE;
DROP FUNCTION insert_real() cascade;
DROP FUNCTION update_real() cascade;
DROP FUNCTION delete_real() cascade;
DROP TABLE real_table;


-- Test if distribution column value can be updated by a non-immutable trigger function.
CREATE TABLE xc_trig_dist(id int, id2 int, id3 int, v varchar) distribute by replication;
ALTER TABLE xc_trig_dist ADD PRIMARY KEY(id2);

insert into xc_trig_dist values (NULL, 1, 1, 'ppppppppppp'), (2, 2, 2, 'qqqqqqqqqqq'), (3, 3, 3, 'rrrrrrrrrrr');
CREATE FUNCTION xc_func_dist() RETURNS trigger AS $$
declare
    BEGIN
		NEW.v = regexp_replace(NEW.v , 'pp$', 'xx');
		if NEW.id is NULL then
			NEW.id = 999;
		else
			NEW.id = NULL;
		end if;

		return NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER xc_dist_trigger BEFORE UPDATE ON xc_trig_dist
    FOR EACH ROW EXECUTE PROCEDURE xc_func_dist();

--This should pass because the table is replicated.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';
SELECT * from xc_trig_dist order by id2;

-- Reinitialize the rows
truncate table xc_trig_dist;
insert into xc_trig_dist values (NULL, 1, 1, 'ppppppppppp'), (2, 2, 2, 'qqqqqqqqqqq'), (3, 3, 3, 'rrrrrrrrrrr');

-- Now make the table distributed
ALTER TABLE xc_trig_dist distribute by hash(v);
--This should fail because v is a distributed col and trigger updates it.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';

ALTER TABLE xc_trig_dist distribute by hash(id);
--This should fail because id is a distributed col and trigger updates it.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';

ALTER TABLE xc_trig_dist distribute by hash(id2);
--This should pass because id2 is not updated.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%qqq%';
SELECT * from xc_trig_dist order by id2;

ALTER TABLE xc_trig_dist distribute by hash(v);
--This should still pass because v does not get modified by the regexp_replace()
-- function in the trigger function.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';
SELECT * from xc_trig_dist order by id2;

DROP table xc_trig_dist cascade;
DROP function xc_func_dist() cascade;

--self add
--test insert and update trigger on merge into condition
create table merge_src_table_with_trigger(product_id integer,product_name varchar2(60),category varchar2(60));
insert into merge_src_table_with_trigger values (1501, 'vivitar 35mm', 'electrncs');
insert into merge_src_table_with_trigger values (1502, 'olympus is50', 'electrncs');
insert into merge_src_table_with_trigger values (1600, 'play gym', 'toys');
create table merge_des_table_with_trigger(product_id integer,product_name varchar2(60),category varchar2(60));
insert into merge_des_table_with_trigger values (1502, 'olympus camera', 'electrncs');
insert into merge_des_table_with_trigger values (1700, 'wait interface', 'books');
create or replace function tri_b_merge_src_table_with_trigger_insert_func() returns trigger as
$$
declare
begin
	raise notice '% % % %', tg_relname, tg_op, tg_when, tg_level;
	return new;  
end
$$ language plpgsql;
create or replace function tri_b_merge_src_table_with_trigger_update_func() returns trigger as
$$
declare
begin
	raise notice '% % % %', tg_relname, tg_op, tg_when, tg_level;
	return new;  
end
$$ language plpgsql;
create or replace function tri_b_merge_src_table_with_trigger_bs_update_func() returns trigger as
$$
declare
begin
	raise notice '% % % %', tg_relname, tg_op, tg_when, tg_level;
	return new;  
end
$$ language plpgsql;
create or replace function tri_b_merge_src_table_with_trigger_as_update_func() returns trigger as
$$
declare
begin
	raise notice '% % % %', tg_relname, tg_op, tg_when, tg_level;
	return new;  
end
$$ language plpgsql;
create or replace function tri_b_merge_src_table_with_trigger_bs_delete_func() returns trigger as
$$
declare
begin
	raise notice '% % % %', tg_relname, tg_op, tg_when, tg_level;
	return new;  
end
$$ language plpgsql;
create or replace function tri_b_merge_src_table_with_trigger_as_delete_func() returns trigger as
$$
declare
begin
	raise notice '% % % %', tg_relname, tg_op, tg_when, tg_level;
	return new;  
end
$$ language plpgsql;
create trigger tri_b_merge_src_table_with_trigger_insert
  before insert on merge_src_table_with_trigger
  for each row
  execute procedure tri_b_merge_src_table_with_trigger_insert_func();
create trigger tri_b_merge_src_table_with_trigger_update
  after update on merge_src_table_with_trigger
  for each row
  execute procedure tri_b_merge_src_table_with_trigger_update_func();
create trigger tri_b_merge_src_table_with_trigger_bs_update
  before update on merge_src_table_with_trigger
  for each statement
  execute procedure tri_b_merge_src_table_with_trigger_bs_update_func();
create trigger tri_b_merge_src_table_with_trigger_as_update
  before update on merge_src_table_with_trigger
  for each statement
  execute procedure tri_b_merge_src_table_with_trigger_as_update_func();
create trigger tri_b_merge_src_table_with_trigger_bs_delete
  before update on merge_src_table_with_trigger
  for each statement
  execute procedure tri_b_merge_src_table_with_trigger_bs_delete_func();
create trigger tri_b_merge_src_table_with_trigger_as_delete
  before update on merge_src_table_with_trigger
  for each statement
  execute procedure tri_b_merge_src_table_with_trigger_as_delete_func();
\d+ merge_src_table_with_trigger

merge into merge_src_table_with_trigger p
using merge_des_table_with_trigger np
on (p.product_id = np.product_id)
when matched then
update set p.product_name = np.product_name, p.category = np.category where p.product_name !=
'play gym'
when not matched then
insert values (np.product_id, np.product_name, np.category) where np.category = 'books';
select * from merge_des_table_with_trigger order by product_id;
select * from merge_src_table_with_trigger order by product_id;
drop table if exists merge_src_table_with_trigger;
drop table if exists merge_des_table_with_trigger;
drop function tri_b_merge_src_table_with_trigger_insert_func;
drop function tri_b_merge_src_table_with_trigger_update_func;
drop function tri_b_merge_src_table_with_trigger_bs_update_func;
drop function tri_b_merge_src_table_with_trigger_as_update_func;
drop function tri_b_merge_src_table_with_trigger_bs_delete_func;
drop function tri_b_merge_src_table_with_trigger_as_delete_func;
--test create trigger on difference type table
create table nor_column_table_with_trigger(c1 int,c2 int,c3 int) with(orientation=column);
create temp table temp_column_table_with_trigger(c1 int,c2 int,c3 int) with(orientation=column);
create temp table temp_row_table_with_trigger(c1 int,c2 int,c3 int unique deferrable);
create unlogged table unlogged_row_table_with_trigger(c1 int,c2 int,c3 int);
create unlogged table unlogged_column_table_with_trigger(c1 int,c2 int,c3 int) with(orientation=column);
create or replace function temp_trigger_func_for_test() returns trigger as $temp_trigger_func_for_test$
begin
   raise notice '% % % %', tg_relname, tg_op, tg_when, tg_level;
   return new ; --important!!! : if return null ,the update operator will not active
end;
$temp_trigger_func_for_test$ language plpgsql;
create trigger temp_trigger before update on nor_column_table_with_trigger
for each row execute procedure temp_trigger_func_for_test();
create trigger temp_trigger before update on temp_column_table_with_trigger
for each row execute procedure temp_trigger_func_for_test();
create trigger temp_trigger before update on temp_row_table_with_trigger
for each row execute procedure temp_trigger_func_for_test();
create trigger temp_trigger before update on unlogged_row_table_with_trigger
for each row execute procedure temp_trigger_func_for_test();
create trigger temp_trigger before update on unlogged_column_table_with_trigger
for each row execute procedure temp_trigger_func_for_test();
drop table if exists nor_column_table_with_trigger;
drop table if exists temp_column_table_with_trigger;
drop table if exists temp_row_table_with_trigger;
drop table if exists unlogged_row_table_with_trigger;
drop table if exists unlogged_column_table_with_trigger;
drop function temp_trigger_func_for_test;
--test trigger on repliaction table
create table replication_table_with_trigger(col char(100) primary key) distribute by replication;
insert into replication_table_with_trigger values('tom');
create or replace function replication_table_with_trigger_func() returns trigger as $$
begin
    raise info 'trigger of replication_table_with_trigger  is triggering';
    return null;
end;
$$ language plpgsql;
create trigger temp_trigger after update on replication_table_with_trigger
for each row execute procedure replication_table_with_trigger_func();
\set verbosity verbose
update replication_table_with_trigger set col='amy';
select * from replication_table_with_trigger;
drop table replication_table_with_trigger;
--test multi node group
create node group test_trigger_min_group with(datanode1);
create or replace function multi_group_trigger_function1() returns trigger as
$$
begin
        insert into multi_group_des_tbl values(new.id1, new.id2);
        return null;
end
$$ language plpgsql;
create or replace function multi_group_trigger_function2() returns trigger as
$$
begin
        update multi_group_des_tbl set id2 = new.id2 where id1 = 100;
        return new;
end
$$ language plpgsql;
create or replace function multi_group_trigger_function3() returns trigger as
$$
begin
        delete multi_group_des_tbl where id1 = old.id1;
        return old;
end
$$ language plpgsql;
create table multi_group_src_tbl(id1 int, id2 int) to group test_trigger_min_group;
create table multi_group_des_tbl(id1 int, id2 int);
create trigger mg_insert_trigger after insert on multi_group_src_tbl for each row execute procedure multi_group_trigger_function1();
create trigger mg_update_trigger before update on multi_group_src_tbl for each row execute procedure multi_group_trigger_function2();
create trigger mg_delete_trigger before delete on multi_group_src_tbl for each row execute procedure multi_group_trigger_function3();
insert into multi_group_src_tbl values(100,200);
insert into multi_group_src_tbl values(200,400);
update multi_group_src_tbl set id2 = 800 where id1 = 100;
delete from multi_group_src_tbl where id1 = 200;
select * from multi_group_des_tbl;
select * from multi_group_src_tbl;
drop table multi_group_des_tbl;
drop table multi_group_src_tbl;
--exchange the node group of src and des table
create table multi_group_src_tbl(id1 int, id2 int);
create table multi_group_des_tbl(id1 int, id2 int) to group test_trigger_min_group;
create trigger mg_insert_trigger after insert on multi_group_src_tbl for each row execute procedure multi_group_trigger_function1();
create trigger mg_update_trigger before update on multi_group_src_tbl for each row execute procedure multi_group_trigger_function2();
create trigger mg_delete_trigger before delete on multi_group_src_tbl for each row execute procedure multi_group_trigger_function3();
insert into multi_group_src_tbl values(100,200);
insert into multi_group_src_tbl values(200,400);
update multi_group_src_tbl set id2 = 800 where id1 = 100;
delete from multi_group_src_tbl where id1 = 200;
select * from multi_group_des_tbl;
select * from multi_group_src_tbl;
drop table multi_group_des_tbl;
drop table multi_group_src_tbl;
drop node group test_trigger_min_group;
--test \h command
\h create trigger
\h alter trigger
\h drop trigger
\h alter table
create table trigger_with_rowtype_inexpr1(a int,b numeric);
create table trigger_with_rowtype_inexpr2(a int,b numeric);
create table trigger_with_rowtype_inexpr3(a int,b numeric);
insert into trigger_with_rowtype_inexpr1 values(generate_series(1,50),generate_series(1,50));
insert into trigger_with_rowtype_inexpr2 values(generate_series(1,50),generate_series(11,60));
insert into trigger_with_rowtype_inexpr3 values(generate_series(1,50),generate_series(38,-60,-2));
create or replace function tg_upt_1() returns trigger as '
declare
  cursor c1 is select a from trigger_with_rowtype_inexpr1 order by 1 limit 10;
  cursor c2 is select a from trigger_with_rowtype_inexpr2 order by 1 desc;
  rec trigger_with_rowtype_inexpr1%ROWTYPE;
begin
  begin
    open c1;
    loop
      fetch c1 into rec;
      update trigger_with_rowtype_inexpr1 set b=b-30 where a = rec.a;
      update trigger_with_rowtype_inexpr2 set b=b+30 where a = rec.a;
      exit when c1%NOTFOUND;
    end loop;
    close c1;
    select 1/0;
  exception
    when others then
      raise notice ''1-ERROR OCCURS'';
  end;
  update trigger_with_rowtype_inexpr2 set b=b-40;
  update trigger_with_rowtype_inexpr3 set b=b+40;
  begin
    open c2;
    loop
      fetch c2 into rec;
      update trigger_with_rowtype_inexpr1 set b=b-5 where a=rec.a;
      update trigger_with_rowtype_inexpr2 set b=b-10 where a=rec.a;
      update trigger_with_rowtype_inexpr3 set b=b+15 where a=rec.a;
      exit when c2%NOTFOUND;
    end loop;
    open c2;
  exception
    when others then
      raise notice ''2-ERROR OCCURS.'';
  end;
  begin
    open c2;
    loop
      fetch c2 into rec;
      update trigger_with_rowtype_inexpr1 set b=b-5 where a=rec.a;
      update trigger_with_rowtype_inexpr2 set b=b-10 where a=rec.a;
      update trigger_with_rowtype_inexpr3 set b=b+15 where a=rec.a;
      exit when c2%NOTFOUND;
    end loop;
    close c2;
  exception
    when others then
      raise notice ''3-ERROR OCCURS.'';
  end;
  begin
    open c2;
    loop
      fetch c2 into rec;
      update trigger_with_rowtype_inexpr1 set b=b+8 where a=rec.a;
      update trigger_with_rowtype_inexpr2 set b=b+12 where a=rec.a;
      update trigger_with_rowtype_inexpr3 set b=b-20 where a=rec.a;
      exit when c2%NOTFOUND;
    end loop;
    close c2;
  exception
    when others then
      raise notice ''4-ERROR OCCURS.'';
  end;
end;
' language plpgsql;
create trigger tg_upt after insert or delete on trigger_with_rowtype_inexpr1 for each row execute procedure tg_upt_1();
insert into trigger_with_rowtype_inexpr1 values(99,50);
drop table trigger_with_rowtype_inexpr1;
drop table trigger_with_rowtype_inexpr2;
drop table trigger_with_rowtype_inexpr3;
