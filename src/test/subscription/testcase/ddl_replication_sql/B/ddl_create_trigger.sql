create table employees(id int,salary int);

create or replace trigger t
before insert or update of salary,id 
or delete on employees
begin
case
when inserting then
dbms_output.put_line('inserting');
when updating ('salary') then
dbms_output.put_line('updating salary');
when updating ('id') then
dbms_output.put_line('updating id');
when deleting then
dbms_output.put_line('deleting');
end case;
end;
/

create table oldtab(id int,c1 char(8));
create table newtab(id int,c1 int);

create or replace trigger tri1
after insert on oldtab
for each statement
begin
insert into newtab values(1,1),(2,2),(3,3);
end;
/

create or replace trigger tri2
after update on oldtab
for each statement
begin
update newtab set c1=4 where id=2;
end;
/

create or replace trigger tri4
after truncate on oldtab
for each statement
begin
insert into newtab values(4,4);
end;
/

create table oldtab2(id int,c1 char(8));
create table newtab2(id int,c1 int);

CREATE OR REPLACE FUNCTION func_tri21()
RETURNS TRIGGER AS $$
BEGIN
insert into newtab2 values(1,1),(2,2),(3,3);
RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_tri22()
RETURNS TRIGGER AS $$
BEGIN
update newtab2 set c1=4 where id=2;
RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_tri24()
RETURNS TRIGGER AS $$
BEGIN
insert into newtab2 values(4,4);
RETURN OLD;
END;
$$
LANGUAGE plpgsql;

create trigger tri21
after insert on oldtab2
for each statement
execute procedure func_tri21();

create trigger tri22
after update on oldtab2
for each statement
execute procedure func_tri22();

create trigger tri24
after truncate on oldtab2
for each statement
execute procedure func_tri24();

create table t_trig_when(f1 boolean primary key, f2 text, f3 int, f4 date);
create or replace function dummy_update_func() returns trigger as $$
begin
	raise notice 'dummy_update_func(%) called: action = %, oid = %, new = %', TG_ARGV[0], TG_OP, OLD, NEW;
    return new;
end;
$$ language plpgsql;

create trigger f1_trig_update after update of f1 on t_trig_when for each row when (not old.f1 and new.f1) execute procedure dummy_update_func('update');

alter table t_trig_when DISABLE TRIGGER f1_trig_update;
alter table t_trig_when ENABLE TRIGGER f1_trig_update;
alter table t_trig_when DISABLE TRIGGER f1_trig_update;
alter table t_trig_when ENABLE ALWAYS TRIGGER f1_trig_update;  
alter table t_trig_when DISABLE TRIGGER ALL;
alter table t_trig_when ENABLE TRIGGER ALL; 
alter table t_trig_when DISABLE TRIGGER USER ;
alter table t_trig_when ENABLE TRIGGER USER ;