--------------------------------------------------------------------------------------------
/* 
 *  procedure
 */
--------------------------------------------------------------------------------------------
\c upsert;
SET CURRENT_SCHEMA TO upsert_test_procedure;

drop table IF EXISTS t_proc;
create table t_proc(c1 int, c2 int, c3 int unique);
insert into t_proc select a,a,a from generate_series(1,20) as a;
select *from t_proc order by c3;

create or replace procedure mul_ups( c in INTEGER)
as
declare
val int;
begin
	for val in select c3 from t_proc loop
		insert into t_proc values(c,c, val) on duplicate key update c1 = c, c2 =c;
	end loop;
end;
/

call mul_ups(100);
select *from t_proc order by c3;

create or replace procedure mul_ups( c in INTEGER)
as
declare
val int;
begin
	for val in select c3 from t_proc loop
		insert into t_proc values(c,c, val) on duplicate key update c1 = $1, c2 =$1;
	end loop;
end;
/

call mul_ups(200);
select *from t_proc order by c3;

create or replace procedure mul_ups_00()
as
declare
val int;
begin
	for val in select c3 from t_proc loop
		insert into t_proc values(val,val,val) on duplicate key update c1 = 300, c2 =300;
	end loop;
end;
/

call mul_ups_00();
select *from t_proc order by c3;

declare
val int;
c int :=400;
begin
	for val in select c3 from t_proc loop
		insert into t_proc values(c,c, val) on duplicate key update c1 = c, c2 =c;
	end loop;
end;
/
select *from t_proc order by c3;

create or replace procedure mul_ups( c in INTEGER)
as
declare
val int;
begin
	for val in select c3 from t_proc loop
		insert into t_proc values(c,c, val+100) on duplicate key update c1 = $1, c2 =$1;
	end loop;
end;
/

call mul_ups(500);
select *from t_proc order by c3;

/*
 * insert
 */
drop table if exists t_default;
CREATE TABLE t_default (c1 INT PRIMARY KEY DEFAULT 10, c2 FLOAT DEFAULT 3.0, c3 TIMESTAMP DEFAULT '20200508');
create view v_default as select *from t_default order by c1;

-- insert src
---- insert values
truncate t_default;
insert into t_default values(DEFAULT, DEFAULT, DEFAULT);
insert into t_default values(1, 2, '20200630');
select *from t_default order by c1;

insert into t_default values(DEFAULT, DEFAULT, DEFAULT) on duplicate key update c2 = 1;
select *from t_default order by c1;

insert into t_default values(1, 2, '20200630') on duplicate key update c2 = 2;
select *from t_default order by c1;

insert into t_default values(DEFAULT, DEFAULT, DEFAULT),(1, 2, '20200630') on duplicate key update c2 = 3;
select *from t_default order by c1;

---- insert subquery
insert into t_default select *from t_default order by c1 on duplicate key update c2 = 4;
select *from t_default order by c1;

insert into t_default select *from t_default order by c1 limit 1 on duplicate key update c2 = 5;
select *from t_default order by c1;

insert into t_default select *from t_default where c1=10 on duplicate key update c2 = 6;
select *from t_default order by c1;

insert into t_default select *from t_default order by c1 on duplicate key update c2 = 7;
select *from t_default order by c1;

insert into t_default select *from v_default order by c1 on duplicate key update c2 = 8;
select *from t_default order by c1;

insert into  t_default select *from t_default union select c1+1,c2+1,c3+1 from t_default order by c1 on duplicate key update c2 = 9;
select *from t_default order by c1;

insert into  t_default(c1,c2) select max(c1),c2 from t_default group by c2 on duplicate key update c2 = 10;

-- index
---- mul index
truncate t_default;
create unique index t_default_mul on t_default(c1,c3);
insert into t_default values(DEFAULT, DEFAULT, DEFAULT);
insert into t_default values(1, 2, '20200630');
select *from t_default order by c1;

insert into t_default values(DEFAULT, DEFAULT, DEFAULT) on duplicate key update c2 = 1;
select *from t_default order by c1;

insert into t_default values(1, 2, '20200630') on duplicate key update c2 = 2;
select *from t_default order by c1;

insert into t_default values(DEFAULT, DEFAULT, DEFAULT),(1, 2, '20200630') on duplicate key update c2 = 3;
select *from t_default order by c1;

insert into t_default select *from t_default on duplicate key update c2 = 4;
select *from t_default order by c1;
