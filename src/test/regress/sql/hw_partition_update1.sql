--11--------------------------------------------------------------------
--create a before/after  update trigger on partitioned table , trigger insert a values to target table ,and cause a interval 
--partition been created , 
--the real update action will cause a row movement to created partition in the trigger .
create table update_table_trigger
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1)
(
	partition update_table_trigger_p0 values less than (10),
	partition update_table_trigger_p1 values less than (20),
	partition update_table_trigger_p2 values less than (50)
)enable row movement;
;
create index on update_table_trigger(c1,c2)local;
insert into update_table_trigger select generate_series(0, 49), generate_series(0, 49), generate_series(0, 49);

CREATE FUNCTION update_trigger() RETURNS trigger AS $update_trigger$
    BEGIN
       delete from update_table_trigger;
       raise NOTICE 'all rowe in update table been delete';
       RETURN NULL ;
    END;
$update_trigger$ LANGUAGE plpgsql;

create trigger update_trigger before update on update_table_trigger
 EXECUTE PROCEDURE update_trigger();



update update_table_trigger set c3 = 32 ;
select * from update_table_trigger order by 1, 2, 3;
--0 rows
drop trigger update_trigger on update_table_trigger;
drop function update_trigger();
drop table update_table_trigger;




create table update_table_trigger
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1)
(
	partition update_table_trigger_p0 values less than (10),
	partition update_table_trigger_p1 values less than (20),
	partition update_table_trigger_p2 values less than (50)
)enable row movement;
create index on update_table_trigger(c1,c2)local;
insert into update_table_trigger select generate_series(0, 49), generate_series(0, 49), generate_series(0, 49);

CREATE FUNCTION update_trigger() RETURNS trigger AS $update_trigger$
    BEGIN
       delete from update_table_trigger;
       raise NOTICE 'all rowe in update table been delete';
       RETURN NULL ;
    END;
$update_trigger$ LANGUAGE plpgsql;

create trigger update_trigger after update on update_table_trigger
 EXECUTE PROCEDURE update_trigger();



update update_table_trigger set c3 = 32 ;
select * from update_table_trigger order by 1, 2, 3;
--0 rows
drop trigger update_trigger on update_table_trigger;
drop function update_trigger();
drop table update_table_trigger;
--12--------------------------------------------------------------------
--syntax test ,with clause
create table update_table_trigger1
(
	c1 int ,
	c2 int 
);

insert into update_table_trigger1 select generate_series(0,9), generate_series(0,9);

create table update_table_trigger
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1 )
(
	partition update_table_trigger_p0 values less than (10),
	partition update_table_trigger_p1 values less than (20),
	partition update_table_trigger_p2 values less than (30)
)
;
create index on update_table_trigger(c1,c2)local;
insert into update_table_trigger select generate_series(0,29), generate_series(0,29), generate_series(0,29);
select * from update_table_trigger order by 1, 2, 3;


with tmp as (select c1 from update_table_trigger1)
update update_table_trigger set c2 = 0 where update_table_trigger.c1 in  (select u.c1 from tmp t , update_table_trigger u where t.c1 = u.c1);

select * from update_table_trigger order by 1, 2, 3;

drop table update_table_trigger;
drop table update_table_trigger1;
--13--------------------------------------------------------------------
--select for update


--14--------------------------------------------------------------------
--update lead to  constraint fail, use not null constraint 
--1、NOT DEFERRABLE
--2、DEFERRABLE  INITIALLY DEFERRED
--3、DEFERRABLE 	 INITIALLY IMMEDIATE

--15--------------------------------------------------------------------
--from clause ,   target relaiton in from cluase
--16--------------------------------------------------------------------
--give a large value to create interval partition
create table update_table_trigger
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition update_table_trigger_p0 values less than (50),
	partition update_table_trigger_p1 values less than (100),
	partition update_table_trigger_p2 values less than (150)
)enable row movement
;

create index on update_table_trigger(c1,c2)local;

insert into update_table_trigger values (1,1);

update update_table_trigger set c2 = 999999999 where c1 = 1;
--error   the sequnece number is too large for range table
select c2  from update_table_trigger;

drop table update_table_trigger;

--17--------------------------------------------------------------------
--update with from clause
create table update_table_trigger
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition update_table_trigger_p0 values less than (50),
	partition update_table_trigger_p1 values less than (100),
	partition update_table_trigger_p2 values less than (150)
)enable row movement
;
create table update_table_trigger1
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition update_table_trigger1_p0 values less than (50),
	partition update_table_trigger1_p1 values less than (100),
	partition update_table_trigger1_p2 values less than (150)
)enable row movement
;

create index on update_table_trigger(c1,c2)local;

insert into update_table_trigger select generate_series(144,146),generate_series(4,6);
insert into update_table_trigger1 select generate_series(144,146),generate_series(7,9);

select * from update_table_trigger order by 1, 2;
select * from update_table_trigger1 order by 1, 2;
select * from update_table_trigger, update_table_trigger1 order by 1, 2, 3, 4;
update update_table_trigger set c2 = update_table_trigger1.c2 from update_table_trigger1 where update_table_trigger.c1 = update_table_trigger1.c1;
select *  from update_table_trigger order by 1, 2;

drop table update_table_trigger;
drop table update_table_trigger1;

--18--------------------------------------------------------------------
--not null constraint
create table update_table_trigger
(
	c1 int ,
	c2 int not null,
	c3 int
)
partition by range (c1,c2)
(
	partition update_table_trigger_p0 values less than (50,0),
	partition update_table_trigger_p1 values less than (100,0),
	partition update_table_trigger_p2 values less than (150,0)
)enable row movement
;

create index on update_table_trigger(c1,c2)local;

insert into update_table_trigger values(99,null);
--error
insert into update_table_trigger values(99,0);

update update_table_trigger set c2 = null where c2  = 0;
--error 
drop table update_table_trigger;

--19--------------------------------------------------------------------
create table update_table_trigger
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition update_table_trigger_p0 values less than (10),
	partition update_table_trigger_p1 values less than (20),
	partition update_table_trigger_p2 values less than (30)
)disable row movement;

create index on update_table_trigger(c1,c2)local;
insert into update_table_trigger values (0, 5);
select * from update_table_trigger partition (update_table_trigger_p0);

--error
update update_table_trigger set c2=15 where c2=5;

alter table update_table_trigger enable row movement;

--success
update update_table_trigger set c2=15 where c2=5;
select * from update_table_trigger partition (update_table_trigger_p0);
select * from update_table_trigger partition (update_table_trigger_p1);

drop table update_table_trigger;


CREATE TABLE hw_partition_update_tt(c_id int NOT NULL,c_first varchar(16) NOT NULL,c_data varchar(500))
partition by range(c_id)
(
	partition hw_partition_update_tt_p1 values less than (11),
	partition hw_partition_update_tt_p2 values less than (31)
) ENABLE ROW MOVEMENT;
insert into hw_partition_update_tt values(1,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(3,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(5,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(7,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(10,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(15,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(18,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(24,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(28,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');

START TRANSACTION ISOLATION LEVEL READ COMMITTED;
UPDATE hw_partition_update_tt SET (c_first,c_data) = ('aaaaa','aaaaa') where c_id > 10 and c_id <= 20;
SELECT distinct c_first,c_data,count(c_first) FROM hw_partition_update_tt where c_id > 10 and c_id <= 20 GROUP BY c_first,c_data order by c_first;
-- /* skip the case for pgxc */
--SELECT RELNAME FROM PG_PARTITION WHERE PARENTID IN (SELECT OID FROM PG_CLASS WHERE RELNAME IN ('hw_partition_update_tt')) order by RELNAME;

--PRAGMA AUTONOMOUS_TRANSACTION;
--UPDATE hw_partition_update_tt SET (c_id,c_first,c_data) = (c_id + 10000,'aaaaa','aaaaa') where c_id > 20;
--SELECT RELNAME FROM PG_PARTITION WHERE PARENTID IN (SELECT OID FROM PG_CLASS WHERE RELNAME IN ('hw_partition_update_tt')) order by RELNAME;
--SELECT distinct c_first,c_data,count(c_first) FROM hw_partition_update_tt where c_id > 10000 GROUP BY c_first,c_data order by c_first;
--ROLLBACK;

SELECT distinct c_first,c_data,count(c_first) FROM hw_partition_update_tt where c_id > 10 and c_id <= 20 GROUP BY c_first,c_data order by c_first;
SELECT count(c_first) FROM hw_partition_update_tt;
COMMIT;

DROP TABLE hw_partition_update_tt;


create table test_update_rowmovement (a int, b int)
partition by range (b)
(
	partition test_update_rowmovement_p1 values less than (10)
) enable row movement;

insert into test_update_rowmovement values (-1, 5);

update test_update_rowmovement set b=16;

drop table test_update_rowmovement;

