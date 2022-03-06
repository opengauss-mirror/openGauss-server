/* 
 *  upsert negative test cases
 *
	table cases
		col table
		ORC table
		VIEW
                MAT view
	index
		DEFERABLE index
	insert stmt
		returning
		with
		with recur
	update stmt
		VALUES with expr
		update distribute key
		update primary key
		update unique key
		update partition key
		where clause
		with clause
		sub query
 */
--------------------------------------------------------------------------------------------
\c upsert
SET CURRENT_SCHEMA TO upsert_test_etc;

insert into up_neg_04 select a,a,a from generate_series(1,20) as a;
insert into up_neg_05 select a,a,a,a,a from generate_series(1,20) as a;
insert into up_neg_06 select a,a,a from generate_series(1,20) as a;
insert into up_neg_07 select a,a,a from generate_series(1,20) as a;
insert into up_neg_08 select a,a,a from generate_series(1,20) as a;
insert into up_neg_09 select a,a,a from generate_series(1,20) as a;
insert into up_neg_10 select a,a,a from generate_series(1,20) as a;
insert into up_neg_11 values(1,1),(2,2),(11,11),(12,12);

-- table cases
---- col table
insert into up_neg_01 values(1,2,3) on duplicate key update c3 = 1;

---- ORC table
insert into up_neg_02 values(1,2,3) on duplicate key update c3 = 1;

---- VIEW
insert into up_view values(1) on duplicate key update c3 = 1;

-- matview
refresh materialized view mat_view;
insert into mat_view values(1) on duplicate key update c3 = 1;

-- index
---- DEFERABLE index
insert into up_neg_03 values(1,2,3) on duplicate key update c1 = 1;
insert into up_neg_03 values(1) on duplicate key update c1 = 1;

-- insert stmt
----returning
insert into up_neg_04 values(1,1,1) on duplicate key update c1 = 1 returning c1;
----with
with sub as (select *from up_neg_04)
insert into up_neg_04 select *from sub on duplicate key update c1 =1;

----with recur
with RECURSIVE  sub as (select *from up_neg_04)
insert into up_neg_04 select *from sub on duplicate key update c1 =1;

with sub as (select *from up_neg_04)
insert into up_neg_04 select *from sub on duplicate key update c1 =1 returning c1;

-- update stmt
---- VALUES with expr
insert into up_neg_05 values(1,1,1,1) on duplicate key update c3 = values(1+100);
insert into up_neg_05 values(1,1,1,1) on duplicate key update c3 = values(100);

insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c1 = 1;
---- update primary key
insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c5 = 1;
---- update unique key
insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c4 = 1;
insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c2 = 1, c3=2;
insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c2 = 1;
insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c3 = 2;
insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c1 =1, c2 = 1, c3=2, c4=1,c5=1;

---- from clause
insert into up_neg_05 values(1,1,1,1,1) on duplicate key update c4 = 1 from up_neg_04 where c1=1;

---- update partition key
insert into up_neg_07 values(101, 1, 300) on duplicate key update c3=101;
insert into up_neg_08 values(101, 1, 300) on duplicate key update c3=101;
insert into up_neg_09 values(101, 1, 1) on duplicate key update c3=101;
insert into up_neg_09 values(101, 1, 1) on duplicate key update c2=101;
insert into up_neg_09 values(101, 1, 1) on duplicate key update c2 =1,c3=101;

---- cross-partition upsert
insert into up_neg_11 values(1, 1) on duplicate key update c1 = 15;
select * from up_neg_11 partition(p1);
select * from up_neg_11 partition(p2);

--update unique key mul type plans
EXPLAIN (VERBOSE on, COSTS off) insert into up_neg_10 values(101, 1, 300) on duplicate key update c1=101;
insert into up_neg_10 values(101, 1, 300) on duplicate key update c3=101;

--trigger
create table upsert_base
(c1 int primary key,
 c2 varchar(100), --record the type
 cold varchar(1024),
 cnew varchar(1024) );
create table upsert_tri
(c1 int,
 c2 varchar(100),
 c3 int,
 unique(c1,c3)
);

--S1 after trigger
CREATE OR REPLACE FUNCTION trig_after() RETURNS TRIGGER AS $emp_audit$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            insert into upsert_base(c2,cnew) values('after insert',new.c2);
            RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
            insert into upsert_base(c2,cold,cnew) values('after update',old.c2,new.c2);
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$emp_audit$ LANGUAGE plpgsql;
CREATE TRIGGER tri_after
AFTER UPDATE OR INSERT ON upsert_tri
    FOR EACH ROW EXECUTE PROCEDURE trig_after();
--S2 before trigger
CREATE OR REPLACE FUNCTION trig_before() RETURNS TRIGGER AS $emp_audit$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            insert into upsert_base(c2,cnew) values('before insert',new.c2);
            RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
            insert into upsert_base(c2,cold,cnew) values('before update',old.c2,new.c2);
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
$emp_audit$ LANGUAGE plpgsql;
CREATE TRIGGER tri_before
BEFORE UPDATE OR INSERT ON upsert_tri
    FOR EACH ROW EXECUTE PROCEDURE trig_before();
---- upsert-insert
insert into upsert_tri(c1,c2,c3) values(1000,'abc',1) on duplicate key update nothing;
insert into upsert_tri(c1,c2,c3) values(2000,'abc',1) on duplicate key update c2='bcd';
---- upsert-nothing
insert into upsert_tri(c1,c2,c3) values(1000,'abc',1) on duplicate key update nothing;
---- upsert-update
insert into upsert_tri(c1,c2,c3) values(2000,'abc',1) on duplicate key update c2='bcd';
---- check results
SELECT * from upsert_tri;
SELECT * from upsert_base;

-- foreign key
insert into pkt values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);
insert into fkt values(1,1,1),(2,2,2),(3,3,3);

insert into fkt values(1,1,1),(2,2,2),(3,3,3) on duplicate key update b=excluded.b+1, c=0;
insert into fkt values(1,1,1),(2,2,2),(3,3,3) on duplicate key update b=excluded.b+3, c=-1;
insert into fkt values(1,8,1),(2,9,2),(3,3,3),(4,9,4) on duplicate key update b=excluded.b+3, c=-1;
