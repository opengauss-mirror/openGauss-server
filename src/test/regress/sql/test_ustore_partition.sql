drop table if exists ustore_part;
drop table if exists ustore_part2;
drop table if exists ustore_part3;

CREATE TABLE ustore_part
(
  a int,
  b int,
  c int,
  d int
) with (storage_type=USTORE)
PARTITION BY RANGE (b, c)
(
  PARTITION P_050_BEFORE VALUES LESS THAN (100, 100),
  PARTITION P_100 VALUES LESS THAN (100, 200),
  PARTITION P_150 VALUES LESS THAN (200, 100),
  PARTITION P_200 VALUES LESS THAN (200, 200)
);

insert into ustore_part(b, c) values(1, 2),(2, 3);
select * from ustore_part where b < 2;

begin;
insert into ustore_part values (1, 0, 2, 0), (2, 3, 4, 5) returning *;
update ustore_part set b = 50 where b < 2;
select * from ustore_part order by 1, 2, 3, 4;
select * from ustore_part where b < 2;
delete from ustore_part where b > 40 returning *;
select * from ustore_part;
truncate ustore_part;
select * from ustore_part;
rollback;

select * from ustore_part;

-- \set filepath '''' || :v1  '/dest/single_node/pg_copydir/parttest.dat'''
-- COPY ustore_part FROM :filepath;
-- select count(*) from ustore_part;

-- truncate ustore_part;
-- select * from ustore_part;

drop table ustore_part;

-- check null inserts

create table ustore_part (a text, b text) with (storage_type=USTORE)
partition by range (b)
(
        partition part_p1 values less than ('B'),
        partition part_p2 values less than ('D'),
        partition part_p3 values less than ('F')
);

insert into ustore_part(b) values (lpad('A',409600,'A'));

-- verify functionalities with inplaceheap alignment

insert into ustore_part values (lpad('A',409600,'A'), lpad('A',409600,'A'));

create table ustore_part2
(
c_smallint smallint,
c_integer integer,
c_bigint bigint
) with (storage_type=USTORE)
partition by range (c_integer)
(
partition part2_1  values less than (100),
partition part2_2  values less than (1000)
);
insert into ustore_part2 values(10, generate_series(999, 0, -1), 100);

create table ustore_part3
(
c_smallint text,
c_integer integer,
c_bigint bigint
) with (storage_type=USTORE)
partition by range (c_integer)
(
partition part3_1  values less than (100),
partition part3_2  values less than (1000)
);
insert into ustore_part3 values('asdf', generate_series(999, 0, -1), 100);
create index ind on ustore_part3(c_integer) local;

drop table ustore_part;
drop table ustore_part2;
drop table ustore_part3;

-- test cluster and vacuum for partition table

create table ustore_part(c1 int) with (storage_type=USTORE) 
partition by range(c1)
(
partition p1 values less than(3),
partition p2 values less than(7)
);

insert into ustore_part values(1),(3),(2),(5),(4),(6);
create index ind1 on ustore_part(c1) local;
select * from ustore_part;

cluster ustore_part using ind1;
select * from ustore_part;

delete from ustore_part where c1 < 2;
-- vacuum full ustore_part;
select * from ustore_part;

delete from ustore_part where c1 > 5;
vacuum ustore_part;
select * from ustore_part;

drop table ustore_part;

-- test cluster and vacuum for partition table with toast values

create table ustore_part (a text, b text) with (storage_type=USTORE)
partition by range (b)
(
        partition part_p1 values less than ('B'),
        partition part_p2 values less than ('D'),
        partition part_p3 values less than ('F')
);

insert into ustore_part(b) values (lpad('A',409600,'A'));

create index ind2 on ustore_part(a) local;
cluster ustore_part using ind2;
drop index ind2;

insert into ustore_part values (lpad('A',409600,'A'), lpad('A',409600,'A'));
vacuum ustore_part;

insert into ustore_part values (lpad('B',409600,'B'), lpad('C',409600,'C'));
-- vacuum full ustore_part;

drop table ustore_part;

-- test upsert for partition table in ustore
create table ustore_part(c1 int, c2 int, c3 int) with (storage_type=USTORE) 
partition by range(c1)
(
partition p1 values less than(3),
partition p2 values less than(7),
partition p3 values less than(10)
);
CREATE UNIQUE INDEX ustore_part_u2 ON ustore_part (c2) TABLESPACE pg_default;
ALTER TABLE ustore_part ADD CONSTRAINT ustore_part_pk PRIMARY KEY (c1);
INSERT INTO ustore_part values(1, 10) on duplicate key update c3 = 10;
INSERT INTO ustore_part values(1, 11) on duplicate key update c3 = 10;
drop table ustore_part;