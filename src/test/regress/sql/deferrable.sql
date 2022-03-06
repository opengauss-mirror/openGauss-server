DROP SCHEMA test_deferrable CASCADE;
CREATE SCHEMA test_deferrable;
SET CURRENT_SCHEMA TO test_deferrable;

-- partition table for deferrable
drop table t_kenyon;
create table t_kenyon(id int primary key deferrable)
partition by range(id)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (6000)
)ENABLE ROW MOVEMENT;
insert into t_kenyon values(1);
begin;
set constraints all deferred;
insert into t_kenyon values(1);
end;

begin;
set constraints all IMMEDIATE;
insert into t_kenyon values(1);
end;

drop table t_kenyon;
create table t_kenyon(id int primary key not deferrable )
partition by range(id)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (6000)
)ENABLE ROW MOVEMENT;
insert into t_kenyon values(1);
begin;
set constraints all deferred;
insert into t_kenyon values(1);
end;

begin;
set constraints all IMMEDIATE;
insert into t_kenyon values(1);
end;

drop table t_kenyon;
create table t_kenyon(id int primary key initially immediate )
partition by range(id)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (6000)
)ENABLE ROW MOVEMENT;
insert into t_kenyon values(1);
begin;
set constraints all deferred;
insert into t_kenyon values(1);
end;

begin;
set constraints all IMMEDIATE;
insert into t_kenyon values(1);
end;

drop table t_kenyon;
create table t_kenyon(id int primary key  initially deferred)
partition by range(id)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (6000)
)ENABLE ROW MOVEMENT;
insert into t_kenyon values(1);
begin;
set constraints all deferred;
insert into t_kenyon values(1);
end;

begin;
set constraints all IMMEDIATE;
insert into t_kenyon values(1);
end;


-- foreign key for deferrable
drop table warehouse_t23;
drop table city_t23;
CREATE TABLE city_t23
(
	W_CITY VARCHAR(60) PRIMARY KEY,
	W_ADDRESS TEXT
);
CREATE TABLE warehouse_t23
(
	W_INT int,
	W_CITY VARCHAR(60) ,
	FOREIGN KEY(W_CITY) REFERENCES city_t23(W_CITY) deferrable
);
begin;
set constraints all deferred;
insert into warehouse_t23 values(1,'sss');
end;

begin;
set constraints all IMMEDIATE;
insert into warehouse_t23 values(1,'sss');
end;

drop table warehouse_t23;
drop table city_t23;
CREATE TABLE city_t23
(
	W_CITY VARCHAR(60) PRIMARY KEY,
	W_ADDRESS TEXT
);
CREATE TABLE warehouse_t23
(
	W_INT int,
	W_CITY VARCHAR(60) ,
	FOREIGN KEY(W_CITY) REFERENCES city_t23(W_CITY) not deferrable
);
begin;
set constraints all deferred;
insert into warehouse_t23 values(1,'sss');
end;

begin;
set constraints all IMMEDIATE;
insert into warehouse_t23 values(1,'sss');
end;

drop table warehouse_t23;
drop table city_t23;
CREATE TABLE city_t23
(
	W_CITY VARCHAR(60) PRIMARY KEY,
	W_ADDRESS TEXT
);
CREATE TABLE warehouse_t23
(
	W_INT int,
	W_CITY VARCHAR(60) ,
	FOREIGN KEY(W_CITY) REFERENCES city_t23(W_CITY) initially immediate
);
begin;
set constraints all deferred;
insert into warehouse_t23 values(1,'sss');
end;

begin;
set constraints all IMMEDIATE;
insert into warehouse_t23 values(1,'sss');
end;

drop table warehouse_t23;
drop table city_t23;
CREATE TABLE city_t23
(
	W_CITY VARCHAR(60) PRIMARY KEY,
	W_ADDRESS TEXT
);
CREATE TABLE warehouse_t23
(
	W_INT int,
	W_CITY VARCHAR(60) ,
	FOREIGN KEY(W_CITY) REFERENCES city_t23(W_CITY) initially deferred
);
begin;
set constraints all deferred;
insert into warehouse_t23 values(1,'sss');
end;

begin;
set constraints all IMMEDIATE;
insert into warehouse_t23 values(1,'sss');
end;

DROP SCHEMA test_deferrable CASCADE;
