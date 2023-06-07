replace into replace_test values(1,'aaaaa','aaaaa');

create database db_replaceinto dbcompatibility='B';
\c db_replaceinto

CREATE TABLE replace_test(
    id int NOT NULL,
    name varchar(20) DEFAULT NULL,
	key varchar(20) DEFAULT NULL,
    PRIMARY KEY(id)
);
create unique index reidx on replace_test(key);

create or replace function before_tri() returns trigger as $$
begin
    if TG_OP = 'INSERT' then
        raise notice 'before insert trigger';
    end if;
    if TG_OP = 'DELETE' then
        raise notice 'before delete trigger';
    end if;
    return new;
end $$ language plpgsql;

create trigger replace_before before insert or delete on
replace_test for each row execute procedure before_tri();


create or replace function after_tri() returns trigger as $$
begin
    if TG_OP = 'INSERT' then
        raise notice 'after insert trigger';
    end if;
    if TG_OP = 'DELETE' then
        raise notice 'after delete trigger';
    end if;
    return new;
end $$ language plpgsql;

create trigger replace_after after insert or delete on
replace_test for each row execute procedure after_tri();

select * from replace_test;
replace into replace_test values(1,'aaaaa','aaaaa');
select * from replace_test;
replace into replace_test values(2,'bbbbb','bbbbb');
select * from replace_test;
replace into replace_test values(3,'ccccc','ccccc'),(4,'ddddd','ddddd'),(5,'eeeee','eeeee');
select * from replace_test;
replace into replace_test values(1,'aaaaa','ddddd');
select * from replace_test;
replace into replace_test values(3,'ccccc','ccccc'),(3,'ddddd','ccccc'),(3,'eeeee','eeeee');
select * from replace_test;
explain analyze REPLACE INTO replace_test values(2,'aaaa','bbbb');
explain performance REPLACE INTO replace_test values(2,'ddddd','fffff');
truncate replace_test;

select * from replace_test;
replace into replace_test set id=1,name='aaaaa', key='bbbbb';
select * from replace_test;
replace into replace_test set id=2,name=default, key='bbbbb';
select * from replace_test;
replace into replace_test values(3,default,'ccccc'),(4,'ddddd','ddddd'),(5,'eeeee','eeeee');
select * from replace_test;
replace into replace_test values(1,'aaaaa','ddddd');
select * from replace_test;
replace into replace_test select * from replace_test;
select * from replace_test;
replace into replace_test set id=1,name = (select name from replace_test where id=1);
drop table replace_test;

CREATE TABLE replace_partition_test(
    id int NOT NULL,
    name varchar(20) DEFAULT NULL,
	key varchar(20) DEFAULT NULL,
    PRIMARY KEY(id)
)
partition by range(id)
(
	partition replace_p1 values less than (5),
	partition replace_p2 values less than (10),
	partition replace_p3 values less than (20)
);
create unique index repartidx on replace_partition_test(key);


replace into replace_partition_test values(1,'aaaaa','aaaaa');
replace into replace_partition_test values(6,'bbbbb','bbbbb');
replace into replace_partition_test values(15,'ccccc','ccccc');
select * from replace_partition_test;
replace into replace_partition_test values(7,'ddddd','ddddd');
select * from replace_partition_test;
replace into replace_partition_test values(7,'ddddd','aaaaa');
select * from replace_partition_test;
select * from replace_partition_test partition(replace_p1);
select * from replace_partition_test partition(replace_p2);
drop table replace_partition_test;

create table replace_test1(a serial primary key, b int);

replace into replace_test1 values(1,2);

select * from replace_test1;

drop table replace_test1;

create table replace_test2(id int, name char(20));

replace into replace_test2 values(1,'aaaaa');
replace into replace_test2 values(1,'aaaaa');
replace into replace_test2 values(2,'bbbbb');
select * from replace_test2;

create or replace procedure  replace_p() as 
begin
replace into replace_test2 values(1,'ccccc');
end;
/

call replace_p();
select * from replace_test2;
drop table replace_test2;

create table t_range_list
( id number PRIMARY KEY not null,
partition_key int,
subpartition_key int,
col2 varchar2(10) )
partition by range(partition_key)
subpartition by list(subpartition_key)(
partition p1 values less than (100)
(subpartition sub_1_1 values (10),
subpartition sub_1_2 values (20)),
partition p2 values less than(200)
(subpartition sub_2_1 values (10),
subpartition sub_2_2 values (20)),
partition p3 values less than (300)
(subpartition sub_3_1 values (10),
subpartition sub_3_2 values (20)));
REPLACE INTO t_range_list VALUES(1,50,10,'sub_1_1');
REPLACE INTO t_range_list VALUES(2,150,20,'sub_2_2');
REPLACE INTO t_range_list VALUES(3,250,10,'sub_3_1');

select * from t_range_list partition(p1);
select * from t_range_list partition(p2);
select * from t_range_list partition(p3);

CREATE TABLE t1(id int,p int,sub int);
INSERT INTO t1 VALUES(2,60,10);

select * from t_range_list;
REPLACE INTO t_range_list VALUES(1,210,20,'sub_3_2');
REPLACE INTO t_range_list(id,partition_key,subpartition_key )  SELECT * FROM t1;
REPLACE INTO t_range_list SET id=3,partition_key=190,subpartition_key=20,col2='sub_2_2';
select * from t_range_list;

REPLACE INTO t_range_list partition(p2) VALUES(1,50,10,'fail');
select * from t_range_list;
REPLACE INTO t_range_list subpartition(sub_2_1) VALUES(2,150,20,'fail');
select * from t_range_list;
REPLACE INTO t_range_list subpartition(sub_3_1) VALUES(3,250,10,'new');
select * from t_range_list;

drop table t_range_list;
drop table t1;

CREATE TABLE ustore_test (col1 int PRIMARY KEY, col2 INT) with(storage_type=ustore);
REPLACE INTO ustore_test values(1,2);
REPLACE INTO ustore_test values(2,3);
REPLACE INTO ustore_test values(3,4);
REPLACE INTO ustore_test values(4,5);
select * from ustore_test;
REPLACE INTO ustore_test values(1,5);
REPLACE INTO ustore_test values(2,6);
select * from ustore_test;

drop table ustore_test;

CREATE TABLE replace_segment_test(
    id int NOT NULL,
    name varchar(20) DEFAULT NULL,
        key varchar(20) DEFAULT NULL,
    PRIMARY KEY(id)
)with (parallel_workers=10, SEGMENT=ON)
partition by range(id)
(
        partition replace_p1 values less than (5),
        partition replace_p2 values less than (10),
        partition replace_p3 values less than (20)
);
create unique index repartidx on replace_segment_test(key);
replace into replace_segment_test values(1,'aaaaa','aaaaa');
replace into replace_segment_test values(6,'bbbbb','bbbbb');
replace into replace_segment_test values(15,'ccccc','ccccc');
select * from replace_segment_test;
replace into replace_segment_test values(7,'ddddd','ddddd');
select * from replace_segment_test;

drop table replace_segment_test;
\c postgres
drop database db_replaceinto;
