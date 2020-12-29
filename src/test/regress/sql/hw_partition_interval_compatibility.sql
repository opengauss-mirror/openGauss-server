-- the date tyte of the compatible B/C/PG version is different from the A compatible version

create database td_db dbcompatibility 'C';
\c td_db

CREATE TABLE interval_tab1 (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) 
PARTITION BY RANGE (logdate)
INTERVAL ('1 day')
(
	PARTITION p1 VALUES LESS THAN (('2020-03-01'))
);

select relname, parttype, partstrategy, boundaries from pg_partition;

insert into interval_tab1 values(1,'2020-2-29',  1, 1);
insert into interval_tab1 values(1,'2020-3-1',  1, 1);
insert into interval_tab1 values(1,'2020-3-1 02:00:00',  1, 1);
insert into interval_tab1 values(1,'2020-3-3',  1, 1);
insert into interval_tab1 values(1,'2020-3-4',  1, 1);
insert into interval_tab1 values(1,'2020-3-5',  1, 1);
insert into interval_tab1 values(1,'2020-3-6',  1, 1);
insert into interval_tab1 values(1,'2020-3-7',  1, 1);
insert into interval_tab1 values(1,'2020-4-2',  1, 1);


select relname, parttype, partstrategy, boundaries from pg_partition;

select * from interval_tab1 where logdate >= '2020-3-1' and logdate < '2020-3-7';

explain (costs off, verbose on) select * from interval_tab1 where logdate >= '2020-3-1' and logdate < '2020-3-7';

alter table interval_tab1 drop partition p1;

select relname, parttype, partstrategy, boundaries from pg_partition;

alter table interval_tab1 merge partitions sys_p5, sys_p6 into partition sys_p5_p6;

select relname, parttype, partstrategy, boundaries from pg_partition;

drop table interval_tab1;

CREATE TABLE interval_tab1 (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) 
PARTITION BY RANGE (logdate)
INTERVAL ('2 day')
(
	PARTITION p1 VALUES LESS THAN (('2020-03-01'))
);

insert into interval_tab1 values(1,'2020-2-29',  1, 1);
insert into interval_tab1 values(1,'2020-3-1',  1, 1);
insert into interval_tab1 values(1,'2020-3-1 02:00:00',  1, 1);
insert into interval_tab1 values(1,'2020-3-2',  1, 1);
insert into interval_tab1 values(1,'2020-4-2',  1, 1);

select relname, parttype, partstrategy, boundaries from pg_partition;

select * from interval_tab1 partition(sys_p1);

alter table interval_tab1 split partition sys_p1 at (to_date('2020-03-02', 'YYYY-MM-DD')) into (partition sys_p1_1, partition sys_p1_2);

select relname, parttype, partstrategy, boundaries from pg_partition;

drop table interval_tab1;

CREATE TABLE interval_tab1 (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) 
PARTITION BY RANGE (logdate)
INTERVAL ('1 hour')
(
	PARTITION p1 VALUES LESS THAN (('2020-03-01'))
);

\c regression
drop database td_db;
