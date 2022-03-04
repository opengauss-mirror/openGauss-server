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

insert into interval_tab1 values(1,'2020-4-7 2:0:0',  1, 1);

insert into interval_tab1 values(1,'2020-4-8 2:0:0',  1, 1);

select relname, boundaries from pg_partition order by 1,2;

select * from interval_tab1 where logdate < '2020-4-7 0:0:0' order by 1,2,3,4;

explain (costs off, verbose on) select * from interval_tab1 where logdate < '2020-4-7 0:0:0';

select * from interval_tab1 where logdate > '2020-4-6' order by 1,2,3,4;

explain (costs off, verbose on) select * from interval_tab1 where logdate > '2020-4-6';

select * from interval_tab1 where logdate = '2020-4-7 2:0:0' order by 1,2,3,4;

insert into interval_tab1 values(1,'2020-4-7 0:0:0',  1, 1);

select * from interval_tab1 where logdate = '2020-4-7 0:0:0' order by 1,2,3,4;

select * from interval_tab1 where logdate != '2020-4-7 0:0:0' order by 1,2,3,4;

select * from interval_tab1 where logdate >= '2020-4-7 0:0:0' order by 1,2,3,4;

insert into interval_tab1 values(1,'2020-4-5 2:0:0',  1, 1);

select relname, boundaries from pg_partition order by 1,2;

insert into interval_tab1 values(1,'2020-4-9 0:0:0',  1, 1);


select * from interval_tab1 where logdate >= '2020-4-7 0:0:0' and logdate < '2020-4-9 0:0:0' order by 1,2,3,4;

select * from interval_tab1 where logdate > '2020-4-7 0:0:0' and logdate <= '2020-4-9 0:0:0' order by 1,2,3,4;

select * from interval_tab1 where logdate >= '2020-4-7 0:0:0' and logdate <= '2020-4-9 0:0:0' order by 1,2,3,4;

select * from interval_tab1 where logdate > '2020-4-6 0:0:0' and logdate <= '2020-4-9 0:0:0' order by 1,2,3,4;

explain (costs off, verbose on)  select * from interval_tab1 where logdate >= '2020-4-10 0:0:0';

drop table interval_tab1;
