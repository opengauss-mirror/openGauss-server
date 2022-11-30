create database rightref with dbcompatibility 'B';
\c rightref

-- test fields order
create table test_order_t(n1 int default 100, n2 int default 100, s int);
insert into test_order_t values(1000, 1000, n1 + n2);
insert into test_order_t(s, n1, n2) values(n1 + n2, 300,  300);
select * from test_order_t;
drop table test_order_t;

-- test non-idempotent function
create table non_idempotent_t(c1 float, c2 float, c3 float);
insert into non_idempotent_t values(random(), c1, c1);
select c1 = c2 as f1, c1 = c3 as f2 from non_idempotent_t;
drop table non_idempotent_t;

-- test auto increment
create table auto_increment_t(n int, c1 int primary key auto_increment, c2 int, c3 int);
insert into auto_increment_t values(1, c1, c1, c1);
insert into auto_increment_t values(2, 0, c1, c1);
insert into auto_increment_t values(3, 0, c1, c1);
insert into auto_increment_t values(4, -1, c1, c1);
insert into auto_increment_t(n, c2, c3, c1) values(5, c1, c1, 1000);
insert into auto_increment_t values(5, c1, c1, c1);
select * from auto_increment_t order by n;
drop table auto_increment_t;

-- test series
create table test_series_t(c1 int, c2 int, c3 int);
insert into test_series_t values(c2 + 10, generate_series(1, 10), c2 * 2);
select * from test_series_t;
drop table test_series_t;

-- test upsert
-- 1
create table upser(c1 int, c2 int, c3 int);
create unique index idx_upser_c1 on upser(c1);
insert into upser values (1, 10, 10), (2, 10, 10), (3, 10, 10), (4, 10, 10), (5, 10, 10), (6, 10, 10), (7, 10, 10),
                         (8, 10, 10), (9, 10, 10), (10, 10, 10);
insert into upser values (5, 100, 100), (6, 100, 100), (7, 100, 100), (8, 100, 100), (9, 100, 100), (10, 100, 100),
                         (11, 100, 100), (12, 100, 100), (13, 100, 100), (14, 100, 100), (15, 100, 100)
    on duplicate key update c2 = 2000, c3 = 2000;
select * from upser order by c1;

-- 2
truncate upser;
insert into upser values (1, 10, 10), (2, 10, 10), (3, 10, 10), (4, 10, 10), (5, 10, 10), (6, 10, 10), (7, 10, 10),
                         (8, 10, 10), (9, 10, 10), (10, 10, 10);
insert into upser values (5, 100, 100), (6, 100, 100), (7, 100, 100), (8, 100, 100), (9, 100, 100), (10, 100, 100),
                         (11, 100, 100), (12, 100, 100), (13, 100, 100), (14, 100, 100), (15, 100, 100)
                         on duplicate key update c2 = c1 + c2, c3 = c2 + c3;
select * from upser order by c1;

-- 3
truncate upser;
insert into upser values (1, 10, 10), (2, 10, 10), (3, 10, 10), (4, 10, 10), (5, 10, 10), (6, 10, 10),
                         (7, 10, 10), (8, 10, 10), (9, 10, 10), (10, 10, 10);

insert into upser values (5, c1 + 100, 100), (6, c1 + 100, 100), (7, c1 + 100, 100), (8, c1 + 100, 100),
                         (9, c1 + 100, 100), (10, c1 + 100, 100), (11, c1 + 100, 100), (12, c1 + 100, 100),
                         (13, c1 + 100, 100), (14, c1 + 100, 100), (15, c1 + 100, c1 + c2)
                         on duplicate key update c2 = c1 + c2, c3 = c2 + c3;

select * from upser order by c1;

drop table upser;

\c postgres

drop database rightref;
