create schema aggregate;
set current_schema='aggregate';

create table test_aggregate(c1 int, c2 NUMBER(8,2), c3 varchar(20), c4 timestamp);

insert into test_aggregate values(1,0.1,'1','2024-09-01 09:22:00'),
(2,0.2,'2','2024-09-02 09:22:00'),
(3,0.1,'3','2024-09-03 09:22:00'),
(3,0.2,'3','2024-09-04 09:22:00'),
(3,0.3,'3','2024-09-05 09:22:00'),
(3,0.3,'3','2024-09-05 09:22:00'),
(4,0.2,'4','2024-09-06 09:22:00'),
(5,0.2,'5','2024-09-07 09:22:00'),
(6,0.2,'6','2024-09-08 09:22:00'),
(7,0.2,'7','2024-09-09 09:22:00'),
(8,0.2,'8','2024-09-10 09:22:00'),
(9,0.2,'9','2024-09-11 09:22:00'),
(10,0.2,'10','2024-09-12 09:22:00');

--cume_dist 
select cume_dist(3,0.2) within group (order by c1,c2) from test_aggregate;

--percent_rank
select percent_rank(3,0.2) within group (order by c1,c2) from test_aggregate;

--dense_rank
select dense_rank(4,0.2) within group (order by c1,c2) from test_aggregate;

--rank
select rank(4,0.2) within group (order by c1,c2) from test_aggregate;

-- divide by zero check
select percent_rank(0) within group (order by x) from generate_series(1,0) x;

--error, The number of parameters does not match
select cume_dist(3,0.2) within group (order by c1) from test_aggregate;
select cume_dist(3) within group (order by c1,c2) from test_aggregate;

-- error, ordered-set aggs can't use ungrouped vars in direct args:
select rank(x) within group (order by x) from generate_series(1,5) x;


-- enable_aggr_coerce_type = off,  type conversion test

--error
select cume_dist(3) within group (order by c3) from test_aggregate;

--error 
select cume_dist('a') within group (order by c1) from test_aggregate;

--error 
select cume_dist('2024') within group (order by c4) from test_aggregate;

--success
select cume_dist('2024-12-12') within group (order by c4) from test_aggregate;

--success
select cume_dist('1') within group (order by c1) from test_aggregate;

--error boolean
select rank(1) within group (order by x) from (values (true),(false)) v(x);

-- enable_aggr_coerce_type = on,  type conversion test

set enable_aggr_coerce_type = on;
--success
select cume_dist(3) within group (order by c3) from test_aggregate;

--error
select cume_dist('a') within group (order by c1) from test_aggregate;

--error
select cume_dist('2024') within group (order by c4) from test_aggregate;

--success
select cume_dist('2024-12-12') within group (order by c4) from test_aggregate;

--success
select cume_dist('1') within group (order by c1) from test_aggregate;

--sucdess boolean
select rank(1) within group (order by x) from (values (true),(false)) v(x);


set enable_aggr_coerce_type = off;
drop table test_aggregate;
drop schema aggregate CASCADE;
