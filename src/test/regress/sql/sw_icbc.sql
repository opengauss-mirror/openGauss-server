set client_min_messages = error;
set search_path=swtest;
SET CLIENT_ENCODING='UTF8';

--signle table columns test
explain (costs off)
select * from t1 start with id = 1 connect by prior id = pid;
select * from t1 start with id = 1 connect by prior id = pid;

explain (costs off)
select * from t1 start with t1.id = 1 connect by prior t1.id = t1.pid;
select * from t1 start with t1.id = 1 connect by prior t1.id = t1.pid;

explain (costs off)
select * from t1 as test start with test.id = 1 connect by prior test.id = test.pid;
select * from t1 as test start with test.id = 1 connect by prior test.id = test.pid;

explain (costs off)
select * from t1 start with id = 1 connect by prior id = pid order by id desc;
select * from t1 start with id = 1 connect by prior id = pid order by id desc;

explain (costs off)
select * from t1 start with id IN (select id from t2 where id = 1) connect by prior id = pid order by id desc;
select * from t1 start with id IN (select id from t2 where id = 1) connect by prior id = pid order by id desc;

explain (costs off) select t1.id, t1.pid, t1.name from t1 start with id = 1 connect by prior id = pid;
select t1.id, t1.pid, t1.name from t1 start with id = 1 connect by prior id = pid;

explain (costs off) select sum(name) from t1 start with id = 1 connect by prior id = pid group by id, pid;
select sum(name) from t1 start with id = 1 connect by prior id = pid group by id, pid;

explain (costs off) select * from t1 start with id = 1 connect by prior id = pid and id IN (select id from t2);
select * from t1 start with id = 1 connect by prior id = pid and id IN (select id from t2);

explain (costs off) select * from t1 start with id = 1 and id is not NULL connect by prior id = pid;
select * from t1 start with id = 1 and id is not NULL connect by prior id = pid;

explain (costs off)
select *
from
(select t1.id id, t1.pid pid, t1.name name from t1 
 union
 select t1.id id, t1.pid pid, t1.name name from t1) as test
start with test.id = 1
connect by prior test.id = test.pid;

select *
from
(select t1.id id, t1.pid pid, t1.name name from t1
 union
 select t1.id id, t1.pid pid, t1.name name from t1) as test
start with test.id = 1
connect by prior test.id = test.pid;

explain (costs off)
select *
from
(select * 
    from(select t1.id id, t1.pid pid, t1.name name from t1 
         union
         select t1.id id, t1.pid pid, t1.name name from t1) as test
    start with test.id = 1
    connect by prior test.id = test.pid) as tt
CONNECT BY PRIOR tt.id = tt.pid
START WITH tt.id = 1;

select *
from
(select *
    from(select t1.id id, t1.pid pid, t1.name name from t1
         union
         select t1.id id, t1.pid pid, t1.name name from t1) as test
    start with test.id = 1
    connect by prior test.id = test.pid) as tt
CONNECT BY PRIOR tt.id = tt.pid
START WITH tt.id = 1;

--test correlated sublink in targetlist
explain select b.id, (select count(a.id) from t1 a where a.pid = b.id) c from t1 b
start with b.id=1 connect by prior b.id = b.pid;

explain select * from t1 as test
where not exists (select 1 from t1 where test.id = t1.id)
start with test.id = 1 connect by prior test.id = test.pid;

--multiple tables case
explain (costs off) select * from t1, t2 where t1.id = t2.id start with t1.id = t2.id and t1.id = 1 connect by prior t1.id = t1.pid;
explain (costs off) select * from t1 join t2 on t1.id = t2.id start with t1.id = t2.id and t1.id = 1 connect by prior t1.id = t1.pid;
explain (costs off) select * from t1, (select * from t2) as test where t1.id = test.id start with t1.id = test.id and t1.id = 1 connect by prior t1.id = t1.pid;

explain (costs off) select id, (select id from t2 start with t2.id = t1.id connect by t2.id = t1.id limit 1) from t1 where id = 1;

--unsupport case
select prior id cc from t1 start with id = 1 connect by prior id = pid;

create INCREMENTAL MATERIALIZED view mv as select * from t1 start with id=141 connect by prior id=pid;
