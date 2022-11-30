CREATE TABLE t11 (
sec_code character(6) NOT NULL,
issue_type character(3) NOT NULL
);

CREATE TABLE t22 (
company_code character(6) NOT NULL,
list_profit_flag character(1) NOT NULL
);

CREATE TABLE t33 (
company_code character(6) NOT NULL,
issue_flag character(6) NOT NULL
);

insert into t11 values(1,'S04');
insert into t22 values(1,'Y');
insert into t33 values(1,'1');
insert into t33 values(2,'2');

select
T5.issue_type
,T6.is_type
from
(
select T1.sec_code
,issue_type
--,case when list_profit_flag='Y' then '是' else '否' end as list_profit_flag
from
(
select sec_code
,case when issue_type in ('S04','S05','S06','S09') then '增发'
when issue_type in ('S01','S02') then '首发'
else '其他' end as issue_type
from t11
)T1
left join
(
select list_profit_flag
,company_code
from t22
)T2
on T1.sec_code=T2.company_code
)T5
full join
(
SELECT CASE
WHEN issue_flag = ANY (ARRAY['1'::bpchar, '3'::bpchar, '11'::bpchar]) THEN '首发'::text
ELSE '增发'::text
END AS is_type from t33
)T6
on T5.issue_type=T6.is_type
;

-- full join with non-equal condition
create table fulljointest(c1 int,c2 varchar2(20),c3 varchar2(20),c4 int);
insert into fulljointest values(1,'li','adjani',100);
insert into fulljointest values(2,'li','adjani',2000);
insert into fulljointest values(3,'li','adjani',5000);

set query_dop=1;

explain(costs off) select * from fulljointest t1 full join fulljointest t2 on case t2.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
join fulljointest t3 on case t3.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
order by 1,2,3,4;

select * from fulljointest t1 full join fulljointest t2 on case t2.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
join fulljointest t3 on case t3.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
order by 1,2,3,4;

set query_dop=4;
explain(costs off) select * from fulljointest t1 full join fulljointest t2 on case t2.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
join fulljointest t3 on case t3.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
order by 1,2,3,4;

select * from fulljointest t1 full join fulljointest t2 on case t2.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
join fulljointest t3 on case t3.c4 when 100 then 'low'
when 5000 then 'high'
when 2000 then 'medium'
end between 'high' and 'high'
order by 1,2,3,4;

--CTE test
create table cte_test(w_zip text);

create table cte_test2(w_name text);

create table cte_test3(d_id text);

with alias1 as (select w_zip alias2 from cte_test) select w_name from cte_test2 union select d_id from cte_test3 full join alias1 on cte_test3.d_id>alias1.alias2;

-- CTE test with smp
set query_dop = 4;
with alias1 as (select w_zip alias2 from cte_test) select w_name from cte_test2 union select d_id from cte_test3 full join alias1 on cte_test3.d_id>alias1.alias2;

drop table cte_test;
drop table cte_test2;
drop table cte_test3;