create table fulljoin_test(w_zip text);

create table fulljoin_test2(w_name varchar(20),w_tax int,w_street_2 varchar(50));

create table fulljoin_test3(d_id int);

create table fulljoin_test4(w_id int,w_ytd numeric(6,2));

SELECT
    fulljoin_test3.d_id
FROM
(
    SELECT
        alias2.w_name alias6 ,
        alias2.w_tax alias7,
        MOD(fulljoin_test4.w_id,
        fulljoin_test4.w_ytd + 10) alias8
    FROM
        fulljoin_test alias1
    FULL JOIN fulljoin_test2 alias2 ON
        alias1.w_zip = alias2.w_street_2,
        fulljoin_test4)alias9
FULL JOIN fulljoin_test3 ON
    alias9.alias7 != fulljoin_test3.d_id
WHERE
    alias9.alias8 = 2
    OR alias9.alias7 = 2;

with alias11 as(select 1 alias1 from fulljoin_test),
alias25 as(select * from (with alias19 as (select rownum from fulljoin_test2)select * from alias19)alias18)
select * from alias11,alias25 full join fulljoin_test3 on 1=1 full join fulljoin_test4 on 1=1 where 1=1;

explain (costs off) with alias11 as(select 1 alias1 from fulljoin_test),
alias25 as(select * from (with alias19 as (select rownum from fulljoin_test2)select * from alias19)alias18)
select * from alias11,alias25 full join fulljoin_test3 on 1=1 full join fulljoin_test4 on 1=1 where 1=1;

drop table fulljoin_test;

drop table fulljoin_test2;

drop table fulljoin_test3;

drop table fulljoin_test4;

create table fulltest(col int4 primary key ,w_col numeric(6,2));
create table fulltest2(w_id int4 primary key,w_ytd numeric(6,2),w_zip bpchar(27));
explain (verbose, costs off) select left(alias8.w_zip ,alias8.w_id) as alias10,true alias11,dense_rank() over(order by 1) alias12 from fulltest alias7 full join fulltest2 as alias8  on alias7.col!=alias8.w_ytd group by alias7.col,alias7.w_col ,alias8.w_id;
select left(alias8.w_zip ,alias8.w_id) as alias10,true alias11,dense_rank() over(order by 1) alias12 from fulltest alias7 full join fulltest2 as alias8  on alias7.col!=alias8.w_ytd group by alias7.col,alias7.w_col ,alias8.w_id;

drop table fulltest;
drop table fulltest2;

-- contain system column, don't rewrite full join
explain (costs off) select t1.oid from pg_class t1 full join pg_constraint t2 on t1.relname = t2.conname;

