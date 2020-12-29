create schema distribute_setop_1;
set current_schema = distribute_setop_1;

-- case 1
create table test_union_1(a int, b int, c int) ;
create table test_union_2(a int, b int, c int) ;
explain (verbose on, costs off) select a, b from  test_union_1 union all select a, b from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 union all select b, c from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 union all select b, a from  test_union_2;
explain (verbose on, costs off) select b, a from  test_union_1 union all select b, a from  test_union_2;
explain (verbose on, costs off) select b, c from  test_union_1 union all select b, c from  test_union_2;

explain (verbose on, costs off) select a, b from  test_union_1 intersect select a, b from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 intersect select b, c from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 intersect select b, a from  test_union_2;
explain (verbose on, costs off) select b, a from  test_union_1 intersect select b, a from  test_union_2;
explain (verbose on, costs off) select b, c from  test_union_1 intersect select b, c from  test_union_2;

explain (verbose on, costs off) select a, b from  test_union_1 minus select a, b from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 minus select b, c from  test_union_2;
explain (verbose on, costs off) select a, b from  test_union_1 minus select b, a from  test_union_2;
explain (verbose on, costs off) select b, a from  test_union_1 minus select b, a from  test_union_2;
explain (verbose on, costs off) select b, c from  test_union_1 minus select b, c from  test_union_2;
explain (verbose on, costs off) select b, substr(c, 1, 3), c from  test_union_1 minus (select 1, t2.b::varchar(10), t1.c from (select a,b,case c when 1 then 1 else null end as c from test_union_2 where b<0) t1 right join test_union_2 t2 on t1.b=t2.c group by 1, 2, 3);

explain (verbose on, costs off) SELECT b,a,c FROM test_union_1 INTERSECT (((SELECT a,b,c FROM test_union_1 UNION ALL SELECT a,b,c FROM test_union_1)));

-- case 2
create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer
)
;
create table store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_manager                 varchar(40)                   ,
	s_market_id               integer                       ,
    s_company_id              integer
)
;
create table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_city                   varchar(60)
)
;
create table item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_class_id                integer
)
;
explain (verbose on, costs off)
select s_market_id
          from store
         inner join item
            on i_class_id = s_company_id
         where s_manager like '%a%'
         group by 1
        union
        select 1
          from call_center
         where cc_city like '%b%'
        union all 
        select count(*) from income_band;

-- Setop targetlist doesn't match subplan's targetlist
explain (verbose on, costs off)
select '',  s_market_id,
octet_length('abc') + s_market_id%4 - (array_length(array [ '', ' ', 'fgsg' ], 1) - 32.09)   from store
inner join item
on i_class_id = s_company_id
and (array_length(array [ '', ' ', 'fgsg' ], 1) - 32.09 < 0)
and octet_length('abc') %8 > 0
where s_manager like '%a%'
group by 1, i_class_id, s_market_id
union select 'dalf', bit_length(''), 12.009
from call_center
where cc_city like '%b%'
union all select '123', count(*), lengthb(749275)
from income_band order by 1, 2, 3;

reset current_schema;
drop schema distribute_setop_1;
