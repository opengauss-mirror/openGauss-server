set current_schema=vector_engine;
set enable_vector_engine=on;
set enable_nestloop=off;
explain (costs off) select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= '1993-07-01'::date
	and o_orderdate < '1993-07-01'::date + interval '3 month'
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
        order_count;

select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= '1993-07-01'::date
	and o_orderdate < '1993-07-01'::date + interval '3 month'
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
        order_count;
