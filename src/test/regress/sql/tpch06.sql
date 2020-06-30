-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= '1994-01-01'::date 
	and l_shipdate < '1994-01-01'::date + interval '1 year'
	and l_discount >= 0.06 - 0.01
    and l_discount <= 0.06 + 0.01
	and l_quantity < 24;


