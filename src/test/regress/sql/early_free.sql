set current_schema=vector_engine;
set enable_vector_engine=on;

-- turn on early free policy.
set enable_early_free=on;

-- tpch Q5, HashJoin
set enable_nestloop=off;
set enable_mergejoin=off;
set enable_hashjoin=on;

explain (costs off) select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= '1994-01-01'::date
	and o_orderdate < '1994-01-01'::date + interval '1 year'
group by
	n_name
order by
	revenue desc;


select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= '1994-01-01'::date
	and o_orderdate < '1994-01-01'::date + interval '1 year'
group by
	n_name
order by
	revenue desc;


-- tpch Q5, MergeJoin
set enable_nestloop=off;
set enable_mergejoin=on;
set enable_hashjoin=off;

explain (costs off) select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= '1994-01-01'::date
	and o_orderdate < '1994-01-01'::date + interval '1 year'
group by
	n_name
order by
	revenue desc;


select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= '1994-01-01'::date
	and o_orderdate < '1994-01-01'::date + interval '1 year'
group by
	n_name
order by
	revenue desc;


-- tpch Q5, NestLoop
set enable_nestloop=on;
set enable_mergejoin=off;
set enable_hashjoin=off;

explain (costs off) select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= '1994-01-01'::date
	and o_orderdate < '1994-01-01'::date + interval '1 year'
group by
	n_name
order by
	revenue desc;


select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= '1994-01-01'::date
	and o_orderdate < '1994-01-01'::date + interval '1 year'
group by
	n_name
order by
	revenue desc;
