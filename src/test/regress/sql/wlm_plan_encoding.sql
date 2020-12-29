-- 0. aiEngine
-- 0.1 illegal ip
select check_engine_status(' ',' ');
select check_engine_status('111','11');
select check_engine_status('11.11.11','11');
select check_engine_status('.11.11.11','11');
select check_engine_status('.11.11.11','11');
select check_engine_status('1111.11.11.11','11');
select check_engine_status('111.1111.11.11','11');
select check_engine_status('111.11.1111.11','11');
select check_engine_status('111.11.11.1111','11');

-- 0.2 illegal port
select check_engine_status('11.11.11.11','-1');
select check_engine_status('11.11.11.11','65537');
select check_engine_status('11.11.11.11','100000');
select check_engine_status('11.11.11.11','0');

-- 0.3 unreachable engine
select check_engine_status('11.11.11.11','11');

\c postgres
-- 1.TPCH Q1
-- 1.1 clean temporary tables
delete from gs_wlm_plan_encoding_table;
delete from gs_wlm_plan_operator_info;
select create_wlm_operator_info(0);

\c regression
-- 1.2 run query
select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)/1000) as sum_charge, --add /1000
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= date '1998-12-01' - interval '3 day'
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;

\c postgres
-- 1.3 collect temporary information to _info tables
select create_wlm_operator_info(1);

-- 1.4 show plan_operator_info
select * from gs_wlm_plan_operator_info order by queryid, plan_node_id;

-- 1.5 run plan encoding
select gather_encoding_info('regression');

-- 1.6 show plan encoding
select * from gs_wlm_plan_encoding_table;

-- 2.TPCH Q2
-- 2.1 clean temporary tables
delete from gs_wlm_plan_encoding_table;
delete from gs_wlm_plan_operator_info;
select create_wlm_operator_info(0);

\c regression
-- 2.2 run query
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 15
	and p_type like 'SMALL%'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE '
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'EUROPE '
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100
;

\c postgres
-- 2.3 collect temporary information to _info tables
select create_wlm_operator_info(1);

-- 2.4 show plan_operator_info
select * from gs_wlm_plan_operator_info order by queryid, plan_node_id;

-- 2.5 run plan encoding
select gather_encoding_info('regression');

-- 2.6 show plan encoding
select * from gs_wlm_plan_encoding_table;

-- 3.TPCH Q3
-- 3.1 clean temporary tables
delete from gs_wlm_plan_encoding_table;
delete from gs_wlm_plan_operator_info;
select create_wlm_operator_info(0);

\c regression
-- 3.2 run query
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15'::date
	and l_shipdate > '1995-03-15'::date
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10
;

\c postgres
-- 3.3 collect temporary information to _info tables
select create_wlm_operator_info(1);

-- 3.4 show plan_operator_info
select * from gs_wlm_plan_operator_info order by queryid, plan_node_id;

-- 3.5 run plan encoding
select gather_encoding_info('regression');

-- 3.6 show plan encoding
select * from gs_wlm_plan_encoding_table;

-- 4.TPCH Q4
-- 4.1 clean temporary tables
delete from gs_wlm_plan_encoding_table;
delete from gs_wlm_plan_operator_info;
select create_wlm_operator_info(0);

\c regression
-- 4.2 run query
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
	o_orderpriority;


\c postgres
-- 4.3 collect temporary information to _info tables
select create_wlm_operator_info(1);

-- 4.4 show plan_operator_info
select * from gs_wlm_plan_operator_info order by queryid, plan_node_id;

-- 4.5 run plan encoding
select gather_encoding_info('regression');

-- 4.6 show plan encoding
select * from gs_wlm_plan_encoding_table;

-- 5 model train

-- 5.1 train with model not existed in gs_opt_model 
delete from gs_opt_model;
select * from model_train_opt('void', 'void');
select * from model_train_opt('rlstm', 'void');

-- 5. invalid template_name
insert into gs_opt_model values('rlstm1', 'tmp_name', 'regression', '128.0.0.1', 5000, 0, 0, 0, 0, 0, 0, false, false, '{S, T}', '{0,0}', '{0,0}', 'Description');
select model_train_opt('rlstm1', 'tmp_name');

-- 5. train with false label targets
delete from gs_opt_model;
insert into gs_opt_model values('rlstm', 'rlstm_tmp', 'regression', '128.0.0.1', 5000, 1000, 1, -1, 50, 2000, 0, false, false, '{}', '{}', '{}', 'Description');
select model_train_opt('rlstm', 'rlstm_tmp');
update gs_opt_model set label = '{t}';
update gs_opt_model set label = '{T,T}';
select model_train_opt('rlstm', 'rlstm_tmp');

-- 5. positive cases: connection failure
update gs_opt_model set label = '{T}';
select model_train_opt('rlstm', 'rlstm_tmp');
update gs_opt_model set label = '{S,T}';
select model_train_opt('rlstm', 'rlstm_tmp');

-- 6 model predict
-- 6.1 predict with model not existed in gs_opt_model
delete from gs_opt_model;
\c regression
explain (analyze on, predictor rlstm_tmp)
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15'::date
	and l_shipdate > '1995-03-15'::date
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10
;

-- 6.2 wrong database
insert into gs_opt_model values('rlstm', 'rlstm_tmp', 'postgres', '128.0.0.1', 5000, 1000, 1, -1, 50, 2000, 0, false, false, '{S,T}', '{10000,10000}', '{10,10}', 'Description');
explain (analyze on, predictor rlstm_tmp)
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15'::date
	and l_shipdate > '1995-03-15'::date
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10
;

-- 6.3 connection failed
update gs_opt_model set datname = 'regression';
explain (analyze on, predictor rlstm_tmp)
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15'::date
	and l_shipdate > '1995-03-15'::date
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10
;

-- final cleanup
\c postgres
select create_wlm_operator_info(0);
select create_wlm_session_info(0);
drop database regression;
create database regression;