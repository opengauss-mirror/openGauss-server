set enable_global_stats = true;
-- $ID$
-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- Functional Query Definition
-- Approved February 1998
set current_schema=vector_engine;
set enable_nestloop=off;
select
        sum(l_extendedprice* (1 - l_discount)) as revenue
from
        lineitem,
        part
where
        (
                p_partkey = l_partkey
                and substring(p_brand, 1, 7) = 'Brand#1'
                and p_container in ('SM CASE', 'SM BOX ', 'SM PACK', 'SM PKG ')
                and l_quantity >= 1 and l_quantity <= 1 + 10
                and p_size between 1 and 5
                and l_shipmode in ('AIR    ', 'AIR REG')
                and substring(l_shipinstruct, 1, 7) = 'DELIVER'
        )
        or
        (
                p_partkey = l_partkey
                and substring(p_brand, 1, 7) = 'Brand#2'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 10 and l_quantity <= 10 + 10
                and p_size between 1 and 10
                and l_shipmode in ('AIR    ', 'AIR REG')
                and substring(l_shipinstruct, 1, 7) = 'DELIVER'
        )
        or
        (
                p_partkey = l_partkey
                and substring(p_brand, 1, 7) = 'Brand#3'
                and p_container in ('LG CASE', 'LG BOX ', 'LG PACK', 'LG PKG ')
                and l_quantity >= 20 and l_quantity <= 20 + 10
                and p_size between 1 and 15
                and l_shipmode in ('AIR    ', 'AIR REG')
                and substring(l_shipinstruct, 1, 7) = 'DELIVER'
        );

