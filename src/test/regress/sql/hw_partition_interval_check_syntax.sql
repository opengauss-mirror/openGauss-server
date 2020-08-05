create table partiton_table_001(
COL_1 bigint,
COL_2 TIMESTAMP WITHOUT TIME ZONE,
COL_3 bool,
COL_4 decimal
)
PARTITION BY RANGE (COL_4)
INTERVAL (10000)
(
PARTITION partiton_table_001_p1 VALUES LESS THAN (1000)
);

create table partiton_table_001(
COL_1 bigint,
COL_2 TIMESTAMP WITHOUT TIME ZONE,
COL_3 bool,
COL_4 decimal
)
PARTITION BY RANGE (COL_2)
INTERVAL ('2018-1-1')
(
PARTITION partiton_table_001_p1 VALUES LESS THAN ('2020-03-01')
);
