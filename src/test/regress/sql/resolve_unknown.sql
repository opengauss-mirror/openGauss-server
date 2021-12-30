explain (verbose on, costs off) select pg_typeof('vasya'), 'vasya' is of (unknown);
select pg_typeof('vasya'), 'vasya' is of (unknown);

explain (verbose on, costs off) select 1 where 'vasya' = (select 'vasya' as col1);
explain (verbose on, costs off) select 1 where 'vasya' = (select 'vasya' as col1 where col1 <> 'aaa');
select 1 where 'vasya' = (select 'vasya' as col1);
select 1 where 'vasya' = (select 'vasya' as col1 where col1 <> 'aaa');

explain (verbose on, costs off) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) order by col1;
explain (verbose on, costs off) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) group by col1;
explain (verbose on, costs off) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) group by col1 having col1 <> 'aaa';
explain (verbose on, costs off) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) where col1 <> 'aaa';
select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) order by col1;
select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) group by col1;
select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) group by col1 having col1 <> 'aaa';
select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1) where col1 <> 'aaa';

explain (verbose on, costs off) with t as materialized (select 'vasya' as col1) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from t where substr(col1, 1, 2) <> 'a';
explain (verbose on, costs off) with t as materialized (select 'vasya' as col1) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from t where col1 <> 'aaa';
with t as (select 'vasya' as col1) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from t where substr(col1, 1, 2) <> 'a';
with t as (select 'vasya' as col1) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from t where col1 <> 'aaa';

explain (verbose on, costs off) select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1 union all select 'vasya1' as col order by 1) where col1 <> 'aaa';
select pg_typeof(col1), col1, col1 is of (unknown), col1 is of (text) from (select 'vasya' as col1 union all select 'vasya1' as col order by 1) where col1 <> 'aaa';

CREATE SCHEMA resolve_unknown;
SET search_path to resolve_unknown;
CREATE TABLE normal_table_count1
(
    sc character varying,
    tb character varying,
    cnt bigint,
    daate text,
    ssize character varying,
    pretty_size character varying
)
WITH (orientation=column, compression=low)
;

CREATE TABLE cities1
(
    city character varying(80),
    location1 integer
)
WITH (orientation=column, compression=low)
;

TRUNCATE normal_table_count1;

INSERT INTO normal_table_count1(tb,cnt)
SELECT
    'cities1' as tablename,
    count(*) as cnt
FROM cities1;

INSERT INTO normal_table_count1(tb,cnt) VALUES ('beijing', 1);
INSERT INTO normal_table_count1(tb,cnt) VALUES ('shenzhen', 2), ('shanghai', 3);
INSERT INTO normal_table_count1(tb,cnt) VALUES ('suzhou', 4), ('hangzhou', 5) ORDER BY 2 LIMIT 1;

TABLE normal_table_count1 ORDER BY cnt;

CREATE TABLE test_unknown(a int, b unknown, c int);
INSERT INTO test_unknown(a, b) VALUES (1, 'beijing');
INSERT INTO test_unknown(a, b) VALUES (1, 'beijing') LIMIT 1;
INSERT INTO test_unknown(a, b) VALUES (2, 'shenzhen'),  (3, 'shanghai');
INSERT INTO test_unknown(a, b) VALUES (4, 'suzhou'),  (5, 'hangzhou') ORDER BY 1 LIMIT 1;
INSERT INTO test_unknown(a, b) SELECT 6, 'xian';
TABLE test_unknown ORDER BY a;

TRUNCATE test_unknown;
INSERT INTO test_unknown(a, b) VALUES (1, 'beijing');
INSERT INTO test_unknown(a, b) VALUES (1, 'beijing') LIMIT 1;
INSERT INTO test_unknown(a, b) VALUES (2, 'shenzhen'),  (3, 'shanghai');
INSERT INTO test_unknown(a, b) VALUES (4, 'suzhou'),  (5, 'hangzhou') ORDER BY 1 LIMIT 1;
INSERT INTO test_unknown(a, b) SELECT 6, 'xian';

TABLE test_unknown ORDER BY a;

DROP SCHEMA resolve_unknown CASCADE;
