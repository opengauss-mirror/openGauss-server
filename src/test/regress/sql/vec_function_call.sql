-- FunctionCallInfo initialization in vector operators and scalar wrappers.
\set ON_ERROR_STOP on
\set VERBOSITY terse
BEGIN;
CREATE SCHEMA vec_function_call;
SET LOCAL current_schema = vec_function_call;
SET LOCAL client_min_messages = error;
SET LOCAL query_dop = 1;
SET LOCAL statement_timeout = '60s';

CREATE TABLE sample(a varchar, b int) WITH (orientation=column);
INSERT INTO sample VALUES ('x',1),('y',2);
CREATE TABLE batch_data(id int, a varchar, c varchar, n numeric, v float8,
    iv interval, tz timetz) WITH (orientation=column);
-- A partition and peer group span batch boundaries.
INSERT INTO batch_data
SELECT i, CASE WHEN i<=2050 THEN 'x' WHEN i<=4000 THEN 'y' ELSE NULL END,
    CASE WHEN i%11=0 THEN NULL ELSE 'k'||(i%257)::text||repeat('z',80) END,
    CASE WHEN i%13=0 THEN NULL ELSE i::numeric/8 END,
    CASE WHEN i%13=0 THEN NULL ELSE (i%16)::float8 END,
    interval '1 hour'*(i%9), '12:00:00+08'::timetz+interval '1 minute'*(i%9)
FROM generate_series(1,4098) i;
CREATE FUNCTION check_case(label text, query_sql text, plan_node text,
    options text DEFAULT '') RETURNS text AS $$
DECLARE
    plan_line record;
    plan_text text := '';
    different boolean;
    setting text;
BEGIN
    EXECUTE 'SET LOCAL enable_vector_engine=on';
    EXECUTE 'SET LOCAL try_vector_engine_strategy=force';
    EXECUTE 'SET LOCAL enable_codegen=off';
    EXECUTE 'SET LOCAL enable_hashagg=on';
    EXECUTE 'SET LOCAL enable_sort=on';
    EXECUTE 'SET LOCAL enable_nestloop=off';
    EXECUTE 'SET LOCAL enable_mergejoin=off';
    EXECUTE 'SET LOCAL enable_sonic_hashjoin=off';
    EXECUTE 'SET LOCAL enable_sonic_hashagg=off';
    EXECUTE 'SET LOCAL hashagg_table_size=100000';
    EXECUTE 'SET LOCAL work_mem=''4MB''';
    IF length(options)>0 THEN
        FOREACH setting IN ARRAY string_to_array(options,';') LOOP
            IF length(btrim(setting))>0 THEN
                EXECUTE setting;
            END IF;
        END LOOP;
    END IF;
    FOR plan_line IN EXECUTE 'EXPLAIN (COSTS OFF) '||query_sql LOOP
        plan_text := plan_text||plan_line."QUERY PLAN"||E'\n';
    END LOOP;
    IF plan_text !~ plan_node THEN
        RAISE EXCEPTION '%: required plan % missing (sonic=%); actual plan: %',
            label,plan_node,current_setting('enable_sonic_hashagg'),plan_text;
    END IF;
    EXECUTE 'CREATE TEMP TABLE v_result AS '||query_sql;
    EXECUTE 'SET LOCAL try_vector_engine_strategy=off';
    EXECUTE 'SET LOCAL enable_vector_engine=off';
    EXECUTE 'CREATE TEMP TABLE r_result AS '||query_sql;
    EXECUTE 'SELECT EXISTS((TABLE v_result EXCEPT ALL TABLE r_result)
        UNION ALL (TABLE r_result EXCEPT ALL TABLE v_result))' INTO different;
    IF different THEN
        RAISE EXCEPTION '%: vector/row results differ',label;
    END IF;
    DROP TABLE v_result, r_result;
    RETURN label||': PASS';
END;
$$ LANGUAGE plpgsql;

-- Issue #8650: default RANGE frame and a string partition key.
SELECT check_case('window minimal',
    'SELECT a,count(*) OVER(PARTITION BY a ORDER BY b) FROM sample','Vector WindowAgg');
SELECT check_case('window cross-batch / NULL / aggregates',
    'SELECT id,count(*) OVER(PARTITION BY a ORDER BY id),
      sum(n) OVER(PARTITION BY a ORDER BY id),avg(v) OVER(PARTITION BY a ORDER BY id),
      max(n) OVER(PARTITION BY a ORDER BY id)
     FROM batch_data','Vector WindowAgg');
SELECT check_case('window string peers / multikey',
    'SELECT id,rank() OVER(PARTITION BY a ORDER BY c COLLATE "C"),
      count(*) OVER(PARTITION BY a,c ORDER BY id)
     FROM batch_data','Vector WindowAgg');

-- Build/probe hashes and non-integer equality, including NULLs.
SELECT check_case('hashjoin',
    'SELECT l.id,r.b FROM batch_data l JOIN sample r ON l.a=r.a','Vector Hash Join');
SELECT check_case('hashjoin multikey / spill',
    'SELECT count(*),sum(l.id::bigint+r.id) FROM batch_data l JOIN batch_data r
     ON l.a=r.a AND l.c=r.c','Vector Hash Join','SET LOCAL work_mem=''64kB''');
SELECT check_case('hashjoin left',
    'SELECT l.id,r.b FROM batch_data l LEFT JOIN sample r ON l.a=r.a','Vector Hash (Left|Right) Join');
CREATE VIEW grouped AS SELECT a,c,count(*) AS cnt,
    sum(id) AS si,avg(id) AS ai,stddev_samp(id) AS sdi,
    sum(id::bigint) AS sb,avg(id::bigint) AS ab,stddev_samp(id::bigint) AS sdb,
    sum(v) AS sv,avg(v) AS av,stddev_samp(v) AS sdv,
    sum(v::float4) AS sf,avg(v::float4) AS af,
    sum(n) AS sn,avg(n) AS an,stddev_samp(n) AS sdn,min(n) AS mn,max(n) AS mx
FROM batch_data GROUP BY a,c;
SELECT check_case('ordinary hashagg / numeric primitives','SELECT * FROM grouped',
    'Vector Hash Aggregate','SET LOCAL enable_sort=off');
SELECT check_case('sonic hashagg / numeric primitives / spill',
    'SELECT a,c,count(*),sum(id) AS si,avg(id) AS ai,sum(n) AS sn,avg(n) AS an
     FROM batch_data GROUP BY a,c','Vector Sonic Hash Aggregate',
    'SET LOCAL enable_sonic_hashagg=on; SET LOCAL enable_sort=off; SET LOCAL work_mem=''64kB''');
SELECT check_case('sortagg / interval / timetz',
    'SELECT a,c,avg(iv),min(tz),max(tz) FROM batch_data GROUP BY a,c',
    'Vector Sort Aggregate','SET LOCAL enable_hashagg=off');
SELECT check_case('grouping sets / phase collation',
    'SELECT a COLLATE "C",c COLLATE "default",count(*) FROM batch_data
     GROUP BY GROUPING SETS((a COLLATE "C"),(c COLLATE "default"),
         (a COLLATE "C",c COLLATE "default"),())','Vector Sort Aggregate');
SELECT check_case('general hash / interval',
    'SELECT iv,count(*) FROM batch_data GROUP BY iv','Vector Sonic Hash Aggregate',
    'SET LOCAL enable_sonic_hashagg=on; SET LOCAL enable_sort=off');
SELECT check_case('shared hash / setop',
    'SELECT a FROM batch_data EXCEPT SELECT a FROM sample','Vector HashSetOp',
    'SET LOCAL enable_sort=off');

SELECT check_case('scalar wrappers / selection / generic / row comparison',
    'SELECT id,n+n AS nadd,n*n AS nmul,n/3 AS ndiv,
      v+v AS fadd,v*v AS fmul,v/3 AS fdiv,least(n,n+1) AS mn,greatest(a,c) AS mx,
      a::char(4) AS cast_a,btrim(c) AS bt,ltrim(c) AS lt,rtrim(c) AS rt,
      (a,id)<(''y'',3000) AS row_cmp,id=ANY(ARRAY[1,4,7]) AS any_cmp,
      md5(a) AS digest,pg_typeof(n)::text AS type_name,pg_backend_pid()>0 AS has_pid,
      CASE WHEN id%2=0 THEN btrim(a)::char(3) ELSE ltrim(a)::char(3) END AS selected_text,
      CASE WHEN id%2=0 THEN n/7 ELSE NULL END AS selected_n
     FROM batch_data WHERE id%3<>0',
    'CStore Scan');

ROLLBACK;
