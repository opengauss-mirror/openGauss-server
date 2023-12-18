DROP SCHEMA IF EXISTS result_node_reuse;

CREATE SCHEMA result_node_reuse;
SET search_path = 'result_node_reuse';

-- PREPARE BEGIN
create table t1(a int, b text);
-- PREPARE END;
-- dont need bypass
SET enable_opfusion = off;

explain insert into t1 values(1, md5(1::text));
prepare p_insert as insert into t1 values($1, md5($2::text));

execute p_insert(1, 10);
execute p_insert(2, 20);
execute p_insert(3, 30);
execute p_insert(4, 40);
execute p_insert(5, 50);
execute p_insert(6, 60);
execute p_insert(7, 70);
execute p_insert(8, 80);
execute p_insert(9, 90);

DROP SCHEMA IF EXISTS result_node_reuse CASCADE;
