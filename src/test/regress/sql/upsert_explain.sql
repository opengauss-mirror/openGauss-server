--------------------------------------------------------------------------------------------
/* 
 *  explain upsert
 */
--------------------------------------------------------------------------------------------
\c upsert;
SET CURRENT_SCHEMA TO upsert_test_explain;

create temp table up_expl_temp(c1 int, c2 int, c3 int unique) ;


insert into up_expl_hash select a,a,a from generate_series(1,20) as a;
insert into up_expl_repl select a,a,a from generate_series(1,20) as a;
insert into up_expl_part select a,a,a from generate_series(1,20) as a;
insert into up_expl_temp select a,a,a from generate_series(1,20) as a;
insert into up_expl_unlog select a,a,a from generate_series(1,20) as a;
insert into up_expl_node select a,a,a from generate_series(1,20) as a;
insert into up_expl_repl2 select a,a,a,a,a from generate_series(1,20) as a;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_hash values(1,1,1) on duplicate key update c1 = 2;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_hash values(1,1,1) on duplicate key update c1 = 2;
EXPLAIN PERFORMANCE insert into up_expl_hash values(1,1,1) on duplicate key update c1 = 2;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_hash values(1,1,1),(2,2,2)on duplicate key update c1 = 2;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_hash values(1,1,1),(2,2,2)on duplicate key update c1 = 2;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_repl values(1,1,1) on duplicate key update c1 = 2;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_repl values(1,1,1) on duplicate key update c1 = 2;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_part values(1,1,1) on duplicate key update c1 = 2;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_part values(1,1,1) on duplicate key update c1 = 2;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_temp values(1,1,1) on duplicate key update c1 = 2;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_temp values(1,1,1) on duplicate key update c1 = 2;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_unlog values(1,1,1) on duplicate key update c1 = 2;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_unlog values(1,1,1) on duplicate key update c1 = 2;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_node values(1,1,1) on duplicate key update c1 = 2;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_node values(1,1,1) on duplicate key update c1 = 2;


EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_hash values(1,1,1),(2,2,2)on duplicate key update nothing;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_hash values(1,1,1),(2,2,2)on duplicate key update nothing;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_repl values(1,1,1) on duplicate key update nothing;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_repl values(1,1,1) on duplicate key update nothing;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_part values(1,1,1) on duplicate key update nothing;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_part values(1,1,1) on duplicate key update nothing;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_temp values(1,1,1) on duplicate key update nothing;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_temp values(1,1,1) on duplicate key update nothing;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_unlog values(1,1,1) on duplicate key update nothing;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_unlog values(1,1,1) on duplicate key update nothing;

EXPLAIN (VERBOSE on, COSTS off, TIMING off)  insert into up_expl_node values(1,1,1) on duplicate key update nothing;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)  insert into up_expl_node values(1,1,1) on duplicate key update nothing;

