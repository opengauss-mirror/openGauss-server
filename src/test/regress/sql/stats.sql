--
-- Test Statistics Collector
--

CREATE SCHEMA sch_stats;
SET search_path = sch_stats;

CREATE TABLE t_stats (
  unique1 int4,
  unique2 int4,
  two int4,
  four  int4,
  ten int4,
  twenty  int4,
  hundred int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous int4,
  odd int4,
  even  int4,
  stringu1  name,
  stringu2  name,
  string4 name
) with(autovacuum_enabled = off);

CREATE INDEX i_t_stats ON t_stats USING btree(unique1 int4_ops);

insert into t_stats select a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a from generate_series(1,100,1) as t(a);

-- conditio sine qua non
SHOW track_counts;  -- must be on

-- ensure that both seqscan and indexscan plans are allowed
SET enable_seqscan TO on;
SET enable_indexscan TO on;
-- for the moment, we don't want index-only scans here
SET enable_indexonlyscan TO off;
-- wait to let any prior tests finish dumping out stats;
-- else our messages might get lost due to contention
SELECT pg_sleep(1.0);
-- reset counters
SELECT pg_stat_reset_single_table_counters('t_stats'::regclass);
SELECT pg_stat_reset_single_table_counters('i_t_stats'::regclass);
SELECT pg_sleep(2.0);


-- save counters
CREATE TEMP TABLE prevstats AS
SELECT t.seq_scan, t.seq_tup_read, t.idx_scan, t.idx_tup_fetch,
       (b.heap_blks_read + b.heap_blks_hit) AS heap_blks,
       (b.idx_blks_read + b.idx_blks_hit) AS idx_blks
  FROM pg_catalog.pg_stat_user_tables AS t,
       pg_catalog.pg_statio_user_tables AS b
 WHERE t.relid='t_stats'::regclass AND b.relid='t_stats'::regclass;
SELECT * from prevstats;

-- function to wait for counters to advance
create function wait_for_stats() returns void as $$
declare
  start_time timestamptz := clock_timestamp();
  updated bool;
begin
  -- we don't want to wait forever; loop will exit after 10 seconds
  for i in 1 .. 100 loop

    -- check to see if indexscan has been sensed
    SELECT (st.idx_scan >= pr.idx_scan + 1) INTO updated
      FROM pg_stat_user_tables AS st, pg_class AS cl, prevstats AS pr
     WHERE st.relid='t_stats'::regclass AND cl.oid='t_stats'::regclass;

    exit when updated;

    -- wait a little
    perform pg_sleep(0.1);

    -- reset stats snapshot so we can test again
    perform pg_stat_clear_snapshot();

  end loop;

  -- report time waited in postmaster log (where it won't change test output)
  raise log 'wait_for_stats delayed % seconds',
    extract(epoch from clock_timestamp() - start_time);
end
$$ language plpgsql;

-- do a seqscan
SELECT count(*) FROM t_stats;
-- do an indexscan
-- make sure it is not a bitmap scan, which might skip fetching heap tuples
SET enable_bitmapscan TO off;
SELECT count(*) FROM t_stats WHERE unique1 = 1;
RESET enable_bitmapscan;

-- force the rate-limiting logic in pgstat_report_tabstat() to time out
-- and send a message
SELECT pg_sleep(1.0);

-- wait for stats collector to update
SELECT wait_for_stats();

-- check effects
SELECT st.seq_scan >= pr.seq_scan + 1,
       st.seq_tup_read >= pr.seq_tup_read + cl.reltuples,
       st.idx_scan >= pr.idx_scan + 1,
       st.idx_tup_fetch >= pr.idx_tup_fetch + 1
  FROM pg_stat_user_tables AS st, pg_class AS cl, prevstats AS pr
 WHERE st.relid='t_stats'::regclass AND cl.oid='t_stats'::regclass;
SELECT st.heap_blks_read + st.heap_blks_hit >= pr.heap_blks + cl.relpages,
       st.idx_blks_read + st.idx_blks_hit >= pr.idx_blks + 1
  FROM pg_statio_user_tables AS st, pg_class AS cl, prevstats AS pr
 WHERE st.relid='t_stats'::regclass AND cl.oid='t_stats'::regclass;

-- check estimation on a whole var
EXPLAIN (COSTS OFF) SELECT count(*) FROM (SELECT t_stats, unique2 FROM t_stats ORDER BY unique2) t1, t_stats t2 WHERE t1.unique2=t2.unique1 AND t1=(1,1,1,1,1,1,1,1,1,1,1,1,1,'abc','abc','abc',1);



-- pg_stat_get_lastscan, pg_stat_force_next_flush
SELECT proname,pronamespace,proowner,prolang,procost,prorows,provariadic,prokind,prosecdef,
  proleakproof,proisstrict,proretset,provolatile,pronargs,pronargdefaults,prorettype,proargtypes,
  proallargtypes,proargmodes,proargnames,proargdefaults,prosrc,probin,proconfig,proacl
FROM pg_proc WHERE proname='pg_stat_get_lastscan' OR proname='pg_stat_force_next_flush'
ORDER BY proname;


-----
-- Test that last_seq_scan, last_idx_scan are correctly maintained
--
-- Perform test using a temporary table. That way autovacuum etc won't
-- interfere. To be able to check that timestamps increase, we sleep for 100ms
-- between tests, assuming that there aren't systems with a coarser timestamp
-- granularity.
-----

CREATE TEMPORARY TABLE test_check(id Oid, test_last_seq timestamptz, test_last_idx timestamptz);
BEGIN;
CREATE TEMPORARY TABLE test_last_scan(idx_col int primary key, noidx_col int);
INSERT INTO test_last_scan(idx_col, noidx_col) VALUES(1, 1);

-- partition table
CREATE TABLE test_last_scan_part(idx_col int primary key, noidx_col int)
PARTITION BY RANGE (idx_col)
(PARTITION p1 values less than (10), PARTITION p2 values less than (20));
INSERT INTO test_last_scan_part(idx_col, noidx_col) VALUES(1, 1);
INSERT INTO test_last_scan_part(idx_col, noidx_col) VALUES(12, 12);

-- subpartition table
CREATE TABLE test_last_scan_subpart(idx_col int primary key, noidx_col int)
PARTITION BY RANGE (idx_col) SUBPARTITION BY LIST (idx_col)
(PARTITION p1 values less than (10) (SUBPARTITION sp1 VALUES (1), SUBPARTITION sp2 VALUES (default)),
PARTITION p2 values less than (20) (SUBPARTITION sp3 VALUES (10, 11), SUBPARTITION sp4 VALUES (default)));
INSERT INTO test_last_scan_subpart(idx_col, noidx_col) VALUES(1, 1);
INSERT INTO test_last_scan_subpart(idx_col, noidx_col) VALUES(2, 2);
INSERT INTO test_last_scan_subpart(idx_col, noidx_col) VALUES(11, 11);
INSERT INTO test_last_scan_subpart(idx_col, noidx_col) VALUES(12, 12);

SELECT pg_stat_force_next_flush();
SELECT last_seq_scan, last_idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;
SELECT last_seq_scan, last_idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan_part'::regclass;
SELECT last_seq_scan, last_idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan_subpart'::regclass;
COMMIT;

SELECT pg_stat_reset_single_table_counters('test_last_scan'::regclass);
SELECT pg_stat_reset_single_table_counters('test_last_scan_part'::regclass);
SELECT pg_stat_reset_single_table_counters('test_last_scan_subpart'::regclass);
SELECT pg_sleep(1);
SELECT seq_scan, idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;
SELECT seq_scan, idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan_part'::regclass;
SELECT seq_scan, idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan_subpart'::regclass;

-- ensure we start out with exactly one index and sequential scan
BEGIN;
SET LOCAL enable_seqscan TO on;
SET LOCAL enable_indexscan TO on;
SET LOCAL enable_bitmapscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;
SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;
SET LOCAL enable_seqscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan WHERE idx_col = 1;

SET LOCAL enable_seqscan TO on;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_part WHERE noidx_col = 1 or noidx_col = 12;
SELECT count(*) FROM test_last_scan_part WHERE noidx_col = 1 or noidx_col = 12;
SET LOCAL enable_seqscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_part WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan_part WHERE idx_col = 1;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_part WHERE idx_col = 12;
SELECT count(*) FROM test_last_scan_part WHERE idx_col = 12;

SET LOCAL enable_seqscan TO on;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_subpart WHERE noidx_col = 1 or noidx_col = 12;
SELECT count(*) FROM test_last_scan_subpart WHERE noidx_col = 1 or noidx_col = 12;
SET LOCAL enable_seqscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 2;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 12;
SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 12;
SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 11;

SELECT pg_stat_force_next_flush();
COMMIT;
SELECT pg_sleep(1);

SELECT seq_scan, idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;
SELECT seq_scan, idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan_part'::regclass;
SELECT seq_scan, idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan_subpart'::regclass;

-- fetch timestamps from before the next test
INSERT INTO test_check SELECT relid, last_seq_scan, last_idx_scan
FROM pg_stat_all_tables WHERE relid in ('test_last_scan'::regclass, 'test_last_scan_part'::regclass, 'test_last_scan_subpart'::regclass);
SELECT pg_sleep(0.1); -- assume a minimum timestamp granularity of 100ms

-- cause one sequential scan
BEGIN;
SET LOCAL enable_seqscan TO on;
SET LOCAL enable_indexscan TO off;
SET LOCAL enable_bitmapscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;
SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;

-- scan one partition
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_part partition (p1) WHERE noidx_col = 1;
SELECT count(*) FROM test_last_scan_part partition (p1) WHERE noidx_col = 1;

-- scan one subpartition
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_subpart subpartition (sp4) WHERE noidx_col = 12;
SELECT count(*) FROM test_last_scan_subpart subpartition (sp4) WHERE noidx_col = 12;

SELECT pg_stat_force_next_flush();
COMMIT;
SELECT pg_sleep(1);
-- check that just sequential scan stats were incremented
SELECT relname, seq_scan, test_last_seq < last_seq_scan AS seq_ok, idx_scan, test_last_idx = last_idx_scan AS idx_ok
FROM test_check left join pg_stat_all_tables on id=relid ORDER BY relname;

-- fetch timestamps from before the next test
DELETE FROM test_check;
INSERT INTO test_check SELECT relid, last_seq_scan, last_idx_scan
FROM pg_stat_all_tables WHERE relid in ('test_last_scan'::regclass, 'test_last_scan_part'::regclass, 'test_last_scan_subpart'::regclass);
SELECT pg_sleep(0.1);

-- cause one index scan
BEGIN;
SET LOCAL enable_seqscan TO off;
SET LOCAL enable_indexscan TO on;
SET LOCAL enable_bitmapscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan WHERE idx_col = 1;

-- scan one partition
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_part WHERE idx_col = 12;
SELECT count(*) FROM test_last_scan_part WHERE idx_col = 12;

-- scan one subpartition
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 2;
SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 2;

SELECT pg_stat_force_next_flush();
COMMIT;
SELECT pg_sleep(1);
-- check that just index scan stats were incremented
SELECT relname, seq_scan, test_last_seq = last_seq_scan AS seq_ok, idx_scan, test_last_idx < last_idx_scan AS idx_ok
FROM test_check left join pg_stat_all_tables on id=relid ORDER BY relname;

-- fetch timestamps from before the next test
DELETE FROM test_check;
INSERT INTO test_check SELECT relid, last_seq_scan, last_idx_scan
FROM pg_stat_all_tables WHERE relid in ('test_last_scan'::regclass, 'test_last_scan_part'::regclass, 'test_last_scan_subpart'::regclass);
SELECT pg_sleep(0.1);

-- cause one bitmap index scan
BEGIN;
SET LOCAL enable_seqscan TO off;
SET LOCAL enable_indexscan TO off;
SET LOCAL enable_bitmapscan TO on;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan WHERE idx_col = 1;

-- scan all partition
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_part WHERE idx_col = 1 or idx_col = 12;
SELECT count(*) FROM test_last_scan_part WHERE idx_col = 1 or idx_col = 12;

-- scan one partition
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 1 or idx_col = 2;
SELECT count(*) FROM test_last_scan_subpart WHERE idx_col = 1 or idx_col = 2;

SELECT pg_stat_force_next_flush();
COMMIT;
SELECT pg_sleep(1);
-- check that just index scan stats were incremented
SELECT relname, seq_scan, test_last_seq = last_seq_scan AS seq_ok, idx_scan, test_last_idx < last_idx_scan AS idx_ok
FROM test_check left join pg_stat_all_tables on id=relid ORDER BY relname;



-- End of Stats Test
reset search_path;
set client_min_messages = error;
drop schema sch_stats cascade;
