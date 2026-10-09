-- LWLock activity: no-wait counter columns added to wait events
SELECT * FROM pg_catalog.get_instr_wait_event(NULL) LIMIT 0;
SELECT * FROM dbe_perf.wait_events LIMIT 0;
SELECT * FROM dbe_perf.global_wait_events LIMIT 0;
-- the new no-wait counter columns must be present and non-negative
SELECT COALESCE(bool_and(request_count >= 0 AND nw_acquired >= 0 AND nw_not_acquired >= 0), true) AS counters_valid FROM dbe_perf.wait_events;
