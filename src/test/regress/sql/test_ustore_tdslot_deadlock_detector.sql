-- Test TD slot deadlock detecting logic.
-- Expected: UStore engine should not loop forever if there are two transactions
-- that wait for each other for free TD slot.

CREATE TABLE bmsql_district (
  d_w_id       integer       not null,
  d_id         integer       not null,
  d_ytd        decimal(12,2),
  d_tax        decimal(4,4),
  d_next_o_id  integer,
  d_name       varchar(10),
  d_street_1   varchar(20),
  d_street_2   varchar(20),
  d_city       varchar(20),
  d_state      char(2),
  d_zip        char(8)
) WITH (FILLFACTOR=100, STORAGE_TYPE=USTORE, INIT_TD=2);

-- Populate two blocks: block 0 and block 1
BEGIN;
INSERT INTO bmsql_district SELECT 1, generate_series(1, 93), 30000.00, 0.4, 0, 'District 1', 'Street 1', 'Street 2', 'City 1', 'CA', '12345';
INSERT INTO bmsql_district values (1, 94, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO bmsql_district values (1, 95, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO bmsql_district values (1, 96, NULL, 0.4, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO bmsql_district SELECT 2, generate_series(1, 93), 30000.00, 0.4, 0, 'District 2', 'Street 2', 'Street 2', 'City 2', 'CA', '12345';
INSERT INTO bmsql_district values (2, 94, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO bmsql_district values (2, 95, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO bmsql_district values (2, 96, NULL, 0.4, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
COMMIT;


\parallel on 3

-- T1 reserves TD slot on block 0, but dont commit yet
BEGIN
	DELETE FROM bmsql_district WHERE d_w_id = 1 AND d_id = 1;

	pg_sleep(2); -- allow T3 to reserve the last ITL on block 1

	-- T1 update a tuple on block 1 but no ITL slot available (occupied by T2 and T3), so wait for oldest XID (T2)
	UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = 2 AND d_id = 3;
END;
/

-- T2 reserve ITL on block 1, but dont commit
BEGIN
	DELETE FROM bmsql_district WHERE d_w_id = 2 AND d_id = 1;

	pg_sleep(4); -- allow T3 to reserve the last ITL on block 0 and allow T1 to do the UPDATE first

	-- T2 update a tuple on block 0 but no ITL slot available (occupied by T1 and T3), so wait for oldest XID (T1)
	UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = 1 AND d_id = 3;
END;
/

-- T3, reserve last ITLs on block 0 and 1, dont commit
BEGIN
	pg_sleep(1); -- allow T1 and T2 to start first
	DELETE FROM bmsql_district WHERE d_w_id = 1 AND d_id = 2;
	DELETE FROM bmsql_district WHERE d_w_id = 2 AND d_id = 2;

	-- Keep holding the TD slot. This forces T1 and T2 to wait for each other.
	-- T2 should hit a deadlock and T1 continues
	-- Note this assumes macro TD_RESERVATION_TIMEOUT_MS is 60 seconds.
	pg_sleep(90);
END;
/

\parallel off

-- d_next_o_id should be 1 because T1 was able to update it
SELECT d_w_id, d_id, d_next_o_id from bmsql_district WHERE d_w_id = 2 AND d_id = 3;

-- d_next_o_id should be 0 because T2 aborted
SELECT d_w_id, d_id, d_next_o_id from bmsql_district WHERE d_w_id = 1 AND d_id = 3;

DROP TABLE bmsql_district;
