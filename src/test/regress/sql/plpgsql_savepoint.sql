/*
################################################################################
# TESTCASE NAME : plpgsql_savepoint
# COMPONENT(S)  : plpgsql savepoint
################################################################################
*/

CREATE TABLE pl_txn_t(tc1 INT, tc2 INT);

-- normal case 1
CREATE OR REPLACE PROCEDURE sp_normal_1 IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT SAVE_A;

    INSERT INTO pl_txn_t VALUES(2, 2);
    SAVEPOINT SAVE_B;

    INSERT INTO pl_txn_t VALUES(3, 3);
    SAVEPOINT SAVE_C;

    INSERT INTO pl_txn_t VALUES(4, 4);
    INSERT INTO pl_txn_t VALUES(5, 5);
    ROLLBACK TO SAVEPOINT SAVE_C;

    INSERT INTO pl_txn_t VALUES(6, 6);
    ROLLBACK TO SAVEPOINT SAVE_B;

    INSERT INTO pl_txn_t VALUES(2, 2);
    ROLLBACK TO SAVEPOINT SAVE_A;
  END;
  /
SELECT sp_normal_1();
COMMIT;
SELECT sp_normal_1(), sp_normal_1();
DROP PROCEDURE sp_normal_1;

-- normal case 2
CREATE OR REPLACE PROCEDURE sp_normal_2 IS
  BEGIN
    SAVEPOINT
          SAVE_A;
    INSERT INTO pl_txn_t VALUES(1, 1);
    ROLLBACK TO SAVEPOINT
          SAVE_A;
    SAVEPOINT B;
  END;
  /
SELECT sp_normal_2();
BEGIN;
SELECT sp_normal_2();
SELECT sp_normal_2();  -- 执行失败，暂不支持语句外部SAVEPOINT
COMMIT;
DROP PROCEDURE sp_normal_2;

-- savepoint name as variable in PL
CREATE OR REPLACE PROCEDURE sp_name_variable IS
    sp_name NVARCHAR2(100) := 'SAVE_A';
  BEGIN
    SAVEPOINT sp_name;
    ROLLBACK TO sp_name;
  END;
  /
CALL sp_name_variable();

CREATE OR REPLACE PROCEDURE sp_name_variable IS
    sp_name NVARCHAR2(100) := 'SAVE_A';
  BEGIN
    SAVEPOINT sp_name;
    ROLLBACK TO SAVE_A;   -- no such savepoint
  END;
  /
CALL sp_name_variable();
DROP PROCEDURE sp_name_variable;

-- length of savepoint name is too big.
CREATE OR REPLACE PROCEDURE sp_name_length IS
  BEGIN
    SAVEPOINT sp_name_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;
    ROLLBACK TO sp_name_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;
  END;
  /
CALL sp_name_length();

-- no savepoint outside statement
CREATE OR REPLACE PROCEDURE sp_no_outside IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    COMMIT;
    ROLLBACK TO SAVEPOINT SAVE_A;
  END;
  /
CALL sp_no_outside();
BEGIN;
SAVEPOINT SAVE_A;
ROLLBACK TO SAVEPOINT SAVE_A;
SELECT sp_no_outside();
ROLLBACK;
BEGIN;
SAVEPOINT SAVE_A;
RELEASE SAVEPOINT SAVE_A;
CALL sp_no_outside();
ROLLBACK;
DROP PROCEDURE sp_no_outside;

-- savepoint + commit / rollback
CREATE OR REPLACE PROCEDURE sp_commit_rollback(p INT) IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT SAVE_A;
    IF p%2 = 0 then
      ROLLBACK;
    ELSE
      COMMIT;
    END IF;
  END;
  /
SELECT sp_commit_rollback(0);
SELECT sp_commit_rollback(1);

CREATE OR REPLACE PROCEDURE sp_commit_rollback IS
  BEGIN
    SAVEPOINT save_a;
    INSERT INTO pl_txn_t VALUES(1, 1);
    ROLLBACK TO save_a;
    COMMIT;
  END;
  /
CALL sp_commit_rollback();
DROP PROCEDURE sp_commit_rollback;

CREATE OR REPLACE PROCEDURE pl_commit IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    COMMIT;
  END;
  /
SELECT pl_commit();
DROP PROCEDURE pl_commit;

CREATE OR REPLACE PROCEDURE commit_drop_sp IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT SAVE_1;
    INSERT INTO pl_txn_t VALUES(2, 2);
    SAVEPOINT SAVE_2;
    COMMIT;
    INSERT INTO pl_txn_t VALUES(4, 4);
    ROLLBACK TO SAVEPOINT SAVE_1;  --commit已删除了SAVE_1
  END;
  /
SELECT commit_drop_sp();  -- no such savepoint
DROP PROCEDURE commit_drop_sp;

-- savepoint in cursor
CREATE OR REPLACE FUNCTION sp_inner RETURN INTEGER
AS
  BEGIN
    SAVEPOINT save_a;
    COMMIT;
    SAVEPOINT save_a;
    RETURN 1;
  END;
  /
CREATE OR REPLACE PROCEDURE sp_in_cursor IS
    CURSOR c1 FOR SELECT sp_inner() FROM pl_txn_t;
    val INT;
  BEGIN
    SAVEPOINT save_a;
    OPEN c1;
    FETCH c1 INTO val;
    CLOSE c1;
  EXCEPTION
    WHEN OTHERS THEN
       RAISE NOTICE 'wrong 1'; 
  END;
  /
SELECT sp_in_cursor(); 
DROP PROCEDURE sp_in_cursor;
DROP PROCEDURE sp_inner;

CREATE OR REPLACE FUNCTION sp_inner  RETURN INTEGER
AS
  BEGIN
    ROLLBACK TO save_axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;
    RETURN 1;
  END;
  /
CREATE OR REPLACE PROCEDURE sp_in_cursor is
    CURSOR c1 FOR SELECT sp_inner() FROM pl_txn_t;
    val INT;
  BEGIN
    SAVEPOINT save_a;
    OPEN c1;
    FETCH c1 INTO val;
    CLOSE c1;
  END;
  /
CALL sp_in_cursor();
DROP PROCEDURE sp_in_cursor;
DROP PROCEDURE sp_inner;

-- savepoint in subroutine
CREATE OR REPLACE PROCEDURE sp_subroutine IS
  BEGIN
    SAVEPOINT save_0;
    INSERT INTO pl_txn_t VALUES(1, 1);
    ROLLBACK TO save_0;
    SAVEPOINT save_2;
    SAVEPOINT save_3;
  END;
  /
CREATE OR REPLACE PROCEDURE sp_in_subroutine IS
  BEGIN
    SAVEPOINT save_1;
    sp_subroutine();
    INSERT INTO pl_txn_t VALUES(1, 1);
    ROLLBACK TO save_1;
    INSERT INTO pl_txn_t VALUES(2, 2);
  END;
  /
SELECT sp_in_subroutine();
SELECT sp_in_subroutine();

CREATE OR REPLACE PROCEDURE sp_in_subroutine IS
  BEGIN
    sp_subroutine();
    sp_subroutine();
  END;
  /
SELECT sp_in_subroutine();
DROP PROCEDURE sp_in_subroutine;
DROP PROCEDURE sp_subroutine;

-- duplicate name
CREATE OR REPLACE PROCEDURE sp_duplicate_name IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT SAVE_A;
    ROLLBACK TO SAVEPOINT SAVE_A;
    SAVEPOINT SAVE_A;
    ROLLBACK TO SAVEPOINT SAVE_A;
  END;
  /
SELECT sp_duplicate_name();
DROP PROCEDURE sp_duplicate_name;

-- savepoint in SPI executor context
CREATE OR REPLACE PROCEDURE pl_subroutine IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    ROLLBACK;   -- 该行会销毁PopOverrideSearchPath，导致不匹配
  END;
  /
CREATE OR REPLACE PROCEDURE sp_spi_rollback IS
  BEGIN
    SAVEPOINT save_1;
    pl_subroutine();
  END;
  /
SELECT sp_spi_rollback();
DROP PROCEDURE sp_spi_rollback;
DROP PROCEDURE pl_subroutine;

CREATE OR REPLACE PROCEDURE pl_subroutine IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    ROLLBACK TO save_1;  -- 该行会销毁SavepointTest0的调用上下文
  END;
  /
CREATE OR REPLACE PROCEDURE sp_spi_rollbackto IS
  BEGIN
    SAVEPOINT save_1;
    --ROLLBACK;
    pl_subroutine();
    INSERT INTO pl_txn_t VALUES(2, 2);
    ROLLBACK TO save_1;
    INSERT INTO pl_txn_t VALUES(3, 3);
  END;
  /
SELECT sp_spi_rollbackto();
SELECT sp_spi_rollbackto();
DROP PROCEDURE sp_spi_rollbackto;
DROP PROCEDURE pl_subroutine;

-- savepoint + subroutine's commit/rollback
CREATE OR REPLACE PROCEDURE pl_commit IS
  BEGIN
    COMMIT;        -- snapshot destoryed when substransaction finishes.
  END;
  /
CREATE OR REPLACE PROCEDURE sp_inner_commit IS
  BEGIN
    SAVEPOINT SAVE_A0;
    pl_commit();
  END;
  /
SELECT sp_inner_commit();
DROP PROCEDURE sp_inner_commit;
DROP PROCEDURE pl_commit;

CREATE OR REPLACE PROCEDURE pl_rollback IS
  BEGIN
    ROLLBACK;        -- snapshot destoryed when substransaction finishes.
  END;
  /
CREATE OR REPLACE PROCEDURE sp_inner_rollback IS
  BEGIN
    SAVEPOINT SAVE_A0;
    pl_rollback();
  END;
  /
CALL sp_inner_rollback();
DROP PROCEDURE sp_inner_rollback;
DROP PROCEDURE pl_rollback;

-- savepoint + exception
CREATE OR REPLACE PROCEDURE SavepointTest IS
    exc_1 EXCEPTION;
  BEGIN
    COMMIT;
    SAVEPOINT SAVE_A;
    RAISE exc_1;
  EXCEPTION
    WHEN OTHERS THEN
       RAISE NOTICE 'wrong 1'; 
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  BEGIN
    SavepointTest();
  EXCEPTION
    WHEN OTHERS THEN
       ROLLBACK TO SAVE_A;
       RAISE NOTICE 'wrong 2'; 
  END;
  /
SELECT SavepointTest();
SELECT SavepointTest0();

CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT SAVE_A;
    INSERT INTO pl_txn_t VALUES(2, 2);
    ROLLBACK TO SAVEPOINT SAVE_B;
  EXCEPTION
    WHEN exc_1 THEN
      ROLLBACK TO SAVEPOINT SAVE_A;
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      ROLLBACK TO SAVEPOINT SAVE_B;
      INSERT INTO pl_txn_t VALUES(3,3);
  END;
  /
SELECT SavepointTest();

CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT SAVE_A;
    INSERT INTO pl_txn_t VALUES(2, 2);
    RAISE exc_1;
  EXCEPTION
    WHEN exc_1 THEN
      ROLLBACK TO SAVEPOINT SAVE_A;
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      ROLLBACK TO SAVEPOINT SAVE_B;
      INSERT INTO pl_txn_t VALUES(3,3);
  END;
  /
SELECT SavepointTest();
DROP PROCEDURE SavepointTest;
DROP PROCEDURE SavepointTest0;

-- savepoint + cursor hold
CREATE OR REPLACE PROCEDURE SavepointTest IS
    CURSOR c1 IS SELECT tc1 FROM pl_txn_t;
    val INT;
    val1 INT;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    INSERT INTO pl_txn_t VALUES(2,2);
    OPEN c1;
    SAVEPOINT save_a;
    FETCH c1 INTO val;
    ROLLBACK TO save_a;
    FETCH c1 INTO val1;
    CLOSE c1;
  END;
  /
SELECT SavepointTest();

CREATE OR REPLACE PROCEDURE SavepointTest IS
    CURSOR c1 IS SELECT tc1 FROM pl_txn_t;
    val INT;
    val1 INT;
  BEGIN
    INSERT INTO pl_txn_t values(1,1);
    INSERT INTO pl_txn_t values(2,2);
    SAVEPOINT save_a;
    OPEN c1;
    FETCH c1 INTO val;
    ROLLBACK to save_a;
    FETCH c1 INTO val1;   --fetch out of sequence
    CLOSE c1;
  END;
  /
SELECT SavepointTest();

CREATE OR REPLACE PROCEDURE SavepointTest IS
    CURSOR c1 IS SELECT tc1 FROM pl_txn_t;
    val INT;
    val1 INT;
  BEGIN
    INSERT INTO pl_txn_t values(1,1);
    INSERT INTO pl_txn_t values(2,2);
    SAVEPOINT save_a;
    OPEN c1;
    FETCH c1 INTO val;
    COMMIT;
    FETCH c1 INTO val1;
    CLOSE c1;
  END;
  /
SELECT SavepointTest();
DROP PROCEDURE SavepointTest;

-- spi connect
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  CURSOR c1 IS SELECT tc1 FROM pl_txn_t;
  val INT;
  val1 INT;
  BEGIN
    ROLLBACK TO SAVEPOINT SAVE_A0;
    --INSERT INTO pl_txn_t VALUES(1, 1);
    --INSERT INTO pl_txn_t VALUES(2, 2);
    SAVEPOINT SAVE_A1;
    OPEN c1;
    FETCH c1 INTO val;
    COMMIT;
    FETCH c1 INTO val;
    CLOSE c1;
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  BEGIN
    SAVEPOINT SAVE_A0;
    --INSERT INTO pl_txn_t VALUES(1, 1);
    --INSERT INTO pl_txn_t VALUES(2, 2);
    --SAVEPOINT SAVE_A1;
    SavepointTest0();
  END;
  /
SELECT SavepointTest();
DROP PROCEDURE SavepointTest0;
DROP PROCEDURE SavepointTest;

-- savepoint in exception, don't destory exception's subtransaction
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(2, 2);
    COMMIT;
    INSERT INTO pl_txn_t VALUES(3, 3);
    SAVEPOINT save_b;
    INSERT INTO pl_txn_t VALUES(4, 4);
  EXCEPTION
    WHEN exc_1 THEN
      ROLLBACK TO SAVEPOINT save_a;
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      ROLLBACK TO SAVEPOINT save_a;
      INSERT INTO pl_txn_t VALUES(5, 5);
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT save_a;
    SavepointTest0();
    ROLLBACK TO save_b;
  END;
  /
TRUNCATE pl_txn_t;
SELECT SavepointTest();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

-- exception's subtransaction id changes.
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  exc_1 EXCEPTION;
  BEGIN
    -- exception's subtransaction id is 3
    INSERT INTO pl_txn_t VALUES(2, 2);
    COMMIT;   -- exception's subtransaction id changes to 2.
    INSERT INTO pl_txn_t VALUES(3, 3);
  EXCEPTION
    WHEN exc_1 THEN
      ROLLBACK TO SAVEPOINT SAVE_A;
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      ROLLBACK TO SAVEPOINT SAVE_A;
      INSERT INTO pl_txn_t VALUES(5, 5);
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT save_a;   -- subtransaction id = 2
    SavepointTest0();
  END;
  /
TRUNCATE pl_txn_t;
SELECT SavepointTest();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

-- automatic rollback to the last savepoint:save_b in exception
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  exc_1 EXCEPTION;
  BEGIN
    -- exception's subtransaction id is 3
    INSERT INTO pl_txn_t VALUES(2, 2);
    COMMIT;   -- destory save_a automatically
    INSERT INTO pl_txn_t VALUES(3, 3);
    SAVEPOINT save_b;
    INSERT INTO pl_txn_t VALUES(4, 4);
    RAISE exc_1;
  EXCEPTION
    -- auto rollback to save_b
    WHEN exc_1 THEN
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      INSERT INTO pl_txn_t VALUES(5, 5);
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT save_a;
    SavepointTest0();
  END;
  /
TRUNCATE pl_txn_t;
SELECT SavepointTest();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

-- rollback to in exception
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  exc_1 EXCEPTION;
  BEGIN
    -- exception's subtransaction id is 3
    INSERT INTO pl_txn_t VALUES(2, 2);
    COMMIT;   -- destory save_a automatically
    INSERT INTO pl_txn_t VALUES(3, 3);
    SAVEPOINT save_b;
    INSERT INTO pl_txn_t VALUES(4, 4);
    SAVEPOINT save_c;
    ROLLBACK TO save_none;  -- no such savepoint
  EXCEPTION
    -- auto rollback to save_c
    WHEN exc_1 THEN
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      ROLLBACK TO SAVEPOINT SAVE_b;
      INSERT INTO pl_txn_t VALUES(5, 5);
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT save_a;
    SavepointTest0();
  END;
  /
TRUNCATE pl_txn_t;
SELECT SavepointTest();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

-- destory SPI connect while abort subtransaction
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  exc_1 EXCEPTION;
  var1 INT;
  BEGIN
    SAVEPOINT save_c;
    ROLLBACK TO SAVEPOINT save_a;
    SELECT sum(t1.tc1 + t2.tc2) INTO var1 FROM pl_txn_t t1, pl_txn_t t2 WHERE T1.TC2 + 1 = T2.TC2 + 4;
    CREATE TABLE pl_txn_t(tc1 INT, tc2 INT);
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT save_a;
    SAVEPOINT save_b;
    SavepointTest0();
  END;
  /
SELECT SavepointTest();

-- savepoint outside STP
create or replace procedure SavepointTest is
  exc_1 exception;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT save_a;
    ROLLBACK TO save_out;
    SAVEPOINT save_b;
  END;
  /
BEGIN;
INSERT INTO pl_txn_t VALUES(0, 0);
SAVEPOINT save_out;
SELECT SavepointTest();
SELECT SavepointTest();
ROLLBACK TO save_b;
COMMIT;

CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(2, 2);
    INSERT INTO pl_txn_t VALUES(3, 3);
    SAVEPOINT save_a;
    INSERT INTO pl_txn_t VALUES(4, 4);
    RAISE exc_1;
  EXCEPTION
    WHEN exc_1 THEN
      ROLLBACK TO SAVEPOINT save_out;
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      ROLLBACK TO SAVEPOINT save_a;
      INSERT INTO pl_txn_t VALUES(5, 5);
  END;
  /
BEGIN;
SAVEPOINT save_out;
SELECT SavepointTest();
SELECT SavepointTest();
ROLLBACK TO save_out;
END;

-- don't switch to top portal's resourceowner since it is invalid.
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(2, 2);
    INSERT INTO pl_txn_t VALUES(3, 3);
    ROLLBACK TO SAVEPOINT save_out;
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    SavepointTest0();
    INSERT INTO pl_txn_t VALUES(1, 1);
    RAISE exc_1;
  EXCEPTION
    WHEN exc_1 THEN
      INSERT INTO pl_txn_t VALUES(4, 4);
    WHEN OTHERS THEN
      INSERT INTO pl_txn_t VALUES(6, 6);
  END;
  /
TRUNCATE pl_txn_t;
BEGIN;
SAVEPOINT save_out;
SELECT SavepointTest();
SELECT * from pl_txn_t order by 1, 2;
END;

-- exception's subtransaction is destoryed by rollbackiing to outside savepoint
CREATE OR REPLACE PROCEDURE SavepointTest0 IS
  exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(2, 2);
    INSERT INTO pl_txn_t VALUES(3, 3);
    SAVEPOINT save_a;
    INSERT INTO pl_txn_t VALUES(4, 4);
    RAISE exc_1;
  EXCEPTION
    WHEN exc_1 THEN
      ROLLBACK TO SAVEPOINT save_out;
      RAISE NOTICE 'wrong 1';
    WHEN OTHERS THEN
      RAISE NOTICE 'wrong 2';
      ROLLBACK TO SAVEPOINT save_a;
      INSERT INTO pl_txn_t VALUES(5, 5);
  END;
  /
CREATE OR REPLACE PROCEDURE SavepointTest IS
  exc_1 EXCEPTION;
  BEGIN
    SavepointTest0();
    INSERT INTO pl_txn_t VALUES(1, 1);
    RAISE exc_1;
  EXCEPTION
    WHEN exc_1 THEN
      INSERT INTO pl_txn_t VALUES(4, 4);
    WHEN OTHERS THEN
      INSERT INTO pl_txn_t VALUES(6, 6);
  END;
  /
BEGIN;
SAVEPOINT save_out;
SELECT SavepointTest();
SELECT SavepointTest();
END;

DROP PROCEDURE SavepointTest0;
DROP PROCEDURE SavepointTest;

-- switch to stmt top portal memory context
CREATE OR REPLACE PROCEDURE SavepointTest IS
  val VARCHAR(10) := '0';
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT save_a;
    val := val || '1';
    ROLLBACK TO SAVEPOINT save_a;
    val :=  val || '2';
  END;
  /
SELECT SavepointTest();
SELECT SavepointTest();
DROP PROCEDURE SavepointTest;

-- don't support execute immedidate savepoint
CREATE OR REPLACE PROCEDURE SavepointTest IS
  BEGIN
    INSERT INTO pl_txn_t VALUES(1, 1);
    SAVEPOINT SAVE_A;

    INSERT INTO pl_txn_t VALUES(2, 2);
    execute immediate 'rollback to ' || 'save_a';

    INSERT INTO pl_txn_t VALUES(2, 2);
    ROLLBACK TO SAVEPOINT SAVE_A;
  END;
  /
select SavepointTest();
DROP PROCEDURE SavepointTest;

-- wrong during execut stage
CREATE OR REPLACE PROCEDURE sp_inner1 IS
  BEGIN
    SAVEPOINT save_a;
    INSERT INTO pl_txn_t VALUES(2,2);
    INSERT INTO pl_txn_t VALUES(2,2); --wrong
  END;
  /
CREATE OR REPLACE PROCEDURE sp_test is
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    sp_inner1();
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CREATE UNIQUE INDEX idx_unique_tc1_tc2 ON pl_txn_t(tc1, tc2);
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
DROP INDEX idx_unique_tc1_tc2;

-- wrong during plan stage
CREATE OR REPLACE PROCEDURE sp_inner1 IS
  BEGIN
    SAVEPOINT save_a;
    INSERT INTO pl_txn_t VALUES(2,2);
    INSERT INTO pl_txn_t VALUES(2,2,2); --wrong
  END;
  /
CREATE OR REPLACE PROCEDURE sp_test is
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    sp_inner1();
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

-- wrong during pl others
CREATE OR REPLACE PROCEDURE sp_inner1 IS
  exc_1 EXCEPTION;
  BEGIN
    SAVEPOINT save_a;
    INSERT INTO pl_txn_t VALUES(2,2);
    RAISE exc_1;
  END;
  /
CREATE OR REPLACE PROCEDURE sp_test is
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    sp_inner1();
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
DROP PROCEDURE sp_test;
DROP PROCEDURE sp_inner1;

-- don't rollback exception's subtxn
CREATE OR REPLACE PROCEDURE sp_test is
    exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    RAISE exc_1;
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

DROP PROCEDURE sp_test;

-- release savepoint
CREATE OR REPLACE PROCEDURE sp_test is
    exc_1 EXCEPTION;
  BEGIN
    SAVEPOINT s1;
    INSERT INTO pl_txn_t VALUES(1,1);
    RELEASE s1;
    INSERT INTO pl_txn_t VALUES(2,2);
    ROLLBACK TO s1;
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

-- rollback to savepoint before released one
CREATE OR REPLACE PROCEDURE sp_test is
    exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(0,0);
    SAVEPOINT s1;
    INSERT INTO pl_txn_t VALUES(1,1);
    SAVEPOINT s2;
    INSERT INTO pl_txn_t VALUES(2,2);
    RELEASE s2;
    INSERT INTO pl_txn_t VALUES(3,3);
    ROLLBACK TO s1;
    INSERT INTO pl_txn_t VALUES(4,4);
    RAISE exc_1;
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
DROP PROCEDURE sp_test;

-- wrong during plan stage without savepoint
CREATE OR REPLACE PROCEDURE sp_test is
    exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    -- cast wrong with hold some resource
    INSERT INTO pl_txn_t VALUES(1,1,1);  --wrong execute
    UPDATE pl_txn_t SET tc2 = 'null'::numeric; --wrong no execute
    INSERT INTO pl_txn_t VALUES(2,2); -- no execute
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;

-- wrong during execute stage without savepoint
CREATE OR REPLACE PROCEDURE sp_test is
    exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    -- cast wrong with hold some resource
    UPDATE pl_txn_t SET tc2 = 0 WHERE tc1 / (tc2 - 1) = 1;
    SELECT COUNT(1) FROM pl_txn_t WHERE tc1 / (tc2 - 1) = 1;
    INSERT INTO pl_txn_t VALUES(2,2); -- no execute
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
CREATE OR REPLACE PROCEDURE sp_test1 is
    exc_1 EXCEPTION;
  BEGIN
    INSERT INTO pl_txn_t VALUES(1,1);
    -- cast wrong with hold some resource
    SELECT COUNT(1) FROM pl_txn_t WHERE tc1 / (tc2 - 1) = 1;
    INSERT INTO pl_txn_t VALUES(2,2); -- no execute
  EXCEPTION
      WHEN OTHERS THEN
        RAISE INFO 'wrong1';
  END;
  /
create table t1(a int);
drop type if exists typet1;
create type typet1 is table of t1%rowtype;
create or replace procedure p3()
as
l_error_count number:=0;
type02 typet1;
begin
for i in 1..2 loop
    type02(i).a=1;
    raise info 'type02(1).a %',type02(1).a;
end loop;
savepoint s1;
select 1/0;
exception
when others then
    rollback to savepoint s1;
end;
/
call p3();

SET behavior_compat_options = 'plstmt_implicit_savepoint';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
TRUNCATE TABLE pl_txn_t;
CALL sp_test1();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
SET behavior_compat_options = '';
TRUNCATE TABLE pl_txn_t;
CALL sp_test();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
TRUNCATE TABLE pl_txn_t;
CALL sp_test1();
SELECT * FROM pl_txn_t ORDER BY 1, 2;
DROP PROCEDURE sp_test;
DROP PROCEDURE sp_test1;
DROP PROCEDURE P3;
DROP TABLE pl_txn_t;

SET behavior_compat_options = '';
-- test tuplde desc refed by sql
create table test_subxact_t1 (a int, b varchar2(100), c int);
insert into test_subxact_t1 values(1,'aa',2),(2,'bb',2),(3,'cc',3);


create or replace procedure insert_log(v1 int, v2 varchar2, v3 int) as
begin
raise info '%',v1;
commit;
end;
/
create or replace procedure test_subxact_p1() as
    type r1 is record (a int, b varchar2(100), c int);
    type r2 is table of r1;
    va r2;
begin
    select a,b,c from test_subxact_t1 bulk collect into va;
    insert_log(v1=>va(1).a,v2=>'aa',v3=>3);
    insert_log(v1=>va(2).a,v2=>'aa',v3=>3);
    insert_log(v1=>va(3).a,v2=>'aa',v3=>3);
exception when others then
    commit;
end;
/

call test_subxact_p1();
drop procedure test_subxact_p1;
drop procedure insert_log;
drop table test_subxact_t1;

