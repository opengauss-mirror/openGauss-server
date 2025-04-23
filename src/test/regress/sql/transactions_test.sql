CREATE TABLE no_reset_test(a int);
insert into no_reset_test values(1);

-- Test transaction variable cannot be reset.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
select count(*) from no_reset_test;
reset transaction_isolation; --error
END;

BEGIN TRANSACTION READ ONLY;
select count(*) from no_reset_test;
reset transaction_read_only; --error
END;

BEGIN TRANSACTION DEFERRABLE;
select count(*) from no_reset_test;
reset transaction_deferrable; --error
END;

DROP TABLE no_reset_test;

SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;
START TRANSACTION READ WRITE;
CREATE TABLE transaction_test_table(a int);
insert into transaction_test_table values(1);
update transaction_test_table set a=2 where a=1;
select * from transaction_test_table;
commit;
-- error
delete from transaction_test_table;
drop table transaction_test_table;

START TRANSACTION READ WRITE;
drop table transaction_test_table;
commit;
