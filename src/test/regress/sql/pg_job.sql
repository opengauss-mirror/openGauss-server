create database pl_test_job DBCOMPATIBILITY 'pg';
\c pl_test_job;
CREATE TABLE pg_job_test_1(COL1 INT);

CREATE OR REPLACE PROCEDURE pg_job_test()
AS
aaa int;
BEGIN
    FOR i IN 0..20 LOOP
        INSERT INTO pg_job_test_1(COL1) VALUES (i);
        IF i % 2 = 0 THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;
    END LOOP;
END;
/
select dbe_task.id_submit(103, 'call pg_job_test();', sysdate, 'sysdate+3.0/24');
select pg_sleep(5);
select count(*) from pg_job_test_1;

drop procedure pg_job_test;
drop table if exists pg_job_test_1;
call dbe_task.cancel(103);
\c regression;
drop database IF EXISTS pl_test_job;
