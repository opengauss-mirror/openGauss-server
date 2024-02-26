DROP SCHEMA IF EXISTS test_alter_seq_max_in_txn_01 CASCADE;

CREATE SCHEMA test_alter_seq_max_in_txn_01;

SET CURRENT_SCHEMA TO test_alter_seq_max_in_txn_01;

create sequence seqa maxvalue 50;

--test what sequence would be like before/after ALTER SEQUENCE MAXVALUE txns' rollback

begin;

select * from seqa;

alter sequence seqa maxvalue 40;

select * from seqa;

rollback;

select * from seqa;

--test what sequence would be like before/after ALTER SEQUENCE MAXVALUE txns' rollback

begin;

select * from seqa;

alter sequence seqa maxvalue 40;

select * from seqa;

commit;

select * from seqa;

DROP SCHEMA test_alter_seq_max_in_txn_01 CASCADE;