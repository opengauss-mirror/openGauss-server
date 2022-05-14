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
