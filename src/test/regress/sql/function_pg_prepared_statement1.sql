drop table if exists table_test_prepared_statement1;

create table table_test_prepared_statement1 (a int);

prepare stmt1_in_session1 as insert into table_test_prepared_statement1 values (1);

select pg_sleep(2);

drop table table_test_prepared_statement1;
